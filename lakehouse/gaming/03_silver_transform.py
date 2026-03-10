"""
GamingIQ — Silver Layer Transforms
====================================
Reads bronze Delta tables and produces curated silver tables:

  raw_player_events  ->  player_profiles   (engagement metrics, churn scoring)
  raw_transactions   ->  economy_metrics   (aggregate by item category, inflation)
  raw_match_events   ->  match_quality     (fairness scores, completion rates)

Usage:
    spark-submit lakehouse/gaming/03_silver_transform.py
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("gaming_silver_transform")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "gaming_iq"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Churn model registered in MLflow
CHURN_MODEL_NAME = "Player_Churn_Predictor"
CHURN_MODEL_URI = f"models:/{CHURN_MODEL_NAME}/Production"

# Churn feature columns
CHURN_FEATURE_COLS = [
    "days_since_last_login",
    "session_frequency_7d",
    "purchase_count_30d",
    "friend_count",
    "skill_rating",
]

# Player segments and their LTV multipliers
SEGMENT_LTV_MULTIPLIER = {
    "Whale": 8.0,
    "Dolphin": 3.0,
    "Minnow": 1.0,
    "Free-to-Play": 0.1,
}

# Inflation baseline
INFLATION_BASELINE = 1.0

# Fairness score parameters
IDEAL_SKILL_SPREAD = 50.0   # ideal spread in rating points
MAX_QUEUE_PENALTY_SEC = 60.0 # queue times beyond this degrade fairness


# =========================================================================
# 1.  raw_player_events  ->  player_profiles
# =========================================================================
def transform_player_profiles(spark: SparkSession) -> DataFrame:
    """
    Aggregate raw player events into enriched player profiles with
    engagement metrics and churn risk scoring.

    Computes per player:
      - days_since_last_login
      - session_frequency_7d (avg sessions per day, last 7 days)
      - purchase_count_30d
      - friend_count (from social events)
      - skill_rating (from level progression)
      - churn_risk_score (ML model or rule-based fallback)
      - churn_risk_level (Low / Medium / High / Critical)
      - ltv (predicted lifetime value)
    """
    logger.info(
        "Transforming: %s.raw_player_events -> %s.player_profiles", BRONZE, SILVER
    )

    events_df = spark.table(f"{BRONZE}.raw_player_events")

    # Current timestamp reference for recency calculations.
    now = F.current_timestamp()
    now_date = F.current_date()

    # --- Per-player aggregation ---
    player_agg = (
        events_df
        .groupBy("player_id", "game_title", "region")
        .agg(
            # Days since last login.
            F.datediff(now_date, F.max(F.to_date("timestamp"))).alias("days_since_last_login"),

            # Session frequency (last 7 days): count distinct sessions / 7.
            (
                F.count(
                    F.when(
                        F.datediff(now_date, F.to_date("timestamp")) <= 7,
                        F.col("session_id"),
                    )
                ) / 7.0
            ).alias("session_frequency_7d"),

            # Purchase count (last 30 days).
            F.count(
                F.when(
                    (F.col("event_type") == "purchase")
                    & (F.datediff(now_date, F.to_date("timestamp")) <= 30),
                    F.lit(1),
                )
            ).alias("purchase_count_30d"),

            # Friend count proxy: count social events.
            F.count(
                F.when(F.col("event_type") == "social", F.lit(1))
            ).alias("friend_count"),

            # Skill rating proxy: max level * 15 (Elo approximation).
            (F.max("level") * 15.0).alias("skill_rating"),

            # Latest device type for segment inference.
            F.last("device_type").alias("device_type"),

            # Average session duration for engagement scoring.
            F.avg(
                F.when(F.col("session_duration_sec").isNotNull(), F.col("session_duration_sec"))
            ).alias("avg_session_sec"),
        )
    )

    # --- Segment assignment ---
    # Heuristic: based on purchase frequency and session engagement.
    segment = (
        F.when(F.col("purchase_count_30d") >= 10, F.lit("Whale"))
        .when(F.col("purchase_count_30d") >= 3, F.lit("Dolphin"))
        .when(F.col("purchase_count_30d") >= 1, F.lit("Minnow"))
        .otherwise(F.lit("Free-to-Play"))
    )

    player_agg = player_agg.withColumn("segment", segment)

    # --- Churn scoring ---
    try:
        scored_df = _score_churn_mlflow(spark, player_agg)
    except Exception as exc:
        logger.warning(
            "Churn model not available (%s). Falling back to rule-based scoring.", exc
        )
        scored_df = _score_churn_rule_based(player_agg)

    # --- Churn risk level ---
    churn_level = (
        F.when(F.col("churn_risk_score") >= 0.8, F.lit("Critical"))
        .when(F.col("churn_risk_score") >= 0.5, F.lit("High"))
        .when(F.col("churn_risk_score") >= 0.3, F.lit("Medium"))
        .otherwise(F.lit("Low"))
    )

    scored_df = scored_df.withColumn("churn_risk_level", churn_level)

    # --- LTV estimation ---
    # Base LTV from avg session value, scaled by segment multiplier.
    base_ltv = F.lit(47.80)  # from config: ltv_avg
    ltv_multiplier = (
        F.when(F.col("segment") == "Whale", F.lit(8.0))
        .when(F.col("segment") == "Dolphin", F.lit(3.0))
        .when(F.col("segment") == "Minnow", F.lit(1.0))
        .otherwise(F.lit(0.1))
    )
    # Discount LTV by churn risk.
    ltv = F.round(base_ltv * ltv_multiplier * (1.0 - F.col("churn_risk_score")), 2)

    scored_df = scored_df.withColumn("ltv", ltv)

    # --- Select final columns ---
    result_df = scored_df.select(
        "player_id",
        "game_title",
        "region",
        "segment",
        F.col("days_since_last_login").cast("int"),
        F.round("session_frequency_7d", 2).alias("session_frequency_7d"),
        F.col("purchase_count_30d").cast("int"),
        F.col("friend_count").cast("int"),
        F.round("skill_rating", 1).alias("skill_rating"),
        F.round("churn_risk_score", 4).alias("churn_risk_score"),
        "churn_risk_level",
        "ltv",
    )

    _write_silver(result_df, "player_profiles")
    return result_df


def _score_churn_mlflow(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Load the production churn model from MLflow and score each player.
    Uses applyInPandas for vectorised scoring.
    """
    import mlflow
    import numpy as np
    import pandas as pd

    model = mlflow.pyfunc.load_model(CHURN_MODEL_URI)

    output_schema = df.schema.add(
        T.StructField("churn_risk_score", T.DoubleType())
    )

    def _predict(pdf: pd.DataFrame) -> pd.DataFrame:
        X = pdf[CHURN_FEATURE_COLS].fillna(0).values.astype(np.float64)
        feature_df = pd.DataFrame(X, columns=CHURN_FEATURE_COLS)
        proba = model.predict(feature_df)

        if isinstance(proba, pd.DataFrame):
            pdf["churn_risk_score"] = proba.iloc[:, 0].values
        else:
            pdf["churn_risk_score"] = np.asarray(proba, dtype=np.float64)

        return pdf

    return df.groupby("game_title").applyInPandas(_predict, schema=output_schema)


def _score_churn_rule_based(df: DataFrame) -> DataFrame:
    """
    Deterministic rule-based churn scorer used when the ML model is
    unavailable.

    Risk factors:
      - days_since_last_login: high weight (recency is the strongest signal)
      - session_frequency_7d: inverse relationship
      - purchase_count_30d: inverse relationship (spenders churn less)
      - friend_count: inverse relationship (social ties reduce churn)
    """
    # Normalise each factor to 0-1 range.
    recency_score = F.least(
        F.col("days_since_last_login").cast("double") / 30.0,
        F.lit(1.0),
    )
    frequency_score = F.greatest(
        1.0 - F.least(F.col("session_frequency_7d") / 3.0, F.lit(1.0)),
        F.lit(0.0),
    )
    spend_score = F.greatest(
        1.0 - F.least(F.col("purchase_count_30d").cast("double") / 5.0, F.lit(1.0)),
        F.lit(0.0),
    )
    social_score = F.greatest(
        1.0 - F.least(F.col("friend_count").cast("double") / 20.0, F.lit(1.0)),
        F.lit(0.0),
    )

    # Weighted combination.
    churn_risk = F.round(
        recency_score * 0.40
        + frequency_score * 0.25
        + spend_score * 0.20
        + social_score * 0.15,
        4,
    )

    return df.withColumn("churn_risk_score", churn_risk)


# =========================================================================
# 2.  raw_transactions  ->  economy_metrics
# =========================================================================
def transform_economy_metrics(spark: SparkSession) -> DataFrame:
    """
    Aggregate raw transactions into economy health metrics per
    (game_title, item_category) with:
      - transaction_volume
      - avg_price
      - price_change_pct (vs prior hour)
      - inflation_index (cumulative)
      - suspicious_flag (anomalous transaction patterns)
    """
    logger.info(
        "Transforming: %s.raw_transactions -> %s.economy_metrics", BRONZE, SILVER
    )

    txn_df = spark.table(f"{BRONZE}.raw_transactions")

    # Map item names to categories.
    item_category = (
        F.when(F.col("item_name").isin(
            "Plasma Rifle", "Shadow Blade", "Turbo Cannon", "Void Staff", "Frost Bow"
        ), F.lit("Weapon"))
        .when(F.col("item_name").isin(
            "Titan Shield", "Phantom Cloak", "Speed Suit", "Dragon Mail", "Energy Barrier"
        ), F.lit("Armor"))
        .when(F.col("item_name").isin(
            "Health Potion", "Mana Crystal", "Speed Boost", "XP Scroll", "Shield Flask"
        ), F.lit("Consumable"))
        .when(F.col("item_name").isin(
            "Neon Skin", "Victory Emote", "Star Trail", "Galaxy Mount", "Holo Badge"
        ), F.lit("Cosmetic"))
        .when(F.col("item_name").isin(
            "Gold Pack 100", "Gold Pack 500", "Gold Pack 1000", "Gem Bundle", "Season Pass"
        ), F.lit("Currency"))
        .otherwise(F.lit("Other"))
    )

    txn_categorised = txn_df.withColumn("item_category", item_category)

    # Truncate timestamp to hourly windows.
    txn_windowed = txn_categorised.withColumn(
        "window_ts", F.date_trunc("hour", F.col("timestamp"))
    )

    # Aggregate by (window_ts, game_title, item_category).
    agg_df = (
        txn_windowed
        .groupBy("window_ts", "game_title", "item_category")
        .agg(
            F.count("*").alias("transaction_volume"),
            F.round(F.avg("amount_usd"), 4).alias("avg_price"),
            # Suspicious flag: more than 3 transactions > $500 in one hour-category.
            F.sum(
                F.when(F.col("amount_usd") > 500.0, F.lit(1)).otherwise(F.lit(0))
            ).alias("high_value_count"),
        )
    )

    # Compute price change vs prior window using lag.
    price_window = Window.partitionBy("game_title", "item_category").orderBy("window_ts")

    prior_price = F.lag("avg_price", 1).over(price_window)
    price_change_pct = F.round(
        F.when(
            prior_price.isNotNull() & (prior_price > 0),
            ((F.col("avg_price") - prior_price) / prior_price) * 100.0,
        ).otherwise(F.lit(0.0)),
        4,
    )

    # Cumulative inflation index.
    # Start at INFLATION_BASELINE; compound hourly price changes.
    cumulative_change = F.sum(
        F.when(
            prior_price.isNotNull() & (prior_price > 0),
            (F.col("avg_price") - prior_price) / prior_price,
        ).otherwise(F.lit(0.0))
    ).over(price_window.rowsBetween(Window.unboundedPreceding, Window.currentRow))

    inflation_index = F.round(F.lit(INFLATION_BASELINE) + cumulative_change, 4)

    # Suspicious flag.
    suspicious_flag = F.col("high_value_count") >= 3

    result_df = (
        agg_df
        .withColumn("price_change_pct", price_change_pct)
        .withColumn("inflation_index", inflation_index)
        .withColumn("suspicious_flag", suspicious_flag)
        .select(
            F.col("window_ts").alias("timestamp"),
            "game_title",
            "item_category",
            "transaction_volume",
            "avg_price",
            "price_change_pct",
            "inflation_index",
            "suspicious_flag",
        )
    )

    _write_silver(result_df, "economy_metrics")
    return result_df


# =========================================================================
# 3.  raw_match_events  ->  match_quality
# =========================================================================
def transform_match_quality(spark: SparkSession) -> DataFrame:
    """
    Enrich raw match events with computed fairness scores and
    completion rate estimates.

    Fairness score (0-1) is derived from:
      - Skill spread: lower spread = fairer (weight 0.6)
      - Queue time: moderate queues suggest good matching (weight 0.2)
      - Player count: fuller lobbies = better experience (weight 0.2)

    Completion rate is estimated from match duration relative to the
    expected duration for the game mode.
    """
    logger.info(
        "Transforming: %s.raw_match_events -> %s.match_quality", BRONZE, SILVER
    )

    matches_df = spark.table(f"{BRONZE}.raw_match_events")

    # --- Fairness score ---
    # Skill spread component: inversely proportional to spread.
    # Ideal spread is ~50 rating points; penalise linearly beyond that.
    skill_component = F.greatest(
        1.0 - (F.col("skill_spread") / (IDEAL_SKILL_SPREAD * 10.0)),
        F.lit(0.0),
    )

    # Queue time component: short queues may mean poor matching;
    # moderate queues (5-20s) are ideal; long queues degrade experience.
    queue_component = (
        F.when(F.col("queue_time_sec") < 3.0, F.lit(0.5))  # too fast
        .when(
            F.col("queue_time_sec") <= 20.0,
            F.lit(1.0),  # sweet spot
        )
        .otherwise(
            F.greatest(
                1.0 - ((F.col("queue_time_sec") - 20.0) / MAX_QUEUE_PENALTY_SEC),
                F.lit(0.0),
            )
        )
    )

    # Player count component: closer to full lobby is better.
    # Normalise against a typical max of 100 players.
    player_component = F.least(F.col("player_count").cast("double") / 100.0, F.lit(1.0))

    fairness_score = F.round(
        skill_component * 0.6 + queue_component * 0.2 + player_component * 0.2,
        4,
    )

    # --- Completion rate ---
    # Expected durations by mode (seconds).
    expected_duration = (
        F.when(F.col("game_mode") == "Ranked", F.lit(1800))
        .when(F.col("game_mode") == "Casual", F.lit(900))
        .when(F.col("game_mode") == "Tournament", F.lit(2700))
        .otherwise(F.lit(1200))  # Custom
    )

    # Completion rate: matches that lasted >= 50% of expected duration
    # are considered "completed". Use a soft sigmoid-like mapping.
    duration_ratio = F.col("match_duration_sec").cast("double") / expected_duration
    completion_rate = F.round(
        F.least(duration_ratio, F.lit(1.0)),
        4,
    )

    result_df = (
        matches_df
        .withColumn("fairness_score", fairness_score)
        .withColumn("completion_rate", completion_rate)
        .select(
            "match_id",
            "timestamp",
            "game_title",
            "region",
            "game_mode",
            "skill_spread",
            "queue_time_sec",
            "fairness_score",
            "completion_rate",
            "reported_unfair",
        )
    )

    _write_silver(result_df, "match_quality")
    return result_df


# =========================================================================
# Shared writer
# =========================================================================
def _write_silver(df: DataFrame, table_name: str) -> None:
    """Overwrite the silver table with the latest transform output."""
    full_name = f"{SILVER}.{table_name}"
    logger.info("Writing silver table %s ...", full_name)

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(full_name)

    row_count = df.count()
    logger.info("Silver table %s written — %d rows.", full_name, row_count)


# =========================================================================
# Orchestrator
# =========================================================================
def run_all(spark: SparkSession) -> None:
    """Run all silver transforms in dependency order."""
    logger.info("=== GamingIQ Silver transforms started ===")

    transform_player_profiles(spark)
    transform_economy_metrics(spark)
    transform_match_quality(spark)

    logger.info("=== GamingIQ Silver transforms finished ===")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    spark = (
        SparkSession.builder.appName("GamingIQ_SilverTransform")
        .getOrCreate()
    )

    try:
        run_all(spark)
    except Exception:
        logger.exception("GamingIQ silver transform failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
