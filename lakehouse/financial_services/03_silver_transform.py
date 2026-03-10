"""
FinancialServicesIQ — Silver Layer Transformations
====================================================
Transforms bronze tables into enriched/scored silver tables:

  bronze.raw_transactions  ->  silver.fraud_scores
  bronze.raw_accounts      ->  silver.credit_risk_profiles
  bronze.raw_market_data   ->  silver.portfolio_positions

Each transform applies feature engineering, rule-based scoring,
and business logic to prepare data for the gold aggregation layer.

Usage (Databricks notebook):
    spark = SparkSession.builder.getOrCreate()
    run_all_silver(spark)
"""

from __future__ import annotations

import math
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, BooleanType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "financial_services_iq"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Fraud scoring thresholds
FRAUD_SCORE_THRESHOLD = 0.70       # above this -> is_fraud = True
BLOCK_SCORE_THRESHOLD = 0.85       # above this -> blocked = True

# Credit risk tier boundaries (credit score)
TIER_PRIME_MIN = 720
TIER_NEAR_PRIME_MIN = 660
TIER_SUBPRIME_MIN = 580
# Below TIER_SUBPRIME_MIN -> Deep-Subprime

# Fraud type classification rules (score-weighted random assignment is
# replaced with a deterministic mapping based on channel + merchant)
FRAUD_TYPE_MAP = {
    "Mobile App":     "card_not_present",
    "Web Banking":    "card_not_present",
    "ATM":            "counterfeit",
    "Branch":         "first_party",
    "Wire Transfer":  "account_takeover",
    "ACH":            "synthetic_identity",
}


# ===================================================================
# 1. raw_transactions -> fraud_scores
# ===================================================================
def transform_fraud_scores(spark: SparkSession, write: bool = True) -> DataFrame:
    """
    Enrich raw transactions with behavioural features and a rule-based
    fraud score, then classify and decide blocking.

    Engineered features
    -------------------
    - velocity_1h      : count of txns by the same customer in a 1-hour window
    - distance_from_home : haversine km between txn location and the customer's
                           median location (proxy for home)
    - time_since_last_txn : seconds since the customer's previous transaction

    Fraud score (rule-based, 0-1)
    ----------------------------
    Weighted sum of normalised signals:
        0.25 * device_risk_score
      + 0.20 * normalised(amount)           [capped at 1]
      + 0.20 * normalised(velocity_1h)      [> 5 in 1 h -> 1.0]
      + 0.15 * normalised(distance_from_home) [> 500 km -> 1.0]
      + 0.10 * (1 - normalised(time_since_last_txn)) [< 30 s -> 1.0]
      + 0.10 * channel_risk_factor
    """

    raw = spark.read.table(f"{BRONZE}.raw_transactions")

    # ----- Customer home location (median lat/lon as proxy) -----
    home = (
        raw
        .groupBy("customer_id")
        .agg(
            F.percentile_approx("location_lat", 0.5).alias("home_lat"),
            F.percentile_approx("location_lon", 0.5).alias("home_lon"),
        )
    )

    txn = raw.join(home, on="customer_id", how="left")

    # ----- Haversine distance (km) -----
    txn = txn.withColumn(
        "distance_from_home",
        F.when(
            F.col("home_lat").isNotNull() & F.col("location_lat").isNotNull(),
            # Simplified spherical law of cosines (accurate enough for scoring)
            F.lit(6371.0) * F.acos(
                F.least(F.lit(1.0), F.greatest(F.lit(-1.0),
                    F.sin(F.radians(F.col("location_lat"))) * F.sin(F.radians(F.col("home_lat")))
                    + F.cos(F.radians(F.col("location_lat"))) * F.cos(F.radians(F.col("home_lat")))
                      * F.cos(F.radians(F.col("location_lon") - F.col("home_lon")))
                ))
            )
        ).otherwise(F.lit(0.0))
    )

    # ----- Velocity in 1-hour window -----
    w_1h = (
        Window
        .partitionBy("customer_id")
        .orderBy(F.col("timestamp").cast("long"))
        .rangeBetween(-3600, 0)
    )
    txn = txn.withColumn("velocity_1h", F.count("transaction_id").over(w_1h))

    # ----- Time since last transaction (seconds) -----
    w_prev = (
        Window
        .partitionBy("customer_id")
        .orderBy("timestamp")
    )
    txn = txn.withColumn(
        "time_since_last_txn",
        (F.col("timestamp").cast("long") - F.lag("timestamp").over(w_prev).cast("long"))
    ).fillna({"time_since_last_txn": 86400})  # default 24 h for first txn

    # ----- Channel risk factor -----
    channel_risk = {
        "Mobile App": 0.3,
        "Web Banking": 0.35,
        "ATM": 0.5,
        "Branch": 0.1,
        "Wire Transfer": 0.7,
        "ACH": 0.4,
    }
    channel_risk_expr = F.lit(0.3)  # default
    for ch, risk in channel_risk.items():
        channel_risk_expr = F.when(F.col("channel") == ch, F.lit(risk)).otherwise(channel_risk_expr)

    txn = txn.withColumn("_channel_risk", channel_risk_expr)

    # ----- Normalised components -----
    txn = (
        txn
        .withColumn("_norm_amount", F.least(F.col("amount") / 10000.0, F.lit(1.0)))
        .withColumn("_norm_velocity", F.least(F.col("velocity_1h") / 5.0, F.lit(1.0)))
        .withColumn("_norm_distance", F.least(F.col("distance_from_home") / 500.0, F.lit(1.0)))
        .withColumn(
            "_norm_time_inv",
            F.greatest(F.lit(0.0), F.lit(1.0) - F.col("time_since_last_txn") / 300.0)
        )
    )

    # ----- Composite fraud score -----
    txn = txn.withColumn(
        "fraud_score",
        F.round(
            F.lit(0.25) * F.coalesce(F.col("device_risk_score"), F.lit(0.0))
            + F.lit(0.20) * F.col("_norm_amount")
            + F.lit(0.20) * F.col("_norm_velocity")
            + F.lit(0.15) * F.col("_norm_distance")
            + F.lit(0.10) * F.col("_norm_time_inv")
            + F.lit(0.10) * F.col("_channel_risk"),
            4
        )
    )

    # ----- Classification -----
    txn = txn.withColumn("is_fraud", F.col("fraud_score") >= FRAUD_SCORE_THRESHOLD)
    txn = txn.withColumn("blocked", F.col("fraud_score") >= BLOCK_SCORE_THRESHOLD)

    # ----- Fraud type (only when is_fraud) -----
    fraud_type_expr = F.lit(None).cast(StringType())
    for ch, ft in FRAUD_TYPE_MAP.items():
        fraud_type_expr = F.when(
            (F.col("is_fraud") == True) & (F.col("channel") == ch), F.lit(ft)
        ).otherwise(fraud_type_expr)

    txn = txn.withColumn("fraud_type", fraud_type_expr)

    # ----- Select final columns -----
    result = txn.select(
        "transaction_id",
        "timestamp",
        "account_id",
        "customer_id",
        "channel",
        "merchant_category",
        "amount",
        F.col("velocity_1h").cast(IntegerType()).alias("velocity_1h"),
        F.round("distance_from_home", 2).alias("distance_from_home"),
        F.col("time_since_last_txn").cast(IntegerType()).alias("time_since_last_txn"),
        "fraud_score",
        "is_fraud",
        "fraud_type",
        "blocked",
    )

    if write:
        (
            result.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{SILVER}.fraud_scores")
        )
        print(f"[silver] fraud_scores written -> {SILVER}.fraud_scores")

    return result


# ===================================================================
# 2. raw_accounts -> credit_risk_profiles
# ===================================================================
def transform_credit_risk_profiles(spark: SparkSession, write: bool = True) -> DataFrame:
    """
    Compute credit risk metrics for each account.

    Default probability model (logistic approximation)
    ---------------------------------------------------
    logit = -8.0
         + 0.015 * (850 - credit_score)
         + 2.5   * dti_ratio
         + 1.8   * utilization_ratio
         - 0.08  * employment_years
         + 1.2   * delinquency_flag
    P(default) = 1 / (1 + exp(-logit))

    Risk tiers
    ----------
    Prime          : credit_score >= 720
    Near-Prime     : 660 <= credit_score < 720
    Subprime       : 580 <= credit_score < 660
    Deep-Subprime  : credit_score < 580

    Expected loss = balance * P(default) * LGD  (LGD = 0.45)
    """

    LGD = 0.45  # Loss Given Default

    raw = spark.read.table(f"{BRONZE}.raw_accounts")

    # Delinquency flag: any 'D' or 'L' in payment_history_12m
    accts = raw.withColumn(
        "delinquency_flag",
        F.col("payment_history_12m").rlike("[DL]")
    )

    # Default probability via logistic function
    accts = accts.withColumn(
        "_logit",
        F.lit(-8.0)
        + F.lit(0.015) * (F.lit(850) - F.coalesce(F.col("credit_score"), F.lit(714)))
        + F.lit(2.5)   * F.coalesce(F.col("dti_ratio"), F.lit(0.35))
        + F.lit(1.8)   * F.coalesce(F.col("utilization_ratio"), F.lit(0.30))
        - F.lit(0.08)  * F.coalesce(F.col("employment_years"), F.lit(5))
        + F.lit(1.2)   * F.col("delinquency_flag").cast(IntegerType())
    )

    accts = accts.withColumn(
        "default_probability",
        F.round(F.lit(1.0) / (F.lit(1.0) + F.exp(-F.col("_logit"))), 6)
    )

    # Risk tier
    accts = accts.withColumn(
        "risk_tier",
        F.when(F.col("credit_score") >= TIER_PRIME_MIN, "Prime")
         .when(F.col("credit_score") >= TIER_NEAR_PRIME_MIN, "Near-Prime")
         .when(F.col("credit_score") >= TIER_SUBPRIME_MIN, "Subprime")
         .otherwise("Deep-Subprime")
    )

    # Expected loss
    accts = accts.withColumn(
        "expected_loss",
        F.round(F.coalesce(F.col("balance"), F.lit(0.0)) * F.col("default_probability") * F.lit(LGD), 2)
    )

    result = accts.select(
        "account_id",
        "customer_id",
        "business_line",
        "credit_score",
        "dti_ratio",
        "utilization_ratio",
        "payment_history_12m",
        "delinquency_flag",
        "default_probability",
        "risk_tier",
        "expected_loss",
    )

    if write:
        (
            result.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{SILVER}.credit_risk_profiles")
        )
        print(f"[silver] credit_risk_profiles written -> {SILVER}.credit_risk_profiles")

    return result


# ===================================================================
# 3. raw_market_data -> portfolio_positions
# ===================================================================
def transform_portfolio_positions(spark: SparkSession, write: bool = True) -> DataFrame:
    """
    Derive per-position risk analytics from raw market data.

    Features
    --------
    - market_value     : price * implied_quantity (computed to match AUM target)
    - daily_pnl        : market_value * daily_return
    - var_contribution : parametric VaR = market_value * volatility_30d * z_95 / sqrt(252)
                         z_95 = 1.6449
    - beta             : covariance proxy based on daily return vs. market mean
    - sharpe_contribution : (daily_return * 252 - risk_free) / (volatility_30d * sqrt(252))
                            risk_free = 0.045 (4.5 % annual)
    """

    Z_95 = 1.6449
    SQRT_252 = math.sqrt(252)
    RISK_FREE_ANNUAL = 0.045
    AUM_TARGET = 24_500_000_000.0
    POSITIONS_TARGET = 8_450

    raw = spark.read.table(f"{BRONZE}.raw_market_data")

    # Implied market value: distribute AUM across positions proportional to price
    total_price = raw.agg(F.sum("price")).collect()[0][0] or 1.0
    avg_mv_per_unit = AUM_TARGET / total_price

    mkt = raw.withColumn(
        "market_value",
        F.round(F.col("price") * F.lit(avg_mv_per_unit), 2)
    )

    # Daily P&L
    mkt = mkt.withColumn(
        "daily_pnl",
        F.round(F.col("market_value") * F.coalesce(F.col("daily_return"), F.lit(0.0)), 2)
    )

    # VaR contribution (parametric, individual position)
    mkt = mkt.withColumn(
        "var_contribution",
        F.round(
            F.col("market_value")
            * F.coalesce(F.col("volatility_30d"), F.lit(0.15))
            * F.lit(Z_95 / SQRT_252),
            2
        )
    )

    # Beta proxy: daily_return / market average daily return
    # (simplified: use 0.0005 as market daily return benchmark)
    MARKET_DAILY_RETURN = 0.0005
    mkt = mkt.withColumn(
        "beta",
        F.round(
            F.coalesce(F.col("daily_return"), F.lit(0.0)) / F.lit(MARKET_DAILY_RETURN),
            4
        )
    )

    # Sharpe contribution per position (annualised)
    mkt = mkt.withColumn(
        "sharpe_contribution",
        F.round(
            (F.coalesce(F.col("daily_return"), F.lit(0.0)) * F.lit(252) - F.lit(RISK_FREE_ANNUAL))
            / (F.coalesce(F.col("volatility_30d"), F.lit(0.15)) * F.lit(SQRT_252)),
            4
        )
    )

    # Position ID (use record_id as position_id)
    result = mkt.select(
        F.col("record_id").alias("position_id"),
        "timestamp",
        "asset_class",
        "ticker",
        "region",
        "market_value",
        "daily_pnl",
        "var_contribution",
        "beta",
        "sharpe_contribution",
    )

    if write:
        (
            result.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{SILVER}.portfolio_positions")
        )
        print(f"[silver] portfolio_positions written -> {SILVER}.portfolio_positions")

    return result


# ===================================================================
# Orchestrator
# ===================================================================
def run_all_silver(spark: SparkSession) -> dict[str, DataFrame]:
    """Run all silver transformations sequentially."""
    print("=" * 72)
    print("FinancialServicesIQ — Silver Layer Transformations")
    print("=" * 72)

    results: dict[str, DataFrame] = {}

    print("\n[1/3] raw_transactions -> fraud_scores ...")
    results["fraud_scores"] = transform_fraud_scores(spark)

    print("\n[2/3] raw_accounts -> credit_risk_profiles ...")
    results["credit_risk_profiles"] = transform_credit_risk_profiles(spark)

    print("\n[3/3] raw_market_data -> portfolio_positions ...")
    results["portfolio_positions"] = transform_portfolio_positions(spark)

    print("\n" + "=" * 72)
    print("Silver transformations complete.")
    print("=" * 72)

    return results


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FinancialServicesIQ_Silver") \
        .getOrCreate()
    run_all_silver(spark)
