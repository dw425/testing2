"""
RiskIQ — Gold Layer Aggregation
=================================
Reads silver Delta tables and produces dashboard-ready gold tables:

  access_anomalies + pii_exposure_map  ->  risk_summary
  compliance_status                    ->  compliance_scores
  access_anomalies + pii_exposure_map  ->  active_alerts
  access_anomalies                     ->  user_risk_profiles

Usage:
    spark-submit lakehouse/risk/04_gold_aggregate.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
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
logger = logging.getLogger("risk_gold_aggregate")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "risk_iq"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Risk thresholds
HIGH_SEVERITY_LEVELS = ["Critical", "High"]
ALERT_SEVERITY_THRESHOLD = "High"

# Financial risk estimation multipliers (per alert type)
FINANCIAL_RISK_PER_ACCESS_ANOMALY = 0.05    # $50k per anomalous access
FINANCIAL_RISK_PER_PII_EXPOSURE = 0.15      # $150k per PII exposure
FINANCIAL_RISK_PER_COMPLIANCE_FAIL = 0.25   # $250k per compliance failure

# User risk score weights
WEIGHT_ANOMALY_COUNT = 0.4
WEIGHT_MAX_CONFIDENCE = 0.3
WEIGHT_HIGH_SENSITIVITY = 0.3


# =========================================================================
# 1.  risk_summary
# =========================================================================
def aggregate_risk_summary(spark: SparkSession) -> DataFrame:
    """
    Build a point-in-time risk KPI snapshot.  One row is appended per
    invocation.

    Metrics computed:
      - total_financial_risk:   estimated financial exposure in millions,
                                derived from anomaly counts and PII exposures
      - compliance_risk_score:  0-100 score based on open violations and
                                framework statuses
      - active_alerts:          count of active high-severity alerts
      - risk_trend:             Increasing / Stable / Decreasing based on
                                comparison with the previous snapshot
    """
    logger.info("Aggregating: silver tables -> %s.risk_summary", GOLD)

    # --- Count high-severity access anomalies ---
    anomalies_df = spark.table(f"{SILVER}.access_anomalies")
    anomalous_count = anomalies_df.filter(F.col("is_anomalous") == True).count()
    high_risk_anomalies = anomalies_df.filter(
        (F.col("is_anomalous") == True)
        & (F.col("risk_level").isin(*HIGH_SEVERITY_LEVELS))
    ).count()

    # --- Count high-severity PII exposures ---
    pii_df = spark.table(f"{SILVER}.pii_exposure_map")
    critical_pii = pii_df.filter(
        F.col("exposure_risk").isin("Critical", "High")
    ).count()
    open_pii = pii_df.filter(F.col("remediation_status") == "Open").count()

    # --- Compliance violations ---
    compliance_df = spark.table(f"{SILVER}.compliance_status")
    total_violations = compliance_df.filter(F.col("status") == "Fail").count()
    total_controls = compliance_df.count()
    compliance_pass_rate = (
        (total_controls - total_violations) / max(total_controls, 1)
    )

    # --- Financial risk estimation (in millions) ---
    financial_risk = round(
        (high_risk_anomalies * FINANCIAL_RISK_PER_ACCESS_ANOMALY)
        + (critical_pii * FINANCIAL_RISK_PER_PII_EXPOSURE)
        + (total_violations * FINANCIAL_RISK_PER_COMPLIANCE_FAIL),
        2,
    )

    # --- Compliance risk score (0-100, higher = more risk) ---
    compliance_risk_score = int(round((1 - compliance_pass_rate) * 100))

    # --- Active alerts count ---
    active_alerts_count = high_risk_anomalies + critical_pii

    # --- Risk trend: compare with previous snapshot ---
    risk_trend = _compute_risk_trend(spark, financial_risk)

    snapshot = spark.createDataFrame(
        [
            (
                datetime.utcnow(),
                float(financial_risk),
                compliance_risk_score,
                int(active_alerts_count),
                risk_trend,
            )
        ],
        schema=T.StructType([
            T.StructField("snapshot_time", T.TimestampType()),
            T.StructField("total_financial_risk", T.DoubleType()),
            T.StructField("compliance_risk_score", T.IntegerType()),
            T.StructField("active_alerts", T.IntegerType()),
            T.StructField("risk_trend", T.StringType()),
        ]),
    )

    _write_gold(snapshot, "risk_summary", mode="append")
    return snapshot


def _compute_risk_trend(spark: SparkSession, current_risk: float) -> str:
    """Compare current financial risk with the previous snapshot to determine trend."""
    try:
        prev = (
            spark.table(f"{GOLD}.risk_summary")
            .orderBy(F.col("snapshot_time").desc())
            .select("total_financial_risk")
            .limit(1)
            .collect()
        )
        if prev and prev[0]["total_financial_risk"] is not None:
            prev_risk = float(prev[0]["total_financial_risk"])
            delta = current_risk - prev_risk
            if delta > 0.5:
                return "Increasing"
            elif delta < -0.5:
                return "Decreasing"
            return "Stable"
    except Exception:
        pass
    return "Stable"


# =========================================================================
# 2.  compliance_scores
# =========================================================================
def aggregate_compliance_scores(spark: SparkSession) -> DataFrame:
    """
    Compute per-framework compliance scores from silver.compliance_status.

    For each framework:
      - status:          overall framework status (Compliant / Needs Review / At Risk)
      - last_audit_date: timestamp of the most recent scan
      - open_violations: total open violations
      - compliance_pct:  percentage of controls passing
      - risk_level:      Critical / High / Medium / Low based on compliance_pct
    """
    logger.info(
        "Aggregating: %s.compliance_status -> %s.compliance_scores", SILVER, GOLD
    )

    compliance_df = spark.table(f"{SILVER}.compliance_status")

    # Aggregate per framework
    framework_agg = compliance_df.groupBy("framework").agg(
        F.max("timestamp").alias("last_audit_date"),
        F.sum(
            F.when(F.col("status") == "Fail", F.lit(1)).otherwise(F.lit(0))
        ).cast("int").alias("open_violations"),
        F.count("*").alias("total_controls"),
        F.sum(
            F.when(F.col("status") == "Pass", F.lit(1)).otherwise(F.lit(0))
        ).alias("passing_controls"),
    )

    # Compute compliance percentage
    with_pct = framework_agg.withColumn(
        "compliance_pct",
        F.round(
            F.col("passing_controls") / F.col("total_controls") * 100, 2
        ),
    )

    # Determine overall status
    status_expr = (
        F.when(F.col("compliance_pct") >= 95, F.lit("Compliant"))
        .when(F.col("compliance_pct") >= 80, F.lit("Needs Review"))
        .otherwise(F.lit("At Risk"))
    )

    # Risk level
    risk_level_expr = (
        F.when(F.col("compliance_pct") < 70, F.lit("Critical"))
        .when(F.col("compliance_pct") < 80, F.lit("High"))
        .when(F.col("compliance_pct") < 90, F.lit("Medium"))
        .otherwise(F.lit("Low"))
    )

    result_df = with_pct.select(
        "framework",
        status_expr.alias("status"),
        "last_audit_date",
        "open_violations",
        "compliance_pct",
        risk_level_expr.alias("risk_level"),
        F.current_timestamp().alias("last_updated"),
    )

    _write_gold(result_df, "compliance_scores", mode="overwrite")
    return result_df


# =========================================================================
# 3.  active_alerts
# =========================================================================
def aggregate_active_alerts(spark: SparkSession) -> DataFrame:
    """
    Generate active alerts from high-severity findings in:
      - silver.access_anomalies (anomalous accesses with Critical/High risk)
      - silver.pii_exposure_map (Critical/High exposure risk, Open remediation)

    Each alert includes:
      - alert_type: Access Anomaly / PII Exposure
      - severity: derived from the source risk level
      - description: human-readable summary
      - affected_users: count of impacted users (from access anomalies)
    """
    logger.info(
        "Aggregating: silver.access_anomalies + silver.pii_exposure_map -> %s.active_alerts",
        GOLD,
    )

    # --- Access anomaly alerts ---
    anomalies_df = spark.table(f"{SILVER}.access_anomalies")

    # Group anomalous accesses by (asset_accessed, risk_level) to create alerts
    access_alerts = (
        anomalies_df
        .filter(
            (F.col("is_anomalous") == True)
            & (F.col("risk_level").isin(*HIGH_SEVERITY_LEVELS))
        )
        .groupBy("asset_accessed", "risk_level")
        .agg(
            F.max("timestamp").alias("timestamp"),
            F.countDistinct("user_id").cast("int").alias("affected_users"),
            F.count("*").alias("event_count"),
        )
    )

    access_alert_rows = access_alerts.select(
        F.expr("uuid()").alias("alert_id"),
        F.col("timestamp"),
        F.lit("Access Anomaly").alias("alert_type"),
        F.col("risk_level").alias("severity"),
        F.concat(
            F.lit("Anomalous access detected on "),
            F.col("asset_accessed"),
            F.lit(": "),
            F.col("event_count").cast("string"),
            F.lit(" events from "),
            F.col("affected_users").cast("string"),
            F.lit(" unique users"),
        ).alias("description"),
        F.lit("silver.access_anomalies").alias("source_table"),
        F.col("affected_users"),
        F.lit("Open").alias("status"),
    )

    # --- PII exposure alerts ---
    pii_df = spark.table(f"{SILVER}.pii_exposure_map")

    pii_alerts = (
        pii_df
        .filter(
            (F.col("exposure_risk").isin("Critical", "High"))
            & (F.col("remediation_status") == "Open")
        )
        .groupBy("table_name", "pii_type", "exposure_risk")
        .agg(
            F.max("timestamp").alias("timestamp"),
            F.sum("record_count").cast("int").alias("total_records"),
            F.count("*").alias("detection_count"),
        )
    )

    pii_alert_rows = pii_alerts.select(
        F.expr("uuid()").alias("alert_id"),
        F.col("timestamp"),
        F.lit("PII Exposure").alias("alert_type"),
        F.when(F.col("exposure_risk") == "Critical", F.lit("Critical"))
        .otherwise(F.lit("High"))
        .alias("severity"),
        F.concat(
            F.lit("Unmasked "),
            F.col("pii_type"),
            F.lit(" detected in "),
            F.col("table_name"),
            F.lit(": "),
            F.col("total_records").cast("string"),
            F.lit(" records across "),
            F.col("detection_count").cast("string"),
            F.lit(" columns"),
        ).alias("description"),
        F.lit("silver.pii_exposure_map").alias("source_table"),
        F.lit(0).cast("int").alias("affected_users"),
        F.lit("Open").alias("status"),
    )

    # Union all alerts
    all_alerts = access_alert_rows.unionByName(pii_alert_rows)

    _write_gold(all_alerts, "active_alerts", mode="overwrite")
    return all_alerts


# =========================================================================
# 4.  user_risk_profiles
# =========================================================================
def aggregate_user_risk_profiles(spark: SparkSession) -> DataFrame:
    """
    Compute per-user risk profiles from silver.access_anomalies.

    For each user:
      - risk_score:              composite 0-100 score based on:
                                   40% anomalous access count (normalised)
                                   30% max anomaly confidence
                                   30% high-sensitivity access ratio
      - anomalous_access_count:  total anomalous events for this user
      - last_anomaly_time:       timestamp of most recent anomalous access
      - risk_level:              Critical / High / Medium / Low
    """
    logger.info(
        "Aggregating: %s.access_anomalies -> %s.user_risk_profiles", SILVER, GOLD
    )

    anomalies_df = spark.table(f"{SILVER}.access_anomalies")

    # Aggregate per user
    user_agg = anomalies_df.groupBy("user_id", "email").agg(
        F.sum(
            F.when(F.col("is_anomalous") == True, F.lit(1)).otherwise(F.lit(0))
        ).cast("int").alias("anomalous_access_count"),
        F.max(
            F.when(F.col("is_anomalous") == True, F.col("timestamp"))
        ).alias("last_anomaly_time"),
        F.max("anomaly_confidence").alias("max_confidence"),
        F.count("*").alias("total_accesses"),
        F.sum(
            F.when(F.col("data_sensitivity") == "High", F.lit(1)).otherwise(F.lit(0))
        ).alias("high_sensitivity_accesses"),
    )

    # Compute the maximum anomalous access count for normalisation
    max_anomalous = user_agg.agg(
        F.max("anomalous_access_count").alias("max_anomalous")
    ).collect()[0]["max_anomalous"]
    max_anomalous = max(max_anomalous, 1)  # avoid division by zero

    # Normalised anomaly count component (0-100 scale)
    anomaly_count_score = (
        F.col("anomalous_access_count") / F.lit(float(max_anomalous)) * 100.0
    )

    # Max confidence component (0-100 scale)
    confidence_score = F.coalesce(F.col("max_confidence"), F.lit(0.0)) * 100.0

    # High-sensitivity access ratio component (0-100 scale)
    sensitivity_ratio = (
        F.col("high_sensitivity_accesses") / F.greatest(F.col("total_accesses"), F.lit(1)) * 100.0
    )

    # Composite risk score
    composite_score = F.round(
        (anomaly_count_score * WEIGHT_ANOMALY_COUNT)
        + (confidence_score * WEIGHT_MAX_CONFIDENCE)
        + (sensitivity_ratio * WEIGHT_HIGH_SENSITIVITY),
        2,
    )

    risk_level = (
        F.when(composite_score >= 75, F.lit("Critical"))
        .when(composite_score >= 50, F.lit("High"))
        .when(composite_score >= 25, F.lit("Medium"))
        .otherwise(F.lit("Low"))
    )

    result_df = user_agg.select(
        "user_id",
        "email",
        composite_score.alias("risk_score"),
        "anomalous_access_count",
        "last_anomaly_time",
        risk_level.alias("risk_level"),
        F.current_timestamp().alias("last_updated"),
    )

    _write_gold(result_df, "user_risk_profiles", mode="overwrite")
    return result_df


# =========================================================================
# Shared writer
# =========================================================================
def _write_gold(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """Write to a gold Delta table."""
    full_name = f"{GOLD}.{table_name}"
    logger.info("Writing gold table %s (mode=%s) …", full_name, mode)

    df.write.format("delta").mode(mode).option(
        "overwriteSchema", "true"
    ).saveAsTable(full_name)

    row_count = df.count()
    logger.info("Gold table %s written — %d rows.", full_name, row_count)


# =========================================================================
# Orchestrator
# =========================================================================
def run_all(spark: SparkSession) -> None:
    """Run all gold aggregations in dependency order."""
    logger.info("=== RiskIQ Gold aggregations started ===")

    # compliance_scores first — risk_summary references compliance data
    aggregate_compliance_scores(spark)
    aggregate_risk_summary(spark)
    aggregate_active_alerts(spark)
    aggregate_user_risk_profiles(spark)

    logger.info("=== RiskIQ Gold aggregations finished ===")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    spark = (
        SparkSession.builder.appName("RiskIQ_GoldAggregate")
        .getOrCreate()
    )

    try:
        run_all(spark)
    except Exception:
        logger.exception("Gold aggregation failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
