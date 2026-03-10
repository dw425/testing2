"""
RiskIQ — Silver Layer Transforms
==================================
Reads bronze Delta tables and produces curated silver tables:

  raw_access_logs       ->  access_anomalies      (rule-based anomaly scoring)
  raw_compliance_scans  ->  compliance_status      (enrichment + trend analysis)
  raw_pii_detections    ->  pii_exposure_map       (exposure risk classification)

Usage:
    spark-submit lakehouse/risk/03_silver_transform.py
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
logger = logging.getLogger("risk_silver_transform")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "risk_iq"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Sensitivity classification for data assets
SENSITIVITY_MAP = {
    "finance.payroll": "High",
    "finance.treasury": "High",
    "finance.reporting": "Medium",
    "customer.pii": "High",
    "customer.orders": "Medium",
    "customer.support": "Low",
    "hr.benefits": "High",
    "hr.analytics": "Medium",
    "hr.recruiting": "Low",
    "healthcare.claims": "High",
    "healthcare.records": "High",
}

# Role-to-domain mapping for role_match_score computation
ROLE_DOMAIN_MAP = {
    "j_doe": ["finance.payroll", "hr.benefits"],
    "sys_pipeline_01": ["finance.reporting", "hr.analytics"],
    "a_smith": ["customer.orders", "customer.support"],
    "ml_service_account": ["hr.analytics", "finance.reporting"],
}

# Known business-hours window (UTC)
BUSINESS_HOUR_START = 7
BUSINESS_HOUR_END = 19

# Known normal geolocations for the organization
NORMAL_GEOS = ["New York, US", "San Francisco, US", "Chicago, US", "London, UK", "Berlin, DE"]

# Access frequency thresholds (per 24h window)
HIGH_FREQUENCY_THRESHOLD = 50
VERY_HIGH_FREQUENCY_THRESHOLD = 100

# PII exposure risk thresholds
PII_HIGH_CONFIDENCE_THRESHOLD = 0.9
PII_MEDIUM_CONFIDENCE_THRESHOLD = 0.75
PII_HIGH_RECORD_COUNT = 100_000


# =========================================================================
# 1.  raw_access_logs  ->  access_anomalies
# =========================================================================
def transform_access_anomalies(spark: SparkSession) -> DataFrame:
    """
    Score every access log through rule-based anomaly detection.

    Scoring rules:
      - **Unusual hours**: access outside 07:00-19:00 UTC adds 0.2 to score
      - **Geo distance**: access from unusual locations adds 0.2 to score
      - **Data sensitivity**: accessing High-sensitivity assets adds 0.15
      - **Access frequency**: high-frequency access in 24h adds 0.2
      - **Role mismatch**: accessing domains outside normal role adds 0.25

    An event is flagged as anomalous when the combined score >= 0.5.
    Risk levels: Critical (>=0.8), High (>=0.6), Medium (>=0.4), Low (<0.4).
    """
    logger.info("Transforming: %s.raw_access_logs -> %s.access_anomalies", BRONZE, SILVER)

    access_df = spark.table(f"{BRONZE}.raw_access_logs")

    # --- Access frequency: count accesses per user in the 24h window ---
    user_window = Window.partitionBy("user_id")
    access_with_freq = access_df.withColumn(
        "access_frequency",
        F.count("event_id").over(user_window).cast("int"),
    )

    # --- Data sensitivity classification ---
    # Build a mapping expression from SENSITIVITY_MAP
    sensitivity_expr = F.lit("Low")  # default
    for domain, level in SENSITIVITY_MAP.items():
        sensitivity_expr = F.when(
            F.col("asset_accessed") == domain, F.lit(level)
        ).otherwise(sensitivity_expr)

    access_with_sens = access_with_freq.withColumn(
        "data_sensitivity", sensitivity_expr
    )

    # --- Anomaly scoring components ---

    # 1. Unusual hours score
    hour_of_day = F.hour(F.col("timestamp"))
    unusual_hours_score = F.when(
        (hour_of_day < BUSINESS_HOUR_START) | (hour_of_day >= BUSINESS_HOUR_END),
        F.lit(0.2),
    ).otherwise(F.lit(0.0))

    # 2. Geo distance score — flag accesses from non-standard locations
    normal_geos_arr = F.array(*[F.lit(g) for g in NORMAL_GEOS])
    geo_score = F.when(
        ~F.array_contains(normal_geos_arr, F.col("geo_location")),
        F.lit(0.2),
    ).otherwise(F.lit(0.0))

    # 3. Data sensitivity score
    sensitivity_score = (
        F.when(F.col("data_sensitivity") == "High", F.lit(0.15))
        .when(F.col("data_sensitivity") == "Medium", F.lit(0.05))
        .otherwise(F.lit(0.0))
    )

    # 4. Access frequency score
    freq_score = (
        F.when(F.col("access_frequency") >= VERY_HIGH_FREQUENCY_THRESHOLD, F.lit(0.2))
        .when(F.col("access_frequency") >= HIGH_FREQUENCY_THRESHOLD, F.lit(0.1))
        .otherwise(F.lit(0.0))
    )

    # 5. Role match score — lower score means worse match
    # Build role match expression: check if user_id has known domains
    role_match = F.lit(0.5)  # default for unknown users
    for user_id, allowed_domains in ROLE_DOMAIN_MAP.items():
        domain_arr = F.array(*[F.lit(d) for d in allowed_domains])
        role_match = F.when(
            (F.col("user_id") == user_id) & F.array_contains(domain_arr, F.col("asset_accessed")),
            F.lit(1.0),
        ).when(
            (F.col("user_id") == user_id) & ~F.array_contains(domain_arr, F.col("asset_accessed")),
            F.lit(0.2),
        ).otherwise(role_match)

    role_mismatch_score = F.when(
        F.col("role_match_score") < 0.5, F.lit(0.25)
    ).otherwise(F.lit(0.0))

    # --- Compute composite anomaly score ---
    scored_df = access_with_sens.withColumn(
        "role_match_score", F.round(role_match, 4)
    ).withColumn(
        "_unusual_hours_score", unusual_hours_score
    ).withColumn(
        "_geo_score", geo_score
    ).withColumn(
        "_sensitivity_score", sensitivity_score
    ).withColumn(
        "_freq_score", freq_score
    ).withColumn(
        "_role_mismatch_score",
        F.when(F.col("role_match_score") < 0.5, F.lit(0.25)).otherwise(F.lit(0.0)),
    )

    composite_score = (
        F.col("_unusual_hours_score")
        + F.col("_geo_score")
        + F.col("_sensitivity_score")
        + F.col("_freq_score")
        + F.col("_role_mismatch_score")
    )

    scored_df = scored_df.withColumn(
        "anomaly_confidence", F.round(F.least(composite_score, F.lit(1.0)), 4)
    ).withColumn(
        "is_anomalous", composite_score >= 0.5
    ).withColumn(
        "risk_level",
        F.when(composite_score >= 0.8, F.lit("Critical"))
        .when(composite_score >= 0.6, F.lit("High"))
        .when(composite_score >= 0.4, F.lit("Medium"))
        .otherwise(F.lit("Low")),
    )

    result_df = scored_df.select(
        "event_id",
        "timestamp",
        "user_id",
        "email",
        "asset_accessed",
        "ip_address",
        "geo_location",
        "access_frequency",
        "data_sensitivity",
        "role_match_score",
        "is_anomalous",
        "anomaly_confidence",
        "risk_level",
    )

    _write_silver(result_df, "access_anomalies")
    return result_df


# =========================================================================
# 2.  raw_compliance_scans  ->  compliance_status
# =========================================================================
def transform_compliance_status(spark: SparkSession) -> DataFrame:
    """
    Enrich compliance scan data with:
      - open_violations: running count of Fail statuses per control
      - days_since_last_audit: days between the scan and the most recent
        audit for that framework
      - trend: Improving / Stable / Degrading based on violation trajectory

    The trend is computed by comparing the violation count in the most recent
    scan cycle to the previous cycle for each (framework, domain) pair.
    """
    logger.info(
        "Transforming: %s.raw_compliance_scans -> %s.compliance_status",
        BRONZE, SILVER,
    )

    scans_df = spark.table(f"{BRONZE}.raw_compliance_scans")

    # --- Count open violations per (framework, control_id) ---
    control_window = Window.partitionBy("framework", "control_id")
    with_violations = scans_df.withColumn(
        "open_violations",
        F.sum(
            F.when(F.col("status") == "Fail", F.lit(1)).otherwise(F.lit(0))
        ).over(control_window).cast("int"),
    )

    # --- Days since last audit per framework ---
    fw_window = Window.partitionBy("framework")
    max_ts_per_fw = F.max("timestamp").over(fw_window)
    with_audit_gap = with_violations.withColumn(
        "days_since_last_audit",
        F.datediff(F.current_date(), F.to_date(max_ts_per_fw)).cast("int"),
    )

    # --- Trend analysis ---
    # Partition scans into two halves (older vs newer) and compare fail counts
    fw_domain_window = Window.partitionBy("framework", "domain").orderBy("timestamp")
    row_num = F.row_number().over(fw_domain_window)
    total_rows = F.count("*").over(Window.partitionBy("framework", "domain"))
    midpoint = F.floor(total_rows / 2)

    with_halves = with_audit_gap.withColumn("_row_num", row_num).withColumn(
        "_midpoint", midpoint
    ).withColumn(
        "_half",
        F.when(F.col("_row_num") <= F.col("_midpoint"), F.lit("first"))
        .otherwise(F.lit("second")),
    )

    # Count failures per half per (framework, domain)
    half_window = Window.partitionBy("framework", "domain", "_half")
    with_half_fails = with_halves.withColumn(
        "_half_fails",
        F.sum(
            F.when(F.col("status") == "Fail", F.lit(1)).otherwise(F.lit(0))
        ).over(half_window),
    )

    # Compute first-half and second-half failure counts
    fw_domain_all = Window.partitionBy("framework", "domain")
    first_half_fails = F.max(
        F.when(F.col("_half") == "first", F.col("_half_fails")).otherwise(F.lit(0))
    ).over(fw_domain_all)
    second_half_fails = F.max(
        F.when(F.col("_half") == "second", F.col("_half_fails")).otherwise(F.lit(0))
    ).over(fw_domain_all)

    trend = (
        F.when(second_half_fails < first_half_fails, F.lit("Improving"))
        .when(second_half_fails > first_half_fails, F.lit("Degrading"))
        .otherwise(F.lit("Stable"))
    )

    result_df = with_half_fails.withColumn("trend", trend).select(
        "scan_id",
        "timestamp",
        "framework",
        "domain",
        "control_id",
        "control_name",
        "status",
        "severity",
        "open_violations",
        "days_since_last_audit",
        "trend",
    )

    _write_silver(result_df, "compliance_status")
    return result_df


# =========================================================================
# 3.  raw_pii_detections  ->  pii_exposure_map
# =========================================================================
def transform_pii_exposure_map(spark: SparkSession) -> DataFrame:
    """
    Classify each PII detection's exposure risk and assign a remediation
    status based on masking state and confidence level.

    Exposure risk rules:
      - **Critical**: unmasked, confidence >= 0.9, record_count >= 100k
      - **High**: unmasked, confidence >= 0.75
      - **Medium**: masked but confidence >= 0.9 (potential re-identification)
      - **Low**: masked and low confidence

    Remediation status:
      - **Remediated**: masked and low exposure risk
      - **In Progress**: masked but medium/high exposure risk
      - **Open**: unmasked and any confidence
      - **Acknowledged**: very low record counts (< 100)
    """
    logger.info(
        "Transforming: %s.raw_pii_detections -> %s.pii_exposure_map",
        BRONZE, SILVER,
    )

    pii_df = spark.table(f"{BRONZE}.raw_pii_detections")

    # --- Exposure risk classification ---
    exposure_risk = (
        F.when(
            (~F.col("masked"))
            & (F.col("confidence") >= PII_HIGH_CONFIDENCE_THRESHOLD)
            & (F.col("record_count") >= PII_HIGH_RECORD_COUNT),
            F.lit("Critical"),
        )
        .when(
            (~F.col("masked"))
            & (F.col("confidence") >= PII_MEDIUM_CONFIDENCE_THRESHOLD),
            F.lit("High"),
        )
        .when(
            (F.col("masked"))
            & (F.col("confidence") >= PII_HIGH_CONFIDENCE_THRESHOLD),
            F.lit("Medium"),
        )
        .otherwise(F.lit("Low"))
    )

    # --- Remediation status ---
    remediation_status = (
        F.when(
            (F.col("record_count") < 100),
            F.lit("Acknowledged"),
        )
        .when(
            (F.col("masked")) & (F.col("exposure_risk") == "Low"),
            F.lit("Remediated"),
        )
        .when(
            (F.col("masked")) & (F.col("exposure_risk").isin("Medium", "High")),
            F.lit("In Progress"),
        )
        .otherwise(F.lit("Open"))
    )

    result_df = (
        pii_df
        .withColumn("exposure_risk", exposure_risk)
        .withColumn("remediation_status", remediation_status)
        .select(
            "detection_id",
            "timestamp",
            "table_name",
            "column_name",
            "pii_type",
            "confidence",
            "record_count",
            "masked",
            "exposure_risk",
            "remediation_status",
        )
    )

    _write_silver(result_df, "pii_exposure_map")
    return result_df


# =========================================================================
# Shared writer
# =========================================================================
def _write_silver(df: DataFrame, table_name: str) -> None:
    """Overwrite the silver table with the latest transform output."""
    full_name = f"{SILVER}.{table_name}"
    logger.info("Writing silver table %s …", full_name)

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
    logger.info("=== RiskIQ Silver transforms started ===")

    transform_access_anomalies(spark)
    transform_compliance_status(spark)
    transform_pii_exposure_map(spark)

    logger.info("=== RiskIQ Silver transforms finished ===")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    spark = (
        SparkSession.builder.appName("RiskIQ_SilverTransform")
        .getOrCreate()
    )

    try:
        run_all(spark)
    except Exception:
        logger.exception("Silver transform failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
