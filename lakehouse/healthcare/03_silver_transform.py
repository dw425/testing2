"""
HealthcareIQ — Silver Layer Transforms
=======================================
Reads bronze Delta tables and produces curated silver tables:

  raw_admissions + raw_vitals   ->  patient_flow       (join, wait times, bed assignment)
  raw_discharges                ->  readmission_risk   (score with risk model features)
  raw_equipment_telemetry       ->  equipment_health   (health scores, failure prediction)

Usage:
    spark-submit lakehouse/healthcare/03_silver_transform.py
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
logger = logging.getLogger("healthcare_silver_transform")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "healthcare_iq"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Readmission risk model registered in MLflow
READMISSION_MODEL_NAME = "Readmission_Risk_Predictor"
READMISSION_MODEL_URI = f"models:/{READMISSION_MODEL_NAME}/Production"

# Equipment failure model registered in MLflow
EQUIPMENT_MODEL_NAME = "Equipment_Failure_Predictor"
EQUIPMENT_MODEL_URI = f"models:/{EQUIPMENT_MODEL_NAME}/Production"

# Feature columns for the readmission model
READMISSION_FEATURE_COLS = [
    "age",
    "diagnosis_code",
    "los_days",
    "prior_admissions_12m",
    "comorbidity_index",
    "insurance_type",
]

# Equipment health thresholds
EQUIP_HEALTH_CRITICAL_THRESHOLD = 30.0
EQUIP_HEALTH_WARNING_THRESHOLD = 60.0

# Risk-level thresholds
RISK_CRITICAL_THRESHOLD = 0.85
RISK_HIGH_THRESHOLD = 0.65
RISK_MEDIUM_THRESHOLD = 0.40

# Bed capacity per department (used for bed assignment simulation)
BED_CAPACITY = {
    "Emergency": 60,
    "ICU": 40,
    "Cardiology": 45,
    "Orthopedics": 35,
    "Oncology": 30,
    "Pediatrics": 25,
}

BED_PREFIXES = {
    "Emergency": "ED",
    "ICU": "ICU",
    "Cardiology": "CARD",
    "Orthopedics": "ORTH",
    "Oncology": "ONC",
    "Pediatrics": "PEDS",
}


# =========================================================================
# 1.  raw_admissions + raw_vitals  ->  patient_flow
# =========================================================================
def transform_patient_flow(spark: SparkSession) -> DataFrame:
    """
    Join admissions with the latest vitals to build an enriched patient-flow
    record.  Computes wait time from admission timestamp, assigns a bed
    identifier, and derives current patient status.

    When vitals data provides deteriorating signals (low SpO2, high HR),
    the current_status is escalated accordingly.

    Falls back to admission-only data when no matching vitals are found.
    """
    logger.info(
        "Transforming: %s.raw_admissions + %s.raw_vitals -> %s.patient_flow",
        BRONZE, BRONZE, SILVER,
    )

    admissions_df = spark.table(f"{BRONZE}.raw_admissions")
    vitals_df = spark.table(f"{BRONZE}.raw_vitals")

    # --- Get the latest vital-sign reading per patient ---
    vitals_window = Window.partitionBy("patient_id").orderBy(F.col("timestamp").desc())

    latest_vitals = (
        vitals_df
        .withColumn("rn", F.row_number().over(vitals_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            F.col("patient_id").alias("v_patient_id"),
            "heart_rate",
            "blood_pressure_sys",
            "spo2",
            "respiratory_rate",
            F.col("timestamp").alias("vitals_timestamp"),
        )
    )

    # --- Join admissions with latest vitals ---
    joined_df = admissions_df.join(
        latest_vitals,
        admissions_df["patient_id"] == latest_vitals["v_patient_id"],
        how="left",
    ).drop("v_patient_id")

    # --- Compute wait time (minutes from admission to first vitals reading) ---
    # If no vitals, estimate wait from acuity level
    wait_time_from_vitals = F.round(
        (F.unix_timestamp("vitals_timestamp") - F.unix_timestamp("timestamp")) / 60.0
    ).cast("int")

    # Acuity-based fallback: higher acuity = shorter wait
    wait_time_fallback = (
        F.when(F.col("acuity_level") == 1, F.lit(5))
        .when(F.col("acuity_level") == 2, F.lit(15))
        .when(F.col("acuity_level") == 3, F.lit(30))
        .when(F.col("acuity_level") == 4, F.lit(45))
        .otherwise(F.lit(60))
    )

    wait_time_min = F.coalesce(
        F.when(wait_time_from_vitals > 0, wait_time_from_vitals),
        wait_time_fallback,
    )

    # --- Bed assignment ---
    # Deterministic bed assignment using admission_id hash and department capacity
    bed_prefix_expr = (
        F.when(F.col("department") == "Emergency", F.lit("ED"))
        .when(F.col("department") == "ICU", F.lit("ICU"))
        .when(F.col("department") == "Cardiology", F.lit("CARD"))
        .when(F.col("department") == "Orthopedics", F.lit("ORTH"))
        .when(F.col("department") == "Oncology", F.lit("ONC"))
        .when(F.col("department") == "Pediatrics", F.lit("PEDS"))
        .otherwise(F.lit("GEN"))
    )

    bed_number = (F.abs(F.hash(F.col("admission_id"))) % F.lit(50)) + F.lit(1)
    bed_assigned = F.concat(
        bed_prefix_expr,
        F.lit("-"),
        F.lpad(bed_number.cast("string"), 3, "0"),
    )

    # --- Current status ---
    # Derive status from vitals + admission timing
    hours_since_admission = (
        F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("timestamp")
    ) / 3600.0

    current_status = (
        F.when(
            (F.col("spo2").isNotNull()) & (F.col("spo2") < 90),
            F.lit("In Treatment"),
        )
        .when(
            (F.col("heart_rate").isNotNull()) & (F.col("heart_rate") > 120),
            F.lit("In Treatment"),
        )
        .when(hours_since_admission > 24 * 7, F.lit("Discharged"))
        .when(hours_since_admission > 2, F.lit("Admitted"))
        .otherwise(F.lit("Admitted"))
    )

    # --- LOS (null if still admitted; compute from admission time if > 24h) ---
    los_days = F.when(
        current_status == "Discharged",
        F.round(hours_since_admission / 24.0, 1),
    ).otherwise(F.lit(None).cast("double"))

    flow_df = (
        joined_df
        .withColumn("wait_time_min", wait_time_min)
        .withColumn("bed_assigned", bed_assigned)
        .withColumn("los_days", los_days)
        .withColumn("current_status", current_status)
        .select(
            "admission_id",
            "patient_id",
            "facility",
            "department",
            "admission_type",
            "acuity_level",
            "wait_time_min",
            "bed_assigned",
            "los_days",
            "current_status",
        )
    )

    _write_silver(flow_df, "patient_flow")
    return flow_df


# =========================================================================
# 2.  raw_discharges  ->  readmission_risk
# =========================================================================
def transform_readmission_risk(spark: SparkSession) -> DataFrame:
    """
    Score each discharged patient for 30-day readmission risk.  Attempts to
    use the MLflow-registered Readmission_Risk_Predictor model; falls back
    to a deterministic rule-based scorer when the model is unavailable.

    Enriches each discharge record with simulated demographic features
    (age, prior admissions, comorbidity index) since raw discharge data
    does not carry these directly.
    """
    logger.info(
        "Transforming: %s.raw_discharges -> %s.readmission_risk", BRONZE, SILVER,
    )

    discharges_df = spark.table(f"{BRONZE}.raw_discharges")

    # --- Enrich with simulated demographic features ---
    # In production, these would come from an EHR/demographics table join.
    # Here we derive deterministic-but-realistic values from patient_id hash.
    patient_hash = F.abs(F.hash(F.col("patient_id")))

    age = (patient_hash % F.lit(60)) + F.lit(18)  # age 18-77

    prior_admissions_12m = (
        F.when(F.col("readmission_risk_score") >= 0.65, (patient_hash % F.lit(5)) + F.lit(2))
        .when(F.col("readmission_risk_score") >= 0.40, (patient_hash % F.lit(3)) + F.lit(1))
        .otherwise(patient_hash % F.lit(2))
    ).cast("int")

    # Charlson comorbidity index: 0 (healthy) to 10+ (severe)
    comorbidity_index = F.round(
        F.when(F.col("readmission_risk_score") >= 0.65, F.lit(3.0) + (patient_hash % F.lit(50)) / F.lit(10.0))
        .when(F.col("readmission_risk_score") >= 0.40, F.lit(1.5) + (patient_hash % F.lit(30)) / F.lit(10.0))
        .otherwise((patient_hash % F.lit(20)) / F.lit(10.0)),
        2,
    )

    enriched_df = (
        discharges_df
        .withColumn("age", age.cast("int"))
        .withColumn("prior_admissions_12m", prior_admissions_12m)
        .withColumn("comorbidity_index", comorbidity_index)
    )

    # --- Score with model or fallback ---
    try:
        scored_df = _score_readmission_mlflow(spark, enriched_df)
    except Exception as exc:
        logger.warning(
            "MLflow readmission model not available (%s). Falling back to rule-based scoring.",
            exc,
        )
        scored_df = _score_readmission_rules(enriched_df)

    risk_df = scored_df.select(
        "patient_id",
        "facility",
        "department",
        "age",
        "diagnosis_code",
        "los_days",
        "prior_admissions_12m",
        "comorbidity_index",
        "risk_score",
        "risk_level",
    )

    _write_silver(risk_df, "readmission_risk")
    return risk_df


def _score_readmission_mlflow(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Load the production readmission model from MLflow and score each
    discharge record.  Uses applyInPandas for vectorised prediction.
    """
    import mlflow
    import numpy as np
    import pandas as pd

    model = mlflow.pyfunc.load_model(READMISSION_MODEL_URI)

    def _predict(pdf: pd.DataFrame) -> pd.DataFrame:
        feature_cols = ["age", "los_days", "prior_admissions_12m", "comorbidity_index"]
        X = pdf[feature_cols].values.astype(np.float64)

        proba = model.predict(pd.DataFrame(X, columns=feature_cols))
        if isinstance(proba, pd.DataFrame):
            scores = proba.iloc[:, 0].values
        else:
            scores = np.asarray(proba, dtype=np.float64)

        pdf["risk_score"] = np.round(scores, 4)
        pdf["risk_level"] = pd.cut(
            scores,
            bins=[0, 0.40, 0.65, 0.85, 1.01],
            labels=["Low", "Medium", "High", "Critical"],
        ).astype(str)

        return pdf

    result_schema = df.schema.add(
        T.StructField("risk_score", T.DoubleType())
    ).add(
        T.StructField("risk_level", T.StringType())
    )

    return df.groupby("facility").applyInPandas(_predict, schema=result_schema)


def _score_readmission_rules(df: DataFrame) -> DataFrame:
    """
    Deterministic rule-based readmission scorer.  Used as a fallback when
    no MLflow model is registered.

    Combines the pre-computed readmission_risk_score from the discharge
    record with enriched demographic features to refine the final score.
    """
    # Adjust the raw score with demographic risk factors
    age_factor = F.when(F.col("age") >= 75, F.lit(0.10)).when(
        F.col("age") >= 65, F.lit(0.05)
    ).otherwise(F.lit(0.0))

    comorbidity_factor = F.when(
        F.col("comorbidity_index") >= 5.0, F.lit(0.10)
    ).when(
        F.col("comorbidity_index") >= 3.0, F.lit(0.05)
    ).otherwise(F.lit(0.0))

    prior_adm_factor = F.when(
        F.col("prior_admissions_12m") >= 3, F.lit(0.08)
    ).when(
        F.col("prior_admissions_12m") >= 2, F.lit(0.04)
    ).otherwise(F.lit(0.0))

    los_factor = F.when(
        F.col("los_days") >= 10.0, F.lit(0.05)
    ).when(
        F.col("los_days") >= 7.0, F.lit(0.02)
    ).otherwise(F.lit(0.0))

    adjusted_score = F.least(
        F.col("readmission_risk_score") + age_factor + comorbidity_factor + prior_adm_factor + los_factor,
        F.lit(1.0),
    )

    risk_score = F.round(adjusted_score, 4)

    risk_level = (
        F.when(risk_score >= RISK_CRITICAL_THRESHOLD, F.lit("Critical"))
        .when(risk_score >= RISK_HIGH_THRESHOLD, F.lit("High"))
        .when(risk_score >= RISK_MEDIUM_THRESHOLD, F.lit("Medium"))
        .otherwise(F.lit("Low"))
    )

    return df.withColumn("risk_score", risk_score).withColumn("risk_level", risk_level)


# =========================================================================
# 3.  raw_equipment_telemetry  ->  equipment_health
# =========================================================================
def transform_equipment_health(spark: SparkSession) -> DataFrame:
    """
    Compute a composite health score for each medical equipment asset and
    predict a failure date.  Attempts to use the MLflow Equipment_Failure_Predictor
    model; falls back to rule-based scoring.

    Health score (0-100):
      - 100 = perfect condition
      - Below 60 = Warning
      - Below 30 = Critical

    Maintenance priority is derived from the health score and whether
    maintenance is already overdue.
    """
    logger.info(
        "Transforming: %s.raw_equipment_telemetry -> %s.equipment_health",
        BRONZE, SILVER,
    )

    telemetry_df = spark.table(f"{BRONZE}.raw_equipment_telemetry")

    # --- Get the latest reading per asset ---
    asset_window = Window.partitionBy("asset_id").orderBy(F.col("timestamp").desc())

    latest_df = (
        telemetry_df
        .withColumn("rn", F.row_number().over(asset_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # --- Score with model or fallback ---
    try:
        scored_df = _score_equipment_mlflow(spark, latest_df)
    except Exception as exc:
        logger.warning(
            "MLflow equipment model not available (%s). Falling back to rule-based scoring.",
            exc,
        )
        scored_df = _score_equipment_rules(latest_df)

    health_df = scored_df.select(
        "asset_id",
        "facility",
        "equipment_type",
        "health_score",
        "predicted_failure_date",
        "maintenance_priority",
        "alert_level",
    )

    _write_silver(health_df, "equipment_health")
    return health_df


def _score_equipment_mlflow(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Load the production equipment failure model from MLflow and score
    each asset.
    """
    import mlflow
    import numpy as np
    import pandas as pd

    model = mlflow.pyfunc.load_model(EQUIPMENT_MODEL_URI)

    def _predict(pdf: pd.DataFrame) -> pd.DataFrame:
        feature_cols = ["usage_hours", "temperature", "vibration", "error_count"]
        X = pdf[feature_cols].values.astype(np.float64)

        proba = model.predict(pd.DataFrame(X, columns=feature_cols))
        if isinstance(proba, pd.DataFrame):
            failure_proba = proba.iloc[:, 0].values
        else:
            failure_proba = np.asarray(proba, dtype=np.float64)

        # Health score is inverse of failure probability
        pdf["health_score"] = np.round((1.0 - failure_proba) * 100, 1)

        # Predicted failure date: based on probability and usage trajectory
        days_to_failure = np.where(
            failure_proba >= 0.7, np.random.randint(1, 14, size=len(failure_proba)),
            np.where(failure_proba >= 0.4, np.random.randint(14, 60, size=len(failure_proba)),
                      np.random.randint(60, 365, size=len(failure_proba)))
        )
        pdf["predicted_failure_date"] = [
            (datetime.utcnow() + timedelta(days=int(d))).date()
            for d in days_to_failure
        ]

        pdf["maintenance_priority"] = np.where(
            pdf["health_score"] < 30, "Critical",
            np.where(pdf["health_score"] < 60, "Urgent",
                      np.where(pdf["maintenance_due"], "Elevated", "Routine"))
        )

        pdf["alert_level"] = np.where(
            pdf["health_score"] < 30, "Critical",
            np.where(pdf["health_score"] < 60, "Warning", "Normal")
        )

        return pdf

    result_schema = (
        df.schema
        .add(T.StructField("health_score", T.DoubleType()))
        .add(T.StructField("predicted_failure_date", T.DateType()))
        .add(T.StructField("maintenance_priority", T.StringType()))
        .add(T.StructField("alert_level", T.StringType()))
    )

    return df.groupby("facility").applyInPandas(_predict, schema=result_schema)


def _score_equipment_rules(df: DataFrame) -> DataFrame:
    """
    Deterministic rule-based equipment health scorer.  Computes a composite
    health score from usage hours, temperature, vibration, and error count.

    Health score formula (each factor contributes 0-25 points deducted):
      - usage_hours:  deduct up to 25 for hours > 15,000
      - temperature:  deduct up to 25 for temp > 50 C
      - vibration:    deduct up to 25 for vibration > 5.0
      - error_count:  deduct up to 25 for errors > 20
    """
    # Usage hours penalty: 0 at <= 5000h, 25 at >= 20000h
    usage_penalty = F.least(
        F.greatest((F.col("usage_hours") - 5000) / 600.0, F.lit(0.0)),
        F.lit(25.0),
    )

    # Temperature penalty: 0 at <= 30C, 25 at >= 90C
    temp_penalty = F.least(
        F.greatest((F.col("temperature") - 30) / 2.4, F.lit(0.0)),
        F.lit(25.0),
    )

    # Vibration penalty: 0 at <= 1.0, 25 at >= 10.0
    vibration_penalty = F.least(
        F.greatest((F.col("vibration") - 1.0) / 0.36, F.lit(0.0)),
        F.lit(25.0),
    )

    # Error count penalty: 0 at 0 errors, 25 at >= 100 errors
    error_penalty = F.least(
        F.greatest(F.col("error_count") / 4.0, F.lit(0.0)),
        F.lit(25.0),
    )

    health_score = F.round(
        F.greatest(
            F.lit(100.0) - usage_penalty - temp_penalty - vibration_penalty - error_penalty,
            F.lit(0.0),
        ),
        1,
    )

    # Predicted failure date: lower health = sooner failure
    days_to_failure = (
        F.when(health_score < 30, F.lit(7))
        .when(health_score < 60, F.lit(30))
        .when(health_score < 80, F.lit(90))
        .otherwise(F.lit(180))
    )
    predicted_failure_date = F.date_add(F.current_date(), days_to_failure)

    # Maintenance priority
    maintenance_priority = (
        F.when(health_score < EQUIP_HEALTH_CRITICAL_THRESHOLD, F.lit("Critical"))
        .when(health_score < EQUIP_HEALTH_WARNING_THRESHOLD, F.lit("Urgent"))
        .when(F.col("maintenance_due") == True, F.lit("Elevated"))
        .otherwise(F.lit("Routine"))
    )

    # Alert level
    alert_level = (
        F.when(health_score < EQUIP_HEALTH_CRITICAL_THRESHOLD, F.lit("Critical"))
        .when(health_score < EQUIP_HEALTH_WARNING_THRESHOLD, F.lit("Warning"))
        .otherwise(F.lit("Normal"))
    )

    return (
        df
        .withColumn("health_score", health_score)
        .withColumn("predicted_failure_date", predicted_failure_date)
        .withColumn("maintenance_priority", maintenance_priority)
        .withColumn("alert_level", alert_level)
    )


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
    logger.info("=== HealthcareIQ Silver transforms started ===")

    transform_patient_flow(spark)
    transform_readmission_risk(spark)
    transform_equipment_health(spark)

    logger.info("=== HealthcareIQ Silver transforms finished ===")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    spark = (
        SparkSession.builder.appName("HealthcareIQ_SilverTransform")
        .getOrCreate()
    )

    try:
        run_all(spark)
    except Exception:
        logger.exception("HealthcareIQ Silver transform failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
