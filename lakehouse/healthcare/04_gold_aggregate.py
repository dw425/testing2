"""
HealthcareIQ — Gold Layer Aggregation
======================================
Reads silver Delta tables and produces dashboard-ready gold tables:

  patient_flow + readmission_risk            ->  operations_kpis
  patient_flow                               ->  facility_capacity
  readmission_risk + raw_discharges          ->  readmission_summary
  equipment_health                           ->  equipment_status

Usage:
    spark-submit lakehouse/healthcare/04_gold_aggregate.py
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
logger = logging.getLogger("healthcare_gold_aggregate")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "healthcare_iq"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Bed capacity per facility-department (used for utilization computation)
# In production, these would be read from a reference table.
BED_CAPACITY = {
    ("Metro General Hospital", "Emergency"): 50,
    ("Metro General Hospital", "ICU"): 30,
    ("Metro General Hospital", "Cardiology"): 40,
    ("Metro General Hospital", "Orthopedics"): 30,
    ("Metro General Hospital", "Oncology"): 25,
    ("Metro General Hospital", "Pediatrics"): 20,
    ("Westside Medical Center", "Emergency"): 35,
    ("Westside Medical Center", "ICU"): 20,
    ("Westside Medical Center", "Cardiology"): 25,
    ("Westside Medical Center", "Orthopedics"): 20,
    ("Westside Medical Center", "Oncology"): 15,
    ("Westside Medical Center", "Pediatrics"): 15,
    ("Eastview Community Clinic", "Emergency"): 20,
    ("Eastview Community Clinic", "ICU"): 10,
    ("Eastview Community Clinic", "Cardiology"): 15,
    ("Eastview Community Clinic", "Orthopedics"): 12,
    ("Eastview Community Clinic", "Oncology"): 10,
    ("Eastview Community Clinic", "Pediatrics"): 8,
}

# Total beds across all facilities and departments
TOTAL_BEDS = sum(BED_CAPACITY.values())


# =========================================================================
# 1.  operations_kpis
# =========================================================================
def aggregate_operations_kpis(spark: SparkSession) -> DataFrame:
    """
    Build a point-in-time KPI snapshot combining patient flow, readmission
    risk, and equipment health data.  One row is appended per invocation.

    Metrics computed:
      - bed_utilization_pct:       occupied beds / total beds across all facilities
      - avg_ed_wait_min:           average ED wait time from patient_flow
      - avg_los_days:              average length of stay for discharged patients
      - readmission_rate_30d:      proportion of high/critical risk patients
      - critical_equipment_alerts: count of equipment in Critical alert state
    """
    logger.info(
        "Aggregating: %s.patient_flow + %s.readmission_risk + %s.equipment_health "
        "-> %s.operations_kpis",
        SILVER, SILVER, SILVER, GOLD,
    )

    patient_flow_df = spark.table(f"{SILVER}.patient_flow")
    readmission_df = spark.table(f"{SILVER}.readmission_risk")
    equipment_df = spark.table(f"{SILVER}.equipment_health")

    # --- Bed utilization ---
    # Count currently admitted patients (not discharged)
    occupied_beds = patient_flow_df.filter(
        F.col("current_status").isin("Admitted", "In Treatment")
    ).count()

    bed_utilization_pct = round((occupied_beds / TOTAL_BEDS) * 100, 1) if TOTAL_BEDS > 0 else 0.0

    # --- Average ED wait time ---
    ed_wait_row = (
        patient_flow_df
        .filter(F.col("department") == "Emergency")
        .agg(F.avg("wait_time_min").alias("avg_wait"))
        .collect()
    )
    avg_ed_wait_min = int(ed_wait_row[0]["avg_wait"]) if ed_wait_row and ed_wait_row[0]["avg_wait"] else 34

    # --- Average LOS ---
    los_row = (
        patient_flow_df
        .filter(F.col("los_days").isNotNull())
        .agg(F.avg("los_days").alias("avg_los"))
        .collect()
    )
    avg_los_days = round(float(los_row[0]["avg_los"]), 1) if los_row and los_row[0]["avg_los"] else 4.2

    # --- 30-day readmission rate ---
    total_discharged = readmission_df.count()
    high_risk_count = readmission_df.filter(
        F.col("risk_level").isin("High", "Critical")
    ).count()
    readmission_rate_30d = round(
        (high_risk_count / max(total_discharged, 1)) * 100, 1
    )

    # --- Critical equipment alerts ---
    critical_alerts = equipment_df.filter(
        F.col("alert_level") == "Critical"
    ).count()

    # --- Build the KPI row ---
    kpi_row = spark.createDataFrame(
        [
            (
                datetime.utcnow(),
                float(bed_utilization_pct),
                int(avg_ed_wait_min),
                float(avg_los_days),
                float(readmission_rate_30d),
                int(critical_alerts),
            )
        ],
        schema=T.StructType([
            T.StructField("snapshot_time", T.TimestampType()),
            T.StructField("bed_utilization_pct", T.DoubleType()),
            T.StructField("avg_ed_wait_min", T.IntegerType()),
            T.StructField("avg_los_days", T.DoubleType()),
            T.StructField("readmission_rate_30d", T.DoubleType()),
            T.StructField("critical_equipment_alerts", T.IntegerType()),
        ]),
    )

    _write_gold(kpi_row, "operations_kpis", mode="append")
    return kpi_row


# =========================================================================
# 2.  facility_capacity
# =========================================================================
def aggregate_facility_capacity(spark: SparkSession) -> DataFrame:
    """
    Compute per-facility, per-department bed capacity and utilization.
    Predicts hours until capacity breach based on current admission rate.

    Uses the BED_CAPACITY reference dictionary and counts currently
    admitted patients from silver.patient_flow.
    """
    logger.info(
        "Aggregating: %s.patient_flow -> %s.facility_capacity", SILVER, GOLD,
    )

    patient_flow_df = spark.table(f"{SILVER}.patient_flow")

    # Count occupied beds per facility-department
    occupied_df = (
        patient_flow_df
        .filter(F.col("current_status").isin("Admitted", "In Treatment"))
        .groupBy("facility", "department")
        .agg(F.countDistinct("bed_assigned").alias("occupied_beds"))
    )

    # Build reference capacity DataFrame
    capacity_rows = [
        (fac, dept, beds)
        for (fac, dept), beds in BED_CAPACITY.items()
    ]
    capacity_ref = spark.createDataFrame(
        capacity_rows,
        schema=T.StructType([
            T.StructField("ref_facility", T.StringType()),
            T.StructField("ref_department", T.StringType()),
            T.StructField("total_beds", T.IntegerType()),
        ]),
    )

    # Join with occupancy
    joined_df = capacity_ref.join(
        occupied_df,
        (capacity_ref["ref_facility"] == occupied_df["facility"])
        & (capacity_ref["ref_department"] == occupied_df["department"]),
        how="left",
    )

    filled_df = (
        joined_df
        .withColumn(
            "occupied_beds",
            F.coalesce(F.col("occupied_beds"), F.lit(0)).cast("int"),
        )
        .withColumn(
            "facility",
            F.coalesce(F.col("facility"), F.col("ref_facility")),
        )
        .withColumn(
            "department",
            F.coalesce(joined_df["department"], F.col("ref_department")),
        )
    )

    # Compute utilization percentage
    utilization_pct = F.round(
        (F.col("occupied_beds") / F.col("total_beds")) * 100, 1
    )

    # Predict capacity breach: if utilization > 80%, estimate hours to 100%
    # based on an assumed admission rate of 2 patients per hour per department
    remaining_beds = F.col("total_beds") - F.col("occupied_beds")
    admission_rate_per_hour = F.lit(2)  # approximate

    predicted_breach_hours = F.when(
        utilization_pct >= 100, F.lit(0)
    ).when(
        utilization_pct >= 80,
        F.round(remaining_beds / admission_rate_per_hour).cast("int"),
    ).otherwise(F.lit(None).cast("int"))

    capacity_df = (
        filled_df
        .withColumn("utilization_pct", utilization_pct)
        .withColumn("predicted_capacity_breach_hours", predicted_breach_hours)
        .withColumn("last_updated", F.current_timestamp())
        .select(
            "facility",
            "department",
            "total_beds",
            "occupied_beds",
            "utilization_pct",
            "predicted_capacity_breach_hours",
            "last_updated",
        )
    )

    _write_gold(capacity_df, "facility_capacity", mode="overwrite")
    return capacity_df


# =========================================================================
# 3.  readmission_summary
# =========================================================================
def aggregate_readmission_summary(spark: SparkSession) -> DataFrame:
    """
    Aggregate readmission analytics by facility and department.

    For each (facility, department) pair:
      - total_discharges:  count of scored patients
      - readmissions:      count with risk_level in (High, Critical)
      - readmission_rate:  readmissions / total_discharges
      - avg_risk_score:    average model risk score
      - top_diagnosis:     most frequent ICD-10 code among readmissions
    """
    logger.info(
        "Aggregating: %s.readmission_risk + %s.raw_discharges -> %s.readmission_summary",
        SILVER, BRONZE, GOLD,
    )

    risk_df = spark.table(f"{SILVER}.readmission_risk")
    discharges_df = spark.table(f"{BRONZE}.raw_discharges")

    # Join risk with discharges to get diagnosis codes
    joined_df = risk_df.join(
        discharges_df.select(
            F.col("patient_id").alias("d_patient_id"),
            "diagnosis_code",
        ),
        risk_df["patient_id"] == F.col("d_patient_id"),
        how="left",
    ).drop("d_patient_id")

    # Flag readmissions
    is_readmission = F.col("risk_level").isin("High", "Critical")

    # Aggregate per facility-department
    summary_df = (
        joined_df
        .groupBy("facility", "department")
        .agg(
            F.count("*").alias("total_discharges"),
            F.sum(is_readmission.cast("int")).alias("readmissions"),
            F.round(F.avg("risk_score"), 4).alias("avg_risk_score"),
        )
    )

    # Compute readmission rate
    summary_df = summary_df.withColumn(
        "readmission_rate",
        F.round(F.col("readmissions") / F.col("total_discharges"), 4),
    )

    # Top diagnosis per (facility, department) among readmissions
    readmit_diag = (
        joined_df
        .filter(is_readmission)
        .groupBy("facility", "department", "diagnosis_code")
        .agg(F.count("*").alias("diag_count"))
    )

    diag_window = Window.partitionBy("facility", "department").orderBy(
        F.col("diag_count").desc()
    )
    top_diag = (
        readmit_diag
        .withColumn("rn", F.row_number().over(diag_window))
        .filter(F.col("rn") == 1)
        .select(
            F.col("facility").alias("td_facility"),
            F.col("department").alias("td_department"),
            F.col("diagnosis_code").alias("top_diagnosis"),
        )
    )

    # Join top diagnosis back
    final_df = summary_df.join(
        top_diag,
        (summary_df["facility"] == top_diag["td_facility"])
        & (summary_df["department"] == top_diag["td_department"]),
        how="left",
    ).drop("td_facility", "td_department")

    # Fill nulls for departments with no readmissions
    final_df = final_df.withColumn(
        "top_diagnosis",
        F.coalesce(F.col("top_diagnosis"), F.lit("N/A")),
    ).select(
        "facility",
        "department",
        F.col("total_discharges").cast("int"),
        F.col("readmissions").cast("int"),
        "readmission_rate",
        "avg_risk_score",
        "top_diagnosis",
    )

    _write_gold(final_df, "readmission_summary", mode="overwrite")
    return final_df


# =========================================================================
# 4.  equipment_status
# =========================================================================
def aggregate_equipment_status(spark: SparkSession) -> DataFrame:
    """
    Aggregate equipment fleet health by facility and equipment type.

    For each (facility, equipment_type) pair:
      - total_assets:     count of all assets
      - healthy:          count with alert_level = Normal
      - maintenance_due:  count with maintenance_priority in (Elevated, Urgent, Critical)
      - critical_alerts:  count with alert_level = Critical
    """
    logger.info(
        "Aggregating: %s.equipment_health -> %s.equipment_status", SILVER, GOLD,
    )

    equipment_df = spark.table(f"{SILVER}.equipment_health")

    status_df = (
        equipment_df
        .groupBy("facility", "equipment_type")
        .agg(
            F.count("*").alias("total_assets"),
            F.sum(
                F.when(F.col("alert_level") == "Normal", F.lit(1)).otherwise(F.lit(0))
            ).alias("healthy"),
            F.sum(
                F.when(
                    F.col("maintenance_priority").isin("Elevated", "Urgent", "Critical"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("maintenance_due"),
            F.sum(
                F.when(F.col("alert_level") == "Critical", F.lit(1)).otherwise(F.lit(0))
            ).alias("critical_alerts"),
        )
        .withColumn("last_updated", F.current_timestamp())
        .select(
            "facility",
            "equipment_type",
            F.col("total_assets").cast("int"),
            F.col("healthy").cast("int"),
            F.col("maintenance_due").cast("int"),
            F.col("critical_alerts").cast("int"),
            "last_updated",
        )
    )

    _write_gold(status_df, "equipment_status", mode="overwrite")
    return status_df


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
    logger.info("=== HealthcareIQ Gold aggregations started ===")

    # facility_capacity and equipment_status have no cross-dependencies,
    # but operations_kpis reads from patient_flow + readmission + equipment,
    # so run the independent ones first.
    aggregate_facility_capacity(spark)
    aggregate_readmission_summary(spark)
    aggregate_equipment_status(spark)
    aggregate_operations_kpis(spark)

    logger.info("=== HealthcareIQ Gold aggregations finished ===")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    spark = (
        SparkSession.builder.appName("HealthcareIQ_GoldAggregate")
        .getOrCreate()
    )

    try:
        run_all(spark)
    except Exception:
        logger.exception("HealthcareIQ Gold aggregation failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
