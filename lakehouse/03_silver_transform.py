"""
ManufacturingIQ — Silver Layer Transforms
==========================================
Reads bronze Delta tables and produces curated silver tables:

  raw_iot_telemetry   ->  cnc_anomalies      (anomaly detection + SHAP)
  raw_erp_orders      ->  enriched_orders     (inventory join + stock status)
  raw_inspections     ->  tolerance_stats     (deviation + out-of-spec flag)
  telemetry events    ->  build_tracking      (station-level tracking)

Usage:
    spark-submit lakehouse/03_silver_transform.py
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
logger = logging.getLogger("silver_transform")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "manufacturing_iq"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Anomaly-detection model registered in MLflow
ANOMALY_MODEL_NAME = "manufacturing_iq_anomaly_detector"
ANOMALY_MODEL_URI = f"models:/{ANOMALY_MODEL_NAME}/Production"

# Feature columns fed to the anomaly model
FEATURE_COLS = [
    "vibration_hz",
    "temp_c",
    "spindle_rpm",
    "tool_wear_index",
    "feed_rate",
]

# Inventory thresholds
STOCK_CRITICAL_DAYS = 7
STOCK_LOW_DAYS = 21

# Stations for build tracking (ordered)
STATIONS = ["LOAD", "ROUGH", "FINISH", "INSPECT", "PACK"]


# =========================================================================
# 1.  raw_iot_telemetry  ->  cnc_anomalies
# =========================================================================
def transform_cnc_anomalies(spark: SparkSession) -> DataFrame:
    """
    Score every telemetry row through the anomaly-detection model registered
    in MLflow and compute SHAP explanations for the top contributing feature.

    When the MLflow model is unavailable (e.g., first-time demo), a
    rule-based fallback is used so the pipeline never hard-fails.
    """
    logger.info("Transforming: %s.raw_iot_telemetry -> %s.cnc_anomalies", BRONZE, SILVER)

    telemetry_df = spark.table(f"{BRONZE}.raw_iot_telemetry")

    try:
        scored_df = _score_with_mlflow(spark, telemetry_df)
    except Exception as exc:
        logger.warning(
            "MLflow model not available (%s). Falling back to rule-based scoring.",
            exc,
        )
        scored_df = _score_rule_based(telemetry_df)

    scored_df = scored_df.select(
        "event_id",
        "timestamp",
        "asset_id",
        "site",
        "machine_type",
        "vibration_hz",
        "temp_c",
        "spindle_rpm",
        "tool_wear_index",
        "feed_rate",
        "is_anomalous",
        "anomaly_confidence",
        "shap_top_driver",
        "shap_top_value",
    )

    _write_silver(scored_df, "cnc_anomalies")
    return scored_df


def _score_with_mlflow(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Load the production anomaly-detection model from MLflow and apply it as
    a Pandas UDF for vectorised scoring.  SHAP values are computed for each
    row and the single highest-impact feature is extracted.
    """
    import mlflow
    import numpy as np
    import pandas as pd
    import shap
    from pyspark.sql.functions import pandas_udf

    model = mlflow.pyfunc.load_model(ANOMALY_MODEL_URI)
    underlying = model._model_impl  # access sklearn / xgboost handle
    explainer = shap.TreeExplainer(underlying)

    feature_schema = T.StructType(
        [
            T.StructField("is_anomalous", T.BooleanType()),
            T.StructField("anomaly_confidence", T.DoubleType()),
            T.StructField("shap_top_driver", T.StringType()),
            T.StructField("shap_top_value", T.DoubleType()),
        ]
    )

    @pandas_udf(feature_schema, F.PandasUDFType.GROUPED_MAP)
    def _predict_udf(pdf: pd.DataFrame) -> pd.DataFrame:
        X = pdf[FEATURE_COLS].values.astype(np.float64)

        # Predict probabilities (binary classifier: 0=normal, 1=anomaly)
        proba = model.predict(pd.DataFrame(X, columns=FEATURE_COLS))
        if isinstance(proba, pd.DataFrame):
            confidence = proba.iloc[:, 0].values
        else:
            confidence = np.asarray(proba, dtype=np.float64)

        is_anomalous = confidence >= 0.5

        # SHAP
        shap_values = explainer.shap_values(X)
        if isinstance(shap_values, list):
            shap_values = shap_values[1]  # class-1 SHAP

        top_idx = np.argmax(np.abs(shap_values), axis=1)
        top_driver = [FEATURE_COLS[i] for i in top_idx]
        top_value = [float(shap_values[row, idx]) for row, idx in enumerate(top_idx)]

        return pd.DataFrame(
            {
                "is_anomalous": is_anomalous,
                "anomaly_confidence": confidence,
                "shap_top_driver": top_driver,
                "shap_top_value": top_value,
            }
        )

    # Group-apply requires a grouping key; use site for parallelism
    predictions = df.groupby("site").applyInPandas(
        lambda pdf: _predict_udf_plain(pdf, model, explainer),
        schema=df.schema.add(T.StructField("is_anomalous", T.BooleanType()))
        .add(T.StructField("anomaly_confidence", T.DoubleType()))
        .add(T.StructField("shap_top_driver", T.StringType()))
        .add(T.StructField("shap_top_value", T.DoubleType())),
    )

    return predictions


def _predict_udf_plain(pdf, model, explainer):
    """Plain Python function used inside applyInPandas."""
    import numpy as np
    import pandas as pd

    X = pdf[FEATURE_COLS].values.astype(np.float64)

    proba = model.predict(pd.DataFrame(X, columns=FEATURE_COLS))
    if isinstance(proba, pd.DataFrame):
        confidence = proba.iloc[:, 0].values
    else:
        confidence = np.asarray(proba, dtype=np.float64)

    pdf["is_anomalous"] = confidence >= 0.5
    pdf["anomaly_confidence"] = confidence

    shap_values = explainer.shap_values(X)
    if isinstance(shap_values, list):
        shap_values = shap_values[1]

    top_idx = np.argmax(np.abs(shap_values), axis=1)
    pdf["shap_top_driver"] = [FEATURE_COLS[i] for i in top_idx]
    pdf["shap_top_value"] = [
        float(shap_values[row, idx]) for row, idx in enumerate(top_idx)
    ]

    return pdf


def _score_rule_based(df: DataFrame) -> DataFrame:
    """
    Deterministic rule-based anomaly scorer.  Used as a fallback when no
    MLflow model is registered yet.

    Rules (any triggers anomaly):
      - vibration_hz  > 45 Hz
      - temp_c        > 82 C
      - tool_wear_index > 0.85
    """
    vibration_flag = F.col("vibration_hz") > 45.0
    temp_flag = F.col("temp_c") > 82.0
    wear_flag = F.col("tool_wear_index") > 0.85

    # Confidence: proportion of rules triggered (0.33, 0.67, 1.0)
    rule_count = (
        vibration_flag.cast("int") + temp_flag.cast("int") + wear_flag.cast("int")
    )

    confidence = F.round(rule_count / 3.0, 4)

    # Top driver: pick the highest normalised deviation
    vibration_dev = F.abs(F.col("vibration_hz") - 30.0) / 30.0
    temp_dev = F.abs(F.col("temp_c") - 65.0) / 65.0
    wear_dev = F.abs(F.col("tool_wear_index") - 0.5) / 0.5
    spindle_dev = F.abs(F.col("spindle_rpm") - 8000.0) / 8000.0
    feed_dev = F.abs(F.col("feed_rate") - 250.0) / 250.0

    top_driver = (
        F.when(
            (vibration_dev >= temp_dev)
            & (vibration_dev >= wear_dev)
            & (vibration_dev >= spindle_dev)
            & (vibration_dev >= feed_dev),
            F.lit("vibration_hz"),
        )
        .when(
            (temp_dev >= wear_dev) & (temp_dev >= spindle_dev) & (temp_dev >= feed_dev),
            F.lit("temp_c"),
        )
        .when(
            (wear_dev >= spindle_dev) & (wear_dev >= feed_dev),
            F.lit("tool_wear_index"),
        )
        .when(spindle_dev >= feed_dev, F.lit("spindle_rpm"))
        .otherwise(F.lit("feed_rate"))
    )

    top_value = (
        F.when(top_driver == "vibration_hz", vibration_dev)
        .when(top_driver == "temp_c", temp_dev)
        .when(top_driver == "tool_wear_index", wear_dev)
        .when(top_driver == "spindle_rpm", spindle_dev)
        .otherwise(feed_dev)
    )

    return df.withColumn(
        "is_anomalous", (rule_count >= 1)
    ).withColumn(
        "anomaly_confidence", confidence
    ).withColumn(
        "shap_top_driver", top_driver
    ).withColumn(
        "shap_top_value", F.round(top_value, 4)
    )


# =========================================================================
# 2.  raw_erp_orders  ->  enriched_orders
# =========================================================================
def transform_enriched_orders(spark: SparkSession) -> DataFrame:
    """
    Enrich ERP orders with current inventory levels and compute stock
    status (OK / LOW / CRITICAL) based on remaining days-of-stock.

    Inventory levels are derived from the most recent order quantities and
    a simulated daily consumption rate.
    """
    logger.info("Transforming: %s.raw_erp_orders -> %s.enriched_orders", BRONZE, SILVER)

    orders_df = spark.table(f"{BRONZE}.raw_erp_orders")

    # Compute running stock per (site, component) using window aggregation.
    # current_stock = total ordered quantity (simplified model).
    site_component_window = Window.partitionBy("site", "component").orderBy("order_date")

    cumulative_qty = F.sum("quantity").over(site_component_window)

    # Simulate daily consumption rate: ~2 % of cumulative stock per day.
    days_since_first = F.datediff(F.current_date(), F.col("order_date"))

    consumed = F.round(cumulative_qty * 0.02 * days_since_first).cast("int")

    current_stock = F.greatest(cumulative_qty - consumed, F.lit(0)).cast("int")

    # days_of_stock: remaining stock / daily consumption rate
    daily_rate = F.greatest(F.round(cumulative_qty * 0.02, 2), F.lit(1.0))
    days_of_stock = F.round(current_stock / daily_rate).cast("int")

    stock_status = (
        F.when(days_of_stock <= STOCK_CRITICAL_DAYS, F.lit("CRITICAL"))
        .when(days_of_stock <= STOCK_LOW_DAYS, F.lit("LOW"))
        .otherwise(F.lit("OK"))
    )

    enriched_df = (
        orders_df.withColumn("current_stock", current_stock)
        .withColumn("days_of_stock", days_of_stock)
        .withColumn("stock_status", stock_status)
        .select(
            "order_id",
            "batch_id",
            "component",
            "site",
            "quantity",
            "supplier",
            "lead_time_days",
            "order_date",
            "expected_delivery",
            "current_stock",
            "days_of_stock",
            "stock_status",
        )
    )

    _write_silver(enriched_df, "enriched_orders")
    return enriched_df


# =========================================================================
# 3.  raw_inspections  ->  tolerance_stats
# =========================================================================
def transform_tolerance_stats(spark: SparkSession) -> DataFrame:
    """
    Compute the measurement deviation from the nominal midpoint and flag
    rows whose measurement falls outside the [spec_lower, spec_upper] band.
    """
    logger.info(
        "Transforming: %s.raw_inspections -> %s.tolerance_stats", BRONZE, SILVER
    )

    inspections_df = spark.table(f"{BRONZE}.raw_inspections")

    nominal = (F.col("spec_upper_um") + F.col("spec_lower_um")) / 2.0
    deviation = F.round(F.col("measurement_um") - nominal, 4)
    is_oos = (F.col("measurement_um") > F.col("spec_upper_um")) | (
        F.col("measurement_um") < F.col("spec_lower_um")
    )

    tolerance_df = (
        inspections_df.withColumn("deviation_um", deviation)
        .withColumn("is_out_of_spec", is_oos)
        .select(
            "inspection_id",
            "timestamp",
            "asset_id",
            "site",
            "deviation_um",
            "is_out_of_spec",
            "inspection_method",
        )
    )

    _write_silver(tolerance_df, "tolerance_stats")
    return tolerance_df


# =========================================================================
# 4.  telemetry events  ->  build_tracking
# =========================================================================
def transform_build_tracking(spark: SparkSession) -> DataFrame:
    """
    Derive station-level build-tracking events from raw IoT telemetry.

    Logic:
      - Assign each event a station based on ``spindle_rpm`` buckets.
      - Infer status from operational ranges.
      - Flag defects using the anomaly rules (vibration / temp / wear).
    """
    logger.info(
        "Transforming: %s.raw_iot_telemetry -> %s.build_tracking", BRONZE, SILVER
    )

    telemetry_df = spark.table(f"{BRONZE}.raw_iot_telemetry")

    # Deterministic station assignment from spindle_rpm buckets
    station = (
        F.when(F.col("spindle_rpm") < 3000, F.lit("LOAD"))
        .when(F.col("spindle_rpm") < 6000, F.lit("ROUGH"))
        .when(F.col("spindle_rpm") < 9000, F.lit("FINISH"))
        .when(F.col("spindle_rpm") < 11000, F.lit("INSPECT"))
        .otherwise(F.lit("PACK"))
    )

    # Status inference
    status = (
        F.when(F.col("tool_wear_index") > 0.9, F.lit("ERROR"))
        .when(F.col("feed_rate") < 50, F.lit("IDLE"))
        .otherwise(F.lit("IN_PROGRESS"))
    )

    # Defect flag
    defect_flag = (
        F.when(F.col("vibration_hz") > 50, F.lit("VIBRATION_HIGH"))
        .when(F.col("temp_c") > 85, F.lit("OVERHEAT"))
        .when(F.col("tool_wear_index") > 0.9, F.lit("TOOL_WEAR"))
        .otherwise(F.lit("NONE"))
    )

    # Derive batch_id from asset_id + date
    batch_id = F.concat(
        F.col("asset_id"),
        F.lit("-"),
        F.date_format(F.col("timestamp"), "yyyyMMdd"),
    )

    build_df = (
        telemetry_df.withColumn("batch_id", batch_id)
        .withColumn("station", station)
        .withColumn("status", status)
        .withColumn("defect_flag", defect_flag)
        .select(
            "event_id",
            "timestamp",
            "batch_id",
            "station",
            "site",
            "status",
            "defect_flag",
        )
    )

    _write_silver(build_df, "build_tracking")
    return build_df


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
    logger.info("=== Silver transforms started ===")

    transform_cnc_anomalies(spark)
    transform_enriched_orders(spark)
    transform_tolerance_stats(spark)
    transform_build_tracking(spark)

    logger.info("=== Silver transforms finished ===")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    spark = (
        SparkSession.builder.appName("ManufacturingIQ_SilverTransform")
        .getOrCreate()
    )

    try:
        run_all(spark)
    except Exception:
        logger.exception("Silver transform failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
