"""
ManufacturingIQ — Gold Layer Aggregation
=========================================
Reads silver Delta tables and produces dashboard-ready gold tables:

  cnc_anomalies + model registry  ->  production_kpis
  enriched_orders                 ->  inventory_forecast
  enriched_orders                 ->  site_component_status
  model registry + cnc_anomalies  ->  model_health_metrics

Usage:
    spark-submit lakehouse/04_gold_aggregate.py
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
logger = logging.getLogger("gold_aggregate")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "manufacturing_iq"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Thresholds
ANOMALY_HIGH_CONF_THRESHOLD = 0.85
STOCK_CRITICAL_DAYS = 7
STOCK_LOW_DAYS = 21

# Default model version when MLflow is unavailable
DEFAULT_MODEL_VERSION = "v1.0-demo"
DEFAULT_MODEL_NAME = "manufacturing_iq_anomaly_detector"


# =========================================================================
# 1.  production_kpis
# =========================================================================
def aggregate_production_kpis(spark: SparkSession) -> DataFrame:
    """
    Build a point-in-time KPI snapshot from the latest anomaly data and
    model registry.  One row is appended per invocation.

    Metrics computed:
      - anomalies_1h:        anomaly count in the most recent 1-hour window
      - anomalies_high_conf: anomalies with confidence >= 0.85
      - model_f1_score:      latest F1 from the model_health_metrics table
                              (falls back to a demo value when unavailable)
      - inference_latency_ms: P95 latency
      - data_drift_pct:      placeholder drift metric
      - model_version:       from MLflow or demo default
    """
    logger.info("Aggregating: %s.cnc_anomalies -> %s.production_kpis", SILVER, GOLD)

    anomalies_df = spark.table(f"{SILVER}.cnc_anomalies")

    snapshot_time = F.current_timestamp()

    # Find the latest timestamp in the data
    max_ts_row = anomalies_df.agg(F.max("timestamp").alias("max_ts")).collect()
    max_ts = max_ts_row[0]["max_ts"] if max_ts_row and max_ts_row[0]["max_ts"] else datetime.utcnow()

    one_hour_ago = max_ts - timedelta(hours=1)

    recent = anomalies_df.filter(F.col("timestamp") >= F.lit(one_hour_ago))

    anomalies_1h = recent.filter(F.col("is_anomalous") == True).count()
    anomalies_high_conf = recent.filter(
        (F.col("is_anomalous") == True)
        & (F.col("anomaly_confidence") >= ANOMALY_HIGH_CONF_THRESHOLD)
    ).count()

    # Attempt to read the latest model health metrics
    model_f1 = _get_latest_model_metric(spark, "f1_score", 0.92)
    latency_ms = _get_latest_model_metric(spark, "latency_ms", 45.0)
    drift_pct = _get_latest_model_metric(spark, "drift_pct", 1.2)
    model_version = _get_latest_model_version(spark)

    kpi_row = spark.createDataFrame(
        [
            (
                datetime.utcnow(),
                float(model_f1),
                float(latency_ms),
                float(drift_pct),
                int(anomalies_1h),
                int(anomalies_high_conf),
                str(model_version),
            )
        ],
        schema=T.StructType(
            [
                T.StructField("snapshot_time", T.TimestampType()),
                T.StructField("model_f1_score", T.DoubleType()),
                T.StructField("inference_latency_ms", T.DoubleType()),
                T.StructField("data_drift_pct", T.DoubleType()),
                T.StructField("anomalies_1h", T.IntegerType()),
                T.StructField("anomalies_high_conf", T.IntegerType()),
                T.StructField("model_version", T.StringType()),
            ]
        ),
    )

    _write_gold(kpi_row, "production_kpis", mode="append")
    return kpi_row


def _get_latest_model_metric(
    spark: SparkSession, column: str, default: float
) -> float:
    """Read the most recent value of *column* from gold.model_health_metrics."""
    try:
        row = (
            spark.table(f"{GOLD}.model_health_metrics")
            .orderBy(F.col("timestamp").desc())
            .select(column)
            .limit(1)
            .collect()
        )
        if row:
            return float(row[0][0])
    except Exception:
        pass
    return default


def _get_latest_model_version(spark: SparkSession) -> str:
    """Read the latest model_version from the existing KPIs or return default."""
    try:
        import mlflow

        client = mlflow.tracking.MlflowClient()
        latest = client.get_latest_versions(DEFAULT_MODEL_NAME, stages=["Production"])
        if latest:
            return f"v{latest[0].version}"
    except Exception:
        pass
    return DEFAULT_MODEL_VERSION


# =========================================================================
# 2.  inventory_forecast
# =========================================================================
def aggregate_inventory_forecast(spark: SparkSession) -> DataFrame:
    """
    Compute per-site, per-component inventory forecasts from
    silver.enriched_orders.

    For each (site, component) pair the function:
      - Takes the latest stock level and daily consumption rate.
      - Projects a predicted shortage date.
      - Classifies ramp impact.
    """
    logger.info(
        "Aggregating: %s.enriched_orders -> %s.inventory_forecast", SILVER, GOLD
    )

    orders_df = spark.table(f"{SILVER}.enriched_orders")

    # Latest record per (site, component) — the most recent order carries
    # the most up-to-date stock info.
    window = Window.partitionBy("site", "component").orderBy(
        F.col("order_date").desc()
    )

    latest_df = (
        orders_df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Predict shortage date
    predicted_shortage = F.when(
        F.col("days_of_stock") > 0,
        F.date_add(F.current_date(), F.col("days_of_stock")),
    ).otherwise(F.current_date())

    # Ramp impact classification
    ramp_impact = (
        F.when(F.col("stock_status") == "CRITICAL", F.lit("LINE_STOP_RISK"))
        .when(F.col("stock_status") == "LOW", F.lit("RAMP_DELAY"))
        .otherwise(F.lit("ON_TRACK"))
    )

    forecast_df = latest_df.select(
        F.current_date().alias("forecast_date"),
        "site",
        "component",
        "current_stock",
        predicted_shortage.alias("predicted_shortage_date"),
        "days_of_stock",
        "stock_status",
        ramp_impact.alias("ramp_impact"),
    )

    _write_gold(forecast_df, "inventory_forecast", mode="overwrite")
    return forecast_df


# =========================================================================
# 3.  site_component_status
# =========================================================================
def aggregate_site_component_status(spark: SparkSession) -> DataFrame:
    """
    Snapshot of current inventory status per (site, component), suitable
    for real-time dashboard tiles.  Mirrors inventory_forecast with a
    last_updated timestamp.
    """
    logger.info(
        "Aggregating: %s.enriched_orders -> %s.site_component_status", SILVER, GOLD
    )

    orders_df = spark.table(f"{SILVER}.enriched_orders")

    window = Window.partitionBy("site", "component").orderBy(
        F.col("order_date").desc()
    )

    latest_df = (
        orders_df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    predicted_shortage = F.when(
        F.col("days_of_stock") > 0,
        F.date_add(F.current_date(), F.col("days_of_stock")),
    ).otherwise(F.current_date())

    ramp_impact = (
        F.when(F.col("stock_status") == "CRITICAL", F.lit("LINE_STOP_RISK"))
        .when(F.col("stock_status") == "LOW", F.lit("RAMP_DELAY"))
        .otherwise(F.lit("ON_TRACK"))
    )

    status_df = latest_df.select(
        "site",
        "component",
        "stock_status",
        F.col("days_of_stock").alias("days_remaining"),
        predicted_shortage.alias("predicted_shortage"),
        ramp_impact.alias("ramp_impact"),
        F.current_timestamp().alias("last_updated"),
    )

    _write_gold(status_df, "site_component_status", mode="overwrite")
    return status_df


# =========================================================================
# 4.  model_health_metrics
# =========================================================================
def aggregate_model_health_metrics(spark: SparkSession) -> DataFrame:
    """
    Capture a point-in-time snapshot of model health.  Attempts to pull
    live metrics from the MLflow Model Registry; falls back to computing
    proxy metrics from the cnc_anomalies silver table.
    """
    logger.info(
        "Aggregating: model registry + %s.cnc_anomalies -> %s.model_health_metrics",
        SILVER,
        GOLD,
    )

    try:
        metrics_df = _model_metrics_from_mlflow(spark)
    except Exception as exc:
        logger.warning(
            "MLflow metrics unavailable (%s). Computing proxy metrics.", exc
        )
        metrics_df = _model_metrics_proxy(spark)

    _write_gold(metrics_df, "model_health_metrics", mode="append")
    return metrics_df


def _model_metrics_from_mlflow(spark: SparkSession) -> DataFrame:
    """Pull metrics from the MLflow tracking server for the production model."""
    import mlflow

    client = mlflow.tracking.MlflowClient()
    latest_versions = client.get_latest_versions(
        DEFAULT_MODEL_NAME, stages=["Production"]
    )

    if not latest_versions:
        raise RuntimeError("No production model version found.")

    mv = latest_versions[0]
    run = client.get_run(mv.run_id)
    metrics = run.data.metrics

    f1 = metrics.get("f1_score", 0.0)
    precision_val = metrics.get("precision", 0.0)
    recall = metrics.get("recall", 0.0)
    latency = metrics.get("inference_latency_ms", 0.0)
    drift = metrics.get("data_drift_pct", 0.0)

    # Feature importance from run params (serialised as JSON)
    import json

    fi_raw = run.data.params.get("feature_importance", "{}")
    fi_dict = json.loads(fi_raw)

    row = spark.createDataFrame(
        [
            (
                datetime.utcnow(),
                DEFAULT_MODEL_NAME,
                float(f1),
                float(precision_val),
                float(recall),
                float(latency),
                float(drift),
                fi_dict,
            )
        ],
        schema=_model_health_schema(),
    )

    return row


def _model_metrics_proxy(spark: SparkSession) -> DataFrame:
    """
    When MLflow is not available, compute proxy model-health metrics
    from the cnc_anomalies silver table.
    """
    anomalies_df = spark.table(f"{SILVER}.cnc_anomalies")

    total = anomalies_df.count()
    if total == 0:
        total = 1  # avoid division by zero

    anomalous_count = anomalies_df.filter(F.col("is_anomalous") == True).count()
    high_conf = anomalies_df.filter(
        (F.col("is_anomalous") == True)
        & (F.col("anomaly_confidence") >= ANOMALY_HIGH_CONF_THRESHOLD)
    ).count()

    # Proxy metrics (heuristic)
    anomaly_rate = anomalous_count / total
    precision_proxy = high_conf / max(anomalous_count, 1)
    recall_proxy = min(anomaly_rate * 10, 1.0)  # scaled
    f1_proxy = (
        2 * precision_proxy * recall_proxy / max(precision_proxy + recall_proxy, 1e-9)
    )

    # Compute average confidence as a latency proxy (ms)
    avg_conf_row = anomalies_df.agg(
        F.avg("anomaly_confidence").alias("avg_conf")
    ).collect()
    avg_conf = avg_conf_row[0]["avg_conf"] if avg_conf_row[0]["avg_conf"] else 0.5
    latency_proxy = avg_conf * 100.0  # scale to ms range

    # Feature importance from SHAP top-driver distribution
    fi_rows = (
        anomalies_df.filter(F.col("is_anomalous") == True)
        .groupBy("shap_top_driver")
        .agg(F.avg("shap_top_value").alias("avg_shap"))
        .collect()
    )
    fi_dict = {row["shap_top_driver"]: round(float(row["avg_shap"]), 4) for row in fi_rows}

    row = spark.createDataFrame(
        [
            (
                datetime.utcnow(),
                DEFAULT_MODEL_NAME,
                round(f1_proxy, 4),
                round(precision_proxy, 4),
                round(recall_proxy, 4),
                round(latency_proxy, 2),
                round(anomaly_rate * 100, 2),  # drift as anomaly rate pct
                fi_dict,
            )
        ],
        schema=_model_health_schema(),
    )

    return row


def _model_health_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("model_name", T.StringType()),
            T.StructField("f1_score", T.DoubleType()),
            T.StructField("precision_val", T.DoubleType()),
            T.StructField("recall", T.DoubleType()),
            T.StructField("latency_ms", T.DoubleType()),
            T.StructField("drift_pct", T.DoubleType()),
            T.StructField(
                "feature_importance", T.MapType(T.StringType(), T.DoubleType())
            ),
        ]
    )


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
    logger.info("=== Gold aggregations started ===")

    # model_health_metrics first — production_kpis reads from it
    aggregate_model_health_metrics(spark)
    aggregate_production_kpis(spark)
    aggregate_inventory_forecast(spark)
    aggregate_site_component_status(spark)

    logger.info("=== Gold aggregations finished ===")


# =========================================================================
# CLI
# =========================================================================
def main() -> None:
    spark = (
        SparkSession.builder.appName("ManufacturingIQ_GoldAggregate")
        .getOrCreate()
    )

    try:
        run_all(spark)
    except Exception:
        logger.exception("Gold aggregation failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
