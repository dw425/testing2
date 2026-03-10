"""
PySpark StructType schema definitions for the ManufacturingIQ lakehouse.

Defines schemas across three medallion layers:
  - Bronze: raw ingestion tables
  - Silver: cleansed / enriched tables
  - Gold: aggregated analytics tables

All schemas match the Unity Catalog DDL for catalog `manufacturing_iq`.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Bronze layer
# ---------------------------------------------------------------------------

# bronze.raw_iot_telemetry
# One row per sensor reading from CNC / assembly / press machines.
RAW_IOT_TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("machine_id", StringType(), nullable=False),
        StructField("vibration_hz", DoubleType(), nullable=False),
        StructField("temp_c", DoubleType(), nullable=False),
        StructField("spindle_rpm", DoubleType(), nullable=False),
        StructField("tool_wear_index", DoubleType(), nullable=False),
        StructField("feed_rate", DoubleType(), nullable=False),
        StructField("ingestion_ts", TimestampType(), nullable=False),
        StructField("source_file", StringType(), nullable=True),
    ]
)

# bronze.raw_erp_orders
# One row per production order imported from ERP.
RAW_ERP_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("product_line", StringType(), nullable=False),
        StructField("order_qty", IntegerType(), nullable=False),
        StructField("fulfilled_qty", IntegerType(), nullable=True),
        StructField("order_status", StringType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("due_date", TimestampType(), nullable=False),
        StructField("priority", StringType(), nullable=True),
        StructField("ingestion_ts", TimestampType(), nullable=False),
    ]
)

# bronze.raw_inspections
# One row per dimensional inspection measurement.
RAW_INSPECTIONS_SCHEMA = StructType(
    [
        StructField("inspection_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("machine_id", StringType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("part_serial", StringType(), nullable=False),
        StructField("measurement_um", DoubleType(), nullable=False),
        StructField("lower_spec_um", DoubleType(), nullable=False),
        StructField("upper_spec_um", DoubleType(), nullable=False),
        StructField("in_spec", BooleanType(), nullable=False),
        StructField("ingestion_ts", TimestampType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# Silver layer
# ---------------------------------------------------------------------------

# silver.cnc_anomalies
# One row per detected anomaly from the ML anomaly model.
CNC_ANOMALIES_SCHEMA = StructType(
    [
        StructField("anomaly_id", StringType(), nullable=False),
        StructField("event_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("machine_id", StringType(), nullable=False),
        StructField("anomaly_score", DoubleType(), nullable=False),
        StructField("vibration_hz", DoubleType(), nullable=False),
        StructField("temp_c", DoubleType(), nullable=False),
        StructField("spindle_rpm", DoubleType(), nullable=False),
        StructField("tool_wear_index", DoubleType(), nullable=False),
        StructField("feed_rate", DoubleType(), nullable=False),
        StructField("model_version", StringType(), nullable=False),
        StructField("detected_at", TimestampType(), nullable=False),
    ]
)

# silver.enriched_orders
# ERP orders enriched with fulfillment metrics and batch metadata.
ENRICHED_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("product_line", StringType(), nullable=False),
        StructField("order_qty", IntegerType(), nullable=False),
        StructField("fulfilled_qty", IntegerType(), nullable=True),
        StructField("fulfillment_pct", DoubleType(), nullable=True),
        StructField("order_status", StringType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("due_date", TimestampType(), nullable=False),
        StructField("priority", StringType(), nullable=True),
        StructField("days_until_due", IntegerType(), nullable=True),
        StructField("is_overdue", BooleanType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

# silver.tolerance_stats
# Aggregated tolerance statistics per machine per batch.
TOLERANCE_STATS_SCHEMA = StructType(
    [
        StructField("stat_id", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("machine_id", StringType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("measurement_count", LongType(), nullable=False),
        StructField("mean_um", DoubleType(), nullable=False),
        StructField("std_um", DoubleType(), nullable=False),
        StructField("min_um", DoubleType(), nullable=False),
        StructField("max_um", DoubleType(), nullable=False),
        StructField("out_of_spec_count", LongType(), nullable=False),
        StructField("out_of_spec_rate", DoubleType(), nullable=False),
        StructField("cp", DoubleType(), nullable=True),
        StructField("cpk", DoubleType(), nullable=True),
        StructField("computed_at", TimestampType(), nullable=False),
    ]
)

# silver.build_tracking
# One row per build-stage event for batch tracking.
BUILD_TRACKING_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("batch_id", StringType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("station", StringType(), nullable=False),
        StructField("stage_seq", IntegerType(), nullable=False),
        StructField("units_in", IntegerType(), nullable=False),
        StructField("units_out", IntegerType(), nullable=False),
        StructField("defect_count", IntegerType(), nullable=False),
        StructField("yield_pct", DoubleType(), nullable=False),
        StructField("cycle_time_sec", DoubleType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("defect_flag", StringType(), nullable=True),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("completed_at", TimestampType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Gold layer
# ---------------------------------------------------------------------------

# gold.production_kpis
# Periodic snapshot of top-level production KPIs for the dashboard.
PRODUCTION_KPIS_SCHEMA = StructType(
    [
        StructField("snapshot_id", StringType(), nullable=False),
        StructField("snapshot_time", TimestampType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("total_units_produced", LongType(), nullable=False),
        StructField("total_defects", LongType(), nullable=False),
        StructField("overall_yield_pct", DoubleType(), nullable=False),
        StructField("anomalies_1h", IntegerType(), nullable=False),
        StructField("model_f1_score", DoubleType(), nullable=False),
        StructField("inference_latency_ms", DoubleType(), nullable=False),
        StructField("data_drift_pct", DoubleType(), nullable=False),
        StructField("oee_pct", DoubleType(), nullable=True),
        StructField("computed_at", TimestampType(), nullable=False),
    ]
)

# gold.inventory_forecast
# Per-component inventory forecast produced by the Prophet model.
INVENTORY_FORECAST_SCHEMA = StructType(
    [
        StructField("forecast_id", StringType(), nullable=False),
        StructField("component_name", StringType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("forecast_date", TimestampType(), nullable=False),
        StructField("current_stock", IntegerType(), nullable=False),
        StructField("daily_usage", IntegerType(), nullable=False),
        StructField("stock_days_remaining", DoubleType(), nullable=False),
        StructField("forecasted_demand", DoubleType(), nullable=False),
        StructField("reorder_point", IntegerType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("model_version", StringType(), nullable=True),
        StructField("computed_at", TimestampType(), nullable=False),
    ]
)

# gold.site_component_status
# Current inventory health status per site and component.
SITE_COMPONENT_STATUS_SCHEMA = StructType(
    [
        StructField("status_id", StringType(), nullable=False),
        StructField("site", StringType(), nullable=False),
        StructField("component_name", StringType(), nullable=False),
        StructField("current_stock", IntegerType(), nullable=False),
        StructField("daily_usage", IntegerType(), nullable=False),
        StructField("stock_days_remaining", DoubleType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("last_restock_date", TimestampType(), nullable=True),
        StructField("next_delivery_date", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

# gold.model_health_metrics
# ML model monitoring metrics written after each evaluation run.
MODEL_HEALTH_METRICS_SCHEMA = StructType(
    [
        StructField("metric_id", StringType(), nullable=False),
        StructField("model_name", StringType(), nullable=False),
        StructField("model_version", StringType(), nullable=False),
        StructField("evaluation_time", TimestampType(), nullable=False),
        StructField("f1_score", DoubleType(), nullable=False),
        StructField("precision", DoubleType(), nullable=False),
        StructField("recall", DoubleType(), nullable=False),
        StructField("accuracy", DoubleType(), nullable=False),
        StructField("inference_latency_ms", DoubleType(), nullable=False),
        StructField("data_drift_pct", DoubleType(), nullable=False),
        StructField("feature_importance", MapType(StringType(), DoubleType()), nullable=True),
        StructField("alert_triggered", BooleanType(), nullable=False),
        StructField("alert_message", StringType(), nullable=True),
        StructField("computed_at", TimestampType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# Registry: convenient mapping of fully-qualified table name to schema.
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = {
    "bronze.raw_iot_telemetry": RAW_IOT_TELEMETRY_SCHEMA,
    "bronze.raw_erp_orders": RAW_ERP_ORDERS_SCHEMA,
    "bronze.raw_inspections": RAW_INSPECTIONS_SCHEMA,
    "silver.cnc_anomalies": CNC_ANOMALIES_SCHEMA,
    "silver.enriched_orders": ENRICHED_ORDERS_SCHEMA,
    "silver.tolerance_stats": TOLERANCE_STATS_SCHEMA,
    "silver.build_tracking": BUILD_TRACKING_SCHEMA,
    "gold.production_kpis": PRODUCTION_KPIS_SCHEMA,
    "gold.inventory_forecast": INVENTORY_FORECAST_SCHEMA,
    "gold.site_component_status": SITE_COMPONENT_STATUS_SCHEMA,
    "gold.model_health_metrics": MODEL_HEALTH_METRICS_SCHEMA,
}


def get_schema(table_fqn: str) -> StructType:
    """Return the StructType for a fully-qualified table name.

    Args:
        table_fqn: Table name in ``<schema>.<table>`` format,
                   e.g. ``"bronze.raw_iot_telemetry"``.

    Raises:
        KeyError: If the table name is not registered.
    """
    try:
        return TABLE_SCHEMAS[table_fqn]
    except KeyError:
        valid = ", ".join(sorted(TABLE_SCHEMAS))
        raise KeyError(
            f"Unknown table '{table_fqn}'. Valid tables: {valid}"
        ) from None
