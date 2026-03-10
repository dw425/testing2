"""
ManufacturingIQ — Bronze Layer Ingest
=====================================
Ingests raw data into the bronze Delta tables in Unity Catalog.

Supports two modes:
  1. **Streaming** (Auto Loader / cloudFiles) — for production workloads.
  2. **Batch**   — for demo / back-fill scenarios.

Usage:
    spark-submit lakehouse/02_bronze_ingest.py [--mode streaming|batch] [--source-dir /path]
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("bronze_ingest")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "manufacturing_iq"
SCHEMA = "bronze"

# Checkpoint root for Auto Loader streams
DEFAULT_CHECKPOINT_ROOT = "/tmp/manufacturing_iq/checkpoints/bronze"

# ---------------------------------------------------------------------------
# Schemas (match 01_setup_catalog.sql)
# ---------------------------------------------------------------------------
IOT_TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("asset_id", StringType(), True),
        StructField("site", StringType(), True),
        StructField("machine_type", StringType(), True),
        StructField("vibration_hz", DoubleType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("spindle_rpm", DoubleType(), True),
        StructField("tool_wear_index", DoubleType(), True),
        StructField("feed_rate", DoubleType(), True),
    ]
)

ERP_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("component", StringType(), True),
        StructField("site", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("supplier", StringType(), True),
        StructField("lead_time_days", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("expected_delivery", DateType(), True),
    ]
)

INSPECTIONS_SCHEMA = StructType(
    [
        StructField("inspection_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("asset_id", StringType(), True),
        StructField("site", StringType(), True),
        StructField("measurement_um", DoubleType(), True),
        StructField("spec_upper_um", DoubleType(), True),
        StructField("spec_lower_um", DoubleType(), True),
        StructField("pass_fail", StringType(), True),
        StructField("inspection_method", StringType(), True),
    ]
)

# Mapping: table_name -> (source subfolder, schema)
TABLE_CONFIG: dict[str, dict] = {
    "raw_iot_telemetry": {
        "subfolder": "iot_telemetry",
        "schema": IOT_TELEMETRY_SCHEMA,
        "file_format": "json",
    },
    "raw_erp_orders": {
        "subfolder": "erp_orders",
        "schema": ERP_ORDERS_SCHEMA,
        "file_format": "json",
    },
    "raw_inspections": {
        "subfolder": "inspections",
        "schema": INSPECTIONS_SCHEMA,
        "file_format": "json",
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _full_table_name(table: str) -> str:
    """Return the three-level Unity Catalog table name."""
    return f"{CATALOG}.{SCHEMA}.{table}"


def _add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Append the _ingested_at audit column."""
    return df.withColumn("_ingested_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# Streaming ingest (Auto Loader)
# ---------------------------------------------------------------------------
def ingest_streaming(
    spark: SparkSession,
    table_name: str,
    source_dir: str,
    checkpoint_root: str = DEFAULT_CHECKPOINT_ROOT,
) -> None:
    """
    Start a structured-streaming micro-batch using Auto Loader (cloudFiles).

    Each source subfolder is expected at ``<source_dir>/<subfolder>/``.
    """
    cfg = TABLE_CONFIG[table_name]
    source_path = f"{source_dir}/{cfg['subfolder']}"
    checkpoint_path = f"{checkpoint_root}/{table_name}"
    full_name = _full_table_name(table_name)

    logger.info(
        "Starting Auto Loader stream: %s -> %s (checkpoint: %s)",
        source_path,
        full_name,
        checkpoint_path,
    )

    stream_reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", cfg["file_format"])
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", _schema_hints(cfg["schema"]))
        .schema(cfg["schema"])
        .load(source_path)
    )

    stream_with_meta = _add_ingestion_metadata(stream_reader)

    query = (
        stream_with_meta.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(full_name)
    )

    query.awaitTermination()
    logger.info("Stream for %s completed current micro-batch.", full_name)


def _schema_hints(schema: StructType) -> str:
    """Build a comma-separated schema-hints string from a StructType."""
    mapping = {
        "StringType": "STRING",
        "TimestampType": "TIMESTAMP",
        "DoubleType": "DOUBLE",
        "IntegerType": "INT",
        "DateType": "DATE",
    }
    hints = []
    for field in schema.fields:
        spark_type = type(field.dataType).__name__
        if spark_type in mapping:
            hints.append(f"{field.name} {mapping[spark_type]}")
    return ", ".join(hints)


# ---------------------------------------------------------------------------
# Batch ingest
# ---------------------------------------------------------------------------
def ingest_batch(
    spark: SparkSession,
    table_name: str,
    source_dir: str,
) -> None:
    """
    One-shot batch read from ``<source_dir>/<subfolder>/`` into the bronze
    Delta table.  Suitable for demos and back-fills.
    """
    cfg = TABLE_CONFIG[table_name]
    source_path = f"{source_dir}/{cfg['subfolder']}"
    full_name = _full_table_name(table_name)

    logger.info("Batch ingest: %s -> %s", source_path, full_name)

    raw_df = (
        spark.read.format(cfg["file_format"])
        .schema(cfg["schema"])
        .load(source_path)
    )

    enriched_df = _add_ingestion_metadata(raw_df)

    enriched_df.write.format("delta").mode("append").saveAsTable(full_name)

    row_count = enriched_df.count()
    logger.info("Batch ingest complete for %s — %d rows written.", full_name, row_count)


# ---------------------------------------------------------------------------
# Batch ingest from DataFrame (for demo / seed scripts)
# ---------------------------------------------------------------------------
def ingest_dataframe(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
) -> None:
    """
    Write an already-constructed DataFrame into the target bronze table.
    Used by the demo seed script (05_seed_demo_data.py) which generates
    data in-memory rather than reading from files on disk.
    """
    full_name = _full_table_name(table_name)
    logger.info("DataFrame ingest -> %s", full_name)

    enriched_df = _add_ingestion_metadata(df)
    enriched_df.write.format("delta").mode("append").saveAsTable(full_name)

    row_count = enriched_df.count()
    logger.info("DataFrame ingest complete for %s — %d rows.", full_name, row_count)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_all(
    spark: SparkSession,
    mode: str = "batch",
    source_dir: str = "/mnt/manufacturing_iq/landing",
    checkpoint_root: str = DEFAULT_CHECKPOINT_ROOT,
) -> None:
    """Ingest all bronze tables in the chosen mode."""
    logger.info(
        "=== Bronze ingest started (mode=%s, source=%s) ===", mode, source_dir
    )

    for table_name in TABLE_CONFIG:
        if mode == "streaming":
            ingest_streaming(spark, table_name, source_dir, checkpoint_root)
        else:
            ingest_batch(spark, table_name, source_dir)

    logger.info("=== Bronze ingest finished ===")


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ManufacturingIQ bronze-layer ingest."
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "streaming"],
        default="batch",
        help="Ingest mode (default: batch).",
    )
    parser.add_argument(
        "--source-dir",
        default="/mnt/manufacturing_iq/landing",
        help="Root path containing source subfolders.",
    )
    parser.add_argument(
        "--checkpoint-root",
        default=DEFAULT_CHECKPOINT_ROOT,
        help="Root path for streaming checkpoints.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)

    spark = (
        SparkSession.builder.appName("ManufacturingIQ_BronzeIngest")
        .getOrCreate()
    )

    try:
        run_all(
            spark,
            mode=args.mode,
            source_dir=args.source_dir,
            checkpoint_root=args.checkpoint_root,
        )
    except Exception:
        logger.exception("Bronze ingest failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
