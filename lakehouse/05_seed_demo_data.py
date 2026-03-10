"""
ManufacturingIQ — Demo Data Seed & Orchestration
==================================================
End-to-end script that:

  1. Generates synthetic bronze data (IoT telemetry, ERP orders, inspections).
  2. Writes it to the bronze Delta tables.
  3. Runs the silver transforms.
  4. Runs the gold aggregations.

Designed for first-time demo setup on a Databricks workspace.

Usage:
    spark-submit lakehouse/05_seed_demo_data.py [--num-telemetry 5000]
                                                 [--num-orders 200]
                                                 [--num-inspections 1000]
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import math
import random
import sys
import uuid
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("seed_demo_data")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CATALOG = "manufacturing_iq"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Reference data
SITES = ["ATX-1", "ATX-2", "SJC-1", "MUC-1"]
MACHINE_TYPES = ["CNC-5Axis", "CNC-3Axis", "CNC-Lathe", "CNC-Mill"]
COMPONENTS = [
    "Titanium Blisk",
    "Turbine Blade",
    "Compressor Disk",
    "Fan Case",
    "Bearing Housing",
    "Combustor Liner",
]
SUPPLIERS = [
    "AeroSupply Co",
    "PrecisionParts Inc",
    "MetalWorks GmbH",
    "TitanForge Ltd",
    "GlobalCast SA",
]
INSPECTION_METHODS = ["CMM", "Vision", "Laser", "Manual"]

# Random seed for reproducibility
RANDOM_SEED = 42


# =========================================================================
# Synthetic data generators
# =========================================================================


def _deterministic_uuid(prefix: str, index: int) -> str:
    """Generate a deterministic UUID-like string from a prefix and index."""
    raw = f"{prefix}-{index}"
    digest = hashlib.md5(raw.encode()).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


def generate_iot_telemetry(
    spark: SparkSession,
    num_rows: int = 5000,
    base_time: Optional[datetime] = None,
) -> DataFrame:
    """
    Generate synthetic IoT telemetry rows.

    Roughly 8-12 % of rows are injected with anomalous sensor readings
    (high vibration, high temperature, or high tool wear) to exercise the
    anomaly-detection pipeline.
    """
    logger.info("Generating %d synthetic IoT telemetry rows …", num_rows)

    rng = random.Random(RANDOM_SEED)
    if base_time is None:
        base_time = datetime.utcnow() - timedelta(hours=24)

    rows = []
    for i in range(num_rows):
        event_id = _deterministic_uuid("evt", i)
        ts = base_time + timedelta(seconds=i * 17 + rng.randint(0, 10))
        asset_id = f"CNC-{rng.randint(1, 20):03d}"
        site = rng.choice(SITES)
        machine_type = rng.choice(MACHINE_TYPES)

        # Normal operating ranges
        vibration = round(rng.gauss(28.0, 5.0), 2)
        temp = round(rng.gauss(62.0, 8.0), 2)
        spindle = round(rng.gauss(8000.0, 2000.0), 1)
        wear = round(rng.uniform(0.05, 0.65), 4)
        feed = round(rng.gauss(250.0, 50.0), 1)

        # Inject anomalies (~10 % of rows)
        if rng.random() < 0.10:
            anomaly_type = rng.choice(["vibration", "temp", "wear", "multi"])
            if anomaly_type == "vibration":
                vibration = round(rng.gauss(55.0, 8.0), 2)
            elif anomaly_type == "temp":
                temp = round(rng.gauss(92.0, 6.0), 2)
            elif anomaly_type == "wear":
                wear = round(rng.uniform(0.86, 0.99), 4)
            else:
                vibration = round(rng.gauss(52.0, 5.0), 2)
                temp = round(rng.gauss(88.0, 5.0), 2)
                wear = round(rng.uniform(0.87, 0.98), 4)

        # Clamp to physically plausible ranges
        vibration = max(0.5, vibration)
        temp = max(15.0, temp)
        spindle = max(500.0, spindle)
        wear = max(0.0, min(1.0, wear))
        feed = max(10.0, feed)

        rows.append(
            Row(
                event_id=event_id,
                timestamp=ts,
                asset_id=asset_id,
                site=site,
                machine_type=machine_type,
                vibration_hz=vibration,
                temp_c=temp,
                spindle_rpm=spindle,
                tool_wear_index=wear,
                feed_rate=feed,
            )
        )

    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("asset_id", T.StringType()),
            T.StructField("site", T.StringType()),
            T.StructField("machine_type", T.StringType()),
            T.StructField("vibration_hz", T.DoubleType()),
            T.StructField("temp_c", T.DoubleType()),
            T.StructField("spindle_rpm", T.DoubleType()),
            T.StructField("tool_wear_index", T.DoubleType()),
            T.StructField("feed_rate", T.DoubleType()),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)
    logger.info("IoT telemetry DataFrame created: %d rows.", df.count())
    return df


def generate_erp_orders(
    spark: SparkSession,
    num_rows: int = 200,
    base_date: Optional[datetime] = None,
) -> DataFrame:
    """
    Generate synthetic ERP purchase-order rows spread over the last 90 days.
    """
    logger.info("Generating %d synthetic ERP order rows …", num_rows)

    rng = random.Random(RANDOM_SEED + 1)
    if base_date is None:
        base_date = datetime.utcnow()

    rows = []
    for i in range(num_rows):
        order_id = _deterministic_uuid("ord", i)
        batch_id = f"BATCH-{rng.randint(1000, 9999)}"
        component = rng.choice(COMPONENTS)
        site = rng.choice(SITES)
        quantity = rng.randint(50, 5000)
        supplier = rng.choice(SUPPLIERS)
        lead_time = rng.randint(3, 45)
        days_ago = rng.randint(0, 90)
        order_date = (base_date - timedelta(days=days_ago)).date()
        expected_delivery = order_date + timedelta(days=lead_time)

        rows.append(
            Row(
                order_id=order_id,
                batch_id=batch_id,
                component=component,
                site=site,
                quantity=quantity,
                supplier=supplier,
                lead_time_days=lead_time,
                order_date=order_date,
                expected_delivery=expected_delivery,
            )
        )

    schema = T.StructType(
        [
            T.StructField("order_id", T.StringType()),
            T.StructField("batch_id", T.StringType()),
            T.StructField("component", T.StringType()),
            T.StructField("site", T.StringType()),
            T.StructField("quantity", T.IntegerType()),
            T.StructField("supplier", T.StringType()),
            T.StructField("lead_time_days", T.IntegerType()),
            T.StructField("order_date", T.DateType()),
            T.StructField("expected_delivery", T.DateType()),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)
    logger.info("ERP orders DataFrame created: %d rows.", df.count())
    return df


def generate_inspections(
    spark: SparkSession,
    num_rows: int = 1000,
    base_time: Optional[datetime] = None,
) -> DataFrame:
    """
    Generate synthetic quality-inspection rows.

    Approximately 5-8 % of measurements are intentionally set outside the
    specification band to test out-of-spec detection.
    """
    logger.info("Generating %d synthetic inspection rows …", num_rows)

    rng = random.Random(RANDOM_SEED + 2)
    if base_time is None:
        base_time = datetime.utcnow() - timedelta(hours=24)

    rows = []
    for i in range(num_rows):
        inspection_id = _deterministic_uuid("insp", i)
        ts = base_time + timedelta(seconds=i * 86 + rng.randint(0, 30))
        asset_id = f"CNC-{rng.randint(1, 20):03d}"
        site = rng.choice(SITES)
        method = rng.choice(INSPECTION_METHODS)

        # Specification band
        nominal = rng.choice([25.0, 50.0, 75.0, 100.0, 150.0])
        tolerance = rng.choice([0.5, 1.0, 2.0, 5.0])
        spec_upper = nominal + tolerance
        spec_lower = nominal - tolerance

        # Measurement — mostly within spec
        if rng.random() < 0.07:
            # Out-of-spec: offset beyond the tolerance
            direction = rng.choice([-1, 1])
            measurement = nominal + direction * (tolerance + rng.uniform(0.1, tolerance))
        else:
            measurement = round(rng.gauss(nominal, tolerance * 0.3), 4)

        measurement = round(measurement, 4)
        spec_upper = round(spec_upper, 4)
        spec_lower = round(spec_lower, 4)

        pass_fail = "PASS" if spec_lower <= measurement <= spec_upper else "FAIL"

        rows.append(
            Row(
                inspection_id=inspection_id,
                timestamp=ts,
                asset_id=asset_id,
                site=site,
                measurement_um=measurement,
                spec_upper_um=spec_upper,
                spec_lower_um=spec_lower,
                pass_fail=pass_fail,
                inspection_method=method,
            )
        )

    schema = T.StructType(
        [
            T.StructField("inspection_id", T.StringType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("asset_id", T.StringType()),
            T.StructField("site", T.StringType()),
            T.StructField("measurement_um", T.DoubleType()),
            T.StructField("spec_upper_um", T.DoubleType()),
            T.StructField("spec_lower_um", T.DoubleType()),
            T.StructField("pass_fail", T.StringType()),
            T.StructField("inspection_method", T.StringType()),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)
    logger.info("Inspections DataFrame created: %d rows.", df.count())
    return df


# =========================================================================
# Bronze writer (reuses 02_bronze_ingest module when available)
# =========================================================================
def _write_to_bronze(spark: SparkSession, df: DataFrame, table_name: str) -> None:
    """
    Write a DataFrame to the specified bronze table.  Tries to import the
    bronze ingest module for consistency; falls back to direct write.
    """
    full_name = f"{BRONZE}.{table_name}"
    try:
        from lakehouse.bronze_ingest_02 import ingest_dataframe

        ingest_dataframe(spark, table_name, df)
    except ImportError:
        try:
            # Alternative import path (running from repo root)
            sys.path.insert(0, ".")
            from lakehouse import bronze_ingest_02  # type: ignore[import]

            bronze_ingest_02.ingest_dataframe(spark, table_name, df)
        except ImportError:
            logger.info("Direct write to %s (bronze ingest module not on path).", full_name)
            enriched = df.withColumn("_ingested_at", F.current_timestamp())
            enriched.write.format("delta").mode("append").saveAsTable(full_name)


# =========================================================================
# Silver & Gold runners (import from sibling modules)
# =========================================================================
def _run_silver(spark: SparkSession) -> None:
    """Run all silver transforms."""
    try:
        from lakehouse.silver_transform_03 import run_all as silver_run_all

        silver_run_all(spark)
    except ImportError:
        try:
            sys.path.insert(0, ".")
            from lakehouse import silver_transform_03  # type: ignore[import]

            silver_transform_03.run_all(spark)
        except ImportError:
            # Inline execution — import the functions directly from the file
            logger.info("Importing silver transforms inline …")
            import importlib.util

            spec = importlib.util.spec_from_file_location(
                "silver_transform",
                _sibling_path("03_silver_transform.py"),
            )
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                mod.run_all(spark)
            else:
                raise RuntimeError("Cannot locate 03_silver_transform.py")


def _run_gold(spark: SparkSession) -> None:
    """Run all gold aggregations."""
    try:
        from lakehouse.gold_aggregate_04 import run_all as gold_run_all

        gold_run_all(spark)
    except ImportError:
        try:
            sys.path.insert(0, ".")
            from lakehouse import gold_aggregate_04  # type: ignore[import]

            gold_aggregate_04.run_all(spark)
        except ImportError:
            logger.info("Importing gold aggregations inline …")
            import importlib.util

            spec = importlib.util.spec_from_file_location(
                "gold_aggregate",
                _sibling_path("04_gold_aggregate.py"),
            )
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                mod.run_all(spark)
            else:
                raise RuntimeError("Cannot locate 04_gold_aggregate.py")


def _sibling_path(filename: str) -> str:
    """Return the absolute path of a file in the same directory as this script."""
    import os

    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)


# =========================================================================
# Orchestrator
# =========================================================================
def seed_and_run(
    spark: SparkSession,
    num_telemetry: int = 5000,
    num_orders: int = 200,
    num_inspections: int = 1000,
) -> None:
    """
    Full demo pipeline:
      1. Generate synthetic data.
      2. Write to bronze.
      3. Run silver transforms.
      4. Run gold aggregations.
    """
    logger.info("=" * 70)
    logger.info("ManufacturingIQ — Demo Seed & Orchestration")
    logger.info("=" * 70)

    # ------------------------------------------------------------------
    # Step 1: Generate synthetic DataFrames
    # ------------------------------------------------------------------
    logger.info("STEP 1/4 — Generating synthetic data …")
    telemetry_df = generate_iot_telemetry(spark, num_rows=num_telemetry)
    orders_df = generate_erp_orders(spark, num_rows=num_orders)
    inspections_df = generate_inspections(spark, num_rows=num_inspections)

    # ------------------------------------------------------------------
    # Step 2: Write to bronze tables
    # ------------------------------------------------------------------
    logger.info("STEP 2/4 — Writing to bronze tables …")
    _write_to_bronze(spark, telemetry_df, "raw_iot_telemetry")
    _write_to_bronze(spark, orders_df, "raw_erp_orders")
    _write_to_bronze(spark, inspections_df, "raw_inspections")

    # ------------------------------------------------------------------
    # Step 3: Silver transforms
    # ------------------------------------------------------------------
    logger.info("STEP 3/4 — Running silver transforms …")
    _run_silver(spark)

    # ------------------------------------------------------------------
    # Step 4: Gold aggregations
    # ------------------------------------------------------------------
    logger.info("STEP 4/4 — Running gold aggregations …")
    _run_gold(spark)

    logger.info("=" * 70)
    logger.info("Demo seed complete.  All lakehouse layers populated.")
    logger.info("=" * 70)

    # Print summary counts
    _print_summary(spark)


def _print_summary(spark: SparkSession) -> None:
    """Print row counts for every table in the lakehouse."""
    tables = [
        f"{BRONZE}.raw_iot_telemetry",
        f"{BRONZE}.raw_erp_orders",
        f"{BRONZE}.raw_inspections",
        f"{SILVER}.cnc_anomalies",
        f"{SILVER}.enriched_orders",
        f"{SILVER}.tolerance_stats",
        f"{SILVER}.build_tracking",
        f"{GOLD}.production_kpis",
        f"{GOLD}.inventory_forecast",
        f"{GOLD}.site_component_status",
        f"{GOLD}.model_health_metrics",
    ]
    logger.info("-" * 50)
    logger.info("Table row counts:")
    for t in tables:
        try:
            count = spark.table(t).count()
            logger.info("  %-50s %d", t, count)
        except Exception:
            logger.info("  %-50s (not found)", t)
    logger.info("-" * 50)


# =========================================================================
# CLI
# =========================================================================
def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ManufacturingIQ demo data seed and pipeline orchestration."
    )
    parser.add_argument(
        "--num-telemetry",
        type=int,
        default=5000,
        help="Number of synthetic telemetry rows (default: 5000).",
    )
    parser.add_argument(
        "--num-orders",
        type=int,
        default=200,
        help="Number of synthetic ERP order rows (default: 200).",
    )
    parser.add_argument(
        "--num-inspections",
        type=int,
        default=1000,
        help="Number of synthetic inspection rows (default: 1000).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)

    spark = (
        SparkSession.builder.appName("ManufacturingIQ_SeedDemoData")
        .getOrCreate()
    )

    try:
        seed_and_run(
            spark,
            num_telemetry=args.num_telemetry,
            num_orders=args.num_orders,
            num_inspections=args.num_inspections,
        )
    except Exception:
        logger.exception("Demo seed pipeline failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
