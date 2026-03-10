"""
Config-driven synthetic data generator for ManufacturingIQ.

Reads parameters from ``config/manufacturing.yaml`` and writes realistic
synthetic data into bronze-layer Delta tables in Unity Catalog.

Supports two execution modes:
  - **full**        : Truncate-and-load.  Generates a complete day's worth
                      of records.
  - **incremental** : Append-only.  Generates a small micro-batch
                      (roughly 1/24 of a full day) suitable for streaming
                      simulation or scheduled refreshes.

Usage (from the repo root on a Databricks cluster)::

    python -m data.generate_synthetic --use-case manufacturing --mode full
    python -m data.generate_synthetic --use-case manufacturing --mode incremental
"""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data.schemas import (
    BUILD_TRACKING_SCHEMA,
    RAW_ERP_ORDERS_SCHEMA,
    RAW_INSPECTIONS_SCHEMA,
    RAW_IOT_TELEMETRY_SCHEMA,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ManufacturingIQ.SyntheticGenerator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_INCREMENTAL_FRACTION = 1 / 24  # one hour's worth of a full day
_BERLIN_ANOMALY_MACHINE = "CNC-Milling-BER-04"
_BERLIN_ANOMALY_RATE_MULTIPLIER = 5.0  # 5x the baseline anomaly rate


# ===================================================================
# Configuration helpers
# ===================================================================

def _resolve_config_path(config_rel: str = "config/manufacturing.yaml") -> Path:
    """Resolve config path relative to the repository root."""
    # Try the working directory first, then walk upward.
    cwd = Path.cwd()
    candidate = cwd / config_rel
    if candidate.is_file():
        return candidate
    # Fallback: relative to this file's location (data/ -> repo root).
    repo_root = Path(__file__).resolve().parent.parent
    candidate = repo_root / config_rel
    if candidate.is_file():
        return candidate
    raise FileNotFoundError(
        f"Cannot locate config file '{config_rel}' from {cwd} or {repo_root}"
    )


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load and return the YAML configuration dictionary."""
    path = Path(config_path) if config_path else _resolve_config_path()
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh)
    logger.info("Loaded config from %s", path)
    return cfg


# ===================================================================
# Spark session
# ===================================================================

def get_spark() -> SparkSession:
    """Return or create the SparkSession for the generator."""
    return (
        SparkSession.builder
        .appName("ManufacturingIQ-SyntheticGenerator")
        .getOrCreate()
    )


# ===================================================================
# Deterministic helpers
# ===================================================================

def _uuid() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _random_timestamps(
    rng: np.random.Generator,
    start: datetime,
    end: datetime,
    n: int,
) -> List[datetime]:
    """Return *n* uniformly distributed timestamps in [start, end)."""
    start_ts = start.timestamp()
    end_ts = end.timestamp()
    ts_arr = rng.uniform(start_ts, end_ts, size=n)
    return [datetime.fromtimestamp(t, tz=timezone.utc) for t in np.sort(ts_arr)]


def _pick(rng: np.random.Generator, choices: List[str]) -> str:
    return choices[rng.integers(0, len(choices))]


def _expand_machines(cfg: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Return a flat list of (site, machine_id) pairs from config."""
    pairs: List[Tuple[str, str]] = []
    for site, machines in cfg["data"]["machine_types"].items():
        for m in machines:
            pairs.append((site, m))
    return pairs


# ===================================================================
# Core generators
# ===================================================================

def generate_telemetry(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 42,
) -> DataFrame:
    """Generate synthetic IoT telemetry rows for ``bronze.raw_iot_telemetry``.

    The Berlin machine ``CNC-Milling-BER-04`` is tagged with a higher
    anomaly rate to match the mockup scenario.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed for reproducibility.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_IOT_TELEMETRY_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    telem_cfg = cfg["data"]["telemetry"]
    records_per_day: int = telem_cfg["records_per_day"]
    base_anomaly_rate: float = telem_cfg["anomaly_rate"]
    normal = telem_cfg["normal_ranges"]
    anomaly = telem_cfg["anomaly_ranges"]

    n_records = records_per_day
    if mode == "incremental":
        n_records = int(records_per_day * _INCREMENTAL_FRACTION)

    machine_pairs = _expand_machines(cfg)
    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    timestamps = _random_timestamps(rng, start, now, n_records)

    rows: List[Dict[str, Any]] = []
    for i in range(n_records):
        site, machine_id = machine_pairs[rng.integers(0, len(machine_pairs))]

        # Determine anomaly for this reading.
        effective_rate = base_anomaly_rate
        if machine_id == _BERLIN_ANOMALY_MACHINE:
            effective_rate = min(base_anomaly_rate * _BERLIN_ANOMALY_RATE_MULTIPLIER, 1.0)

        is_anomaly = rng.random() < effective_rate
        ranges = anomaly if is_anomaly else normal

        row = {
            "event_id": _uuid(),
            "timestamp": timestamps[i],
            "site": site,
            "machine_id": machine_id,
            "vibration_hz": float(rng.uniform(*ranges["vibration_hz"])),
            "temp_c": float(rng.uniform(*ranges["temp_c"])),
            "spindle_rpm": float(rng.uniform(*ranges["spindle_rpm"])),
            "tool_wear_index": float(rng.uniform(*ranges["tool_wear_index"])),
            "feed_rate": float(rng.uniform(*ranges["feed_rate"])),
            "ingestion_ts": now,
            "source_file": f"iot_batch_{now.strftime('%Y%m%d_%H%M%S')}.json",
        }
        rows.append(row)

    logger.info(
        "Generated %d telemetry rows (mode=%s, window=%dh)",
        len(rows), mode, window_hours,
    )
    return spark.createDataFrame(rows, schema=RAW_IOT_TELEMETRY_SCHEMA)


def generate_inspections(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 43,
) -> DataFrame:
    """Generate synthetic inspection rows for ``bronze.raw_inspections``.

    Produces ~2.85 M records per full day to match the mockup numbers.
    Out-of-spec parts are injected at the configured rate, with a higher
    deviation applied to parts machined by ``CNC-Milling-BER-04``.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_INSPECTIONS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    insp_cfg = cfg["data"]["inspections"]
    records_per_day: int = insp_cfg["records_per_day"]
    oos_rate: float = insp_cfg["out_of_spec_rate"]
    lower_spec, upper_spec = insp_cfg["spec_range_um"]
    normal_std: float = insp_cfg["normal_deviation_std"]
    anomaly_std: float = insp_cfg["anomaly_deviation_std"]

    n_records = records_per_day
    if mode == "incremental":
        n_records = int(records_per_day * _INCREMENTAL_FRACTION)

    machine_pairs = _expand_machines(cfg)
    known_batches = [b["batch_id"] for b in cfg["data"]["build_tracking"]["known_batches"]]

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)

    # --- vectorized generation for performance at 2.85 M scale ---
    spec_mid = (lower_spec + upper_spec) / 2.0

    # Pre-draw random indices for machines.
    machine_indices = rng.integers(0, len(machine_pairs), size=n_records)
    # Pre-draw out-of-spec flags.
    oos_flags = rng.random(size=n_records) < oos_rate
    # Boost OOS rate for the Berlin anomaly machine.
    berlin_anomaly_idx = [
        i for i, mi in enumerate(machine_indices)
        if machine_pairs[mi][1] == _BERLIN_ANOMALY_MACHINE
    ]
    if berlin_anomaly_idx:
        boost_flags = rng.random(size=len(berlin_anomaly_idx)) < (oos_rate * _BERLIN_ANOMALY_RATE_MULTIPLIER)
        for j, idx in enumerate(berlin_anomaly_idx):
            oos_flags[idx] = oos_flags[idx] or boost_flags[j]

    # Generate measurements: normal distribution centered on spec_mid.
    std_arr = np.where(oos_flags, anomaly_std, normal_std)
    measurements = rng.normal(loc=spec_mid, scale=std_arr)

    # Timestamps: sorted uniform
    ts_floats = np.sort(rng.uniform(start.timestamp(), now.timestamp(), size=n_records))

    # Batch assignment: mostly random known batches + new ones.
    extra_batches = [f"GEN-{rng.integers(10000, 99999)}-{chr(rng.integers(65, 91))}" for _ in range(20)]
    all_batches = known_batches + extra_batches
    batch_indices = rng.integers(0, len(all_batches), size=n_records)

    logger.info(
        "Assembling %d inspection rows (mode=%s) — this may take a moment...",
        n_records, mode,
    )

    rows: List[Dict[str, Any]] = []
    for i in range(n_records):
        site, machine_id = machine_pairs[machine_indices[i]]
        meas = float(measurements[i])
        in_spec = lower_spec <= meas <= upper_spec

        rows.append({
            "inspection_id": _uuid(),
            "timestamp": datetime.fromtimestamp(ts_floats[i], tz=timezone.utc),
            "site": site,
            "machine_id": machine_id,
            "batch_id": all_batches[batch_indices[i]],
            "part_serial": f"P-{rng.integers(100000, 999999)}",
            "measurement_um": meas,
            "lower_spec_um": lower_spec,
            "upper_spec_um": upper_spec,
            "in_spec": bool(in_spec),
            "ingestion_ts": now,
        })

    logger.info("Generated %d inspection rows", len(rows))
    return spark.createDataFrame(rows, schema=RAW_INSPECTIONS_SCHEMA)


def generate_orders(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 44,
) -> DataFrame:
    """Generate synthetic ERP order rows for ``bronze.raw_erp_orders``.

    Injects the three known batch IDs from config (B-9982-XYZ, A-1102-MDF,
    C-4421-ALP) alongside randomly generated orders.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_ERP_ORDERS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    build_cfg = cfg["data"]["build_tracking"]
    batches_per_day: int = build_cfg["batches_per_day"]
    known_batches = build_cfg["known_batches"]
    sites: List[str] = cfg["data"]["sites"]

    n_orders = batches_per_day
    if mode == "incremental":
        n_orders = max(int(batches_per_day * _INCREMENTAL_FRACTION), 5)

    now = _utcnow()
    product_lines = ["Alpha-9", "Beta-5", "Gamma-3", "Delta-X", "Sigma-1"]
    priorities = ["Standard", "High", "Urgent"]
    statuses = ["Open", "In Progress", "Complete", "Cancelled"]

    rows: List[Dict[str, Any]] = []

    # --- inject known batches first ---
    for kb in known_batches:
        order_qty = int(rng.integers(500, 5000))
        fulfilled = order_qty if kb["status"] == "Complete" else int(rng.integers(0, order_qty))
        created = now - timedelta(days=int(rng.integers(1, 15)))
        due = created + timedelta(days=int(rng.integers(7, 30)))
        status_map = {"Complete": "Complete", "Defect": "In Progress", "In Progress": "In Progress"}
        rows.append({
            "order_id": f"ORD-{_uuid()[:8].upper()}",
            "batch_id": kb["batch_id"],
            "site": kb["site"],
            "product_line": _pick(rng, product_lines),
            "order_qty": order_qty,
            "fulfilled_qty": fulfilled,
            "order_status": status_map.get(kb["status"], "Open"),
            "created_at": created,
            "due_date": due,
            "priority": _pick(rng, priorities),
            "ingestion_ts": now,
        })

    # --- fill remaining orders ---
    for _ in range(n_orders - len(known_batches)):
        site = _pick(rng, sites)
        order_qty = int(rng.integers(100, 5000))
        status = _pick(rng, statuses)
        fulfilled = order_qty if status == "Complete" else int(rng.integers(0, order_qty))
        created = now - timedelta(days=int(rng.integers(0, 30)))
        due = created + timedelta(days=int(rng.integers(7, 45)))
        rows.append({
            "order_id": f"ORD-{_uuid()[:8].upper()}",
            "batch_id": f"GEN-{rng.integers(1000, 9999)}-{chr(rng.integers(65, 91))}{chr(rng.integers(65, 91))}{chr(rng.integers(65, 91))}",
            "site": site,
            "product_line": _pick(rng, product_lines),
            "order_qty": order_qty,
            "fulfilled_qty": fulfilled,
            "order_status": status,
            "created_at": created,
            "due_date": due,
            "priority": _pick(rng, priorities),
            "ingestion_ts": now,
        })

    logger.info("Generated %d ERP order rows (mode=%s)", len(rows), mode)
    return spark.createDataFrame(rows, schema=RAW_ERP_ORDERS_SCHEMA)


def generate_build_events(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 45,
) -> DataFrame:
    """Generate synthetic build-stage events for ``silver.build_tracking``.

    Each batch passes through a sequence of stations. The known batches are
    given realistic stage progressions; ``A-1102-MDF`` is tagged with a
    vibration-anomaly defect.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`BUILD_TRACKING_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    build_cfg = cfg["data"]["build_tracking"]
    batches_per_day: int = build_cfg["batches_per_day"]
    defect_rate: float = build_cfg["defect_rate"]
    stations: List[str] = build_cfg["stations"]
    known_batches = build_cfg["known_batches"]
    sites: List[str] = cfg["data"]["sites"]

    n_batches = batches_per_day
    if mode == "incremental":
        n_batches = max(int(batches_per_day * _INCREMENTAL_FRACTION), 5)

    now = _utcnow()
    rows: List[Dict[str, Any]] = []

    def _build_stages(
        batch_id: str,
        site: str,
        status: str,
        defect_flag: Optional[str],
        batch_start: datetime,
    ) -> None:
        """Append stage rows for one batch."""
        units = int(rng.integers(200, 2000))
        current_units = units
        # Determine how many stages this batch has completed.
        if status == "Complete":
            completed_stages = len(stations)
        elif status == "In Progress":
            completed_stages = rng.integers(1, len(stations))
        else:
            completed_stages = len(stations)

        stage_time = batch_start
        for seq, station in enumerate(stations):
            if seq >= completed_stages:
                break

            cycle = float(rng.uniform(45, 300))
            stage_end = stage_time + timedelta(seconds=cycle * current_units / 100)
            defects = 0
            row_defect_flag: Optional[str] = None

            # Inject defects.
            if defect_flag and station == "Quality Check":
                defects = int(rng.integers(5, 30))
                row_defect_flag = defect_flag
            elif rng.random() < defect_rate:
                defects = int(rng.integers(1, 10))
                row_defect_flag = _pick(rng, [
                    "Surface finish deviation",
                    "Dimensional tolerance breach",
                    "Material inconsistency",
                ])

            units_out = max(current_units - defects, 0)
            yield_pct = (units_out / current_units * 100) if current_units > 0 else 0.0

            stage_status = "Complete"
            completed_at = stage_end
            if seq == completed_stages - 1 and status == "In Progress":
                stage_status = "In Progress"
                completed_at = None

            rows.append({
                "event_id": _uuid(),
                "batch_id": batch_id,
                "site": site,
                "station": station,
                "stage_seq": seq + 1,
                "units_in": current_units,
                "units_out": units_out,
                "defect_count": defects,
                "yield_pct": round(yield_pct, 2),
                "cycle_time_sec": round(cycle, 2),
                "status": stage_status,
                "defect_flag": row_defect_flag,
                "started_at": stage_time,
                "completed_at": completed_at,
            })

            current_units = units_out
            stage_time = stage_end

    # --- Known batches ---
    for kb in known_batches:
        batch_start = now - timedelta(hours=float(rng.uniform(2, 12)))
        _build_stages(
            batch_id=kb["batch_id"],
            site=kb["site"],
            status=kb["status"],
            defect_flag=kb.get("defect_flag"),
            batch_start=batch_start,
        )

    # --- Random batches ---
    n_random = n_batches - len(known_batches)
    for _ in range(max(n_random, 0)):
        site = _pick(rng, sites)
        batch_id = f"GEN-{rng.integers(1000, 9999)}-{chr(rng.integers(65, 91))}{chr(rng.integers(65, 91))}{chr(rng.integers(65, 91))}"
        status = _pick(rng, ["Complete", "In Progress", "Complete", "Complete"])
        has_defect = rng.random() < defect_rate
        defect_flag = "Micro-stoppage detected" if has_defect else None
        batch_start = now - timedelta(hours=float(rng.uniform(0.5, 24)))
        _build_stages(batch_id, site, status, defect_flag, batch_start)

    logger.info("Generated %d build-tracking rows from %d batches (mode=%s)", len(rows), n_batches, mode)
    return spark.createDataFrame(rows, schema=BUILD_TRACKING_SCHEMA)


# ===================================================================
# Writer
# ===================================================================

def _write_delta(
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    mode: str,
) -> None:
    """Write a DataFrame to a Unity Catalog Delta table.

    Args:
        df:      DataFrame to persist.
        catalog: UC catalog name (e.g. ``manufacturing_iq``).
        schema:  UC schema / database name (e.g. ``bronze``).
        table:   Table name (e.g. ``raw_iot_telemetry``).
        mode:    ``"full"`` maps to overwrite; ``"incremental"`` maps to append.
    """
    fqn = f"{catalog}.{schema}.{table}"
    write_mode = "overwrite" if mode == "full" else "append"

    logger.info("Writing to %s (mode=%s) ...", fqn, write_mode)

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true" if write_mode == "overwrite" else "false")
        .saveAsTable(fqn)
    )

    count = df.count()
    logger.info("Wrote %d rows to %s", count, fqn)


def _ensure_schemas_exist(spark: SparkSession, catalog: str) -> None:
    """Create the catalog and medallion schemas if they do not exist."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    logger.info("Ensured catalog '%s' and schemas bronze/silver/gold exist", catalog)


# ===================================================================
# Orchestrator
# ===================================================================

def run_generation(
    cfg: Dict[str, Any],
    mode: str = "full",
) -> None:
    """Run the full synthetic-data generation pipeline.

    Generates telemetry, inspections, orders, and build events, then
    writes them to the corresponding bronze / silver Delta tables.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
    """
    spark = get_spark()
    catalog: str = cfg["app"]["catalog"]

    logger.info(
        "=== ManufacturingIQ Synthetic Data Generation ===  catalog=%s  mode=%s",
        catalog, mode,
    )

    _ensure_schemas_exist(spark, catalog)

    # --- Bronze: telemetry ---
    telemetry_df = generate_telemetry(cfg, mode=mode)
    _write_delta(telemetry_df, catalog, "bronze", "raw_iot_telemetry", mode)

    # --- Bronze: inspections ---
    inspections_df = generate_inspections(cfg, mode=mode)
    _write_delta(inspections_df, catalog, "bronze", "raw_inspections", mode)

    # --- Bronze: ERP orders ---
    orders_df = generate_orders(cfg, mode=mode)
    _write_delta(orders_df, catalog, "bronze", "raw_erp_orders", mode)

    # --- Silver: build tracking (directly generated, not transformed) ---
    build_df = generate_build_events(cfg, mode=mode)
    _write_delta(build_df, catalog, "silver", "build_tracking", mode)

    logger.info("=== Generation complete ===")


# ===================================================================
# CLI entry point
# ===================================================================

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ManufacturingIQ synthetic data generator",
    )
    parser.add_argument(
        "--use-case",
        type=str,
        default="manufacturing",
        help="Use-case identifier (default: manufacturing). "
             "Reserved for multi-use-case support.",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["full", "incremental"],
        default="full",
        help="Generation mode: 'full' (truncate & load) or 'incremental' (append).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to the YAML config file. Defaults to config/manufacturing.yaml.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logger.info(
        "Starting generator — use_case=%s  mode=%s  config=%s",
        args.use_case, args.mode, args.config or "(auto-detect)",
    )
    config = load_config(args.config)
    run_generation(config, mode=args.mode)
