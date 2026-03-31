# Databricks notebook source
# MAGIC %md
# MAGIC # I-485 Data Generation Pipeline
# MAGIC Orchestrates all three generators in sequence:
# MAGIC 1. **Clean** — 140K valid applications
# MAGIC 2. **Anomaly** — 60K broken applications
# MAGIC 3. **Fraud** — ~15K records modified with conspiracy patterns
# MAGIC
# MAGIC Total output: **200 000** applications across 20 tables.

# COMMAND ----------

import argparse, os, sys, time

_DEFAULTS = {
    "clean_count": "140000", "anomaly_count": "60000",
    "fraud_count": "15000", "seed": "42", "output_dir": "",
}
try:
    dbutils  # type: ignore[name-defined]
    for k, v in _DEFAULTS.items():
        dbutils.widgets.text(k, v, k)  # type: ignore[name-defined]
    CLEAN_COUNT   = int(dbutils.widgets.get("clean_count"))    # type: ignore[name-defined]
    ANOMALY_COUNT = int(dbutils.widgets.get("anomaly_count"))  # type: ignore[name-defined]
    FRAUD_COUNT   = int(dbutils.widgets.get("fraud_count"))    # type: ignore[name-defined]
    SEED          = int(dbutils.widgets.get("seed"))           # type: ignore[name-defined]
    OUTPUT_DIR    = dbutils.widgets.get("output_dir") or None  # type: ignore[name-defined]
except NameError:
    _p = argparse.ArgumentParser()
    _p.add_argument("--clean-count", type=int,
                    default=int(_DEFAULTS["clean_count"]))
    _p.add_argument("--anomaly-count", type=int,
                    default=int(_DEFAULTS["anomaly_count"]))
    _p.add_argument("--fraud-count", type=int,
                    default=int(_DEFAULTS["fraud_count"]))
    _p.add_argument("--seed", type=int, default=int(_DEFAULTS["seed"]))
    _p.add_argument("--output-dir", type=str, default="")
    _a, _ = _p.parse_known_args()
    CLEAN_COUNT   = _a.clean_count
    ANOMALY_COUNT = _a.anomaly_count
    FRAUD_COUNT   = _a.fraud_count
    SEED          = _a.seed
    OUTPUT_DIR    = _a.output_dir or None

# COMMAND ----------

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline function

# COMMAND ----------

def run_pipeline(clean_count: int = 140_000,
                 anomaly_count: int = 60_000,
                 fraud_count: int = 15_000,
                 seed: int = 42,
                 output_dir: str | None = None):
    """Execute the full I-485 data generation pipeline.

    Parameters
    ----------
    clean_count : int
        Number of clean (valid) applications.
    anomaly_count : int
        Number of anomaly applications.
    fraud_count : int
        Approximate number of records to overlay with fraud patterns.
    seed : int
        Base RNG seed (each generator offsets from this).
    output_dir : str, optional
        Override output directory.
    """
    total = clean_count + anomaly_count
    print("=" * 70)
    print(f"  I-485 Synthetic Data Pipeline")
    print(f"  Clean: {clean_count:,}  |  Anomaly: {anomaly_count:,}  |  "
          f"Fraud overlay: ~{fraud_count:,}")
    print(f"  Total applications: {total:,}  |  Seed: {seed}")
    print("=" * 70)

    t0 = time.time()

    # ── Step 1: Clean ───────────────────────────────────────────────────
    from importlib import import_module
    gen_clean = import_module("01_generate_clean")
    t1 = time.time()
    gen_clean.generate_clean(
        count=clean_count, start_id=1, seed=seed, output_dir=output_dir)
    print(f"  [Step 1 done in {time.time() - t1:.1f}s]\n")

    # ── Step 2: Anomalies ───────────────────────────────────────────────
    gen_anom = import_module("02_generate_anomalies")
    t2 = time.time()
    gen_anom.generate_anomalies(
        count=anomaly_count,
        start_id=clean_count + 1,
        seed=seed + 42,
        output_dir=output_dir,
    )
    print(f"  [Step 2 done in {time.time() - t2:.1f}s]\n")

    # ── Step 3: Fraud overlay ───────────────────────────────────────────
    gen_fraud = import_module("03_generate_fraud")
    t3 = time.time()
    gen_fraud.generate_fraud(
        fraud_count=fraud_count,
        seed=seed + 84,
        output_dir=output_dir,
    )
    print(f"  [Step 3 done in {time.time() - t3:.1f}s]\n")

    # ── Verification ────────────────────────────────────────────────────
    print("=" * 70)
    print("  Verification checks")
    print("=" * 70)
    _verify(output_dir)

    elapsed = time.time() - t0
    print(f"\n  Pipeline complete in {elapsed / 60:.1f} minutes.")


def _verify(output_dir):
    """Quick automated checks on generated data."""
    import pandas as pd
    from config_i485 import load_i485_table

    try:
        app = load_i485_table("application", output_dir)
        info = load_i485_table("applicant_info", output_dir)
        addr = load_i485_table("addresses", output_dir)
    except FileNotFoundError as e:
        print(f"  SKIP verification — {e}")
        return

    n_app = len(app)
    print(f"  Total applications:        {n_app:,}")
    print(f"  Total applicant_info rows:  {len(info):,}")
    print(f"  Total address rows:         {len(addr):,}")

    # FK check: child table IDs exist in application
    child_ids = set(info["application_id"].unique())
    parent_ids = set(app["application_id"].unique())
    orphans = child_ids - parent_ids
    if orphans:
        print(f"  WARNING: {len(orphans)} orphaned applicant_info rows")
    else:
        print("  FK check (applicant_info → application): OK")

    # Anomaly manifest
    from config_i485 import I485_CSV
    csv_dir = os.path.join(output_dir, "csv") if output_dir else I485_CSV
    anom_path = os.path.join(csv_dir, "_anomaly_manifest.csv")
    if os.path.exists(anom_path):
        am = pd.read_csv(anom_path)
        print(f"  Anomaly manifest entries:   {len(am):,}")
    else:
        print("  Anomaly manifest: not found")

    fraud_path = os.path.join(csv_dir, "_fraud_manifest.csv")
    if os.path.exists(fraud_path):
        fm = pd.read_csv(fraud_path)
        print(f"  Fraud manifest entries:     {len(fm):,}")
    else:
        print("  Fraud manifest: not found")

# COMMAND ----------

if __name__ == "__main__":
    run_pipeline(clean_count=CLEAN_COUNT, anomaly_count=ANOMALY_COUNT,
                 fraud_count=FRAUD_COUNT, seed=SEED, output_dir=OUTPUT_DIR)
