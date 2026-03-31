# Databricks notebook source
# MAGIC %md
# MAGIC # Load I-485 Synthetic Data to Databricks
# MAGIC Uploads generated Parquet files to a Unity Catalog volume and registers
# MAGIC them as Delta tables in `lho_ucm.i485_form`.
# MAGIC
# MAGIC Run **after** the generation pipeline (04_run_pipeline).

# COMMAND ----------

import argparse, os, sys, time

_DEFAULTS = {
    "catalog": "lho_ucm", "schema": "i485_form",
    "output_dir": "", "truncate_first": "true",
}
try:
    dbutils  # type: ignore[name-defined]
    for k, v in _DEFAULTS.items():
        dbutils.widgets.text(k, v, k)  # type: ignore[name-defined]
    CATALOG   = dbutils.widgets.get("catalog")         # type: ignore[name-defined]
    SCHEMA    = dbutils.widgets.get("schema")           # type: ignore[name-defined]
    OUTPUT_DIR = dbutils.widgets.get("output_dir") or None  # type: ignore[name-defined]
    TRUNCATE  = dbutils.widgets.get("truncate_first").lower() == "true"  # type: ignore[name-defined]
except NameError:
    _p = argparse.ArgumentParser()
    _p.add_argument("--catalog", default=_DEFAULTS["catalog"])
    _p.add_argument("--schema", default=_DEFAULTS["schema"])
    _p.add_argument("--output-dir", default="")
    _p.add_argument("--truncate-first", default=_DEFAULTS["truncate_first"])
    _a, _ = _p.parse_known_args()
    CATALOG   = _a.catalog
    SCHEMA    = _a.schema
    OUTPUT_DIR = _a.output_dir or None
    TRUNCATE  = _a.truncate_first.lower() == "true"

# COMMAND ----------

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_i485 import I485_PQ

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loader function

# COMMAND ----------

VOLUME_BASE = f"/Volumes/{_DEFAULTS['catalog']}/{_DEFAULTS['schema']}/staging_data"

# Tables to load (skip manifest files only)
SKIP_PREFIXES = ("_",)

# Load order: ref table first, then application (root), then all children
LOAD_ORDER = [
    "ref_filing_categories",  # reference/lookup table (52 rows)
    "application",            # root — all others FK to this
    "applicant_info", "addresses", "other_names",
    "filing_category", "affidavit_exemption", "additional_info",
    "employment_history", "parents", "marital_history", "children",
    "biographic_info", "eligibility_responses", "organizations",
    "public_charge", "benefits_received", "institutionalization",
    "contacts_signatures", "interview_signature",
    "additional_information",
]


def _execute(cursor, sql, label=""):
    try:
        cursor.execute(sql)
        if label:
            print(f"    OK: {label}")
        return True
    except Exception as e:
        print(f"    FAIL: {label} — {e}")
        return False


def load_to_databricks(catalog: str = "lho_ucm",
                       schema: str = "i485_form",
                       output_dir: str | None = None,
                       truncate_first: bool = True):
    """Upload I-485 Parquet files to Databricks and register as Delta tables.

    Parameters
    ----------
    catalog : str
        Unity Catalog name.
    schema : str
        Schema (database) within the catalog.
    output_dir : str, optional
        Override the local Parquet directory.
    truncate_first : bool
        If True, truncate existing tables before loading.
    """
    from databricks import sql as dbsql
    from databricks.sdk import WorkspaceClient

    pq_dir = os.path.join(output_dir, "parquet") if output_dir else I485_PQ
    volume_base = f"/Volumes/{catalog}/{schema}/staging_data"

    print(f"=== Loading I-485 data to {catalog}.{schema} ===")
    print(f"  Source: {pq_dir}")
    print(f"  Volume: {volume_base}")

    # ── Connect ─────────────────────────────────────────────────────────
    w = WorkspaceClient(profile="planxs")
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("ERROR: No SQL warehouses found")
        return
    wh = warehouses[0]
    print(f"  Warehouse: {wh.name} ({wh.id})")

    if str(wh.state) != "State.RUNNING":
        print("  Starting warehouse …")
        w.warehouses.start(wh.id)
        for _ in range(60):
            wh = w.warehouses.get(wh.id)
            if str(wh.state) == "State.RUNNING":
                break
            time.sleep(5)

    host = w.config.host.rstrip("/")
    server = host.replace("https://", "")
    headers = w.config.authenticate()
    token = headers.get("Authorization", "").replace("Bearer ", "")

    import ssl, certifi
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    conn = dbsql.connect(server_hostname=server,
                         http_path=f"/sql/1.0/warehouses/{wh.id}",
                         access_token=token,
                         _tls_verify_hostname=True,
                         _tls_trusted_ca_file=certifi.where())
    cursor = conn.cursor()

    _execute(cursor, f"USE CATALOG {catalog}", "USE CATALOG")
    _execute(cursor, f"USE SCHEMA {schema}", "USE SCHEMA")

    # ── Upload & register tables ────────────────────────────────────────
    success, fail = 0, 0
    for table_name in LOAD_ORDER:
        pq_file = os.path.join(pq_dir, f"{table_name}.parquet")
        if not os.path.exists(pq_file):
            print(f"    SKIP: {table_name}.parquet not found locally")
            continue

        # Upload to volume
        vol_path = f"{volume_base}/parquet/{table_name}.parquet"
        print(f"  Uploading {table_name} …")
        try:
            with open(pq_file, "rb") as f:
                w.files.upload(vol_path, f, overwrite=True)
        except Exception as e:
            print(f"    FAIL upload: {e}")
            fail += 1
            continue

        # Truncate if requested
        if truncate_first:
            _execute(cursor, f"DROP TABLE IF EXISTS {table_name}",
                     f"DROP {table_name}")

        # Register as Delta table
        sql = f"""
        CREATE OR REPLACE TABLE {table_name}
        AS SELECT * FROM read_files(
            '{vol_path}',
            format => 'parquet'
        )
        """
        if _execute(cursor, sql, table_name):
            success += 1
        else:
            fail += 1

    # ── Summary ─────────────────────────────────────────────────────────
    print(f"\n  Tables loaded: {success}, failed: {fail}")

    total_rows = 0
    for table_name in LOAD_ORDER:
        try:
            cursor.execute(
                f"SELECT COUNT(*) FROM {catalog}.{schema}.{table_name}")
            cnt = cursor.fetchone()[0]
            total_rows += cnt
            print(f"    {table_name}: {cnt:,} rows")
        except Exception:
            pass
    print(f"\n  Total rows: {total_rows:,}")

    cursor.close()
    conn.close()
    print("  Done.")

# COMMAND ----------

if __name__ == "__main__":
    load_to_databricks(catalog=CATALOG, schema=SCHEMA,
                       output_dir=OUTPUT_DIR,
                       truncate_first=TRUNCATE)
