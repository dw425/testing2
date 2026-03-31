# Databricks notebook source
# MAGIC %md
# MAGIC # Generator 2 — Anomaly I-485 Records
# MAGIC Generates **60 000** applications with randomised data-quality problems.
# MAGIC Each record receives 1–5 anomaly types from a 30-type catalog.
# MAGIC
# MAGIC Run **after** Generator 1 (01_generate_clean).

# COMMAND ----------

import argparse, os, sys

_DEFAULTS = {"count": "60000", "start_id": "140001", "seed": "84",
             "output_dir": ""}
try:
    dbutils  # type: ignore[name-defined]
    for k, v in _DEFAULTS.items():
        dbutils.widgets.text(k, v, k)  # type: ignore[name-defined]
    COUNT      = int(dbutils.widgets.get("count"))       # type: ignore[name-defined]
    START_ID   = int(dbutils.widgets.get("start_id"))    # type: ignore[name-defined]
    SEED       = int(dbutils.widgets.get("seed"))        # type: ignore[name-defined]
    OUTPUT_DIR = dbutils.widgets.get("output_dir") or None  # type: ignore[name-defined]
except NameError:
    _p = argparse.ArgumentParser()
    _p.add_argument("--count", type=int, default=int(_DEFAULTS["count"]))
    _p.add_argument("--start-id", type=int, default=int(_DEFAULTS["start_id"]))
    _p.add_argument("--seed", type=int, default=int(_DEFAULTS["seed"]))
    _p.add_argument("--output-dir", type=str, default="")
    _a, _ = _p.parse_known_args()
    COUNT, START_ID, SEED = _a.count, _a.start_id, _a.seed
    OUTPUT_DIR = _a.output_dir or None

# COMMAND ----------

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from faker import Faker

from config_i485 import (
    append_i485_table, save_i485_table, I485_CSV, I485_PQ,
    CATEGORY_CODES, CATEGORY_WEIGHTS, CATEGORY_GROUPS,
    CATEGORY_DESCRIPTIONS, AFFIDAVIT_EXEMPT_CATEGORIES,
    AFFIDAVIT_REASON_CODES, ORG_TYPES, BENEFIT_TYPES,
)
from profiles import build_profiles
from utils import pick, gen_timestamps

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly catalog — 30 types across 8 categories

# COMMAND ----------

ANOMALY_TYPES = [
    # A. Missing required fields
    "MISSING_NAME", "MISSING_DOB", "MISSING_COUNTRY",
    "MISSING_ADDRESS", "MISSING_CATEGORY", "MISSING_SIGNATURE",
    # B. Invalid formats
    "BAD_SSN", "BAD_A_NUMBER", "BAD_RECEIPT", "BAD_DATE_FORMAT", "BAD_ZIP",
    # C. Date logic violations
    "FUTURE_DOB", "ARRIVAL_AFTER_FILING", "PASSPORT_EXPIRED_BEFORE_FILING",
    "CHILD_OLDER_THAN_PARENT",
    # D. Contradictory data
    "SSN_CONTRADICTION", "MARRIED_NO_SPOUSE", "SINGLE_WITH_SPOUSE",
    "NO_CHILDREN_BUT_LISTED", "CREWMAN_CONTRADICTION",
    # E. Out-of-range values
    "EXTREME_HEIGHT", "EXTREME_WEIGHT", "EXTREME_AGE", "EXTREME_CHILDREN",
    # F. Orphaned / inconsistent references
    "WRONG_CATEGORY_GROUP", "MISMATCHED_NAMES", "DUPLICATE_A_NUMBER",
    # G. Eligibility contradictions
    "ALL_YES_SECURITY", "APPROVED_WITH_BARS",
    # H. Completeness issues
    "SPARSE_RECORD",
]

# How many anomalies per record (weighted: 1=30%, 2=30%, 3=20%, 4=15%, 5=5%)
_ANOM_COUNT_OPTS = [1, 2, 3, 4, 5]
_ANOM_COUNT_W    = [0.30, 0.30, 0.20, 0.15, 0.05]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly injection functions

# COMMAND ----------

def _inject_anomalies(tables: dict[str, pd.DataFrame],
                      app_id: int, anomalies: list[str],
                      rng: np.random.Generator,
                      manifest: list[dict]) -> None:
    """Mutate *tables* in-place for a single application."""

    def _log(atype, table, column):
        manifest.append({"application_id": app_id,
                         "anomaly_type": atype,
                         "affected_table": table,
                         "affected_column": column})

    app_mask   = tables["application"]["application_id"] == app_id
    info_mask  = tables["applicant_info"]["application_id"] == app_id
    bio_mask   = tables["biographic_info"]["application_id"] == app_id
    fc_mask    = tables["filing_category"]["application_id"] == app_id
    elig_mask  = tables["eligibility_responses"]["application_id"] == app_id

    for atype in anomalies:

        # ── A. Missing required fields ──────────────────────────────────
        if atype == "MISSING_NAME":
            col = str(rng.choice(["family_name", "given_name"]))
            tables["applicant_info"].loc[info_mask, col] = None
            _log(atype, "applicant_info", col)

        elif atype == "MISSING_DOB":
            tables["applicant_info"].loc[info_mask, "date_of_birth"] = None
            _log(atype, "applicant_info", "date_of_birth")

        elif atype == "MISSING_COUNTRY":
            col = str(rng.choice(["country_of_birth",
                                  "country_of_citizenship"]))
            tables["applicant_info"].loc[info_mask, col] = None
            _log(atype, "applicant_info", col)

        elif atype == "MISSING_ADDRESS":
            addr_mask = ((tables["addresses"]["application_id"] == app_id)
                         & (tables["addresses"]["address_type"]
                            == "CURRENT_PHYSICAL"))
            tables["addresses"] = tables["addresses"][~addr_mask]
            _log(atype, "addresses", "CURRENT_PHYSICAL row removed")

        elif atype == "MISSING_CATEGORY":
            tables["filing_category"].loc[fc_mask, "category_code"] = None
            _log(atype, "filing_category", "category_code")

        elif atype == "MISSING_SIGNATURE":
            cs_mask = ((tables["contacts_signatures"]["application_id"]
                        == app_id)
                       & (tables["contacts_signatures"]["contact_type"]
                          == "APPLICANT"))
            tables["contacts_signatures"] = (
                tables["contacts_signatures"][~cs_mask])
            _log(atype, "contacts_signatures", "APPLICANT row removed")

        # ── B. Invalid formats ──────────────────────────────────────────
        elif atype == "BAD_SSN":
            bad = str(rng.choice([
                "000-00-0000", "999-99-9999", "123-45-6789",
                "12345678", "123-456-789", "SSN"]))
            tables["applicant_info"].loc[info_mask, "ssn"] = bad
            tables["applicant_info"].loc[info_mask, "has_ssn"] = True
            _log(atype, "applicant_info", "ssn")

        elif atype == "BAD_A_NUMBER":
            bad = str(rng.choice([
                "12345", "AXXXXXXXXX", "A12", "A12345678901", ""]))
            tables["applicant_info"].loc[info_mask, "a_number"] = bad
            tables["application"].loc[app_mask, "a_number"] = bad
            _log(atype, "applicant_info", "a_number")

        elif atype == "BAD_RECEIPT":
            bad = str(rng.choice(["XX1234", "1234567890", "IOE", "ZZZ0"]))
            tables["application"].loc[app_mask, "receipt_number"] = bad
            _log(atype, "application", "receipt_number")

        elif atype == "BAD_DATE_FORMAT":
            bad = str(rng.choice([
                "13/45/2025", "2025-13-01", "00/00/0000", "not-a-date"]))
            tables["applicant_info"].loc[info_mask, "date_of_birth"] = bad
            _log(atype, "applicant_info", "date_of_birth")

        elif atype == "BAD_ZIP":
            bad = str(rng.choice(["123", "1234567", "ABCDE", "0"]))
            addr_mask = ((tables["addresses"]["application_id"] == app_id)
                         & (tables["addresses"]["address_type"]
                            == "CURRENT_PHYSICAL"))
            tables["addresses"].loc[addr_mask, "zip_code"] = bad
            _log(atype, "addresses", "zip_code")

        # ── C. Date logic violations ────────────────────────────────────
        elif atype == "FUTURE_DOB":
            tables["applicant_info"].loc[info_mask, "date_of_birth"] = (
                "2030-06-15")
            _log(atype, "applicant_info", "date_of_birth")

        elif atype == "ARRIVAL_AFTER_FILING":
            filing = tables["application"].loc[
                app_mask, "filing_date"].iloc[0]
            new_date = pd.Timestamp(filing) + pd.Timedelta(days=365)
            tables["applicant_info"].loc[info_mask, "arrival_date"] = (
                str(new_date.date()))
            _log(atype, "applicant_info", "arrival_date")

        elif atype == "PASSPORT_EXPIRED_BEFORE_FILING":
            filing = tables["application"].loc[
                app_mask, "filing_date"].iloc[0]
            new_date = pd.Timestamp(filing) - pd.Timedelta(days=180)
            tables["applicant_info"].loc[
                info_mask, "passport_expiration"] = str(new_date.date())
            _log(atype, "applicant_info", "passport_expiration")

        elif atype == "CHILD_OLDER_THAN_PARENT":
            par_mask = ((tables["parents"]["application_id"] == app_id)
                        & (tables["parents"]["parent_number"] == 1))
            if par_mask.any():
                tables["parents"].loc[par_mask, "date_of_birth"] = (
                    "2010-01-01")
                _log(atype, "parents", "date_of_birth")

        # ── D. Contradictory data ───────────────────────────────────────
        elif atype == "SSN_CONTRADICTION":
            has = tables["applicant_info"].loc[info_mask, "has_ssn"].iloc[0]
            if has:
                tables["applicant_info"].loc[info_mask, "ssn"] = None
            else:
                tables["applicant_info"].loc[info_mask, "ssn"] = "999-88-7777"
            _log(atype, "applicant_info", "ssn/has_ssn")

        elif atype == "MARRIED_NO_SPOUSE":
            tables["applicant_info"].loc[info_mask, "sex"] = (
                tables["applicant_info"].loc[info_mask, "sex"])  # no-op
            mh_mask = tables["marital_history"]["application_id"] == app_id
            tables["marital_history"].loc[mh_mask, "marital_status"] = "Married"
            tables["marital_history"].loc[mh_mask, "spouse_family_name"] = None
            tables["marital_history"].loc[mh_mask, "spouse_given_name"] = None
            _log(atype, "marital_history", "spouse_family_name")

        elif atype == "SINGLE_WITH_SPOUSE":
            mh_mask = tables["marital_history"]["application_id"] == app_id
            tables["marital_history"].loc[mh_mask, "marital_status"] = "Single"
            tables["marital_history"].loc[mh_mask, "spouse_family_name"] = (
                "FakeSpouse")
            tables["marital_history"].loc[mh_mask, "spouse_given_name"] = "X"
            _log(atype, "marital_history", "marital_status/spouse")

        elif atype == "NO_CHILDREN_BUT_LISTED":
            # Inject a child record but set num_children context to 0
            tables["children"] = pd.concat([
                tables["children"],
                pd.DataFrame([{
                    "application_id": app_id, "child_number": 1,
                    "family_name": "Ghost", "given_name": "Child",
                    "date_of_birth": "2020-01-01",
                    "country_of_birth": "United States",
                    "also_applying": False,
                }])
            ], ignore_index=True)
            _log(atype, "children", "phantom child row")

        elif atype == "CREWMAN_CONTRADICTION":
            tables["applicant_info"].loc[
                info_mask, "alien_crewman_visa"] = False
            tables["applicant_info"].loc[
                info_mask, "arrived_as_crewman"] = True
            _log(atype, "applicant_info",
                 "alien_crewman_visa/arrived_as_crewman")

        # ── E. Out-of-range values ──────────────────────────────────────
        elif atype == "EXTREME_HEIGHT":
            tables["biographic_info"].loc[bio_mask, "height_feet"] = (
                int(rng.choice([0, 8, 9])))
            tables["biographic_info"].loc[bio_mask, "height_inches"] = (
                int(rng.choice([0, 13, 15])))
            _log(atype, "biographic_info", "height_feet/height_inches")

        elif atype == "EXTREME_WEIGHT":
            tables["biographic_info"].loc[bio_mask, "weight_pounds"] = (
                int(rng.choice([10, 30, 650, 999])))
            _log(atype, "biographic_info", "weight_pounds")

        elif atype == "EXTREME_AGE":
            tables["applicant_info"].loc[info_mask, "date_of_birth"] = (
                "1900-01-01")
            _log(atype, "applicant_info", "date_of_birth")

        elif atype == "EXTREME_CHILDREN":
            ch_rows = [{"application_id": app_id, "child_number": j,
                         "family_name": "Doe", "given_name": f"Child{j}",
                         "date_of_birth": "2015-01-01",
                         "country_of_birth": "United States",
                         "also_applying": False}
                        for j in range(1, 26)]
            tables["children"] = pd.concat([
                tables["children"], pd.DataFrame(ch_rows)
            ], ignore_index=True)
            _log(atype, "children", "25 child rows added")

        # ── F. Orphaned / inconsistent references ───────────────────────
        elif atype == "WRONG_CATEGORY_GROUP":
            tables["filing_category"].loc[fc_mask, "category_group"] = (
                "WRONG_GROUP")
            _log(atype, "filing_category", "category_group")

        elif atype == "MISMATCHED_NAMES":
            cs_mask = ((tables["contacts_signatures"]["application_id"]
                        == app_id)
                       & (tables["contacts_signatures"]["contact_type"]
                          == "APPLICANT"))
            tables["contacts_signatures"].loc[
                cs_mask, "family_name"] = "DIFFERENT_NAME"
            _log(atype, "contacts_signatures", "family_name")

        elif atype == "DUPLICATE_A_NUMBER":
            # Will be handled in bulk post-loop for efficiency
            _log(atype, "applicant_info", "a_number (duplicate)")

        # ── G. Eligibility contradictions ───────────────────────────────
        elif atype == "ALL_YES_SECURITY":
            for q in range(42, 56):
                tables["eligibility_responses"].loc[
                    elig_mask, f"q{q}"] = True
            _log(atype, "eligibility_responses", "q42-q55")

        elif atype == "APPROVED_WITH_BARS":
            tables["application"].loc[app_mask, "status"] = "APPROVED"
            for q in [22, 23, 24, 25, 26]:
                tables["eligibility_responses"].loc[
                    elig_mask, f"q{q}"] = True
            _log(atype, "eligibility_responses/application",
                 "status + criminal questions")

        # ── H. Completeness ─────────────────────────────────────────────
        elif atype == "SPARSE_RECORD":
            # Remove most child-table rows for this application
            for tbl in ["addresses", "employment_history", "parents",
                        "marital_history", "organizations",
                        "contacts_signatures"]:
                tbl_mask = tables[tbl]["application_id"] == app_id
                tables[tbl] = tables[tbl][~tbl_mask]
            _log(atype, "multiple", "all child rows removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main generator function

# COMMAND ----------

def generate_anomalies(count: int = 60_000, start_id: int = 140_001,
                       seed: int = 84,
                       output_dir: str | None = None):
    """Generate *count* anomaly-laden I-485 applications.

    Produces the same 20 tables as the clean generator, then injects
    1–5 anomalies per record.  Appends to existing output files.
    """
    print(f"=== Generator 2: Anomalies — {count:,} applications "
          f"(IDs {start_id}–{start_id + count - 1}) ===")

    rng  = np.random.default_rng(seed)
    fake = Faker("en_US")
    Faker.seed(seed)

    # ── Step 1: build base profiles (valid first, then break) ───────
    print("  Building base profiles …")
    P = build_profiles(count, start_id, seed)
    n = len(P)
    ids = P["application_id"].values

    # ── Step 2: generate all 20 tables (reuse clean logic) ──────────
    print("  Generating base tables …")
    # Import the clean generator's logic — we call it with our profiles
    from importlib import import_module
    clean_mod = import_module("01_generate_clean")

    # We'll generate tables in-memory by calling generate_clean with a
    # temporary output directory, then load them back as DataFrames.
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        clean_mod.generate_clean(count=count, start_id=start_id,
                                 seed=seed, output_dir=tmpdir)
        # Load all tables into a dict
        tables = {}
        _csv_dir = os.path.join(tmpdir, "csv")
        for fname in os.listdir(_csv_dir):
            if fname.endswith(".csv"):
                tname = fname[:-4]
                tables[tname] = pd.read_csv(
                    os.path.join(_csv_dir, fname))
        # Coerce columns that might be read as numeric but must accept strings
        _str_cols = {
            "addresses": ["zip_code", "street", "city", "state"],
            "applicant_info": ["ssn", "a_number", "passport_number",
                               "i94_number", "date_of_birth",
                               "arrival_date", "passport_expiration"],
            "application": ["a_number", "receipt_number", "filing_date"],
            "parents": ["date_of_birth"],
        }
        for tbl_name, cols in _str_cols.items():
            if tbl_name in tables:
                for col in cols:
                    if col in tables[tbl_name].columns:
                        tables[tbl_name][col] = (
                            tables[tbl_name][col].astype(object))

    # ── Step 3: assign anomalies ────────────────────────────────────
    print("  Injecting anomalies …")
    manifest = []
    anom_per_record = pick(rng, _ANOM_COUNT_OPTS, n, _ANOM_COUNT_W).astype(int)

    for i in range(n):
        app_id = int(ids[i])
        k = int(anom_per_record[i])
        chosen = list(rng.choice(ANOMALY_TYPES, size=k, replace=False))
        _inject_anomalies(tables, app_id, chosen, rng, manifest)

    # ── Step 3b: bulk handle DUPLICATE_A_NUMBER ─────────────────────
    dup_ids = [m["application_id"] for m in manifest
               if m["anomaly_type"] == "DUPLICATE_A_NUMBER"]
    if len(dup_ids) >= 2:
        shared_a = tables["applicant_info"].loc[
            tables["applicant_info"]["application_id"] == dup_ids[0],
            "a_number"].iloc[0]
        for did in dup_ids[1:]:
            mask = tables["applicant_info"]["application_id"] == did
            tables["applicant_info"].loc[mask, "a_number"] = shared_a
            mask2 = tables["application"]["application_id"] == did
            tables["application"].loc[mask2, "a_number"] = shared_a

    # ── Step 4: append tables to existing output ────────────────────
    print("  Saving anomaly tables …")
    for tname, df in tables.items():
        append_i485_table(df, tname, output_dir)

    # ── Step 5: save anomaly manifest ───────────────────────────────
    manifest_df = pd.DataFrame(manifest)
    csv_dir = os.path.join(output_dir, "csv") if output_dir else I485_CSV
    os.makedirs(csv_dir, exist_ok=True)
    mpath = os.path.join(csv_dir, "_anomaly_manifest.csv")
    manifest_df.to_csv(mpath, index=False)
    print(f"    _anomaly_manifest: {len(manifest_df):,} entries → {mpath}")

    print(f"\n  Anomaly generation complete — {count:,} applications, "
          f"{len(manifest_df):,} anomalies injected.")

# COMMAND ----------

if __name__ == "__main__":
    generate_anomalies(count=COUNT, start_id=START_ID,
                       seed=SEED, output_dir=OUTPUT_DIR)
