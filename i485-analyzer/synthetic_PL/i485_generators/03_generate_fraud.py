# Databricks notebook source
# MAGIC %md
# MAGIC # Generator 3 — Fraud Pattern Overlay
# MAGIC Reads the combined clean + anomaly output, selects **~15 000** records,
# MAGIC and modifies them in-place to create 10 detectable conspiracy patterns.
# MAGIC
# MAGIC Run **after** Generators 1 and 2.

# COMMAND ----------

import argparse, os, sys

_DEFAULTS = {"fraud_count": "15000", "seed": "126", "output_dir": ""}
try:
    dbutils  # type: ignore[name-defined]
    for k, v in _DEFAULTS.items():
        dbutils.widgets.text(k, v, k)  # type: ignore[name-defined]
    FRAUD_COUNT = int(dbutils.widgets.get("fraud_count"))  # type: ignore[name-defined]
    SEED        = int(dbutils.widgets.get("seed"))         # type: ignore[name-defined]
    OUTPUT_DIR  = dbutils.widgets.get("output_dir") or None  # type: ignore[name-defined]
except NameError:
    _p = argparse.ArgumentParser()
    _p.add_argument("--fraud-count", type=int,
                    default=int(_DEFAULTS["fraud_count"]))
    _p.add_argument("--seed", type=int, default=int(_DEFAULTS["seed"]))
    _p.add_argument("--output-dir", type=str, default="")
    _a, _ = _p.parse_known_args()
    FRAUD_COUNT, SEED = _a.fraud_count, _a.seed
    OUTPUT_DIR = _a.output_dir or None

# COMMAND ----------

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from faker import Faker

from config_i485 import (
    load_i485_table, save_i485_table, I485_CSV,
    CATEGORY_CODES, STATE_CODES,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: build lookup indexes for O(1) access

# COMMAND ----------

def _build_indexes(tables):
    """Build application_id → row-index mappings for fast lookup.

    For 1:1 tables (applicant_info, application, etc.), the index maps
    application_id → integer position.
    For 1:N tables (addresses, employment_history, etc.), it maps
    application_id → list of integer positions.
    """
    ONE_TO_ONE = {
        "application", "applicant_info", "additional_info",
        "biographic_info", "eligibility_responses", "filing_category",
        "public_charge",
    }
    idx = {}
    for name, df in tables.items():
        if "application_id" not in df.columns:
            continue
        if name in ONE_TO_ONE:
            # dict: aid → single positional index
            idx[name] = dict(zip(df["application_id"].values,
                                 range(len(df))))
        else:
            # dict: aid → list of positional indexes
            mapping = {}
            for pos, aid in enumerate(df["application_id"].values):
                mapping.setdefault(aid, []).append(pos)
            idx[name] = mapping
    return idx


def _pick_ids(rng, all_ids, k, exclude=None):
    """Sample *k* unique IDs from *all_ids*, excluding already-used."""
    if exclude:
        pool = np.setdiff1d(all_ids, list(exclude))
    else:
        pool = all_ids
    k = min(k, len(pool))
    return rng.choice(pool, size=k, replace=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud pattern functions (vectorized)

# COMMAND ----------

# ── Pattern 1: SSN Sharing Ring ─────────────────────────────────────────────

def pattern_ssn_sharing(tables, idx, rng, manifest, all_ids, used, budget=2000):
    """SSN sharing ring: *budget* records in rings of 5."""
    ring_size = 5
    n_rings = max(1, budget // ring_size)
    target_ids = _pick_ids(rng, all_ids, n_rings * ring_size, used)
    used.update(target_ids.tolist())

    info = tables["applicant_info"]
    info_idx = idx["applicant_info"]
    addr_df = tables["addresses"]
    addr_idx = idx["addresses"]

    for ring_i in range(n_rings):
        ring_ids = target_ids[ring_i * ring_size:(ring_i + 1) * ring_size]
        # Take SSN from first member
        src_pos = info_idx.get(ring_ids[0])
        if src_pos is None:
            continue
        source_ssn = info.iat[src_pos, info.columns.get_loc("ssn")]
        if pd.isna(source_ssn):
            source_ssn = f"{rng.integers(100,899):03d}-{rng.integers(1,99):02d}-{rng.integers(1,9999):04d}"
            info.iat[src_pos, info.columns.get_loc("ssn")] = source_ssn
            info.iat[src_pos, info.columns.get_loc("has_ssn")] = True

        ssn_col = info.columns.get_loc("ssn")
        has_ssn_col = info.columns.get_loc("has_ssn")
        state_col = addr_df.columns.get_loc("state")
        atype_col = addr_df.columns.get_loc("address_type")

        for aid in ring_ids[1:]:
            pos = info_idx.get(int(aid))
            if pos is None:
                continue
            info.iat[pos, ssn_col] = source_ssn
            info.iat[pos, has_ssn_col] = True
            # Spread across states
            for apos in addr_idx.get(int(aid), []):
                if addr_df.iat[apos, atype_col] == "CURRENT_PHYSICAL":
                    addr_df.iat[apos, state_col] = str(rng.choice(STATE_CODES))
            manifest.append({"application_id": int(aid),
                             "fraud_pattern": "SSN_SHARING_RING",
                             "pattern_group_id": f"SSN_RING_{ring_i}",
                             "details": f"shares SSN {source_ssn}"})
    print(f"    Pattern 1 (SSN Sharing): {n_rings} rings, "
          f"{n_rings * ring_size} records")


# ── Pattern 2: Address Fraud Ring ───────────────────────────────────────────

def pattern_address_ring(tables, idx, rng, fake, manifest, all_ids, used,
                         budget=3000):
    """Address fraud ring: *budget* records across shared addresses."""
    per_addr_avg = 15
    n_addrs = max(1, budget // per_addr_avg)

    addr_df = tables["addresses"]
    addr_idx = idx["addresses"]
    street_col = addr_df.columns.get_loc("street")
    city_col = addr_df.columns.get_loc("city")
    state_col = addr_df.columns.get_loc("state")
    zip_col = addr_df.columns.get_loc("zip_code")
    atype_col = addr_df.columns.get_loc("address_type")

    for ring_i in range(n_addrs):
        k = int(rng.integers(10, 21))
        target_ids = _pick_ids(rng, all_ids, k, used)
        used.update(target_ids.tolist())

        shared_street = fake.street_address()
        shared_city = fake.city()
        shared_state = str(rng.choice(["CA", "NY", "TX", "FL", "NJ"]))
        shared_zip = fake.zipcode()

        for aid in target_ids:
            for apos in addr_idx.get(int(aid), []):
                if addr_df.iat[apos, atype_col] == "CURRENT_PHYSICAL":
                    addr_df.iat[apos, street_col] = shared_street
                    addr_df.iat[apos, city_col] = shared_city
                    addr_df.iat[apos, state_col] = shared_state
                    addr_df.iat[apos, zip_col] = shared_zip
            manifest.append({"application_id": int(aid),
                             "fraud_pattern": "ADDRESS_FRAUD_RING",
                             "pattern_group_id": f"ADDR_RING_{ring_i}",
                             "details": f"{shared_street}, {shared_city}"})
    print(f"    Pattern 2 (Address Ring): {n_addrs} addresses")


# ── Pattern 3: Document Recycling ───────────────────────────────────────────

def pattern_document_recycling(tables, idx, rng, manifest, all_ids, used,
                               budget=1500):
    """Document recycling: *budget* records sharing documents."""
    n_groups = max(1, budget // 3)
    info = tables["applicant_info"]
    info_idx = idx["applicant_info"]

    pp_col = info.columns.get_loc("passport_number")
    i94_col = info.columns.get_loc("i94_number")

    for grp in range(n_groups):
        k = int(rng.choice([2, 2, 3]))
        target_ids = _pick_ids(rng, all_ids, k, used)
        used.update(target_ids.tolist())

        src_pos = info_idx.get(int(target_ids[0]))
        if src_pos is None:
            continue
        doc_type = str(rng.choice(["passport", "i94"]))
        col = pp_col if doc_type == "passport" else i94_col
        col_name = "passport_number" if doc_type == "passport" else "i94_number"
        val = info.iat[src_pos, col]
        if pd.isna(val):
            val = f"RECYCLED{rng.integers(100000, 999999)}"

        for aid in target_ids[1:]:
            pos = info_idx.get(int(aid))
            if pos is None:
                continue
            info.iat[pos, col] = val
            manifest.append({"application_id": int(aid),
                             "fraud_pattern": "DOCUMENT_RECYCLING",
                             "pattern_group_id": f"DOC_RECYCLE_{grp}",
                             "details": f"{col_name}={val}"})
    print(f"    Pattern 3 (Document Recycling): {n_groups} groups")


# ── Pattern 4: Identity Theft ───────────────────────────────────────────────

def pattern_identity_theft(tables, idx, rng, fake, manifest, all_ids, used,
                           budget=1000):
    """Identity theft: *budget* records in pairs."""
    n_pairs = max(1, budget // 2)
    info = tables["applicant_info"]
    info_idx = idx["applicant_info"]

    ssn_col = info.columns.get_loc("ssn")
    has_ssn_col = info.columns.get_loc("has_ssn")
    dob_col = info.columns.get_loc("date_of_birth")
    gn_col = info.columns.get_loc("given_name")

    for pair_i in range(n_pairs):
        ids_pair = _pick_ids(rng, all_ids, 2, used)
        used.update(ids_pair.tolist())

        src_pos = info_idx.get(int(ids_pair[0]))
        tgt_pos = info_idx.get(int(ids_pair[1]))
        if src_pos is None or tgt_pos is None:
            continue

        src_ssn = info.iat[src_pos, ssn_col]
        src_dob = info.iat[src_pos, dob_col]
        if pd.isna(src_ssn):
            src_ssn = f"{rng.integers(100,899):03d}-{rng.integers(1,99):02d}-{rng.integers(1,9999):04d}"
            info.iat[src_pos, ssn_col] = src_ssn
            info.iat[src_pos, has_ssn_col] = True

        info.iat[tgt_pos, ssn_col] = src_ssn
        info.iat[tgt_pos, has_ssn_col] = True
        info.iat[tgt_pos, dob_col] = src_dob
        orig_given = info.iat[src_pos, gn_col]
        info.iat[tgt_pos, gn_col] = (
            str(orig_given) + " " + str(rng.choice(["A", "B", "E", "M"])))

        manifest.append({"application_id": int(ids_pair[1]),
                         "fraud_pattern": "IDENTITY_THEFT",
                         "pattern_group_id": f"ID_THEFT_{pair_i}",
                         "details": f"copied from {ids_pair[0]}"})
    print(f"    Pattern 4 (Identity Theft): {n_pairs} pairs")


# ── Pattern 5: Attorney Mill ───────────────────────────────────────────────

def pattern_attorney_mill(tables, idx, rng, fake, manifest, all_ids, used,
                          budget=2500):
    """Attorney mill: *budget* records across suspicious attorneys."""
    n_attorneys = max(1, min(5, budget // 50))
    per_atty = max(1, budget // n_attorneys)
    app_df = tables["application"]
    app_idx = idx["application"]
    emp_df = tables["employment_history"]
    emp_idx = idx["employment_history"]
    fc_df = tables["filing_category"]
    fc_idx = idx["filing_category"]

    app_ha_col = app_df.columns.get_loc("has_attorney")
    app_an_col = app_df.columns.get_loc("atty_name")
    app_bar_col = app_df.columns.get_loc("atty_state_bar_number")
    app_fd_col = app_df.columns.get_loc("filing_date")
    emp_en_col = emp_df.columns.get_loc("employer_name")
    emp_so_col = emp_df.columns.get_loc("sort_order")
    fc_cc_col = fc_df.columns.get_loc("category_code")

    for atty_i in range(n_attorneys):
        target_ids = _pick_ids(rng, all_ids, per_atty, used)
        used.update(target_ids.tolist())

        atty_name = fake.name()
        atty_bar = f"NY{rng.integers(100000, 999999)}"
        cookie_employer = fake.company()
        cookie_category = str(rng.choice(["FAM_IR_SPOUSE", "FAM_F2A"]))
        base_date = pd.Timestamp("2023-01-01") + pd.Timedelta(
            days=int(rng.integers(0, 730)))

        for j, aid in enumerate(target_ids):
            aid_int = int(aid)
            # Application
            apos = app_idx.get(aid_int)
            if apos is not None:
                app_df.iat[apos, app_ha_col] = True
                app_df.iat[apos, app_an_col] = atty_name
                app_df.iat[apos, app_bar_col] = atty_bar
                fd = base_date + pd.Timedelta(days=int(j // 50 * 7))
                app_df.iat[apos, app_fd_col] = str(fd.date())

            # Cookie-cutter employment (sort_order == 1)
            for epos in emp_idx.get(aid_int, []):
                if emp_df.iat[epos, emp_so_col] == 1:
                    emp_df.iat[epos, emp_en_col] = cookie_employer
                    break

            # Same category
            fcpos = fc_idx.get(aid_int)
            if fcpos is not None:
                fc_df.iat[fcpos, fc_cc_col] = cookie_category

            manifest.append({"application_id": aid_int,
                             "fraud_pattern": "ATTORNEY_MILL",
                             "pattern_group_id": f"ATTY_MILL_{atty_i}",
                             "details": f"atty={atty_name} bar={atty_bar}"})

    print(f"    Pattern 5 (Attorney Mill): {n_attorneys} attorneys, "
          f"{n_attorneys * per_atty} records")


# ── Pattern 6: Rapid Filing / Velocity Anomaly ─────────────────────────────

def pattern_rapid_filing(tables, idx, rng, manifest, all_ids, used,
                         budget=1000):
    """Rapid filing: *budget* records in pairs."""
    n_groups = max(1, budget // 2)
    app_df = tables["application"]
    app_idx = idx["application"]
    info = tables["applicant_info"]
    info_idx = idx["applicant_info"]

    an_col = info.columns.get_loc("a_number")
    fd_col = app_df.columns.get_loc("filing_date")

    for grp in range(n_groups):
        ids_pair = _pick_ids(rng, all_ids, 2, used)
        used.update(ids_pair.tolist())

        src_pos = info_idx.get(int(ids_pair[0]))
        tgt_pos = info_idx.get(int(ids_pair[1]))
        if src_pos is None or tgt_pos is None:
            continue

        a_num = info.iat[src_pos, an_col]
        if pd.isna(a_num):
            a_num = f"A{rng.integers(100000000, 999999999)}"
            info.iat[src_pos, an_col] = a_num
        info.iat[tgt_pos, an_col] = a_num

        src_app_pos = app_idx.get(int(ids_pair[0]))
        tgt_app_pos = app_idx.get(int(ids_pair[1]))
        if src_app_pos is not None and tgt_app_pos is not None:
            src_filing = app_df.iat[src_app_pos, fd_col]
            new_fd = pd.Timestamp(src_filing) + pd.Timedelta(
                days=int(rng.integers(1, 30)))
            app_df.iat[tgt_app_pos, fd_col] = str(new_fd.date())

        for aid in ids_pair:
            manifest.append({"application_id": int(aid),
                             "fraud_pattern": "RAPID_FILING",
                             "pattern_group_id": f"VELOCITY_{grp}",
                             "details": f"a_number={a_num}"})
    print(f"    Pattern 6 (Rapid Filing): {n_groups} pairs")


# ── Pattern 7: Family Relationship Fraud ────────────────────────────────────

def pattern_family_fraud(tables, idx, rng, fake, manifest, all_ids, used,
                         budget=1500):
    """Family fraud: *budget* records in groups of 3."""
    n_groups = max(1, budget // 3)
    fc_df = tables["filing_category"]
    fc_idx = idx["filing_category"]
    mh_df = tables["marital_history"]
    mh_idx = idx["marital_history"]
    par_df = tables["parents"]
    par_idx = idx["parents"]

    fc_pan_col = fc_df.columns.get_loc("principal_a_number")
    fc_pn_col = fc_df.columns.get_loc("principal_name")
    mh_ms_col = mh_df.columns.get_loc("marital_status")
    mh_sfn_col = mh_df.columns.get_loc("spouse_family_name")
    mh_sgn_col = mh_df.columns.get_loc("spouse_given_name")
    par_dob_col = par_df.columns.get_loc("date_of_birth")
    par_pn_col = par_df.columns.get_loc("parent_number")

    for grp in range(n_groups):
        target_ids = _pick_ids(rng, all_ids, 3, used)
        used.update(target_ids.tolist())

        fraud_variant = int(rng.choice([1, 2, 3]))

        if fraud_variant == 1:
            # Circular sponsorship: A→B→C→A
            for j, aid in enumerate(target_ids):
                next_aid = target_ids[(j + 1) % 3]
                pos = fc_idx.get(int(aid))
                if pos is not None:
                    fc_df.iat[pos, fc_pan_col] = f"A{next_aid + 100000000}"
                    fc_df.iat[pos, fc_pn_col] = fake.name()
            detail = "circular sponsorship"

        elif fraud_variant == 2:
            # Same spouse on multiple applications
            shared_spouse = fake.name().split()
            sfn = shared_spouse[-1] if len(shared_spouse) > 1 else shared_spouse[0]
            sgn = shared_spouse[0]
            for aid in target_ids:
                for mpos in mh_idx.get(int(aid), []):
                    mh_df.iat[mpos, mh_ms_col] = "Married"
                    mh_df.iat[mpos, mh_sfn_col] = sfn
                    mh_df.iat[mpos, mh_sgn_col] = sgn
            detail = f"shared spouse {' '.join(shared_spouse)}"

        else:
            # Age-impossible parent
            for aid in target_ids:
                for ppos in par_idx.get(int(aid), []):
                    if par_df.iat[ppos, par_pn_col] == 1:
                        par_df.iat[ppos, par_dob_col] = "2005-01-01"
            detail = "parent younger than child"

        for aid in target_ids:
            manifest.append({"application_id": int(aid),
                             "fraud_pattern": "FAMILY_RELATIONSHIP_FRAUD",
                             "pattern_group_id": f"FAM_FRAUD_{grp}",
                             "details": detail})
    print(f"    Pattern 7 (Family Fraud): {n_groups} groups")


# ── Pattern 8: Financial Pattern Fraud ──────────────────────────────────────

def pattern_financial_fraud(tables, idx, rng, manifest, all_ids, used,
                            budget=1000):
    """Financial fraud: *budget* records with identical financials."""
    per_group = 20
    n_groups = max(1, budget // per_group)
    pc_df = tables["public_charge"]
    pc_idx = idx["public_charge"]

    inc_col = pc_df.columns.get_loc("household_income")
    ast_col = pc_df.columns.get_loc("household_assets")
    lia_col = pc_df.columns.get_loc("household_liabilities")

    for grp in range(n_groups):
        target_ids = _pick_ids(rng, all_ids, per_group, used)
        used.update(target_ids.tolist())

        income = int(rng.choice([50000, 75000, 100000]))
        assets = int(rng.choice([25000, 50000, 100000]))
        liab   = int(rng.choice([0, 5000, 10000]))

        for aid in target_ids:
            pos = pc_idx.get(int(aid))
            if pos is None:
                continue
            pc_df.iat[pos, inc_col] = income
            pc_df.iat[pos, ast_col] = assets
            pc_df.iat[pos, lia_col] = liab
            manifest.append({"application_id": int(aid),
                             "fraud_pattern": "FINANCIAL_PATTERN_FRAUD",
                             "pattern_group_id": f"FIN_FRAUD_{grp}",
                             "details": f"income={income} assets={assets}"})
    print(f"    Pattern 8 (Financial Fraud): {n_groups} groups")


# ── Pattern 9: Country/Nationality Mismatch Ring ───────────────────────────

def pattern_country_mismatch(tables, idx, rng, manifest, all_ids, used,
                              budget=500):
    """Country mismatch: *budget* records with incompatible nationalities."""
    per_group = 10
    n_groups = max(1, budget // per_group)
    info = tables["applicant_info"]
    info_idx = idx["applicant_info"]

    cob_col = info.columns.get_loc("country_of_birth")
    pc_col = info.columns.get_loc("passport_country")
    coc_col = info.columns.get_loc("country_of_citizenship")

    mismatches = [
        ("Mexico", "Canada", "Brazil"),
        ("India", "Pakistan", "Bangladesh"),
        ("China", "Japan", "South Korea"),
        ("Nigeria", "Ghana", "Kenya"),
        ("El Salvador", "Honduras", "Guatemala"),
    ]

    for grp in range(n_groups):
        target_ids = _pick_ids(rng, all_ids, per_group, used)
        used.update(target_ids.tolist())

        combo = mismatches[grp % len(mismatches)]
        for aid in target_ids:
            pos = info_idx.get(int(aid))
            if pos is None:
                continue
            info.iat[pos, cob_col] = combo[0]
            info.iat[pos, pc_col] = combo[1]
            info.iat[pos, coc_col] = combo[2]
            manifest.append({"application_id": int(aid),
                             "fraud_pattern": "COUNTRY_MISMATCH_RING",
                             "pattern_group_id": f"COUNTRY_{grp}",
                             "details": (f"birth={combo[0]} passport="
                                         f"{combo[1]} citizen={combo[2]}")})
    print(f"    Pattern 9 (Country Mismatch): {n_groups} groups")


# ── Pattern 10: Temporal Impossibility ──────────────────────────────────────

def pattern_temporal_impossibility(tables, idx, rng, manifest, all_ids, used,
                                   budget=500):
    """Temporal impossibility: *budget* records with contradictory timelines."""
    target_ids = _pick_ids(rng, all_ids, budget, used)
    used.update(target_ids.tolist())

    app_df = tables["application"]
    app_idx = idx["application"]
    info = tables["applicant_info"]
    info_idx = idx["applicant_info"]
    emp_df = tables["employment_history"]
    emp_idx = idx["employment_history"]

    info_arr_col = info.columns.get_loc("arrival_date")
    app_fd_col = app_df.columns.get_loc("filing_date")
    app_fe_col = app_df.columns.get_loc("form_edition")
    emp_sd_col = emp_df.columns.get_loc("start_date")
    emp_ed_col = emp_df.columns.get_loc("end_date")
    emp_es_col = emp_df.columns.get_loc("employer_state")

    for i, aid in enumerate(target_ids):
        aid_int = int(aid)
        variant = i % 3

        if variant == 0:
            pos = info_idx.get(aid_int)
            if pos is not None:
                info.iat[pos, info_arr_col] = "2020-06-01"
            detail = "arrival_date vs I-94 mismatch"

        elif variant == 1:
            pos = app_idx.get(aid_int)
            if pos is not None:
                app_df.iat[pos, app_fd_col] = "2015-01-01"
                app_df.iat[pos, app_fe_col] = "01/20/25"
            detail = "filing_date before form_edition"

        else:
            epositions = emp_idx.get(aid_int, [])
            if len(epositions) >= 2:
                emp_df.iat[epositions[0], emp_sd_col] = "2022-01-01"
                emp_df.iat[epositions[0], emp_ed_col] = "2023-06-01"
                emp_df.iat[epositions[0], emp_es_col] = "CA"
                emp_df.iat[epositions[1], emp_sd_col] = "2022-03-01"
                emp_df.iat[epositions[1], emp_ed_col] = "2023-03-01"
                emp_df.iat[epositions[1], emp_es_col] = "NY"
                detail = "overlapping employment in different states"
            else:
                pos = info_idx.get(aid_int)
                if pos is not None:
                    info.iat[pos, info_arr_col] = "2020-06-01"
                detail = "arrival_date vs I-94 mismatch (fallback)"

        manifest.append({"application_id": aid_int,
                         "fraud_pattern": "TEMPORAL_IMPOSSIBILITY",
                         "pattern_group_id": f"TEMPORAL_{i}",
                         "details": detail})
    print(f"    Pattern 10 (Temporal Impossibility): {len(target_ids)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main generator function

# COMMAND ----------

def generate_fraud(fraud_count: int = 15_000, seed: int = 126,
                   output_dir: str | None = None):
    """Apply fraud patterns to ~*fraud_count* existing records.

    Reads all 20 tables from the output directory, modifies records
    in-place, and overwrites the files.
    """
    import time as _time
    t0 = _time.time()

    print(f"=== Generator 3: Fraud Overlay — targeting ~{fraud_count:,} "
          "records ===")

    rng  = np.random.default_rng(seed)
    fake = Faker("en_US")
    Faker.seed(seed)

    # ── Load all tables ─────────────────────────────────────────────────
    print("  Loading existing tables …")
    table_names = [
        "application", "applicant_info", "addresses", "other_names",
        "filing_category", "affidavit_exemption", "additional_info",
        "employment_history", "parents", "marital_history", "children",
        "biographic_info", "eligibility_responses", "organizations",
        "public_charge", "benefits_received", "institutionalization",
        "contacts_signatures", "interview_signature",
        "additional_information",
    ]
    tables = {}
    for name in table_names:
        try:
            tables[name] = load_i485_table(name, output_dir)
        except FileNotFoundError:
            print(f"    WARNING: {name} not found, creating empty frame")
            tables[name] = pd.DataFrame(columns=["application_id"])
    t_load = _time.time()
    print(f"    Loaded {len(tables)} tables in {t_load - t0:.1f}s")

    # ── Build indexes for O(1) lookup ────────────────────────────────────
    print("  Building indexes …")
    idxmap = _build_indexes(tables)
    t_idx = _time.time()
    print(f"    Indexed in {t_idx - t_load:.1f}s")

    # Bias toward clean pool (IDs < 140001) so fraud hides in "good" data
    all_ids = tables["application"]["application_id"].values
    clean_ids = all_ids[all_ids <= 140_000]
    anomaly_ids = all_ids[all_ids > 140_000]
    # 70 % from clean, 30 % from anomaly
    fraud_pool = np.concatenate([
        rng.choice(clean_ids, size=min(len(clean_ids),
                                       int(fraud_count * 0.70)),
                   replace=False),
        rng.choice(anomaly_ids, size=min(len(anomaly_ids),
                                         int(fraud_count * 0.30)),
                   replace=False),
    ])
    rng.shuffle(fraud_pool)

    manifest = []
    used = set()

    # ── Apply all 10 patterns (budgets proportional to fraud_count) ────
    print("  Applying fraud patterns …")
    fc = fraud_count
    pattern_ssn_sharing(tables, idxmap, rng, manifest, fraud_pool, used,
                        budget=max(5, int(fc * 0.13)))
    pattern_address_ring(tables, idxmap, rng, fake, manifest, fraud_pool,
                         used, budget=max(5, int(fc * 0.20)))
    pattern_document_recycling(tables, idxmap, rng, manifest, fraud_pool,
                               used, budget=max(3, int(fc * 0.10)))
    pattern_identity_theft(tables, idxmap, rng, fake, manifest, fraud_pool,
                           used, budget=max(2, int(fc * 0.07)))
    pattern_attorney_mill(tables, idxmap, rng, fake, manifest, fraud_pool,
                          used, budget=max(5, int(fc * 0.17)))
    pattern_rapid_filing(tables, idxmap, rng, manifest, fraud_pool, used,
                         budget=max(2, int(fc * 0.07)))
    pattern_family_fraud(tables, idxmap, rng, fake, manifest, fraud_pool,
                         used, budget=max(3, int(fc * 0.10)))
    pattern_financial_fraud(tables, idxmap, rng, manifest, fraud_pool, used,
                            budget=max(5, int(fc * 0.07)))
    pattern_country_mismatch(tables, idxmap, rng, manifest, fraud_pool, used,
                              budget=max(5, int(fc * 0.03)))
    pattern_temporal_impossibility(tables, idxmap, rng, manifest, fraud_pool,
                                   used, budget=max(3, int(fc * 0.03)))
    t_pat = _time.time()
    print(f"    All patterns applied in {t_pat - t_idx:.1f}s")

    # ── Overwrite tables ────────────────────────────────────────────────
    print("  Saving modified tables …")
    for name in table_names:
        if name in tables and len(tables[name]) > 0:
            save_i485_table(tables[name], name, output_dir)

    # ── Save fraud manifest ─────────────────────────────────────────────
    manifest_df = pd.DataFrame(manifest)
    csv_dir = os.path.join(output_dir, "csv") if output_dir else I485_CSV
    os.makedirs(csv_dir, exist_ok=True)
    mpath = os.path.join(csv_dir, "_fraud_manifest.csv")
    manifest_df.to_csv(mpath, index=False)
    print(f"    _fraud_manifest: {len(manifest_df):,} entries → {mpath}")

    # ── Pattern summary ─────────────────────────────────────────────────
    if len(manifest_df) > 0:
        print("\n  Fraud pattern distribution:")
        for pat, cnt in (manifest_df["fraud_pattern"]
                         .value_counts().items()):
            print(f"    {pat}: {cnt:,}")

    elapsed = _time.time() - t0
    print(f"\n  Fraud overlay complete — {len(used):,} unique records "
          f"modified in {elapsed:.1f}s.")

# COMMAND ----------

if __name__ == "__main__":
    generate_fraud(fraud_count=FRAUD_COUNT, seed=SEED,
                   output_dir=OUTPUT_DIR)
