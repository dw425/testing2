# Databricks notebook source
# MAGIC %md
# MAGIC # Generator 1 — Clean I-485 Records
# MAGIC Generates **140 000** valid, spec-compliant applications across all 20 child
# MAGIC tables.  Run this notebook first; generators 2 and 3 build on its output.

# COMMAND ----------

# Parameters — override via Databricks widgets or CLI args
import argparse, os, sys

_DEFAULTS = {"count": "140000", "start_id": "1", "seed": "42", "output_dir": ""}

try:
    dbutils  # type: ignore[name-defined]  # noqa: F821
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

# Imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from faker import Faker

from config_i485 import (
    save_i485_table, AFFIDAVIT_EXEMPT_CATEGORIES,
    AFFIDAVIT_REASON_CODES, ORG_TYPES, BENEFIT_TYPES,
    CATEGORY_DESCRIPTIONS, CATEGORY_GROUPS,
)
from profiles import build_profiles
from utils import pick, gen_dates, gen_timestamps

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build profiles

# COMMAND ----------

def generate_clean(count: int = 140_000, start_id: int = 1,
                   seed: int = 42, output_dir: str | None = None):
    """Generate *count* valid I-485 applications (all 20 tables).

    Fully reusable — call from a notebook, CLI, or import as a library.
    """
    print(f"=== Generator 1: Clean — {count:,} applications (IDs {start_id}–"
          f"{start_id + count - 1}) ===")

    rng  = np.random.default_rng(seed)
    fake = Faker("en_US")
    Faker.seed(seed)

    # ── Build coherent profiles ─────────────────────────────────────────
    print("  Building profiles …")
    P = build_profiles(count, start_id, seed)
    n = len(P)
    ids = P["application_id"].values

    # Helper: save wrapper
    def _save(df, name):
        save_i485_table(df, name, output_dir)

    # ================================================================
    # Table 1 — application
    # ================================================================
    print("  Generating tables …")
    interviewed = rng.random(n) < 0.25
    interview_waived = (~interviewed) & (rng.random(n) < 0.10)

    app = pd.DataFrame({
        "application_id":             ids,
        "a_number":                   P["a_number"],
        "receipt_number":             P["receipt_number"],
        "filing_date":                P["filing_date"],
        "form_edition":               P["form_edition"],
        "uscis_section_of_law":       P["section_of_law"],
        "status":                     P["status"],
        "uscis_applicant_interviewed": interviewed,
        "uscis_interview_waived":     interview_waived,
        "has_attorney":               P["has_attorney"],
        "atty_name":                  P["atty_name"],
        "atty_state_bar_number":      P["atty_bar_number"],
        "volag_number":               [
            f"V{rng.integers(10000,99999)}" if P["has_attorney"].iloc[i]
            else None for i in range(n)
        ],
        "uscis_oan":                  [
            f"IOE{rng.integers(100000,999999)}" if P["has_attorney"].iloc[i]
            else None for i in range(n)
        ],
        "created_at":                 gen_timestamps(rng, n),
        "updated_at":                 gen_timestamps(rng, n),
    })
    _save(app, "application")

    # ================================================================
    # Table 2 — applicant_info
    # ================================================================
    applicant = pd.DataFrame({
        "application_id":           ids,
        "family_name":              P["family_name"],
        "given_name":               P["given_name"],
        "middle_name":              P["middle_name"],
        "date_of_birth":            P["date_of_birth"],
        "city_of_birth":            P["city_of_birth"],
        "country_of_birth":         P["country_of_birth"],
        "country_of_citizenship":   P["country_of_citizenship"],
        "sex":                      P["sex"],
        "ssn":                      P["ssn"],
        "has_ssn":                  P["has_ssn"],
        "a_number":                 P["a_number"],
        "has_a_number":             P["has_a_number"],
        "passport_number":          P["passport_number"],
        "passport_country":         P["passport_country"],
        "passport_expiration":      P["passport_expiration"],
        "i94_number":               P["i94_number"],
        "arrival_date":             P["arrival_date"],
        "arrival_city":             P["arrival_city"],
        "arrival_state":            P["arrival_state"],
        "arrival_type":             P["arrival_type"],
        "current_immigration_status": P["current_immigration_status"],
        "admitted_as":              P["arrival_type"],
        "alien_crewman_visa":       rng.random(n) < 0.005,
        "arrived_as_crewman":       rng.random(n) < 0.005,
        "created_at":               gen_timestamps(rng, n),
        "updated_at":               gen_timestamps(rng, n),
    })
    _save(applicant, "applicant_info")

    # ================================================================
    # Table 3 — addresses  (~3.5 per application)
    # ================================================================
    has_mailing = rng.random(n) < 0.30
    has_prior   = rng.random(n) < 0.60
    has_foreign = rng.random(n) < 0.40
    addr_counts = (1 + has_mailing.astype(int) + has_prior.astype(int)
                   + has_foreign.astype(int))

    addr_rows = []
    for i in range(n):
        aid = int(ids[i])
        order = 1
        # Current physical (always)
        addr_rows.append({
            "application_id": aid, "address_type": "CURRENT_PHYSICAL",
            "sort_order": order,
            "street": P["us_street"].iloc[i],
            "city": P["us_city"].iloc[i],
            "state": P["us_state"].iloc[i],
            "zip_code": P["us_zip"].iloc[i],
            "country": "United States",
        })
        order += 1
        if has_mailing[i]:
            addr_rows.append({
                "application_id": aid, "address_type": "MAILING",
                "sort_order": order,
                "street": P["us_street"].iloc[i],
                "city": P["us_city"].iloc[i],
                "state": P["us_state"].iloc[i],
                "zip_code": P["us_zip"].iloc[i],
                "country": "United States",
            })
            order += 1
        if has_prior[i]:
            addr_rows.append({
                "application_id": aid, "address_type": "PRIOR",
                "sort_order": order,
                "street": fake.street_address(),
                "city": fake.city(),
                "state": str(rng.choice(["CA", "NY", "TX", "FL", "IL", "NJ"])),
                "zip_code": fake.zipcode(),
                "country": "United States",
            })
            order += 1
        if has_foreign[i]:
            addr_rows.append({
                "application_id": aid, "address_type": "FOREIGN",
                "sort_order": order,
                "street": fake.street_address(),
                "city": fake.city(),
                "state": None,
                "zip_code": None,
                "country": str(P["country_of_birth"].iloc[i]),
            })
    _save(pd.DataFrame(addr_rows), "addresses")

    # ================================================================
    # Table 4 — other_names  (~40 % of applicants)
    # ================================================================
    name_types = ["MAIDEN", "ALIAS", "BIRTH_NAME", "NICKNAME"]
    oname_rows = []
    for i in range(n):
        if rng.random() < 0.40:
            aid = int(ids[i])
            ntype = str(rng.choice(name_types))
            oname_rows.append({
                "application_id": aid,
                "name_type": ntype,
                "family_name": P["family_name"].iloc[i],
                "given_name": fake.first_name(),
                "middle_name": None,
            })
            # 10 % get a second other-name
            if rng.random() < 0.10:
                oname_rows.append({
                    "application_id": aid,
                    "name_type": str(rng.choice(name_types)),
                    "family_name": fake.last_name(),
                    "given_name": P["given_name"].iloc[i],
                    "middle_name": None,
                })
    _save(pd.DataFrame(oname_rows) if oname_rows else
          pd.DataFrame(columns=["application_id", "name_type",
                                "family_name", "given_name", "middle_name"]),
          "other_names")

    # ================================================================
    # Table 5 — filing_category
    # ================================================================
    fc = pd.DataFrame({
        "application_id":    ids,
        "category_code":     P["category_code"],
        "category_group":    P["category_group"],
        "category_description": [
            CATEGORY_DESCRIPTIONS.get(str(c), str(c))
            for c in P["category_code"]
        ],
        "applicant_type":    P["applicant_type"],
        "principal_a_number": [
            f"A{rng.integers(100000000,999999999)}"
            if P["applicant_type"].iloc[i] == "DERIVATIVE" else None
            for i in range(n)
        ],
        "principal_name": [
            fake.name() if P["applicant_type"].iloc[i] == "DERIVATIVE"
            else None for i in range(n)
        ],
        "applying_245i":    rng.random(n) < 0.04,
        "cspa":             rng.random(n) < 0.02,
        "asylum_granted_date": [
            str(P["filing_date"].iloc[i] - pd.Timedelta(
                days=int(rng.integers(30, 730))))
            if str(P["category_code"].iloc[i]) == "AR_ASYLEE" else None
            for i in range(n)
        ],
    })
    _save(fc, "filing_category")

    # ================================================================
    # Table 6 — affidavit_exemption  (~28 %)
    # ================================================================
    exempt_mask = P["category_code"].isin(AFFIDAVIT_EXEMPT_CATEGORIES).values
    # Also ~15 % of non-exempt get an entry
    extra_mask = (~exempt_mask) & (rng.random(n) < 0.15)
    ae_mask = exempt_mask | extra_mask
    ae_ids = ids[ae_mask]
    ae = pd.DataFrame({
        "application_id": ae_ids,
        "exempt": exempt_mask[ae_mask],
        "reason_code": pick(rng, AFFIDAVIT_REASON_CODES, len(ae_ids)),
    })
    _save(ae, "affidavit_exemption")

    # ================================================================
    # Table 7 — additional_info
    # ================================================================
    ai = pd.DataFrame({
        "application_id":       ids,
        "applied_abroad":       rng.random(n) < 0.15,
        "embassy_city":         [
            fake.city() if rng.random() < 0.15 else None for _ in range(n)
        ],
        "embassy_country":      [
            str(rng.choice(BENEFIT_TYPES)) if rng.random() < 0.15  # placeholder
            else None for _ in range(n)
        ],
        "previously_applied_us": rng.random(n) < 0.08,
        "had_lpr_rescinded":    rng.random(n) < 0.005,
    })
    _save(ai, "additional_info")

    # ================================================================
    # Table 8 — employment_history  (~2.5 per application)
    # ================================================================
    emp_rows = []
    for i in range(n):
        aid = int(ids[i])
        filing_dt = P["filing_date"].iloc[i]
        n_emp = int(rng.choice([2, 2, 3, 3, 3]))
        for j in range(n_emp):
            is_current = (j == 0)
            if is_current:
                emp_name = str(P["employer_name"].iloc[i])
                occ = str(P["occupation"].iloc[i])
            else:
                emp_name = fake.company()
                occ = fake.job()
            start_dt = filing_dt - pd.Timedelta(
                days=int(rng.integers(365 * j, 365 * (j + 2) + 1)))
            end_dt = None if is_current else (
                filing_dt - pd.Timedelta(
                    days=int(rng.integers(0, 365 * j + 1))))
            emp_rows.append({
                "application_id": aid,
                "sort_order": j + 1,
                "employer_name": emp_name,
                "occupation": occ,
                "start_date": start_dt,
                "end_date": end_dt,
                "is_current": is_current,
                "employer_street": fake.street_address(),
                "employer_city": fake.city(),
                "employer_state": str(P["us_state"].iloc[i]) if rng.random() < 0.80
                    else str(rng.choice(["CA", "NY", "TX"])),
                "employer_country": "United States" if rng.random() < 0.80
                    else str(P["country_of_birth"].iloc[i]),
            })
    _save(pd.DataFrame(emp_rows), "employment_history")

    # ================================================================
    # Table 9 — parents  (2 per application)
    # ================================================================
    par_rows = []
    for i in range(n):
        aid = int(ids[i])
        par_rows.append({
            "application_id": aid,
            "parent_number": 1,
            "family_name": P["parent1_family_name"].iloc[i],
            "given_name": P["parent1_given_name"].iloc[i],
            "birth_family_name": P["parent1_family_name"].iloc[i]
                if rng.random() < 0.30 else None,
            "date_of_birth": P["parent1_dob"].iloc[i],
            "country_of_birth": P["parent1_country"].iloc[i],
            "city_of_birth": fake.city(),
        })
        par_rows.append({
            "application_id": aid,
            "parent_number": 2,
            "family_name": P["parent2_family_name"].iloc[i],
            "given_name": P["parent2_given_name"].iloc[i],
            "birth_family_name": P["parent2_family_name"].iloc[i]
                if rng.random() < 0.30 else None,
            "date_of_birth": P["parent2_dob"].iloc[i],
            "country_of_birth": P["parent2_country"].iloc[i],
            "city_of_birth": fake.city(),
        })
    _save(pd.DataFrame(par_rows), "parents")

    # ================================================================
    # Table 10 — marital_history
    # ================================================================
    mh_rows = []
    for i in range(n):
        aid = int(ids[i])
        ms = str(P["marital_status"].iloc[i])
        filing_dt = P["filing_date"].iloc[i]
        if ms == "Single":
            mh_rows.append({
                "application_id": aid, "sort_order": 1,
                "marital_status": ms,
                "spouse_family_name": None, "spouse_given_name": None,
                "marriage_date": None, "marriage_end_date": None,
                "marriage_end_reason": None,
                "spouse_country_of_birth": None,
            })
        elif ms == "Married":
            m_date = filing_dt - pd.Timedelta(
                days=int(rng.integers(180, 7300)))
            mh_rows.append({
                "application_id": aid, "sort_order": 1,
                "marital_status": ms,
                "spouse_family_name": P["spouse_family_name"].iloc[i],
                "spouse_given_name": P["spouse_given_name"].iloc[i],
                "marriage_date": m_date,
                "marriage_end_date": None,
                "marriage_end_reason": None,
                "spouse_country_of_birth": P["spouse_country"].iloc[i],
            })
            # 20 % had a prior marriage
            if rng.random() < 0.20:
                pm_end = m_date - pd.Timedelta(
                    days=int(rng.integers(180, 1825)))
                mh_rows.append({
                    "application_id": aid, "sort_order": 2,
                    "marital_status": "Divorced",
                    "spouse_family_name": fake.last_name(),
                    "spouse_given_name": fake.first_name(),
                    "marriage_date": pm_end - pd.Timedelta(
                        days=int(rng.integers(365, 3650))),
                    "marriage_end_date": pm_end,
                    "marriage_end_reason": "Divorce",
                    "spouse_country_of_birth": str(
                        rng.choice(BENEFIT_TYPES)),  # placeholder country
                })
        elif ms == "Divorced":
            end_dt = filing_dt - pd.Timedelta(
                days=int(rng.integers(90, 3650)))
            mh_rows.append({
                "application_id": aid, "sort_order": 1,
                "marital_status": ms,
                "spouse_family_name": P["spouse_family_name"].iloc[i],
                "spouse_given_name": P["spouse_given_name"].iloc[i],
                "marriage_date": end_dt - pd.Timedelta(
                    days=int(rng.integers(365, 7300))),
                "marriage_end_date": end_dt,
                "marriage_end_reason": "Divorce",
                "spouse_country_of_birth": P["spouse_country"].iloc[i],
            })
        elif ms == "Widowed":
            end_dt = filing_dt - pd.Timedelta(
                days=int(rng.integers(30, 3650)))
            mh_rows.append({
                "application_id": aid, "sort_order": 1,
                "marital_status": ms,
                "spouse_family_name": P["spouse_family_name"].iloc[i],
                "spouse_given_name": P["spouse_given_name"].iloc[i],
                "marriage_date": end_dt - pd.Timedelta(
                    days=int(rng.integers(365, 7300))),
                "marriage_end_date": end_dt,
                "marriage_end_reason": "Death",
                "spouse_country_of_birth": P["spouse_country"].iloc[i],
            })
        else:  # Legally Separated
            mh_rows.append({
                "application_id": aid, "sort_order": 1,
                "marital_status": ms,
                "spouse_family_name": P["spouse_family_name"].iloc[i],
                "spouse_given_name": P["spouse_given_name"].iloc[i],
                "marriage_date": filing_dt - pd.Timedelta(
                    days=int(rng.integers(365, 7300))),
                "marriage_end_date": None,
                "marriage_end_reason": "Legal Separation",
                "spouse_country_of_birth": P["spouse_country"].iloc[i],
            })
    _save(pd.DataFrame(mh_rows), "marital_history")

    # ================================================================
    # Table 11 — children
    # ================================================================
    ch_rows = []
    for i in range(n):
        nc = int(P["num_children"].iloc[i])
        if nc == 0:
            continue
        aid = int(ids[i])
        app_dob = P["date_of_birth"].iloc[i]
        for j in range(nc):
            ch_dob = app_dob + pd.Timedelta(
                days=int(rng.integers(16 * 365, 40 * 365)))
            ch_rows.append({
                "application_id": aid,
                "child_number": j + 1,
                "family_name": P["family_name"].iloc[i],
                "given_name": fake.first_name(),
                "date_of_birth": ch_dob,
                "country_of_birth": str(P["country_of_birth"].iloc[i])
                    if rng.random() < 0.60 else "United States",
                "also_applying": rng.random() < 0.20,
            })
    _save(pd.DataFrame(ch_rows) if ch_rows else
          pd.DataFrame(columns=["application_id", "child_number",
                                "family_name", "given_name",
                                "date_of_birth", "country_of_birth",
                                "also_applying"]),
          "children")

    # ================================================================
    # Table 12 — biographic_info
    # ================================================================
    bio = pd.DataFrame({
        "application_id": ids,
        "ethnicity":      P["ethnicity"],
        "race":           P["race"],
        "height_feet":    P["height_feet"],
        "height_inches":  P["height_inches"],
        "weight_pounds":  P["weight_pounds"],
        "eye_color":      P["eye_color"],
        "hair_color":     P["hair_color"],
    })
    _save(bio, "biographic_info")

    # ================================================================
    # Table 13 — eligibility_responses
    # ================================================================
    # For clean data most answers are False; a few realistic exceptions
    elig = pd.DataFrame({"application_id": ids})
    for q in range(10, 87):
        col = f"q{q}"
        if q == 19:      # J-visa 2yr requirement
            elig[col] = rng.random(n) < 0.03
        elif q == 22:    # ever arrested
            elig[col] = rng.random(n) < 0.05
        elif q in (35,):  # foreign govt official
            elig[col] = rng.random(n) < 0.01
        elif q == 52:    # communist party
            elig[col] = rng.random(n) < 0.005
        elif q == 76:    # unlawfully present
            elig[col] = rng.random(n) < 0.08
        else:
            elig[col] = False
    _save(elig, "eligibility_responses")

    # ================================================================
    # Table 14 — organizations  (~30 %)
    # ================================================================
    org_rows = []
    for i in range(n):
        if rng.random() < 0.30:
            n_orgs = int(rng.choice([1, 1, 1, 2, 2, 3]))
            for j in range(n_orgs):
                org_rows.append({
                    "application_id": int(ids[i]),
                    "sort_order": j + 1,
                    "organization_name": fake.company(),
                    "organization_type": str(rng.choice(ORG_TYPES)),
                    "start_date": str(P["filing_date"].iloc[i] - pd.Timedelta(
                        days=int(rng.integers(365, 3650)))),
                    "end_date": None if rng.random() < 0.60
                        else str(P["filing_date"].iloc[i] - pd.Timedelta(
                            days=int(rng.integers(30, 365)))),
                })
    _save(pd.DataFrame(org_rows) if org_rows else
          pd.DataFrame(columns=["application_id", "sort_order",
                                "organization_name", "organization_type",
                                "start_date", "end_date"]),
          "organizations")

    # ================================================================
    # Table 15 — public_charge
    # ================================================================
    exempt_cat = P["category_code"].isin(AFFIDAVIT_EXEMPT_CATEGORIES)
    education_levels = ["Less than High School", "High School Diploma",
                        "Some College", "Bachelor's Degree",
                        "Master's Degree", "Doctorate"]
    edu_w_fam = [0.15, 0.25, 0.25, 0.20, 0.10, 0.05]
    edu_w_emp = [0.02, 0.08, 0.15, 0.35, 0.25, 0.15]

    pc = pd.DataFrame({
        "application_id": ids,
        "exempt_category": [
            str(P["category_code"].iloc[i]) if exempt_cat.iloc[i]
            else None for i in range(n)
        ],
        "household_size": rng.integers(1, 8, size=n),
        "household_income": [
            int(rng.integers(80000, 250001))
            if str(P["category_group"].iloc[i]) == "EMPLOYMENT"
            else int(rng.integers(15000, 80001))
            for i in range(n)
        ],
        "household_assets": [
            int(rng.integers(5000, 500001)) for _ in range(n)
        ],
        "household_liabilities": [
            int(rng.integers(0, 100001)) for _ in range(n)
        ],
        "education_level": [
            str(pick(rng, education_levels, 1, edu_w_emp)[0])
            if str(P["category_group"].iloc[i]) == "EMPLOYMENT"
            else str(pick(rng, education_levels, 1, edu_w_fam)[0])
            for i in range(n)
        ],
    })
    _save(pc, "public_charge")

    # ================================================================
    # Table 16 — benefits_received  (~3.5 %)
    # ================================================================
    ben_rows = []
    for i in range(n):
        if rng.random() < 0.035:
            ben_rows.append({
                "application_id": int(ids[i]),
                "benefit_type": str(rng.choice(BENEFIT_TYPES)),
                "amount": int(rng.integers(100, 5001)),
                "start_date": str(P["filing_date"].iloc[i] - pd.Timedelta(
                    days=int(rng.integers(90, 1825)))),
                "end_date": str(P["filing_date"].iloc[i] - pd.Timedelta(
                    days=int(rng.integers(0, 90)))),
                "duration_months": int(rng.integers(1, 24)),
            })
    _save(pd.DataFrame(ben_rows) if ben_rows else
          pd.DataFrame(columns=["application_id", "benefit_type",
                                "amount", "start_date", "end_date",
                                "duration_months"]),
          "benefits_received")

    # ================================================================
    # Table 17 — institutionalization  (<1 %)
    # ================================================================
    inst_rows = []
    for i in range(n):
        if rng.random() < 0.007:
            inst_rows.append({
                "application_id": int(ids[i]),
                "facility_name": fake.company() + " Care Center",
                "start_date": str(P["filing_date"].iloc[i] - pd.Timedelta(
                    days=int(rng.integers(365, 3650)))),
                "end_date": str(P["filing_date"].iloc[i] - pd.Timedelta(
                    days=int(rng.integers(30, 365)))),
                "reason": str(rng.choice([
                    "Medical treatment", "Rehabilitation",
                    "Mental health evaluation", "Other"])),
            })
    _save(pd.DataFrame(inst_rows) if inst_rows else
          pd.DataFrame(columns=["application_id", "facility_name",
                                "start_date", "end_date", "reason"]),
          "institutionalization")

    # ================================================================
    # Table 18 — contacts_signatures  (~1.7 per application)
    # ================================================================
    cs_rows = []
    for i in range(n):
        aid = int(ids[i])
        filing_dt = P["filing_date"].iloc[i]
        # Applicant contact (always)
        cs_rows.append({
            "application_id": aid,
            "contact_type": "APPLICANT",
            "family_name": P["family_name"].iloc[i],
            "given_name": P["given_name"].iloc[i],
            "phone": fake.phone_number(),
            "email": fake.email(),
            "signature_date": str(filing_dt),
        })
        # Preparer (~40 %)
        if P["has_attorney"].iloc[i]:
            cs_rows.append({
                "application_id": aid,
                "contact_type": "PREPARER",
                "family_name": str(P["atty_name"].iloc[i]).split()[-1]
                    if P["atty_name"].iloc[i] else fake.last_name(),
                "given_name": str(P["atty_name"].iloc[i]).split()[0]
                    if P["atty_name"].iloc[i] else fake.first_name(),
                "phone": fake.phone_number(),
                "email": fake.company_email(),
                "signature_date": str(filing_dt),
            })
        # Interpreter (~10 %)
        if rng.random() < 0.10:
            cs_rows.append({
                "application_id": aid,
                "contact_type": "INTERPRETER",
                "family_name": fake.last_name(),
                "given_name": fake.first_name(),
                "phone": fake.phone_number(),
                "email": fake.email(),
                "signature_date": str(filing_dt),
            })
    _save(pd.DataFrame(cs_rows), "contacts_signatures")

    # ================================================================
    # Table 19 — interview_signature  (interviewed ~25 %)
    # ================================================================
    iv_rows = []
    for i in range(n):
        if interviewed[i]:
            filing_dt = P["filing_date"].iloc[i]
            iv_date = filing_dt + pd.Timedelta(
                days=int(rng.integers(60, 365)))
            iv_rows.append({
                "application_id": int(ids[i]),
                "officer_name": fake.name(),
                "officer_id": f"USO{rng.integers(10000,99999)}",
                "interview_date": str(iv_date),
                "signature_date": str(iv_date),
            })
    _save(pd.DataFrame(iv_rows) if iv_rows else
          pd.DataFrame(columns=["application_id", "officer_name",
                                "officer_id", "interview_date",
                                "signature_date"]),
          "interview_signature")

    # ================================================================
    # Table 20 — additional_information  (~30 %)
    # ================================================================
    addinf_rows = []
    _parts = ["Part 1", "Part 2", "Part 3", "Part 4",
              "Part 5", "Part 6", "Part 7", "Part 8"]
    for i in range(n):
        if rng.random() < 0.30:
            addinf_rows.append({
                "application_id": int(ids[i]),
                "page_number": int(rng.integers(1, 15)),
                "part_number": str(rng.choice(_parts)),
                "item_number": str(rng.integers(1, 30)),
                "additional_text": fake.sentence(nb_words=15),
            })
    _save(pd.DataFrame(addinf_rows) if addinf_rows else
          pd.DataFrame(columns=["application_id", "page_number",
                                "part_number", "item_number",
                                "additional_text"]),
          "additional_information")

    # ================================================================
    # Table 21 — ref_filing_categories  (reference/lookup — 52 rows)
    # ================================================================
    from config_i485 import FILING_CATEGORIES
    ref_rows = []
    for code, group, weight in FILING_CATEGORIES:
        ref_rows.append({
            "category_code": code,
            "category_group": group,
            "category_description": CATEGORY_DESCRIPTIONS.get(code, code),
            "weight_pct": round(weight * 100, 2),
            "is_active": True,
        })
    _save(pd.DataFrame(ref_rows), "ref_filing_categories")

    # ── Summary ─────────────────────────────────────────────────────────
    print(f"\n  Clean generation complete — {count:,} applications, "
          "21 tables saved.")
    return P  # return profiles for downstream generators

# COMMAND ----------

# Execute (runs when notebook is executed or script is called)
if __name__ == "__main__":
    generate_clean(count=COUNT, start_id=START_ID,
                   seed=SEED, output_dir=OUTPUT_DIR)
