"""
02_features.py — Feature Engineering Pipeline for I-485 Fraud Detection

Reads 21 Parquet tables and 2 manifest CSVs from the synthetic data directory,
constructs 136 features across 13 categories, and outputs a feature matrix
(200K x ~136) plus a labels file.

Usage:
    python notebooks/02_features.py

Output:
    data/features/feature_matrix.parquet
    data/features/labels.parquet
"""

import logging
import os
import re
import sys
import time

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths — all relative to project root (parent of this script's directory)
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

PARQUET_DIR = os.path.join(PROJECT_ROOT, "synthetic_data", "i485_form", "parquet")
CSV_DIR = os.path.join(PROJECT_ROOT, "synthetic_data", "i485_form", "csv")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "features")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("features")


# ---------------------------------------------------------------------------
# Table loader
# ---------------------------------------------------------------------------
TABLE_NAMES = [
    "application", "applicant_info", "filing_category", "public_charge",
    "eligibility_responses", "biographic_info", "affidavit_exemption",
    "additional_info", "addresses", "employment_history", "parents",
    "contacts_signatures", "children", "marital_history", "other_names",
    "organizations", "benefits_received", "institutionalization",
    "interview_signature", "additional_information", "ref_filing_categories",
]


def load_table(name: str) -> pd.DataFrame:
    """Load a single table from Parquet (preferred) or CSV, with fallback."""
    pq_path = os.path.join(PARQUET_DIR, f"{name}.parquet")
    csv_path = os.path.join(CSV_DIR, f"{name}.csv")
    try:
        if os.path.exists(pq_path):
            return pd.read_parquet(pq_path)
        elif os.path.exists(csv_path):
            return pd.read_csv(csv_path)
        else:
            log.warning("Table '%s' not found — returning empty DataFrame", name)
            return pd.DataFrame(columns=["application_id"])
    except Exception as exc:
        log.warning("Failed to load table '%s': %s — returning empty DataFrame",
                    name, exc)
        return pd.DataFrame(columns=["application_id"])


def load_all_tables() -> dict[str, pd.DataFrame]:
    """Load every table, logging progress."""
    tables: dict[str, pd.DataFrame] = {}
    for name in TABLE_NAMES:
        t0 = time.time()
        tables[name] = load_table(name)
        elapsed = time.time() - t0
        log.info("  %-30s %8d rows  (%.2fs)", name, len(tables[name]), elapsed)
    return tables


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(df: pd.DataFrame, name: str) -> pd.Series | None:
    """Return column if it exists, else None."""
    return df[name] if name in df.columns else None


def _safe_date(series: pd.Series) -> pd.Series:
    """Coerce to datetime, invalid -> NaT."""
    return pd.to_datetime(series, errors="coerce")


def _to_bool_series(series: pd.Series) -> pd.Series:
    """Convert heterogeneous boolean-ish column to numeric 0/1.

    Handles: True/False, 'YES'/'NO', 'True'/'False', 'true'/'false',
             'yes'/'no', 1/0, NaN.
    """
    if series is None:
        return pd.Series(dtype="float64")
    s = series.copy()
    # Convert to string for uniform handling, preserving NaN
    mask_notna = s.notna()
    s_str = s.astype(str).str.strip().str.upper()
    result = pd.Series(0, index=s.index, dtype="float64")
    result[mask_notna & s_str.isin(["TRUE", "YES", "1", "1.0"])] = 1
    result[~mask_notna] = np.nan
    return result


def _shannon_entropy(yes_count: float, no_count: float) -> float:
    """Shannon entropy of a binary distribution."""
    total = yes_count + no_count
    if total == 0:
        return 0.0
    probs = []
    if yes_count > 0:
        probs.append(yes_count / total)
    if no_count > 0:
        probs.append(no_count / total)
    return -sum(p * np.log2(p) for p in probs)


def _benford_deviation(values: pd.Series) -> pd.Series:
    """Compute per-value Benford first-digit deviation.

    For each value, extract the first digit d (1-9).
    Expected frequency for that digit = log10(1 + 1/d).
    Return the absolute deviation from expected for the actual
    distribution observed across the whole series.
    """
    v = values.dropna().astype(float).abs()
    v = v[v > 0]
    if len(v) == 0:
        return pd.Series(0.0, index=values.index)

    first_digits = v.astype(str).str.lstrip("0").str[0]
    first_digits = pd.to_numeric(first_digits, errors="coerce")
    first_digits = first_digits[first_digits.between(1, 9)]

    # Observed frequency of each first digit
    observed_freq = first_digits.value_counts(normalize=True)

    # Expected Benford frequencies
    expected = {d: np.log10(1 + 1 / d) for d in range(1, 10)}

    # Map each value to its first-digit deviation
    all_first = values.copy().astype(float).abs()
    all_first = all_first.where(all_first > 0)
    fd = all_first.astype(str).str.lstrip("0").str[0]
    fd = pd.to_numeric(fd, errors="coerce")

    deviations = fd.map(
        lambda d: abs(observed_freq.get(d, 0) - expected.get(d, 0))
        if pd.notna(d) and 1 <= d <= 9 else 0.0
    )
    return deviations.fillna(0.0)


# =========================================================================
#  Feature group builders
# =========================================================================

def build_1a_identity(app_df, info_df, other_names_df, feat):
    """1A. Identity & Document Validation (14 features)."""
    log.info("Building 1A: Identity & Document Validation ...")
    t0 = time.time()
    n = len(feat)
    aids = feat["application_id"]

    # SSN cluster size
    ssn_col = _col(info_df, "ssn")
    if ssn_col is not None:
        info_merged = info_df[["application_id", "ssn"]].dropna(subset=["ssn"])
        ssn_counts = info_merged.groupby("ssn")["application_id"].transform("count")
        ssn_map = pd.Series(ssn_counts.values, index=info_merged["application_id"].values)
        feat["ssn_cluster_size"] = aids.map(ssn_map).fillna(0).astype(int)
    else:
        feat["ssn_cluster_size"] = 0
    feat["dup_ssn_flag"] = (feat["ssn_cluster_size"] > 1).astype(int)

    # A-number cluster size (from application table)
    anum_col = _col(app_df, "a_number")
    if anum_col is not None:
        app_anum = app_df[["application_id", "a_number"]].dropna(subset=["a_number"])
        app_anum = app_anum[app_anum["a_number"].astype(str).str.strip() != ""]
        anum_counts = app_anum.groupby("a_number")["application_id"].transform("count")
        anum_map = pd.Series(anum_counts.values, index=app_anum["application_id"].values)
        feat["anum_cluster_size"] = aids.map(anum_map).fillna(0).astype(int)
    else:
        feat["anum_cluster_size"] = 0
    feat["dup_anum_flag"] = (feat["anum_cluster_size"] > 1).astype(int)

    # Name+DOB cluster
    if "family_name" in info_df.columns and "date_of_birth" in info_df.columns:
        nd = info_df[["application_id", "family_name", "given_name",
                       "date_of_birth"]].copy()
        nd["key"] = (nd["family_name"].astype(str).str.upper() + "|" +
                     nd["given_name"].astype(str).str.upper() + "|" +
                     nd["date_of_birth"].astype(str))
        nd_valid = nd.dropna(subset=["family_name", "date_of_birth"])
        nd_counts = nd_valid.groupby("key")["application_id"].transform("count")
        nd_map = pd.Series(nd_counts.values, index=nd_valid["application_id"].values)
        feat["name_dob_cluster_size"] = aids.map(nd_map).fillna(0).astype(int)
    else:
        feat["name_dob_cluster_size"] = 0
    feat["dup_name_dob_flag"] = (feat["name_dob_cluster_size"] > 1).astype(int)

    # SSN format invalid
    if ssn_col is not None:
        ssn_lookup = info_df.set_index("application_id")["ssn"]
        ssn_vals = aids.map(ssn_lookup)
        # Valid: NNN-NN-NNNN, area != 000, 666, 9xx
        pattern = re.compile(r"^\d{3}-\d{2}-\d{4}$")
        def _ssn_invalid(v):
            if pd.isna(v) or str(v).strip() == "":
                return 0
            s = str(v).strip()
            if not pattern.match(s):
                return 1
            area = int(s[:3])
            if area == 0 or area == 666 or area >= 900:
                return 1
            return 0
        feat["ssn_format_invalid"] = ssn_vals.apply(_ssn_invalid).astype(int)
    else:
        feat["ssn_format_invalid"] = 0

    # A-number format invalid
    if anum_col is not None:
        anum_lookup = app_df.set_index("application_id")["a_number"]
        anum_vals = aids.map(anum_lookup)
        anum_pattern = re.compile(r"^A\d{9}$")
        def _anum_invalid(v):
            if pd.isna(v) or str(v).strip() == "":
                return 0
            return 0 if anum_pattern.match(str(v).strip()) else 1
        feat["anum_format_invalid"] = anum_vals.apply(_anum_invalid).astype(int)
    else:
        feat["anum_format_invalid"] = 0

    # Passport reuse flag
    if "passport_number" in info_df.columns:
        pp = info_df[["application_id", "passport_number"]].dropna(
            subset=["passport_number"])
        pp = pp[pp["passport_number"].astype(str).str.strip() != ""]
        pp = pp[pp["passport_number"].astype(str) != "None"]
        if len(pp) > 0:
            pp_counts = pp.groupby("passport_number")["application_id"].transform("count")
            pp_map = pd.Series((pp_counts > 1).astype(int).values,
                               index=pp["application_id"].values)
            feat["passport_reuse_flag"] = aids.map(pp_map).fillna(0).astype(int)
        else:
            feat["passport_reuse_flag"] = 0
    else:
        feat["passport_reuse_flag"] = 0

    # I-94 reuse flag
    if "i94_number" in info_df.columns:
        i94 = info_df[["application_id", "i94_number"]].dropna(
            subset=["i94_number"])
        i94 = i94[i94["i94_number"].astype(str).str.strip() != ""]
        i94 = i94[i94["i94_number"].astype(str) != "None"]
        if len(i94) > 0:
            i94_counts = i94.groupby("i94_number")["application_id"].transform("count")
            i94_map = pd.Series((i94_counts > 1).astype(int).values,
                                index=i94["application_id"].values)
            feat["i94_reuse_flag"] = aids.map(i94_map).fillna(0).astype(int)
        else:
            feat["i94_reuse_flag"] = 0
    else:
        feat["i94_reuse_flag"] = 0

    # Num other names & excessive aliases
    if len(other_names_df) > 0 and "application_id" in other_names_df.columns:
        on_counts = other_names_df.groupby("application_id").size()
        feat["num_other_names"] = aids.map(on_counts).fillna(0).astype(int)
    else:
        feat["num_other_names"] = 0
    feat["excessive_aliases"] = (feat["num_other_names"] > 5).astype(int)

    # Name is placeholder
    if "family_name" in info_df.columns:
        fn_lookup = info_df.set_index("application_id")["family_name"]
        fn_vals = aids.map(fn_lookup).astype(str).str.upper().str.strip()
        feat["name_is_placeholder"] = fn_vals.isin(
            ["DOE", "TEST", "UNKNOWN", "NONE", "NAN", ""]).astype(int)
    else:
        feat["name_is_placeholder"] = 0

    # Missing SSN
    if ssn_col is not None:
        ssn_lookup2 = info_df.set_index("application_id")["ssn"]
        ssn_mapped = aids.map(ssn_lookup2)
        feat["missing_ssn"] = (ssn_mapped.isna() |
                               (ssn_mapped.astype(str).str.strip() == "") |
                               (ssn_mapped.astype(str) == "None")).astype(int)
    else:
        feat["missing_ssn"] = 1

    log.info("  1A complete: 14 features (%.1fs)", time.time() - t0)
    return feat


def build_1b_demographic(app_df, info_df, bio_df, feat):
    """1B. Demographic & Biographic Anomalies (14 features)."""
    log.info("Building 1B: Demographic & Biographic Anomalies ...")
    t0 = time.time()
    aids = feat["application_id"]

    # Merge key info for easy access
    info_lookup = info_df.set_index("application_id")
    app_lookup = app_df.set_index("application_id")

    # Filing date
    if "filing_date" in app_df.columns:
        filing_dates = aids.map(app_lookup["filing_date"])
        filing_dates = _safe_date(filing_dates)
    else:
        filing_dates = pd.Series(pd.NaT, index=aids.index)

    # DOB
    if "date_of_birth" in info_df.columns:
        dob = aids.map(info_lookup["date_of_birth"])
        dob = _safe_date(dob)
    else:
        dob = pd.Series(pd.NaT, index=aids.index)

    # age_at_filing
    age = (filing_dates - dob).dt.days / 365.25
    feat["age_at_filing"] = age.fillna(0).round(2)
    feat["age_impossibly_young"] = ((age < 14) & age.notna()).astype(int)
    feat["age_impossibly_old"] = ((age > 120) & age.notna()).astype(int)
    feat["future_dob"] = ((dob > filing_dates) & dob.notna() &
                          filing_dates.notna()).astype(int)
    feat["missing_dob"] = dob.isna().astype(int)

    # Missing name
    if "family_name" in info_df.columns:
        fn = aids.map(info_lookup["family_name"])
        feat["missing_name"] = (fn.isna() |
                                (fn.astype(str).str.strip() == "") |
                                (fn.astype(str) == "None")).astype(int)
    else:
        feat["missing_name"] = 1

    # Sex encoded
    if "sex" in info_df.columns:
        sex_vals = aids.map(info_lookup["sex"]).astype(str).str.strip().str.lower()
        feat["sex_encoded"] = sex_vals.map(
            {"male": 1, "female": 0}).fillna(-1).astype(int)
    else:
        feat["sex_encoded"] = -1

    # Height/weight from biographic_info (stored as feet + inches)
    bio_lookup = bio_df.set_index("application_id") if len(bio_df) > 0 else pd.DataFrame()
    if len(bio_lookup) > 0 and "height_feet" in bio_df.columns:
        h_feet = aids.map(bio_lookup.get("height_feet", pd.Series(dtype="float64")))
        h_inches = aids.map(bio_lookup.get("height_inches", pd.Series(dtype="float64")))
        h_feet = pd.to_numeric(h_feet, errors="coerce").fillna(0)
        h_inches = pd.to_numeric(h_inches, errors="coerce").fillna(0)
        total_inches = h_feet * 12 + h_inches
        feat["height_outlier"] = ((total_inches > 85) |
                                  ((total_inches < 48) &
                                   (total_inches > 0))).astype(int)
    else:
        total_inches = pd.Series(0.0, index=aids.index)
        feat["height_outlier"] = 0

    if len(bio_lookup) > 0 and "weight_pounds" in bio_df.columns:
        weight = aids.map(bio_lookup.get("weight_pounds", pd.Series(dtype="float64")))
        weight = pd.to_numeric(weight, errors="coerce").fillna(0)
        feat["weight_outlier"] = ((weight > 400) |
                                  ((weight < 80) & (weight > 0))).astype(int)
    else:
        weight = pd.Series(0.0, index=aids.index)
        feat["weight_outlier"] = 0

    # BMI outlier (height in inches, weight in pounds)
    # BMI = (weight_lb / height_in^2) * 703
    safe_h = total_inches.replace(0, np.nan)
    bmi = (weight / (safe_h ** 2)) * 703
    feat["bmi_outlier"] = (((bmi > 50) | (bmi < 15)) & bmi.notna()).astype(int)

    # Country citizenship mismatch
    if ("country_of_citizenship" in info_df.columns and
            "country_of_birth" in info_df.columns):
        coc = aids.map(info_lookup["country_of_citizenship"]).astype(str)
        cob = aids.map(info_lookup["country_of_birth"]).astype(str)
        feat["country_citizenship_mismatch"] = (
            (coc != cob) & (coc != "nan") & (cob != "nan") &
            (coc != "None") & (cob != "None")
        ).astype(int)
    else:
        feat["country_citizenship_mismatch"] = 0

    # Passport country mismatch
    if ("passport_country" in info_df.columns
            and "country_of_citizenship" in info_df.columns):
        pp_c = aids.map(info_lookup["passport_country"]).astype(str).str.upper().str.strip()
        cit_c = aids.map(info_lookup["country_of_citizenship"]).astype(str).str.upper().str.strip()
        feat["passport_country_mismatch"] = (
            (pp_c != cit_c) &
            (~pp_c.isin(["NAN", "NONE", ""])) &
            (~cit_c.isin(["NAN", "NONE", ""]))
        ).astype(int)
    else:
        feat["passport_country_mismatch"] = 0

    # Arrival after filing
    if "arrival_date" in info_df.columns:
        arr = aids.map(info_lookup["arrival_date"])
        arr = _safe_date(arr)
        feat["arrival_after_filing"] = (
            (arr > filing_dates) & arr.notna() & filing_dates.notna()
        ).astype(int)
    else:
        feat["arrival_after_filing"] = 0

    # Passport expired before filing
    if "passport_expiration" in info_df.columns:
        pp_exp = _safe_date(aids.map(info_lookup["passport_expiration"]))
        feat["passport_expired_before_filing"] = (
            (pp_exp < filing_dates) & pp_exp.notna() & filing_dates.notna()
        ).astype(int)
    else:
        feat["passport_expired_before_filing"] = 0

    log.info("  1B complete: 14 features (%.1fs)", time.time() - t0)
    return feat


def build_1c_family(app_df, info_df, children_df, marital_df, parents_df,
                    contacts_df, fc_df, feat):
    """1C. Family/Marriage Fraud Indicators (14 features)."""
    log.info("Building 1C: Family/Marriage Fraud Indicators ...")
    t0 = time.time()
    aids = feat["application_id"]

    # num_children
    if len(children_df) > 0 and "application_id" in children_df.columns:
        ch_counts = children_df.groupby("application_id").size()
        feat["num_children"] = aids.map(ch_counts).fillna(0).astype(int)
    else:
        feat["num_children"] = 0
    feat["excessive_children"] = (feat["num_children"] > 20).astype(int)

    # num_marital_records
    if len(marital_df) > 0 and "application_id" in marital_df.columns:
        mh_counts = marital_df.groupby("application_id").size()
        feat["num_marital_records"] = aids.map(mh_counts).fillna(0).astype(int)
    else:
        feat["num_marital_records"] = 0

    feat["children_no_marriage"] = (
        (feat["num_children"] > 0) & (feat["num_marital_records"] == 0)
    ).astype(int)

    # single_with_spouse_data
    if (len(marital_df) > 0 and "marital_status" in marital_df.columns and
            "spouse_family_name" in marital_df.columns):
        # Find apps where status=Single but have non-null spouse data
        single_mask = marital_df["marital_status"].astype(str).str.strip() == "Single"
        has_spouse = marital_df["spouse_family_name"].notna() & (
            marital_df["spouse_family_name"].astype(str).str.strip() != "") & (
            marital_df["spouse_family_name"].astype(str) != "None")
        problem_ids = set(marital_df.loc[single_mask & has_spouse,
                                         "application_id"].unique())
        feat["single_with_spouse_data"] = aids.isin(problem_ids).astype(int)
    else:
        feat["single_with_spouse_data"] = 0

    feat["serial_marriages"] = (feat["num_marital_records"] > 3).astype(int)

    # child_older_than_parent: any child DOB < applicant DOB
    if (len(children_df) > 0 and "date_of_birth" in children_df.columns and
            "date_of_birth" in info_df.columns):
        ch = children_df[["application_id", "date_of_birth"]].copy()
        ch["child_dob"] = _safe_date(ch["date_of_birth"])
        info_dob = info_df[["application_id", "date_of_birth"]].copy()
        info_dob["parent_dob"] = _safe_date(info_dob["date_of_birth"])
        merged = ch.merge(info_dob[["application_id", "parent_dob"]],
                          on="application_id", how="left")
        bad = merged[merged["child_dob"] < merged["parent_dob"]]
        bad_ids = set(bad["application_id"].unique())
        feat["child_older_than_parent"] = aids.isin(bad_ids).astype(int)
    else:
        feat["child_older_than_parent"] = 0

    # child_spacing_impossible: two children < 9 months apart
    if (len(children_df) > 0 and "date_of_birth" in children_df.columns):
        ch2 = children_df[["application_id", "date_of_birth"]].copy()
        ch2["dob"] = _safe_date(ch2["date_of_birth"])
        ch2 = ch2.dropna(subset=["dob"]).sort_values(["application_id", "dob"])
        ch2["prev_dob"] = ch2.groupby("application_id")["dob"].shift(1)
        ch2["gap_days"] = (ch2["dob"] - ch2["prev_dob"]).dt.days
        bad_spacing = ch2[ch2["gap_days"].between(1, 270)]
        spacing_ids = set(bad_spacing["application_id"].unique())
        feat["child_spacing_impossible"] = aids.isin(spacing_ids).astype(int)
    else:
        feat["child_spacing_impossible"] = 0

    # parent_age_at_birth_min
    if (len(children_df) > 0 and "date_of_birth" in children_df.columns and
            "date_of_birth" in info_df.columns):
        ch3 = children_df[["application_id", "date_of_birth"]].copy()
        ch3["child_dob"] = _safe_date(ch3["date_of_birth"])
        info_dob2 = info_df[["application_id", "date_of_birth"]].copy()
        info_dob2["parent_dob"] = _safe_date(info_dob2["date_of_birth"])
        m3 = ch3.merge(info_dob2[["application_id", "parent_dob"]],
                       on="application_id", how="left")
        m3["parent_age"] = (m3["child_dob"] - m3["parent_dob"]).dt.days / 365.25
        min_age = m3.groupby("application_id")["parent_age"].min()
        feat["parent_age_at_birth_min"] = aids.map(min_age).fillna(0).round(2)
    else:
        feat["parent_age_at_birth_min"] = 0

    # shared_spouse_flag
    if (len(marital_df) > 0 and "spouse_family_name" in marital_df.columns and
            "spouse_given_name" in marital_df.columns):
        mh = marital_df[["application_id", "spouse_family_name",
                          "spouse_given_name"]].copy()
        mh["spouse_name"] = (mh["spouse_family_name"].astype(str).str.upper() +
                             "|" +
                             mh["spouse_given_name"].astype(str).str.upper())
        mh = mh[~mh["spouse_name"].isin(["NONE|NONE", "NAN|NAN", "|"])]
        sp_counts = mh.groupby("spouse_name")["application_id"].transform("count")
        shared_mask = sp_counts > 1
        shared_ids = set(mh.loc[shared_mask, "application_id"].unique())
        feat["shared_spouse_flag"] = aids.isin(shared_ids).astype(int)
    else:
        feat["shared_spouse_flag"] = 0

    # Circular sponsorship: A sponsors B and B sponsors A
    feat["circular_sponsorship"] = 0  # default
    try:
        if (len(fc_df) > 0 and "principal_a_number" in fc_df.columns
                and len(info_df) > 0 and "a_number" in info_df.columns):
            # app → own a_number
            own = info_df[["application_id", "a_number"]].dropna(subset=["a_number"])
            own = own[own["a_number"].astype(str).str.strip().ne("")]
            own_map = own.set_index("application_id")["a_number"].to_dict()
            # app → principal (who sponsors me)
            princ = fc_df[["application_id", "principal_a_number"]].dropna(
                subset=["principal_a_number"])
            princ = princ[princ["principal_a_number"].astype(str).str.strip().ne("")]
            princ = princ[princ["principal_a_number"].astype(str).ne("None")]
            princ_map = princ.set_index("application_id")[
                "principal_a_number"].to_dict()
            # Reverse: a_number → application_id
            anum_to_app = {v: k for k, v in own_map.items()}
            # Check: for each app A, if A's principal is B's a_number,
            # and B's principal is A's a_number → circular
            circ_ids = set()
            for app_a, principal_a in princ_map.items():
                app_b = anum_to_app.get(principal_a)
                if app_b is None:
                    continue
                a_anum = own_map.get(app_a)
                b_principal = princ_map.get(app_b)
                if a_anum and b_principal and a_anum == b_principal:
                    circ_ids.add(app_a)
                    circ_ids.add(app_b)
            if circ_ids:
                feat["circular_sponsorship"] = aids.isin(circ_ids).astype(int)
    except Exception:
        pass

    # parent_age_impossible: parent DOB after applicant DOB
    if (len(parents_df) > 0 and "date_of_birth" in parents_df.columns and
            "date_of_birth" in info_df.columns):
        par = parents_df[["application_id", "date_of_birth"]].copy()
        par["par_dob"] = _safe_date(par["date_of_birth"])
        info_dob3 = info_df[["application_id", "date_of_birth"]].copy()
        info_dob3["app_dob"] = _safe_date(info_dob3["date_of_birth"])
        m4 = par.merge(info_dob3[["application_id", "app_dob"]],
                       on="application_id", how="left")
        bad_parent = m4[m4["par_dob"] > m4["app_dob"]]
        bad_par_ids = set(bad_parent["application_id"].unique())
        feat["parent_age_impossible"] = aids.isin(bad_par_ids).astype(int)
    else:
        feat["parent_age_impossible"] = 0

    # also_applying_count
    if (len(children_df) > 0 and "also_applying" in children_df.columns):
        ch4 = children_df[["application_id", "also_applying"]].copy()
        ch4["also_val"] = _to_bool_series(ch4["also_applying"])
        aa_counts = ch4.groupby("application_id")["also_val"].sum()
        feat["also_applying_count"] = aids.map(aa_counts).fillna(0).astype(int)
    else:
        feat["also_applying_count"] = 0

    # has_preparer
    if (len(contacts_df) > 0 and "contact_type" in contacts_df.columns):
        prep_ids = set(contacts_df.loc[
            contacts_df["contact_type"].astype(str).str.upper() == "PREPARER",
            "application_id"].unique())
        feat["has_preparer"] = aids.isin(prep_ids).astype(int)
    else:
        feat["has_preparer"] = 0

    log.info("  1C complete: 14 features (%.1fs)", time.time() - t0)
    return feat


def build_1d_financial(pc_df, feat):
    """1D. Financial Fraud Signals (16 features)."""
    log.info("Building 1D: Financial Fraud Signals ...")
    t0 = time.time()
    aids = feat["application_id"]

    if len(pc_df) > 0 and "application_id" in pc_df.columns:
        pc_lookup = pc_df.set_index("application_id")

        inc_col = "household_income"
        ast_col = "household_assets"
        lia_col = "household_liabilities"

        income = aids.map(pc_lookup.get(inc_col, pd.Series(dtype="float64")))
        income = pd.to_numeric(income, errors="coerce")
        assets = aids.map(pc_lookup.get(ast_col, pd.Series(dtype="float64")))
        assets = pd.to_numeric(assets, errors="coerce")
        liabilities = aids.map(pc_lookup.get(lia_col, pd.Series(dtype="float64")))
        liabilities = pd.to_numeric(liabilities, errors="coerce")
    else:
        income = pd.Series(np.nan, index=aids.index)
        assets = pd.Series(np.nan, index=aids.index)
        liabilities = pd.Series(np.nan, index=aids.index)

    feat["income"] = income.fillna(0)
    feat["assets"] = assets.fillna(0)
    feat["liabilities"] = liabilities.fillna(0)
    feat["net_worth"] = feat["assets"] - feat["liabilities"]
    feat["negative_networth"] = (feat["net_worth"] < -50000).astype(int)
    feat["zero_income_high_assets"] = (
        (feat["income"] == 0) & (feat["assets"] > 500000)
    ).astype(int)

    # debt_to_income (capped at 1000)
    safe_income = feat["income"].replace(0, np.nan)
    feat["debt_to_income"] = (feat["liabilities"] / safe_income).clip(
        upper=1000).fillna(0)

    # assets_to_income (capped at 1000)
    feat["assets_to_income"] = (feat["assets"] / safe_income).clip(
        upper=1000).fillna(0)

    # Percentile ranks
    feat["income_percentile"] = feat["income"].rank(pct=True).fillna(0)
    feat["assets_percentile"] = feat["assets"].rank(pct=True).fillna(0)

    # missing_income
    feat["missing_income"] = (income.isna() | (income == 0)).astype(int)

    # round_number_income
    feat["round_number_income"] = (
        (feat["income"] > 0) & (feat["income"] % 100000 == 0)
    ).astype(int)

    # round_number_assets
    feat["round_number_assets"] = (
        (feat["assets"] > 0) & (feat["assets"] % 100000 == 0)
    ).astype(int)

    # identical_financials_flag: same income+assets+liabilities as 2+ other apps
    fin_key = (feat["income"].astype(str) + "|" +
               feat["assets"].astype(str) + "|" +
               feat["liabilities"].astype(str))
    fin_counts = fin_key.map(fin_key.value_counts())
    feat["identical_financials_flag"] = (fin_counts > 2).astype(int)

    # benford_income_deviation
    feat["benford_income_deviation"] = _benford_deviation(income).values

    # mahalanobis_financial: robust Mahalanobis on (income, assets, liabilities)
    try:
        from scipy.spatial.distance import mahalanobis as _mah_dist
        fin_cols = np.column_stack([
            feat["income"].values.astype(float),
            feat["assets"].values.astype(float),
            feat["liabilities"].values.astype(float),
        ])
        # Filter rows with any variation for covariance estimation
        valid_mask = np.isfinite(fin_cols).all(axis=1) & (fin_cols.std(axis=0) > 0).all()
        if valid_mask.sum() > 10:
            try:
                from sklearn.covariance import MinCovDet
                mcd = MinCovDet(support_fraction=0.75, random_state=42).fit(
                    fin_cols[valid_mask])
                mah_vals = mcd.mahalanobis(fin_cols)
                feat["mahalanobis_financial"] = np.where(
                    np.isfinite(mah_vals), mah_vals, 0.0).round(4)
            except Exception:
                # Fallback to standard covariance
                cov = np.cov(fin_cols[valid_mask], rowvar=False)
                cov_inv = np.linalg.pinv(cov)
                mu = fin_cols[valid_mask].mean(axis=0)
                mah_vals = np.array([
                    np.sqrt(max(0, (x - mu) @ cov_inv @ (x - mu)))
                    for x in fin_cols
                ])
                feat["mahalanobis_financial"] = np.where(
                    np.isfinite(mah_vals), mah_vals, 0.0).round(4)
        else:
            feat["mahalanobis_financial"] = 0.0
    except Exception:
        feat["mahalanobis_financial"] = 0.0

    log.info("  1D complete: 16 features (%.1fs)", time.time() - t0)
    return feat


def build_1e_eligibility(app_df, elig_df, feat):
    """1E. Eligibility Response Patterns (16 features)."""
    log.info("Building 1E: Eligibility Response Patterns ...")
    t0 = time.time()
    aids = feat["application_id"]

    # Identify all q columns
    q_cols = [c for c in elig_df.columns if re.match(r"^q\d+$", c)]
    q_cols_sorted = sorted(q_cols, key=lambda x: int(x[1:]))

    if len(elig_df) == 0 or len(q_cols) == 0:
        for f_name in ["total_yes_count", "critical_yes_count",
                       "high_yes_count", "criminal_yes_count",
                       "security_yes_count", "immigration_yes_count",
                       "has_critical_elig", "has_high_elig",
                       "self_reported_fraud", "deported_filing",
                       "all_yes_data_error", "all_null_not_completed",
                       "combined_security_criminal", "elig_answer_entropy",
                       "elig_block_contradiction", "approved_with_bars"]:
            feat[f_name] = 0
        log.info("  1E complete: 16 features (no data) (%.1fs)",
                 time.time() - t0)
        return feat

    # Convert all q columns to boolean (0/1)
    elig_bool = elig_df[["application_id"]].copy()
    for col in q_cols_sorted:
        elig_bool[col] = _to_bool_series(elig_df[col])

    elig_indexed = elig_bool.set_index("application_id")

    # Define question groups
    critical_qs = [f"q{q}" for q in [26, 27, 36, 37, 41, 42, 53]
                   if f"q{q}" in q_cols_sorted]
    high_qs = [f"q{q}" for q in [22, 24, 30, 70, 74]
               if f"q{q}" in q_cols_sorted]
    criminal_qs = [f"q{q}" for q in range(22, 42)
                   if f"q{q}" in q_cols_sorted]
    security_qs = [f"q{q}" for q in range(42, 56)
                   if f"q{q}" in q_cols_sorted]
    immigration_qs = ([f"q{q}" for q in range(10, 22)
                       if f"q{q}" in q_cols_sorted] +
                      [f"q{q}" for q in range(67, 87)
                       if f"q{q}" in q_cols_sorted])
    fraud_qs = [f"q{q}" for q in [68, 69, 70] if f"q{q}" in q_cols_sorted]

    # Compute per-app sums
    all_q_data = q_cols_sorted  # all q columns

    total_yes = elig_indexed[all_q_data].sum(axis=1)
    feat["total_yes_count"] = aids.map(total_yes).fillna(0).astype(int)

    critical_yes = (elig_indexed[critical_qs].sum(axis=1) if critical_qs
                    else pd.Series(0, index=elig_indexed.index))
    feat["critical_yes_count"] = aids.map(critical_yes).fillna(0).astype(int)

    high_yes = (elig_indexed[high_qs].sum(axis=1) if high_qs
                else pd.Series(0, index=elig_indexed.index))
    feat["high_yes_count"] = aids.map(high_yes).fillna(0).astype(int)

    criminal_yes = (elig_indexed[criminal_qs].sum(axis=1) if criminal_qs
                    else pd.Series(0, index=elig_indexed.index))
    feat["criminal_yes_count"] = aids.map(criminal_yes).fillna(0).astype(int)

    security_yes = (elig_indexed[security_qs].sum(axis=1) if security_qs
                    else pd.Series(0, index=elig_indexed.index))
    feat["security_yes_count"] = aids.map(security_yes).fillna(0).astype(int)

    immigration_yes = (elig_indexed[immigration_qs].sum(axis=1) if immigration_qs
                       else pd.Series(0, index=elig_indexed.index))
    feat["immigration_yes_count"] = aids.map(immigration_yes).fillna(0).astype(int)

    feat["has_critical_elig"] = (feat["critical_yes_count"] > 0).astype(int)
    feat["has_high_elig"] = (feat["high_yes_count"] > 0).astype(int)

    # self_reported_fraud: q68, q69, q70 = YES
    fraud_yes = (elig_indexed[fraud_qs].sum(axis=1) if fraud_qs
                 else pd.Series(0, index=elig_indexed.index))
    feat["self_reported_fraud"] = (aids.map(fraud_yes).fillna(0) > 0).astype(int)

    # deported_filing: q74 = YES
    if "q74" in elig_indexed.columns:
        q74_vals = elig_indexed["q74"]
        feat["deported_filing"] = (aids.map(q74_vals).fillna(0) > 0).astype(int)
    else:
        feat["deported_filing"] = 0

    # all_yes_data_error: total_yes > 20
    feat["all_yes_data_error"] = (feat["total_yes_count"] > 20).astype(int)

    # all_null_not_completed: all 77 questions null
    null_count = elig_indexed[all_q_data].isna().sum(axis=1)
    feat["all_null_not_completed"] = (
        aids.map(null_count).fillna(len(all_q_data)) == len(all_q_data)
    ).astype(int)

    # combined_security_criminal
    feat["combined_security_criminal"] = (
        (feat["security_yes_count"] > 0) & (feat["criminal_yes_count"] > 0)
    ).astype(int)

    # elig_answer_entropy: Shannon entropy of YES/NO distribution per row
    yes_c = elig_indexed[all_q_data].sum(axis=1)
    no_c = elig_indexed[all_q_data].notna().sum(axis=1) - yes_c
    entropy_vals = pd.Series(
        [_shannon_entropy(y, n) for y, n in zip(yes_c.values, no_c.values)],
        index=elig_indexed.index)
    feat["elig_answer_entropy"] = aids.map(entropy_vals).fillna(0).round(4)

    # elig_block_contradiction: contradictory answers within related blocks
    # e.g. says "no" to ever arrested (q22) but "yes" to convicted (q24)
    # or "no" to immigration violation (q67) but "yes" to deported (q74)
    contradiction_pairs = [
        ("q22", "q24"),  # arrested → convicted (can't be convicted without arrest)
        ("q22", "q26"),  # arrested → charged with crime
        ("q36", "q37"),  # drug offense → drug trafficker
        ("q42", "q43"),  # espionage → sabotage (often co-occur)
        ("q67", "q74"),  # immigration violation → deported
        ("q68", "q70"),  # failed to attend removal → immigration fraud
    ]
    contra_count = pd.Series(0, index=elig_indexed.index, dtype=int)
    for q_no, q_yes in contradiction_pairs:
        if q_no in all_q_data and q_yes in all_q_data:
            # Contradiction: answered NO to prerequisite but YES to consequence
            no_first = elig_indexed[q_no].fillna(0) == 0
            yes_second = elig_indexed[q_yes].fillna(0) == 1
            contra_count += (no_first & yes_second).astype(int)
    feat["elig_block_contradiction"] = aids.map(contra_count).fillna(0).astype(int)

    # approved_with_bars: status=APPROVED but critical_yes > 0
    if "status" in app_df.columns:
        app_status = app_df.set_index("application_id")["status"]
        status_mapped = aids.map(app_status).astype(str).str.upper().str.strip()
        feat["approved_with_bars"] = (
            (status_mapped == "APPROVED") & (feat["critical_yes_count"] > 0)
        ).astype(int)
    else:
        feat["approved_with_bars"] = 0

    log.info("  1E complete: 16 features (%.1fs)", time.time() - t0)
    return feat


def build_1f_attorney(app_df, contacts_df, feat):
    """1F. Attorney/Preparer Patterns (12 features)."""
    log.info("Building 1F: Attorney/Preparer Patterns ...")
    t0 = time.time()
    aids = feat["application_id"]

    # Attorney features from application table
    if "atty_state_bar_number" in app_df.columns:
        bar = app_df[["application_id", "atty_state_bar_number",
                       "filing_date", "status"]].copy()
        bar_valid = bar.dropna(subset=["atty_state_bar_number"])
        bar_valid = bar_valid[
            bar_valid["atty_state_bar_number"].astype(str).str.strip() != ""]
        bar_valid = bar_valid[
            bar_valid["atty_state_bar_number"].astype(str) != "None"]

        if len(bar_valid) > 0:
            # attorney_app_count
            atty_counts = bar_valid.groupby(
                "atty_state_bar_number")["application_id"].transform("count")
            atty_map = pd.Series(atty_counts.values,
                                 index=bar_valid["application_id"].values)
            feat["attorney_app_count"] = aids.map(atty_map).fillna(0).astype(int)

            # attorney_denial_rate
            bar_valid["is_denied"] = (
                bar_valid["status"].astype(str).str.upper() == "DENIED"
            ).astype(int)
            atty_denial = bar_valid.groupby("atty_state_bar_number").agg(
                denial_rate=("is_denied", "mean")
            )
            bar_to_rate = bar_valid.merge(
                atty_denial, on="atty_state_bar_number", how="left")
            rate_map = pd.Series(bar_to_rate["denial_rate"].values,
                                 index=bar_to_rate["application_id"].values)
            feat["attorney_denial_rate"] = aids.map(rate_map).fillna(0).round(4)

            # attorney_same_day_burst
            bar_valid["bar_date"] = (
                bar_valid["atty_state_bar_number"].astype(str) + "|" +
                bar_valid["filing_date"].astype(str))
            bd_counts = bar_valid.groupby("bar_date")[
                "application_id"].transform("count")
            bd_map = pd.Series(bd_counts.values,
                               index=bar_valid["application_id"].values)
            feat["attorney_same_day_burst"] = aids.map(bd_map).fillna(0).astype(int)
        else:
            feat["attorney_app_count"] = 0
            feat["attorney_denial_rate"] = 0.0
            feat["attorney_same_day_burst"] = 0
    else:
        feat["attorney_app_count"] = 0
        feat["attorney_denial_rate"] = 0.0
        feat["attorney_same_day_burst"] = 0

    feat["high_vol_attorney"] = (feat["attorney_app_count"] > 50).astype(int)
    feat["extreme_vol_attorney"] = (feat["attorney_app_count"] > 500).astype(int)

    # attorney_elig_similarity: cosine similarity of eligibility answers
    # within same attorney's client group
    feat["attorney_elig_similarity"] = 0.0
    feat["attorney_financial_similarity"] = 0.0
    feat["attorney_employment_cookie_cutter"] = 0.0
    if len(bar_valid) > 0:
        try:
            # Get eligibility answers for each app
            elig_cols_in_feat = [c for c in feat.columns
                                 if c.startswith("total_yes_count") or
                                 c.startswith("criminal_yes_count") or
                                 c.startswith("security_yes_count") or
                                 c.startswith("immigration_yes_count")]
            if not elig_cols_in_feat:
                elig_cols_in_feat = ["total_yes_count"]

            atty_lookup = bar_valid.set_index("application_id")[
                "atty_state_bar_number"].to_dict()
            feat_atty = aids.map(atty_lookup)

            # For high-volume attorneys, compute avg within-group similarity
            from sklearn.metrics.pairwise import cosine_similarity as _cos_sim

            # Financial similarity: identical financials within attorney group
            fin_key = (feat["income"].astype(str) + "|" +
                       feat["assets"].astype(str) + "|" +
                       feat["liabilities"].astype(str))
            atty_fin_sim = pd.Series(0.0, index=aids.index)
            atty_emp_cookie = pd.Series(0.0, index=aids.index)

            high_vol_attys = feat_atty.value_counts()
            high_vol_attys = high_vol_attys[high_vol_attys >= 10].index

            for atty_bar in high_vol_attys:
                mask = feat_atty == atty_bar
                group_idx = mask[mask].index
                if len(group_idx) < 2:
                    continue

                # Financial similarity: fraction with identical financials
                group_fin = fin_key.loc[group_idx]
                fin_mode_count = group_fin.value_counts().iloc[0]
                fin_sim_score = fin_mode_count / len(group_fin)
                atty_fin_sim.loc[group_idx] = round(fin_sim_score, 4)

                # Employment cookie-cutter: fraction sharing top occupation
                if "num_employment_records" in feat.columns:
                    emp_vals = feat.loc[group_idx, "num_employment_records"]
                    emp_mode_count = emp_vals.value_counts().iloc[0]
                    emp_sim = emp_mode_count / len(emp_vals)
                    atty_emp_cookie.loc[group_idx] = round(emp_sim, 4)

            feat["attorney_financial_similarity"] = atty_fin_sim
            feat["attorney_employment_cookie_cutter"] = atty_emp_cookie

            # Eligibility similarity: use total_yes_count variance within group
            atty_elig_sim = pd.Series(0.0, index=aids.index)
            if "total_yes_count" in feat.columns:
                for atty_bar in high_vol_attys:
                    mask = feat_atty == atty_bar
                    group_idx = mask[mask].index
                    if len(group_idx) < 2:
                        continue
                    yes_vals = feat.loc[group_idx, "total_yes_count"]
                    # Low variance → high similarity (cookie-cutter)
                    if yes_vals.std() < 1.0:
                        atty_elig_sim.loc[group_idx] = 1.0
                    elif yes_vals.std() < 3.0:
                        atty_elig_sim.loc[group_idx] = round(
                            1.0 - yes_vals.std() / 10.0, 4)
            feat["attorney_elig_similarity"] = atty_elig_sim
        except Exception:
            pass

    # Preparer features from contacts_signatures
    if (len(contacts_df) > 0 and "contact_type" in contacts_df.columns):
        prep = contacts_df[
            contacts_df["contact_type"].astype(str).str.upper() == "PREPARER"
        ].copy()

        if len(prep) > 0 and "family_name" in prep.columns:
            prep["prep_name"] = (prep["family_name"].astype(str).str.upper() +
                                 "|" +
                                 prep.get("given_name",
                                          pd.Series("", index=prep.index)
                                          ).astype(str).str.upper())
            prep_counts = prep.groupby("prep_name")[
                "application_id"].transform("count")
            prep_map = pd.Series(prep_counts.values,
                                 index=prep["application_id"].values)
            feat["preparer_app_count"] = aids.map(prep_map).fillna(0).astype(int)

            # missing_preparer_info: has preparer but family_name is null
            prep_missing = prep[
                prep["family_name"].isna() |
                (prep["family_name"].astype(str).str.strip() == "") |
                (prep["family_name"].astype(str) == "None")
            ]
            missing_prep_ids = set(prep_missing["application_id"].unique())
            feat["missing_preparer_info"] = aids.isin(
                missing_prep_ids).astype(int)
        else:
            feat["preparer_app_count"] = 0
            feat["missing_preparer_info"] = 0
    else:
        feat["preparer_app_count"] = 0
        feat["missing_preparer_info"] = 0

    # preparer_client_diversity_low: low country diversity among preparer's clients
    feat["preparer_client_diversity_low"] = 0
    if len(prep) > 0 and "prep_name" in prep.columns:
        try:
            # Need country info — check if country_of_citizenship is available in feat
            # We use a proxy: count distinct application_ids per preparer
            # and flag if > 10 clients but very few distinct names (proxy for diversity)
            prep_grp = prep.groupby("prep_name").agg(
                client_count=("application_id", "nunique"),
                name_diversity=("family_name", "nunique"),
            )
            # Flag if ratio of unique names to clients is very low (< 0.3)
            prep_grp["diversity_ratio"] = (
                prep_grp["name_diversity"] / prep_grp["client_count"].clip(lower=1)
            )
            low_div_preps = prep_grp[
                (prep_grp["client_count"] >= 10) &
                (prep_grp["diversity_ratio"] < 0.3)
            ].index
            if len(low_div_preps) > 0:
                low_div_ids = set(
                    prep.loc[prep["prep_name"].isin(low_div_preps),
                             "application_id"].unique())
                feat["preparer_client_diversity_low"] = aids.isin(
                    low_div_ids).astype(int)
        except Exception:
            pass

    # has_interpreter
    if (len(contacts_df) > 0 and "contact_type" in contacts_df.columns):
        interp_ids = set(contacts_df.loc[
            contacts_df["contact_type"].astype(str).str.upper() == "INTERPRETER",
            "application_id"].unique())
        feat["has_interpreter"] = aids.isin(interp_ids).astype(int)
    else:
        feat["has_interpreter"] = 0

    log.info("  1F complete: 12 features (%.1fs)", time.time() - t0)
    return feat


def build_1g_address(addresses_df, emp_df, info_df, feat):
    """1G. Address Fraud Patterns (10 features)."""
    log.info("Building 1G: Address Fraud Patterns ...")
    t0 = time.time()
    aids = feat["application_id"]

    if len(addresses_df) == 0 or "application_id" not in addresses_df.columns:
        for f_name in ["address_cluster_size", "extreme_address_cluster",
                       "missing_address", "bad_zip_format",
                       "address_employment_mismatch",
                       "multiple_current_addresses",
                       "address_unrelated_cluster",
                       "foreign_address_domestic_filing",
                       "num_prior_addresses", "address_state_count"]:
            feat[f_name] = 0
        feat["missing_address"] = 1
        log.info("  1G complete: 10 features (no data) (%.1fs)",
                 time.time() - t0)
        return feat

    addr = addresses_df.copy()

    # Identify current physical addresses
    addr_type_col = "address_type"
    current_mask = addr[addr_type_col].astype(str).str.upper().isin(
        ["CURRENT_PHYSICAL", "CURRENT PHYSICAL"])

    current_addr = addr[current_mask].copy()

    # address_cluster_size
    if len(current_addr) > 0:
        current_addr["addr_key"] = (
            current_addr.get("street", pd.Series("", index=current_addr.index)
                             ).astype(str).str.upper() + "|" +
            current_addr.get("city", pd.Series("", index=current_addr.index)
                             ).astype(str).str.upper() + "|" +
            current_addr.get("state", pd.Series("", index=current_addr.index)
                             ).astype(str).str.upper())
        addr_counts = current_addr.groupby("addr_key")[
            "application_id"].transform("count")
        addr_map = pd.Series(addr_counts.values,
                             index=current_addr["application_id"].values)
        feat["address_cluster_size"] = aids.map(addr_map).fillna(0).astype(int)
    else:
        feat["address_cluster_size"] = 0
    feat["extreme_address_cluster"] = (
        feat["address_cluster_size"] > 10).astype(int)

    # missing_address: no address record at all
    has_addr_ids = set(addr["application_id"].unique())
    feat["missing_address"] = (~aids.isin(has_addr_ids)).astype(int)

    # bad_zip_format
    if "zip_code" in current_addr.columns and len(current_addr) > 0:
        zip_pattern = re.compile(r"^\d{5}(-\d{4})?$")
        def _bad_zip(v):
            if pd.isna(v) or str(v).strip() == "" or str(v) == "None":
                return 0  # missing != bad format
            return 0 if zip_pattern.match(str(v).strip()) else 1
        current_addr_check = current_addr[["application_id", "zip_code"]].copy()
        current_addr_check["bad"] = current_addr_check["zip_code"].apply(_bad_zip)
        bad_zip_map = current_addr_check.groupby("application_id")["bad"].max()
        feat["bad_zip_format"] = aids.map(bad_zip_map).fillna(0).astype(int)
    else:
        feat["bad_zip_format"] = 0

    # address_employment_mismatch: current address state != current employment state
    feat["address_employment_mismatch"] = 0
    if (len(current_addr) > 0 and "state" in current_addr.columns
            and len(emp_df) > 0 and "employer_state" in emp_df.columns):
        try:
            addr_state = current_addr.groupby("application_id")["state"].first()
            # Get current employment state
            emp_curr = emp_df.copy()
            if "is_current" in emp_curr.columns:
                emp_curr = emp_curr[emp_curr["is_current"].astype(str).str.upper().isin(
                    ["TRUE", "1", "YES"])]
            if len(emp_curr) > 0:
                emp_state = emp_curr.groupby("application_id")["employer_state"].first()
                both = pd.DataFrame({"addr": aids.map(addr_state),
                                      "emp": aids.map(emp_state)})
                both_valid = both.dropna()
                mismatch = (
                    both_valid["addr"].astype(str).str.upper().str.strip() !=
                    both_valid["emp"].astype(str).str.upper().str.strip()
                ) & (both_valid["addr"].astype(str).str.strip() != "") & (
                    both_valid["emp"].astype(str).str.strip() != "")
                feat.loc[mismatch.index, "address_employment_mismatch"] = (
                    mismatch.astype(int))
        except Exception:
            pass

    # multiple_current_addresses
    if len(current_addr) > 0:
        curr_counts = current_addr.groupby("application_id").size()
        feat["multiple_current_addresses"] = (
            aids.map(curr_counts).fillna(0) > 1).astype(int)
    else:
        feat["multiple_current_addresses"] = 0

    # address_unrelated_cluster: address shared by 5+ apps with different family names
    feat["address_unrelated_cluster"] = 0
    if (len(current_addr) > 0 and "street" in current_addr.columns
            and "city" in current_addr.columns and len(info_df) > 0
            and "family_name" in info_df.columns):
        try:
            ca = current_addr[["application_id", "street", "city", "state"]].copy()
            ca["addr_key"] = (ca["street"].astype(str).str.upper().str.strip() +
                              "|" +
                              ca["city"].astype(str).str.upper().str.strip() +
                              "|" +
                              ca["state"].astype(str).str.upper().str.strip())
            # Merge with family names
            name_lookup = info_df.set_index("application_id")["family_name"]
            ca["family_name"] = ca["application_id"].map(name_lookup).astype(
                str).str.upper().str.strip()
            # For each address, count apps and unique family names
            addr_grp = ca.groupby("addr_key").agg(
                app_count=("application_id", "nunique"),
                name_count=("family_name", "nunique"),
            )
            # Flag: 5+ apps AND more than 3 distinct family names (unrelated)
            unrelated_addrs = addr_grp[
                (addr_grp["app_count"] >= 5) & (addr_grp["name_count"] >= 3)
            ].index
            if len(unrelated_addrs) > 0:
                unrelated_ids = set(
                    ca.loc[ca["addr_key"].isin(unrelated_addrs),
                           "application_id"].unique())
                feat["address_unrelated_cluster"] = aids.isin(
                    unrelated_ids).astype(int)
        except Exception:
            pass

    # foreign_address_domestic_filing: FOREIGN address type but domestic filing
    feat["foreign_address_domestic_filing"] = 0
    if len(addr) > 0 and "address_type" in addr.columns:
        try:
            foreign_types = addr[
                addr["address_type"].astype(str).str.upper().str.contains("FOREIGN")
            ]
            if len(foreign_types) > 0:
                # Check if country is US-like
                if "country" in foreign_types.columns:
                    us_foreign = foreign_types[
                        foreign_types["country"].astype(str).str.upper().isin(
                            ["US", "USA", "UNITED STATES"])
                    ]
                    if len(us_foreign) > 0:
                        foreign_dom_ids = set(us_foreign["application_id"].unique())
                        feat["foreign_address_domestic_filing"] = aids.isin(
                            foreign_dom_ids).astype(int)
                else:
                    foreign_ids = set(foreign_types["application_id"].unique())
                    feat["foreign_address_domestic_filing"] = aids.isin(
                        foreign_ids).astype(int)
        except Exception:
            pass

    # num_prior_addresses (non-current)
    non_current = addr[~current_mask]
    if len(non_current) > 0:
        prior_counts = non_current.groupby("application_id").size()
        feat["num_prior_addresses"] = aids.map(prior_counts).fillna(0).astype(int)
    else:
        feat["num_prior_addresses"] = 0

    # address_state_count
    if "state" in addr.columns:
        addr_valid = addr[addr["state"].notna() &
                          (addr["state"].astype(str).str.strip() != "") &
                          (addr["state"].astype(str) != "None")]
        if len(addr_valid) > 0:
            state_nuniq = addr_valid.groupby("application_id")["state"].nunique()
            feat["address_state_count"] = aids.map(
                state_nuniq).fillna(0).astype(int)
        else:
            feat["address_state_count"] = 0
    else:
        feat["address_state_count"] = 0

    log.info("  1G complete: 10 features (%.1fs)", time.time() - t0)
    return feat


def build_1h_filing_patterns(app_df, info_df, addr_df, feat):
    """1H. Filing Pattern Features (10 features)."""
    log.info("Building 1H: Filing Pattern Features ...")
    t0 = time.time()
    aids = feat["application_id"]

    if "filing_date" not in app_df.columns:
        for f_name in ["daily_filing_volume", "filing_volume_zscore",
                       "same_day_burst_flag", "filing_dow", "weekend_filing",
                       "filing_date_is_future", "rapid_filing_pair",
                       "same_country_burst", "coordinated_filing",
                       "filing_date_very_old"]:
            feat[f_name] = 0
        log.info("  1H complete: 10 features (no data) (%.1fs)",
                 time.time() - t0)
        return feat

    app_fd = app_df[["application_id", "filing_date"]].copy()
    app_fd["fd"] = _safe_date(app_fd["filing_date"])
    fd_lookup = app_fd.set_index("application_id")["fd"]
    filing_dates = aids.map(fd_lookup)

    # daily_filing_volume
    app_fd_valid = app_fd.dropna(subset=["fd"])
    day_counts = app_fd_valid.groupby("fd")["application_id"].transform("count")
    day_map = pd.Series(day_counts.values,
                        index=app_fd_valid["application_id"].values)
    feat["daily_filing_volume"] = aids.map(day_map).fillna(0).astype(int)

    # filing_volume_zscore
    dv = feat["daily_filing_volume"].astype(float)
    dv_mean = dv.mean()
    dv_std = dv.std()
    if dv_std > 0:
        feat["filing_volume_zscore"] = ((dv - dv_mean) / dv_std).round(4)
    else:
        feat["filing_volume_zscore"] = 0.0
    feat["same_day_burst_flag"] = (feat["filing_volume_zscore"] > 3.0).astype(int)

    # filing_dow
    feat["filing_dow"] = filing_dates.dt.dayofweek.fillna(0).astype(int)
    feat["weekend_filing"] = feat["filing_dow"].isin([5, 6]).astype(int)

    # filing_date_is_future
    cutoff = pd.Timestamp("2026-03-01")
    feat["filing_date_is_future"] = (
        (filing_dates > cutoff) & filing_dates.notna()
    ).astype(int)

    # rapid_filing_pair: same A-number filed within 30 days
    if "a_number" in app_df.columns:
        anum_fd = app_df[["application_id", "a_number", "filing_date"]].copy()
        anum_fd["fd"] = _safe_date(anum_fd["filing_date"])
        anum_fd = anum_fd.dropna(subset=["a_number", "fd"])
        anum_fd = anum_fd[anum_fd["a_number"].astype(str).str.strip() != ""]
        anum_fd = anum_fd[anum_fd["a_number"].astype(str) != "None"]

        if len(anum_fd) > 0:
            # Self-join on a_number
            anum_groups = anum_fd.groupby("a_number").filter(
                lambda x: len(x) > 1)
            rapid_ids = set()
            if len(anum_groups) > 0:
                for anum, group in anum_groups.groupby("a_number"):
                    fds = group["fd"].sort_values().values
                    app_ids_in_group = group["application_id"].values
                    for i in range(len(fds)):
                        for j in range(i + 1, len(fds)):
                            diff = (fds[j] - fds[i]) / np.timedelta64(1, "D")
                            if 0 < diff <= 30:
                                rapid_ids.add(int(app_ids_in_group[i]))
                                rapid_ids.add(int(app_ids_in_group[j]))
            feat["rapid_filing_pair"] = aids.isin(rapid_ids).astype(int)
        else:
            feat["rapid_filing_pair"] = 0
    else:
        feat["rapid_filing_pair"] = 0

    # same_country_burst: >10 apps from same country filed on same date
    feat["same_country_burst"] = 0
    if (len(info_df) > 0 and "country_of_citizenship" in info_df.columns
            and "filing_date" in app_df.columns):
        try:
            combo = app_df[["application_id", "filing_date"]].copy()
            combo["fd"] = _safe_date(combo["filing_date"])
            country_lookup = info_df.set_index("application_id")[
                "country_of_citizenship"]
            combo["country"] = combo["application_id"].map(country_lookup).astype(
                str).str.upper().str.strip()
            combo = combo.dropna(subset=["fd"])
            combo = combo[~combo["country"].isin(["NAN", "NONE", ""])]
            combo["country_date"] = combo["country"] + "|" + combo["fd"].astype(str)
            cd_counts = combo.groupby("country_date")[
                "application_id"].transform("count")
            burst_mask = cd_counts > 10
            burst_ids = set(combo.loc[burst_mask, "application_id"].unique())
            if burst_ids:
                feat["same_country_burst"] = aids.isin(burst_ids).astype(int)
        except Exception:
            pass

    # coordinated_filing: same attorney + same day + same address
    feat["coordinated_filing"] = 0
    if ("atty_state_bar_number" in app_df.columns
            and "filing_date" in app_df.columns and len(addr_df) > 0):
        try:
            cf = app_df[["application_id", "atty_state_bar_number",
                          "filing_date"]].copy()
            cf["fd"] = _safe_date(cf["filing_date"])
            cf = cf.dropna(subset=["atty_state_bar_number", "fd"])
            cf = cf[cf["atty_state_bar_number"].astype(str).str.strip() != ""]
            cf = cf[cf["atty_state_bar_number"].astype(str) != "None"]

            # Get current address for each app
            addr_curr = addr_df[
                addr_df["address_type"].astype(str).str.upper().isin(
                    ["CURRENT_PHYSICAL", "CURRENT PHYSICAL"])
            ].copy()
            if len(addr_curr) > 0 and "street" in addr_curr.columns:
                addr_curr["addr_key"] = (
                    addr_curr["street"].astype(str).str.upper().str.strip() +
                    "|" +
                    addr_curr["city"].astype(str).str.upper().str.strip())
                addr_first = addr_curr.groupby("application_id")[
                    "addr_key"].first()
                cf["addr"] = cf["application_id"].map(addr_first)
                cf = cf.dropna(subset=["addr"])

                # Group key: attorney + date + address
                cf["coord_key"] = (
                    cf["atty_state_bar_number"].astype(str) + "|" +
                    cf["fd"].astype(str) + "|" +
                    cf["addr"])
                ck_counts = cf.groupby("coord_key")[
                    "application_id"].transform("count")
                coord_mask = ck_counts > 1
                coord_ids = set(cf.loc[coord_mask, "application_id"].unique())
                if coord_ids:
                    feat["coordinated_filing"] = aids.isin(
                        coord_ids).astype(int)
        except Exception:
            pass

    # filing_date_very_old
    old_cutoff = pd.Timestamp("2000-01-01")
    feat["filing_date_very_old"] = (
        (filing_dates < old_cutoff) & filing_dates.notna()
    ).astype(int)

    log.info("  1H complete: 10 features (%.1fs)", time.time() - t0)
    return feat


def build_1i_filing_category(app_df, fc_df, feat):
    """1I. Filing Category & Status Features (6 features)."""
    log.info("Building 1I: Filing Category & Status Features ...")
    t0 = time.time()
    aids = feat["application_id"]

    if len(fc_df) == 0 or "application_id" not in fc_df.columns:
        for f_name in ["wrong_category_group", "missing_category",
                       "category_encoded", "rare_category",
                       "status_category_mismatch", "ina_245i_flag"]:
            feat[f_name] = 0
        log.info("  1I complete: 6 features (no data) (%.1fs)",
                 time.time() - t0)
        return feat

    fc_lookup = fc_df.set_index("application_id")

    # wrong_category_group
    if "category_group" in fc_df.columns:
        cg = aids.map(fc_lookup["category_group"]).astype(str).str.upper().str.strip()
        feat["wrong_category_group"] = (cg == "WRONG_GROUP").astype(int)
        feat["missing_category"] = cg.isin(
            ["NAN", "NONE", ""]).astype(int)

        # category_encoded: frequency-encoded
        cg_clean = cg.replace({"NAN": np.nan, "NONE": np.nan, "": np.nan})
        cg_freq = cg_clean.value_counts(normalize=True)
        feat["category_encoded"] = cg_clean.map(cg_freq).fillna(0).round(6)

        # rare_category: used by < 100 apps
        cg_raw_counts = cg_clean.value_counts()
        rare_cats = set(cg_raw_counts[cg_raw_counts < 100].index)
        feat["rare_category"] = cg_clean.isin(rare_cats).astype(int)
    else:
        feat["wrong_category_group"] = 0
        feat["missing_category"] = 1
        feat["category_encoded"] = 0.0
        feat["rare_category"] = 0

    # status_category_mismatch: APPROVED + wrong_category_group
    if "status" in app_df.columns:
        app_status = app_df.set_index("application_id")["status"]
        status_mapped = aids.map(app_status).astype(str).str.upper().str.strip()
        feat["status_category_mismatch"] = (
            (status_mapped == "APPROVED") &
            (feat["wrong_category_group"] == 1)
        ).astype(int)
    else:
        feat["status_category_mismatch"] = 0

    # ina_245i_flag
    ina_col = None
    for candidate in ["ina_245i_status", "ina_245i", "applying_245i"]:
        if candidate in fc_df.columns:
            ina_col = candidate
            break
    if ina_col is not None:
        ina_vals = aids.map(fc_lookup[ina_col])
        feat["ina_245i_flag"] = _to_bool_series(ina_vals).fillna(0).astype(int)
    else:
        feat["ina_245i_flag"] = 0

    log.info("  1I complete: 6 features (%.1fs)", time.time() - t0)
    return feat


def build_1j_cross_table(tables, feat):
    """1J. Cross-Table Consistency (10 features)."""
    log.info("Building 1J: Cross-Table Consistency ...")
    t0 = time.time()
    aids = feat["application_id"]

    expected_tables = ["applicant_info", "eligibility_responses",
                       "public_charge", "filing_category", "biographic_info"]

    def _has_record(tbl_name):
        df = tables.get(tbl_name, pd.DataFrame(columns=["application_id"]))
        if len(df) == 0 or "application_id" not in df.columns:
            return pd.Series(0, index=aids.index)
        present_ids = set(df["application_id"].unique())
        return aids.isin(present_ids).astype(int)

    feat["orphan_application"] = (1 - _has_record("applicant_info")).astype(int)
    feat["missing_eligibility"] = (
        1 - _has_record("eligibility_responses")).astype(int)
    feat["missing_public_charge"] = (
        1 - _has_record("public_charge")).astype(int)
    feat["missing_filing_category"] = (
        1 - _has_record("filing_category")).astype(int)

    # total_missing_tables
    missing_count = (feat["orphan_application"] +
                     feat["missing_eligibility"] +
                     feat["missing_public_charge"] +
                     feat["missing_filing_category"] +
                     (1 - _has_record("biographic_info")))
    feat["total_missing_tables"] = missing_count.astype(int)

    # data_completeness_score
    feat["data_completeness_score"] = (
        (len(expected_tables) - feat["total_missing_tables"]) /
        len(expected_tables)
    ).round(4)

    # name_mismatch_across_tables: names differ between applicant_info and
    # contacts_signatures (APPLICANT contact type)
    feat["name_mismatch_across_tables"] = 0
    try:
        info_t = tables.get("applicant_info", pd.DataFrame())
        contacts_t = tables.get("contacts_signatures", pd.DataFrame())
        if (len(info_t) > 0 and "family_name" in info_t.columns
                and len(contacts_t) > 0 and "family_name" in contacts_t.columns):
            # Get applicant contact records
            app_contacts = contacts_t[
                contacts_t["contact_type"].astype(str).str.upper() == "APPLICANT"
            ].copy()
            if len(app_contacts) > 0:
                info_names = info_t.set_index("application_id")["family_name"].astype(
                    str).str.upper().str.strip()
                contact_names = app_contacts.groupby("application_id")[
                    "family_name"].first().astype(str).str.upper().str.strip()
                both_names = pd.DataFrame({
                    "info_name": aids.map(info_names),
                    "contact_name": aids.map(contact_names),
                })
                valid = both_names.dropna()
                valid = valid[
                    ~valid["info_name"].isin(["NAN", "NONE", ""]) &
                    ~valid["contact_name"].isin(["NAN", "NONE", ""])]
                mismatch = valid["info_name"] != valid["contact_name"]
                feat.loc[mismatch[mismatch].index,
                         "name_mismatch_across_tables"] = 1
    except Exception:
        pass

    # ssn_contradiction: SSN conflicts across tables (applicant_info vs
    # marital_history spouse_ssn matching applicant SSN in another app)
    feat["ssn_contradiction"] = 0
    try:
        info_t = tables.get("applicant_info", pd.DataFrame())
        marital_t = tables.get("marital_history", pd.DataFrame())
        if (len(info_t) > 0 and "ssn" in info_t.columns
                and len(marital_t) > 0 and "spouse_family_name" in marital_t.columns):
            # Check if any applicant's SSN appears as another applicant's
            # record via duplicate SSN detection (already covered by ssn_cluster)
            # Here detect: apps where their own SSN format differs across records
            ssn_lookup = info_t.groupby("application_id")["ssn"].nunique()
            multi_ssn = ssn_lookup[ssn_lookup > 1].index
            if len(multi_ssn) > 0:
                feat["ssn_contradiction"] = aids.isin(
                    set(multi_ssn)).astype(int)
    except Exception:
        pass

    # has_benefits
    feat["has_benefits"] = _has_record("benefits_received")

    # has_institutionalization
    feat["has_institutionalization"] = _has_record("institutionalization")

    log.info("  1J complete: 10 features (%.1fs)", time.time() - t0)
    return feat


def build_1k_employment(emp_df, fc_df, pc_df, info_df, feat):
    """1K. Employment & Education (8 features)."""
    log.info("Building 1K: Employment & Education ...")
    t0 = time.time()
    aids = feat["application_id"]

    # num_employment_records
    if len(emp_df) > 0 and "application_id" in emp_df.columns:
        emp_counts = emp_df.groupby("application_id").size()
        feat["num_employment_records"] = aids.map(
            emp_counts).fillna(0).astype(int)
    else:
        feat["num_employment_records"] = 0

    # employment_gap_max_months: longest gap between sequential jobs
    feat["employment_gap_max_months"] = 0.0
    feat["employment_gap_flag"] = 0
    if (len(emp_df) > 0 and "start_date" in emp_df.columns
            and "end_date" in emp_df.columns):
        try:
            emp_dates = emp_df[["application_id", "start_date", "end_date"]].copy()
            emp_dates["start"] = _safe_date(emp_dates["start_date"])
            emp_dates["end"] = _safe_date(emp_dates["end_date"])
            emp_dates = emp_dates.dropna(subset=["start"])
            # Fill missing end with today for current jobs
            emp_dates["end"] = emp_dates["end"].fillna(pd.Timestamp("2025-01-01"))
            emp_dates = emp_dates.sort_values(["application_id", "end"])

            # Compute gap between consecutive jobs
            emp_dates["prev_end"] = emp_dates.groupby("application_id")["end"].shift(1)
            emp_dates["gap_days"] = (
                emp_dates["start"] - emp_dates["prev_end"]).dt.days
            # Only positive gaps (negative = overlap)
            emp_dates["gap_months"] = emp_dates["gap_days"].clip(lower=0) / 30.44

            max_gap = emp_dates.groupby("application_id")["gap_months"].max()
            feat["employment_gap_max_months"] = aids.map(
                max_gap).fillna(0).round(1)
            feat["employment_gap_flag"] = (
                feat["employment_gap_max_months"] > 12).astype(int)
        except Exception:
            pass

    # zero_employment_with_eb: no employment + category_group=EMPLOYMENT
    if len(fc_df) > 0 and "category_group" in fc_df.columns:
        fc_lookup = fc_df.set_index("application_id")
        cg = aids.map(fc_lookup["category_group"]).astype(str).str.upper()
        feat["zero_employment_with_eb"] = (
            (feat["num_employment_records"] == 0) & (cg == "EMPLOYMENT")
        ).astype(int)
    else:
        feat["zero_employment_with_eb"] = 0

    # overlapping_employment: employment dates overlap across different states
    feat["overlapping_employment"] = 0
    if (len(emp_df) > 0 and "start_date" in emp_df.columns
            and "end_date" in emp_df.columns
            and "employer_state" in emp_df.columns):
        try:
            emp_ov = emp_df[["application_id", "start_date", "end_date",
                              "employer_state"]].copy()
            emp_ov["start"] = _safe_date(emp_ov["start_date"])
            emp_ov["end"] = _safe_date(emp_ov["end_date"])
            emp_ov["end"] = emp_ov["end"].fillna(pd.Timestamp("2025-01-01"))
            emp_ov = emp_ov.dropna(subset=["start"])

            overlap_ids = set()
            # For each app with 2+ jobs, check if any pair overlaps in different states
            multi_emp = emp_ov.groupby("application_id").filter(lambda x: len(x) > 1)
            for aid, group in multi_emp.groupby("application_id"):
                rows = group.sort_values("start").reset_index(drop=True)
                for i in range(len(rows)):
                    for j in range(i + 1, min(i + 5, len(rows))):
                        # Overlap: start_j < end_i AND different states
                        s_i = rows.loc[i, "state"] if "state" in rows.columns else rows.loc[i, "employer_state"]
                        s_j = rows.loc[j, "state"] if "state" in rows.columns else rows.loc[j, "employer_state"]
                        si_str = str(s_i).upper().strip()
                        sj_str = str(s_j).upper().strip()
                        if (rows.loc[j, "start"] < rows.loc[i, "end"]
                                and si_str != sj_str
                                and si_str not in ("NAN", "NONE", "")
                                and sj_str not in ("NAN", "NONE", "")):
                            overlap_ids.add(aid)
                            break
                    if aid in overlap_ids:
                        break
            if overlap_ids:
                feat["overlapping_employment"] = aids.isin(
                    overlap_ids).astype(int)
        except Exception:
            pass

    # employment_before_14: employment start date before applicant was 14
    feat["employment_before_14"] = 0
    if (len(emp_df) > 0 and "start_date" in emp_df.columns
            and len(info_df) > 0 and "date_of_birth" in info_df.columns):
        try:
            emp_age = emp_df[["application_id", "start_date"]].copy()
            emp_age["emp_start"] = _safe_date(emp_age["start_date"])
            dob_lookup = info_df.set_index("application_id")["date_of_birth"]
            dob_series = _safe_date(emp_age["application_id"].map(dob_lookup))
            emp_age["age_at_start"] = (
                (emp_age["emp_start"] - dob_series).dt.days / 365.25)
            under_14 = emp_age[emp_age["age_at_start"] < 14]
            under_14_ids = set(under_14["application_id"].unique())
            if under_14_ids:
                feat["employment_before_14"] = aids.isin(
                    under_14_ids).astype(int)
        except Exception:
            pass

    # education_level_encoded
    if len(pc_df) > 0 and "education_level" in pc_df.columns:
        pc_lookup = pc_df.set_index("application_id")
        edu = aids.map(pc_lookup["education_level"]).astype(str).str.strip()
        edu_map = {
            "less than high school": 0,
            "high school diploma": 1,
            "some college": 2,
            "bachelor's degree": 3,
            "master's degree": 4,
            "doctorate": 5,
        }
        feat["education_level_encoded"] = edu.str.lower().map(
            edu_map).fillna(0).astype(int)
    else:
        feat["education_level_encoded"] = 0

    # education_income_mismatch: low education (< high school) but very high income
    feat["education_income_mismatch"] = 0
    if "income" in feat.columns:
        income_90th = feat["income"][feat["income"] > 0].quantile(0.90)
        if income_90th > 0:
            feat["education_income_mismatch"] = (
                (feat["education_level_encoded"] <= 1) &
                (feat["income"] > income_90th)
            ).astype(int)

    log.info("  1K complete: 8 features (%.1fs)", time.time() - t0)
    return feat


def build_1l_temporal(app_df, info_df, interview_df, addl_info_df, feat):
    """1L. Temporal & Statistical Features (6 features)."""
    log.info("Building 1L: Temporal & Statistical Features ...")
    t0 = time.time()
    aids = feat["application_id"]

    # benford_anum_deviation (from application a_number)
    if "a_number" in app_df.columns:
        anum_vals = app_df.set_index("application_id")["a_number"]
        anum_numeric = anum_vals.astype(str).str.replace(
            r"^A", "", regex=True)
        anum_numeric = pd.to_numeric(anum_numeric, errors="coerce")
        anum_mapped = aids.map(anum_numeric)
        feat["benford_anum_deviation"] = _benford_deviation(anum_mapped).values
    else:
        feat["benford_anum_deviation"] = 0.0

    # days_since_arrival: filing_date - arrival_date
    feat["days_since_arrival"] = 0
    if ("filing_date" in app_df.columns and len(info_df) > 0
            and "arrival_date" in info_df.columns):
        try:
            fd_lookup = app_df.set_index("application_id")["filing_date"]
            fd_mapped = _safe_date(aids.map(fd_lookup))
            arr_lookup = info_df.set_index("application_id")["arrival_date"]
            arr_mapped = _safe_date(aids.map(arr_lookup))
            diff = (fd_mapped - arr_mapped).dt.days
            feat["days_since_arrival"] = diff.fillna(0).astype(int)
        except Exception:
            pass

    # filing_to_interview_days
    if (len(interview_df) > 0 and "interview_date" in interview_df.columns
            and "filing_date" in app_df.columns):
        iv = interview_df[["application_id", "interview_date"]].copy()
        iv["iv_date"] = _safe_date(iv["interview_date"])
        iv_lookup = iv.set_index("application_id")["iv_date"]
        iv_mapped = aids.map(iv_lookup)

        app_fd = app_df.set_index("application_id")["filing_date"]
        fd_mapped = _safe_date(aids.map(app_fd))

        diff_days = (iv_mapped - fd_mapped).dt.days
        feat["filing_to_interview_days"] = diff_days.fillna(0).astype(int)
    else:
        feat["filing_to_interview_days"] = 0

    # has_interview
    if len(interview_df) > 0 and "application_id" in interview_df.columns:
        iv_ids = set(interview_df["application_id"].unique())
        feat["has_interview"] = aids.isin(iv_ids).astype(int)
    else:
        feat["has_interview"] = 0

    # interview_waiver: uscis_interview_waived from application table
    feat["interview_waiver"] = 0
    if "uscis_interview_waived" in app_df.columns:
        try:
            waiver_lookup = app_df.set_index("application_id")[
                "uscis_interview_waived"]
            waiver_vals = aids.map(waiver_lookup)
            feat["interview_waiver"] = _to_bool_series(
                waiver_vals).fillna(0).astype(int)
        except Exception:
            pass

    # prior_applications_count
    if (len(addl_info_df) > 0 and
            "previously_applied_us" in addl_info_df.columns):
        ai_lookup = addl_info_df.set_index("application_id")
        pa_vals = aids.map(ai_lookup["previously_applied_us"])
        feat["prior_applications_count"] = _to_bool_series(
            pa_vals).fillna(0).astype(int)
    else:
        feat["prior_applications_count"] = 0

    log.info("  1L complete: 6 features (%.1fs)", time.time() - t0)
    return feat


# =========================================================================
#  NEW — E1: Marriage Fraud Indicators (7 features)
# =========================================================================

def build_1m_marriage_fraud(app_df, info_df, marital_df, fc_df, feat):
    """1M. Marriage Fraud Indicators (7 features)."""
    log.info("Building 1M: Marriage Fraud Indicators ...")
    t0 = time.time()
    aids = feat["application_id"]

    # ── marriage_to_filing_days ──
    if (len(marital_df) > 0 and "marriage_date" in marital_df.columns
            and "filing_date" in app_df.columns):
        mh = marital_df[["application_id", "sort_order", "marriage_date",
                          "marriage_end_date", "spouse_country_of_birth"]].copy()
        mh["sort_order"] = pd.to_numeric(mh["sort_order"], errors="coerce").fillna(0)
        latest = mh.sort_values("sort_order").groupby("application_id").last()
        latest["m_date"] = _safe_date(latest["marriage_date"])

        fd_lookup = app_df.set_index("application_id")["filing_date"]
        fd_mapped = _safe_date(aids.map(fd_lookup))
        m_mapped = aids.map(latest["m_date"])

        diff = (fd_mapped - m_mapped).dt.days
        feat["marriage_to_filing_days"] = diff.fillna(-9999).astype(int)
        feat["marriage_to_filing_suspicious"] = (
            (diff >= 0) & (diff < 90) & diff.notna()
        ).astype(int)
        feat["marriage_after_filing"] = (
            (m_mapped > fd_mapped) & m_mapped.notna() & fd_mapped.notna()
        ).astype(int)
    else:
        feat["marriage_to_filing_days"] = -9999
        feat["marriage_to_filing_suspicious"] = 0
        feat["marriage_after_filing"] = 0

    # ── serial_petitioner_count ──
    if len(fc_df) > 0 and "principal_a_number" in fc_df.columns:
        pa = fc_df[["application_id", "principal_a_number"]].copy()
        pa = pa.dropna(subset=["principal_a_number"])
        pa = pa[pa["principal_a_number"].astype(str).str.strip().ne("")]
        pa = pa[pa["principal_a_number"].astype(str).ne("None")]
        if len(pa) > 0:
            pa_counts = pa.groupby("principal_a_number")[
                "application_id"].transform("count")
            pa_map = pd.Series(pa_counts.values,
                               index=pa["application_id"].values)
            feat["serial_petitioner_count"] = aids.map(pa_map).fillna(0).astype(int)
        else:
            feat["serial_petitioner_count"] = 0
    else:
        feat["serial_petitioner_count"] = 0
    feat["multiple_marriage_based"] = (
        feat["serial_petitioner_count"] > 2
    ).astype(int)

    # ── rapid_remarriage ──
    if (len(marital_df) > 0 and "marriage_date" in marital_df.columns
            and "marriage_end_date" in marital_df.columns):
        mh2 = marital_df[["application_id", "sort_order",
                           "marriage_date", "marriage_end_date"]].copy()
        mh2["sort_order"] = pd.to_numeric(
            mh2["sort_order"], errors="coerce").fillna(0)
        mh2 = mh2.sort_values(["application_id", "sort_order"])
        mh2["m_date"] = _safe_date(mh2["marriage_date"])
        mh2["end_date"] = _safe_date(mh2["marriage_end_date"])
        mh2["prev_end"] = mh2.groupby("application_id")["end_date"].shift(1)
        mh2["gap_days"] = (mh2["m_date"] - mh2["prev_end"]).dt.days
        rapid = mh2[mh2["gap_days"].between(0, 90)]
        rapid_ids = set(rapid["application_id"].unique())
        feat["rapid_remarriage"] = aids.isin(rapid_ids).astype(int)
    else:
        feat["rapid_remarriage"] = 0

    # ── spouse_country_mismatch ──
    CONTINENT = {
        "UNITED STATES": "NA", "CANADA": "NA", "MEXICO": "NA",
        "BRAZIL": "SA", "COLOMBIA": "SA", "ARGENTINA": "SA", "PERU": "SA",
        "VENEZUELA": "SA", "CHILE": "SA", "ECUADOR": "SA",
        "UNITED KINGDOM": "EU", "FRANCE": "EU", "GERMANY": "EU",
        "ITALY": "EU", "SPAIN": "EU", "POLAND": "EU", "UKRAINE": "EU",
        "RUSSIA": "EU", "ROMANIA": "EU", "NETHERLANDS": "EU",
        "CHINA": "AS", "INDIA": "AS", "JAPAN": "AS", "SOUTH KOREA": "AS",
        "PHILIPPINES": "AS", "VIETNAM": "AS", "PAKISTAN": "AS",
        "BANGLADESH": "AS", "IRAN": "AS", "IRAQ": "AS", "THAILAND": "AS",
        "INDONESIA": "AS", "TAIWAN": "AS", "NEPAL": "AS",
        "NIGERIA": "AF", "ETHIOPIA": "AF", "GHANA": "AF", "KENYA": "AF",
        "SOUTH AFRICA": "AF", "EGYPT": "AF", "CAMEROON": "AF",
        "AUSTRALIA": "OC", "NEW ZEALAND": "OC",
        "EL SALVADOR": "NA", "GUATEMALA": "NA", "HONDURAS": "NA",
        "CUBA": "NA", "HAITI": "NA", "JAMAICA": "NA",
        "DOMINICAN REPUBLIC": "NA", "TRINIDAD AND TOBAGO": "NA",
    }
    if (len(marital_df) > 0 and "spouse_country_of_birth" in marital_df.columns
            and "country_of_citizenship" in info_df.columns):
        latest_spouse = marital_df.copy()
        latest_spouse["sort_order"] = pd.to_numeric(
            latest_spouse["sort_order"], errors="coerce").fillna(0)
        latest_spouse = latest_spouse.sort_values("sort_order").groupby(
            "application_id")["spouse_country_of_birth"].last()
        sp_country = aids.map(latest_spouse).astype(str).str.upper().str.strip()
        sp_cont = sp_country.map(CONTINENT)

        info_lookup = info_df.set_index("application_id")
        app_country = aids.map(
            info_lookup["country_of_citizenship"]
        ).astype(str).str.upper().str.strip()
        app_cont = app_country.map(CONTINENT)

        feat["spouse_country_mismatch"] = (
            (sp_cont != app_cont) & sp_cont.notna() & app_cont.notna()
        ).astype(int)
    else:
        feat["spouse_country_mismatch"] = 0

    log.info("  1M complete: 7 features (%.1fs)", time.time() - t0)
    return feat


# =========================================================================
#  NEW — E2: Temporal Velocity Features (9 features)
# =========================================================================

def _rolling_count_vectorized(df, group_col, date_col, window_days):
    """Count rows in same group within ±window_days for each row."""
    result = pd.Series(0, index=df.index, dtype=int)
    delta = np.timedelta64(window_days, "D")
    for _, grp in df.groupby(group_col):
        if len(grp) < 2:
            result.loc[grp.index] = 1
            continue
        dates = grp[date_col].values
        idx = grp.index.values
        sorted_order = np.argsort(dates)
        dates_sorted = dates[sorted_order]
        idx_sorted = idx[sorted_order]
        for i in range(len(dates_sorted)):
            d = dates_sorted[i]
            if np.isnat(d):
                continue
            lo = np.searchsorted(dates_sorted, d - delta)
            hi = np.searchsorted(dates_sorted, d + delta, side="right")
            result.loc[idx_sorted[i]] = hi - lo
    return result


def build_1n_temporal_velocity(app_df, contacts_df, addr_df, feat):
    """1N. Temporal Velocity Features (9 features)."""
    log.info("Building 1N: Temporal Velocity Features ...")
    t0 = time.time()
    aids = feat["application_id"]

    # Default all to 0
    for f in ["atty_7d_rolling_count", "atty_30d_rolling_count",
              "atty_90d_rolling_count", "preparer_7d_rolling_count",
              "preparer_30d_rolling_count", "address_30d_filing_count",
              "atty_filing_acceleration", "filing_hour_suspicious",
              "volume_anomaly_zscore"]:
        feat[f] = 0

    if "filing_date" not in app_df.columns:
        log.info("  1N complete: 9 features (no filing_date) (%.1fs)",
                 time.time() - t0)
        return feat

    # Prepare attorney + filing date
    if "atty_state_bar_number" in app_df.columns:
        atty_df = app_df[["application_id", "atty_state_bar_number",
                           "filing_date"]].copy()
        atty_df = atty_df.dropna(subset=["atty_state_bar_number"])
        atty_df = atty_df[
            atty_df["atty_state_bar_number"].astype(str).str.strip().ne("")]
        atty_df = atty_df[
            atty_df["atty_state_bar_number"].astype(str).ne("None")]
        atty_df["fd"] = _safe_date(atty_df["filing_date"])
        atty_df = atty_df.dropna(subset=["fd"])

        if len(atty_df) > 0:
            log.info("    Computing attorney rolling windows (%d records)...",
                     len(atty_df))
            for window, col in [(7, "atty_7d_rolling_count"),
                                (30, "atty_30d_rolling_count"),
                                (90, "atty_90d_rolling_count")]:
                counts = _rolling_count_vectorized(
                    atty_df, "atty_state_bar_number", "fd", window)
                count_map = pd.Series(counts.values,
                                      index=atty_df["application_id"].values)
                feat[col] = aids.map(count_map).fillna(0).astype(int)

            # atty_filing_acceleration: (30d / 90d) * 3
            a30 = feat["atty_30d_rolling_count"].astype(float)
            a90 = feat["atty_90d_rolling_count"].replace(0, np.nan).astype(float)
            feat["atty_filing_acceleration"] = (
                (a30 / a90) * 3.0
            ).fillna(0.0).round(4)

    # Preparer rolling windows
    if (len(contacts_df) > 0 and "contact_type" in contacts_df.columns
            and "filing_date" in app_df.columns):
        prep = contacts_df[
            contacts_df["contact_type"].astype(str).str.upper() == "PREPARER"
        ].copy()
        if len(prep) > 0 and "family_name" in prep.columns:
            prep["prep_key"] = (
                prep["family_name"].astype(str).str.upper() + "|" +
                prep.get("given_name", pd.Series(
                    "", index=prep.index)).astype(str).str.upper()
            )
            # Merge filing date from app
            fd_map = app_df.set_index("application_id")["filing_date"]
            prep["fd"] = _safe_date(prep["application_id"].map(fd_map))
            prep = prep.dropna(subset=["fd"])
            if len(prep) > 0:
                log.info("    Computing preparer rolling windows (%d records)...",
                         len(prep))
                for window, col in [(7, "preparer_7d_rolling_count"),
                                    (30, "preparer_30d_rolling_count")]:
                    counts = _rolling_count_vectorized(
                        prep, "prep_key", "fd", window)
                    count_map = pd.Series(
                        counts.values,
                        index=prep["application_id"].values)
                    feat[col] = aids.map(count_map).fillna(0).astype(int)

    # Address 30-day filing count
    if (len(addr_df) > 0 and "filing_date" in app_df.columns):
        current_mask = addr_df["address_type"].astype(str).str.upper().isin(
            ["CURRENT_PHYSICAL", "CURRENT PHYSICAL"])
        ca = addr_df[current_mask][["application_id", "street", "city",
                                     "state"]].copy()
        if len(ca) > 0:
            ca["addr_key"] = (
                ca["street"].astype(str).str.upper() + "|" +
                ca["city"].astype(str).str.upper() + "|" +
                ca["state"].astype(str).str.upper()
            )
            fd_map2 = app_df.set_index("application_id")["filing_date"]
            ca["fd"] = _safe_date(ca["application_id"].map(fd_map2))
            ca = ca.dropna(subset=["fd"])
            if len(ca) > 0:
                log.info("    Computing address 30d filing count (%d records)...",
                         len(ca))
                counts = _rolling_count_vectorized(ca, "addr_key", "fd", 30)
                count_map = pd.Series(counts.values,
                                      index=ca["application_id"].values)
                feat["address_30d_filing_count"] = aids.map(
                    count_map).fillna(0).astype(int)

    # Filing hour suspicious (from contacts signature_date)
    if (len(contacts_df) > 0 and "signature_date" in contacts_df.columns):
        app_sigs = contacts_df[
            contacts_df["contact_type"].astype(str).str.upper() == "APPLICANT"
        ][["application_id", "signature_date"]].copy()
        if len(app_sigs) > 0:
            app_sigs["sig_dt"] = pd.to_datetime(
                app_sigs["signature_date"], errors="coerce")
            app_sigs["hour"] = app_sigs["sig_dt"].dt.hour
            # Flag outside 7am-8pm or if hour is not 0 (meaning we have time data)
            has_time = app_sigs["hour"].notna() & (app_sigs["hour"] != 0)
            suspicious = has_time & (
                (app_sigs["hour"] < 7) | (app_sigs["hour"] >= 20))
            sus_ids = set(app_sigs.loc[suspicious, "application_id"].unique())
            feat["filing_hour_suspicious"] = aids.isin(sus_ids).astype(int)

    # Volume anomaly z-score (weekly)
    if "filing_date" in app_df.columns:
        app_fd = app_df[["application_id", "filing_date"]].copy()
        app_fd["fd"] = _safe_date(app_fd["filing_date"])
        app_fd = app_fd.dropna(subset=["fd"])
        app_fd["week_num"] = (
            app_fd["fd"] - app_fd["fd"].min()
        ).dt.days // 7
        weekly = app_fd.groupby("week_num").size()
        if len(weekly) >= 4:
            roll_mean = weekly.rolling(4, min_periods=1).mean()
            roll_std = weekly.rolling(4, min_periods=1).std().fillna(1).replace(0, 1)
            week_zscore = (weekly - roll_mean) / roll_std
            app_fd_z = app_fd.set_index("application_id")["week_num"].map(
                week_zscore)
            feat["volume_anomaly_zscore"] = aids.map(
                app_fd_z).fillna(0.0).round(4)

    log.info("  1N complete: 9 features (%.1fs)", time.time() - t0)
    return feat


# =========================================================================
#  NEW — E3: NLP Text Similarity (5 features)
# =========================================================================

def build_1o_nlp_similarity(addl_text_df, app_df, feat):
    """1O. NLP Text Similarity / Boilerplate Detection (5 features)."""
    log.info("Building 1O: NLP Text Similarity ...")
    t0 = time.time()
    aids = feat["application_id"]

    feat["has_additional_text"] = 0
    feat["additional_text_length"] = 0
    feat["text_similarity_max_same_atty"] = 0.0
    feat["text_similarity_above_085"] = 0
    feat["text_duplicate_cluster_size"] = 0

    if (len(addl_text_df) == 0 or "additional_text" not in addl_text_df.columns):
        log.info("  1O complete: 5 features (no text data) (%.1fs)",
                 time.time() - t0)
        return feat

    # Concatenate text per app
    text_agg = addl_text_df.groupby("application_id")[
        "additional_text"
    ].apply(lambda x: " ".join(str(v) for v in x if pd.notna(v))).reset_index()
    text_agg.columns = ["application_id", "full_text"]
    text_agg = text_agg[text_agg["full_text"].str.strip().ne("")]

    if len(text_agg) == 0:
        log.info("  1O complete: 5 features (empty text) (%.1fs)",
                 time.time() - t0)
        return feat

    has_text_ids = set(text_agg["application_id"])
    feat["has_additional_text"] = aids.isin(has_text_ids).astype(int)

    len_map = text_agg.set_index("application_id")["full_text"].str.len()
    feat["additional_text_length"] = aids.map(len_map).fillna(0).astype(int)

    # Need attorney info for similarity comparison
    if "atty_state_bar_number" not in app_df.columns:
        log.info("  1O complete: 5 features (no attorney col) (%.1fs)",
                 time.time() - t0)
        return feat

    text_atty = text_agg.merge(
        app_df[["application_id", "atty_state_bar_number"]],
        on="application_id", how="left"
    )
    text_atty = text_atty.dropna(subset=["atty_state_bar_number"])
    text_atty = text_atty[
        text_atty["atty_state_bar_number"].astype(str).str.strip().ne("")]
    text_atty = text_atty[
        text_atty["atty_state_bar_number"].astype(str).ne("None")]

    if len(text_atty) < 2:
        log.info("  1O complete: 5 features (too few text records) (%.1fs)",
                 time.time() - t0)
        return feat

    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity as cos_sim
    except ImportError:
        log.warning("  sklearn not available for NLP — skipping similarity")
        log.info("  1O complete: 5 features (%.1fs)", time.time() - t0)
        return feat

    max_sim_map = {}
    cluster_size_map = {}
    MAX_GROUP = 500

    atty_groups = text_atty.groupby("atty_state_bar_number")
    n_groups = sum(1 for _, g in atty_groups if len(g) >= 2)
    log.info("    Computing TF-IDF cosine similarity for %d attorney groups...",
             n_groups)

    for atty, grp in text_atty.groupby("atty_state_bar_number"):
        if len(grp) < 2:
            continue
        g = grp.head(MAX_GROUP)
        try:
            tfidf = TfidfVectorizer(
                max_features=5000, stop_words="english",
                min_df=1, max_df=0.95)
            X = tfidf.fit_transform(g["full_text"])
            sim = cos_sim(X)
            np.fill_diagonal(sim, 0)

            app_ids_in_grp = g["application_id"].values
            for i in range(len(app_ids_in_grp)):
                aid = int(app_ids_in_grp[i])
                ms = float(sim[i].max())
                max_sim_map[aid] = ms
                cluster_size_map[aid] = int((sim[i] >= 0.85).sum()) + 1
        except Exception:
            continue

    if max_sim_map:
        sim_series = pd.Series(max_sim_map)
        feat["text_similarity_max_same_atty"] = aids.map(
            sim_series).fillna(0.0).round(4)
        feat["text_similarity_above_085"] = (
            feat["text_similarity_max_same_atty"] >= 0.85
        ).astype(int)
        clust_series = pd.Series(cluster_size_map)
        feat["text_duplicate_cluster_size"] = aids.map(
            clust_series).fillna(0).astype(int)

    log.info("  1O complete: 5 features (%.1fs)", time.time() - t0)
    return feat


# =========================================================================
#  NEW — E4: Employment & Shell Company Detection (7 features)
# =========================================================================

def build_1p_employment_enhanced(emp_df, pc_df, app_df, addr_df, feat):
    """1P. Employment & Shell Company Detection (7 features)."""
    log.info("Building 1P: Employment & Shell Company Detection ...")
    t0 = time.time()
    aids = feat["application_id"]

    for f in ["employer_app_count", "employer_high_volume",
              "employer_extreme_volume", "employer_atty_concentration",
              "salary_occupation_zscore", "employment_gap_max_days",
              "employment_state_address_mismatch"]:
        feat[f] = 0

    if len(emp_df) == 0 or "application_id" not in emp_df.columns:
        log.info("  1P complete: 7 features (no emp data) (%.1fs)",
                 time.time() - t0)
        return feat

    # Current employer
    is_current = emp_df.get("is_current", pd.Series(dtype="object"))
    if is_current is not None and len(is_current) > 0:
        current_emp = emp_df[
            _to_bool_series(is_current).fillna(0).astype(bool)
        ].copy()
    else:
        current_emp = emp_df.copy()

    # ── employer_app_count ──
    if "employer_name" in current_emp.columns and len(current_emp) > 0:
        ce = current_emp[["application_id", "employer_name"]].copy()
        ce["emp_norm"] = ce["employer_name"].astype(str).str.upper().str.strip()
        ce["emp_norm"] = ce["emp_norm"].str.replace(
            r"\s+", " ", regex=True)
        ce = ce[~ce["emp_norm"].isin(["NAN", "NONE", ""])]
        if len(ce) > 0:
            emp_counts = ce.groupby("emp_norm")[
                "application_id"].transform("count")
            emp_map = pd.Series(emp_counts.values,
                                index=ce["application_id"].values)
            feat["employer_app_count"] = aids.map(emp_map).fillna(0).astype(int)
    feat["employer_high_volume"] = (
        feat["employer_app_count"] > 20).astype(int)
    feat["employer_extreme_volume"] = (
        feat["employer_app_count"] > 100).astype(int)

    # ── employer_atty_concentration ──
    if ("employer_name" in current_emp.columns
            and "atty_state_bar_number" in app_df.columns
            and len(current_emp) > 0):
        ce2 = current_emp[["application_id", "employer_name"]].copy()
        ce2["emp_norm"] = ce2["employer_name"].astype(str).str.upper().str.strip()
        ce2["emp_norm"] = ce2["emp_norm"].str.replace(r"\s+", " ", regex=True)
        ce2 = ce2[~ce2["emp_norm"].isin(["NAN", "NONE", ""])]
        ce2 = ce2.merge(
            app_df[["application_id", "atty_state_bar_number"]],
            on="application_id", how="left")
        if len(ce2) > 0:
            emp_atty = ce2.groupby("emp_norm").agg(
                app_count=("application_id", "count"),
                atty_count=("atty_state_bar_number", "nunique")
            )
            emp_atty["concentration"] = (
                emp_atty["app_count"] /
                emp_atty["atty_count"].replace(0, 1)
            ).round(4)
            conc_map = ce2.merge(
                emp_atty[["concentration"]], left_on="emp_norm",
                right_index=True, how="left")
            conc_aid_map = pd.Series(
                conc_map["concentration"].values,
                index=conc_map["application_id"].values)
            feat["employer_atty_concentration"] = aids.map(
                conc_aid_map).fillna(0.0).round(4)

    # ── salary_occupation_zscore ──
    if ("occupation" in emp_df.columns and "income" in feat.columns
            and len(current_emp) > 0):
        occ = current_emp[["application_id", "occupation"]].copy()
        occ["occ_norm"] = occ["occupation"].astype(str).str.upper().str.strip()
        occ = occ[~occ["occ_norm"].isin(["NAN", "NONE", ""])]
        occ_income = occ.merge(
            feat[["application_id", "income"]],
            on="application_id", how="left")
        if len(occ_income) > 0:
            occ_stats = occ_income.groupby("occ_norm")["income"].agg(
                ["mean", "std", "count"])
            occ_stats = occ_stats[occ_stats["count"] >= 10]
            occ_stats["std"] = occ_stats["std"].replace(0, 1)
            occ_income = occ_income.merge(
                occ_stats[["mean", "std"]], left_on="occ_norm",
                right_index=True, how="left")
            occ_income["zscore"] = (
                (occ_income["income"] - occ_income["mean"]) /
                occ_income["std"]
            ).fillna(0).round(4)
            z_map = pd.Series(occ_income["zscore"].values,
                              index=occ_income["application_id"].values)
            feat["salary_occupation_zscore"] = aids.map(
                z_map).fillna(0.0).round(4)

    # ── employment_gap_max_days ──
    if ("start_date" in emp_df.columns and "end_date" in emp_df.columns):
        eg = emp_df[["application_id", "start_date", "end_date"]].copy()
        eg["sd"] = _safe_date(eg["start_date"])
        eg["ed"] = _safe_date(eg["end_date"])
        eg = eg.dropna(subset=["sd"]).sort_values(
            ["application_id", "sd"])
        eg["prev_end"] = eg.groupby("application_id")["ed"].shift(1)
        eg["gap"] = (eg["sd"] - eg["prev_end"]).dt.days
        max_gap = eg.groupby("application_id")["gap"].max()
        feat["employment_gap_max_days"] = aids.map(
            max_gap).fillna(0).astype(int)

    # ── employment_state_address_mismatch ──
    if ("employer_state" in current_emp.columns and len(addr_df) > 0
            and "state" in addr_df.columns):
        emp_state = current_emp.groupby("application_id")[
            "employer_state"].first()
        ca = addr_df[addr_df["address_type"].astype(str).str.upper().isin(
            ["CURRENT_PHYSICAL", "CURRENT PHYSICAL"])]
        addr_state = ca.groupby("application_id")["state"].first()
        emp_s = aids.map(emp_state).astype(str).str.upper().str.strip()
        addr_s = aids.map(addr_state).astype(str).str.upper().str.strip()
        feat["employment_state_address_mismatch"] = (
            (emp_s != addr_s) &
            (~emp_s.isin(["NAN", "NONE", ""])) &
            (~addr_s.isin(["NAN", "NONE", ""]))
        ).astype(int)

    log.info("  1P complete: 7 features (%.1fs)", time.time() - t0)
    return feat


# =========================================================================
#  NEW — E5: Geographic Features (6 features)
# =========================================================================

def build_1q_geographic(addr_df, info_df, emp_df, labels_df, feat):
    """1Q. Geographic Features (6 features)."""
    log.info("Building 1Q: Geographic Features ...")
    t0 = time.time()
    aids = feat["application_id"]

    for f in ["zip_fraud_rate", "po_box_address", "zip_density_apps",
              "foreign_address_domestic", "address_unrelated_families",
              "multi_state_employment"]:
        feat[f] = 0

    # Get current physical addresses
    if len(addr_df) > 0 and "address_type" in addr_df.columns:
        current = addr_df[
            addr_df["address_type"].astype(str).str.upper().isin(
                ["CURRENT_PHYSICAL", "CURRENT PHYSICAL"])
        ].copy()
    else:
        current = pd.DataFrame(columns=["application_id"])
        log.info("  1Q complete: 6 features (no addr data) (%.1fs)",
                 time.time() - t0)
        return feat

    # ── zip_fraud_rate (Bayesian smoothed) ──
    if "zip_code" in current.columns and labels_df is not None:
        ca_zip = current[["application_id", "zip_code"]].copy()
        ca_zip = ca_zip.dropna(subset=["zip_code"])
        ca_zip["zip5"] = ca_zip["zip_code"].astype(str).str[:5]
        ca_zip = ca_zip.merge(
            labels_df[["application_id", "is_fraud"]],
            on="application_id", how="left")
        zip_stats = ca_zip.groupby("zip5").agg(
            total=("application_id", "count"),
            fraud=("is_fraud", "sum"))
        # Bayesian: (fraud + 1) / (total + 10)
        zip_stats["rate"] = (
            (zip_stats["fraud"] + 1) / (zip_stats["total"] + 10)
        ).round(6)
        # Map to app via their zip
        app_zip = current.groupby("application_id")["zip_code"].first()
        app_zip5 = app_zip.astype(str).str[:5]
        app_rate = app_zip5.map(zip_stats["rate"])
        feat["zip_fraud_rate"] = aids.map(app_rate).fillna(0.0).round(6)

    # ── po_box_address ──
    if "street" in current.columns:
        streets = current.groupby("application_id")["street"].first()
        street_mapped = aids.map(streets).astype(str).str.upper()
        feat["po_box_address"] = street_mapped.str.contains(
            r"P\.?\s*O\.?\s*BOX|POST\s+OFFICE\s+BOX",
            regex=True, na=False
        ).astype(int)

    # ── zip_density_apps ──
    if "zip_code" in current.columns:
        ca_zip2 = current[["application_id", "zip_code"]].copy()
        ca_zip2["zip5"] = ca_zip2["zip_code"].astype(str).str[:5]
        zip_counts = ca_zip2.groupby("zip5")["application_id"].transform("count")
        zc_map = pd.Series(zip_counts.values,
                           index=ca_zip2["application_id"].values)
        feat["zip_density_apps"] = aids.map(zc_map).fillna(0).astype(int)

    # ── foreign_address_domestic ──
    if "country" in current.columns:
        addr_country = current.groupby("application_id")["country"].first()
        c_mapped = aids.map(addr_country).astype(str).str.upper().str.strip()
        feat["foreign_address_domestic"] = (
            ~c_mapped.isin(["US", "USA", "UNITED STATES", "NAN", "NONE", ""])
            & c_mapped.notna()
        ).astype(int)

    # ── address_unrelated_families ──
    if ("street" in current.columns and "city" in current.columns
            and "state" in current.columns and "family_name" in info_df.columns):
        ca2 = current[["application_id", "street", "city", "state"]].copy()
        ca2["addr_key"] = (
            ca2["street"].astype(str).str.upper() + "|" +
            ca2["city"].astype(str).str.upper() + "|" +
            ca2["state"].astype(str).str.upper()
        )
        fn_lookup = info_df.set_index("application_id")["family_name"]
        ca2["family_name"] = ca2["application_id"].map(fn_lookup)
        addr_families = ca2.groupby("addr_key")["family_name"].nunique()
        ca2_count = ca2.merge(
            addr_families.rename("n_families").reset_index(),
            on="addr_key", how="left")
        fam_map = pd.Series(
            ca2_count["n_families"].values,
            index=ca2_count["application_id"].values)
        feat["address_unrelated_families"] = (
            aids.map(fam_map).fillna(0) > 5
        ).astype(int)

    # ── multi_state_employment ──
    if (len(emp_df) > 0 and "employer_state" in emp_df.columns):
        es = emp_df[["application_id", "employer_state"]].copy()
        es = es.dropna(subset=["employer_state"])
        es = es[es["employer_state"].astype(str).str.strip().ne("")]
        if len(es) > 0:
            state_count = es.groupby("application_id")[
                "employer_state"].nunique()
            feat["multi_state_employment"] = aids.map(
                state_count).fillna(0).astype(int)

    log.info("  1Q complete: 6 features (%.1fs)", time.time() - t0)
    return feat


# =========================================================================
#  Label construction
# =========================================================================

def build_labels(app_df):
    """Build fraud labels from manifests."""
    log.info("Building labels from manifests ...")

    fraud_manifest_path = os.path.join(CSV_DIR, "_fraud_manifest.csv")
    anomaly_manifest_path = os.path.join(CSV_DIR, "_anomaly_manifest.csv")

    all_ids = app_df["application_id"].copy()

    # Fraud manifest
    fraud_ids = set()
    fraud_type_map = {}
    if os.path.exists(fraud_manifest_path):
        fraud_manifest = pd.read_csv(fraud_manifest_path)
        fraud_ids = set(fraud_manifest["application_id"].unique())
        if "fraud_pattern" in fraud_manifest.columns:
            # Pick the first fraud pattern per app as the type
            ft = fraud_manifest.groupby("application_id")[
                "fraud_pattern"].first()
            fraud_type_map = ft.to_dict()
        log.info("  Fraud manifest: %d entries, %d unique apps",
                 len(fraud_manifest), len(fraud_ids))
    else:
        log.warning("  Fraud manifest not found at %s", fraud_manifest_path)

    # Anomaly manifest (loaded for reference, not used in label directly)
    if os.path.exists(anomaly_manifest_path):
        anomaly_manifest = pd.read_csv(anomaly_manifest_path)
        log.info("  Anomaly manifest: %d entries, %d unique apps",
                 len(anomaly_manifest),
                 anomaly_manifest["application_id"].nunique())
    else:
        log.warning("  Anomaly manifest not found at %s",
                    anomaly_manifest_path)

    labels_df = pd.DataFrame({
        "application_id": all_ids,
        "is_fraud": all_ids.isin(fraud_ids).astype(int),
        "fraud_type": all_ids.map(fraud_type_map).fillna("CLEAN"),
    })

    return labels_df


# =========================================================================
#  Main pipeline
# =========================================================================

def main():
    """Run the full feature engineering pipeline."""
    pipeline_t0 = time.time()
    log.info("=" * 70)
    log.info("I-485 Feature Engineering Pipeline")
    log.info("=" * 70)
    log.info("Project root : %s", PROJECT_ROOT)
    log.info("Parquet dir  : %s", PARQUET_DIR)
    log.info("Output dir   : %s", OUTPUT_DIR)

    # ── Load tables ───────────────────────────────────────────────────────
    log.info("")
    log.info("Loading tables ...")
    tables = load_all_tables()

    app_df = tables["application"]
    info_df = tables["applicant_info"]
    fc_df = tables["filing_category"]
    pc_df = tables["public_charge"]
    elig_df = tables["eligibility_responses"]
    bio_df = tables["biographic_info"]
    addr_df = tables["addresses"]
    emp_df = tables["employment_history"]
    parents_df = tables["parents"]
    contacts_df = tables["contacts_signatures"]
    children_df = tables["children"]
    marital_df = tables["marital_history"]
    other_names_df = tables["other_names"]
    interview_df = tables["interview_signature"]
    addl_info_df = tables["additional_info"]

    if len(app_df) == 0:
        log.error("Application table is empty — cannot proceed.")
        sys.exit(1)

    # ── Initialize feature DataFrame ─────────────────────────────────────
    feat = pd.DataFrame({"application_id": app_df["application_id"].copy()})
    log.info("")
    log.info("Base application count: %d", len(feat))
    log.info("")

    # ── Build all feature groups ─────────────────────────────────────────
    feat = build_1a_identity(app_df, info_df, other_names_df, feat)
    feat = build_1b_demographic(app_df, info_df, bio_df, feat)
    feat = build_1c_family(app_df, info_df, children_df, marital_df,
                           parents_df, contacts_df, fc_df, feat)
    feat = build_1d_financial(pc_df, feat)
    feat = build_1e_eligibility(app_df, elig_df, feat)
    feat = build_1f_attorney(app_df, contacts_df, feat)
    feat = build_1g_address(addr_df, emp_df, info_df, feat)
    feat = build_1h_filing_patterns(app_df, info_df, addr_df, feat)
    feat = build_1i_filing_category(app_df, fc_df, feat)
    feat = build_1j_cross_table(tables, feat)
    feat = build_1k_employment(emp_df, fc_df, pc_df, info_df, feat)
    feat = build_1l_temporal(app_df, info_df, interview_df, addl_info_df, feat)

    # ── NEW: Enhanced feature groups (E1–E5) ─────────────────────────────
    addl_text_df = tables["additional_information"]
    feat = build_1m_marriage_fraud(app_df, info_df, marital_df, fc_df, feat)
    feat = build_1n_temporal_velocity(app_df, contacts_df, addr_df, feat)
    feat = build_1o_nlp_similarity(addl_text_df, app_df, feat)
    feat = build_1p_employment_enhanced(emp_df, pc_df, app_df, addr_df, feat)

    # ── Build labels (needed before E5 geographic) ────────────────────────
    log.info("")
    labels_df = build_labels(app_df)

    # ── E5: Geographic features (needs labels for zip_fraud_rate) ────────
    feat = build_1q_geographic(addr_df, info_df, emp_df, labels_df, feat)

    # ── Post-processing ──────────────────────────────────────────────────
    log.info("")
    log.info("Post-processing ...")

    # Drop the application_id from feature columns (keep it as index reference)
    feature_cols = [c for c in feat.columns if c != "application_id"]

    # Fill remaining NaN with 0 for numeric features
    for col in feature_cols:
        if feat[col].dtype in [np.float64, np.float32, np.int64, np.int32,
                               "float64", "float32", "int64", "int32"]:
            feat[col] = feat[col].fillna(0)
        else:
            feat[col] = pd.to_numeric(feat[col], errors="coerce").fillna(0)

    # ── Save output ──────────────────────────────────────────────────────
    log.info("Saving output ...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    feat_path = os.path.join(OUTPUT_DIR, "feature_matrix.parquet")
    labels_path = os.path.join(OUTPUT_DIR, "labels.parquet")

    feat.to_parquet(feat_path, index=False)
    labels_df.to_parquet(labels_path, index=False)

    log.info("  Feature matrix : %s", feat_path)
    log.info("  Labels         : %s", labels_path)

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.time() - pipeline_t0
    log.info("")
    log.info("=" * 70)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 70)
    log.info("  Total rows        : %d", len(feat))
    log.info("  Total features    : %d", len(feature_cols))
    log.info("  Feature columns   :")
    for i, col in enumerate(feature_cols):
        log.info("    %3d. %-40s  (non-zero: %d)", i + 1, col,
                 (feat[col] != 0).sum())
    log.info("")
    log.info("  Label distribution:")
    label_counts = labels_df["is_fraud"].value_counts()
    for val, cnt in label_counts.items():
        label_name = "FRAUD" if val == 1 else "CLEAN"
        log.info("    %s : %d (%.2f%%)", label_name, cnt,
                 100.0 * cnt / len(labels_df))
    if "fraud_type" in labels_df.columns:
        log.info("")
        log.info("  Fraud type breakdown:")
        for ftype, cnt in labels_df[
                labels_df["is_fraud"] == 1]["fraud_type"].value_counts().items():
            log.info("    %-30s : %d", ftype, cnt)
    log.info("")
    log.info("  Output size:")
    if os.path.exists(feat_path):
        log.info("    feature_matrix.parquet : %.1f MB",
                 os.path.getsize(feat_path) / 1024 / 1024)
    if os.path.exists(labels_path):
        log.info("    labels.parquet         : %.1f MB",
                 os.path.getsize(labels_path) / 1024 / 1024)
    log.info("  Elapsed time      : %.1f seconds", elapsed)
    log.info("=" * 70)


if __name__ == "__main__":
    main()
