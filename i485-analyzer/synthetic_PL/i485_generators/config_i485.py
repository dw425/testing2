"""I-485 synthetic data generation — constants, distributions, and I/O helpers.

This module is imported by all generators and the profile builder.
Nothing here is executable on its own.
"""

import os
import sys

import numpy as np
import pandas as pd

# Allow imports from parent package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_DIR, SEED

# ── Output directories ──────────────────────────────────────────────────────
I485_OUTPUT = os.path.join(DATA_DIR, "i485_form")
I485_CSV = os.path.join(I485_OUTPUT, "csv")
I485_PQ = os.path.join(I485_OUTPUT, "parquet")

for _d in (I485_CSV, I485_PQ):
    os.makedirs(_d, exist_ok=True)

# ── Default record counts ───────────────────────────────────────────────────
CLEAN_COUNT = 140_000
ANOMALY_COUNT = 60_000
TOTAL_COUNT = 200_000
FRAUD_OVERLAY_COUNT = 15_000

# ── Filing category distribution ────────────────────────────────────────────
# (code, group, weight)  — weights sum to 1.0
FILING_CATEGORIES = [
    # Family — 65 %
    ("FAM_IR_SPOUSE",       "FAMILY",              0.1800),
    ("FAM_IR_CHILD",        "FAMILY",              0.0800),
    ("FAM_IR_PARENT",       "FAMILY",              0.0700),
    ("FAM_F2A",             "FAMILY",              0.0800),
    ("FAM_F2B_CHILD",       "FAMILY",              0.0400),
    ("FAM_F2B_ADULT",       "FAMILY",              0.0300),
    ("FAM_F1",              "FAMILY",              0.0400),
    ("FAM_F3",              "FAMILY",              0.0300),
    ("FAM_F4",              "FAMILY",              0.0400),
    ("FAM_IR_K1K2",         "FAMILY",              0.0200),
    ("FAM_IR_WIDOW",        "FAMILY",              0.0100),
    ("FAM_IR_NDAA",         "FAMILY",              0.0050),
    ("FAM_VAWA_SPOUSE",     "FAMILY",              0.0150),
    ("FAM_VAWA_CHILD",      "FAMILY",              0.0050),
    ("FAM_VAWA_PARENT",     "FAMILY",              0.0050),
    # Employment — 20 %
    ("EMP_EB1_EXTRAORDINARY", "EMPLOYMENT",        0.0200),
    ("EMP_EB1_PROFESSOR",     "EMPLOYMENT",        0.0150),
    ("EMP_EB1_MANAGER",       "EMPLOYMENT",        0.0250),
    ("EMP_EB2_ADV_DEGREE",    "EMPLOYMENT",        0.0400),
    ("EMP_EB2_NIW",           "EMPLOYMENT",        0.0200),
    ("EMP_EB3_SKILLED",       "EMPLOYMENT",        0.0300),
    ("EMP_EB3_PROFESSIONAL",  "EMPLOYMENT",        0.0200),
    ("EMP_EB3_OTHER",         "EMPLOYMENT",        0.0150),
    ("EMP_INVESTOR",          "EMPLOYMENT",        0.0150),
    # Asylee / Refugee — 8 %
    ("AR_ASYLEE",             "ASYLEE_REFUGEE",    0.0500),
    ("AR_REFUGEE",            "ASYLEE_REFUGEE",    0.0300),
    # Special Immigrant — 3 %
    ("SI_SIJ",                "SPECIAL_IMMIGRANT", 0.0060),
    ("SI_RELIGIOUS",          "SPECIAL_IMMIGRANT", 0.0050),
    ("SI_ARMED_FORCES",       "SPECIAL_IMMIGRANT", 0.0040),
    ("SI_IRAQAFGHAN_SIV",     "SPECIAL_IMMIGRANT", 0.0040),
    ("SI_BROADCASTER",        "SPECIAL_IMMIGRANT", 0.0020),
    ("SI_INTERNATIONAL_ORG",  "SPECIAL_IMMIGRANT", 0.0020),
    ("SI_NATO",               "SPECIAL_IMMIGRANT", 0.0020),
    ("SI_PANAMA_CANAL",       "SPECIAL_IMMIGRANT", 0.0010),
    ("SI_PHYSICIAN",          "SPECIAL_IMMIGRANT", 0.0020),
    ("SI_OTHER",              "SPECIAL_IMMIGRANT", 0.0020),
    # Special Program — 2 %
    ("SP_DIVERSITY_VISA",     "SPECIAL_PROGRAM",   0.0050),
    ("SP_CUBAN_ADJUSTMENT",   "SPECIAL_PROGRAM",   0.0030),
    ("SP_NACARA",             "SPECIAL_PROGRAM",   0.0020),
    ("SP_HRIFA",              "SPECIAL_PROGRAM",   0.0020),
    ("SP_REGISTRY",           "SPECIAL_PROGRAM",   0.0020),
    ("SP_LULAC",              "SPECIAL_PROGRAM",   0.0010),
    ("SP_DIPLOMATS",          "SPECIAL_PROGRAM",   0.0010),
    ("SP_BORN_US_ABROAD",     "SPECIAL_PROGRAM",   0.0020),
    ("SP_OTHER",              "SPECIAL_PROGRAM",   0.0020),
    # Trafficking Victim — 1 %
    ("HT_T_VISA",            "TRAFFICKING_VICTIM", 0.0050),
    ("HT_U_VISA",            "TRAFFICKING_VICTIM", 0.0050),
    # Additional — 1 %
    ("AO_DIVERSITY_LOTTERY",  "ADDITIONAL",        0.0030),
    ("AO_REGISTRY_249",       "ADDITIONAL",        0.0020),
    ("AO_SPECIAL_ACT",        "ADDITIONAL",        0.0020),
    ("AO_PRIVATE_BILL",       "ADDITIONAL",        0.0010),
    ("AO_OTHER",              "ADDITIONAL",        0.0020),
]

CATEGORY_CODES   = [c[0] for c in FILING_CATEGORIES]
CATEGORY_GROUPS  = {c[0]: c[1] for c in FILING_CATEGORIES}
CATEGORY_WEIGHTS = [c[2] for c in FILING_CATEGORIES]

# Human-readable descriptions (used in filing_category table)
CATEGORY_DESCRIPTIONS = {
    "FAM_IR_SPOUSE": "Immediate Relative — Spouse of U.S. Citizen",
    "FAM_IR_CHILD": "Immediate Relative — Child of U.S. Citizen",
    "FAM_IR_PARENT": "Immediate Relative — Parent of U.S. Citizen",
    "FAM_F2A": "Family 2A — Spouse/Child of LPR",
    "FAM_F2B_CHILD": "Family 2B — Unmarried Child (under 21) of LPR",
    "FAM_F2B_ADULT": "Family 2B — Unmarried Son/Daughter (21+) of LPR",
    "FAM_F1": "Family 1st Preference — Unmarried Son/Daughter of U.S. Citizen",
    "FAM_F3": "Family 3rd Preference — Married Son/Daughter of U.S. Citizen",
    "FAM_F4": "Family 4th Preference — Sibling of U.S. Citizen",
    "FAM_IR_K1K2": "Immediate Relative — K-1/K-2 Fiancee Adjustment",
    "FAM_IR_WIDOW": "Immediate Relative — Widow(er) of U.S. Citizen",
    "FAM_IR_NDAA": "Immediate Relative — NDAA Military Parole",
    "FAM_VAWA_SPOUSE": "VAWA Self-Petition — Spouse",
    "FAM_VAWA_CHILD": "VAWA Self-Petition — Child",
    "FAM_VAWA_PARENT": "VAWA Self-Petition — Parent",
    "EMP_EB1_EXTRAORDINARY": "EB-1A Extraordinary Ability",
    "EMP_EB1_PROFESSOR": "EB-1B Outstanding Professor/Researcher",
    "EMP_EB1_MANAGER": "EB-1C Multinational Manager/Executive",
    "EMP_EB2_ADV_DEGREE": "EB-2 Advanced Degree Professional",
    "EMP_EB2_NIW": "EB-2 National Interest Waiver",
    "EMP_EB3_SKILLED": "EB-3 Skilled Worker",
    "EMP_EB3_PROFESSIONAL": "EB-3 Professional",
    "EMP_EB3_OTHER": "EB-3 Other Worker",
    "EMP_INVESTOR": "EB-5 Immigrant Investor",
    "AR_ASYLEE": "Asylee Adjustment (INA 209)",
    "AR_REFUGEE": "Refugee Adjustment (INA 209)",
    "SI_SIJ": "Special Immigrant Juvenile",
    "SI_RELIGIOUS": "Special Immigrant Religious Worker",
    "SI_ARMED_FORCES": "Special Immigrant Armed Forces Member",
    "SI_IRAQAFGHAN_SIV": "Iraqi/Afghan Special Immigrant Visa",
    "SI_BROADCASTER": "Special Immigrant Broadcaster",
    "SI_INTERNATIONAL_ORG": "Special Immigrant International Organization",
    "SI_NATO": "Special Immigrant NATO Civilian",
    "SI_PANAMA_CANAL": "Special Immigrant Panama Canal Zone",
    "SI_PHYSICIAN": "Special Immigrant Physician (Conrad 30)",
    "SI_OTHER": "Special Immigrant Other",
    "SP_DIVERSITY_VISA": "Diversity Visa Lottery Winner",
    "SP_CUBAN_ADJUSTMENT": "Cuban Adjustment Act",
    "SP_NACARA": "NACARA Adjustment",
    "SP_HRIFA": "HRIFA Adjustment",
    "SP_REGISTRY": "Registry (INA 249)",
    "SP_LULAC": "LULAC Settlement",
    "SP_DIPLOMATS": "Diplomats/International Organization Adjustment",
    "SP_BORN_US_ABROAD": "Born Abroad to US Citizen Parent",
    "SP_OTHER": "Special Program Other",
    "HT_T_VISA": "Trafficking Victim (T Visa)",
    "HT_U_VISA": "Crime Victim (U Visa)",
    "AO_DIVERSITY_LOTTERY": "Diversity Visa Lottery (Additional)",
    "AO_REGISTRY_249": "Registry Under INA 249",
    "AO_SPECIAL_ACT": "Special Legislation Act",
    "AO_PRIVATE_BILL": "Private Bill",
    "AO_OTHER": "Additional Category — Other",
}

# ── Country of birth distribution ───────────────────────────────────────────
_TOP_COUNTRIES = [
    ("Mexico", 0.15), ("India", 0.12), ("China", 0.10),
    ("Philippines", 0.08), ("Dominican Republic", 0.05),
    ("El Salvador", 0.04), ("Cuba", 0.035), ("Vietnam", 0.03),
    ("Haiti", 0.025), ("Guatemala", 0.025), ("Colombia", 0.02),
    ("South Korea", 0.02), ("Jamaica", 0.02), ("Brazil", 0.015),
    ("Bangladesh", 0.015), ("Pakistan", 0.015), ("Nigeria", 0.015),
    ("Peru", 0.01), ("Honduras", 0.01), ("United Kingdom", 0.01),
]

_OTHER_COUNTRIES = [
    "Ethiopia", "Egypt", "Ghana", "Ecuador", "Poland", "Ukraine",
    "Iran", "Nepal", "Japan", "Canada", "Germany", "France",
    "Russia", "Taiwan", "Thailand", "Indonesia", "Turkey", "Morocco",
    "Kenya", "Argentina", "Venezuela", "Trinidad and Tobago",
    "Guyana", "Myanmar", "Sri Lanka", "Syria", "Iraq", "Afghanistan",
    "Cameroon", "Somalia", "Sudan", "Uzbekistan", "Romania", "Italy",
    "Portugal", "Spain", "Israel", "Jordan", "Lebanon", "Australia",
    "New Zealand", "South Africa", "Congo", "Tanzania", "Algeria",
    "Tunisia", "Bolivia", "Chile", "Paraguay", "Uruguay", "Laos",
    "Cambodia", "Malaysia",
]

# Distribute remaining weight evenly across the long tail
_top_total = sum(w for _, w in _TOP_COUNTRIES)
_other_each = (1.0 - _top_total) / len(_OTHER_COUNTRIES)
COUNTRY_DIST = _TOP_COUNTRIES + [(c, _other_each) for c in _OTHER_COUNTRIES]
COUNTRY_CODES   = [c[0] for c in COUNTRY_DIST]
COUNTRY_WEIGHTS = [c[1] for c in COUNTRY_DIST]

# ── Demographic distributions ───────────────────────────────────────────────
SEX_OPTIONS  = ["Male", "Female"]
SEX_WEIGHTS  = [0.48, 0.52]

ETHNICITY_OPTIONS = ["Hispanic or Latino", "Not Hispanic or Latino"]
ETHNICITY_WEIGHTS = [0.35, 0.65]

RACE_OPTIONS = [
    "White", "Asian", "Black or African American",
    "American Indian or Alaska Native",
    "Native Hawaiian or Other Pacific Islander",
    "Two or More Races", "Other",
]
RACE_WEIGHTS = [0.30, 0.25, 0.15, 0.02, 0.01, 0.05, 0.22]

# (min_age, max_age, weight)
AGE_BRACKETS = [
    (18, 25, 0.10), (26, 35, 0.35), (36, 45, 0.30),
    (46, 55, 0.15), (56, 65, 0.07), (66, 90, 0.03),
]
AGE_BRACKET_WEIGHTS = [b[2] for b in AGE_BRACKETS]

MARITAL_OPTIONS = ["Single", "Married", "Divorced", "Widowed", "Legally Separated"]
MARITAL_WEIGHTS = [0.25, 0.55, 0.12, 0.05, 0.03]

EYE_COLOR_OPTIONS = ["Brown", "Black", "Blue", "Hazel", "Green", "Gray", "Other"]
EYE_COLOR_WEIGHTS = [0.55, 0.15, 0.10, 0.08, 0.05, 0.03, 0.04]

HAIR_COLOR_OPTIONS = ["Black", "Brown", "Blond", "Red", "Gray", "Sandy", "White", "Bald"]
HAIR_COLOR_WEIGHTS = [0.40, 0.30, 0.08, 0.02, 0.08, 0.03, 0.04, 0.05]

# ── Section of law ──────────────────────────────────────────────────────────
SECTION_OF_LAW_OPTIONS = [
    "INA 245(a)", "INA 245(m)", "INA 209(a)", "INA 209(b)",
    "INA 245(i)", "INA 245(j)", "INA 249",
    "Sec 13 Act of 9/11/57", "Cuban Adjustment Act", "Other",
]
SECTION_OF_LAW_WEIGHTS = [0.70, 0.05, 0.03, 0.05, 0.04, 0.01, 0.005, 0.005, 0.035, 0.075]

# Mapping from category group to most-likely section of law
SECTION_OF_LAW_BY_GROUP = {
    "FAMILY":              "INA 245(a)",
    "EMPLOYMENT":          "INA 245(a)",
    "ASYLEE_REFUGEE":      "INA 209(a)",
    "SPECIAL_IMMIGRANT":   "INA 245(a)",
    "SPECIAL_PROGRAM":     "INA 245(a)",
    "TRAFFICKING_VICTIM":  "INA 245(a)",
    "ADDITIONAL":          "INA 245(a)",
}

# ── US state distribution (by immigration volume) ───────────────────────────
_TOP_STATES = [
    ("CA", 0.22), ("NY", 0.12), ("TX", 0.10), ("FL", 0.09),
    ("NJ", 0.05), ("IL", 0.05), ("MA", 0.03), ("VA", 0.03),
    ("MD", 0.03), ("GA", 0.025), ("WA", 0.025), ("PA", 0.02),
]
_TOP_STATE_SET = {s[0] for s in _TOP_STATES}
_REMAINING_STATES = [
    s for s in [
        "AL", "AK", "AZ", "AR", "CO", "CT", "DE", "HI", "ID", "IN",
        "IA", "KS", "KY", "LA", "ME", "MI", "MN", "MS", "MO", "MT",
        "NE", "NV", "NH", "NM", "NC", "ND", "OH", "OK", "OR", "RI",
        "SC", "SD", "TN", "UT", "VT", "WV", "WI", "WY", "DC", "PR",
    ] if s not in _TOP_STATE_SET
]
_rem_each = 0.21 / len(_REMAINING_STATES)
STATE_DIST    = _TOP_STATES + [(s, _rem_each) for s in _REMAINING_STATES]
STATE_CODES   = [s[0] for s in STATE_DIST]
STATE_WEIGHTS = [s[1] for s in STATE_DIST]

# ── Application status ──────────────────────────────────────────────────────
STATUS_OPTIONS = [
    "RECEIVED", "PENDING", "APPROVED", "DENIED",
    "WITHDRAWN", "TRANSFERRED", "RFE_ISSUED", "INTERVIEW_SCHEDULED",
]
STATUS_WEIGHTS = [0.15, 0.25, 0.35, 0.10, 0.03, 0.05, 0.05, 0.02]

# ── Date ranges ─────────────────────────────────────────────────────────────
FILING_DATE_START   = "2018-01-01"
FILING_DATE_END     = "2026-02-28"
PRIORITY_DATE_START = "2005-01-01"
PRIORITY_DATE_END   = "2026-02-28"
ARRIVAL_DATE_START  = "1990-01-01"
ARRIVAL_DATE_END    = "2026-01-01"

# ── Format helpers ──────────────────────────────────────────────────────────
RECEIPT_PREFIXES       = ["IOE", "MSC", "LIN", "SRC", "EAC", "WAC"]
RECEIPT_PREFIX_WEIGHTS = [0.30, 0.20, 0.15, 0.15, 0.10, 0.10]

ARRIVAL_TYPES        = ["INSPECTED_ADMITTED", "INSPECTED_PAROLED",
                        "WITHOUT_INSPECTION", "OTHER"]
ARRIVAL_TYPE_WEIGHTS = [0.70, 0.10, 0.15, 0.05]

IMMIGRATION_STATUS_OPTIONS = [
    "H-1B", "H-4", "F-1", "L-1", "J-1", "K-1", "B-1/B-2",
    "TPS", "Asylee", "Refugee", "Parolee", "EWI", "Other",
]

# ── Children count distribution ─────────────────────────────────────────────
CHILDREN_COUNTS  = [0, 1, 2, 3, 4, 5]
CHILDREN_WEIGHTS = [0.25, 0.30, 0.25, 0.15, 0.03, 0.02]

# ── Organization types ──────────────────────────────────────────────────────
ORG_TYPES = ["RELIGIOUS", "PROFESSIONAL", "CIVIC",
             "EDUCATIONAL", "POLITICAL", "CULTURAL"]

# ── Public benefit types ────────────────────────────────────────────────────
BENEFIT_TYPES = ["SSI", "TANF", "STATE_ASSISTANCE", "MEDICAID", "SNAP"]

# ── Categories exempt from public-charge affidavit ──────────────────────────
AFFIDAVIT_EXEMPT_CATEGORIES = {
    "AR_ASYLEE", "AR_REFUGEE", "SI_SIJ",
    "HT_T_VISA", "HT_U_VISA",
    "FAM_VAWA_SPOUSE", "FAM_VAWA_CHILD", "FAM_VAWA_PARENT",
    "FAM_IR_WIDOW",
}

AFFIDAVIT_REASON_CODES = [
    "40_QUARTERS", "UNDER_18_USC_CHILD", "WIDOW",
    "VAWA", "NOT_REQUIRED", "NOT_EXEMPT",
]

# ── Faker locale mapping ───────────────────────────────────────────────────
# Only Latin-script locales — non-Latin countries use romanised name pools
# defined in profiles.py.
FAKER_LOCALES = {
    "Mexico": "es_MX", "Dominican Republic": "es", "El Salvador": "es",
    "Cuba": "es", "Guatemala": "es", "Colombia": "es_CO",
    "Jamaica": "en_US", "Brazil": "pt_BR", "Peru": "es",
    "Honduras": "es", "Ecuador": "es", "Poland": "pl_PL",
    "Germany": "de_DE", "France": "fr_FR", "Italy": "it_IT",
    "Portugal": "pt_PT", "Spain": "es_ES", "Romania": "ro_RO",
    "Canada": "en_CA", "United Kingdom": "en_GB", "Australia": "en_AU",
    "New Zealand": "en_US", "Argentina": "es_AR", "Venezuela": "es",
    "Trinidad and Tobago": "en_US", "Guyana": "en_US",
    "Chile": "es", "Bolivia": "es", "Paraguay": "es", "Uruguay": "es",
    "Indonesia": "id_ID", "Turkey": "tr_TR", "Philippines": "en_PH",
    "South Africa": "en_US", "Ghana": "en_US", "Nigeria": "en_US",
    "Kenya": "en_US", "Ethiopia": "en_US", "Cameroon": "en_US",
    "Tanzania": "en_US", "Algeria": "fr_FR", "Tunisia": "fr_FR",
    "Morocco": "fr_FR", "Congo": "fr_FR", "Haiti": "fr_FR",
    "Malaysia": "en_US",
}

# Countries whose Faker locale produces non-Latin glyphs — handled by
# romanised name pools in profiles.py
NON_LATIN_COUNTRIES = {
    "China", "Taiwan", "South Korea", "Japan", "Vietnam",
    "India", "Bangladesh", "Pakistan", "Nepal", "Sri Lanka",
    "Iran", "Egypt", "Syria", "Iraq", "Afghanistan",
    "Russia", "Ukraine", "Israel", "Thailand",
    "Myanmar", "Cambodia", "Laos", "Jordan", "Lebanon",
    "Somalia", "Sudan", "Uzbekistan",
}

# ── I/O helpers ─────────────────────────────────────────────────────────────

def save_i485_table(df: pd.DataFrame, name: str,
                    output_dir: str | None = None) -> None:
    """Save DataFrame as CSV + Parquet into the i485_form output tree."""
    csv_dir = os.path.join(output_dir, "csv") if output_dir else I485_CSV
    pq_dir  = os.path.join(output_dir, "parquet") if output_dir else I485_PQ
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(pq_dir, exist_ok=True)

    csv_path = os.path.join(csv_dir, f"{name}.csv")
    pq_path  = os.path.join(pq_dir, f"{name}.parquet")

    df.to_csv(csv_path, index=False)

    # Clean object columns for Parquet
    df_pq = df.copy()
    for col in df_pq.columns:
        if df_pq[col].dtype == object:
            df_pq[col] = (
                df_pq[col].astype(str)
                .replace({"None": None, "nan": None, "NaT": None})
            )
    try:
        df_pq.to_parquet(pq_path, index=False, engine="pyarrow")
    except Exception:
        for col in df_pq.columns:
            df_pq[col] = df_pq[col].astype(str)
        df_pq.to_parquet(pq_path, index=False, engine="pyarrow")

    mb = os.path.getsize(csv_path) / 1024 / 1024
    print(f"    {name}: {len(df):,} records  ({mb:.1f} MB csv)")


def load_i485_table(name: str, output_dir: str | None = None) -> pd.DataFrame:
    """Load a previously-generated i485 table (Parquet preferred)."""
    pq_dir  = os.path.join(output_dir, "parquet") if output_dir else I485_PQ
    csv_dir = os.path.join(output_dir, "csv") if output_dir else I485_CSV

    pq_path  = os.path.join(pq_dir, f"{name}.parquet")
    csv_path = os.path.join(csv_dir, f"{name}.csv")

    if os.path.exists(pq_path):
        return pd.read_parquet(pq_path)
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"Table '{name}' not found in {pq_dir} or {csv_dir}")


def append_i485_table(new_df: pd.DataFrame, name: str,
                      output_dir: str | None = None) -> None:
    """Append rows to an existing i485 table (or create it)."""
    try:
        existing = load_i485_table(name, output_dir)
        combined = pd.concat([existing, new_df], ignore_index=True)
    except FileNotFoundError:
        combined = new_df
    save_i485_table(combined, name, output_dir)
