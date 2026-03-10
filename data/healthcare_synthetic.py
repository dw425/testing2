"""
Config-driven synthetic data generator for HealthcareIQ.

Reads parameters from ``config/healthcare.yaml`` and writes realistic
synthetic data into bronze-layer Delta tables in Unity Catalog.

Supports two execution modes:
  - **full**        : Truncate-and-load.  Generates a complete day's worth
                      of records.
  - **incremental** : Append-only.  Generates a small micro-batch
                      (roughly 1/24 of a full day) suitable for streaming
                      simulation or scheduled refreshes.

Usage (from the repo root on a Databricks cluster)::

    python -m data.healthcare_synthetic --mode full
    python -m data.healthcare_synthetic --mode incremental
"""

from __future__ import annotations

import argparse
import logging
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
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("HealthcareIQ.SyntheticGenerator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_INCREMENTAL_FRACTION = 1 / 24  # one hour's worth of a full day

# Realistic ICD-10 diagnosis codes
ICD10_CODES = [
    "I21.0",   # Acute ST-elevation myocardial infarction of anterior wall
    "I50.9",   # Heart failure, unspecified
    "J18.9",   # Pneumonia, unspecified organism
    "J44.1",   # COPD with acute exacerbation
    "E11.9",   # Type 2 diabetes mellitus without complications
    "N17.9",   # Acute kidney failure, unspecified
    "K92.0",   # Hematemesis (GI bleed)
    "I63.9",   # Cerebral infarction, unspecified
    "A41.9",   # Sepsis, unspecified organism
    "S72.001A", # Fracture of unspecified part of neck of right femur
    "J96.01",  # Acute respiratory failure with hypoxia
    "K35.80",  # Unspecified acute appendicitis
    "I48.91",  # Unspecified atrial fibrillation
    "N39.0",   # Urinary tract infection, site not specified
    "G40.909", # Epilepsy, unspecified, not intractable
    "M79.3",   # Panniculitis, unspecified
    "R55",     # Syncope and collapse
    "I10",     # Essential hypertension
    "E87.1",   # Hypo-osmolality and hyponatremia
    "J06.9",   # Acute upper respiratory infection, unspecified
]

PHYSICIAN_FIRST = [
    "Sarah", "James", "Maria", "David", "Emily", "Robert", "Jennifer",
    "Michael", "Lisa", "William", "Rachel", "Thomas", "Amanda", "Daniel",
    "Susan", "Andrew", "Patricia", "Christopher", "Nicole", "Joseph",
]

PHYSICIAN_LAST = [
    "Chen", "Patel", "Williams", "Rodriguez", "Kim", "Johnson",
    "Martinez", "Thompson", "Garcia", "Lee", "Anderson", "Taylor",
    "Wilson", "Moore", "Jackson", "White", "Harris", "Clark",
    "Lewis", "Walker",
]

INSURANCE_TYPES = [
    "Medicare", "Medicaid", "Private - Blue Cross", "Private - Aetna",
    "Private - Cigna", "Private - UnitedHealth", "Self-Pay", "Tricare",
    "Workers Compensation",
]

ADMISSION_TYPES = ["Emergency", "Elective", "Urgent", "Observation", "Transfer"]

DISCHARGE_DISPOSITIONS = [
    "Home", "Home with Home Health", "SNF", "Rehab Facility",
    "AMA", "Transfer to Another Facility", "Expired", "Hospice",
]

BED_PREFIXES = {
    "Emergency": "ED",
    "ICU": "ICU",
    "Cardiology": "CARD",
    "Orthopedics": "ORTH",
    "Oncology": "ONC",
    "Pediatrics": "PEDS",
}

# ---------------------------------------------------------------------------
# PySpark Schemas
# ---------------------------------------------------------------------------
RAW_ADMISSIONS_SCHEMA = StructType([
    StructField("admission_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("patient_id", StringType(), nullable=False),
    StructField("facility", StringType(), nullable=False),
    StructField("department", StringType(), nullable=False),
    StructField("admission_type", StringType(), nullable=False),
    StructField("acuity_level", IntegerType(), nullable=False),
    StructField("insurance_type", StringType(), nullable=False),
    StructField("attending_physician", StringType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
])

RAW_VITALS_SCHEMA = StructType([
    StructField("reading_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("patient_id", StringType(), nullable=False),
    StructField("facility", StringType(), nullable=False),
    StructField("heart_rate", IntegerType(), nullable=False),
    StructField("blood_pressure_sys", IntegerType(), nullable=False),
    StructField("blood_pressure_dia", IntegerType(), nullable=False),
    StructField("temp_f", DoubleType(), nullable=False),
    StructField("spo2", DoubleType(), nullable=False),
    StructField("respiratory_rate", IntegerType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
])

RAW_EQUIPMENT_TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("asset_id", StringType(), nullable=False),
    StructField("facility", StringType(), nullable=False),
    StructField("equipment_type", StringType(), nullable=False),
    StructField("usage_hours", DoubleType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=False),
    StructField("vibration", DoubleType(), nullable=False),
    StructField("error_count", IntegerType(), nullable=False),
    StructField("maintenance_due", BooleanType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
])

RAW_DISCHARGES_SCHEMA = StructType([
    StructField("discharge_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("patient_id", StringType(), nullable=False),
    StructField("facility", StringType(), nullable=False),
    StructField("department", StringType(), nullable=False),
    StructField("los_days", DoubleType(), nullable=False),
    StructField("diagnosis_code", StringType(), nullable=False),
    StructField("discharge_disposition", StringType(), nullable=False),
    StructField("readmission_risk_score", DoubleType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
])


# ===================================================================
# Configuration helpers
# ===================================================================

def _resolve_config_path(config_rel: str = "config/healthcare.yaml") -> Path:
    """Resolve config path relative to the repository root."""
    cwd = Path.cwd()
    candidate = cwd / config_rel
    if candidate.is_file():
        return candidate
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
        .appName("HealthcareIQ-SyntheticGenerator")
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


def _pick_weighted(
    rng: np.random.Generator,
    choices: List[str],
    weights: List[float],
) -> str:
    """Pick from choices with specified probability weights."""
    probs = np.array(weights) / sum(weights)
    return choices[rng.choice(len(choices), p=probs)]


def _expand_equipment(cfg: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """Return a flat list of (facility, asset_id, equipment_type) from config."""
    pairs: List[Tuple[str, str, str]] = []
    for facility, assets in cfg["data"]["equipment_types"].items():
        for asset_id in assets:
            # Derive equipment_type from asset_id prefix
            eq_type = asset_id.rsplit("-", 1)[0] if "-" in asset_id else asset_id
            # Clean up: MRI-MGH -> MRI, CT-Scanner-MGH -> CT-Scanner, etc.
            parts = eq_type.split("-")
            if len(parts) >= 2:
                eq_type = "-".join(parts[:-1])  # drop facility suffix
            pairs.append((facility, asset_id, eq_type))
    return pairs


def _generate_physician(rng: np.random.Generator) -> str:
    """Generate a realistic physician name."""
    first = _pick(rng, PHYSICIAN_FIRST)
    last = _pick(rng, PHYSICIAN_LAST)
    return f"Dr. {first} {last}"


def _generate_patient_id(rng: np.random.Generator) -> str:
    """Generate a de-identified patient identifier."""
    return f"PAT-{rng.integers(100000, 999999)}"


# ===================================================================
# Core generators
# ===================================================================

def generate_admissions(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 42,
) -> DataFrame:
    """Generate synthetic patient admission rows for ``bronze.raw_admissions``.

    Produces 285 admissions per full day spread across 3 facilities and 6
    departments.  Acuity levels are weighted so that level-3 is most common
    (general medical floor) while level-5 (critical) is rare.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed for reproducibility.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_ADMISSIONS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    flow_cfg = cfg["data"]["patient_flow"]
    records_per_day: int = flow_cfg["avg_daily_admissions"]  # 285
    facilities: List[str] = cfg["data"]["facilities"]
    departments: List[str] = cfg["data"]["departments"]

    n_records = records_per_day
    if mode == "incremental":
        n_records = max(int(records_per_day * _INCREMENTAL_FRACTION), 5)

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    timestamps = _random_timestamps(rng, start, now, n_records)

    # Acuity weights: 1=rare trauma, 2=serious, 3=moderate (most common),
    # 4=minor, 5=minimal
    acuity_weights = [0.05, 0.15, 0.40, 0.25, 0.15]

    # Facility weights: Metro General is larger
    facility_weights = [0.50, 0.30, 0.20]

    # Generate a pool of physicians per facility for consistency
    physician_pool: Dict[str, List[str]] = {}
    for fac in facilities:
        physician_pool[fac] = [_generate_physician(rng) for _ in range(15)]

    rows: List[Dict[str, Any]] = []
    for i in range(n_records):
        facility = _pick_weighted(rng, facilities, facility_weights)

        # Emergency department has more high-acuity; adjust per department
        department = _pick(rng, departments)
        if department == "Emergency":
            dept_acuity_weights = [0.10, 0.25, 0.35, 0.20, 0.10]
        elif department == "ICU":
            dept_acuity_weights = [0.20, 0.40, 0.30, 0.08, 0.02]
        else:
            dept_acuity_weights = acuity_weights

        acuity = int(rng.choice([1, 2, 3, 4, 5], p=np.array(dept_acuity_weights) / sum(dept_acuity_weights)))

        # Emergency admissions are more common for high-acuity patients
        if acuity <= 2:
            adm_type_weights = [0.60, 0.05, 0.30, 0.03, 0.02]
        else:
            adm_type_weights = [0.20, 0.35, 0.20, 0.15, 0.10]
        admission_type = _pick_weighted(rng, ADMISSION_TYPES, adm_type_weights)

        physician = _pick(rng, physician_pool[facility])

        row = {
            "admission_id": _uuid(),
            "timestamp": timestamps[i],
            "patient_id": _generate_patient_id(rng),
            "facility": facility,
            "department": department,
            "admission_type": admission_type,
            "acuity_level": acuity,
            "insurance_type": _pick(rng, INSURANCE_TYPES),
            "attending_physician": physician,
            "_ingested_at": now,
        }
        rows.append(row)

    logger.info(
        "Generated %d admission rows (mode=%s, window=%dh)",
        len(rows), mode, window_hours,
    )
    return spark.createDataFrame(rows, schema=RAW_ADMISSIONS_SCHEMA)


def generate_vitals(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 43,
) -> DataFrame:
    """Generate synthetic vital-sign readings for ``bronze.raw_vitals``.

    Produces 5,000 readings per full day.  A subset of patients (~8 %)
    exhibit deteriorating patterns (tachycardia, hypotension, desaturation)
    to simulate early-warning-score triggers.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_VITALS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    records_per_day = 5000
    facilities: List[str] = cfg["data"]["facilities"]

    n_records = records_per_day
    if mode == "incremental":
        n_records = max(int(records_per_day * _INCREMENTAL_FRACTION), 10)

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    timestamps = _random_timestamps(rng, start, now, n_records)

    # Generate a pool of patient IDs to reuse (patients get multiple readings)
    n_patients = 350
    patient_pool = [_generate_patient_id(rng) for _ in range(n_patients)]
    # Mark ~8% of patients as deteriorating
    deteriorating_patients = set(patient_pool[: int(n_patients * 0.08)])

    # Facility distribution
    facility_weights = [0.50, 0.30, 0.20]

    rows: List[Dict[str, Any]] = []
    for i in range(n_records):
        patient_id = _pick(rng, patient_pool)
        facility = _pick_weighted(rng, facilities, facility_weights)
        is_deteriorating = patient_id in deteriorating_patients

        if is_deteriorating:
            # Deteriorating vitals: tachycardia, hypotension, low SpO2
            heart_rate = int(rng.normal(115, 15))
            bp_sys = int(rng.normal(85, 10))
            bp_dia = int(rng.normal(50, 8))
            temp_f = float(rng.normal(101.5, 1.2))
            spo2 = float(rng.normal(89, 4))
            respiratory_rate = int(rng.normal(28, 5))
        else:
            # Normal vitals
            heart_rate = int(rng.normal(78, 12))
            bp_sys = int(rng.normal(122, 14))
            bp_dia = int(rng.normal(78, 10))
            temp_f = float(rng.normal(98.6, 0.5))
            spo2 = float(rng.normal(97.5, 1.2))
            respiratory_rate = int(rng.normal(16, 3))

        # Clamp values to physiologically plausible ranges
        heart_rate = max(30, min(220, heart_rate))
        bp_sys = max(60, min(250, bp_sys))
        bp_dia = max(30, min(150, bp_dia))
        temp_f = round(max(95.0, min(106.0, temp_f)), 1)
        spo2 = round(max(60.0, min(100.0, spo2)), 1)
        respiratory_rate = max(4, min(50, respiratory_rate))

        row = {
            "reading_id": _uuid(),
            "timestamp": timestamps[i],
            "patient_id": patient_id,
            "facility": facility,
            "heart_rate": heart_rate,
            "blood_pressure_sys": bp_sys,
            "blood_pressure_dia": bp_dia,
            "temp_f": temp_f,
            "spo2": spo2,
            "respiratory_rate": respiratory_rate,
            "_ingested_at": now,
        }
        rows.append(row)

    logger.info(
        "Generated %d vital-sign rows (mode=%s, window=%dh)",
        len(rows), mode, window_hours,
    )
    return spark.createDataFrame(rows, schema=RAW_VITALS_SCHEMA)


def generate_equipment_telemetry(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 44,
) -> DataFrame:
    """Generate synthetic equipment telemetry for ``bronze.raw_equipment_telemetry``.

    Models 1,240 total assets across 3 facilities.  47 assets are flagged
    with maintenance due and 3 are in a critical state (high temperature,
    excessive vibration, elevated error counts).

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_EQUIPMENT_TELEMETRY_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    equip_cfg = cfg["data"]["equipment_monitoring"]
    total_assets: int = equip_cfg["total_assets"]       # 1240
    maintenance_due_count: int = equip_cfg["maintenance_due"]  # 47
    critical_count: int = equip_cfg["critical_alerts"]  # 3
    facilities: List[str] = cfg["data"]["facilities"]

    # Expand known equipment from config and generate additional generic assets
    known_equipment = _expand_equipment(cfg)
    equipment_types = [
        "MRI", "CT-Scanner", "Ventilator", "Ultrasound", "Infusion-Pump",
        "X-Ray", "ECG-Monitor", "Lab-Analyzer", "Defibrillator",
        "Patient-Monitor", "Surgical-Robot", "Anesthesia-Machine",
        "Blood-Gas-Analyzer", "Dialysis-Machine", "C-Arm",
    ]

    # Build a full asset roster of 1,240 devices
    all_assets: List[Tuple[str, str, str]] = list(known_equipment)
    facility_weights = [0.50, 0.30, 0.20]
    asset_counter = len(all_assets)
    while asset_counter < total_assets:
        fac = _pick_weighted(rng, facilities, facility_weights)
        eq_type = _pick(rng, equipment_types)
        fac_code = fac[:3].upper()
        asset_id = f"{eq_type}-{fac_code}-{asset_counter:04d}"
        all_assets.append((fac, asset_id, eq_type))
        asset_counter += 1

    # Designate which assets have maintenance due / are critical
    rng.shuffle(all_assets)
    critical_assets = set()
    maintenance_due_assets = set()
    for idx, (fac, aid, etype) in enumerate(all_assets):
        if idx < critical_count:
            critical_assets.add(aid)
            maintenance_due_assets.add(aid)
        elif idx < critical_count + maintenance_due_count:
            maintenance_due_assets.add(aid)

    # Generate one telemetry reading per asset (snapshot style)
    n_records = total_assets
    if mode == "incremental":
        n_records = max(int(total_assets * _INCREMENTAL_FRACTION), 50)
        all_assets = all_assets[:n_records]

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    timestamps = _random_timestamps(rng, start, now, len(all_assets))

    rows: List[Dict[str, Any]] = []
    for i, (facility, asset_id, eq_type) in enumerate(all_assets):
        is_critical = asset_id in critical_assets
        is_maint_due = asset_id in maintenance_due_assets

        if is_critical:
            # Critical: high temp, high vibration, many errors
            usage_hours = float(rng.uniform(15000, 25000))
            temperature = float(rng.uniform(85, 105))
            vibration = float(rng.uniform(8.0, 15.0))
            error_count = int(rng.integers(50, 200))
        elif is_maint_due:
            # Maintenance due: moderately elevated readings
            usage_hours = float(rng.uniform(8000, 18000))
            temperature = float(rng.uniform(55, 80))
            vibration = float(rng.uniform(3.5, 7.0))
            error_count = int(rng.integers(10, 50))
        else:
            # Healthy: normal operating ranges
            usage_hours = float(rng.uniform(100, 12000))
            temperature = float(rng.uniform(20, 45))
            vibration = float(rng.uniform(0.1, 2.5))
            error_count = int(rng.integers(0, 5))

        row = {
            "event_id": _uuid(),
            "timestamp": timestamps[i],
            "asset_id": asset_id,
            "facility": facility,
            "equipment_type": eq_type,
            "usage_hours": round(usage_hours, 1),
            "temperature": round(temperature, 1),
            "vibration": round(vibration, 2),
            "error_count": error_count,
            "maintenance_due": is_maint_due,
            "_ingested_at": now,
        }
        rows.append(row)

    logger.info(
        "Generated %d equipment telemetry rows (%d maintenance-due, %d critical, mode=%s)",
        len(rows), len(maintenance_due_assets), len(critical_assets), mode,
    )
    return spark.createDataFrame(rows, schema=RAW_EQUIPMENT_TELEMETRY_SCHEMA)


def generate_discharges(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 45,
) -> DataFrame:
    """Generate synthetic discharge rows for ``bronze.raw_discharges``.

    Produces 270 discharges per full day with an 8.2% readmission rate.
    156 patients are scored as high-risk (risk_score >= 0.65).  Diagnosis
    codes use realistic ICD-10 format.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_DISCHARGES_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    readmit_cfg = cfg["data"]["readmissions"]
    records_per_day = 270
    readmission_rate: float = readmit_cfg["readmission_rate"]  # 0.082
    high_risk_count: int = readmit_cfg["high_risk_patients"]   # 156
    facilities: List[str] = cfg["data"]["facilities"]
    departments: List[str] = cfg["data"]["departments"]

    n_records = records_per_day
    if mode == "incremental":
        n_records = max(int(records_per_day * _INCREMENTAL_FRACTION), 5)

    # Scale high-risk count for incremental mode
    n_high_risk = high_risk_count
    if mode == "incremental":
        n_high_risk = max(int(high_risk_count * _INCREMENTAL_FRACTION), 2)

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    timestamps = _random_timestamps(rng, start, now, n_records)

    # Facility weights
    facility_weights = [0.50, 0.30, 0.20]

    # LOS distribution: most are 2-5 days, some longer for ICU / complex cases
    avg_los = cfg["data"]["patient_flow"]["avg_los_days"]  # 4.2

    # Disposition weights: most go home
    disposition_weights = [0.45, 0.15, 0.12, 0.08, 0.03, 0.05, 0.02, 0.10]

    rows: List[Dict[str, Any]] = []
    for i in range(n_records):
        facility = _pick_weighted(rng, facilities, facility_weights)
        department = _pick(rng, departments)
        diagnosis_code = _pick(rng, ICD10_CODES)

        # Length of stay: log-normal distribution centered around avg_los
        if department == "ICU":
            los = float(rng.lognormal(mean=np.log(avg_los * 1.5), sigma=0.6))
        elif department == "Emergency":
            los = float(rng.lognormal(mean=np.log(0.5), sigma=0.5))
        else:
            los = float(rng.lognormal(mean=np.log(avg_los), sigma=0.5))
        los = round(max(0.1, min(60.0, los)), 1)

        # Readmission risk scoring
        is_high_risk = i < n_high_risk
        if is_high_risk:
            risk_score = float(rng.uniform(0.65, 0.98))
        else:
            # Calibrate so ~8.2% are above readmission threshold (0.5)
            if rng.random() < readmission_rate:
                risk_score = float(rng.uniform(0.50, 0.65))
            else:
                risk_score = float(rng.uniform(0.02, 0.50))
        risk_score = round(risk_score, 4)

        # Higher-risk patients more likely to go to SNF or rehab
        if risk_score >= 0.65:
            disp_weights = [0.20, 0.15, 0.25, 0.15, 0.02, 0.08, 0.05, 0.10]
        else:
            disp_weights = disposition_weights

        disposition = _pick_weighted(
            rng, DISCHARGE_DISPOSITIONS, disp_weights
        )

        row = {
            "discharge_id": _uuid(),
            "timestamp": timestamps[i],
            "patient_id": _generate_patient_id(rng),
            "facility": facility,
            "department": department,
            "los_days": los,
            "diagnosis_code": diagnosis_code,
            "discharge_disposition": disposition,
            "readmission_risk_score": risk_score,
            "_ingested_at": now,
        }
        rows.append(row)

    logger.info(
        "Generated %d discharge rows (%d high-risk, readmission_rate=%.1f%%, mode=%s)",
        len(rows), min(n_high_risk, n_records),
        readmission_rate * 100, mode,
    )
    return spark.createDataFrame(rows, schema=RAW_DISCHARGES_SCHEMA)


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
        catalog: UC catalog name (e.g. ``healthcare_iq``).
        schema:  UC schema / database name (e.g. ``bronze``).
        table:   Table name (e.g. ``raw_admissions``).
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

    Generates admissions, vitals, equipment telemetry, and discharges,
    then writes them to the corresponding bronze Delta tables.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
    """
    spark = get_spark()
    catalog: str = cfg["app"]["catalog"]

    logger.info(
        "=== HealthcareIQ Synthetic Data Generation ===  catalog=%s  mode=%s",
        catalog, mode,
    )

    _ensure_schemas_exist(spark, catalog)

    # --- Bronze: admissions ---
    admissions_df = generate_admissions(cfg, mode=mode)
    _write_delta(admissions_df, catalog, "bronze", "raw_admissions", mode)

    # --- Bronze: vitals ---
    vitals_df = generate_vitals(cfg, mode=mode)
    _write_delta(vitals_df, catalog, "bronze", "raw_vitals", mode)

    # --- Bronze: equipment telemetry ---
    equipment_df = generate_equipment_telemetry(cfg, mode=mode)
    _write_delta(equipment_df, catalog, "bronze", "raw_equipment_telemetry", mode)

    # --- Bronze: discharges ---
    discharges_df = generate_discharges(cfg, mode=mode)
    _write_delta(discharges_df, catalog, "bronze", "raw_discharges", mode)

    logger.info("=== Generation complete ===")


# ===================================================================
# CLI entry point
# ===================================================================

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="HealthcareIQ synthetic data generator",
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
        help="Path to the YAML config file. Defaults to config/healthcare.yaml.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logger.info(
        "Starting generator — mode=%s  config=%s",
        args.mode, args.config or "(auto-detect)",
    )
    config = load_config(args.config)
    run_generation(config, mode=args.mode)
