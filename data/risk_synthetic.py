"""
Config-driven synthetic data generator for RiskIQ.

Reads parameters from ``config/risk.yaml`` and writes realistic
synthetic data into bronze-layer Delta tables in Unity Catalog.

Supports two execution modes:
  - **full**        : Truncate-and-load.  Generates a complete day's worth
                      of records.
  - **incremental** : Append-only.  Generates a small micro-batch
                      (roughly 1/24 of a full day) suitable for streaming
                      simulation or scheduled refreshes.

Usage (from the repo root on a Databricks cluster)::

    python -m data.risk_synthetic --use-case risk --mode full
    python -m data.risk_synthetic --use-case risk --mode incremental
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
logger = logging.getLogger("RiskIQ.SyntheticGenerator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_INCREMENTAL_FRACTION = 1 / 24  # one hour's worth of a full day
_ACCESS_LOGS_PER_DAY = 50_000
_ANOMALY_RATE = 0.023  # 2.3%
_TOTAL_RECORDS_SCANNED = 1_200_000_000
_FLAGGED_ANOMALIES = 142

# Users from config with specific behavioral profiles
_USER_PROFILES = {
    "j.doe@company.com": {
        "risk_level": "High",
        "anomaly_rate_multiplier": 5.0,
        "preferred_domains": ["finance.payroll", "customer.pii", "hr.benefits"],
        "unusual_hours": True,
        "device_types": ["Desktop", "Mobile"],
    },
    "sys_pipeline_01": {
        "risk_level": "Standard Baseline",
        "anomaly_rate_multiplier": 0.2,
        "preferred_domains": ["finance.reporting", "hr.analytics"],
        "unusual_hours": False,
        "device_types": ["Service"],
    },
    "a.smith@company.com": {
        "risk_level": "Medium",
        "anomaly_rate_multiplier": 1.5,
        "preferred_domains": ["customer.pii", "finance.treasury"],
        "unusual_hours": False,
        "device_types": ["Desktop", "API"],
    },
    "ml_service_account": {
        "risk_level": "Low",
        "anomaly_rate_multiplier": 0.5,
        "preferred_domains": ["hr.analytics", "finance.reporting"],
        "unusual_hours": False,
        "device_types": ["Service"],
    },
}

# Data assets / domains
_DATA_DOMAINS = [
    "finance.payroll",
    "finance.treasury",
    "finance.reporting",
    "customer.pii",
    "customer.orders",
    "customer.support",
    "hr.benefits",
    "hr.analytics",
    "hr.recruiting",
    "healthcare.claims",
    "healthcare.records",
]

_SENSITIVITY_MAP = {
    "finance.payroll": "High",
    "finance.treasury": "High",
    "finance.reporting": "Medium",
    "customer.pii": "High",
    "customer.orders": "Medium",
    "customer.support": "Low",
    "hr.benefits": "High",
    "hr.analytics": "Medium",
    "hr.recruiting": "Low",
    "healthcare.claims": "High",
    "healthcare.records": "High",
}

_GEO_LOCATIONS = [
    "New York, US",
    "San Francisco, US",
    "Chicago, US",
    "London, UK",
    "Berlin, DE",
    "Tokyo, JP",
    "Sydney, AU",
    "Mumbai, IN",
    "Sao Paulo, BR",
    "Toronto, CA",
]

_ACCESS_METHODS = ["UI", "API", "JDBC", "Service Account"]
_DEVICE_TYPES = ["Desktop", "Mobile", "API", "Service"]

# Compliance framework definitions
_FRAMEWORK_CONTROLS = {
    "GDPR": {
        "region": "EU",
        "controls": [
            ("GDPR-ART5", "Lawfulness of Processing"),
            ("GDPR-ART6", "Legal Basis for Processing"),
            ("GDPR-ART7", "Conditions for Consent"),
            ("GDPR-ART12", "Transparent Information"),
            ("GDPR-ART15", "Right of Access"),
            ("GDPR-ART17", "Right to Erasure"),
            ("GDPR-ART20", "Right to Data Portability"),
            ("GDPR-ART25", "Data Protection by Design"),
            ("GDPR-ART30", "Records of Processing Activities"),
            ("GDPR-ART32", "Security of Processing"),
            ("GDPR-ART33", "Breach Notification"),
            ("GDPR-ART35", "Data Protection Impact Assessment"),
        ],
    },
    "CCPA": {
        "region": "California",
        "controls": [
            ("CCPA-1798.100", "Right to Know"),
            ("CCPA-1798.105", "Right to Delete"),
            ("CCPA-1798.110", "Right to Disclosure"),
            ("CCPA-1798.115", "Right to Opt-Out"),
            ("CCPA-1798.120", "Non-Discrimination"),
            ("CCPA-1798.125", "Financial Incentives"),
            ("CCPA-1798.130", "Notice Requirements"),
            ("CCPA-1798.135", "Opt-Out Link"),
            ("CCPA-1798.140", "Service Provider Obligations"),
            ("CCPA-1798.145", "Exemptions"),
        ],
    },
    "HIPAA": {
        "region": "US",
        "controls": [
            ("HIPAA-164.308", "Administrative Safeguards"),
            ("HIPAA-164.310", "Physical Safeguards"),
            ("HIPAA-164.312", "Technical Safeguards"),
            ("HIPAA-164.314", "Organizational Requirements"),
            ("HIPAA-164.316", "Policies and Procedures"),
            ("HIPAA-164.502", "Uses and Disclosures"),
            ("HIPAA-164.504", "Business Associates"),
            ("HIPAA-164.510", "Uses for Treatment"),
            ("HIPAA-164.512", "Uses for Public Interest"),
            ("HIPAA-164.524", "Access of Individuals"),
            ("HIPAA-164.526", "Amendment of PHI"),
            ("HIPAA-164.528", "Accounting of Disclosures"),
        ],
    },
    "SOC 2 Type II": {
        "region": "Global",
        "controls": [
            ("SOC2-CC1", "Control Environment"),
            ("SOC2-CC2", "Communication and Information"),
            ("SOC2-CC3", "Risk Assessment"),
            ("SOC2-CC4", "Monitoring Activities"),
            ("SOC2-CC5", "Control Activities"),
            ("SOC2-CC6", "Logical and Physical Access"),
            ("SOC2-CC7", "System Operations"),
            ("SOC2-CC8", "Change Management"),
            ("SOC2-CC9", "Risk Mitigation"),
            ("SOC2-A1", "Availability"),
            ("SOC2-PI1", "Processing Integrity"),
            ("SOC2-C1", "Confidentiality"),
        ],
    },
    "PCI-DSS": {
        "region": "Global",
        "controls": [
            ("PCI-1", "Install and Maintain Firewall"),
            ("PCI-2", "Vendor-Supplied Defaults"),
            ("PCI-3", "Protect Stored Cardholder Data"),
            ("PCI-4", "Encrypt Transmission"),
            ("PCI-5", "Anti-Virus Software"),
            ("PCI-6", "Secure Systems and Applications"),
            ("PCI-7", "Restrict Access by Need-to-Know"),
            ("PCI-8", "Identify and Authenticate Access"),
            ("PCI-9", "Restrict Physical Access"),
            ("PCI-10", "Track and Monitor Access"),
            ("PCI-11", "Test Security Systems"),
            ("PCI-12", "Information Security Policy"),
        ],
    },
}

# Config-driven framework statuses (from risk.yaml)
_FRAMEWORK_STATUS_OVERRIDES = {
    "GDPR": {"status": "Compliant", "violations": 0},
    "CCPA": {"status": "Needs Review", "violations": 2},
    "HIPAA": {"status": "At Risk", "violations": 14},
    "SOC 2 Type II": {"status": "Compliant", "violations": 0},
    "PCI-DSS": {"status": "Needs Review", "violations": 1},
}

# PII detection targets
_PII_TYPES = ["PHONE_NUMBER", "EMAIL_ADDRESS", "US_SSN", "CREDIT_CARD"]

_PII_TARGET_TABLES = [
    "silver.customer_profiles",
    "silver.user_contact_info",
    "silver.payment_records",
    "silver.hr_employee_data",
    "silver.claims_submissions",
    "bronze.raw_form_submissions",
    "bronze.raw_support_tickets",
    "bronze.raw_transaction_logs",
    "silver.order_details",
    "silver.insurance_applications",
]

_PII_COLUMNS_BY_TYPE = {
    "PHONE_NUMBER": ["phone", "mobile_number", "contact_phone", "fax"],
    "EMAIL_ADDRESS": ["email", "contact_email", "user_email", "notification_email"],
    "US_SSN": ["ssn", "social_security", "tax_id", "national_id"],
    "CREDIT_CARD": ["card_number", "cc_number", "payment_card", "primary_card"],
}

_ASSESSORS = [
    "AutoScan-v3.2",
    "ComplianceBot",
    "AuditEngine-Pro",
    "ManualAudit-TeamA",
    "ManualAudit-TeamB",
    "SecurityScanner-v2",
]

_FINDING_TEMPLATES = {
    "Pass": [
        "Control verified and operating effectively.",
        "All requirements met. No findings.",
        "Evidence of compliance collected and validated.",
    ],
    "Fail": [
        "Control not implemented. Remediation required.",
        "Evidence of non-compliance detected. Immediate action needed.",
        "Access controls insufficient for required data classification.",
        "Data retention policy not enforced for this domain.",
        "Encryption requirements not met for data at rest.",
    ],
    "Warning": [
        "Control partially implemented. Improvement recommended.",
        "Minor gaps identified. Follow-up audit scheduled.",
        "Configuration drift detected since last assessment.",
        "Documentation incomplete for this control area.",
    ],
    "Not Applicable": [
        "Control not applicable to current system scope.",
        "Exemption approved by CISO.",
    ],
}

_SEVERITIES = ["Critical", "High", "Medium", "Low", "Info"]

# ---------------------------------------------------------------------------
# PySpark Schemas
# ---------------------------------------------------------------------------
RAW_ACCESS_LOGS_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("asset_accessed", StringType(), nullable=False),
    StructField("ip_address", StringType(), nullable=False),
    StructField("geo_location", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=False),
    StructField("access_method", StringType(), nullable=False),
    StructField("session_duration_sec", IntegerType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
])

RAW_COMPLIANCE_SCANS_SCHEMA = StructType([
    StructField("scan_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("framework", StringType(), nullable=False),
    StructField("domain", StringType(), nullable=False),
    StructField("control_id", StringType(), nullable=False),
    StructField("control_name", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("finding_detail", StringType(), nullable=True),
    StructField("severity", StringType(), nullable=False),
    StructField("assessor", StringType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
])

RAW_PII_DETECTIONS_SCHEMA = StructType([
    StructField("detection_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("table_name", StringType(), nullable=False),
    StructField("column_name", StringType(), nullable=False),
    StructField("pii_type", StringType(), nullable=False),
    StructField("confidence", DoubleType(), nullable=False),
    StructField("record_count", IntegerType(), nullable=False),
    StructField("masked", BooleanType(), nullable=False),
    StructField("pipeline_id", StringType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
])


# ===================================================================
# Configuration helpers
# ===================================================================

def _resolve_config_path(config_rel: str = "config/risk.yaml") -> Path:
    """Resolve config path relative to the repository root."""
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
        .appName("RiskIQ-SyntheticGenerator")
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


def _generate_ip(rng: np.random.Generator) -> str:
    """Generate a random IPv4 address."""
    return f"{rng.integers(10, 200)}.{rng.integers(0, 256)}.{rng.integers(0, 256)}.{rng.integers(1, 255)}"


# ===================================================================
# Core generators
# ===================================================================

def generate_access_logs(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 42,
) -> DataFrame:
    """Generate synthetic access log rows for ``bronze.raw_access_logs``.

    Produces ~50,000 records per full day with 2.3% anomalous access
    patterns.  The four users from config are given distinct behavioral
    profiles, with j.doe@company.com exhibiting unusual access patterns.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed for reproducibility.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_ACCESS_LOGS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    n_records = _ACCESS_LOGS_PER_DAY
    if mode == "incremental":
        n_records = int(_ACCESS_LOGS_PER_DAY * _INCREMENTAL_FRACTION)

    users = cfg["data"]["rbac_logs"]["users"]
    # Extend with generic users to dilute the known ones
    generic_users = [
        f"user{i:03d}@company.com" for i in range(1, 51)
    ]
    all_users = users + generic_users

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    timestamps = _random_timestamps(rng, start, now, n_records)

    rows: List[Dict[str, Any]] = []
    for i in range(n_records):
        user = _pick(rng, all_users)
        profile = _USER_PROFILES.get(user, None)

        # Determine anomaly for this access event
        effective_anomaly_rate = _ANOMALY_RATE
        if profile:
            effective_anomaly_rate = min(
                _ANOMALY_RATE * profile["anomaly_rate_multiplier"], 1.0
            )

        is_anomaly = rng.random() < effective_anomaly_rate

        # Select data domain
        if profile and rng.random() < 0.7:
            # Known users access their preferred domains 70% of the time
            asset = _pick(rng, profile["preferred_domains"])
        else:
            asset = _pick(rng, _DATA_DOMAINS)

        # For anomalous access, bias toward high-sensitivity assets
        if is_anomaly and rng.random() < 0.6:
            high_sensitivity = [
                d for d, s in _SENSITIVITY_MAP.items() if s == "High"
            ]
            asset = _pick(rng, high_sensitivity)

        # Device type
        if profile:
            device = _pick(rng, profile["device_types"])
        else:
            device = _pick(rng, _DEVICE_TYPES)

        # Access method
        if device == "Service":
            access_method = "Service Account"
        elif device == "API":
            access_method = "API"
        else:
            access_method = _pick(rng, ["UI", "API", "JDBC"])

        # Geo location — anomalous access more likely from unusual locations
        if is_anomaly and rng.random() < 0.4:
            geo = _pick(rng, ["Tokyo, JP", "Mumbai, IN", "Sao Paulo, BR", "Sydney, AU"])
        else:
            geo = _pick(rng, _GEO_LOCATIONS[:5])  # usual locations

        # Session duration — anomalous sessions tend to be shorter or much longer
        if is_anomaly:
            session_dur = int(rng.choice([
                rng.integers(5, 30),      # suspiciously short
                rng.integers(3600, 14400), # suspiciously long
            ]))
        else:
            session_dur = int(rng.integers(60, 3600))

        # Adjust timestamp for unusual-hours users
        ts = timestamps[i]
        if profile and profile.get("unusual_hours") and is_anomaly:
            # Push to 1am-5am range
            ts = ts.replace(hour=int(rng.integers(1, 5)))

        # Generate user_id from email
        user_id = user.replace("@company.com", "").replace(".", "_")
        if "@" not in user:
            user_id = user
        email = user if "@" in user else f"{user}@system.internal"

        row = {
            "event_id": _uuid(),
            "timestamp": ts,
            "user_id": user_id,
            "email": email,
            "asset_accessed": asset,
            "ip_address": _generate_ip(rng),
            "geo_location": geo,
            "device_type": device,
            "access_method": access_method,
            "session_duration_sec": session_dur,
            "_ingested_at": now,
        }
        rows.append(row)

    logger.info(
        "Generated %d access log rows (mode=%s, window=%dh)",
        len(rows), mode, window_hours,
    )
    return spark.createDataFrame(rows, schema=RAW_ACCESS_LOGS_SCHEMA)


def generate_compliance_scans(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 43,
) -> DataFrame:
    """Generate synthetic compliance scan rows for ``bronze.raw_compliance_scans``.

    All 5 frameworks from config (GDPR, CCPA, HIPAA, SOC 2 Type II, PCI-DSS)
    with realistic control IDs and statuses matching the config-defined states
    (e.g., GDPR=Compliant, HIPAA=At Risk with 14 violations).

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_COMPLIANCE_SCANS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    frameworks = cfg["data"]["compliance_frameworks"]
    domains = cfg["data"]["domains"]
    now = _utcnow()

    rows: List[Dict[str, Any]] = []

    for fw_cfg in frameworks:
        fw_name = fw_cfg["name"]
        fw_violations = fw_cfg["violations"]
        fw_status = fw_cfg["status"]

        fw_def = _FRAMEWORK_CONTROLS.get(fw_name, None)
        if fw_def is None:
            continue

        controls = fw_def["controls"]

        # Determine how many scan iterations (multiple audit cycles)
        n_cycles = 3 if mode == "full" else 1

        violations_remaining = fw_violations

        for cycle in range(n_cycles):
            cycle_offset = timedelta(days=cycle * 30)
            cycle_start = now - timedelta(days=90) + cycle_offset

            for control_id, control_name in controls:
                for domain in domains:
                    # Determine control status for this scan
                    if fw_status == "Compliant":
                        # Almost all pass
                        status = "Pass" if rng.random() < 0.95 else "Warning"
                    elif fw_status == "At Risk":
                        # Distribute violations across controls
                        if violations_remaining > 0 and rng.random() < 0.15:
                            status = "Fail"
                            violations_remaining -= 1
                        elif rng.random() < 0.3:
                            status = "Warning"
                        else:
                            status = "Pass"
                    elif fw_status == "Needs Review":
                        if violations_remaining > 0 and rng.random() < 0.05:
                            status = "Fail"
                            violations_remaining -= 1
                        elif rng.random() < 0.2:
                            status = "Warning"
                        else:
                            status = "Pass"
                    else:
                        status = "Pass"

                    # Severity based on status
                    if status == "Fail":
                        severity = _pick(rng, ["Critical", "High"])
                    elif status == "Warning":
                        severity = _pick(rng, ["Medium", "Low"])
                    else:
                        severity = "Info"

                    # Finding detail
                    finding = _pick(rng, _FINDING_TEMPLATES.get(status, ["No finding."]))

                    scan_ts = cycle_start + timedelta(
                        seconds=int(rng.integers(0, 86400))
                    )

                    rows.append({
                        "scan_id": _uuid(),
                        "timestamp": scan_ts,
                        "framework": fw_name,
                        "domain": domain,
                        "control_id": control_id,
                        "control_name": control_name,
                        "status": status,
                        "finding_detail": finding,
                        "severity": severity,
                        "assessor": _pick(rng, _ASSESSORS),
                        "_ingested_at": now,
                    })

    # For incremental mode, only keep the latest cycle
    if mode == "incremental":
        cutoff = now - timedelta(hours=2)
        rows = [r for r in rows if r["timestamp"] >= cutoff]

    logger.info("Generated %d compliance scan rows (mode=%s)", len(rows), mode)
    return spark.createDataFrame(rows, schema=RAW_COMPLIANCE_SCANS_SCHEMA)


def generate_pii_detections(
    cfg: Dict[str, Any],
    mode: str = "full",
    seed: int = 44,
) -> DataFrame:
    """Generate synthetic PII detection rows for ``bronze.raw_pii_detections``.

    Simulates a scanning context of 1.2B records with 142 flagged anomalies.
    PII types include PHONE_NUMBER, EMAIL_ADDRESS, US_SSN, and CREDIT_CARD.
    Most detections occur in unmasked Silver tables.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
        seed: Numpy random seed.

    Returns:
        A PySpark DataFrame conforming to :data:`RAW_PII_DETECTIONS_SCHEMA`.
    """
    spark = get_spark()
    rng = np.random.default_rng(seed)

    pii_cfg = cfg["data"]["pii_monitoring"]
    pii_types = pii_cfg["pii_types"]
    total_anomalies = pii_cfg["flagged_anomalies"]  # 142

    n_detections = total_anomalies
    if mode == "incremental":
        n_detections = max(int(total_anomalies * _INCREMENTAL_FRACTION), 5)

    now = _utcnow()
    window_hours = 24 if mode == "full" else 1
    start = now - timedelta(hours=window_hours)
    timestamps = _random_timestamps(rng, start, now, n_detections)

    # Pipeline IDs
    pipeline_ids = [f"pipeline-{i:03d}" for i in range(1, 16)]

    rows: List[Dict[str, Any]] = []
    for i in range(n_detections):
        pii_type = _pick(rng, pii_types)
        table = _pick(rng, _PII_TARGET_TABLES)

        # Most detections in silver (unmasked) tables
        is_silver = table.startswith("silver.")

        # Column name based on PII type
        columns_for_type = _PII_COLUMNS_BY_TYPE.get(pii_type, ["unknown_col"])
        column = _pick(rng, columns_for_type)

        # Confidence: higher for well-known PII types
        if pii_type in ("US_SSN", "CREDIT_CARD"):
            confidence = float(rng.uniform(0.92, 0.99))
        else:
            confidence = float(rng.uniform(0.75, 0.98))

        # Record count: varies by table
        record_count = int(rng.integers(100, 500_000))

        # Masked status: bronze tables tend to be unmasked; silver tables
        # have some masked columns
        if is_silver:
            masked = bool(rng.random() < 0.3)  # 30% masked in silver
        else:
            masked = bool(rng.random() < 0.1)  # 10% masked in bronze

        rows.append({
            "detection_id": _uuid(),
            "timestamp": timestamps[i],
            "table_name": table,
            "column_name": column,
            "pii_type": pii_type,
            "confidence": round(confidence, 4),
            "record_count": record_count,
            "masked": masked,
            "pipeline_id": _pick(rng, pipeline_ids),
            "_ingested_at": now,
        })

    logger.info("Generated %d PII detection rows (mode=%s)", len(rows), mode)
    return spark.createDataFrame(rows, schema=RAW_PII_DETECTIONS_SCHEMA)


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
        catalog: UC catalog name (e.g. ``risk_iq``).
        schema:  UC schema / database name (e.g. ``bronze``).
        table:   Table name (e.g. ``raw_access_logs``).
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

    Generates access logs, compliance scans, and PII detections, then
    writes them to the corresponding bronze Delta tables.

    Args:
        cfg:  Loaded YAML config dict.
        mode: ``"full"`` or ``"incremental"``.
    """
    spark = get_spark()
    catalog: str = cfg["app"]["catalog"]

    logger.info(
        "=== RiskIQ Synthetic Data Generation ===  catalog=%s  mode=%s",
        catalog, mode,
    )

    _ensure_schemas_exist(spark, catalog)

    # --- Bronze: access logs ---
    access_df = generate_access_logs(cfg, mode=mode)
    _write_delta(access_df, catalog, "bronze", "raw_access_logs", mode)

    # --- Bronze: compliance scans ---
    compliance_df = generate_compliance_scans(cfg, mode=mode)
    _write_delta(compliance_df, catalog, "bronze", "raw_compliance_scans", mode)

    # --- Bronze: PII detections ---
    pii_df = generate_pii_detections(cfg, mode=mode)
    _write_delta(pii_df, catalog, "bronze", "raw_pii_detections", mode)

    logger.info("=== Generation complete ===")


# ===================================================================
# CLI entry point
# ===================================================================

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RiskIQ synthetic data generator",
    )
    parser.add_argument(
        "--use-case",
        type=str,
        default="risk",
        help="Use-case identifier (default: risk). "
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
        help="Path to the YAML config file. Defaults to config/risk.yaml.",
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
