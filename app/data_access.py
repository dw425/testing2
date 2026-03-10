"""
Centralized data access layer supporting all Blueprint IQ verticals.

Supports two modes:
  - DEMO mode  (USE_DEMO_DATA=true): returns config-driven demo data so the
    app renders without a live Databricks connection.  Works for all verticals:
    manufacturing, risk, healthcare, gaming, and financial_services.
  - LIVE mode: executes SQL via Databricks SQL Connector using SDK
    authentication against the Unity Catalog tables.
"""

import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import yaml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_config_cache: Optional[dict] = None
_config_use_case: Optional[str] = None


def _reset_config() -> None:
    """Clear the cached config so the next ``get_config()`` re-reads from disk.

    Useful when the ``USE_CASE`` environment variable has changed at runtime.
    """
    global _config_cache, _config_use_case
    _config_cache = None
    _config_use_case = None


def _current_use_case() -> str:
    """Return the active use-case identifier (e.g. 'manufacturing')."""
    return os.environ.get("USE_CASE", "manufacturing")


def get_config() -> dict:
    """Load the YAML config for the active use case.

    The use case is determined by the ``USE_CASE`` environment variable
    (default: ``manufacturing``).  Config files live in ``config/<use_case>.yaml``.

    The cache is automatically invalidated if ``USE_CASE`` has changed since
    the last call.
    """
    global _config_cache, _config_use_case

    use_case = _current_use_case()

    # Invalidate cache when the vertical changes
    if _config_use_case is not None and _config_use_case != use_case:
        _config_cache = None

    if _config_cache is not None:
        return _config_cache

    config_dir = os.path.join(os.path.dirname(__file__), "..", "config")
    config_path = os.path.join(config_dir, f"{use_case}.yaml")

    with open(config_path, "r") as fh:
        _config_cache = yaml.safe_load(fh)
    _config_use_case = use_case
    return _config_cache


def is_demo_mode() -> bool:
    """Return True when the app should use hardcoded demo data."""
    return os.environ.get("USE_DEMO_DATA", "true").lower() in ("true", "1", "yes")


# ---------------------------------------------------------------------------
# Databricks SQL connection (live mode only)
# ---------------------------------------------------------------------------


def get_connection():
    """Create a Databricks SQL connection using SDK default auth.

    Requires ``DATABRICKS_SERVER_HOSTNAME`` or a configured Databricks CLI
    profile.  Returns a ``databricks.sql.Connection`` object.
    """
    from databricks import sql as dbsql
    from databricks.sdk import WorkspaceClient

    cfg = get_config()
    w = WorkspaceClient()

    server_hostname = os.environ.get(
        "DATABRICKS_SERVER_HOSTNAME",
        w.config.host.replace("https://", ""),
    )
    http_path = os.environ.get(
        "DATABRICKS_HTTP_PATH",
        cfg["app"].get("warehouse_http_path", ""),
    )

    connection = dbsql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: w.config,
    )
    return connection


def _run_query(sql: str) -> List[Dict[str, Any]]:
    """Execute *sql* against Databricks and return rows as list of dicts."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return rows
    finally:
        conn.close()


# ===================================================================
#  HELPER UTILITIES -- name generation, ID hashing, date ranges
# ===================================================================

_FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
    "Charles", "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony",
    "Margaret", "Mark", "Sandra", "Donald", "Ashley", "Steven", "Kimberly",
    "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle", "Kenneth",
    "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca",
    "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia",
    "Jacob", "Kathleen", "Gary", "Amy", "Nicholas", "Angela", "Eric",
    "Shirley", "Jonathan", "Anna", "Stephen", "Brenda", "Larry", "Pamela",
    "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen", "Benjamin",
    "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory",
    "Debra", "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet",
    "Jack", "Catherine", "Dennis", "Maria", "Jerry", "Heather",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris",
    "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan",
    "Cooper", "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos",
    "Kim", "Cox", "Ward", "Richardson", "Watson", "Brooks", "Chavez",
    "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
    "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long",
    "Ross", "Foster", "Jimenez",
]

_USERNAMES_PREFIXES = [
    "Shadow", "Dark", "Blaze", "Storm", "Cyber", "Nova", "Pixel",
    "Frost", "Iron", "Ghost", "Neon", "Alpha", "Omega", "Zero",
    "Hyper", "Turbo", "Ultra", "Mega", "Star", "Thunder", "Dragon",
    "Phoenix", "Wolf", "Eagle", "Hawk", "Viper", "Cobra", "Tiger",
    "Lion", "Bear", "Falcon", "Raven", "Ninja", "Samurai", "Knight",
]

_USERNAMES_SUFFIXES = [
    "Slayer", "Hunter", "Master", "Lord", "King", "Warrior", "Rider",
    "Runner", "Walker", "Breaker", "Striker", "Sniper", "Raider",
    "Crusher", "Blaster", "Seeker", "Finder", "Maker", "Gamer",
    "Player", "Legend", "Hero", "Boss", "Chief", "Prime",
]


def _make_name(rng: np.random.RandomState) -> str:
    """Generate a realistic full name from the seed-controlled RNG."""
    return f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"


def _make_username(rng: np.random.RandomState, idx: int) -> str:
    """Generate a gamer-style username."""
    prefix = rng.choice(_USERNAMES_PREFIXES)
    suffix = rng.choice(_USERNAMES_SUFFIXES)
    num = rng.randint(1, 9999)
    return f"{prefix}{suffix}{num}"


def _make_id(prefix: str, idx: int, width: int = 6) -> str:
    """Deterministic ID like 'PAT-000142'."""
    return f"{prefix}-{str(idx).zfill(width)}"


def _make_email(name: str, domain: str = "company.com") -> str:
    """Turn 'John Smith' into 'j.smith@company.com'."""
    parts = name.lower().split()
    return f"{parts[0][0]}.{parts[-1]}@{domain}"


def _random_timestamps(rng: np.random.RandomState, n: int, days_back: int = 90,
                       base: Optional[datetime] = None) -> List[datetime]:
    """Return *n* random datetimes spread over the last *days_back* days."""
    base = base or datetime.utcnow()
    offsets = rng.randint(0, days_back * 86400, size=n)
    return [base - timedelta(seconds=int(o)) for o in offsets]


def _random_recent_timestamps(rng: np.random.RandomState, n: int, hours_back: int = 24,
                              base: Optional[datetime] = None) -> List[datetime]:
    """Return *n* random datetimes spread over the last *hours_back* hours."""
    base = base or datetime.utcnow()
    offsets = rng.randint(0, hours_back * 3600, size=n)
    return [base - timedelta(seconds=int(o)) for o in offsets]


# ===================================================================
#  DEMO DATA  --  hardcoded datasets matching the mockup screenshots
# ===================================================================

_DEMO_SITES = ["Berlin", "Detroit", "Tokyo"]
_DEMO_MACHINES = {
    "Berlin": ["CNC-Milling-BER-01", "CNC-Milling-BER-04", "Assembly-Arm-BER-07"],
    "Detroit": ["CNC-Line1-DET", "Lathe-DET-12", "Lathe-DET-14"],
    "Tokyo": ["Mill-C4-TOK", "Assembly-Arm-TOK-01", "Press-TOK-03"],
}


def _demo_production_kpis() -> Dict[str, Any]:
    """Return KPI dict.  For manufacturing the original hardcoded values are
    preserved.  For other verticals the dashboard KPIs from the config are
    flattened into a dict keyed by a snake-case version of the KPI title."""
    use_case = _current_use_case()
    if use_case == "manufacturing":
        return {
            "model_f1_score": 0.947,
            "inference_latency_ms": 42,
            "data_drift_pct": 1.2,
            "anomalies_1h": 87,
            "total_units_produced": 148_320,
            "total_defects": 2_966,
            "overall_yield_pct": 98.0,
            "oee_pct": 91.4,
            "snapshot_time": datetime.utcnow().isoformat(),
        }

    # Generic: build KPI dict from the config's dashboard.kpis section
    cfg = get_config()
    kpis_cfg = cfg.get("dashboard", {}).get("kpis", [])
    result: Dict[str, Any] = {"snapshot_time": datetime.utcnow().isoformat()}
    for kpi in kpis_cfg:
        key = kpi["title"].lower().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "")
        result[key] = kpi.get("value", kpi.get("delta", "N/A"))
    return result


def _demo_anomaly_scatter_data(hours: int = 1) -> List[Dict[str, Any]]:
    """Generate 5,000 scatter points for the anomaly chart.

    For manufacturing, the original fields (vibration_hz, temp_c, etc.) are
    used.  For other verticals, the scatter data uses the first ML model's
    features from the config as dimension labels, with generic numeric ranges.
    """
    rng = np.random.RandomState(42)
    use_case = _current_use_case()
    cfg = get_config()
    now = datetime.utcnow()
    points: List[Dict[str, Any]] = []

    if use_case == "manufacturing":
        sites = _DEMO_SITES
        machines = _DEMO_MACHINES
    else:
        data_cfg = cfg.get("data", {})
        sites = (
            data_cfg.get("sites")
            or data_cfg.get("facilities")
            or data_cfg.get("game_titles")
            or data_cfg.get("regions")
            or data_cfg.get("business_lines")
            or data_cfg.get("domains")
            or ["Site-A", "Site-B", "Site-C"]
        )
        machine_key = (
            data_cfg.get("machine_types")
            or data_cfg.get("equipment_types")
        )
        if isinstance(machine_key, dict):
            machines = machine_key
        else:
            machines = {s: [f"{s}-unit-{j}" for j in range(1, 4)] for s in sites}

    # Determine feature columns from the first ML model's features
    ml_cfg = cfg.get("ml", {})
    first_model = next(iter(ml_cfg.values()), {}) if ml_cfg else {}
    features = first_model.get("features", ["metric_a", "metric_b", "metric_c"])

    sites_list = list(sites) if not isinstance(sites, list) else sites

    for i in range(5000):
        site = sites_list[rng.randint(0, len(sites_list))]
        site_machines = machines.get(site, [f"{site}-unit-1"])
        machine = site_machines[rng.randint(0, len(site_machines))]
        is_anomaly = rng.random() < 0.15
        point: Dict[str, Any] = {
            "timestamp": (now - timedelta(minutes=int(rng.randint(0, hours * 60)))).isoformat(),
            "site": site,
            "machine_id": machine,
            "anomaly_score": round(float(rng.uniform(0.75, 0.99)), 3) if is_anomaly else round(float(rng.uniform(0.01, 0.40)), 3),
            "is_anomaly": bool(is_anomaly),
        }
        for feat in features[:5]:
            if is_anomaly:
                point[feat] = round(float(rng.uniform(0.65, 0.99)), 3)
            else:
                point[feat] = round(float(rng.uniform(0.01, 0.45)), 3)
        points.append(point)
    return points


def _demo_shap_importance() -> List[Dict[str, Any]]:
    """Return feature-importance list.

    For manufacturing the original hardcoded values are used.  For other
    verticals the features are read from the first ML model in the config
    and assigned descending synthetic importance values.
    """
    use_case = _current_use_case()
    if use_case == "manufacturing":
        return [
            {"feature": "vibration_hz", "importance": 0.34},
            {"feature": "tool_wear_index", "importance": 0.27},
            {"feature": "temp_c", "importance": 0.19},
            {"feature": "spindle_rpm", "importance": 0.12},
            {"feature": "feed_rate", "importance": 0.08},
        ]

    cfg = get_config()
    ml_cfg = cfg.get("ml", {})
    first_model = next(iter(ml_cfg.values()), {}) if ml_cfg else {}
    features = first_model.get("features", ["feature_a", "feature_b", "feature_c"])

    n = len(features)
    weights = [round(1.0 / (2 ** (i + 1)), 3) for i in range(n)]
    total = sum(weights)
    return [
        {"feature": f, "importance": round(w / total, 3)}
        for f, w in zip(features, weights)
    ]


def _demo_live_inference_feed(limit: int = 20) -> List[Dict[str, Any]]:
    """Return up to 5,000 recent inference predictions.

    Uses machine types / entity names from the config so the feed is
    contextually appropriate for any vertical.
    """
    rng = np.random.RandomState(43)
    cfg = get_config()
    use_case = _current_use_case()
    now = datetime.utcnow()

    if use_case == "manufacturing":
        sites = _DEMO_SITES
        machines = _DEMO_MACHINES
    else:
        data_cfg = cfg.get("data", {})
        sites = (
            data_cfg.get("sites")
            or data_cfg.get("facilities")
            or data_cfg.get("game_titles")
            or data_cfg.get("regions")
            or data_cfg.get("business_lines")
            or data_cfg.get("domains")
            or ["Entity-A", "Entity-B", "Entity-C"]
        )
        machine_key = (
            data_cfg.get("machine_types")
            or data_cfg.get("equipment_types")
        )
        if isinstance(machine_key, dict):
            machines = machine_key
        else:
            machines = {s: [f"{s}-unit-{j}" for j in range(1, 4)] for s in sites}

    sites_list = list(sites) if not isinstance(sites, list) else sites

    # Generate full 5K pool, return requested limit
    count = min(limit, 5000)
    feed: List[Dict[str, Any]] = []
    for i in range(count):
        site = sites_list[rng.randint(0, len(sites_list))]
        site_machines = machines.get(site, [f"{site}-unit-1"])
        machine = site_machines[rng.randint(0, len(site_machines))]
        is_anomaly = rng.random() < 0.12
        entry: Dict[str, Any] = {
            "timestamp": (now - timedelta(seconds=i * 3)).isoformat(),
            "site": site,
            "machine_id": machine,
            "anomaly_score": round(float(rng.uniform(0.78, 0.98)), 3) if is_anomaly else round(float(rng.uniform(0.02, 0.35)), 3),
            "prediction": "Anomaly" if is_anomaly else "Normal",
            "latency_ms": round(float(rng.uniform(30, 55)), 1),
        }
        if use_case == "manufacturing":
            entry["vibration_hz"] = round(float(rng.uniform(28, 45) if is_anomaly else rng.uniform(12, 24)), 1)
            entry["temp_c"] = round(float(rng.uniform(68, 88) if is_anomaly else rng.uniform(42, 58)), 1)
        else:
            ml_cfg = cfg.get("ml", {})
            first_model = next(iter(ml_cfg.values()), {}) if ml_cfg else {}
            features = first_model.get("features", ["metric_a", "metric_b"])
            for feat in features[:3]:
                entry[feat] = round(float(rng.uniform(0.6, 0.95) if is_anomaly else rng.uniform(0.05, 0.4)), 3)
        feed.append(entry)
    return feed


def _demo_inventory_status() -> List[Dict[str, Any]]:
    """Return inventory / component status with 50+ components across all sites.

    If the config contains ``data.inventory.components`` those are used as a
    seed and expanded.  Otherwise a comprehensive set of components is
    generated for the vertical.
    """
    rng = np.random.RandomState(44)
    cfg = get_config()
    inv_components = cfg.get("data", {}).get("inventory", {}).get("components")

    # Build the base component catalogue
    data_cfg = cfg.get("data", {})
    sites = (
        data_cfg.get("sites")
        or data_cfg.get("facilities")
        or data_cfg.get("game_titles")
        or data_cfg.get("regions")
        or data_cfg.get("business_lines")
        or data_cfg.get("domains")
        or ["Location-A", "Location-B", "Location-C"]
    )
    if not isinstance(sites, list):
        sites = list(sites)

    _component_names = [
        "Spindle-Motor", "Ball-Screw-Assembly", "Linear-Guide", "Servo-Drive",
        "Hydraulic-Pump", "Coolant-Filter", "Tool-Holder-HSK63", "Bearing-Set",
        "Control-Board-PCB", "Encoder-Rotary", "Proximity-Sensor", "Thermocouple",
        "Air-Filter-HEPA", "Pneumatic-Cylinder", "Relay-Module", "Power-Supply-24V",
        "Contactor-3P", "Fuse-Block", "Cable-Harness", "Touch-Panel-HMI",
    ]
    statuses = ["Healthy", "Healthy", "Healthy", "Low", "Critical"]

    items: List[Dict[str, Any]] = []

    # Incorporate config-provided components first
    if inv_components:
        for c in inv_components:
            items.append({
                "component": c.get("name", c.get("component", "Unknown")),
                "site": c.get("site", "N/A"),
                "stock_days": c.get("stock_days", 0),
                "status": c.get("status", "Unknown"),
                "current_stock": c.get("current_stock", 0),
                "daily_usage": c.get("daily_usage", 0),
            })

    # Expand to 50+ rows
    idx = len(items)
    while len(items) < 55:
        site = sites[idx % len(sites)]
        comp = _component_names[idx % len(_component_names)]
        status = statuses[rng.randint(0, len(statuses))]
        stock_days_map = {"Healthy": int(rng.randint(8, 30)),
                          "Low": int(rng.randint(2, 5)),
                          "Critical": round(float(rng.uniform(0.1, 1.5)), 1)}
        sd = stock_days_map[status]
        daily = int(rng.randint(50, 900))
        items.append({
            "component": f"{comp}-{idx:03d}",
            "site": site,
            "stock_days": sd,
            "status": status,
            "current_stock": int(sd * daily),
            "daily_usage": daily,
        })
        idx += 1
    return items


def _demo_quality_summary() -> Dict[str, Any]:
    return {
        "total_inspections": 2_854_000,
        "out_of_spec_count": 14,
        "out_of_spec_rate": 0.000005,
        "mean_deviation_um": 0.003,
        "std_deviation_um": 0.005,
        "cp": 3.33,
        "cpk": 3.10,
        "sites": [
            {"site": "Berlin", "inspections": 1_020_000, "out_of_spec": 4, "cpk": 3.22},
            {"site": "Detroit", "inspections": 985_000, "out_of_spec": 6, "cpk": 2.98},
            {"site": "Tokyo", "inspections": 849_000, "out_of_spec": 4, "cpk": 3.18},
        ],
    }


def _demo_build_tracking(
    batch_filter: Optional[str] = None,
    station_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Generate 5,000 build events across 200+ batches."""
    rng = np.random.RandomState(45)
    now = datetime.utcnow()

    stations = [
        "CNC Milling",
        "Lathe Processing",
        "Assembly Line A",
        "Assembly Line B",
        "Quality Check",
        "Packaging",
        "Welding Bay",
        "Surface Treatment",
        "Deburring",
        "Final Inspection",
    ]

    batch_statuses = ["Complete", "Defect", "In Progress", "Complete", "Complete"]
    batch_sites = _DEMO_SITES

    # Generate 210 batches
    batches: List[Dict[str, Any]] = []
    for b_idx in range(210):
        prefix = chr(65 + (b_idx % 26))  # A-Z cycling
        batch_id = f"{prefix}-{rng.randint(1000, 9999)}-{chr(65 + rng.randint(0, 26))}{chr(65 + rng.randint(0, 26))}{chr(65 + rng.randint(0, 26))}"
        status = batch_statuses[rng.randint(0, len(batch_statuses))]
        site = batch_sites[b_idx % len(batch_sites)]
        defect_flag = None
        if status == "Defect":
            defect_flag = rng.choice([
                "Vibration anomaly detected",
                "Temperature exceedance",
                "Dimensional out-of-spec",
                "Surface finish defect",
                "Material contamination",
            ])
        batches.append({
            "batch_id": batch_id,
            "status": status,
            "site": site,
            "defect_flag": defect_flag,
        })

    # Generate events -- target ~5000 events (210 batches x variable stations)
    events: List[Dict[str, Any]] = []
    for b in batches:
        # Each batch goes through a subset of stations
        n_stations = rng.randint(3, len(stations) + 1)
        selected_stations = list(rng.choice(stations, size=min(n_stations, len(stations)), replace=False))
        selected_stations.sort(key=lambda s: stations.index(s) if s in stations else 0)

        for seq, station in enumerate(selected_stations, start=1):
            is_defect = b["status"] == "Defect" and seq == 1
            units_in = int(rng.randint(200, 800))
            defect_count = int(rng.randint(8, 25)) if is_defect else int(rng.randint(0, 4))
            units_out = units_in - defect_count
            started_hours_ago = int(rng.randint(1, 2160))  # up to 90 days
            events.append({
                "batch_id": b["batch_id"],
                "site": b["site"],
                "station": station,
                "stage_seq": seq,
                "units_in": units_in,
                "units_out": units_out,
                "defect_count": defect_count,
                "yield_pct": round(units_out / units_in * 100, 2),
                "cycle_time_sec": round(float(rng.uniform(30, 300)), 1),
                "status": "Defect" if is_defect else (
                    "Complete" if b["status"] == "Complete" else "In Progress"
                ),
                "defect_flag": b["defect_flag"] if is_defect else None,
                "started_at": (now - timedelta(hours=started_hours_ago)).isoformat(),
                "completed_at": (
                    (now - timedelta(hours=max(0, started_hours_ago - int(rng.randint(1, 8))))).isoformat()
                    if b["status"] != "In Progress" else None
                ),
            })

    # Pad to 5000 if needed
    while len(events) < 5000:
        b = batches[rng.randint(0, len(batches))]
        station = stations[rng.randint(0, len(stations))]
        units_in = int(rng.randint(200, 800))
        defect_count = int(rng.randint(0, 5))
        units_out = units_in - defect_count
        started_hours_ago = int(rng.randint(1, 2160))
        events.append({
            "batch_id": b["batch_id"],
            "site": b["site"],
            "station": station,
            "stage_seq": rng.randint(1, len(stations) + 1),
            "units_in": units_in,
            "units_out": units_out,
            "defect_count": defect_count,
            "yield_pct": round(units_out / units_in * 100, 2),
            "cycle_time_sec": round(float(rng.uniform(30, 300)), 1),
            "status": b["status"],
            "defect_flag": b["defect_flag"] if b["status"] == "Defect" else None,
            "started_at": (now - timedelta(hours=started_hours_ago)).isoformat(),
            "completed_at": (
                (now - timedelta(hours=max(0, started_hours_ago - int(rng.randint(1, 8))))).isoformat()
                if b["status"] != "In Progress" else None
            ),
        })

    events = events[:5000]

    if batch_filter:
        events = [e for e in events if e["batch_id"] == batch_filter]
    if station_filter:
        events = [e for e in events if e["station"] == station_filter]

    return events


# ===================================================================
#  HEALTHCARE-SPECIFIC DEMO DATA
# ===================================================================

_HC_FACILITIES = ["Metro General Hospital", "Westside Medical Center", "Eastview Community Clinic"]
_HC_DEPARTMENTS = ["Emergency", "ICU", "Cardiology", "Orthopedics", "Oncology", "Pediatrics"]
_HC_SPECIALTIES = [
    "Internal Medicine", "Emergency Medicine", "Cardiology", "Orthopedic Surgery",
    "Oncology", "Pediatrics", "Neurology", "Pulmonology", "General Surgery",
    "Anesthesiology", "Radiology", "Pathology", "Psychiatry", "Dermatology",
    "Ophthalmology", "Urology", "Nephrology", "Endocrinology", "Gastroenterology",
    "Rheumatology",
]
_HC_OUTCOMES = ["Discharged", "Admitted", "Transferred", "Left AMA", "Observation"]
_HC_ACUITY = [1, 2, 3, 4, 5]  # 1=Resuscitation, 5=Non-urgent
_HC_PROCEDURE_CODES = [
    ("99213", "Office Visit - Est. Patient", 150, 350),
    ("99214", "Office Visit - Detailed", 200, 450),
    ("99284", "ED Visit - High Severity", 800, 2500),
    ("99285", "ED Visit - Critical", 1200, 4000),
    ("99291", "Critical Care - First Hour", 1500, 5000),
    ("36415", "Venipuncture", 25, 80),
    ("71046", "Chest X-Ray 2 Views", 100, 350),
    ("74177", "CT Abdomen w/Contrast", 500, 2200),
    ("70553", "MRI Brain w/wo Contrast", 800, 3500),
    ("93000", "Electrocardiogram", 50, 200),
    ("93306", "Echocardiography", 400, 1500),
    ("43239", "Upper GI Endoscopy w/Biopsy", 600, 3000),
    ("27447", "Total Knee Replacement", 15000, 45000),
    ("33533", "CABG - Single Graft", 25000, 80000),
    ("47562", "Laparoscopic Cholecystectomy", 5000, 18000),
    ("59400", "Obstetric Care - Vaginal", 3000, 12000),
    ("59510", "Obstetric Care - Cesarean", 5000, 20000),
    ("20610", "Joint Injection", 100, 400),
    ("90837", "Psychotherapy 60 min", 120, 250),
    ("96360", "IV Infusion - First Hour", 150, 500),
]
_HC_BILLING_STATUS = ["Paid", "Pending", "Denied", "Appeals"]


def _demo_patient_wait_times() -> List[Dict[str, Any]]:
    """Generate 5,000 patient wait-time records across facilities and departments."""
    rng = np.random.RandomState(100)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for i in range(5000):
        facility = _HC_FACILITIES[rng.randint(0, len(_HC_FACILITIES))]
        department = _HC_DEPARTMENTS[rng.randint(0, len(_HC_DEPARTMENTS))]
        acuity = int(rng.choice(_HC_ACUITY, p=[0.03, 0.12, 0.35, 0.35, 0.15]))

        # Wait time depends on acuity -- lower acuity = longer wait
        base_wait = {1: 2, 2: 8, 3: 22, 4: 45, 5: 75}[acuity]
        wait_min = max(1, int(rng.normal(base_wait, base_wait * 0.4)))

        days_ago = int(rng.randint(0, 90))
        hour = int(rng.choice(range(24), p=[
            0.015, 0.010, 0.008, 0.008, 0.010, 0.015, 0.025, 0.045,
            0.065, 0.070, 0.075, 0.075, 0.070, 0.065, 0.060, 0.055,
            0.055, 0.050, 0.050, 0.045, 0.040, 0.035, 0.025, 0.029,
        ]))
        arrival = (now - timedelta(days=days_ago)).replace(
            hour=hour, minute=int(rng.randint(0, 60)),
            second=int(rng.randint(0, 60)), microsecond=0
        )
        triage_time = arrival + timedelta(minutes=int(rng.randint(2, 15)))
        seen_time = triage_time + timedelta(minutes=wait_min)
        outcome = _HC_OUTCOMES[rng.randint(0, len(_HC_OUTCOMES))]

        records.append({
            "patient_id": _make_id("PAT", i),
            "facility": facility,
            "department": department,
            "arrival_time": arrival.isoformat(),
            "triage_time": triage_time.isoformat(),
            "seen_by_dr_time": seen_time.isoformat(),
            "wait_minutes": wait_min,
            "acuity_level": acuity,
            "outcome": outcome,
        })
    return records


def _demo_doctor_availability() -> List[Dict[str, Any]]:
    """Generate schedules for 200+ doctors across facilities."""
    rng = np.random.RandomState(101)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []
    availability_statuses = ["Available", "In Surgery", "On Break", "Off Duty"]
    avail_weights = [0.45, 0.20, 0.10, 0.25]

    for i in range(220):
        name = _make_name(rng)
        facility = _HC_FACILITIES[i % len(_HC_FACILITIES)]
        department = _HC_DEPARTMENTS[i % len(_HC_DEPARTMENTS)]
        specialty = _HC_SPECIALTIES[rng.randint(0, len(_HC_SPECIALTIES))]

        # Shift patterns
        shift_hour = int(rng.choice([6, 7, 8, 14, 22]))
        shift_start = now.replace(hour=shift_hour, minute=0, second=0, microsecond=0)
        shift_length = int(rng.choice([8, 10, 12]))
        shift_end = shift_start + timedelta(hours=shift_length)

        status = str(rng.choice(availability_statuses, p=avail_weights))
        patients_today = int(rng.randint(0, 35)) if status != "Off Duty" else 0
        avg_consult = round(float(rng.uniform(8, 45)), 1)

        records.append({
            "doctor_id": _make_id("DR", i),
            "name": f"Dr. {name}",
            "facility": facility,
            "department": department,
            "specialty": specialty,
            "shift_start": shift_start.isoformat(),
            "shift_end": shift_end.isoformat(),
            "patients_seen_today": patients_today,
            "availability_status": status,
            "avg_consult_min": avg_consult,
        })
    return records


def _demo_billing_data() -> List[Dict[str, Any]]:
    """Generate 5,000 billing records with realistic procedure and insurance data."""
    rng = np.random.RandomState(102)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for i in range(5000):
        facility = _HC_FACILITIES[rng.randint(0, len(_HC_FACILITIES))]
        department = _HC_DEPARTMENTS[rng.randint(0, len(_HC_DEPARTMENTS))]
        proc = _HC_PROCEDURE_CODES[rng.randint(0, len(_HC_PROCEDURE_CODES))]
        code, proc_name, lo, hi = proc

        charge = round(float(rng.uniform(lo, hi)), 2)
        status = str(rng.choice(_HC_BILLING_STATUS, p=[0.55, 0.25, 0.12, 0.08]))

        insurance_pct = float(rng.uniform(0.60, 0.95)) if status != "Denied" else 0.0
        insurance_paid = round(charge * insurance_pct, 2)
        patient_resp = round(charge - insurance_paid, 2)

        submitted_days_ago = int(rng.randint(1, 90))
        submitted = (now - timedelta(days=submitted_days_ago)).date()
        paid_date = None
        if status == "Paid":
            paid_date = (submitted + timedelta(days=int(rng.randint(5, 45)))).isoformat()

        records.append({
            "billing_id": _make_id("BIL", i),
            "patient_id": _make_id("PAT", rng.randint(0, 5000)),
            "facility": facility,
            "department": department,
            "procedure_code": code,
            "procedure_name": proc_name,
            "charge_amount": charge,
            "insurance_paid": insurance_paid,
            "patient_responsibility": patient_resp,
            "billing_status": status,
            "submitted_date": submitted.isoformat(),
            "paid_date": paid_date,
        })
    return records


def _demo_healthcare_revenue() -> List[Dict[str, Any]]:
    """Generate daily/monthly revenue per facility and department."""
    rng = np.random.RandomState(103)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for day_offset in range(90):
        date = (now - timedelta(days=day_offset)).date()
        for facility in _HC_FACILITIES:
            for department in _HC_DEPARTMENTS:
                # Base revenue varies by department
                base = {
                    "Emergency": 85000, "ICU": 120000, "Cardiology": 95000,
                    "Orthopedics": 75000, "Oncology": 110000, "Pediatrics": 45000,
                }.get(department, 60000)

                gross = round(float(rng.normal(base, base * 0.15)), 2)
                denial_rate = round(float(rng.uniform(0.03, 0.18)), 4)
                denials = round(gross * denial_rate, 2)
                net = round(gross - denials, 2)
                collections = round(net * float(rng.uniform(0.80, 0.97)), 2)
                avg_reimb = round(float(rng.uniform(0.55, 0.92)), 4)

                records.append({
                    "date": date.isoformat(),
                    "facility": facility,
                    "department": department,
                    "gross_revenue": gross,
                    "net_revenue": net,
                    "collections": collections,
                    "denials": denials,
                    "denial_rate": denial_rate,
                    "avg_reimbursement": avg_reimb,
                })
    return records


def _demo_bed_utilization() -> List[Dict[str, Any]]:
    """Generate real-time bed utilization data per facility and department."""
    rng = np.random.RandomState(104)
    records: List[Dict[str, Any]] = []

    bed_counts = {
        ("Metro General Hospital", "Emergency"): 45,
        ("Metro General Hospital", "ICU"): 30,
        ("Metro General Hospital", "Cardiology"): 40,
        ("Metro General Hospital", "Orthopedics"): 35,
        ("Metro General Hospital", "Oncology"): 28,
        ("Metro General Hospital", "Pediatrics"): 25,
        ("Westside Medical Center", "Emergency"): 38,
        ("Westside Medical Center", "ICU"): 22,
        ("Westside Medical Center", "Cardiology"): 30,
        ("Westside Medical Center", "Orthopedics"): 28,
        ("Westside Medical Center", "Oncology"): 20,
        ("Westside Medical Center", "Pediatrics"): 18,
        ("Eastview Community Clinic", "Emergency"): 20,
        ("Eastview Community Clinic", "ICU"): 10,
        ("Eastview Community Clinic", "Cardiology"): 15,
        ("Eastview Community Clinic", "Orthopedics"): 12,
        ("Eastview Community Clinic", "Oncology"): 8,
        ("Eastview Community Clinic", "Pediatrics"): 10,
    }

    for (facility, department), total_beds in bed_counts.items():
        util_pct = float(rng.uniform(0.55, 0.98))
        occupied = int(round(total_beds * util_pct))
        available = total_beds - occupied
        avg_los = round(float(rng.uniform(4, 120)), 1)  # hours
        turnover = round(float(rng.uniform(0.8, 3.5)), 2)

        records.append({
            "facility": facility,
            "department": department,
            "total_beds": total_beds,
            "occupied": occupied,
            "available": available,
            "utilization_pct": round(util_pct * 100, 1),
            "avg_los_hours": avg_los,
            "turnover_rate": turnover,
        })
    return records


# ===================================================================
#  GAMING-SPECIFIC DEMO DATA
# ===================================================================

_GM_TITLES = ["Stellar Conquest", "Shadow Realms", "Velocity Rush"]
_GM_REGIONS = ["NA-East", "NA-West", "EU-West", "EU-North", "APAC-SEA", "APAC-JP"]
_GM_SEGMENTS = ["Whale", "Dolphin", "Minnow", "Free-to-Play"]
_GM_DEVICE_TYPES = ["PC", "PlayStation 5", "Xbox Series X", "Nintendo Switch", "iOS", "Android"]
_GM_OS_VERSIONS = [
    "Windows 11 23H2", "Windows 10 22H2", "macOS 14.3", "iOS 17.4",
    "Android 14", "Android 13", "PS5 v24.01", "Xbox v2411",
]
_GM_ACQ_SOURCES = ["Organic", "Paid Social", "Influencer", "App Store", "Cross-Promo", "Referral"]
_GM_CAMPAIGN_NAMES = [
    "Summer Blitz 2025", "Holiday Push Q4", "Influencer Wave 3", "Retarget Lapsed",
    "Launch Day Boost", "Cross-Promo Bundle", "TikTok Creator Collab", "Reddit AMA",
    "Twitch Drops Event", "App Store Feature", "Google UAC Auto", "Meta Lookalike V2",
    "YouTube Pre-Roll", "Organic ASO", "Referral Program V3", "Brand Campaign Q1",
]


def _demo_server_uptime() -> List[Dict[str, Any]]:
    """Generate 5,000 server uptime/health records."""
    rng = np.random.RandomState(200)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []
    statuses = ["Online", "Online", "Online", "Online", "Degraded", "Maintenance"]

    for i in range(5000):
        region = _GM_REGIONS[rng.randint(0, len(_GM_REGIONS))]
        game = _GM_TITLES[rng.randint(0, len(_GM_TITLES))]
        status = str(rng.choice(statuses))

        uptime = round(float(rng.uniform(
            99.5, 100.0) if status == "Online" else rng.uniform(85.0, 99.4)), 2)
        latency = round(float(
            rng.uniform(8, 45) if status == "Online" else rng.uniform(45, 200)), 1)
        cpu = round(float(
            rng.uniform(15, 75) if status == "Online" else rng.uniform(75, 99)), 1)
        mem = round(float(
            rng.uniform(30, 70) if status == "Online" else rng.uniform(70, 98)), 1)
        connections = int(rng.randint(50, 12000) if status == "Online" else rng.randint(0, 500))
        last_restart = (now - timedelta(hours=int(rng.randint(1, 720)))).isoformat()

        records.append({
            "server_id": f"SRV-{region[:2]}-{game[:3].upper()}-{i:04d}",
            "region": region,
            "game_title": game,
            "status": status,
            "uptime_pct": uptime,
            "latency_ms": latency,
            "cpu_pct": cpu,
            "memory_pct": mem,
            "active_connections": connections,
            "last_restart": last_restart,
        })
    return records


def _demo_player_metrics() -> List[Dict[str, Any]]:
    """Generate 5,000 player metric records with LTV and churn risk."""
    rng = np.random.RandomState(201)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for i in range(5000):
        game = _GM_TITLES[rng.randint(0, len(_GM_TITLES))]
        region = _GM_REGIONS[rng.randint(0, len(_GM_REGIONS))]
        segment = str(rng.choice(_GM_SEGMENTS, p=[0.03, 0.12, 0.35, 0.50]))

        ltv_map = {"Whale": (200, 5000), "Dolphin": (30, 200),
                    "Minnow": (5, 30), "Free-to-Play": (0, 5)}
        ltv_lo, ltv_hi = ltv_map[segment]
        ltv = round(float(rng.uniform(ltv_lo, ltv_hi)), 2)

        acq_date = now - timedelta(days=int(rng.randint(1, 730)))
        last_login = now - timedelta(hours=int(rng.randint(0, 2160)))
        days_since = (now - last_login).days
        churn_risk = round(min(1.0, float(rng.uniform(0, 0.3) + days_since * 0.01)), 3)

        records.append({
            "player_id": _make_id("PLR", i),
            "username": _make_username(rng, i),
            "game_title": game,
            "region": region,
            "segment": segment,
            "total_playtime_hours": round(float(rng.uniform(1, 5000)), 1),
            "sessions_7d": int(rng.randint(0, 50)),
            "sessions_30d": int(rng.randint(0, 180)),
            "purchases_30d": int(rng.randint(0, 25)) if segment != "Free-to-Play" else 0,
            "ltv": ltv,
            "last_login": last_login.isoformat(),
            "churn_risk": churn_risk,
            "acquisition_source": str(rng.choice(_GM_ACQ_SOURCES)),
            "acquisition_date": acq_date.date().isoformat(),
        })
    return records


def _demo_session_tracking() -> List[Dict[str, Any]]:
    """Generate 5,000 game session records with performance data."""
    rng = np.random.RandomState(202)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for i in range(5000):
        game = _GM_TITLES[rng.randint(0, len(_GM_TITLES))]
        region = _GM_REGIONS[rng.randint(0, len(_GM_REGIONS))]
        device = _GM_DEVICE_TYPES[rng.randint(0, len(_GM_DEVICE_TYPES))]
        os_ver = _GM_OS_VERSIONS[rng.randint(0, len(_GM_OS_VERSIONS))]

        start = now - timedelta(hours=int(rng.randint(0, 2160)))
        duration = int(rng.lognormal(3.5, 0.8))  # median ~33 min, long tail
        duration = max(1, min(duration, 720))  # cap at 12 hours
        end = start + timedelta(minutes=duration)
        events = int(rng.randint(10, 5000))
        crashes = int(rng.choice([0, 0, 0, 0, 0, 0, 0, 1, 1, 2]))
        fps = round(float(rng.uniform(25, 144)), 1)

        records.append({
            "session_id": _make_id("SES", i, width=8),
            "player_id": _make_id("PLR", rng.randint(0, 5000)),
            "game_title": game,
            "region": region,
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
            "duration_min": duration,
            "events_count": events,
            "crashes": crashes,
            "fps_avg": fps,
            "device_type": device,
            "os_version": os_ver,
        })
    return records


def _demo_attribution_data() -> List[Dict[str, Any]]:
    """Generate acquisition attribution data across sources and campaigns."""
    rng = np.random.RandomState(203)
    records: List[Dict[str, Any]] = []

    for source in _GM_ACQ_SOURCES:
        for game in _GM_TITLES:
            # Multiple campaigns per source x game
            n_campaigns = rng.randint(2, 6)
            for c_idx in range(n_campaigns):
                campaign = _GM_CAMPAIGN_NAMES[rng.randint(0, len(_GM_CAMPAIGN_NAMES))]
                installs = int(rng.randint(500, 50000))
                d1 = int(installs * float(rng.uniform(0.35, 0.75)))
                d7 = int(installs * float(rng.uniform(0.18, 0.50)))
                d30 = int(installs * float(rng.uniform(0.08, 0.30)))

                cost = round(float(rng.uniform(500, 80000)), 2)
                revenue = round(float(rng.uniform(200, 120000)), 2)
                roas = round(revenue / max(cost, 1), 2)
                ltv_pred = round(float(rng.uniform(5, 120)), 2)

                records.append({
                    "source": source,
                    "campaign": campaign,
                    "game_title": game,
                    "installs": installs,
                    "d1_retained": d1,
                    "d7_retained": d7,
                    "d30_retained": d30,
                    "cost": cost,
                    "revenue": revenue,
                    "roas": roas,
                    "ltv_predicted": ltv_pred,
                })
    return records


def _demo_dau_history() -> List[Dict[str, Any]]:
    """Generate 90 days of DAU/MAU data per game and region."""
    rng = np.random.RandomState(204)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    base_dau = {
        "Stellar Conquest": 950000,
        "Shadow Realms": 720000,
        "Velocity Rush": 530000,
    }

    for day_offset in range(90):
        date = (now - timedelta(days=day_offset)).date()
        for game in _GM_TITLES:
            for region in _GM_REGIONS:
                region_share = {
                    "NA-East": 0.25, "NA-West": 0.18, "EU-West": 0.22,
                    "EU-North": 0.10, "APAC-SEA": 0.15, "APAC-JP": 0.10,
                }[region]

                bdau = int(base_dau[game] * region_share)
                dau = int(rng.normal(bdau, bdau * 0.08))
                wau = int(dau * float(rng.uniform(2.5, 4.0)))
                mau = int(dau * float(rng.uniform(6.0, 12.0)))
                new_users = int(dau * float(rng.uniform(0.02, 0.10)))
                returning = dau - new_users
                peak = int(dau * float(rng.uniform(0.35, 0.55)))
                revenue = round(dau * float(rng.uniform(0.08, 0.18)), 2)

                records.append({
                    "date": date.isoformat(),
                    "game_title": game,
                    "region": region,
                    "dau": max(0, dau),
                    "wau": max(0, wau),
                    "mau": max(0, mau),
                    "new_users": max(0, new_users),
                    "returning_users": max(0, returning),
                    "concurrent_peak": max(0, peak),
                    "revenue": revenue,
                })
    return records


# ===================================================================
#  FINANCIAL SERVICES-SPECIFIC DEMO DATA
# ===================================================================

_FS_BUSINESS_LINES = ["Retail Banking", "Commercial Lending", "Wealth Management", "Insurance"]
_FS_REGIONS = ["Northeast", "Southeast", "Midwest", "West Coast", "International"]
_FS_CHANNELS = ["Mobile App", "Web Banking", "ATM", "Branch", "Wire Transfer", "ACH"]
_FS_LTV_SEGMENTS = ["Platinum", "Gold", "Silver", "Bronze"]
_FS_FRAUD_TYPES = ["Card Not Present", "Account Takeover", "Identity Theft", "Synthetic"]
_FS_INVESTIGATION_STATUS = ["Open", "Under Review", "Escalated", "Closed", "False Positive"]
_FS_ASSET_CLASSES = ["US Equities", "International Equities", "Fixed Income",
                     "Real Estate", "Commodities", "Alternatives", "Cash"]
_FS_MERCHANTS = [
    "Amazon", "Walmart", "Target", "Best Buy", "Home Depot", "Costco",
    "Starbucks", "McDonalds", "Shell Gas", "Uber", "Netflix", "Apple",
    "Whole Foods", "CVS Pharmacy", "Airbnb", "Delta Airlines", "Hilton",
    "Nike", "Sephora", "Zara", "Trader Joes", "Cheesecake Factory",
    "Lyft", "DoorDash", "Grubhub", "Spotify", "Hulu", "Peloton",
    "Etsy", "Nordstrom",
]


def _demo_customer_ltv() -> List[Dict[str, Any]]:
    """Generate 5,000 customer LTV records with segmentation."""
    rng = np.random.RandomState(300)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for i in range(5000):
        name = _make_name(rng)
        biz_line = _FS_BUSINESS_LINES[rng.randint(0, len(_FS_BUSINESS_LINES))]
        region = _FS_REGIONS[rng.randint(0, len(_FS_REGIONS))]
        acct_age = round(float(rng.uniform(0.5, 35)), 1)
        monthly_rev = round(float(rng.lognormal(5.5, 1.2)), 2)
        total_rev = round(monthly_rev * acct_age * 12, 2)
        products = int(rng.randint(1, 12))
        ltv = round(total_rev * float(rng.uniform(0.8, 1.5)), 2)

        if ltv > 500000:
            seg = "Platinum"
        elif ltv > 100000:
            seg = "Gold"
        elif ltv > 25000:
            seg = "Silver"
        else:
            seg = "Bronze"

        churn_prob = round(float(rng.uniform(0.01, 0.45)), 3)
        last_txn = now - timedelta(days=int(rng.randint(0, 90)))

        records.append({
            "customer_id": _make_id("CUS", i),
            "name": name,
            "business_line": biz_line,
            "region": region,
            "account_age_years": acct_age,
            "total_revenue": total_rev,
            "monthly_revenue": monthly_rev,
            "products_held": products,
            "ltv": ltv,
            "ltv_segment": seg,
            "churn_probability": churn_prob,
            "last_transaction": last_txn.isoformat(),
        })
    return records


def _demo_daily_revenue() -> List[Dict[str, Any]]:
    """Generate 365 days of daily revenue by business line and region."""
    rng = np.random.RandomState(301)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    base_revenue = {
        "Retail Banking": 3200000,
        "Commercial Lending": 5800000,
        "Wealth Management": 4100000,
        "Insurance": 2700000,
    }

    for day_offset in range(365):
        date = (now - timedelta(days=day_offset)).date()
        for biz in _FS_BUSINESS_LINES:
            for region in _FS_REGIONS:
                region_share = {
                    "Northeast": 0.28, "Southeast": 0.20, "Midwest": 0.18,
                    "West Coast": 0.24, "International": 0.10,
                }[region]

                base = base_revenue[biz] * region_share
                gross = round(float(rng.normal(base, base * 0.10)), 2)
                fees = round(gross * float(rng.uniform(0.01, 0.05)), 2)
                charge_offs = round(gross * float(rng.uniform(0.001, 0.015)), 2)
                net = round(gross - charge_offs, 2)
                txn_count = int(rng.randint(500, 25000))
                avg_txn = round(gross / max(txn_count, 1), 2)
                nii = round(gross * float(rng.uniform(0.20, 0.45)), 2)

                records.append({
                    "date": date.isoformat(),
                    "business_line": biz,
                    "region": region,
                    "gross_revenue": gross,
                    "net_revenue": net,
                    "transaction_count": txn_count,
                    "avg_transaction": avg_txn,
                    "fees_collected": fees,
                    "charge_offs": charge_offs,
                    "net_interest_income": nii,
                })
    return records


def _demo_revenue_change() -> List[Dict[str, Any]]:
    """Generate MoM/YoY revenue change metrics by business line."""
    rng = np.random.RandomState(302)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for month_offset in range(12):
        month_date = (now - timedelta(days=month_offset * 30)).replace(day=1).date()
        for biz in _FS_BUSINESS_LINES:
            revenue = round(float(rng.uniform(60_000_000, 180_000_000)), 2)
            prev_month = round(revenue * float(rng.uniform(0.92, 1.08)), 2)
            mom_change = round((revenue - prev_month) / prev_month * 100, 2)
            prev_year = round(revenue * float(rng.uniform(0.85, 1.15)), 2)
            yoy_change = round((revenue - prev_year) / prev_year * 100, 2)
            forecast = round(revenue * float(rng.uniform(0.97, 1.06)), 2)

            records.append({
                "month": month_date.isoformat(),
                "business_line": biz,
                "revenue": revenue,
                "prev_month_revenue": prev_month,
                "mom_change_pct": mom_change,
                "prev_year_revenue": prev_year,
                "yoy_change_pct": yoy_change,
                "forecast_next_month": forecast,
            })
    return records


def _demo_fraud_transactions() -> List[Dict[str, Any]]:
    """Generate 5,000 scored fraud transactions."""
    rng = np.random.RandomState(303)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    for i in range(5000):
        ts = now - timedelta(seconds=int(rng.randint(0, 90 * 86400)))
        channel = _FS_CHANNELS[rng.randint(0, len(_FS_CHANNELS))]
        merchant = _FS_MERCHANTS[rng.randint(0, len(_FS_MERCHANTS))]
        amount = round(float(rng.lognormal(4.5, 1.5)), 2)

        is_fraud = rng.random() < 0.023  # ~2.3% fraud rate
        fraud_score = round(float(
            rng.uniform(0.75, 1.0) if is_fraud else rng.uniform(0.0, 0.40)), 3)
        fraud_type = str(rng.choice(_FS_FRAUD_TYPES)) if is_fraud else None
        blocked = bool(is_fraud and rng.random() < 0.85)
        inv_status = str(rng.choice(_FS_INVESTIGATION_STATUS)) if is_fraud else None

        records.append({
            "transaction_id": _make_id("TXN", i, width=8),
            "timestamp": ts.isoformat(),
            "customer_id": _make_id("CUS", rng.randint(0, 5000)),
            "channel": channel,
            "merchant": merchant,
            "amount": amount,
            "fraud_score": fraud_score,
            "is_fraud": bool(is_fraud),
            "fraud_type": fraud_type,
            "blocked": blocked,
            "investigation_status": inv_status,
        })
    return records


def _demo_portfolio_performance() -> List[Dict[str, Any]]:
    """Generate daily portfolio performance data across asset classes."""
    rng = np.random.RandomState(304)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    base_aum = {
        "US Equities": 8_500_000_000, "International Equities": 4_200_000_000,
        "Fixed Income": 6_100_000_000, "Real Estate": 2_800_000_000,
        "Commodities": 1_500_000_000, "Alternatives": 900_000_000,
        "Cash": 500_000_000,
    }

    for day_offset in range(365):
        date = (now - timedelta(days=day_offset)).date()
        for asset_class in _FS_ASSET_CLASSES:
            for region in ["Domestic", "International"]:
                aum_base = base_aum[asset_class] * (0.7 if region == "Domestic" else 0.3)
                drift = float(rng.normal(0, 0.005))
                aum = round(aum_base * (1 + drift * day_offset * 0.001), 2)
                daily_ret = round(float(rng.normal(0.0003, 0.012)), 6)
                cum_ret = round(daily_ret * (365 - day_offset) * 0.8, 6)
                var95 = round(aum * abs(float(rng.normal(0.02, 0.005))), 2)
                sharpe = round(float(rng.normal(1.2, 0.4)), 3)
                bench = round(daily_ret + float(rng.normal(0, 0.003)), 6)
                alpha = round(daily_ret - bench, 6)
                tracking = round(abs(float(rng.normal(0.02, 0.008))), 4)

                records.append({
                    "date": date.isoformat(),
                    "asset_class": asset_class,
                    "region": region,
                    "aum": aum,
                    "daily_return": daily_ret,
                    "cumulative_return": cum_ret,
                    "var_95": var95,
                    "sharpe": sharpe,
                    "benchmark_return": bench,
                    "alpha": alpha,
                    "tracking_error": tracking,
                })
    return records


# ===================================================================
#  RISK-SPECIFIC DEMO DATA
# ===================================================================

_RISK_FRAMEWORKS = [
    ("GDPR", "v2.0", "EU"),
    ("CCPA", "v1.3", "California"),
    ("HIPAA", "v2.1", "US"),
    ("SOC 2 Type II", "v2023", "Global"),
    ("PCI-DSS", "v4.0", "Global"),
    ("ISO 27001", "v2022", "Global"),
    ("NIST CSF", "v2.0", "US"),
    ("GLBA", "v1.0", "US"),
    ("FERPA", "v1.2", "US"),
    ("SOX", "v2024", "US"),
]
_RISK_AUDITORS = [
    "Deloitte", "PwC", "EY", "KPMG", "BDO", "Grant Thornton",
    "RSM", "Crowe", "Baker Tilly", "Moss Adams",
]
_RISK_ACTIONS = ["SELECT", "INSERT", "DELETE", "EXPORT", "UPDATE", "ALTER"]
_RISK_SENSITIVITY = ["Public", "Internal", "Confidential", "Restricted"]
_RISK_PII_TYPES = [
    "PHONE_NUMBER", "EMAIL_ADDRESS", "US_SSN", "CREDIT_CARD",
    "DATE_OF_BIRTH", "PASSPORT_NUMBER", "DRIVERS_LICENSE",
    "IP_ADDRESS", "MEDICAL_RECORD_NUMBER", "BANK_ACCOUNT",
    "TAX_ID", "BIOMETRIC_DATA",
]
_RISK_GEO_LOCATIONS = [
    "New York, US", "San Francisco, US", "London, UK", "Frankfurt, DE",
    "Tokyo, JP", "Singapore, SG", "Sydney, AU", "Toronto, CA",
    "Paris, FR", "Mumbai, IN", "Sao Paulo, BR", "Dubai, AE",
    "Chicago, US", "Boston, US", "Seattle, US", "Austin, US",
    "Berlin, DE", "Amsterdam, NL", "Hong Kong, HK", "Seoul, KR",
]
_RISK_ROLES = [
    "Data Analyst", "Data Engineer", "ML Engineer", "DBA",
    "Security Admin", "Compliance Officer", "VP Engineering",
    "Director Analytics", "System Admin", "Service Account",
    "Auditor", "Data Scientist", "Product Manager", "DevOps Engineer",
]
_RISK_CATALOGS = ["analytics_prod", "ml_workspace", "finance_dw", "hr_data", "customer_360"]
_RISK_SCHEMAS = ["bronze", "silver", "gold", "staging", "raw", "curated"]
_RISK_TABLES = [
    "customers", "transactions", "employees", "payroll", "medical_claims",
    "credit_applications", "user_sessions", "audit_trail", "patient_records",
    "investment_accounts", "insurance_policies", "loan_applications",
    "contact_info", "tax_documents", "background_checks",
]
_RISK_COLUMNS = [
    "ssn", "email", "phone", "dob", "credit_card_number", "account_number",
    "name", "address", "salary", "diagnosis_code", "passport_id",
    "drivers_license", "ip_address", "biometric_hash", "tax_id",
]
_RISK_REMEDIATION = ["Completed", "In Progress", "Pending", "Deferred", "Escalated"]


def _demo_compliance_frameworks() -> List[Dict[str, Any]]:
    """Generate detailed compliance framework status data."""
    rng = np.random.RandomState(400)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []
    statuses = ["Compliant", "Needs Review", "At Risk", "Compliant", "Compliant"]
    trends = ["Improving", "Stable", "Declining", "Stable", "Improving"]

    for fw_name, version, region in _RISK_FRAMEWORKS:
        total_controls = int(rng.randint(50, 400))
        status = str(rng.choice(statuses))
        if status == "Compliant":
            failed = int(rng.randint(0, 3))
        elif status == "Needs Review":
            failed = int(rng.randint(3, 15))
        else:
            failed = int(rng.randint(10, 40))
        not_assessed = int(rng.randint(0, 20))
        passed = total_controls - failed - not_assessed
        compliance_pct = round(passed / total_controls * 100, 1)
        last_audit = (now - timedelta(days=int(rng.randint(30, 365)))).date()
        next_audit = (last_audit + timedelta(days=int(rng.randint(90, 365))))
        auditor = _RISK_AUDITORS[rng.randint(0, len(_RISK_AUDITORS))]
        trend = str(rng.choice(trends))

        records.append({
            "framework": fw_name,
            "version": version,
            "total_controls": total_controls,
            "passed": passed,
            "failed": failed,
            "not_assessed": not_assessed,
            "compliance_pct": compliance_pct,
            "last_audit": last_audit.isoformat(),
            "next_audit": next_audit.isoformat(),
            "auditor": auditor,
            "status": status,
            "trend": trend,
        })
    return records


def _demo_access_log_details() -> List[Dict[str, Any]]:
    """Generate 5,000 detailed access log entries with anomaly scoring."""
    rng = np.random.RandomState(401)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    # Pre-generate a pool of user emails
    user_pool = []
    for u_idx in range(150):
        name = _make_name(rng)
        user_pool.append(_make_email(name))
    # Add service accounts
    user_pool.extend([
        "sys_pipeline_01@system", "ml_service_account@system",
        "etl_scheduler@system", "airflow_runner@system",
        "dbt_cloud@system", "monitoring_agent@system",
    ])

    for i in range(5000):
        ts = now - timedelta(seconds=int(rng.randint(0, 90 * 86400)))
        user = user_pool[rng.randint(0, len(user_pool))]
        role = _RISK_ROLES[rng.randint(0, len(_RISK_ROLES))]
        catalog = _RISK_CATALOGS[rng.randint(0, len(_RISK_CATALOGS))]
        schema = _RISK_SCHEMAS[rng.randint(0, len(_RISK_SCHEMAS))]
        table = _RISK_TABLES[rng.randint(0, len(_RISK_TABLES))]
        asset = f"{catalog}.{schema}.{table}"
        action = str(rng.choice(_RISK_ACTIONS, p=[0.55, 0.15, 0.05, 0.10, 0.10, 0.05]))
        sensitivity = str(rng.choice(_RISK_SENSITIVITY, p=[0.15, 0.35, 0.30, 0.20]))
        ip = f"{rng.randint(10, 220)}.{rng.randint(0, 256)}.{rng.randint(0, 256)}.{rng.randint(1, 255)}"
        geo = _RISK_GEO_LOCATIONS[rng.randint(0, len(_RISK_GEO_LOCATIONS))]

        # Anomaly scoring
        is_risky = (
            (action in ("DELETE", "EXPORT", "ALTER") and sensitivity in ("Confidential", "Restricted"))
            or (ts.hour < 6 or ts.hour > 22)
            or ("system" in user and action == "DELETE")
        )
        anomaly_score = round(float(
            rng.uniform(0.65, 0.99) if is_risky else rng.uniform(0.01, 0.40)), 3)
        risk_flag = bool(anomaly_score > 0.70)

        records.append({
            "timestamp": ts.isoformat(),
            "user_email": user,
            "role": role,
            "asset_accessed": asset,
            "action": action,
            "data_sensitivity": sensitivity,
            "ip_address": ip,
            "geo_location": geo,
            "risk_flag": risk_flag,
            "anomaly_score": anomaly_score,
        })
    return records


def _demo_pii_scan_results() -> List[Dict[str, Any]]:
    """Generate 5,000 PII detection scan results across the data catalog."""
    rng = np.random.RandomState(402)
    now = datetime.utcnow()
    records: List[Dict[str, Any]] = []

    # Pre-generate assignee pool
    assignee_pool = []
    for a_idx in range(30):
        name = _make_name(rng)
        assignee_pool.append(_make_email(name))

    for i in range(5000):
        ts = now - timedelta(seconds=int(rng.randint(0, 90 * 86400)))
        catalog = _RISK_CATALOGS[rng.randint(0, len(_RISK_CATALOGS))]
        schema = _RISK_SCHEMAS[rng.randint(0, len(_RISK_SCHEMAS))]
        table = _RISK_TABLES[rng.randint(0, len(_RISK_TABLES))]
        column = _RISK_COLUMNS[rng.randint(0, len(_RISK_COLUMNS))]
        pii_type = _RISK_PII_TYPES[rng.randint(0, len(_RISK_PII_TYPES))]
        confidence = round(float(rng.uniform(0.65, 1.0)), 3)
        record_count = int(rng.randint(1, 5_000_000))
        masked = bool(rng.random() < 0.62)
        remediation = str(rng.choice(_RISK_REMEDIATION, p=[0.25, 0.30, 0.20, 0.15, 0.10]))
        assigned_to = assignee_pool[rng.randint(0, len(assignee_pool))]

        records.append({
            "scan_id": _make_id("SCN", i, width=8),
            "timestamp": ts.isoformat(),
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "column": column,
            "pii_type": pii_type,
            "confidence": confidence,
            "record_count": record_count,
            "masked": masked,
            "remediation_status": remediation,
            "assigned_to": assigned_to,
        })
    return records


# ===================================================================
#  Public API  --  auto-switches between DEMO and LIVE mode
# ===================================================================


def get_production_kpis() -> Dict[str, Any]:
    """Latest KPI snapshot from gold.production_kpis."""
    if is_demo_mode():
        return _demo_production_kpis()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    rows = _run_query(
        f"SELECT * FROM {catalog}.gold.production_kpis "
        "ORDER BY snapshot_time DESC LIMIT 1"
    )
    return rows[0] if rows else {}


def get_anomaly_scatter_data(hours: int = 1) -> List[Dict[str, Any]]:
    """Scatter data from silver.cnc_anomalies for the last *hours*."""
    if is_demo_mode():
        return _demo_anomaly_scatter_data(hours)

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    rows = _run_query(
        f"SELECT * FROM {catalog}.silver.cnc_anomalies "
        f"WHERE detected_at >= current_timestamp() - INTERVAL {hours} HOUR "
        "ORDER BY detected_at DESC"
    )
    return rows


def get_shap_importance() -> List[Dict[str, Any]]:
    """SHAP feature importance from gold.model_health_metrics."""
    if is_demo_mode():
        return _demo_shap_importance()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    rows = _run_query(
        f"SELECT feature_importance FROM {catalog}.gold.model_health_metrics "
        "ORDER BY evaluation_time DESC LIMIT 1"
    )
    if rows and rows[0].get("feature_importance"):
        fi = rows[0]["feature_importance"]
        return [{"feature": k, "importance": v} for k, v in fi.items()]
    return []


def get_live_inference_feed(limit: int = 20) -> List[Dict[str, Any]]:
    """Latest anomaly predictions."""
    if is_demo_mode():
        return _demo_live_inference_feed(limit)

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    rows = _run_query(
        f"SELECT * FROM {catalog}.silver.cnc_anomalies "
        f"ORDER BY detected_at DESC LIMIT {limit}"
    )
    return rows


def get_inventory_status() -> List[Dict[str, Any]]:
    """Component inventory status from gold.site_component_status."""
    if is_demo_mode():
        return _demo_inventory_status()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    rows = _run_query(
        f"SELECT * FROM {catalog}.gold.site_component_status "
        "ORDER BY stock_days_remaining ASC"
    )
    return rows


def get_quality_summary() -> Dict[str, Any]:
    """Inspection stats from silver.tolerance_stats."""
    if is_demo_mode():
        return _demo_quality_summary()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    rows = _run_query(
        f"SELECT "
        f"  SUM(measurement_count) AS total_inspections, "
        f"  SUM(out_of_spec_count) AS out_of_spec_count, "
        f"  AVG(out_of_spec_rate) AS out_of_spec_rate, "
        f"  AVG(mean_um) AS mean_deviation_um, "
        f"  AVG(std_um) AS std_deviation_um, "
        f"  AVG(cp) AS cp, "
        f"  AVG(cpk) AS cpk "
        f"FROM {catalog}.silver.tolerance_stats"
    )
    return rows[0] if rows else {}


def get_build_tracking(
    batch_filter: Optional[str] = None,
    station_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build events from silver.build_tracking with optional filters."""
    if is_demo_mode():
        return _demo_build_tracking(batch_filter, station_filter)

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    sql = f"SELECT * FROM {catalog}.silver.build_tracking WHERE 1=1"
    if batch_filter:
        sql += f" AND batch_id = '{batch_filter}'"
    if station_filter:
        sql += f" AND station = '{station_filter}'"
    sql += " ORDER BY started_at DESC"
    return _run_query(sql)


# -------------------------------------------------------------------
#  Healthcare public API
# -------------------------------------------------------------------


def get_patient_wait_times() -> List[Dict[str, Any]]:
    """Patient wait-time records by department, facility, and hour."""
    if is_demo_mode():
        return _demo_patient_wait_times()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT patient_id, facility, department, arrival_time, triage_time, "
        f"seen_by_dr_time, wait_minutes, acuity_level, outcome "
        f"FROM {catalog}.gold.patient_wait_times "
        f"ORDER BY arrival_time DESC LIMIT 5000"
    )


def get_doctor_availability() -> List[Dict[str, Any]]:
    """Doctor schedules and availability across facilities."""
    if is_demo_mode():
        return _demo_doctor_availability()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT doctor_id, name, facility, department, specialty, "
        f"shift_start, shift_end, patients_seen_today, availability_status, "
        f"avg_consult_min "
        f"FROM {catalog}.gold.doctor_availability "
        f"ORDER BY facility, department"
    )


def get_billing_data() -> List[Dict[str, Any]]:
    """Billing records with procedure codes and insurance details."""
    if is_demo_mode():
        return _demo_billing_data()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT billing_id, patient_id, facility, department, procedure_code, "
        f"procedure_name, charge_amount, insurance_paid, patient_responsibility, "
        f"billing_status, submitted_date, paid_date "
        f"FROM {catalog}.gold.billing_records "
        f"ORDER BY submitted_date DESC LIMIT 5000"
    )


def get_healthcare_revenue() -> List[Dict[str, Any]]:
    """Daily/monthly revenue by facility and department."""
    if is_demo_mode():
        return _demo_healthcare_revenue()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT date, facility, department, gross_revenue, net_revenue, "
        f"collections, denials, denial_rate, avg_reimbursement "
        f"FROM {catalog}.gold.healthcare_revenue "
        f"ORDER BY date DESC"
    )


def get_bed_utilization() -> List[Dict[str, Any]]:
    """Real-time bed utilization data by facility and department."""
    if is_demo_mode():
        return _demo_bed_utilization()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT facility, department, total_beds, occupied, available, "
        f"utilization_pct, avg_los_hours, turnover_rate "
        f"FROM {catalog}.gold.bed_utilization "
        f"ORDER BY utilization_pct DESC"
    )


# -------------------------------------------------------------------
#  Gaming public API
# -------------------------------------------------------------------


def get_server_uptime() -> List[Dict[str, Any]]:
    """Server uptime and health metrics across regions and games."""
    if is_demo_mode():
        return _demo_server_uptime()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT server_id, region, game_title, status, uptime_pct, "
        f"latency_ms, cpu_pct, memory_pct, active_connections, last_restart "
        f"FROM {catalog}.gold.server_uptime "
        f"ORDER BY uptime_pct ASC LIMIT 5000"
    )


def get_player_metrics() -> List[Dict[str, Any]]:
    """Player metrics including LTV, churn risk, and engagement."""
    if is_demo_mode():
        return _demo_player_metrics()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT player_id, username, game_title, region, segment, "
        f"total_playtime_hours, sessions_7d, sessions_30d, purchases_30d, "
        f"ltv, last_login, churn_risk, acquisition_source, acquisition_date "
        f"FROM {catalog}.gold.player_metrics "
        f"ORDER BY ltv DESC LIMIT 5000"
    )


def get_session_tracking() -> List[Dict[str, Any]]:
    """Game session data with performance and crash metrics."""
    if is_demo_mode():
        return _demo_session_tracking()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT session_id, player_id, game_title, region, start_time, "
        f"end_time, duration_min, events_count, crashes, fps_avg, "
        f"device_type, os_version "
        f"FROM {catalog}.gold.session_tracking "
        f"ORDER BY start_time DESC LIMIT 5000"
    )


def get_attribution_data() -> List[Dict[str, Any]]:
    """Acquisition attribution data by source, campaign, and game."""
    if is_demo_mode():
        return _demo_attribution_data()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT source, campaign, game_title, installs, "
        f"d1_retained, d7_retained, d30_retained, "
        f"cost, revenue, roas, ltv_predicted "
        f"FROM {catalog}.gold.attribution "
        f"ORDER BY installs DESC"
    )


def get_dau_history() -> List[Dict[str, Any]]:
    """90 days of DAU/MAU history per game and region."""
    if is_demo_mode():
        return _demo_dau_history()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT date, game_title, region, dau, wau, mau, "
        f"new_users, returning_users, concurrent_peak, revenue "
        f"FROM {catalog}.gold.dau_history "
        f"ORDER BY date DESC"
    )


# -------------------------------------------------------------------
#  Financial Services public API
# -------------------------------------------------------------------


def get_customer_ltv() -> List[Dict[str, Any]]:
    """Customer LTV records with segmentation and churn probability."""
    if is_demo_mode():
        return _demo_customer_ltv()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT customer_id, name, business_line, region, account_age_years, "
        f"total_revenue, monthly_revenue, products_held, ltv, ltv_segment, "
        f"churn_probability, last_transaction "
        f"FROM {catalog}.gold.customer_ltv "
        f"ORDER BY ltv DESC LIMIT 5000"
    )


def get_daily_revenue() -> List[Dict[str, Any]]:
    """365 days of daily revenue by business line and region."""
    if is_demo_mode():
        return _demo_daily_revenue()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT date, business_line, region, gross_revenue, net_revenue, "
        f"transaction_count, avg_transaction, fees_collected, charge_offs, "
        f"net_interest_income "
        f"FROM {catalog}.gold.daily_revenue "
        f"ORDER BY date DESC"
    )


def get_revenue_change() -> List[Dict[str, Any]]:
    """MoM/YoY revenue change metrics by business line."""
    if is_demo_mode():
        return _demo_revenue_change()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT month, business_line, revenue, prev_month_revenue, "
        f"mom_change_pct, prev_year_revenue, yoy_change_pct, "
        f"forecast_next_month "
        f"FROM {catalog}.gold.revenue_change "
        f"ORDER BY month DESC"
    )


def get_fraud_transactions() -> List[Dict[str, Any]]:
    """Scored fraud transactions with investigation status."""
    if is_demo_mode():
        return _demo_fraud_transactions()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT transaction_id, timestamp, customer_id, channel, merchant, "
        f"amount, fraud_score, is_fraud, fraud_type, blocked, "
        f"investigation_status "
        f"FROM {catalog}.gold.fraud_transactions "
        f"ORDER BY timestamp DESC LIMIT 5000"
    )


def get_portfolio_performance() -> List[Dict[str, Any]]:
    """Daily portfolio performance data across asset classes."""
    if is_demo_mode():
        return _demo_portfolio_performance()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT date, asset_class, region, aum, daily_return, "
        f"cumulative_return, var_95, sharpe, benchmark_return, "
        f"alpha, tracking_error "
        f"FROM {catalog}.gold.portfolio_performance "
        f"ORDER BY date DESC"
    )


# -------------------------------------------------------------------
#  Risk public API
# -------------------------------------------------------------------


def get_compliance_frameworks() -> List[Dict[str, Any]]:
    """Detailed compliance framework status with audit tracking."""
    if is_demo_mode():
        return _demo_compliance_frameworks()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT framework, version, total_controls, passed, failed, "
        f"not_assessed, compliance_pct, last_audit, next_audit, "
        f"auditor, status, trend "
        f"FROM {catalog}.gold.compliance_frameworks "
        f"ORDER BY compliance_pct ASC"
    )


def get_access_log_details() -> List[Dict[str, Any]]:
    """Detailed access log entries with anomaly scoring."""
    if is_demo_mode():
        return _demo_access_log_details()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT timestamp, user_email, role, asset_accessed, action, "
        f"data_sensitivity, ip_address, geo_location, risk_flag, "
        f"anomaly_score "
        f"FROM {catalog}.silver.rbac_access_logs "
        f"ORDER BY timestamp DESC LIMIT 5000"
    )


def get_pii_scan_results() -> List[Dict[str, Any]]:
    """PII detection scan results across the data catalog."""
    if is_demo_mode():
        return _demo_pii_scan_results()

    cfg = get_config()
    catalog = cfg["app"]["catalog"]
    return _run_query(
        f"SELECT scan_id, timestamp, catalog, schema, table, column, "
        f"pii_type, confidence, record_count, masked, "
        f"remediation_status, assigned_to "
        f"FROM {catalog}.gold.pii_scan_results "
        f"ORDER BY timestamp DESC LIMIT 5000"
    )


# ===================================================================
#  Generic multi-vertical public helpers
# ===================================================================


def get_dashboard_kpis() -> List[Dict[str, Any]]:
    """Return the dashboard KPI tiles for **any** vertical.

    Each item contains ``title``, ``value``, ``accent``, ``alert``, and
    ``format``.  In demo mode the ``value`` comes from the config's
    ``dashboard.kpis[].value`` (display_value) when present, otherwise a
    placeholder ``"--"`` is used.  In live mode, the query from the config
    would be executed (not yet wired up for non-manufacturing verticals).
    """
    cfg = get_config()
    kpis_cfg = cfg.get("dashboard", {}).get("kpis", [])

    results: List[Dict[str, Any]] = []
    for kpi in kpis_cfg:
        if is_demo_mode():
            value = kpi.get("value", kpi.get("delta", "--"))
        else:
            # Live mode: run the query defined in the config
            query = kpi.get("query")
            if query:
                catalog = cfg["app"]["catalog"]
                full_query = query.replace("FROM gold.", f"FROM {catalog}.gold.").replace(
                    "FROM silver.", f"FROM {catalog}.silver."
                )
                rows = _run_query(full_query)
                value = list(rows[0].values())[0] if rows else "--"
            else:
                value = "--"

        results.append({
            "title": kpi.get("title", "Untitled"),
            "value": value,
            "accent": kpi.get("accent", "blue"),
            "alert": kpi.get("alert", False),
            "format": kpi.get("format", "{}"),
        })
    return results


def get_page_demo_data(page_id: str) -> Dict[str, Any]:
    """Return demo data for any page in any vertical.

    Reads from the relevant section of the YAML config based on the
    ``page_id`` and the current ``USE_CASE``.  Returns a dict with page-
    specific keys.  If no special data is available for the given page_id
    an empty dict is returned.

    Examples of page_id values per vertical:
      - risk:               compliance, pii, rbac
      - healthcare:         patient_flow, readmissions, equipment
      - gaming:             player_health, economy, matchmaking
      - financial_services: fraud, credit, portfolio
    """
    cfg = get_config()
    use_case = _current_use_case()
    data_cfg = cfg.get("data", {})
    ml_cfg = cfg.get("ml", {})
    now = datetime.utcnow()

    # ---- Risk vertical ----
    if use_case == "risk":
        if page_id == "compliance":
            return {
                "frameworks": data_cfg.get("compliance_frameworks", []),
                "domains": data_cfg.get("domains", []),
                "risk_types": data_cfg.get("risk_types", []),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "pii":
            pii = data_cfg.get("pii_monitoring", {})
            return {
                "records_scanned_24h": pii.get("records_scanned_24h", 0),
                "flagged_anomalies": pii.get("flagged_anomalies", 0),
                "pii_types": pii.get("pii_types", []),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "rbac":
            rbac = data_cfg.get("rbac_logs", {})
            return {
                "users": rbac.get("users", []),
                "risk_levels": rbac.get("risk_levels", []),
                "snapshot_time": now.isoformat(),
            }

    # ---- Healthcare vertical ----
    if use_case == "healthcare":
        if page_id == "patient_flow":
            pf = data_cfg.get("patient_flow", {})
            return {
                "avg_daily_admissions": pf.get("avg_daily_admissions", 0),
                "avg_ed_wait_minutes": pf.get("avg_ed_wait_minutes", 0),
                "bed_utilization_pct": pf.get("bed_utilization_pct", 0),
                "avg_los_days": pf.get("avg_los_days", 0),
                "facilities": data_cfg.get("facilities", []),
                "departments": data_cfg.get("departments", []),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "readmissions":
            ra = data_cfg.get("readmissions", {})
            model = ml_cfg.get("readmission_model", {})
            return {
                "total_discharges_30d": ra.get("total_discharges_30d", 0),
                "readmission_rate": ra.get("readmission_rate", 0),
                "high_risk_patients": ra.get("high_risk_patients", 0),
                "model_name": model.get("name", ""),
                "features": model.get("features", []),
                "target_auc": model.get("target_value", 0),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "equipment":
            eq = data_cfg.get("equipment_monitoring", {})
            return {
                "total_assets": eq.get("total_assets", 0),
                "maintenance_due": eq.get("maintenance_due", 0),
                "critical_alerts": eq.get("critical_alerts", 0),
                "equipment_types": data_cfg.get("equipment_types", {}),
                "model_name": ml_cfg.get("equipment_model", {}).get("name", ""),
                "features": ml_cfg.get("equipment_model", {}).get("features", []),
                "snapshot_time": now.isoformat(),
            }

    # ---- Gaming vertical ----
    if use_case == "gaming":
        if page_id == "player_health":
            ph = data_cfg.get("player_health", {})
            return {
                "d1_retention": ph.get("d1_retention", 0),
                "d7_retention": ph.get("d7_retention", 0),
                "d30_retention": ph.get("d30_retention", 0),
                "churn_risk_high": ph.get("churn_risk_high", 0),
                "ltv_avg": ph.get("ltv_avg", 0),
                "player_segments": data_cfg.get("player_segments", []),
                "game_titles": data_cfg.get("game_titles", []),
                "model_name": ml_cfg.get("churn_model", {}).get("name", ""),
                "features": ml_cfg.get("churn_model", {}).get("features", []),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "economy":
            eco = data_cfg.get("economy", {})
            return {
                "daily_revenue": eco.get("daily_revenue", 0),
                "arpdau": eco.get("arpdau", 0),
                "items_traded_24h": eco.get("items_traded_24h", 0),
                "inflation_index": eco.get("inflation_index", 0),
                "suspicious_transactions": eco.get("suspicious_transactions", 0),
                "fraud_model": ml_cfg.get("fraud_model", {}).get("name", ""),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "matchmaking":
            mm = data_cfg.get("matchmaking", {})
            return {
                "avg_queue_time_sec": mm.get("avg_queue_time_sec", 0),
                "skill_rating_spread": mm.get("skill_rating_spread", 0),
                "matches_24h": mm.get("matches_24h", 0),
                "reported_unfair": mm.get("reported_unfair", 0),
                "regions": data_cfg.get("regions", []),
                "snapshot_time": now.isoformat(),
            }

    # ---- Financial Services vertical ----
    if use_case == "financial_services":
        if page_id == "fraud":
            fd = data_cfg.get("fraud_detection", {})
            model = ml_cfg.get("fraud_model", {})
            return {
                "transactions_per_day": fd.get("transactions_per_day", 0),
                "fraud_rate": fd.get("fraud_rate", 0),
                "blocked_today": fd.get("blocked_today", 0),
                "false_positive_rate": fd.get("false_positive_rate", 0),
                "avg_fraud_amount": fd.get("avg_fraud_amount", 0),
                "transaction_channels": data_cfg.get("transaction_channels", []),
                "model_name": model.get("name", ""),
                "features": model.get("features", []),
                "target_auc": model.get("target_value", 0),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "credit":
            cr = data_cfg.get("credit_risk", {})
            model = ml_cfg.get("credit_model", {})
            return {
                "total_portfolio_value": cr.get("total_portfolio_value", 0),
                "avg_credit_score": cr.get("avg_credit_score", 0),
                "delinquency_rate_30d": cr.get("delinquency_rate_30d", 0),
                "high_risk_accounts": cr.get("high_risk_accounts", 0),
                "expected_loss_rate": cr.get("expected_loss_rate", 0),
                "model_name": model.get("name", ""),
                "features": model.get("features", []),
                "target_auc": model.get("target_value", 0),
                "snapshot_time": now.isoformat(),
            }
        if page_id == "portfolio":
            pf = data_cfg.get("portfolio", {})
            return {
                "aum_total": pf.get("aum_total", 0),
                "var_95_daily": pf.get("var_95_daily", 0),
                "sharpe_ratio": pf.get("sharpe_ratio", 0),
                "beta_portfolio": pf.get("beta_portfolio", 0),
                "active_positions": pf.get("active_positions", 0),
                "business_lines": data_cfg.get("business_lines", []),
                "snapshot_time": now.isoformat(),
            }

    # ---- Manufacturing vertical (legacy pages handled elsewhere) ----
    if use_case == "manufacturing":
        if page_id == "inventory":
            return {"items": _demo_inventory_status()}
        if page_id == "quality":
            return _demo_quality_summary()
        if page_id == "tracking":
            return {"events": _demo_build_tracking()}

    # Fallback: return the raw data section from config for the matching page
    # This covers any future pages or custom configs.
    return {page_id: data_cfg.get(page_id, {}), "snapshot_time": now.isoformat()}
