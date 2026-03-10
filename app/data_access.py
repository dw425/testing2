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
    """Generate ~60 scatter points for the anomaly chart.

    For manufacturing, the original fields (vibration_hz, temp_c, etc.) are
    used.  For other verticals, the scatter data uses the first ML model's
    features from the config as dimension labels, with generic numeric ranges.
    """
    use_case = _current_use_case()
    cfg = get_config()
    now = datetime.utcnow()
    points = []

    if use_case == "manufacturing":
        sites = _DEMO_SITES
        machines = _DEMO_MACHINES
    else:
        # Derive site/entity lists from config data section
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
        # Build a machines-like mapping from the config if available
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

    for i in range(60):
        site = random.choice(list(sites) if not isinstance(sites, list) else sites)
        site_machines = machines.get(site, [f"{site}-unit-1"])
        machine = random.choice(site_machines)
        is_anomaly = random.random() < 0.15
        point: Dict[str, Any] = {
            "timestamp": (now - timedelta(minutes=random.randint(0, hours * 60))).isoformat(),
            "site": site,
            "machine_id": machine,
            "anomaly_score": round(random.uniform(0.75, 0.99), 3) if is_anomaly else round(random.uniform(0.01, 0.40), 3),
            "is_anomaly": is_anomaly,
        }
        # Add feature columns with plausible ranges
        for feat in features[:5]:
            if is_anomaly:
                point[feat] = round(random.uniform(0.65, 0.99), 3)
            else:
                point[feat] = round(random.uniform(0.01, 0.45), 3)
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

    # Generate descending importance values that sum to ~1.0
    n = len(features)
    weights = [round(1.0 / (2 ** (i + 1)), 3) for i in range(n)]
    total = sum(weights)
    return [
        {"feature": f, "importance": round(w / total, 3)}
        for f, w in zip(features, weights)
    ]


def _demo_live_inference_feed(limit: int = 20) -> List[Dict[str, Any]]:
    """Return recent inference predictions.

    Uses machine types / entity names from the config so the feed is
    contextually appropriate for any vertical.
    """
    cfg = get_config()
    use_case = _current_use_case()
    now = datetime.utcnow()

    # Derive sites and machine/entity mappings
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

    feed = []
    for i in range(limit):
        site = random.choice(list(sites) if not isinstance(sites, list) else sites)
        site_machines = machines.get(site, [f"{site}-unit-1"])
        machine = random.choice(site_machines)
        is_anomaly = random.random() < 0.12
        entry: Dict[str, Any] = {
            "timestamp": (now - timedelta(seconds=i * 3)).isoformat(),
            "site": site,
            "machine_id": machine,
            "anomaly_score": round(random.uniform(0.78, 0.98), 3) if is_anomaly else round(random.uniform(0.02, 0.35), 3),
            "prediction": "Anomaly" if is_anomaly else "Normal",
            "latency_ms": round(random.uniform(30, 55), 1),
        }
        # Add vertical-specific telemetry columns
        if use_case == "manufacturing":
            entry["vibration_hz"] = round(random.uniform(28, 45) if is_anomaly else random.uniform(12, 24), 1)
            entry["temp_c"] = round(random.uniform(68, 88) if is_anomaly else random.uniform(42, 58), 1)
        else:
            ml_cfg = cfg.get("ml", {})
            first_model = next(iter(ml_cfg.values()), {}) if ml_cfg else {}
            features = first_model.get("features", ["metric_a", "metric_b"])
            for feat in features[:3]:
                entry[feat] = round(random.uniform(0.6, 0.95) if is_anomaly else random.uniform(0.05, 0.4), 3)
        feed.append(entry)
    return feed


def _demo_inventory_status() -> List[Dict[str, Any]]:
    """Return inventory / component status.

    If the config contains ``data.inventory.components`` those are used
    directly.  Otherwise a set of generic placeholder items is returned.
    """
    cfg = get_config()
    inv_components = cfg.get("data", {}).get("inventory", {}).get("components")
    if inv_components:
        return [
            {
                "component": c.get("name", c.get("component", "Unknown")),
                "site": c.get("site", "N/A"),
                "stock_days": c.get("stock_days", 0),
                "status": c.get("status", "Unknown"),
                "current_stock": c.get("current_stock", 0),
                "daily_usage": c.get("daily_usage", 0),
            }
            for c in inv_components
        ]

    # Fallback: generate generic items appropriate for the vertical
    data_cfg = cfg.get("data", {})
    sites = (
        data_cfg.get("sites")
        or data_cfg.get("facilities")
        or data_cfg.get("game_titles")
        or data_cfg.get("regions")
        or data_cfg.get("business_lines")
        or data_cfg.get("domains")
        or ["Location-A", "Location-B"]
    )
    statuses = ["Healthy", "Healthy", "Low", "Critical"]
    items = []
    for idx, site in enumerate(sites[:3]):
        status = statuses[idx % len(statuses)]
        stock_days = {
            "Healthy": random.randint(10, 25),
            "Low": random.randint(2, 4),
            "Critical": round(random.uniform(0.2, 1.0), 1),
        }[status]
        daily = random.randint(200, 900)
        items.append({
            "component": f"Resource-{idx + 1}",
            "site": site,
            "stock_days": stock_days,
            "status": status,
            "current_stock": int(stock_days * daily),
            "daily_usage": daily,
        })
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
    stations = [
        "CNC Milling",
        "Lathe Processing",
        "Assembly Line A",
        "Assembly Line B",
        "Quality Check",
        "Packaging",
    ]
    batches = [
        {"batch_id": "B-9982-XYZ", "status": "Complete", "site": "Berlin", "defect_flag": None},
        {"batch_id": "A-1102-MDF", "status": "Defect", "site": "Detroit", "defect_flag": "Vibration anomaly detected"},
        {"batch_id": "C-4421-ALP", "status": "In Progress", "site": "Tokyo", "defect_flag": None},
        {"batch_id": "D-5510-BRV", "status": "Complete", "site": "Berlin", "defect_flag": None},
        {"batch_id": "E-7783-QRS", "status": "Complete", "site": "Detroit", "defect_flag": None},
    ]

    now = datetime.utcnow()
    events = []
    for b in batches:
        for seq, station in enumerate(stations, start=1):
            is_defect = b["status"] == "Defect" and station == "CNC Milling"
            units_in = 500
            defect_count = 12 if is_defect else random.randint(0, 3)
            units_out = units_in - defect_count
            events.append(
                {
                    "batch_id": b["batch_id"],
                    "site": b["site"],
                    "station": station,
                    "stage_seq": seq,
                    "units_in": units_in,
                    "units_out": units_out,
                    "defect_count": defect_count,
                    "yield_pct": round(units_out / units_in * 100, 2),
                    "cycle_time_sec": round(random.uniform(45, 180), 1),
                    "status": "Defect" if is_defect else ("Complete" if b["status"] == "Complete" else "In Progress"),
                    "defect_flag": b["defect_flag"] if is_defect else None,
                    "started_at": (now - timedelta(hours=random.randint(1, 48))).isoformat(),
                    "completed_at": (now - timedelta(hours=random.randint(0, 1))).isoformat() if b["status"] != "In Progress" else None,
                }
            )

    if batch_filter:
        events = [e for e in events if e["batch_id"] == batch_filter]
    if station_filter:
        events = [e for e in events if e["station"] == station_filter]

    return events


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
