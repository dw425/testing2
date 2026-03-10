"""Implementation Details page for ManufacturingIQ Databricks app.

Static reference page showing key implementation patterns with syntax-
highlighted code examples (dark theme). Covers data access, model training,
config-driven architecture, and deployment.
"""

from dash import html

# ---------------------------------------------------------------------------
# Theme constants
# ---------------------------------------------------------------------------
try:
    from app.theme import (
        BG_PRIMARY, BG_CARD, BORDER, TEXT_PRIMARY, TEXT_MUTED,
        ACCENT_BLUE, ACCENT_GREEN, ACCENT_YELLOW, ACCENT_RED,
    )
except ImportError:
    BG_PRIMARY = "#0D0F12"
    BG_CARD = "#16181D"
    BORDER = "#272A31"
    TEXT_PRIMARY = "#EAEBF0"
    TEXT_MUTED = "#8A8D98"
    ACCENT_BLUE = "#4C9AFF"
    ACCENT_GREEN = "#36B37E"
    ACCENT_YELLOW = "#FFAB00"
    ACCENT_RED = "#FF5630"

# ===================================================================
# Code block styling
# ===================================================================
_CODE_BLOCK_STYLE = {
    "backgroundColor": "#0E1117",
    "color": "#C9D1D9",
    "padding": "20px",
    "borderRadius": "4px",
    "fontSize": "12.5px",
    "lineHeight": "1.65",
    "fontFamily": "JetBrains Mono, Fira Code, Menlo, monospace",
    "overflowX": "auto",
    "border": f"1px solid {BORDER}",
    "margin": "0",
    "whiteSpace": "pre",
    "tabSize": "4",
}

_SECTION_STYLE = {
    "backgroundColor": BG_CARD,
    "border": f"1px solid {BORDER}",
    "borderRadius": "6px",
    "padding": "24px",
    "marginBottom": "20px",
}

_HEADING_STYLE = {
    "fontSize": "15px",
    "fontWeight": "600",
    "color": TEXT_PRIMARY,
    "marginBottom": "8px",
}

_DESC_STYLE = {
    "fontSize": "13px",
    "color": TEXT_MUTED,
    "marginBottom": "16px",
    "lineHeight": "1.6",
}

_TAG_STYLE = {
    "display": "inline-block",
    "backgroundColor": "rgba(76, 154, 255, 0.12)",
    "color": ACCENT_BLUE,
    "fontSize": "11px",
    "fontWeight": "600",
    "padding": "2px 8px",
    "borderRadius": "3px",
    "marginRight": "8px",
    "marginBottom": "8px",
    "letterSpacing": "0.3px",
}


# ===================================================================
# Code snippets
# ===================================================================
_DATA_ACCESS_CODE = '''\
# app/data_access.py  --  Config-driven data access layer
import os, yaml
from functools import lru_cache

_USE_DEMO = os.getenv("USE_DEMO_DATA", "true").lower() == "true"

@lru_cache(maxsize=1)
def _load_config():
    with open("config/app_config.yaml") as f:
        return yaml.safe_load(f)

def _read_table(table_key: str):
    """Read a Gold-layer Delta table, falling back to demo data."""
    if _USE_DEMO:
        from app.demo_data import DEMO_TABLES
        return DEMO_TABLES[table_key]

    cfg = _load_config()
    catalog = cfg["unity_catalog"]["catalog"]
    schema  = cfg["unity_catalog"]["schema"]
    table   = cfg["tables"][table_key]

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    return w.sql.execute(
        f"SELECT * FROM {catalog}.{schema}.{table} "
        f"ORDER BY timestamp DESC LIMIT 1000"
    )

def get_anomaly_kpis():
    row = _read_table("anomaly_kpis")
    return {
        "f1_score": row["f1_score"],
        "inference_latency_ms": row["latency_p95_ms"],
        "data_drift_pct": row["drift_psi"],
        "anomalies_1h": row["anomaly_count_1h"],
    }
'''

_MODEL_TRAINING_CODE = '''\
# ml/train_anomaly_model.py  --  Isolation Forest training pipeline
import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, classification_report
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

def train(config: dict):
    mlflow.set_experiment(config["experiment_name"])

    # 1. Load features from Databricks Feature Store
    training_set = fs.read_table(
        name=f"{config['catalog']}.{config['schema']}.sensor_features",
        feature_names=config["feature_columns"],
    )
    df = training_set.load_df().toPandas()
    X = df[config["feature_columns"]]
    y = df["label"]

    # 2. Train Isolation Forest
    model = IsolationForest(
        n_estimators=config.get("n_estimators", 200),
        contamination=config.get("contamination", 0.05),
        random_state=42,
        n_jobs=-1,
    )

    with mlflow.start_run(run_name="isolation-forest-v3"):
        model.fit(X)
        preds = model.predict(X)
        preds_binary = [1 if p == -1 else 0 for p in preds]

        f1 = f1_score(y, preds_binary)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_params({
            "n_estimators": model.n_estimators,
            "contamination": model.contamination,
        })
        mlflow.sklearn.log_model(
            model, artifact_path="model",
            registered_model_name="manufacturing_anomaly_detector",
        )
        print(classification_report(y, preds_binary))
    return f1
'''

_CONFIG_YAML_CODE = '''\
# config/app_config.yaml  --  Environment-specific configuration
app:
  title: "ManufacturingIQ"
  refresh_interval_sec: 5
  page_size: 15

unity_catalog:
  catalog: manufacturing_prod
  schema: gold

tables:
  anomaly_kpis: anomaly_kpi_latest
  sensor_readings: sensor_telemetry_5m
  inventory: component_inventory_current
  build_tracking: build_events_stream
  quality_deviations: tolerance_deviation_log

model_serving:
  endpoint_name: anomaly-detector-endpoint
  timeout_ms: 200

alerts:
  slack_webhook_env: SLACK_WEBHOOK_URL
  pagerduty_key_env: PD_ROUTING_KEY
  anomaly_threshold: 100        # per hour
  drift_threshold_psi: 0.15
'''

_DEPLOYMENT_CODE = '''\
# Databricks App entry point  --  app.yaml
command:
  - "gunicorn"
  - "main:server"
  - "--bind=0.0.0.0:8050"
  - "--workers=2"
  - "--timeout=120"

env:
  - name: USE_DEMO_DATA
    value: "false"
  - name: DASH_DEBUG
    value: "false"

# main.py  --  Dash application factory
import dash
from dash import html, dcc

from app.pages import dashboard, inventory, quality, tracking
from app.pages import architecture, details

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title="ManufacturingIQ",
)
server = app.server  # exposed for gunicorn

# Register page callbacks
for page_module in [dashboard, inventory, quality, tracking]:
    if hasattr(page_module, "register_callbacks"):
        page_module.register_callbacks(app)

# Sidebar + page routing (simplified)
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    html.Div(id="page-content"),
])
'''

_CALLBACK_PATTERN_CODE = '''\
# Pattern: resilient callback with demo-data fallback
from dash import Input, Output, callback
import plotly.graph_objects as go

def register_callbacks(app):

    @app.callback(
        Output("dashboard-scatter", "figure"),
        Input("dashboard-interval", "n_intervals"),
    )
    def refresh_scatter(_n):
        try:
            data = get_vibration_temperature_data()  # live
        except Exception:
            data = _demo_vibration_data()             # fallback

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data["normal"]["temperature"],
            y=data["normal"]["vibration"],
            mode="markers",
            marker=dict(color="#4C9AFF", size=6, opacity=0.7),
            name="Normal",
        ))
        fig.add_trace(go.Scatter(
            x=data["anomaly"]["temperature"],
            y=data["anomaly"]["vibration"],
            mode="markers",
            marker=dict(color="#FF5630", size=9, symbol="triangle-up"),
            name="Anomaly",
        ))
        return fig
'''


# ===================================================================
# Section builder
# ===================================================================
def _section(title, description, code, tags=None):
    tag_elements = []
    if tags:
        tag_elements = [html.Span(t, style=_TAG_STYLE) for t in tags]

    return html.Div(
        children=[
            html.Div(
                children=tag_elements,
                style={"marginBottom": "8px"} if tag_elements else {"display": "none"},
            ),
            html.Div(title, style=_HEADING_STYLE),
            html.Div(description, style=_DESC_STYLE),
            html.Pre(
                html.Code(code),
                style=_CODE_BLOCK_STYLE,
            ),
        ],
        style=_SECTION_STYLE,
    )


# ===================================================================
# Layout
# ===================================================================
def layout():
    """Return the Implementation Details page layout."""

    intro = html.Div(
        children=[
            html.Div(
                "This page documents the key implementation patterns used in "
                "ManufacturingIQ. Each section contains a representative code "
                "snippet that illustrates how the layer works. All patterns "
                "follow a config-driven, demo-safe design so the app renders "
                "with or without a Databricks workspace connection.",
                style=_DESC_STYLE,
            ),
        ],
        style={**_SECTION_STYLE, "borderLeft": f"4px solid {ACCENT_BLUE}"},
    )

    return html.Div(
        children=[
            html.H2("Implementation Details", style={
                "color": TEXT_PRIMARY, "fontWeight": "700", "fontSize": "22px",
                "marginBottom": "20px",
            }),
            intro,
            _section(
                "Data Access Layer",
                "Centralised data access module that reads from Unity Catalog Gold "
                "tables when running on Databricks, and seamlessly falls back to "
                "hardcoded demo data when USE_DEMO_DATA is set. Uses lru_cache to "
                "avoid re-parsing config on every request.",
                _DATA_ACCESS_CODE,
                tags=["Python", "Unity Catalog", "Delta Lake"],
            ),
            _section(
                "Anomaly Detection Model Training",
                "MLflow-tracked training pipeline using scikit-learn Isolation Forest. "
                "Features are read from Databricks Feature Store; metrics and the "
                "serialised model are logged to the MLflow experiment and registered "
                "for champion/challenger promotion.",
                _MODEL_TRAINING_CODE,
                tags=["MLflow", "Scikit-learn", "Feature Store"],
            ),
            _section(
                "YAML Configuration",
                "Single-file, environment-specific configuration drives table names, "
                "model serving endpoints, alert thresholds, and UI parameters. Each "
                "deployment environment (dev / staging / prod) has its own config file.",
                _CONFIG_YAML_CODE,
                tags=["YAML", "Config-Driven"],
            ),
            _section(
                "Deployment & App Factory",
                "The Dash app is deployed as a Databricks App via a declarative "
                "app.yaml. Gunicorn serves the WSGI application. Page modules are "
                "imported and their callbacks registered at startup.",
                _DEPLOYMENT_CODE,
                tags=["Databricks Apps", "Gunicorn", "Dash"],
            ),
            _section(
                "Resilient Callback Pattern",
                "Every callback that fetches data wraps the call in try/except and "
                "falls back to demo data on failure. This ensures the dashboard always "
                "renders, even during Databricks outages or local development.",
                _CALLBACK_PATTERN_CODE,
                tags=["Dash Callbacks", "Error Handling"],
            ),
        ],
        style={"padding": "24px", "backgroundColor": BG_PRIMARY, "minHeight": "100vh"},
    )
