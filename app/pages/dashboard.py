"""Production Dashboard page for ManufacturingIQ Databricks app.

Displays real-time anomaly detection KPIs, vibration-temperature scatter plot,
SHAP feature importance, and a live inference feed table. Auto-refreshes via
dcc.Interval.
"""

import datetime
import random

from dash import html, dcc, dash_table, Input, Output, callback
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Theme constants (fallback if app.theme is unavailable)
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

# ---------------------------------------------------------------------------
# KPI card helper
# ---------------------------------------------------------------------------
try:
    from app.components.kpi_cards import make_kpi_card
except ImportError:
    def make_kpi_card(title, value, subtitle, accent_color, alert=False):
        return html.Div(
            children=[
                html.Div(title, style={"fontSize": "13px", "color": TEXT_MUTED,
                                       "textTransform": "uppercase", "letterSpacing": "0.5px",
                                       "marginBottom": "6px"}),
                html.Div(value, style={"fontSize": "32px", "fontWeight": "700",
                                       "color": TEXT_PRIMARY, "lineHeight": "1.2",
                                       "marginBottom": "4px"}),
                html.Div(subtitle, style={"fontSize": "12px", "color": "#6C6F7A"}),
            ],
            style={
                "backgroundColor": BG_CARD, "border": f"1px solid {BORDER}",
                "borderLeft": f"4px solid {accent_color}",
                "borderRadius": "6px", "padding": "20px", "flex": "1",
                "minWidth": "200px", "boxShadow": "0 2px 8px rgba(0,0,0,0.25)",
            },
        )

# ---------------------------------------------------------------------------
# Data access – demo data fallback
# ---------------------------------------------------------------------------
try:
    from app.data_access import (
        get_anomaly_kpis,
        get_vibration_temperature_data,
        get_shap_feature_importance,
        get_live_inference_feed,
    )
except ImportError:
    def get_anomaly_kpis():
        return {
            "f1_score": 0.947,
            "inference_latency_ms": 42,
            "data_drift_pct": 1.2,
            "anomalies_1h": 87,
        }

    def get_vibration_temperature_data():
        random.seed(42)
        normal_x = [random.gauss(35, 8) for _ in range(200)]
        normal_y = [random.gauss(60, 15) for _ in range(200)]
        anom_x = [random.gauss(70, 6) for _ in range(30)]
        anom_y = [random.gauss(110, 10) for _ in range(30)]
        return {
            "normal": {"temperature": normal_x, "vibration": normal_y},
            "anomaly": {"temperature": anom_x, "vibration": anom_y},
        }

    def get_shap_feature_importance():
        return [
            ("vibration_hz", 0.34),
            ("temperature_c", 0.28),
            ("pressure_psi", 0.14),
            ("rpm", 0.10),
            ("humidity_pct", 0.07),
            ("current_amps", 0.04),
            ("voltage_v", 0.03),
        ]

    def get_live_inference_feed():
        random.seed(None)
        drivers = ["vibration_hz", "temperature_c", "pressure_psi", "rpm",
                    "humidity_pct", "current_amps"]
        rows = []
        base = datetime.datetime.now()
        for i in range(15):
            ts = (base - datetime.timedelta(seconds=i * 4)).strftime("%H:%M:%S")
            anomalous = random.random() < 0.3
            rows.append({
                "timestamp": ts,
                "asset_id": f"CNC-{random.randint(100, 999)}",
                "anomalous": "YES" if anomalous else "no",
                "confidence": round(random.uniform(0.70, 0.99), 3),
                "shap_driver": random.choice(drivers),
            })
        return rows


# ===================================================================
# Plot theme helper
# ===================================================================
_PLOT_LAYOUT = dict(
    paper_bgcolor=BG_CARD,
    plot_bgcolor="#0E1117",
    font=dict(color=TEXT_PRIMARY, family="Inter, system-ui, sans-serif", size=12),
    margin=dict(l=50, r=20, t=40, b=40),
    xaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
    yaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
)


# ===================================================================
# Build static charts
# ===================================================================
def _build_scatter():
    data = get_vibration_temperature_data()
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=data["normal"]["temperature"],
        y=data["normal"]["vibration"],
        mode="markers",
        marker=dict(color=ACCENT_BLUE, size=6, opacity=0.7),
        name="Normal",
    ))
    fig.add_trace(go.Scatter(
        x=data["anomaly"]["temperature"],
        y=data["anomaly"]["vibration"],
        mode="markers",
        marker=dict(color=ACCENT_RED, size=9, symbol="triangle-up", opacity=0.9),
        name="Anomaly",
    ))
    fig.update_layout(
        **_PLOT_LAYOUT,
        title=dict(text="Vibration Hz vs Temperature", font=dict(size=15)),
        xaxis_title="Temperature (\u00b0C)",
        yaxis_title="Vibration (Hz)",
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    )
    return fig


def _build_shap_bar():
    features = get_shap_feature_importance()
    names = [f[0] for f in reversed(features)]
    values = [f[1] for f in reversed(features)]
    colors = [ACCENT_BLUE if v < 0.25 else ACCENT_GREEN if v < 0.30 else ACCENT_RED
              for v in values]
    fig = go.Figure(go.Bar(
        x=values, y=names, orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{v:.2f}" for v in values],
        textposition="outside",
        textfont=dict(color=TEXT_PRIMARY, size=11),
    ))
    fig.update_layout(
        **_PLOT_LAYOUT,
        title=dict(text="SHAP Feature Importance", font=dict(size=15)),
        xaxis_title="Mean |SHAP value|",
        height=320,
    )
    return fig


# ===================================================================
# Layout
# ===================================================================
def layout():
    """Return the Production Dashboard page layout."""
    kpis = get_anomaly_kpis()

    kpi_row = html.Div(
        children=[
            make_kpi_card(
                "Model F1-Score",
                f"{kpis['f1_score']:.3f}",
                "Isolation Forest v3.2",
                ACCENT_GREEN,
            ),
            make_kpi_card(
                "Inference Latency",
                f"{kpis['inference_latency_ms']} ms",
                "p95 \u2013 Model Serving",
                ACCENT_BLUE,
            ),
            make_kpi_card(
                "Data Drift",
                f"{kpis['data_drift_pct']:.1f}%",
                "PSI vs training baseline",
                ACCENT_YELLOW,
            ),
            make_kpi_card(
                "Anomalies Detected 1h",
                str(kpis["anomalies_1h"]),
                "across 12 monitored assets",
                ACCENT_RED,
                alert=True,
            ),
        ],
        style={
            "display": "flex",
            "gap": "16px",
            "flexWrap": "wrap",
            "marginBottom": "24px",
        },
    )

    scatter_chart = html.Div(
        dcc.Graph(id="dashboard-scatter", figure=_build_scatter(),
                  config={"displayModeBar": False}),
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "12px",
            "flex": "1",
            "minWidth": "420px",
        },
    )

    shap_chart = html.Div(
        dcc.Graph(id="dashboard-shap", figure=_build_shap_bar(),
                  config={"displayModeBar": False}),
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "12px",
            "flex": "1",
            "minWidth": "380px",
        },
    )

    charts_row = html.Div(
        children=[scatter_chart, shap_chart],
        style={"display": "flex", "gap": "16px", "flexWrap": "wrap",
               "marginBottom": "24px"},
    )

    # Live inference feed table
    feed_data = get_live_inference_feed()
    feed_table = html.Div(
        children=[
            html.Div("Live Inference Feed", style={
                "fontSize": "15px", "fontWeight": "600", "color": TEXT_PRIMARY,
                "marginBottom": "12px",
            }),
            dash_table.DataTable(
                id="dashboard-inference-table",
                columns=[
                    {"name": "Timestamp", "id": "timestamp"},
                    {"name": "Asset ID", "id": "asset_id"},
                    {"name": "Anomalous", "id": "anomalous"},
                    {"name": "Confidence", "id": "confidence", "type": "numeric",
                     "format": dash_table.FormatTemplate.percentage(1)},
                    {"name": "SHAP Driver", "id": "shap_driver"},
                ],
                data=feed_data,
                page_size=10,
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#1A1D23",
                    "color": TEXT_MUTED,
                    "fontWeight": "600",
                    "fontSize": "12px",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.5px",
                    "border": f"1px solid {BORDER}",
                    "padding": "10px 14px",
                },
                style_cell={
                    "backgroundColor": BG_CARD,
                    "color": TEXT_PRIMARY,
                    "border": f"1px solid {BORDER}",
                    "fontSize": "13px",
                    "padding": "10px 14px",
                    "fontFamily": "JetBrains Mono, monospace",
                },
                style_data_conditional=[
                    {
                        "if": {"filter_query": '{anomalous} = "YES"'},
                        "color": ACCENT_RED,
                        "fontWeight": "700",
                    },
                ],
            ),
        ],
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "20px",
        },
    )

    # Interval for auto-refresh
    interval = dcc.Interval(
        id="dashboard-interval",
        interval=5000,  # 5 seconds
        n_intervals=0,
    )

    return html.Div(
        children=[
            interval,
            html.H2("Production Dashboard", style={
                "color": TEXT_PRIMARY, "fontWeight": "700", "fontSize": "22px",
                "marginBottom": "20px",
            }),
            kpi_row,
            charts_row,
            feed_table,
        ],
        style={"padding": "24px", "backgroundColor": BG_PRIMARY, "minHeight": "100vh"},
    )


# ===================================================================
# Callbacks
# ===================================================================
def register_callbacks(app):
    """Register Dash callbacks for auto-refresh behaviour."""

    @app.callback(
        Output("dashboard-inference-table", "data"),
        Input("dashboard-interval", "n_intervals"),
    )
    def refresh_inference_feed(_n):
        try:
            return get_live_inference_feed()
        except Exception:
            return []

    @app.callback(
        Output("dashboard-scatter", "figure"),
        Input("dashboard-interval", "n_intervals"),
    )
    def refresh_scatter(_n):
        try:
            return _build_scatter()
        except Exception:
            return go.Figure()
