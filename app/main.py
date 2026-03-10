"""
Multi-vertical Dash application entry point.

Creates the Dash server, loads the layout, registers page callbacks,
and wires up URL routing plus the Genie panel toggle.
Supports all verticals: manufacturing, risk, healthcare, gaming, financial_services.
"""

import os

# ---------------------------------------------------------------------------
# Default environment  --  demo mode so the app renders without Databricks
# ---------------------------------------------------------------------------
os.environ.setdefault("USE_DEMO_DATA", "true")
os.environ.setdefault("USE_CASE", "manufacturing")

import dash  # noqa: E402
from dash import Input, Output, State, callback_context, dcc, html, ALL  # noqa: E402

from app.data_access import (  # noqa: E402
    get_anomaly_scatter_data,
    get_build_tracking,
    get_config,
    get_config_for,
    get_inventory_status,
    get_live_inference_feed,
    get_production_kpis,
    get_quality_summary,
    get_shap_importance,
    set_active_vertical,
)
from app.genie_backend import ask_genie  # noqa: E402
from app.layout import build_layout, build_sidebar, build_genie_panel  # noqa: E402
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS, get_base_stylesheet  # noqa: E402

# ---------------------------------------------------------------------------
# All supported verticals
# ---------------------------------------------------------------------------
ALL_VERTICALS = ["manufacturing", "risk", "healthcare", "gaming", "financial_services"]

_VERTICAL_META = {
    "manufacturing": {"title": "ManufacturingIQ", "subtitle": "Predictive Quality & Anomaly Detection", "icon": "fa-industry", "color": "#233ED8", "stats": [("Assets", "12"), ("Models", "3"), ("Sites", "3")]},
    "risk": {"title": "RiskIQ", "subtitle": "Data Governance & Compliance", "icon": "fa-shield-halved", "color": "#8B5CF6", "stats": [("Domains", "4"), ("Policies", "24"), ("Scans", "1.2K")]},
    "healthcare": {"title": "HealthcareIQ", "subtitle": "Clinical Operations Analytics", "icon": "fa-heart-pulse", "color": "#22C55E", "stats": [("Facilities", "3"), ("Departments", "6"), ("Patients", "5K")]},
    "gaming": {"title": "GamingIQ", "subtitle": "Player Analytics & Live Ops", "icon": "fa-gamepad", "color": "#EAB308", "stats": [("Titles", "3"), ("DAU", "847K"), ("Regions", "6")]},
    "financial_services": {"title": "FinServIQ", "subtitle": "Risk, Fraud & Portfolio Analytics", "icon": "fa-building-columns", "color": "#EF4444", "stats": [("Lines", "5"), ("Accounts", "12.4K"), ("AUM", "$47M")]},
}

# ---------------------------------------------------------------------------
# Create Dash app
# ---------------------------------------------------------------------------

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title="Blueprint IQ",
    update_title=None,
    external_stylesheets=[
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css",
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    ],
)

server = app.server

# Inject custom CSS
app.index_string = """<!DOCTYPE html>
<html>
<head>
{%metas%}
<title>{%title%}</title>
{%favicon%}
{%css%}
<style>""" + get_base_stylesheet() + """</style>
</head>
<body>
{%app_entry%}
<footer>
{%config%}
{%scripts%}
{%renderer%}
</footer>
</body>
</html>"""

# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

app.layout = build_layout()


# ===================================================================
#  Shared helpers
# ===================================================================

# Icon mapping for KPI cards by accent color
_ACCENT_ICONS = {
    "blue": "fa-chart-line",
    "purple": "fa-bolt",
    "green": "fa-arrow-trend-up",
    "red": "fa-triangle-exclamation",
    "yellow": "fa-circle-exclamation",
}

# Demo KPI values keyed by vertical
_DEMO_KPI_VALUES = {
    "manufacturing": [0.947, 42, 1.2, 87],
    "risk": [2.4, 84, 3],
    "healthcare": [87.3, 34, 8.2, 3],
    "gaming": ["2.4M", "847K", "41%", "34.2K"],
    "financial_services": ["12.5M", "847", "$47M", "12.4K"],
}


def _format_demo_value(raw_value, fmt_str):
    """Format a demo value using the config format string.

    If the raw value is already a string (pre-formatted), return it directly.
    Otherwise apply the format string.
    """
    if isinstance(raw_value, str):
        return raw_value
    try:
        return fmt_str.format(raw_value)
    except (ValueError, TypeError):
        return str(raw_value)


def _build_kpi_card(title, value_str, accent, icon, alert=False):
    """Build a single KPI card component."""
    accent_class = f"accent-{accent}"
    children = [
        html.Div(
            style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"},
            children=[
                html.Span(title, className="card-title"),
                html.I(className=f"fa-solid {icon}", style={"color": COLORS["text_muted"], "fontSize": "14px"}),
            ],
        ),
        html.Div(value_str, className=f"card-value {accent_class}"),
        html.Div("Live", className="card-subtitle"),
    ]
    if alert:
        children.insert(1, html.Div(
            style={"position": "absolute", "top": "12px", "right": "12px"},
            children=html.Span(
                "ALERT",
                style={
                    "fontSize": "9px", "fontWeight": "700", "color": COLORS["red"],
                    "backgroundColor": "rgba(239, 68, 68, 0.12)", "padding": "2px 6px",
                    "borderRadius": "4px", "letterSpacing": "0.5px",
                },
            ),
        ))
    return html.Div(className="card", style={"position": "relative"}, children=children)


def _build_table(headers, rows):
    """Build a styled HTML table from headers and row data."""
    th_style = {
        "padding": "10px 14px", "fontSize": "11px", "color": COLORS["text_muted"],
        "textAlign": "left", "borderBottom": f"1px solid {COLORS['border']}",
        "textTransform": "uppercase", "letterSpacing": "0.5px",
    }
    return html.Table(
        style={"width": "100%", "borderCollapse": "collapse"},
        children=[
            html.Thead(html.Tr([html.Th(h, style=th_style) for h in headers])),
            html.Tbody(rows),
        ],
    )


def _td(text, bold=False, mono=False, color=None):
    """Build a styled table cell."""
    style = {"padding": "10px 14px", "fontSize": "13px"}
    if bold:
        style["fontWeight"] = "500"
    if mono:
        style["fontFamily"] = "monospace"
        style["fontWeight"] = "600"
    if color:
        style["color"] = color
    return html.Td(str(text), style=style)


def _status_td(status_text, status_key=None):
    """Build a table cell with a status badge."""
    key = status_key or status_text
    sc = STATUS_COLORS.get(key, STATUS_COLORS["Healthy"])
    return html.Td(
        html.Span(
            status_text,
            className="status-badge",
            style={"backgroundColor": sc["bg"], "color": sc["text"], "border": f"1px solid {sc['border']}"},
        ),
        style={"padding": "10px 14px"},
    )


def _detail_row(label, value):
    """Single key-value detail row."""
    return html.Div(
        style={"display": "flex", "justifyContent": "space-between", "padding": "6px 0", "borderBottom": f"1px solid {COLORS['border']}"},
        children=[
            html.Span(label, style={"fontSize": "13px", "color": COLORS["text_muted"]}),
            html.Span(str(value), style={"fontSize": "13px", "fontWeight": "500"}),
        ],
    )


# ===================================================================
#  Landing & Hub renderers
# ===================================================================


def _render_landing():
    """Full-screen splash overlay with Blueprint IQ branding."""
    return html.Div(className="landing-overlay", children=[
        html.Div(
            style={"width": "56px", "height": "56px", "borderRadius": "16px",
                   "backgroundColor": COLORS["blue"], "display": "flex",
                   "alignItems": "center", "justifyContent": "center", "marginBottom": "24px"},
            children=html.I(className="fa-solid fa-cube", style={"color": "white", "fontSize": "24px"}),
        ),
        html.Div("Blueprint IQ", className="landing-title"),
        html.Div("AI-Powered Industry Analytics on Databricks", className="landing-subtitle"),
        dcc.Link("Explore Demos", href="/hub", className="landing-enter-btn"),
    ])


def _render_hub():
    """Demo hub grid with a card for each vertical."""
    cards = []
    for vk, vm in _VERTICAL_META.items():
        stat_divs = [
            html.Div(className="vertical-card-stat", children=[
                html.Strong(s[1]), s[0],
            ]) for s in vm["stats"]
        ]
        cards.append(
            dcc.Link(
                href=f"/{vk}/dashboard",
                className="vertical-card",
                children=[
                    html.Div(
                        className="vertical-card-icon",
                        style={"backgroundColor": f"{vm['color']}20"},
                        children=html.I(className=f"fa-solid {vm['icon']}", style={"color": vm["color"]}),
                    ),
                    html.Div(vm["title"], className="vertical-card-title"),
                    html.Div(vm["subtitle"], className="vertical-card-subtitle"),
                    html.Div(className="vertical-card-stats", children=stat_divs),
                ],
            )
        )

    return html.Div(style={"backgroundColor": COLORS["dark"], "minHeight": "100vh"}, children=[
        html.Div(className="hub-header", children=[
            html.Div("Blueprint IQ Demo Hub", className="hub-title"),
            html.Div("Choose an industry vertical to explore", className="hub-subtitle"),
        ]),
        html.Div(className="hub-grid", children=cards),
    ])


# ===================================================================
#  Dashboard renderer  --  config-driven KPIs for all verticals
# ===================================================================


def _render_dashboard():
    """Config-driven dashboard with KPI cards and charts."""
    import plotly.graph_objects as go

    from app.data_access import _current_use_case
    current_uc = _current_use_case()
    cfg = get_config()
    kpi_configs = cfg.get("dashboard", {}).get("kpis", [])
    demo_values = _DEMO_KPI_VALUES.get(current_uc, [])

    # Build KPI cards from config
    kpi_cards = []
    for idx, kpi_cfg in enumerate(kpi_configs):
        title = kpi_cfg["title"]
        accent = kpi_cfg.get("accent", "blue")
        fmt = kpi_cfg.get("format", "{}")
        alert = kpi_cfg.get("alert", False)
        icon = _ACCENT_ICONS.get(accent, "fa-chart-bar")

        # Use demo value or fall back to config value field
        if idx < len(demo_values):
            value_str = _format_demo_value(demo_values[idx], fmt)
        else:
            value_str = str(kpi_cfg.get("value", "N/A"))

        kpi_cards.append(_build_kpi_card(title, value_str, accent, icon, alert))

    # Determine grid class based on number of KPIs
    grid_class = "grid-4" if len(kpi_cards) >= 4 else "grid-4"

    # Find the dashboard page label from config
    dashboard_page = next((p for p in cfg.get("pages", []) if p["id"] == "dashboard"), {})
    page_title = dashboard_page.get("label", "Dashboard")
    page_subtitle = cfg["app"].get("subtitle", "Analytics Dashboard")

    # Charts section -- manufacturing gets scatter + SHAP; others get bar chart
    chart_section = []
    if current_uc == "manufacturing":
        scatter_data = get_anomaly_scatter_data(hours=1)
        shap_data = get_shap_importance()
        feed = get_live_inference_feed(limit=20)

        # Anomaly scatter chart
        scatter_fig = go.Figure()
        if scatter_data:
            normal = [p for p in scatter_data if not p.get("is_anomaly", False)]
            anomalies = [p for p in scatter_data if p.get("is_anomaly", False)]

            if normal:
                scatter_fig.add_trace(go.Scatter(
                    x=[p["vibration_hz"] for p in normal],
                    y=[p["temp_c"] for p in normal],
                    mode="markers",
                    name="Normal",
                    marker=dict(color=COLORS["blue"], size=7, opacity=0.7),
                    text=[f"{p['machine_id']}<br>Score: {p['anomaly_score']}" for p in normal],
                    hovertemplate="%{text}<extra></extra>",
                ))
            if anomalies:
                scatter_fig.add_trace(go.Scatter(
                    x=[p["vibration_hz"] for p in anomalies],
                    y=[p["temp_c"] for p in anomalies],
                    mode="markers",
                    name="Anomaly",
                    marker=dict(color=COLORS["red"], size=9, opacity=0.9, symbol="diamond"),
                    text=[f"{p['machine_id']}<br>Score: {p['anomaly_score']}" for p in anomalies],
                    hovertemplate="%{text}<extra></extra>",
                ))

        scatter_fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=COLORS["panel"],
            plot_bgcolor=COLORS["panel"],
            font=dict(family=FONT_FAMILY, color=COLORS["text_muted"]),
            xaxis_title="Vibration (Hz)",
            yaxis_title="Temperature (\u00b0C)",
            legend=dict(orientation="h", y=1.12),
            margin=dict(l=50, r=20, t=40, b=50),
            height=360,
        )

        # SHAP bar chart
        shap_fig = go.Figure()
        if shap_data:
            shap_sorted = sorted(shap_data, key=lambda x: x["importance"])
            shap_fig.add_trace(go.Bar(
                x=[s["importance"] for s in shap_sorted],
                y=[s["feature"] for s in shap_sorted],
                orientation="h",
                marker_color=COLORS["blue"],
            ))
        shap_fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=COLORS["panel"],
            plot_bgcolor=COLORS["panel"],
            font=dict(family=FONT_FAMILY, color=COLORS["text_muted"]),
            xaxis_title="SHAP Importance",
            margin=dict(l=120, r=20, t=20, b=40),
            height=280,
        )

        chart_section.append(
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px", "marginTop": "16px"},
                children=[
                    html.Div(className="card", children=[
                        html.Span("Anomaly Detection  \u2014  Last 1 Hour", className="card-title"),
                        dcc.Graph(figure=scatter_fig, config={"displayModeBar": False}),
                    ]),
                    html.Div(className="card", children=[
                        html.Span("SHAP Feature Importance", className="card-title"),
                        dcc.Graph(figure=shap_fig, config={"displayModeBar": False}),
                    ]),
                ],
            )
        )

        # Inference feed table
        feed_rows = []
        for row in feed[:10]:
            pred = row.get("prediction", "Normal")
            pred_color = COLORS["red"] if pred == "Anomaly" else COLORS["green"]
            feed_rows.append(
                html.Tr([
                    html.Td(row.get("timestamp", "")[:19], style={"padding": "6px 12px", "fontSize": "12px"}),
                    html.Td(row.get("machine_id", ""), style={"padding": "6px 12px", "fontSize": "12px"}),
                    html.Td(
                        html.Span(pred, style={"color": pred_color, "fontWeight": "600", "fontSize": "12px"}),
                        style={"padding": "6px 12px"},
                    ),
                    html.Td(f"{row.get('anomaly_score', 0):.3f}", style={"padding": "6px 12px", "fontSize": "12px"}),
                    html.Td(f"{row.get('latency_ms', 0):.0f} ms", style={"padding": "6px 12px", "fontSize": "12px"}),
                ])
            )

        feed_table = _build_table(
            ["Timestamp", "Machine", "Prediction", "Score", "Latency"],
            feed_rows,
        )

        chart_section.append(
            html.Div(className="card", style={"marginTop": "16px"}, children=[
                html.Span("Live Inference Feed", className="card-title"),
                feed_table,
            ])
        )
    else:
        # Generic bar chart from KPI data for non-manufacturing verticals
        chart_labels = [k["title"] for k in kpi_configs]
        chart_values = []
        for idx, k in enumerate(kpi_configs):
            if idx < len(demo_values):
                v = demo_values[idx]
                if isinstance(v, str):
                    # Parse numeric portion from formatted strings like "2.4M", "$47M"
                    cleaned = v.replace("$", "").replace("M", "").replace("K", "").replace("%", "")
                    try:
                        chart_values.append(float(cleaned))
                    except ValueError:
                        chart_values.append(0)
                else:
                    chart_values.append(float(v))
            else:
                chart_values.append(0)

        bar_colors = [COLORS.get(k.get("accent", "blue"), COLORS["blue"]) for k in kpi_configs]

        bar_fig = go.Figure()
        bar_fig.add_trace(go.Bar(
            x=chart_labels,
            y=chart_values,
            marker_color=bar_colors,
            text=[_format_demo_value(demo_values[i], kpi_configs[i].get("format", "{}")) if i < len(demo_values) else "" for i in range(len(kpi_configs))],
            textposition="outside",
            textfont=dict(color=COLORS["text_muted"], size=11),
        ))
        bar_fig.update_layout(
            template="plotly_dark",
            paper_bgcolor=COLORS["panel"],
            plot_bgcolor=COLORS["panel"],
            font=dict(family=FONT_FAMILY, color=COLORS["text_muted"]),
            yaxis_title="Value",
            margin=dict(l=60, r=20, t=30, b=80),
            height=360,
            showlegend=False,
        )

        chart_section.append(
            html.Div(className="card", style={"marginTop": "16px"}, children=[
                html.Span("Key Metrics Overview", className="card-title"),
                dcc.Graph(figure=bar_fig, config={"displayModeBar": False}),
            ])
        )

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1(page_title),
            html.P(page_subtitle),
        ]),
        html.Div(className="content-area", children=[
            html.Div(className=grid_class, children=kpi_cards),
        ] + chart_section),
    ])


# ===================================================================
#  Manufacturing-specific page renderers
# ===================================================================


def _render_inventory():
    """Inventory & Forecasting page."""
    items = get_inventory_status()

    rows = []
    for item in items:
        status = item.get("status", "Healthy")
        sc = STATUS_COLORS.get(status, STATUS_COLORS["Healthy"])
        rows.append(
            html.Tr([
                _td(item.get("component", ""), bold=True),
                _td(item.get("site", "")),
                _td(f"{item.get('current_stock', 0):,}"),
                _td(f"{item.get('daily_usage', 0):,}"),
                _td(f"{item.get('stock_days', 0):.1f}"),
                _status_td(status),
            ])
        )

    table = _build_table(["Component", "Site", "Current Stock", "Daily Usage", "Days Remaining", "Status"], rows)

    total_components = len(items)
    critical = sum(1 for i in items if i.get("status") == "Critical")
    low = sum(1 for i in items if i.get("status") == "Low")

    summary_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Total Components", str(total_components), "blue", "fa-boxes-stacked"),
        _build_kpi_card("Healthy", str(total_components - critical - low), "green", "fa-circle-check"),
        _build_kpi_card("Low Stock", str(low), "yellow", "fa-circle-exclamation"),
        _build_kpi_card("Critical", str(critical), "red", "fa-triangle-exclamation"),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Inventory & Forecasting"),
            html.P("Component stock levels and demand forecasting across all sites"),
        ]),
        html.Div(className="content-area", children=[
            summary_cards,
            html.Div(className="card", children=[
                html.Span("Component Inventory Status", className="card-title"),
                table,
            ]),
        ]),
    ])


def _render_quality():
    """Quality & Tolerance page."""
    qs = get_quality_summary()

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Total Inspections", f"{qs.get('total_inspections', 0):,.0f}", "blue", "fa-magnifying-glass"),
        _build_kpi_card("Out of Spec", str(qs.get("out_of_spec_count", 0)), "red", "fa-triangle-exclamation"),
        _build_kpi_card("Cp", f"{qs.get('cp', 0):.2f}", "green", "fa-chart-simple"),
        _build_kpi_card("Cpk", f"{qs.get('cpk', 0):.2f}", "green", "fa-chart-simple"),
    ])

    site_rows = []
    for site_info in qs.get("sites", []):
        site_rows.append(
            html.Tr([
                _td(site_info.get("site", ""), bold=True),
                _td(f"{site_info.get('inspections', 0):,}"),
                _td(str(site_info.get("out_of_spec", 0))),
                _td(f"{site_info.get('cpk', 0):.2f}"),
            ])
        )

    site_table = _build_table(["Site", "Inspections", "Out of Spec", "Cpk"], site_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Quality & Tolerance"),
            html.P("Dimensional inspection results and process capability metrics"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Site Breakdown", className="card-title"),
                site_table,
            ]),
        ]),
    ])


def _render_tracking():
    """Real-Time Build Tracking page."""
    events = get_build_tracking()

    batches = {}
    for e in events:
        bid = e["batch_id"]
        if bid not in batches:
            batches[bid] = {"batch_id": bid, "site": e["site"], "stations": 0, "defects": 0, "status": e["status"], "defect_flag": e.get("defect_flag")}
        batches[bid]["stations"] += 1
        batches[bid]["defects"] += e.get("defect_count", 0)
        if e["status"] == "Defect":
            batches[bid]["status"] = "Defect"
            batches[bid]["defect_flag"] = e.get("defect_flag")

    batch_rows = []
    for b in batches.values():
        status = b["status"]
        sc = STATUS_COLORS.get(status, STATUS_COLORS.get("Nominal", STATUS_COLORS["Healthy"]))
        if status == "Complete":
            sc = STATUS_COLORS["Healthy"]
        elif status == "In Progress":
            sc = STATUS_COLORS["Nominal"]

        batch_rows.append(
            html.Tr([
                _td(b["batch_id"], mono=True),
                _td(b["site"]),
                _td(str(b["stations"])),
                _td(str(b["defects"]), color=COLORS["red"] if b["defects"] > 5 else None),
                _status_td(status),
                _td(b.get("defect_flag") or "\u2014", color=COLORS["text_muted"]),
            ])
        )

    table = _build_table(["Batch ID", "Site", "Stations", "Defects", "Status", "Flag"], batch_rows)

    total_batches = len(batches)
    defect_batches = sum(1 for b in batches.values() if b["status"] == "Defect")
    in_progress = sum(1 for b in batches.values() if b["status"] == "In Progress")

    summary = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Total Batches", str(total_batches), "blue", "fa-layer-group"),
        _build_kpi_card("In Progress", str(in_progress), "yellow", "fa-spinner"),
        _build_kpi_card("Completed", str(total_batches - defect_batches - in_progress), "green", "fa-circle-check"),
        _build_kpi_card("Defects", str(defect_batches), "red", "fa-bug"),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Real-Time Build Tracking"),
            html.P("Batch progress and defect tracking across stations"),
        ]),
        html.Div(className="content-area", children=[
            summary,
            html.Div(className="card", children=[
                html.Span("Batch Summary", className="card-title"),
                table,
            ]),
        ]),
    ])


# ===================================================================
#  Generic page renderers for vertical-specific pages
# ===================================================================


def _render_compliance():
    """Compliance & Audit Hub page (risk vertical)."""
    cfg = get_config()
    frameworks = cfg.get("data", {}).get("compliance_frameworks", [])

    # Status color mapping for compliance
    compliance_status_map = {
        "Compliant": "Healthy",
        "Needs Review": "Low",
        "At Risk": "Critical",
    }

    rows = []
    for fw in frameworks:
        status = fw.get("status", "Compliant")
        status_key = compliance_status_map.get(status, "Healthy")
        rows.append(
            html.Tr([
                _td(fw.get("name", ""), bold=True),
                _td(fw.get("region", "")),
                _status_td(status, status_key),
                _td(str(fw.get("violations", 0)), color=COLORS["red"] if fw.get("violations", 0) > 0 else None),
            ])
        )

    table = _build_table(["Framework", "Region", "Status", "Violations"], rows)

    total = len(frameworks)
    compliant = sum(1 for f in frameworks if f.get("status") == "Compliant")
    at_risk = sum(1 for f in frameworks if f.get("status") == "At Risk")
    needs_review = sum(1 for f in frameworks if f.get("status") == "Needs Review")

    summary = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Total Frameworks", str(total), "blue", "fa-clipboard-check"),
        _build_kpi_card("Compliant", str(compliant), "green", "fa-circle-check"),
        _build_kpi_card("Needs Review", str(needs_review), "yellow", "fa-circle-exclamation"),
        _build_kpi_card("At Risk", str(at_risk), "red", "fa-triangle-exclamation", alert=at_risk > 0),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Compliance & Audit Hub"),
            html.P("Regulatory compliance tracking across all frameworks"),
        ]),
        html.Div(className="content-area", children=[
            summary,
            html.Div(className="card", children=[
                html.Span("Compliance Framework Status", className="card-title"),
                table,
            ]),
        ]),
    ])


def _render_pii():
    """Real-Time PII Monitor page (risk vertical)."""
    cfg = get_config()
    pii = cfg.get("data", {}).get("pii_monitoring", {})

    scanned = pii.get("records_scanned_24h", 0)
    flagged = pii.get("flagged_anomalies", 0)
    pii_types = pii.get("pii_types", [])

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Records Scanned (24h)", f"{scanned:,.0f}", "blue", "fa-magnifying-glass"),
        _build_kpi_card("Flagged Anomalies", str(flagged), "red", "fa-flag", alert=flagged > 0),
        _build_kpi_card("PII Types Tracked", str(len(pii_types)), "purple", "fa-shield-halved"),
        _build_kpi_card("Detection Rate", f"{(flagged / max(scanned, 1)) * 100:.4f}%", "green", "fa-crosshairs"),
    ])

    # PII type breakdown table
    pii_rows = []
    demo_counts = [42, 38, 31, 31]
    for idx, ptype in enumerate(pii_types):
        count = demo_counts[idx] if idx < len(demo_counts) else 0
        severity = "Critical" if ptype in ("US_SSN", "CREDIT_CARD") else "Low"
        severity_key = "Critical" if severity == "Critical" else "Healthy"
        pii_rows.append(
            html.Tr([
                _td(ptype, mono=True),
                _td(str(count)),
                _status_td(severity, severity_key),
            ])
        )

    pii_table = _build_table(["PII Type", "Detections (24h)", "Severity"], pii_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Real-Time PII Monitor"),
            html.P("Continuous PII detection and data classification scanning"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("PII Detection Breakdown", className="card-title"),
                pii_table,
            ]),
        ]),
    ])


def _render_rbac():
    """Advanced RBAC Logs page (risk vertical)."""
    cfg = get_config()
    rbac = cfg.get("data", {}).get("rbac_logs", {})
    users = rbac.get("users", [])
    risk_levels = rbac.get("risk_levels", [])

    # Generate demo access log entries
    import random
    demo_assets = ["customer_pii_table", "financial_reports", "hr_salary_data", "ml_training_set", "compliance_docs", "analytics_dashboard"]
    demo_actions = ["SELECT", "EXPORT", "VIEW", "DOWNLOAD"]
    demo_policies = ["Approved", "Flagged", "Under Review", "Approved"]

    log_rows = []
    for idx, user in enumerate(users):
        risk = risk_levels[idx] if idx < len(risk_levels) else "Standard Baseline"
        risk_key = "Critical" if risk == "High" else ("Low" if risk == "Medium" else "Healthy")
        asset = demo_assets[idx % len(demo_assets)]
        action = demo_actions[idx % len(demo_actions)]
        policy = demo_policies[idx % len(demo_policies)]
        policy_key = "Critical" if policy == "Flagged" else ("Low" if policy == "Under Review" else "Healthy")
        log_rows.append(
            html.Tr([
                _td(user, mono=True),
                _td(asset),
                _td(action),
                _status_td(risk, risk_key),
                _status_td(policy, policy_key),
            ])
        )

    log_table = _build_table(["User", "Asset", "Action", "Risk Level", "Policy Flag"], log_rows)

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Tracked Users", str(len(users)), "blue", "fa-users"),
        _build_kpi_card("Access Events (24h)", "1,247", "purple", "fa-list"),
        _build_kpi_card("High Risk Actions", "3", "red", "fa-triangle-exclamation", alert=True),
        _build_kpi_card("Policy Violations", "1", "yellow", "fa-gavel"),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Advanced RBAC Logs"),
            html.P("Role-based access control monitoring and anomaly detection"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Recent Access Log", className="card-title"),
                log_table,
            ]),
        ]),
    ])


def _render_patient_flow():
    """Patient Flow & Capacity page (healthcare vertical)."""
    cfg = get_config()
    pf = cfg.get("data", {}).get("patient_flow", {})
    facilities = cfg.get("data", {}).get("facilities", [])

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Avg Daily Admissions", str(pf.get("avg_daily_admissions", 0)), "blue", "fa-hospital-user"),
        _build_kpi_card("Avg ED Wait Time", f"{pf.get('avg_ed_wait_minutes', 0)} min", "purple", "fa-clock"),
        _build_kpi_card("Bed Utilization", f"{pf.get('bed_utilization_pct', 0):.1f}%", "green", "fa-bed"),
        _build_kpi_card("Avg Length of Stay", f"{pf.get('avg_los_days', 0):.1f} days", "yellow", "fa-calendar-days"),
    ])

    # Facility breakdown
    demo_facility_data = [
        {"admissions": 142, "ed_wait": 28, "beds": 91.2, "status": "Critical"},
        {"admissions": 98, "ed_wait": 38, "beds": 84.1, "status": "Healthy"},
        {"admissions": 45, "ed_wait": 22, "beds": 72.5, "status": "Healthy"},
    ]
    fac_rows = []
    for idx, fac in enumerate(facilities):
        data = demo_facility_data[idx] if idx < len(demo_facility_data) else {"admissions": 0, "ed_wait": 0, "beds": 0, "status": "Healthy"}
        bed_color = COLORS["red"] if data["beds"] > 90 else None
        status_key = "Critical" if data["beds"] > 90 else "Healthy"
        fac_rows.append(
            html.Tr([
                _td(fac, bold=True),
                _td(str(data["admissions"])),
                _td(f"{data['ed_wait']} min"),
                _td(f"{data['beds']:.1f}%", color=bed_color),
                _status_td("Near Capacity" if data["beds"] > 90 else "Normal", status_key),
            ])
        )

    fac_table = _build_table(["Facility", "Admissions Today", "ED Wait", "Bed Util %", "Status"], fac_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Patient Flow & Capacity"),
            html.P("Real-time patient flow tracking and capacity management across facilities"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Facility Overview", className="card-title"),
                fac_table,
            ]),
        ]),
    ])


def _render_readmissions():
    """Readmission Risk page (healthcare vertical)."""
    cfg = get_config()
    ra = cfg.get("data", {}).get("readmissions", {})

    total_discharges = ra.get("total_discharges_30d", 0)
    readmission_rate = ra.get("readmission_rate", 0)
    high_risk = ra.get("high_risk_patients", 0)
    readmitted = int(total_discharges * readmission_rate)

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Discharges (30d)", f"{total_discharges:,}", "blue", "fa-right-from-bracket"),
        _build_kpi_card("Readmission Rate", f"{readmission_rate * 100:.1f}%", "purple", "fa-rotate-left"),
        _build_kpi_card("Readmitted", str(readmitted), "yellow", "fa-hospital"),
        _build_kpi_card("High-Risk Patients", str(high_risk), "red", "fa-user-injured", alert=True),
    ])

    # Risk factor breakdown
    risk_rows = []
    demo_risk_factors = [
        ("Heart Failure", 42, "18.4%", "Critical"),
        ("COPD", 38, "14.2%", "Critical"),
        ("Pneumonia", 31, "11.8%", "Low"),
        ("Hip Replacement", 22, "6.1%", "Healthy"),
        ("Diabetes Management", 23, "9.5%", "Low"),
    ]
    for dx, count, rate, risk in demo_risk_factors:
        risk_key = risk
        risk_rows.append(
            html.Tr([
                _td(dx, bold=True),
                _td(str(count)),
                _td(rate),
                _status_td(risk, risk_key),
            ])
        )

    risk_table = _build_table(["Diagnosis", "Readmissions (30d)", "Rate", "Risk Level"], risk_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Readmission Risk"),
            html.P("ML-powered readmission prediction and risk stratification"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Readmissions by Diagnosis", className="card-title"),
                risk_table,
            ]),
        ]),
    ])


def _render_equipment():
    """Equipment & Asset Monitoring page (healthcare vertical)."""
    cfg = get_config()
    eq = cfg.get("data", {}).get("equipment_monitoring", {})
    equipment_types = cfg.get("data", {}).get("equipment_types", {})

    total = eq.get("total_assets", 0)
    maint = eq.get("maintenance_due", 0)
    alerts = eq.get("critical_alerts", 0)
    operational = total - maint - alerts

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Total Assets", f"{total:,}", "blue", "fa-stethoscope"),
        _build_kpi_card("Operational", f"{operational:,}", "green", "fa-circle-check"),
        _build_kpi_card("Maintenance Due", str(maint), "yellow", "fa-wrench"),
        _build_kpi_card("Critical Alerts", str(alerts), "red", "fa-bell", alert=alerts > 0),
    ])

    # Equipment status table from config
    eq_rows = []
    demo_statuses = ["Operational", "Operational", "Maintenance Due", "Operational", "Critical", "Operational", "Operational", "Maintenance Due", "Operational"]
    status_map = {"Operational": "Healthy", "Maintenance Due": "Low", "Critical": "Critical"}
    idx = 0
    for facility, machines in equipment_types.items():
        for machine in machines:
            status = demo_statuses[idx % len(demo_statuses)]
            status_key = status_map.get(status, "Healthy")
            eq_rows.append(
                html.Tr([
                    _td(machine, mono=True),
                    _td(facility),
                    _status_td(status, status_key),
                ])
            )
            idx += 1

    eq_table = _build_table(["Equipment ID", "Facility", "Status"], eq_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Equipment & Asset Monitoring"),
            html.P("Predictive maintenance and real-time asset health tracking"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Equipment Status", className="card-title"),
                eq_table,
            ]),
        ]),
    ])


def _render_player_health():
    """Player Health & Retention page (gaming vertical)."""
    cfg = get_config()
    ph = cfg.get("data", {}).get("player_health", {})
    segments = cfg.get("data", {}).get("player_segments", [])

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("D1 Retention", f"{ph.get('d1_retention', 0) * 100:.0f}%", "blue", "fa-calendar-day"),
        _build_kpi_card("D7 Retention", f"{ph.get('d7_retention', 0) * 100:.0f}%", "purple", "fa-calendar-week"),
        _build_kpi_card("D30 Retention", f"{ph.get('d30_retention', 0) * 100:.0f}%", "green", "fa-calendar"),
        _build_kpi_card("High Churn Risk", f"{ph.get('churn_risk_high', 0):,}", "red", "fa-user-minus", alert=True),
    ])

    # Segment breakdown
    demo_segment_data = [
        {"d1": "82%", "d7": "64%", "d30": "45%", "ltv": "$284.50", "churn_risk": "Low"},
        {"d1": "74%", "d7": "48%", "d30": "28%", "ltv": "$68.20", "churn_risk": "Medium"},
        {"d1": "65%", "d7": "35%", "d30": "15%", "ltv": "$12.40", "churn_risk": "High"},
        {"d1": "58%", "d7": "28%", "d30": "8%", "ltv": "$2.10", "churn_risk": "Critical"},
    ]
    seg_rows = []
    for idx, seg in enumerate(segments):
        data = demo_segment_data[idx] if idx < len(demo_segment_data) else {}
        churn = data.get("churn_risk", "Low")
        churn_key = {"Low": "Healthy", "Medium": "Low", "High": "Low", "Critical": "Critical"}.get(churn, "Healthy")
        seg_rows.append(
            html.Tr([
                _td(seg, bold=True),
                _td(data.get("d1", "N/A")),
                _td(data.get("d7", "N/A")),
                _td(data.get("d30", "N/A")),
                _td(data.get("ltv", "N/A")),
                _status_td(churn, churn_key),
            ])
        )

    seg_table = _build_table(["Segment", "D1 Retention", "D7 Retention", "D30 Retention", "Avg LTV", "Churn Risk"], seg_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Player Health & Retention"),
            html.P("Retention cohort analysis and churn risk prediction by player segment"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Segment Breakdown", className="card-title"),
                seg_table,
            ]),
        ]),
    ])


def _render_economy():
    """In-Game Economy page (gaming vertical)."""
    cfg = get_config()
    econ = cfg.get("data", {}).get("economy", {})

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Daily Revenue", f"${econ.get('daily_revenue', 0):,.0f}", "blue", "fa-dollar-sign"),
        _build_kpi_card("ARPDAU", f"${econ.get('arpdau', 0):.3f}", "purple", "fa-chart-line"),
        _build_kpi_card("Inflation Index", f"{econ.get('inflation_index', 1.0):.2f}", "green", "fa-arrow-trend-up"),
        _build_kpi_card("Suspicious Txns", str(econ.get("suspicious_transactions", 0)), "red", "fa-flag", alert=True),
    ])

    # Game title breakdown
    titles = cfg.get("data", {}).get("game_titles", [])
    demo_title_data = [
        {"revenue": "$142K", "arpdau": "$0.142", "items_traded": "820K", "inflation": "1.04"},
        {"revenue": "$98K", "arpdau": "$0.108", "items_traded": "640K", "inflation": "1.02"},
        {"revenue": "$44K", "arpdau": "$0.092", "items_traded": "390K", "inflation": "1.01"},
    ]
    title_rows = []
    for idx, title in enumerate(titles):
        data = demo_title_data[idx] if idx < len(demo_title_data) else {}
        title_rows.append(
            html.Tr([
                _td(title, bold=True),
                _td(data.get("revenue", "N/A")),
                _td(data.get("arpdau", "N/A")),
                _td(data.get("items_traded", "N/A")),
                _td(data.get("inflation", "N/A")),
            ])
        )

    title_table = _build_table(["Game Title", "Revenue (24h)", "ARPDAU", "Items Traded", "Inflation Index"], title_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("In-Game Economy"),
            html.P("Virtual economy health monitoring, revenue analytics, and fraud detection"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Economy by Game Title", className="card-title"),
                title_table,
            ]),
        ]),
    ])


def _render_matchmaking():
    """Matchmaking & Performance page (gaming vertical)."""
    cfg = get_config()
    mm = cfg.get("data", {}).get("matchmaking", {})

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Avg Queue Time", f"{mm.get('avg_queue_time_sec', 0):.1f}s", "blue", "fa-clock"),
        _build_kpi_card("Matches (24h)", f"{mm.get('matches_24h', 0):,.0f}", "purple", "fa-users"),
        _build_kpi_card("Skill Spread", f"{mm.get('skill_rating_spread', 0):.2f}", "green", "fa-chart-simple"),
        _build_kpi_card("Unfair Reports", f"{mm.get('reported_unfair', 0) * 100:.1f}%", "yellow", "fa-flag"),
    ])

    # Region breakdown
    regions = cfg.get("data", {}).get("regions", [])
    demo_region_data = [
        {"queue": "8.2s", "matches": "1.1M", "fairness": "0.92"},
        {"queue": "11.4s", "matches": "840K", "fairness": "0.89"},
        {"queue": "14.1s", "matches": "720K", "fairness": "0.91"},
        {"queue": "12.8s", "matches": "680K", "fairness": "0.90"},
        {"queue": "15.2s", "matches": "540K", "fairness": "0.88"},
        {"queue": "13.6s", "matches": "320K", "fairness": "0.87"},
    ]
    region_rows = []
    for idx, region in enumerate(regions):
        data = demo_region_data[idx] if idx < len(demo_region_data) else {}
        region_rows.append(
            html.Tr([
                _td(region, bold=True),
                _td(data.get("queue", "N/A")),
                _td(data.get("matches", "N/A")),
                _td(data.get("fairness", "N/A")),
            ])
        )

    region_table = _build_table(["Region", "Avg Queue Time", "Matches (24h)", "Fairness Score"], region_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Matchmaking & Performance"),
            html.P("Queue times, match quality, and fairness metrics across all regions"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Regional Breakdown", className="card-title"),
                region_table,
            ]),
        ]),
    ])


def _render_fraud():
    """Fraud Detection & AML page (financial_services vertical)."""
    cfg = get_config()
    fd = cfg.get("data", {}).get("fraud_detection", {})
    channels = cfg.get("data", {}).get("transaction_channels", [])

    txns = fd.get("transactions_per_day", 0)
    blocked = fd.get("blocked_today", 0)
    fpr = fd.get("false_positive_rate", 0)
    avg_amount = fd.get("avg_fraud_amount", 0)

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Transactions Today", f"{txns / 1_000_000:.1f}M", "blue", "fa-credit-card"),
        _build_kpi_card("Fraud Blocked", str(blocked), "red", "fa-shield-halved", alert=True),
        _build_kpi_card("False Positive Rate", f"{fpr * 100:.2f}%", "green", "fa-bullseye"),
        _build_kpi_card("Avg Fraud Amount", f"${avg_amount:,.0f}", "purple", "fa-dollar-sign"),
    ])

    # Channel breakdown
    demo_channel_data = [
        {"txns": "4.2M", "fraud": "312", "rate": "0.0074%", "risk": "Critical"},
        {"txns": "3.8M", "fraud": "198", "rate": "0.0052%", "risk": "Low"},
        {"txns": "1.9M", "fraud": "142", "rate": "0.0075%", "risk": "Critical"},
        {"txns": "1.2M", "fraud": "87", "rate": "0.0073%", "risk": "Low"},
        {"txns": "0.9M", "fraud": "68", "rate": "0.0076%", "risk": "Critical"},
        {"txns": "0.5M", "fraud": "40", "rate": "0.0080%", "risk": "Critical"},
    ]
    chan_rows = []
    for idx, ch in enumerate(channels):
        data = demo_channel_data[idx] if idx < len(demo_channel_data) else {}
        risk = data.get("risk", "Healthy")
        risk_key = {"Critical": "Critical", "Low": "Low"}.get(risk, "Healthy")
        chan_rows.append(
            html.Tr([
                _td(ch, bold=True),
                _td(data.get("txns", "N/A")),
                _td(data.get("fraud", "N/A")),
                _td(data.get("rate", "N/A")),
                _status_td("High" if risk == "Critical" else "Normal", risk_key),
            ])
        )

    chan_table = _build_table(["Channel", "Transactions", "Fraud Blocked", "Fraud Rate", "Risk Level"], chan_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Fraud Detection & AML"),
            html.P("Real-time transaction monitoring and anti-money laundering intelligence"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Fraud by Channel", className="card-title"),
                chan_table,
            ]),
        ]),
    ])


def _render_credit():
    """Credit Risk Analytics page (financial_services vertical)."""
    cfg = get_config()
    cr = cfg.get("data", {}).get("credit_risk", {})
    biz_lines = cfg.get("data", {}).get("business_lines", [])

    portfolio_val = cr.get("total_portfolio_value", 0)
    avg_score = cr.get("avg_credit_score", 0)
    delinquency = cr.get("delinquency_rate_30d", 0)
    high_risk = cr.get("high_risk_accounts", 0)

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Portfolio Value", f"${portfolio_val / 1_000_000_000:.1f}B", "blue", "fa-building-columns"),
        _build_kpi_card("Avg Credit Score", str(avg_score), "purple", "fa-chart-simple"),
        _build_kpi_card("Delinquency Rate", f"{delinquency * 100:.1f}%", "yellow", "fa-clock"),
        _build_kpi_card("High-Risk Accounts", f"{high_risk:,}", "red", "fa-user-shield", alert=True),
    ])

    # Business line breakdown
    demo_biz_data = [
        {"portfolio": "$2.1B", "score": "722", "delinq": "2.8%", "risk": "Healthy"},
        {"portfolio": "$3.4B", "score": "698", "delinq": "4.1%", "risk": "Low"},
        {"portfolio": "$1.8B", "score": "741", "delinq": "1.2%", "risk": "Healthy"},
        {"portfolio": "$1.1B", "score": "695", "delinq": "3.5%", "risk": "Low"},
    ]
    biz_rows = []
    for idx, bl in enumerate(biz_lines):
        data = demo_biz_data[idx] if idx < len(demo_biz_data) else {}
        risk = data.get("risk", "Healthy")
        biz_rows.append(
            html.Tr([
                _td(bl, bold=True),
                _td(data.get("portfolio", "N/A")),
                _td(data.get("score", "N/A")),
                _td(data.get("delinq", "N/A")),
                _status_td(risk),
            ])
        )

    biz_table = _build_table(["Business Line", "Portfolio", "Avg Score", "Delinquency", "Risk"], biz_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Credit Risk Analytics"),
            html.P("Portfolio credit quality monitoring and default prediction"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Credit Risk by Business Line", className="card-title"),
                biz_table,
            ]),
        ]),
    ])


def _render_portfolio():
    """Portfolio & Market Risk page (financial_services vertical)."""
    cfg = get_config()
    pf = cfg.get("data", {}).get("portfolio", {})

    aum = pf.get("aum_total", 0)
    var_95 = pf.get("var_95_daily", 0)
    sharpe = pf.get("sharpe_ratio", 0)
    beta = pf.get("beta_portfolio", 0)
    positions = pf.get("active_positions", 0)

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("AUM Total", f"${aum / 1_000_000_000:.1f}B", "blue", "fa-vault"),
        _build_kpi_card("VaR (95% Daily)", f"${var_95 / 1_000_000:.0f}M", "purple", "fa-chart-line"),
        _build_kpi_card("Sharpe Ratio", f"{sharpe:.2f}", "green", "fa-arrow-trend-up"),
        _build_kpi_card("Portfolio Beta", f"{beta:.2f}", "yellow", "fa-scale-balanced"),
    ])

    # Positions summary
    regions = cfg.get("data", {}).get("regions", [])
    demo_region_data = [
        {"aum": "$6.2B", "var": "$12M", "sharpe": "1.48", "positions": "2,140"},
        {"aum": "$4.8B", "var": "$9M", "sharpe": "1.39", "positions": "1,680"},
        {"aum": "$5.1B", "var": "$11M", "sharpe": "1.44", "positions": "1,890"},
        {"aum": "$4.2B", "var": "$8M", "sharpe": "1.35", "positions": "1,540"},
        {"aum": "$4.2B", "var": "$7M", "sharpe": "1.52", "positions": "1,200"},
    ]
    region_rows = []
    for idx, region in enumerate(regions):
        data = demo_region_data[idx] if idx < len(demo_region_data) else {}
        region_rows.append(
            html.Tr([
                _td(region, bold=True),
                _td(data.get("aum", "N/A")),
                _td(data.get("var", "N/A")),
                _td(data.get("sharpe", "N/A")),
                _td(data.get("positions", "N/A")),
            ])
        )

    region_table = _build_table(["Region", "AUM", "VaR (95%)", "Sharpe Ratio", "Active Positions"], region_rows)

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Portfolio & Market Risk"),
            html.P("Portfolio analytics, Value at Risk, and market exposure monitoring"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            html.Div(className="card", children=[
                html.Span("Portfolio by Region", className="card-title"),
                region_table,
            ]),
            html.Div(className="card", style={"marginTop": "16px"}, children=[
                html.Span("Portfolio Summary", className="card-title"),
                html.Div(style={"marginTop": "12px"}, children=[
                    _detail_row("Active Positions", f"{positions:,}"),
                    _detail_row("Portfolio Beta", f"{beta:.2f}"),
                    _detail_row("Sharpe Ratio", f"{sharpe:.2f}"),
                    _detail_row("AUM Total", f"${aum / 1_000_000_000:.1f}B"),
                ]),
            ]),
        ]),
    ])


# ===================================================================
#  Architecture & Details  --  generic across verticals
# ===================================================================


def _render_architecture():
    """System Architecture informational page -- reads config for context."""
    cfg = get_config()
    app_name = cfg["app"]["name"]
    catalog = cfg["app"].get("catalog", "")
    genie_tables = cfg.get("genie", {}).get("tables", [])

    # Determine silver/gold table names from genie tables
    silver_tables = [t.split(".")[-1] for t in genie_tables if ".silver." in t]
    gold_tables = [t.split(".")[-1] for t in genie_tables if ".gold." in t]

    # Get ML model info
    ml_cfg = cfg.get("ml", {})
    model_names = []
    algorithms = []
    for key, model in ml_cfg.items():
        if isinstance(model, dict) and "name" in model:
            model_names.append(model["name"])
            algorithms.append(model.get("algorithm", ""))

    layers = [
        ("Bronze Layer", "fa-database", COLORS["yellow"],
         f"Raw ingestion from source systems into {catalog} Delta tables "
         "via Auto Loader with schema enforcement and audit logging."),
        ("Silver Layer", "fa-filter", COLORS["blue"],
         "Cleansed and enriched tables: " +
         (", ".join(silver_tables) if silver_tables else "domain-specific tables") +
         " with deduplication, type casting, and SCD2 merges."),
        ("Gold Layer", "fa-gem", COLORS["green"],
         "Business-ready aggregates: " +
         (", ".join(gold_tables) if gold_tables else "KPI and summary tables") +
         ". Optimized for BI and dashboards."),
        ("ML & Serving", "fa-brain", COLORS["purple"],
         (", ".join(f"{n} ({a})" for n, a in zip(model_names, algorithms)) if model_names else "ML models") +
         ". Registered in MLflow, served via Model Serving with real-time inference."),
    ]

    cards = []
    for title, icon, color, desc in layers:
        cards.append(
            html.Div(className="card", children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "12px", "marginBottom": "12px"},
                    children=[
                        html.Div(
                            style={
                                "width": "36px", "height": "36px", "borderRadius": "8px",
                                "backgroundColor": f"{color}15", "display": "flex",
                                "alignItems": "center", "justifyContent": "center",
                            },
                            children=html.I(className=f"fa-solid {icon}", style={"color": color, "fontSize": "16px"}),
                        ),
                        html.Span(title, style={"fontSize": "15px", "fontWeight": "600"}),
                    ],
                ),
                html.P(desc, style={"fontSize": "13px", "color": COLORS["text_muted"], "lineHeight": "1.6"}),
            ])
        )

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Solution Architecture"),
            html.P(f"{app_name} lakehouse medallion architecture and ML pipeline overview"),
        ]),
        html.Div(className="content-area", children=[
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
                children=cards,
            ),
        ]),
    ])


def _render_details():
    """Implementation Details page -- reads all ML models from config."""
    cfg = get_config()
    ml_cfg = cfg.get("ml", {})
    app_name = cfg["app"]["name"]

    # Build model cards dynamically from all ML models in config
    model_cards = []
    for model_key, model_info in ml_cfg.items():
        if not isinstance(model_info, dict) or "name" not in model_info:
            continue
        card_title = model_key.replace("_", " ").title()
        detail_rows = [
            _detail_row("Model Name", model_info.get("name", "N/A")),
            _detail_row("Algorithm", model_info.get("algorithm", "N/A")),
        ]
        if "features" in model_info:
            detail_rows.append(_detail_row("Features", ", ".join(model_info["features"])))
        if "target_f1" in model_info:
            detail_rows.append(_detail_row("Target F1", str(model_info["target_f1"])))
        if "target_metric" in model_info:
            detail_rows.append(_detail_row("Target Metric", model_info["target_metric"]))
        if "target_value" in model_info:
            detail_rows.append(_detail_row("Target Value", str(model_info["target_value"])))
        if "n_estimators" in model_info:
            detail_rows.append(_detail_row("Estimators", str(model_info["n_estimators"])))
        if "max_depth" in model_info:
            detail_rows.append(_detail_row("Max Depth", str(model_info["max_depth"])))
        if "horizon_days" in model_info:
            detail_rows.append(_detail_row("Forecast Horizon", f"{model_info['horizon_days']} days"))

        model_cards.append(
            html.Div(className="card", children=[
                html.Span(card_title, className="card-title"),
                html.Div(style={"marginTop": "12px"}, children=detail_rows),
            ])
        )

    # Data configuration card -- generic across verticals
    data_cfg = cfg.get("data", {})
    data_rows = [_detail_row("Catalog", cfg["app"].get("catalog", "N/A"))]

    # Add domain-specific data summary fields
    if "sites" in data_cfg:
        data_rows.append(_detail_row("Sites", ", ".join(data_cfg["sites"])))
    if "facilities" in data_cfg:
        data_rows.append(_detail_row("Facilities", ", ".join(data_cfg["facilities"])))
    if "game_titles" in data_cfg:
        data_rows.append(_detail_row("Game Titles", ", ".join(data_cfg["game_titles"])))
    if "business_lines" in data_cfg:
        data_rows.append(_detail_row("Business Lines", ", ".join(data_cfg["business_lines"])))
    if "domains" in data_cfg:
        data_rows.append(_detail_row("Domains", ", ".join(data_cfg["domains"])))
    if "regions" in data_cfg:
        data_rows.append(_detail_row("Regions", ", ".join(data_cfg["regions"])))

    # Telemetry stats if available
    if "telemetry" in data_cfg and isinstance(data_cfg["telemetry"], dict):
        telem = data_cfg["telemetry"]
        if "records_per_day" in telem:
            data_rows.append(_detail_row("Telemetry/day", f"{telem['records_per_day']:,}"))
        if "anomaly_rate" in telem:
            data_rows.append(_detail_row("Anomaly Rate", f"{telem['anomaly_rate']:.1%}"))
        if "dau" in telem:
            data_rows.append(_detail_row("DAU", f"{telem['dau']:,}"))
        if "events_per_second" in telem:
            data_rows.append(_detail_row("Events/sec", f"{telem['events_per_second']:,}"))
    if "inspections" in data_cfg:
        insp = data_cfg["inspections"]
        if "records_per_day" in insp:
            data_rows.append(_detail_row("Inspections/day", f"{insp['records_per_day']:,}"))
    if "fraud_detection" in data_cfg:
        fd = data_cfg["fraud_detection"]
        if "transactions_per_day" in fd:
            data_rows.append(_detail_row("Transactions/day", f"{fd['transactions_per_day']:,}"))

    data_card = html.Div(className="card", children=[
        html.Span("Data Configuration", className="card-title"),
        html.Div(style={"marginTop": "12px"}, children=data_rows),
    ])

    # Genie card
    genie_cfg = cfg.get("genie", {})
    genie_card = html.Div(className="card", children=[
        html.Span("Genie AI Space", className="card-title"),
        html.Div(style={"marginTop": "12px"}, children=[
            _detail_row("Space Name", genie_cfg.get("space_name", "N/A")),
            _detail_row("Tables", str(len(genie_cfg.get("tables", [])))),
            _detail_row("Sample Questions", str(len(genie_cfg.get("sample_questions", [])))),
        ]),
    ])

    all_cards = model_cards + [data_card, genie_card]

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Implementation Details"),
            html.P(f"Technical configuration and model parameters for {app_name}"),
        ]),
        html.Div(className="content-area", children=[
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
                children=all_cards,
            ),
        ]),
    ])


# ===================================================================
#  Generic page renderer (fallback)
# ===================================================================


def _render_generic_page(page_id):
    """Fallback renderer for pages without a specific implementation."""
    cfg = get_config()
    page_cfg = next((p for p in cfg.get("pages", []) if p["id"] == page_id), {})
    label = page_cfg.get("label", page_id.replace("_", " ").title())
    icon = page_cfg.get("icon", "fa-file")

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1(label),
            html.P(f"Data and analytics for {label.lower()}"),
        ]),
        html.Div(className="content-area", children=[
            html.Div(className="card", children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "12px", "marginBottom": "16px"},
                    children=[
                        html.I(className=f"fa-solid {icon}", style={"color": COLORS["blue"], "fontSize": "24px"}),
                        html.Span(label, style={"fontSize": "18px", "fontWeight": "600"}),
                    ],
                ),
                html.P(
                    f"This page displays {label} data. Connect to your Databricks workspace to see live data.",
                    style={"fontSize": "13px", "color": COLORS["text_muted"], "lineHeight": "1.6"},
                ),
            ]),
        ]),
    ])


# ===================================================================
#  Page router mapping  --  built dynamically from config
# ===================================================================

# Known page-specific renderers
_SPECIFIC_RENDERERS = {
    # Manufacturing
    "inventory": _render_inventory,
    "quality": _render_quality,
    "tracking": _render_tracking,
    # Risk
    "compliance": _render_compliance,
    "pii": _render_pii,
    "rbac": _render_rbac,
    # Healthcare
    "patient_flow": _render_patient_flow,
    "readmissions": _render_readmissions,
    "equipment": _render_equipment,
    # Gaming
    "player_health": _render_player_health,
    "economy": _render_economy,
    "matchmaking": _render_matchmaking,
    # Financial Services
    "fraud": _render_fraud,
    "credit": _render_credit,
    "portfolio": _render_portfolio,
    # Shared
    "architecture": _render_architecture,
    "details": _render_details,
    "dashboard": _render_dashboard,
}

def _get_renderer(page_id):
    """Look up the renderer for a page id, falling back to generic."""
    if page_id in _SPECIFIC_RENDERERS:
        return _SPECIFIC_RENDERERS[page_id]
    return lambda: _render_generic_page(page_id)


# ===================================================================
#  Callbacks
# ===================================================================


@app.callback(
    Output("page-content", "children"),
    Output("sidebar-container", "children"),
    Output("sidebar-container", "style"),
    Output("genie-panel-container", "children"),
    Output("genie-panel-container", "style"),
    Output("active-vertical", "data"),
    Input("url", "pathname"),
    Input("interval-refresh", "n_intervals"),
)
def route_page(pathname, n_intervals):
    """Route URL to the correct page, updating sidebar and genie panel per vertical.

    URL patterns:
      /              -> landing splash
      /hub           -> demo hub grid
      /<vertical>/<page> -> vertical page (e.g. /manufacturing/dashboard)
    """
    if pathname is None or pathname == "/":
        # Landing page - no sidebar or genie
        return (_render_landing(), [], {"display": "none"}, [], {"display": "none"}, None)

    parts = pathname.strip("/").split("/")

    if parts[0] == "hub":
        return (_render_hub(), [], {"display": "none"}, [], {"display": "none"}, None)

    vertical = parts[0]
    page_id = parts[1] if len(parts) > 1 else "dashboard"

    if vertical not in ALL_VERTICALS:
        # Try treating the whole path as a page id for backward compat
        page_id = vertical
        vertical = os.environ.get("USE_CASE", "manufacturing")

    # Set the active vertical so get_config() returns the right config
    set_active_vertical(vertical)

    renderer = _get_renderer(page_id)

    try:
        content = renderer()
    except Exception as e:
        content = html.Div(
            className="content-area",
            children=[
                html.Div(className="card", children=[
                    html.H3("Error loading page", style={"color": COLORS["red"]}),
                    html.P(str(e), style={"color": COLORS["text_muted"], "marginTop": "8px"}),
                ]),
            ],
        )

    # Build sidebar and genie panel for this vertical
    sidebar_children = build_sidebar(vertical, page_id)
    genie_children = build_genie_panel(vertical)

    return (
        content,
        sidebar_children,
        {"display": "block"},
        genie_children,
        {"display": "block"},
        vertical,
    )


@app.callback(
    Output("genie-panel", "style"),
    Output("genie-open-wrapper", "style"),
    Input("genie-close-btn", "n_clicks"),
    Input("genie-open-btn", "n_clicks"),
    State("genie-panel", "style"),
)
def toggle_genie_panel(close_clicks, open_clicks, current_style):
    """Show/hide the Genie AI panel."""
    ctx = callback_context
    if not ctx.triggered:
        return current_style, {"display": "none"}

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if trigger_id == "genie-close-btn":
        hidden_style = dict(current_style or {})
        hidden_style["width"] = "0px"
        hidden_style["minWidth"] = "0px"
        hidden_style["overflow"] = "hidden"
        hidden_style["borderRight"] = "none"
        return hidden_style, {"display": "block"}
    else:
        visible_style = dict(current_style or {})
        visible_style["width"] = "288px"
        visible_style["minWidth"] = "288px"
        visible_style["overflow"] = "hidden"
        visible_style["borderRight"] = "1px solid #E5E7EB"
        return visible_style, {"display": "none"}


# Active nav highlighting
@app.callback(
    Output({"type": "nav-link", "index": dash.ALL}, "className"),
    Input("url", "pathname"),
    State({"type": "nav-link", "index": dash.ALL}, "id"),
)
def update_active_nav(pathname, nav_ids):
    """Set the active class on the current nav link."""
    if pathname is None or pathname == "/":
        active_id = "dashboard"
    else:
        parts = pathname.strip("/").split("/")
        # URL is /<vertical>/<page_id>; the page_id is the nav target
        active_id = parts[1] if len(parts) > 1 else parts[0]

    classes = []
    for nav_id in nav_ids:
        if nav_id["index"] == active_id:
            classes.append("nav-link active")
        else:
            classes.append("nav-link")
    return classes


# ===================================================================
#  Genie AI Chat Callbacks
# ===================================================================


def _render_chat_messages(history):
    """Convert the chat history list into Dash HTML components for display."""
    elements = []
    for msg in history:
        role = msg.get("role", "user")
        text = msg.get("text", "")

        if role == "user":
            elements.append(
                html.Div(text, className="genie-msg-user")
            )
        elif role == "ai":
            # Parse markdown-style bold (**text**) for display
            parts = []
            remaining = text
            while "**" in remaining:
                before, _, after = remaining.partition("**")
                if "**" in after:
                    bold_text, _, after = after.partition("**")
                    if before:
                        parts.append(before)
                    parts.append(html.Strong(bold_text))
                    remaining = after
                else:
                    parts.append(remaining)
                    remaining = ""
                    break
            if remaining:
                parts.append(remaining)

            ai_children = []
            # Render the text content with line breaks
            if parts:
                # Split by newlines to create proper line breaks
                final_parts = []
                for part in parts:
                    if isinstance(part, str):
                        lines = part.split("\n")
                        for i, line in enumerate(lines):
                            final_parts.append(line)
                            if i < len(lines) - 1:
                                final_parts.append(html.Br())
                    else:
                        final_parts.append(part)
                ai_children.extend(final_parts)
            else:
                lines = text.split("\n")
                for i, line in enumerate(lines):
                    ai_children.append(line)
                    if i < len(lines) - 1:
                        ai_children.append(html.Br())

            elements.append(
                html.Div(ai_children, className="genie-msg-ai")
            )

            # Add SQL block if present
            sql = msg.get("sql")
            if sql:
                elements.append(
                    html.Div([
                        html.Div(
                            "SQL Query",
                            style={
                                "fontSize": "10px",
                                "color": "#9CA3AF",
                                "marginBottom": "4px",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.5px",
                            },
                        ),
                        html.Pre(sql, className="genie-msg-sql"),
                    ])
                )

            # Add source badge
            source = msg.get("source", "demo")
            source_labels = {
                "genie": "Databricks Genie",
                "fm": "Foundation Model",
                "demo": "Demo Mode",
            }
            elements.append(
                html.Div(
                    f"Source: {source_labels.get(source, source)}",
                    className="genie-msg-source",
                )
            )

    return elements


@app.callback(
    Output("genie-response", "children"),
    Output("genie-input", "value"),
    Output("genie-chat-history", "data"),
    Input("genie-send-btn", "n_clicks"),
    Input("genie-input", "n_submit"),
    Input({"type": "genie-q", "index": ALL}, "n_clicks"),
    State("genie-input", "value"),
    State({"type": "genie-q", "index": ALL}, "children"),
    State("genie-chat-history", "data"),
    prevent_initial_call=True,
)
def handle_genie_query(send_clicks, n_submit, q_clicks, input_value, q_labels, chat_history):
    """Handle a Genie chat query from the send button, Enter key, or a question card.

    Determines the question source, calls ask_genie(), and updates the chat display.
    """
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    # Initialize history if needed
    if chat_history is None:
        chat_history = []

    # Determine the question text from the trigger
    trigger = ctx.triggered[0]
    trigger_id = trigger["prop_id"]
    question = None

    if "genie-send-btn" in trigger_id or "genie-input" in trigger_id:
        # Send button or Enter key pressed
        question = input_value
    else:
        # A question card was clicked -- use ctx.triggered_id for reliable detection
        triggered_id = ctx.triggered_id
        if isinstance(triggered_id, dict) and triggered_id.get("type") == "genie-q":
            # Find the matching label by index
            clicked_index = triggered_id.get("index", "")
            for i, label in enumerate(q_labels or []):
                label_text = label if isinstance(label, str) else str(label)
                if label_text[:20] == clicked_index:
                    question = label_text
                    break

        # Fallback: find any card with clicks > 0
        if question is None:
            for i, clicks in enumerate(q_clicks or []):
                if clicks and clicks > 0 and i < len(q_labels):
                    question = q_labels[i] if isinstance(q_labels[i], str) else str(q_labels[i])
                    break

    if not question or (isinstance(question, str) and not question.strip()):
        raise dash.exceptions.PreventUpdate

    question = question.strip()

    # Call the genie backend
    try:
        from app.data_access import _current_use_case
        result = ask_genie(question, _current_use_case())
    except Exception as e:
        result = {
            "answer": f"Sorry, I encountered an error processing your question: {str(e)}",
            "sql": None,
            "source": "error",
            "data": None,
        }

    # Add the exchange to chat history
    chat_history.append({"role": "user", "text": question})
    chat_history.append({
        "role": "ai",
        "text": result.get("answer", "No response available."),
        "sql": result.get("sql"),
        "source": result.get("source", "demo"),
        "data": result.get("data"),
    })

    # Render all chat messages
    rendered = _render_chat_messages(chat_history)

    # Clear input field
    return rendered, "", chat_history


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
