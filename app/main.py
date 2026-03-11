"""
Multi-vertical Dash application entry point -- Blueprint IQ Demo Hub (v3).

Creates the Dash server, loads the layout, registers page callbacks,
and wires up URL routing plus the Genie panel toggle.
Supports verticals: gaming, telecom, media, financial_services, hls,
manufacturing, risk.

The app serves three kinds of pages:
  - Landing  (/)      -- full-screen splash
  - Hub      (/hub)   -- grid of vertical cards
  - Vertical (/<vertical>/<page_id>) -- per-vertical pages
"""

import os
import sys
import json
import re

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so `from app.xxx` imports work
# regardless of the working directory (needed for Databricks Apps runtime).
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Default environment  --  demo mode so the app renders without Databricks
# ---------------------------------------------------------------------------
os.environ.setdefault("USE_DEMO_DATA", "true")
os.environ.setdefault("USE_CASE", "gaming")
os.environ.setdefault("DATABRICKS_FM_ENDPOINT", "databricks-claude-sonnet-4-6")

import dash  # noqa: E402
from dash import Input, Output, State, callback_context, dcc, html, ALL  # noqa: E402

from app.data_access import (  # noqa: E402
    _active_vertical,
    get_config,
    get_config_for,
    set_active_vertical,
)
from app.genie_backend import ask_genie  # noqa: E402
from app.layout import build_layout, build_sidebar  # noqa: E402
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS, get_base_stylesheet  # noqa: E402

# Per-vertical page renderers
from app.pages import gaming as pages_gaming  # noqa: E402
from app.pages import telecom as pages_telecom  # noqa: E402
from app.pages import media as pages_media  # noqa: E402
from app.pages import financial_services as pages_finserv  # noqa: E402
from app.pages import hls as pages_hls  # noqa: E402
from app.pages import manufacturing as pages_manufacturing  # noqa: E402
from app.pages import risk as pages_risk  # noqa: E402

# ---------------------------------------------------------------------------
# App-level constants
# ---------------------------------------------------------------------------

APP_NAME = "Blueprint AI Demo Hub"

# ---------------------------------------------------------------------------
# Vertical metadata for the hub page
# ---------------------------------------------------------------------------

_VERTICALS = [
    {
        "key": "gaming",
        "icon": "fa-gamepad",
        "color": "#FBBF24",
        "stats": [("Tables", "6"), ("Models", "3"), ("KPIs", "17")],
    },
    {
        "key": "telecom",
        "icon": "fa-tower-cell",
        "color": "#34D399",
        "stats": [("Tables", "5"), ("Models", "2"), ("KPIs", "17")],
    },
    {
        "key": "media",
        "icon": "fa-film",
        "color": "#A78BFA",
        "stats": [("Tables", "5"), ("Models", "2"), ("KPIs", "17")],
    },
    {
        "key": "financial_services",
        "icon": "fa-building-columns",
        "color": "#F87171",
        "stats": [("Tables", "7"), ("Models", "3"), ("KPIs", "29")],
    },
    {
        "key": "hls",
        "icon": "fa-heart-pulse",
        "color": "#4B7BF5",
        "stats": [("Tables", "6"), ("Models", "2"), ("KPIs", "22")],
    },
    {
        "key": "manufacturing",
        "icon": "fa-industry",
        "color": "#FB923C",
        "stats": [("Tables", "6"), ("Models", "2"), ("KPIs", "16")],
    },
    {
        "key": "risk",
        "icon": "fa-shield-halved",
        "color": "#F472B6",
        "stats": [("Tables", "5"), ("Models", "2"), ("KPIs", "18")],
    },
]

ALL_VERTICALS = [v["key"] for v in _VERTICALS]

# ---------------------------------------------------------------------------
# Pre-warm all vertical configs at import time
# ---------------------------------------------------------------------------

for _v in ALL_VERTICALS:
    try:
        get_config_for(_v)
    except Exception:
        pass  # Config file may not exist yet during development

# ---------------------------------------------------------------------------
# Create Dash app
# ---------------------------------------------------------------------------

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title=APP_NAME,
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

_ACCENT_ICONS = {
    "blue": "fa-chart-line",
    "purple": "fa-bolt",
    "green": "fa-arrow-trend-up",
    "red": "fa-triangle-exclamation",
    "yellow": "fa-circle-exclamation",
}


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
                    "backgroundColor": "rgba(248, 113, 113, 0.12)", "padding": "2px 6px",
                    "borderRadius": "4px", "letterSpacing": "0.5px",
                },
            ),
        ))
    return html.Div(className="card", style={"position": "relative"}, children=children)


def _detail_row(label, value):
    """Single key-value detail row."""
    return html.Div(
        style={"display": "flex", "justifyContent": "space-between", "padding": "6px 0",
               "borderBottom": f"1px solid {COLORS['border']}"},
        children=[
            html.Span(label, style={"fontSize": "13px", "color": COLORS["text_muted"]}),
            html.Span(str(value), style={"fontSize": "13px", "fontWeight": "500"}),
        ],
    )


# ===================================================================
#  Landing & Hub renderers
# ===================================================================


def _render_landing():
    """Full-screen splash page at /."""
    icon_previews = html.Div(
        style={"display": "flex", "gap": "24px", "marginBottom": "32px"},
        children=[
            html.Div(
                style={
                    "width": "40px", "height": "40px", "borderRadius": "10px",
                    "backgroundColor": f"{v['color']}20",
                    "display": "flex", "alignItems": "center", "justifyContent": "center",
                },
                children=html.I(
                    className=f"fa-solid {v['icon']}",
                    style={"color": v["color"], "fontSize": "16px"},
                ),
            )
            for v in _VERTICALS
        ],
    )

    return html.Div(
        className="landing-overlay",
        children=[
            html.Div(
                className="landing-title",
                children=[
                    html.Span("Blueprint", style={"color": COLORS["blue"]}),
                    " AI Demo Hub",
                ],
            ),
            html.Div(
                "Powered by Databricks Lakehouse | Built by Blueprint Technologies",
                className="landing-subtitle",
            ),
            icon_previews,
            dcc.Link("Explore Demos", href="/hub", className="landing-enter-btn"),
        ],
    )


def _render_hub():
    """Demo hub page with vertical cards at /hub."""
    cards = []
    for v in _VERTICALS:
        try:
            cfg = get_config_for(v["key"])
            title = cfg["app"].get("title", v["key"].replace("_", " ").title())
            subtitle = cfg["app"].get("subtitle", "")
        except Exception:
            title = v["key"].replace("_", " ").title()
            subtitle = ""

        stats_children = [
            html.Div(
                className="vertical-card-stat",
                children=[html.Strong(stat_val), stat_label],
            )
            for stat_label, stat_val in v["stats"]
        ]

        card = dcc.Link(
            href=f"/{v['key']}/dashboard",
            className="vertical-card",
            children=[
                html.Div(
                    className="vertical-card-icon",
                    style={"backgroundColor": f"{v['color']}20"},
                    children=html.I(
                        className=f"fa-solid {v['icon']}",
                        style={"color": v["color"]},
                    ),
                ),
                html.Div(title, className="vertical-card-title"),
                html.Div(subtitle, className="vertical-card-subtitle"),
                html.Div(className="vertical-card-stats", children=stats_children),
            ],
        )
        cards.append(card)

    return html.Div(
        children=[
            html.Div(
                className="hub-header",
                children=[
                    html.Div("Blueprint AI Demo Hub", className="hub-title"),
                    html.Div(
                        "Select an industry vertical to explore",
                        className="hub-subtitle",
                    ),
                ],
            ),
            html.Div(className="hub-grid", children=cards),
        ],
    )


# ===================================================================
#  Architecture & Details  --  generic across verticals
# ===================================================================


def _render_architecture():
    """Lakehouse architecture informational page -- reads config for context."""
    cfg = get_config()
    app_name = cfg["app"]["name"]
    catalog = cfg["app"].get("catalog", "")
    genie_tables = cfg.get("genie", {}).get("tables", [])

    silver_tables = [t.split(".")[-1] for t in genie_tables if ".silver." in t]
    gold_tables = [t.split(".")[-1] for t in genie_tables if ".gold." in t]

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
         ". Optimized for BI and dashboards via the Databricks Lakehouse."),
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
            html.P(f"{app_name} Databricks Lakehouse architecture and ML pipeline overview"),
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
        if "target_metric" in model_info:
            detail_rows.append(_detail_row("Target Metric", model_info["target_metric"]))
        if "target_value" in model_info:
            detail_rows.append(_detail_row("Target Value", str(model_info["target_value"])))

        model_cards.append(
            html.Div(className="card", children=[
                html.Span(card_title, className="card-title"),
                html.Div(style={"marginTop": "12px"}, children=detail_rows),
            ])
        )

    # Data configuration card
    data_cfg = cfg.get("data", {})
    data_rows = [_detail_row("Catalog", cfg["app"].get("catalog", "N/A"))]

    for key in ["game_titles", "regions", "sub_verticals", "technologies",
                "content_types", "platforms", "facilities", "departments",
                "banking_lines", "capital_markets_desks", "insurance_lines",
                "subscriber_segments", "customer_segments", "genres",
                "therapeutic_areas", "medtech_categories"]:
        if key in data_cfg and isinstance(data_cfg[key], list):
            data_rows.append(_detail_row(
                key.replace("_", " ").title(),
                ", ".join(str(v) for v in data_cfg[key])
            ))

    # Metric summaries from data sub-dicts
    for key, value in data_cfg.items():
        if isinstance(value, dict) and key.endswith("_metrics"):
            label = key.replace("_", " ").title()
            for mk, mv in list(value.items())[:4]:
                data_rows.append(_detail_row(f"{label} - {mk}", str(mv)))

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
            _detail_row("Lakehouse Tables", str(len(genie_cfg.get("tables", [])))),
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
                    f"This page displays {label} data from the Databricks Lakehouse. "
                    "Connect to your workspace to see live data.",
                    style={"fontSize": "13px", "color": COLORS["text_muted"], "lineHeight": "1.6"},
                ),
            ]),
        ]),
    ])


# ===================================================================
#  Page router -- maps (vertical, page_id) to renderer
# ===================================================================

# Vertical-specific page renderers: { vertical: { page_id: render_fn(cfg) } }
_VERTICAL_PAGES = {
    "gaming": {
        "dashboard": pages_gaming.render_dashboard,
        "know_player": pages_gaming.render_know_player,
        "grow_playerbase": pages_gaming.render_grow_playerbase,
        "grow_revenue": pages_gaming.render_grow_revenue,
        "build_games": pages_gaming.render_build_games,
        "live_ops": pages_gaming.render_live_ops,
        "efficient_ops": pages_gaming.render_efficient_ops,
    },
    "telecom": {
        "dashboard": pages_telecom.render_dashboard,
        "consumer_cx": pages_telecom.render_consumer_cx,
        "b2b_enterprise": pages_telecom.render_b2b_enterprise,
        "network_ops": pages_telecom.render_network_ops,
        "field_energy": pages_telecom.render_field_energy,
        "fraud_prevention": pages_telecom.render_fraud_prevention,
        "cyber_security": pages_telecom.render_cyber_security,
    },
    "media": {
        "dashboard": pages_media.render_dashboard,
        "content_strategy": pages_media.render_content_strategy,
        "audience_intel": pages_media.render_audience_intel,
        "subscription_intel": pages_media.render_subscription_intel,
        "ad_yield": pages_media.render_ad_yield,
        "platform_delivery": pages_media.render_platform_delivery,
        "personalization_ai": pages_media.render_personalization_ai,
    },
    "financial_services": {
        "dashboard": pages_finserv.render_dashboard,
        "investment_alpha": pages_finserv.render_investment_alpha,
        "trading_advisory": pages_finserv.render_trading_advisory,
        "risk_management": pages_finserv.render_risk_management,
        "regulatory": pages_finserv.render_regulatory,
        "fraud_cyber": pages_finserv.render_fraud_cyber,
        "operations": pages_finserv.render_operations,
    },
    "hls": {
        "dashboard": pages_hls.render_dashboard,
        "healthcare_ops": pages_hls.render_healthcare_ops,
        "network_quality": pages_hls.render_network_quality,
        "biopharma_intel": pages_hls.render_biopharma_intel,
        "supply_chain": pages_hls.render_supply_chain,
        "medtech_surgery": pages_hls.render_medtech_surgery,
        "patient_outcomes": pages_hls.render_patient_outcomes,
    },
    "manufacturing": {
        "dashboard": pages_manufacturing.render_dashboard,
        "production_analytics": pages_manufacturing.render_production_analytics,
        "quality_control": pages_manufacturing.render_quality_control,
        "supply_chain": pages_manufacturing.render_supply_chain,
        "predictive_maintenance": pages_manufacturing.render_predictive_maintenance,
        "energy_sustainability": pages_manufacturing.render_energy_sustainability,
        "workforce_ops": pages_manufacturing.render_workforce_ops,
    },
    "risk": {
        "dashboard": pages_risk.render_dashboard,
        "enterprise_risk": pages_risk.render_enterprise_risk,
        "credit_risk": pages_risk.render_credit_risk,
        "market_risk": pages_risk.render_market_risk,
        "operational_risk": pages_risk.render_operational_risk,
        "compliance": pages_risk.render_compliance,
        "cyber_risk": pages_risk.render_cyber_risk,
    },
}

# Shared pages available across all verticals
_SHARED_PAGES = {
    "architecture": _render_architecture,
    "details": _render_details,
}


def _get_renderer(vertical, page_id):
    """Look up the renderer for a vertical + page id."""
    # Check shared pages first
    if page_id in _SHARED_PAGES:
        return lambda: _SHARED_PAGES[page_id]()

    # Check vertical-specific pages
    vertical_pages = _VERTICAL_PAGES.get(vertical, {})
    if page_id in vertical_pages:
        render_fn = vertical_pages[page_id]
        cfg = get_config_for(vertical)
        return lambda: render_fn(cfg)

    # Fallback to generic
    return lambda: _render_generic_page(page_id)


# ===================================================================
#  Callbacks
# ===================================================================


@app.callback(
    Output("page-content", "children"),
    Output("sidebar-container", "children"),
    Output("sidebar-container", "style"),
    Output("active-vertical", "data"),
    Input("url", "pathname"),
)
def route_page(pathname):
    """Route URL to the correct page and update sidebar per vertical."""
    if pathname is None or pathname == "/" or pathname == "":
        return (_render_hub(), [], {"display": "none"}, None)

    parts = pathname.strip("/").split("/")

    if parts[0] == "hub":
        return (_render_hub(), [], {"display": "none"}, None)

    vertical = parts[0]
    page_id = parts[1] if len(parts) > 1 else "dashboard"

    if vertical not in ALL_VERTICALS:
        page_id = vertical
        vertical = "gaming"

    set_active_vertical(vertical)
    renderer = _get_renderer(vertical, page_id)

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

    sidebar_children = build_sidebar(vertical, page_id)
    sidebar_style = {"width": "220px", "minWidth": "220px", "flexShrink": "0", "position": "relative"}

    return (content, sidebar_children, sidebar_style, vertical)


@app.callback(
    Output("chat-modal", "style"),
    Output("chat-fab", "style"),
    Input("chat-fab", "n_clicks"),
    Input("chat-close-btn", "n_clicks"),
    State("chat-modal", "style"),
    prevent_initial_call=True,
)
def toggle_chat_modal(fab_clicks, close_clicks, current_style):
    """Toggle the floating chat modal open/closed."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    is_open = (current_style or {}).get("display") == "flex"

    fab_base = {
        "position": "fixed",
        "bottom": "24px",
        "right": "24px",
        "width": "56px",
        "height": "56px",
        "borderRadius": "50%",
        "backgroundColor": COLORS["blue"],
        "color": "#FFFFFF",
        "border": "none",
        "cursor": "pointer",
        "display": "flex",
        "alignItems": "center",
        "justifyContent": "center",
        "boxShadow": "0 4px 16px rgba(75, 123, 245, 0.4)",
        "zIndex": "1000",
        "transition": "transform 0.2s ease, box-shadow 0.2s ease",
    }

    modal_base = dict(current_style or {})

    if trigger_id == "chat-close-btn" or (trigger_id == "chat-fab" and is_open):
        # Close
        modal_base["display"] = "none"
        return modal_base, fab_base
    else:
        # Open
        modal_base["display"] = "flex"
        fab_base["display"] = "none"
        return modal_base, fab_base


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


def _bold_parts(text):
    """Parse **bold** markers in a string, returning a list of str/html.Strong."""
    parts = []
    remaining = text
    while "**" in remaining:
        before, _, after = remaining.partition("**")
        if "**" in after:
            bold, _, after = after.partition("**")
            if before:
                parts.append(before)
            parts.append(html.Strong(bold))
            remaining = after
        else:
            parts.append(remaining)
            remaining = ""
            break
    if remaining:
        parts.append(remaining)
    return parts if parts else [text]


def _parse_ai_text(text):
    """Parse AI response text into Dash components with bullet/numbered list support."""
    lines = text.split("\n")
    result = []
    list_items = []
    is_ordered = False

    def flush_list():
        nonlocal list_items, is_ordered
        if not list_items:
            return
        tag = html.Ol if is_ordered else html.Ul
        result.append(tag(
            [html.Li(_bold_parts(item)) for item in list_items],
            style={"margin": "4px 0", "paddingLeft": "20px", "fontSize": "13px"},
        ))
        list_items = []

    for line in lines:
        s = line.strip()

        # Bullet list item
        if re.match(r'^[-*•]\s+', s):
            if is_ordered and list_items:
                flush_list()
            is_ordered = False
            list_items.append(re.sub(r'^[-*•]\s+', '', s))
            continue

        # Numbered list item
        m = re.match(r'^(\d+)\.\s+(.+)', s)
        if m:
            if not is_ordered and list_items:
                flush_list()
            is_ordered = True
            list_items.append(m.group(2))
            continue

        # Non-list line
        flush_list()
        if s:
            result.extend(_bold_parts(s))
            result.append(html.Br())
        elif result:
            result.append(html.Br())

    flush_list()

    # Remove trailing <br>
    while result and isinstance(result[-1], html.Br):
        result.pop()

    return result


def _render_chat_messages(history):
    """Convert the chat history list into Dash HTML components for display."""
    elements = []
    for msg in history:
        role = msg.get("role", "user")
        text = msg.get("text", "")

        if role == "user":
            elements.append(html.Div(text, className="genie-msg-user"))
        elif role == "ai":
            ai_children = _parse_ai_text(text)
            elements.append(html.Div(ai_children, className="genie-msg-ai"))

            sql = msg.get("sql")
            if sql:
                elements.append(
                    html.Div([
                        html.Div(
                            "SQL Query",
                            style={
                                "fontSize": "10px", "color": "#9CA3AF", "marginBottom": "4px",
                                "textTransform": "uppercase", "letterSpacing": "0.5px",
                            },
                        ),
                        html.Pre(sql, className="genie-msg-sql"),
                    ])
                )

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
    State("genie-input", "value"),
    State("genie-chat-history", "data"),
    State("active-vertical", "data"),
    prevent_initial_call=True,
)
def handle_genie_query(send_clicks, n_submit, input_value, chat_history, active_vertical):
    """Handle a Genie chat query from the send button or Enter key."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    if chat_history is None:
        chat_history = []

    question = input_value
    if not question or (isinstance(question, str) and not question.strip()):
        raise dash.exceptions.PreventUpdate

    question = question.strip()

    try:
        result = ask_genie(question, active_vertical or "gaming")
    except Exception as e:
        result = {
            "answer": f"Sorry, I encountered an error processing your question: {str(e)}",
            "sql": None,
            "source": "error",
            "data": None,
        }

    chat_history.append({"role": "user", "text": question})
    chat_history.append({
        "role": "ai",
        "text": result.get("answer", "No response available."),
        "sql": result.get("sql"),
        "source": result.get("source", "demo"),
        "data": result.get("data"),
    })

    rendered = _render_chat_messages(chat_history)
    return rendered, "", chat_history


# ===================================================================
#  Genie welcome & sample question callbacks
# ===================================================================


def _render_welcome(app_name, sample_questions):
    """Render welcome message with clickable sample question chips."""
    children = [
        html.Div(
            style={"textAlign": "center", "padding": "20px 8px 16px"},
            children=[
                html.I(
                    className="fa-solid fa-robot",
                    style={"fontSize": "28px", "color": COLORS["blue"],
                           "marginBottom": "8px", "display": "block"},
                ),
                html.Div(
                    f"Ask me about {app_name}",
                    style={"fontSize": "15px", "fontWeight": "600",
                           "color": "#1F2937", "marginBottom": "4px"},
                ),
                html.Div(
                    "Click a question below or type your own",
                    style={"fontSize": "12px", "color": "#9CA3AF"},
                ),
            ],
        ),
    ]
    for i, q in enumerate(sample_questions):
        children.append(
            html.Button(
                q,
                id={"type": "sample-q", "index": i},
                n_clicks=0,
                className="sample-question-chip",
            )
        )
    return children


@app.callback(
    Output("genie-response", "children", allow_duplicate=True),
    Output("genie-chat-history", "data", allow_duplicate=True),
    Output("genie-current-vertical", "data"),
    Input("active-vertical", "data"),
    State("genie-current-vertical", "data"),
    prevent_initial_call=True,
)
def update_genie_welcome(active_vertical, current_genie_vertical):
    """Reset chat and show welcome with sample questions when vertical changes."""
    if not active_vertical:
        raise dash.exceptions.PreventUpdate

    # Only reset when the vertical actually changes
    if active_vertical == current_genie_vertical:
        raise dash.exceptions.PreventUpdate

    cfg = get_config_for(active_vertical)
    app_name = cfg["app"].get("name", active_vertical.replace("_", " ").title())
    questions = cfg.get("genie", {}).get("sample_questions", [])[:6]

    return _render_welcome(app_name, questions), [], active_vertical


@app.callback(
    Output("genie-input", "value", allow_duplicate=True),
    Input({"type": "sample-q", "index": ALL}, "n_clicks"),
    State("active-vertical", "data"),
    prevent_initial_call=True,
)
def fill_sample_question(n_clicks_list, active_vertical):
    """Fill the chat input when a sample question chip is clicked."""
    ctx = callback_context
    if not ctx.triggered or not any(n for n in n_clicks_list if n):
        raise dash.exceptions.PreventUpdate

    prop_id = ctx.triggered[0]["prop_id"]
    idx = json.loads(prop_id.rsplit(".", 1)[0])["index"]

    vertical = active_vertical or "gaming"
    cfg = get_config_for(vertical)
    questions = cfg.get("genie", {}).get("sample_questions", [])

    if idx < len(questions):
        return questions[idx]
    raise dash.exceptions.PreventUpdate


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
