"""
Multi-vertical Dash application entry point -- Blueprint IQ Demo Hub (v2).

Creates the Dash server, loads the layout, registers page callbacks,
and wires up URL routing plus the Genie panel toggle.
Supports verticals: gaming, telecom, media, financial_services, hls.

The app serves three kinds of pages:
  - Landing  (/)      -- full-screen splash
  - Hub      (/hub)   -- grid of vertical cards
  - Vertical (/<vertical>/<page_id>) -- per-vertical pages
"""

import os
import sys

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
from app.layout import build_layout, build_sidebar, build_genie_panel  # noqa: E402
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS, get_base_stylesheet  # noqa: E402

# Per-vertical page renderers
from app.pages import gaming as pages_gaming  # noqa: E402
from app.pages import telecom as pages_telecom  # noqa: E402
from app.pages import media as pages_media  # noqa: E402
from app.pages import financial_services as pages_finserv  # noqa: E402
from app.pages import hls as pages_hls  # noqa: E402

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
        "color": "#EAB308",
        "stats": [("Tables", "6"), ("Models", "3"), ("KPIs", "17")],
    },
    {
        "key": "telecom",
        "icon": "fa-tower-cell",
        "color": "#22C55E",
        "stats": [("Tables", "5"), ("Models", "2"), ("KPIs", "17")],
    },
    {
        "key": "media",
        "icon": "fa-film",
        "color": "#8B5CF6",
        "stats": [("Tables", "5"), ("Models", "2"), ("KPIs", "17")],
    },
    {
        "key": "financial_services",
        "icon": "fa-building-columns",
        "color": "#EF4444",
        "stats": [("Tables", "7"), ("Models", "3"), ("KPIs", "29")],
    },
    {
        "key": "hls",
        "icon": "fa-heart-pulse",
        "color": "#233ED8",
        "stats": [("Tables", "6"), ("Models", "2"), ("KPIs", "22")],
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
                    "backgroundColor": "rgba(239, 68, 68, 0.12)", "padding": "2px 6px",
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
                    html.Span("Blueprint", style={"color": "#233ED8"}),
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
        "player_intel": pages_gaming.render_player_intel,
        "revenue": pages_gaming.render_revenue,
        "ua_growth": pages_gaming.render_ua_growth,
        "game_dev": pages_gaming.render_game_dev,
        "infrastructure": pages_gaming.render_infrastructure,
    },
    "telecom": {
        "dashboard": pages_telecom.render_dashboard,
        "customer": pages_telecom.render_customer,
        "revenue": pages_telecom.render_revenue,
        "fraud": pages_telecom.render_fraud,
        "field_ops": pages_telecom.render_field_ops,
        "b2b_iot": pages_telecom.render_b2b_iot,
    },
    "media": {
        "dashboard": pages_media.render_dashboard,
        "content": pages_media.render_content,
        "subscriptions": pages_media.render_subscriptions,
        "advertising": pages_media.render_advertising,
        "creative": pages_media.render_creative,
        "platform": pages_media.render_platform,
    },
    "financial_services": {
        "dashboard": pages_finserv.render_dashboard,
        "banking": pages_finserv.render_banking,
        "capital_markets": pages_finserv.render_capital_markets,
        "insurance": pages_finserv.render_insurance,
        "fraud_compliance": pages_finserv.render_fraud_compliance,
        "customer": pages_finserv.render_customer,
    },
    "hls": {
        "dashboard": pages_hls.render_dashboard,
        "provider_ops": pages_hls.render_provider_ops,
        "clinical_quality": pages_hls.render_clinical_quality,
        "health_plans": pages_hls.render_health_plans,
        "biopharma": pages_hls.render_biopharma,
        "medtech": pages_hls.render_medtech,
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
    Output("genie-panel-container", "children"),
    Output("genie-panel-container", "style"),
    Output("active-vertical", "data"),
    Input("url", "pathname"),
    Input("interval-refresh", "n_intervals"),
)
def route_page(pathname, n_intervals):
    """Route URL to the correct page, updating sidebar and genie panel per vertical."""
    if pathname is None or pathname == "/":
        return (_render_landing(), [], {"display": "none"}, [], {"display": "none"}, None)

    parts = pathname.strip("/").split("/")

    if parts[0] == "hub":
        return (_render_hub(), [], {"display": "none"}, [], {"display": "none"}, None)

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
    genie_children = build_genie_panel(vertical)

    sidebar_style = {"width": "220px", "minWidth": "220px", "flexShrink": "0", "position": "relative"}
    genie_style = {"width": "280px", "minWidth": "280px", "flexShrink": "0"}

    return (
        content,
        sidebar_children,
        sidebar_style,
        genie_children,
        genie_style,
        vertical,
    )


@app.callback(
    Output("genie-panel-container", "style", allow_duplicate=True),
    Output("genie-open-wrapper", "style"),
    Input("genie-close-btn", "n_clicks"),
    Input("genie-open-btn", "n_clicks"),
    State("genie-panel-container", "style"),
    prevent_initial_call=True,
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
        visible_style["width"] = "280px"
        visible_style["minWidth"] = "280px"
        visible_style["overflow"] = "hidden"
        visible_style["borderRight"] = "1px solid #E5E7EB"
        return visible_style, {"display": "none"}


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


def _render_chat_messages(history):
    """Convert the chat history list into Dash HTML components for display."""
    elements = []
    for msg in history:
        role = msg.get("role", "user")
        text = msg.get("text", "")

        if role == "user":
            elements.append(html.Div(text, className="genie-msg-user"))
        elif role == "ai":
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
            if parts:
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
    Input({"type": "genie-q", "index": ALL}, "n_clicks"),
    State("genie-input", "value"),
    State({"type": "genie-q", "index": ALL}, "children"),
    State("genie-chat-history", "data"),
    State("active-vertical", "data"),
    prevent_initial_call=True,
)
def handle_genie_query(send_clicks, n_submit, q_clicks, input_value, q_labels, chat_history, active_vertical):
    """Handle a Genie chat query from the send button, Enter key, or a question card."""
    ctx = callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    if chat_history is None:
        chat_history = []

    trigger = ctx.triggered[0]
    trigger_id = trigger["prop_id"]
    question = None

    if "genie-send-btn" in trigger_id or "genie-input" in trigger_id:
        question = input_value
    else:
        triggered_id = ctx.triggered_id
        if isinstance(triggered_id, dict) and triggered_id.get("type") == "genie-q":
            clicked_index = triggered_id.get("index", "")
            for i, label in enumerate(q_labels or []):
                label_text = label if isinstance(label, str) else str(label)
                if label_text[:20] == clicked_index:
                    question = label_text
                    break

        if question is None:
            for i, clicks in enumerate(q_clicks or []):
                if clicks and clicks > 0 and i < len(q_labels):
                    question = q_labels[i] if isinstance(q_labels[i], str) else str(q_labels[i])
                    break

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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
