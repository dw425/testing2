"""
Multi-vertical Dash application entry point -- Blueprint Demo Hub (v4).

Creates the Dash server, loads the layout, registers page callbacks,
and wires up URL routing plus the Genie panel toggle.
Supports verticals: gaming, telecom, media, financial_services, hls,
manufacturing, risk.

The app serves three kinds of pages:
  - Landing  (/)      -- full-screen splash
  - Hub      (/hub)   -- grid of vertical cards
  - Vertical (/<vertical>/<page_id>) -- per-vertical pages
"""

import json
import logging
import os
import re
import sys

logger = logging.getLogger(__name__)

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
from app.page_styles import _hex_to_rgb, _data_table_counter  # noqa: E402
from app.theme import COLORS, get_base_stylesheet  # noqa: E402

# Per-vertical page renderers
from app.pages import gaming as pages_gaming  # noqa: E402
from app.pages import telecom as pages_telecom  # noqa: E402
from app.pages import media as pages_media  # noqa: E402
from app.pages import financial_services as pages_finserv  # noqa: E402
from app.pages import hls as pages_hls  # noqa: E402
from app.pages import manufacturing as pages_manufacturing  # noqa: E402
from app.pages import risk as pages_risk  # noqa: E402
from app.pages import retail as pages_retail  # noqa: E402
from app.pages import customer_support as pages_support  # noqa: E402
from app.pages import real_estate as pages_realestate  # noqa: E402
from app.pages import energy as pages_energy  # noqa: E402

# ---------------------------------------------------------------------------
# App-level constants
# ---------------------------------------------------------------------------

APP_NAME = "Blueprint Demo Hub"

# ---------------------------------------------------------------------------
# Vertical metadata for the hub page
# ---------------------------------------------------------------------------

_VERTICALS = [
    {
        "key": "gaming",
        "icon": "fa-gamepad",
        "color": "#00E5FF",
        "north_star": "Contribution Margin: 68.4%",
        "health": "green",
        "stats": [("Strategic Views", "7"), ("AI Models", "3"), ("North Stars", "4")],
        "tagline": "Player economics, retention strategy, and live ops intelligence",
    },
    {
        "key": "telecom",
        "icon": "fa-tower-cell",
        "color": "#0091D5",
        "north_star": "Customer Lifetime Value: $847",
        "health": "green",
        "stats": [("Strategic Views", "7"), ("AI Models", "2"), ("North Stars", "4")],
        "tagline": "Subscriber value, network ROI, and service reliability",
    },
    {
        "key": "media",
        "icon": "fa-film",
        "color": "#7C4DFF",
        "north_star": "Content ROI: 2.4x",
        "health": "green",
        "stats": [("Strategic Views", "7"), ("AI Models", "2"), ("North Stars", "4")],
        "tagline": "Content economics, audience intelligence, and ad yield optimization",
    },
    {
        "key": "financial_services",
        "icon": "fa-building-columns",
        "color": "#43A047",
        "north_star": "Net Interest Margin: 3.42%",
        "health": "amber",
        "stats": [("Strategic Views", "7"), ("AI Models", "3"), ("North Stars", "5")],
        "tagline": "Capital efficiency, risk-adjusted returns, and regulatory compliance",
    },
    {
        "key": "hls",
        "icon": "fa-heart-pulse",
        "color": "#DC2626",
        "north_star": "Operating Margin: 12.4%",
        "health": "amber",
        "stats": [("Strategic Views", "7"), ("AI Models", "2"), ("North Stars", "4")],
        "tagline": "Margin resilience, labor stability, and patient outcomes",
    },
    {
        "key": "manufacturing",
        "icon": "fa-industry",
        "color": "#FF6D00",
        "north_star": "OEE: 87.4%",
        "health": "green",
        "stats": [("Strategic Views", "7"), ("AI Models", "2"), ("North Stars", "4")],
        "tagline": "Equipment effectiveness, supply chain, and shop-floor intelligence",
    },
    {
        "key": "risk",
        "icon": "fa-shield-halved",
        "color": "#1565C0",
        "north_star": "Risk Exposure Score: 72/100",
        "health": "amber",
        "stats": [("Strategic Views", "7"), ("AI Models", "2"), ("North Stars", "4")],
        "tagline": "Predictive risk, compliance governance, and cyber resilience",
    },
    {
        "key": "retail",
        "icon": "fa-store",
        "color": "#00E5FF",
        "north_star": "Gross Margin: 42.8%",
        "health": "green",
        "stats": [("Strategic Views", "7"), ("AI Models", "3"), ("North Stars", "4")],
        "tagline": "Omnichannel commerce, merchandising, and customer intelligence",
    },
    {
        "key": "customer_support",
        "icon": "fa-headset",
        "color": "#26A69A",
        "north_star": "CSAT: 4.6 / 5.0",
        "health": "green",
        "stats": [("Strategic Views", "7"), ("AI Models", "3"), ("North Stars", "4")],
        "tagline": "Contact center analytics, agent performance, and customer health",
    },
    {
        "key": "real_estate",
        "icon": "fa-building",
        "color": "#8D6E63",
        "north_star": "Portfolio NOI: $284M",
        "health": "amber",
        "stats": [("Strategic Views", "7"), ("AI Models", "3"), ("North Stars", "4")],
        "tagline": "Property analytics, portfolio intelligence, and market insights",
    },
    {
        "key": "energy",
        "icon": "fa-bolt",
        "color": "#FFB300",
        "north_star": "System Reliability: 99.97%",
        "health": "green",
        "stats": [("Strategic Views", "7"), ("AI Models", "3"), ("North Stars", "4")],
        "tagline": "Grid operations, asset performance, and energy transition",
    },
]

ALL_VERTICALS = [v["key"] for v in _VERTICALS]

# ---------------------------------------------------------------------------
# Pre-warm all vertical configs at import time
# ---------------------------------------------------------------------------

for _v in ALL_VERTICALS:
    try:
        get_config_for(_v)
    except FileNotFoundError:
        logger.warning("Config file not found for vertical: %s", _v)
    except Exception as _e:
        logger.warning("Failed to load config for %s: %s", _v, _e)

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
        "https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600;700&display=swap",
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
            html.Img(
                src="/assets/blueprint-logo.jpg",
                style={"height": "52px", "marginBottom": "12px"},
            ),
            html.Div(
                className="landing-title",
                children="Demo Hub",
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
    """Demo hub page with strategic command center cards at /hub."""
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

        # Health status pulse indicator
        health = v.get("health", "green")
        health_pulse = html.Div(
            className=f"status-pulse status-pulse-{health}",
            style={"marginLeft": "auto"},
        )

        # North Star metric preview
        north_star = v.get("north_star", "")
        tagline = v.get("tagline", subtitle)

        card = dcc.Link(
            href=f"/{v['key']}/dashboard",
            className="vertical-card",
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "10px",
                           "marginBottom": "10px"},
                    children=[
                        html.Div(
                            className="vertical-card-icon",
                            style={"backgroundColor": f"{v['color']}20",
                                   "marginBottom": "0", "flexShrink": "0"},
                            children=html.I(
                                className=f"fa-solid {v['icon']}",
                                style={"color": v["color"]},
                            ),
                        ),
                        html.Div(style={"flex": "1"}, children=[
                            html.Div(title, className="vertical-card-title",
                                     style={"marginBottom": "0"}),
                        ]),
                        health_pulse,
                    ],
                ),
                # Strategic tagline
                html.Div(tagline, className="vertical-card-subtitle",
                          style={"marginBottom": "10px"}),
                # North Star metric preview
                html.Div(
                    style={"backgroundColor": f"{v['color']}10",
                           "borderLeft": f"3px solid {v['color']}",
                           "borderRadius": "6px", "padding": "8px 12px",
                           "marginBottom": "12px"},
                    children=[
                        html.Div("NORTH STAR", style={
                            "fontSize": "9px", "fontWeight": "700",
                            "color": COLORS["text_muted"], "letterSpacing": "1px",
                            "marginBottom": "3px"}),
                        html.Div(north_star, style={
                            "fontSize": "14px", "fontWeight": "700",
                            "color": v["color"]}),
                    ],
                ),
                html.Div(className="vertical-card-stats", children=stats_children),
            ],
        )
        cards.append(card)

    # ── "Coming Soon" placeholder card ──────────────────────────────
    coming_soon = html.Div(
        className="vertical-card",
        style={"borderStyle": "dashed", "borderColor": COLORS["border"],
               "display": "flex", "flexDirection": "column",
               "alignItems": "center", "justifyContent": "center",
               "textAlign": "center", "opacity": "0.6",
               "cursor": "default", "minHeight": "180px"},
        children=[
            html.Div(
                style={"width": "42px", "height": "42px", "borderRadius": "10px",
                       "backgroundColor": f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
                       "display": "flex", "alignItems": "center",
                       "justifyContent": "center", "marginBottom": "12px"},
                children=[
                    html.I(className="fa-solid fa-plus",
                           style={"color": COLORS["text_muted"], "fontSize": "18px"}),
                ],
            ),
            html.Div("Coming Soon",
                      style={"fontSize": "15px", "fontWeight": "700",
                             "color": COLORS["text_muted"], "marginBottom": "4px"}),
            html.Div("New vertical in development",
                      style={"fontSize": "12px", "color": COLORS["text_muted"],
                             "opacity": "0.7"}),
        ],
    )
    cards.append(coming_soon)

    return html.Div(
        children=[
            html.Div(
                className="hub-header",
                children=[
                    html.Div(
                        className="hub-title",
                        style={"display": "flex", "alignItems": "baseline",
                               "justifyContent": "center", "gap": "12px"},
                        children=[
                            html.Img(src="/assets/blueprint-logo.jpg",
                                     style={"height": "38px", "alignSelf": "center"}),
                            html.Span("Demo Hub", style={"fontSize": "0.7em",
                                                          "fontWeight": "700",
                                                          "opacity": "0.85",
                                                          "paddingLeft": "4px",
                                                          "position": "relative",
                                                          "top": "5px"}),
                        ],
                    ),
                    html.Div(
                        "Strategic Command Centers — Select a vertical to explore executive intelligence",
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
    """Architecture page with visual diagrams: Lakehouse, Databricks platform,
    App architecture, and Data Model."""
    cfg = get_config()
    app_name = cfg["app"]["name"]
    catalog = cfg["app"].get("catalog", "")
    genie_tables = cfg.get("genie", {}).get("tables", [])

    # Normalize table entries — may be strings ("catalog.schema.table") or dicts ({"name": ...})
    table_names = []
    for t in genie_tables:
        if isinstance(t, str):
            table_names.append(t)
        elif isinstance(t, dict):
            table_names.append(t.get("name", ""))
    silver_tables = [t.split(".")[-1] for t in table_names if ".silver." in t]
    gold_tables = [t.split(".")[-1] for t in table_names if ".gold." in t]

    ml_cfg = cfg.get("ml", {})
    model_names = []
    for key, model in ml_cfg.items():
        if isinstance(model, dict) and "name" in model:
            model_names.append(model["name"])

    # ── Shared styles ──────────────────────────────────────────────────────
    _box = lambda color, bg=None: {
        "border": f"2px solid {color}",
        "borderRadius": "10px",
        "padding": "14px 18px",
        "backgroundColor": bg or f"rgba({_hex_to_rgb(color)}, 0.06)",
        "textAlign": "center",
        "flex": "1",
        "minWidth": "120px",
    }
    _arrow_down = lambda: html.Div(
        html.I(className="fa-solid fa-arrow-down",
               style={"color": COLORS["text_muted"], "fontSize": "18px"}),
        style={"textAlign": "center", "padding": "6px 0"},
    )
    _arrow_right = lambda: html.Div(
        html.I(className="fa-solid fa-arrow-right",
               style={"color": COLORS["text_muted"], "fontSize": "16px"}),
        style={"display": "flex", "alignItems": "center", "padding": "0 6px"},
    )
    _section_title = lambda icon, text, color: html.Div(
        style={"display": "flex", "alignItems": "center", "gap": "10px", "marginBottom": "16px"},
        children=[
            html.Div(
                style={"width": "32px", "height": "32px", "borderRadius": "8px",
                       "backgroundColor": f"rgba({_hex_to_rgb(color)}, 0.15)",
                       "display": "flex", "alignItems": "center", "justifyContent": "center"},
                children=html.I(className=f"fa-solid {icon}",
                                style={"color": color, "fontSize": "14px"}),
            ),
            html.Span(text, style={"fontSize": "16px", "fontWeight": "700",
                                    "color": COLORS["white"]}),
        ],
    )
    _label = lambda text, color=COLORS["white"]: html.Div(
        text, style={"fontSize": "13px", "fontWeight": "600", "color": color}
    )
    _sublabel = lambda text: html.Div(
        text, style={"fontSize": "11px", "color": COLORS["text_muted"], "marginTop": "2px"}
    )

    # ═══════════════════════════════════════════════════════════════════════
    #  1. LAKEHOUSE ARCHITECTURE (Bronze → Silver → Gold)
    # ═══════════════════════════════════════════════════════════════════════
    bronze_tables = ["raw_events", "raw_transactions", "raw_user_profiles", "raw_system_logs"]
    silver_display = silver_tables or ["enriched_events", "clean_profiles", "deduped_transactions"]
    gold_display = gold_tables or ["kpi_aggregates", "retention_cohorts", "revenue_summary"]

    def _layer_box(title, icon, color, tables):
        return html.Div(style=_box(color), children=[
            html.I(className=f"fa-solid {icon}",
                   style={"fontSize": "20px", "color": color, "marginBottom": "6px", "display": "block"}),
            _label(title, color),
            html.Div(style={"marginTop": "8px"},
                     children=[html.Div(t, style={"fontSize": "11px", "color": COLORS["text_muted"],
                                                   "fontFamily": "monospace", "padding": "2px 0"})
                               for t in tables[:4]]),
        ])

    lakehouse_diagram = html.Div(className="card", style={"padding": "24px"}, children=[
        _section_title("fa-layer-group", "Lakehouse Architecture", COLORS["blue"]),
        # Source → Bronze → Silver → Gold flow
        html.Div(style={"display": "flex", "alignItems": "stretch", "gap": "0", "flexWrap": "wrap",
                         "justifyContent": "center"},
                 children=[
            html.Div(style=_box(COLORS["text_muted"]), children=[
                html.I(className="fa-solid fa-cloud-arrow-down",
                       style={"fontSize": "20px", "color": COLORS["text_muted"],
                              "marginBottom": "6px", "display": "block"}),
                _label("Source Systems", COLORS["text_muted"]),
                _sublabel("APIs, Streams, Files"),
            ]),
            _arrow_right(),
            _layer_box("Bronze", "fa-database", COLORS["yellow"], bronze_tables),
            _arrow_right(),
            _layer_box("Silver", "fa-filter", COLORS["blue"], silver_display),
            _arrow_right(),
            _layer_box("Gold", "fa-gem", COLORS["green"], gold_display),
            _arrow_right(),
            html.Div(style=_box(COLORS["purple"]), children=[
                html.I(className="fa-solid fa-chart-pie",
                       style={"fontSize": "20px", "color": COLORS["purple"],
                              "marginBottom": "6px", "display": "block"}),
                _label("BI & Dashboards", COLORS["purple"]),
                _sublabel("SQL Analytics, Apps"),
            ]),
        ]),
        html.Div(style={"marginTop": "12px", "padding": "10px 16px",
                         "backgroundColor": f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.06)",
                         "borderRadius": "8px", "borderLeft": f"3px solid {COLORS['blue']}"},
                 children=[
            html.Div(f"Catalog: {catalog}", style={"fontSize": "12px", "color": COLORS["text_muted"],
                                                     "fontFamily": "monospace"}),
            html.Div("Delta Lake format · Unity Catalog governance · Auto Loader ingestion",
                     style={"fontSize": "11px", "color": COLORS["text_muted"], "marginTop": "4px"}),
        ]),
    ])

    # ═══════════════════════════════════════════════════════════════════════
    #  2. DATABRICKS PLATFORM ARCHITECTURE
    # ═══════════════════════════════════════════════════════════════════════
    platform_services = [
        ("Unity Catalog", "fa-shield-halved", COLORS["green"]),
        ("SQL Warehouse", "fa-database", COLORS["blue"]),
        ("ML Runtime", "fa-brain", COLORS["purple"]),
        ("Model Serving", "fa-rocket", COLORS["red"]),
        ("Workflows", "fa-gears", COLORS["yellow"]),
        ("Genie AI", "fa-robot", COLORS["blue"]),
    ]

    platform_diagram = html.Div(className="card", style={"padding": "24px"}, children=[
        _section_title("fa-cubes", "Databricks Platform", COLORS["purple"]),
        # Control plane
        html.Div(style={"border": f"1px solid {COLORS['border']}", "borderRadius": "10px",
                         "padding": "16px", "marginBottom": "12px"},
                 children=[
            html.Div("Control Plane", style={"fontSize": "11px", "fontWeight": "700",
                                               "color": COLORS["text_muted"], "textTransform": "uppercase",
                                               "letterSpacing": "1px", "marginBottom": "12px",
                                               "textAlign": "center"}),
            html.Div(style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "justifyContent": "center"},
                     children=[
                html.Div(style={"textAlign": "center", "padding": "10px 14px",
                                 "backgroundColor": f"rgba({_hex_to_rgb(color)}, 0.08)",
                                 "borderRadius": "8px", "border": f"1px solid {color}30",
                                 "minWidth": "100px"},
                         children=[
                    html.I(className=f"fa-solid {icon}",
                           style={"fontSize": "16px", "color": color, "display": "block",
                                  "marginBottom": "4px"}),
                    html.Div(name, style={"fontSize": "11px", "fontWeight": "600",
                                           "color": COLORS["white"]}),
                ]) for name, icon, color in platform_services
            ]),
        ]),
        _arrow_down(),
        # Compute layer
        html.Div(style={"display": "flex", "gap": "12px", "justifyContent": "center"},
                 children=[
            html.Div(style=_box(COLORS["blue"]), children=[
                _label("Serverless SQL"),
                _sublabel("Ad-hoc queries"),
            ]),
            html.Div(style=_box(COLORS["purple"]), children=[
                _label("Job Clusters"),
                _sublabel("ETL pipelines"),
            ]),
            html.Div(style=_box(COLORS["green"]), children=[
                _label("Model Endpoints"),
                _sublabel("Real-time inference"),
            ]),
        ]),
        _arrow_down(),
        # Storage
        html.Div(style={**_box(COLORS["yellow"]), "flex": "none"},
                 children=[
            html.I(className="fa-solid fa-hard-drive",
                   style={"fontSize": "18px", "color": COLORS["yellow"],
                          "marginRight": "8px"}),
            html.Span("Cloud Object Storage (Delta Lake)",
                      style={"fontSize": "13px", "fontWeight": "600",
                             "color": COLORS["white"]}),
        ]),
    ])

    # ═══════════════════════════════════════════════════════════════════════
    #  3. APP ARCHITECTURE
    # ═══════════════════════════════════════════════════════════════════════
    app_diagram = html.Div(className="card", style={"padding": "24px"}, children=[
        _section_title("fa-sitemap", "Application Architecture", COLORS["green"]),
        # Top: User layer
        html.Div(style={**_box(COLORS["blue"]), "flex": "none", "marginBottom": "0"},
                 children=[
            html.I(className="fa-solid fa-user",
                   style={"fontSize": "16px", "color": COLORS["blue"], "marginRight": "8px"}),
            html.Span("Browser / Databricks App",
                      style={"fontSize": "13px", "fontWeight": "600", "color": COLORS["white"]}),
        ]),
        _arrow_down(),
        # Middle: Dash app components
        html.Div(style={"display": "flex", "gap": "10px", "justifyContent": "center", "flexWrap": "wrap"},
                 children=[
            html.Div(style=_box(COLORS["green"]), children=[
                _label("Dash App"),
                _sublabel("layout.py · main.py"),
            ]),
            html.Div(style=_box(COLORS["purple"]), children=[
                _label("Page Renderers"),
                _sublabel("7 verticals × 7 pages"),
            ]),
            html.Div(style=_box(COLORS["blue"]), children=[
                _label("Genie Backend"),
                _sublabel("AI chat · FM API"),
            ]),
        ]),
        _arrow_down(),
        # Bottom: Data layer
        html.Div(style={"display": "flex", "gap": "10px", "justifyContent": "center", "flexWrap": "wrap"},
                 children=[
            html.Div(style=_box(COLORS["yellow"]), children=[
                _label("Data Access Layer"),
                _sublabel("data_access.py"),
            ]),
            html.Div(style=_box(COLORS["green"]), children=[
                _label("YAML Config"),
                _sublabel(f"config/{catalog}.yaml"),
            ]),
            html.Div(style=_box(COLORS["purple"]), children=[
                _label("Unity Catalog"),
                _sublabel(f"{catalog}.*.*"),
            ]),
        ]),
    ])

    # ═══════════════════════════════════════════════════════════════════════
    #  4. DATA MODEL (tables, fields, connections)
    # ═══════════════════════════════════════════════════════════════════════
    # Build table schema from genie tables and ML config
    table_schemas = []
    for tbl in table_names[:6]:
        parts = tbl.split(".")
        layer = parts[1] if len(parts) > 1 else "gold"
        tbl_name = parts[-1]
        layer_color = COLORS["blue"] if layer == "silver" else COLORS["green"]
        # Generate plausible fields based on table name
        common_fields = ["id", "timestamp", "created_at"]
        if "player" in tbl_name or "user" in tbl_name:
            fields = ["user_id", "segment", "ltv", "churn_score", "region"]
        elif "retention" in tbl_name:
            fields = ["cohort_date", "d1_rate", "d7_rate", "d30_rate", "segment"]
        elif "revenue" in tbl_name or "transaction" in tbl_name:
            fields = ["txn_id", "amount", "currency", "source", "user_id"]
        elif "kpi" in tbl_name or "metric" in tbl_name:
            fields = ["metric_name", "value", "period", "dimension", "category"]
        elif "subscriber" in tbl_name or "customer" in tbl_name:
            fields = ["customer_id", "plan_type", "tenure", "arpu", "status"]
        elif "event" in tbl_name or "telemetry" in tbl_name:
            fields = ["event_type", "user_id", "payload", "session_id", "device"]
        elif "fraud" in tbl_name or "risk" in tbl_name:
            fields = ["alert_id", "risk_score", "severity", "entity_id", "rule_id"]
        elif "health" in tbl_name or "ops" in tbl_name:
            fields = ["metric", "value", "threshold", "status", "region"]
        else:
            fields = ["entity_id", "value", "category", "status", "updated_at"]
        table_schemas.append((f"{layer}.{tbl_name}", layer_color, fields))

    # Add ML model entries
    for m_name in model_names[:3]:
        table_schemas.append((f"ml.{m_name.lower()}", COLORS["purple"],
                              ["model_version", "features", "predictions", "score", "timestamp"]))

    def _table_card(name, color, fields):
        return html.Div(
            style={"border": f"1px solid {color}40", "borderRadius": "8px", "overflow": "hidden",
                   "backgroundColor": COLORS["panel"], "minWidth": "180px", "flex": "1"},
            children=[
                html.Div(name, style={"backgroundColor": f"rgba({_hex_to_rgb(color)}, 0.12)",
                                       "padding": "8px 12px", "fontSize": "12px",
                                       "fontWeight": "700", "color": color,
                                       "fontFamily": "monospace", "borderBottom": f"1px solid {color}30"}),
                html.Div(children=[
                    html.Div(f, style={"padding": "3px 12px", "fontSize": "11px",
                                        "color": COLORS["text_muted"], "fontFamily": "monospace",
                                        "borderBottom": f"1px solid {COLORS['border']}"})
                    for f in fields
                ]),
            ],
        )

    data_model = html.Div(className="card", style={"padding": "24px"}, children=[
        _section_title("fa-diagram-project", "Data Model", COLORS["yellow"]),
        html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(auto-fill, minmax(180px, 1fr))",
                         "gap": "12px"},
                 children=[_table_card(n, c, f) for n, c, f in table_schemas]),
        # Connection legend
        html.Div(style={"marginTop": "16px", "display": "flex", "gap": "16px", "flexWrap": "wrap"},
                 children=[
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "6px"}, children=[
                html.Div(style={"width": "12px", "height": "12px", "borderRadius": "3px",
                                 "backgroundColor": COLORS["blue"]}),
                html.Span("Silver Tables", style={"fontSize": "11px", "color": COLORS["text_muted"]}),
            ]),
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "6px"}, children=[
                html.Div(style={"width": "12px", "height": "12px", "borderRadius": "3px",
                                 "backgroundColor": COLORS["green"]}),
                html.Span("Gold Tables", style={"fontSize": "11px", "color": COLORS["text_muted"]}),
            ]),
            html.Div(style={"display": "flex", "alignItems": "center", "gap": "6px"}, children=[
                html.Div(style={"width": "12px", "height": "12px", "borderRadius": "3px",
                                 "backgroundColor": COLORS["purple"]}),
                html.Span("ML Models", style={"fontSize": "11px", "color": COLORS["text_muted"]}),
            ]),
        ]),
    ])

    # ═══════════════════════════════════════════════════════════════════════
    #  5. BUSINESS VALUE REALIZATION
    # ═══════════════════════════════════════════════════════════════════════
    _value_row = lambda tech, biz, icon: html.Div(
        style={"display": "flex", "alignItems": "center", "gap": "16px",
               "padding": "14px 0", "borderBottom": f"1px solid {COLORS['border']}"},
        children=[
            html.I(className=f"fa-solid {icon}",
                   style={"color": COLORS["blue"], "fontSize": "16px", "width": "24px",
                          "textAlign": "center"}),
            html.Div(style={"flex": "1"}, children=[
                html.Div(tech, style={"fontSize": "13px", "color": COLORS["text_muted"],
                                       "marginBottom": "2px"}),
                html.Div(biz, style={"fontSize": "14px", "fontWeight": "600",
                                      "color": COLORS["white"]}),
            ]),
        ],
    )

    value_realization = html.Div(className="card", style={"padding": "24px"}, children=[
        _section_title("fa-chart-line", "Business Value Realization", COLORS["green"]),
        html.P("How data engineering excellence translates to executive outcomes:",
               style={"fontSize": "13px", "color": COLORS["text_muted"], "marginBottom": "16px"}),
        _value_row("Real-time Pipeline (Unity Catalog)", "Decision Confidence with Governed Data", "fa-shield-halved"),
        _value_row(f"{len(gold_tables)} Gold + {len(silver_tables)} Silver Tables", "Integrated Financial and Operational Truth", "fa-database"),
        _value_row(f"{len(model_names)} Predictive ML Models", "Early Warning Risk Mitigation", "fa-brain"),
        _value_row("7 Strategic Views per Vertical", "Comprehensive Strategic Oversight", "fa-eye"),
        _value_row("AI/BI Genie Conversational Analytics", "Self-Service Executive Intelligence", "fa-robot"),
        html.Div(
            style={"marginTop": "16px", "padding": "12px 16px",
                   "backgroundColor": f"rgba({_hex_to_rgb(COLORS['green'])}, 0.06)",
                   "borderRadius": "8px", "borderLeft": f"3px solid {COLORS['green']}",
                   "display": "flex", "alignItems": "center", "gap": "8px"},
            children=[
                html.I(className="fa-solid fa-certificate",
                       style={"color": COLORS["green"], "fontSize": "14px"}),
                html.Span("All data sourced from Unity Catalog with end-to-end lineage. "
                           "Meets 6 dimensions of data quality: Accuracy, Completeness, Consistency, "
                           "Timeliness, Validity, Uniqueness.",
                           style={"fontSize": "12px", "color": COLORS["text_muted"], "lineHeight": "1.5"}),
            ],
        ),
    ])

    # ═══════════════════════════════════════════════════════════════════════
    #  ASSEMBLE PAGE
    # ═══════════════════════════════════════════════════════════════════════
    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Solution Architecture"),
            html.P(f"{app_name} — Databricks Lakehouse architecture, platform services, and data model"),
        ]),
        html.Div(className="content-area", children=[
            lakehouse_diagram,
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
                children=[platform_diagram, app_diagram],
            ),
            data_model,
            value_realization,
        ]),
    ])


def _render_details():
    """Implementation Details page -- rich technical overview."""
    cfg = get_config()
    ml_cfg = cfg.get("ml", {})
    data_cfg = cfg.get("data", {})
    genie_cfg = cfg.get("genie", {})
    pages_cfg = cfg.get("pages", [])
    app_name = cfg["app"]["name"]
    catalog = cfg["app"].get("catalog", "N/A")

    # ── Count summary stats ────────────────────────────────────────────
    num_models = sum(1 for v in ml_cfg.values() if isinstance(v, dict) and "name" in v)
    total_features = sum(len(v.get("features", [])) for v in ml_cfg.values() if isinstance(v, dict))
    num_tables = len(genie_cfg.get("tables", []))
    num_questions = len(genie_cfg.get("sample_questions", []))
    num_pages = len([p for p in pages_cfg if p.get("enabled", True)])
    metric_groups = [k for k in data_cfg if isinstance(data_cfg[k], dict) and k.endswith("_metrics")]
    total_metrics = sum(len(data_cfg[k]) for k in metric_groups)

    # ── KPI strip ──────────────────────────────────────────────────────
    kpi_data = [
        {"label": "ML Models", "value": str(num_models), "accent": "blue"},
        {"label": "Model Features", "value": str(total_features), "accent": "purple"},
        {"label": "Lakehouse Tables", "value": str(num_tables), "accent": "green"},
        {"label": "Tracked Metrics", "value": str(total_metrics), "accent": "yellow"},
        {"label": "Dashboard Pages", "value": str(num_pages), "accent": "blue"},
        {"label": "Genie Questions", "value": str(num_questions), "accent": "green"},
    ]

    kpis = html.Div(
        style={"display": "grid", "gridTemplateColumns": f"repeat({len(kpi_data)}, 1fr)",
               "gap": "12px", "marginBottom": "20px"},
        children=[
            html.Div(className="card", style={"padding": "16px"}, children=[
                html.Div(kpi["label"], style={"fontSize": "10px", "color": COLORS["text_muted"],
                    "textTransform": "uppercase", "letterSpacing": "0.5px", "marginBottom": "4px"}),
                html.Div(kpi["value"], style={"fontSize": "24px", "fontWeight": "700",
                    "color": COLORS.get(kpi["accent"], COLORS["blue"])}),
            ])
            for kpi in kpi_data
        ],
    )

    # ── Model cards with hero metric style ─────────────────────────────
    model_cards = []
    model_accents = ["blue", "purple", "green", "yellow", "red"]
    for i, (model_key, model_info) in enumerate(ml_cfg.items()):
        if not isinstance(model_info, dict) or "name" not in model_info:
            continue
        accent = model_accents[i % len(model_accents)]
        accent_color = COLORS.get(accent, COLORS["blue"])
        algo = model_info.get("algorithm", "N/A")
        features = model_info.get("features", [])
        target_metric = model_info.get("target_metric", "")
        target_value = model_info.get("target_value", "")

        # Hero metric at top
        hero_section = html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "alignItems": "flex-start", "marginBottom": "16px"},
            children=[
                html.Div([
                    html.Div(model_info["name"].replace("_", " "),
                             style={"fontSize": "15px", "fontWeight": "700",
                                    "color": COLORS["white"], "marginBottom": "4px"}),
                    html.Div(algo, style={"fontSize": "12px", "color": accent_color,
                                          "fontWeight": "600"}),
                ]),
                html.Div(
                    style={"textAlign": "right"},
                    children=[
                        html.Div(str(target_value) if target_value else "--",
                                 style={"fontSize": "28px", "fontWeight": "700",
                                        "color": accent_color}),
                        html.Div(target_metric.upper() if target_metric else "",
                                 style={"fontSize": "10px", "color": COLORS["text_muted"],
                                        "letterSpacing": "0.5px"}),
                    ] if target_value else [],
                ),
            ],
        )

        # Feature tags
        feat_tags = html.Div(
            style={"display": "flex", "flexWrap": "wrap", "gap": "6px", "marginTop": "12px"},
            children=[
                html.Span(f, style={
                    "fontSize": "11px", "color": COLORS["text_muted"],
                    "backgroundColor": f"rgba({_hex_to_rgb(accent_color)}, 0.08)",
                    "padding": "3px 10px", "borderRadius": "12px",
                    "border": f"1px solid rgba({_hex_to_rgb(accent_color)}, 0.15)",
                })
                for f in features
            ],
        ) if features else html.Div()

        # Stats row
        stats = html.Div(
            style={"display": "flex", "gap": "16px", "marginTop": "14px",
                   "paddingTop": "12px", "borderTop": f"1px solid {COLORS['border']}"},
            children=[
                html.Div([
                    html.Div(str(len(features)), style={"fontSize": "16px", "fontWeight": "700",
                                                         "color": COLORS["white"]}),
                    html.Div("Features", style={"fontSize": "10px", "color": COLORS["text_muted"]}),
                ]),
                html.Div([
                    html.Div("Live" if target_value else "Dev",
                             style={"fontSize": "16px", "fontWeight": "700",
                                    "color": COLORS["green"] if target_value else COLORS["yellow"]}),
                    html.Div("Status", style={"fontSize": "10px", "color": COLORS["text_muted"]}),
                ]),
                html.Div([
                    html.Div(algo.split("(")[0][:12],
                             style={"fontSize": "16px", "fontWeight": "700",
                                    "color": COLORS["white"]}),
                    html.Div("Engine", style={"fontSize": "10px", "color": COLORS["text_muted"]}),
                ]),
            ],
        )

        model_cards.append(
            html.Div(className="card",
                     style={"padding": "20px", "borderLeft": f"3px solid {accent_color}"},
                     children=[hero_section, feat_tags, stats])
        )

    model_section = html.Div([
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px",
                         "marginBottom": "16px"}, children=[
            html.I(className="fa-solid fa-brain", style={"color": COLORS["blue"], "fontSize": "16px"}),
            html.Span("ML Models & AI", style={"fontSize": "16px", "fontWeight": "700",
                                                 "color": COLORS["white"]}),
        ]),
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fill, minmax(340px, 1fr))",
                   "gap": "16px", "alignItems": "start"},
            children=model_cards,
        ),
    ])

    # ── Data & Metrics section ─────────────────────────────────────────
    # Grouped metric cards
    metric_cards = []
    for group_key in metric_groups:
        group_label = group_key.replace("_", " ").replace("metrics", "").strip().title()
        group_data = data_cfg[group_key]
        rows = []
        for mk, mv in group_data.items():
            label = mk.replace("_", " ").title()
            if isinstance(mv, float) and mv < 1:
                display_val = f"{mv:.1%}"
            elif isinstance(mv, float):
                display_val = f"{mv:,.1f}"
            elif isinstance(mv, int) and mv > 9999:
                display_val = f"{mv:,}"
            else:
                display_val = str(mv)
            rows.append(_detail_row(label, display_val))
        metric_cards.append(
            html.Div(className="card", style={"padding": "20px"}, children=[
                html.Div(group_label, style={"fontSize": "13px", "fontWeight": "700",
                    "color": COLORS["white"], "textTransform": "uppercase",
                    "letterSpacing": "0.5px", "marginBottom": "12px"}),
                html.Div(children=rows),
            ])
        )

    # Dimensions card (lists like regions, segments, etc.)
    dim_rows = [_detail_row("Catalog", catalog)]
    for key in sorted(data_cfg.keys()):
        val = data_cfg[key]
        if isinstance(val, list):
            label = key.replace("_", " ").title()
            dim_rows.append(_detail_row(label, f"{len(val)} items"))
            dim_rows.append(html.Div(
                style={"display": "flex", "flexWrap": "wrap", "gap": "4px",
                       "paddingBottom": "8px", "marginTop": "-2px"},
                children=[html.Span(str(v), style={
                    "fontSize": "11px", "color": COLORS["text_muted"],
                    "backgroundColor": "rgba(255,255,255,0.04)",
                    "padding": "2px 8px", "borderRadius": "4px",
                }) for v in val],
            ))

    dim_card = html.Div(className="card", style={"padding": "20px"}, children=[
        html.Div("Data Dimensions", style={"fontSize": "13px", "fontWeight": "700",
            "color": COLORS["white"], "textTransform": "uppercase",
            "letterSpacing": "0.5px", "marginBottom": "12px"}),
        html.Div(children=dim_rows),
    ])

    data_section = html.Div([
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px",
                         "marginBottom": "16px", "marginTop": "24px"}, children=[
            html.I(className="fa-solid fa-database", style={"color": COLORS["green"], "fontSize": "16px"}),
            html.Span("Data & Metrics", style={"fontSize": "16px", "fontWeight": "700",
                                                 "color": COLORS["white"]}),
        ]),
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fill, minmax(300px, 1fr))",
                   "gap": "16px", "alignItems": "start"},
            children=metric_cards + [dim_card],
        ),
    ])

    # ── Platform section ───────────────────────────────────────────────
    tables = genie_cfg.get("tables", [])
    table_items = [
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "8px",
                         "padding": "6px 0", "borderBottom": f"1px solid {COLORS['border']}"},
            children=[
                html.I(className="fa-solid fa-table", style={"color": COLORS["blue"],
                    "fontSize": "11px", "opacity": "0.6"}),
                html.Span(t, style={"fontSize": "12px", "color": COLORS["white"],
                    "fontFamily": "'Courier New', monospace"}),
            ])
        for t in tables
    ]

    platform_cards = [
        html.Div(className="card", style={"padding": "20px",
                 "borderLeft": f"3px solid {COLORS['purple']}"}, children=[
            html.Div(style={"display": "flex", "justifyContent": "space-between",
                            "alignItems": "flex-start", "marginBottom": "16px"}, children=[
                html.Div([
                    html.Div(genie_cfg.get("space_name", "N/A"),
                             style={"fontSize": "15px", "fontWeight": "700",
                                    "color": COLORS["white"], "marginBottom": "4px"}),
                    html.Div("Databricks Genie AI Space",
                             style={"fontSize": "12px", "color": COLORS["purple"], "fontWeight": "600"}),
                ]),
                html.Div(style={"textAlign": "right"}, children=[
                    html.Div(str(num_questions), style={"fontSize": "28px", "fontWeight": "700",
                                                         "color": COLORS["purple"]}),
                    html.Div("QUESTIONS", style={"fontSize": "10px", "color": COLORS["text_muted"],
                                                  "letterSpacing": "0.5px"}),
                ]),
            ]),
            html.Div(style={"display": "flex", "gap": "16px", "paddingTop": "12px",
                            "borderTop": f"1px solid {COLORS['border']}"}, children=[
                html.Div([
                    html.Div(str(num_tables), style={"fontSize": "16px", "fontWeight": "700",
                                                      "color": COLORS["white"]}),
                    html.Div("Tables", style={"fontSize": "10px", "color": COLORS["text_muted"]}),
                ]),
                html.Div([
                    html.Div(str(num_questions), style={"fontSize": "16px", "fontWeight": "700",
                                                         "color": COLORS["white"]}),
                    html.Div("Questions", style={"fontSize": "10px", "color": COLORS["text_muted"]}),
                ]),
                html.Div([
                    html.Div(str(num_pages), style={"fontSize": "16px", "fontWeight": "700",
                                                     "color": COLORS["white"]}),
                    html.Div("Pages", style={"fontSize": "10px", "color": COLORS["text_muted"]}),
                ]),
            ]),
        ]),
        html.Div(className="card", style={"padding": "20px"}, children=[
            html.Div("Lakehouse Tables", style={"fontSize": "13px", "fontWeight": "700",
                "color": COLORS["white"], "textTransform": "uppercase",
                "letterSpacing": "0.5px", "marginBottom": "12px"}),
            html.Div(children=table_items),
        ]),
    ]

    platform_section = html.Div([
        html.Div(style={"display": "flex", "alignItems": "center", "gap": "10px",
                         "marginBottom": "16px", "marginTop": "24px"}, children=[
            html.I(className="fa-solid fa-server", style={"color": COLORS["purple"], "fontSize": "16px"}),
            html.Span("Platform & Infrastructure", style={"fontSize": "16px", "fontWeight": "700",
                                                            "color": COLORS["white"]}),
        ]),
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "repeat(auto-fill, minmax(400px, 1fr))",
                   "gap": "16px", "alignItems": "start"},
            children=platform_cards,
        ),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Implementation Details"),
            html.P(f"Technical configuration, ML models, and platform details for {app_name}"),
        ]),
        html.Div(className="content-area", children=[
            kpis,
            model_section,
            data_section,
            platform_section,
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
    "retail": {
        "dashboard": pages_retail.render_dashboard,
        "merchandising": pages_retail.render_merchandising,
        "customer_analytics": pages_retail.render_customer_analytics,
        "store_ops": pages_retail.render_store_ops,
        "supply_chain": pages_retail.render_supply_chain,
        "ecommerce": pages_retail.render_ecommerce,
        "loss_prevention": pages_retail.render_loss_prevention,
    },
    "customer_support": {
        "dashboard": pages_support.render_dashboard,
        "contact_center": pages_support.render_contact_center,
        "ticket_analytics": pages_support.render_ticket_analytics,
        "agent_performance": pages_support.render_agent_performance,
        "quality_assurance": pages_support.render_quality_assurance,
        "customer_health": pages_support.render_customer_health,
        "self_service": pages_support.render_self_service,
    },
    "real_estate": {
        "dashboard": pages_realestate.render_dashboard,
        "portfolio_analytics": pages_realestate.render_portfolio_analytics,
        "leasing": pages_realestate.render_leasing,
        "market_intel": pages_realestate.render_market_intel,
        "acquisitions": pages_realestate.render_acquisitions,
        "asset_management": pages_realestate.render_asset_management,
        "sustainability": pages_realestate.render_sustainability,
    },
    "energy": {
        "dashboard": pages_energy.render_dashboard,
        "generation": pages_energy.render_generation,
        "grid_ops": pages_energy.render_grid_ops,
        "customer_programs": pages_energy.render_customer_programs,
        "asset_health": pages_energy.render_asset_health,
        "energy_transition": pages_energy.render_energy_transition,
        "regulatory": pages_energy.render_regulatory,
    },
}

# Shared pages available across all verticals
_SHARED_PAGES = {
    "architecture": _render_architecture,
    "details": _render_details,
}


_page_render_cache: dict = {}  # (vertical, page_id) -> rendered component
_USE_RENDER_CACHE = os.environ.get("USE_DEMO_DATA", "true").lower() == "true"


def _get_renderer(vertical, page_id):
    """Look up the renderer for a vertical + page id.

    In demo mode, caches rendered pages to avoid re-creating static charts.
    """
    # Check shared pages first
    if page_id in _SHARED_PAGES:
        return lambda: _SHARED_PAGES[page_id]()

    # Check vertical-specific pages
    vertical_pages = _VERTICAL_PAGES.get(vertical, {})
    if page_id in vertical_pages:
        cache_key = (vertical, page_id)
        if _USE_RENDER_CACHE and cache_key in _page_render_cache:
            return lambda: _page_render_cache[cache_key]

        render_fn = vertical_pages[page_id]
        cfg = get_config_for(vertical)

        def _cached_render():
            result = render_fn(cfg)
            if _USE_RENDER_CACHE:
                _page_render_cache[cache_key] = result
            return result
        return _cached_render

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
    # Reset the DataTable counter so IDs are consistent per page
    _data_table_counter["n"] = 0

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
        content = html.Div([
            html.Div(className="page-header", children=[
                html.H1("Something went wrong"),
                html.P("An error occurred while loading this page"),
            ]),
            html.Div(className="content-area", children=[
                html.Div(className="card", children=[
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "12px",
                               "marginBottom": "16px"},
                        children=[
                            html.I(className="fa-solid fa-triangle-exclamation",
                                   style={"color": COLORS["red"], "fontSize": "24px"}),
                            html.Span("Error Loading Page",
                                      style={"fontSize": "18px", "fontWeight": "600"}),
                        ],
                    ),
                    html.Pre(str(e), style={
                        "color": COLORS["text_muted"], "fontSize": "12px",
                        "backgroundColor": COLORS["dark"], "padding": "12px",
                        "borderRadius": "8px", "fontFamily": "'Courier New', monospace",
                        "whiteSpace": "pre-wrap", "overflowX": "auto",
                    }),
                    html.Div(
                        style={"marginTop": "16px"},
                        children=dcc.Link(
                            "\u2190 Back to Demo Hub", href="/hub",
                            style={"color": COLORS["blue"], "textDecoration": "none",
                                   "fontSize": "14px"},
                        ),
                    ),
                ]),
            ]),
        ])

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
#  Table Row Drill-Down Callback
# ===================================================================


@app.callback(
    Output({"type": "row-detail-panel", "index": ALL}, "children"),
    Output({"type": "row-detail-panel", "index": ALL}, "style"),
    Input({"type": "interactive-table", "index": ALL}, "active_cell"),
    Input({"type": "interactive-table", "index": ALL}, "selected_rows"),
    State({"type": "interactive-table", "index": ALL}, "data"),
    State({"type": "interactive-table", "index": ALL}, "columns"),
    prevent_initial_call=True,
)
def show_row_detail(all_active, all_selected, all_data, all_columns):
    """When a table cell is clicked, show insight-style narrative detail."""
    n = len(all_active)
    children_out = [[] for _ in range(n)]
    styles_out = [{"display": "none"} for _ in range(n)]

    _STATUS_COLORS = {
        "healthy": COLORS["green"], "nominal": COLORS["blue"],
        "warning": COLORS["yellow"], "critical": COLORS["red"],
        "high": COLORS["red"], "medium": COLORS["yellow"],
        "low": COLORS["green"], "active": COLORS["green"],
        "inactive": COLORS["text_muted"], "online": COLORS["green"],
        "offline": COLORS["red"], "degraded": COLORS["yellow"],
        "a": COLORS["green"], "b": COLORS["blue"],
        "c": COLORS["yellow"], "d": COLORS["red"],
    }
    _WARN_STATUSES = {"warning", "critical", "high", "degraded",
                      "offline", "inactive", "d", "paused"}
    _STATUS_KEYS = {"status", "risk_status", "health", "severity",
                    "risk_grade", "state", "tier", "priority", "level"}

    def _parse_num(v):
        """Try to extract a float from a display value like '$1.82' or '42%'."""
        if v is None or v == "":
            return None
        s = str(v).replace(",", "").replace("$", "").replace("%", "")
        s = s.replace("x", "").replace("K", "e3").replace("M", "e6").strip()
        try:
            return float(s)
        except ValueError:
            return None

    def _is_status_col(col_id):
        return col_id.lower() in _STATUS_KEYS

    def _rank_in_table(data, col_id, row_val_num, higher_is_better=True):
        """Return (rank, total) for this value within the table column."""
        vals = []
        for r in data:
            v = _parse_num(r.get(col_id, ""))
            if v is not None:
                vals.append(v)
        if not vals or row_val_num is None:
            return None, len(vals)
        sorted_vals = sorted(vals, reverse=higher_is_better)
        try:
            rank = sorted_vals.index(row_val_num) + 1
        except ValueError:
            rank = len(vals)
        return rank, len(vals)

    # Cols where lower is better
    _LOWER_BETTER = {"cpi", "cost", "cost_hr", "cost_per_unit", "latency",
                     "p99_latency", "mttr", "cycle_time", "defect_rate",
                     "scrap_rate", "rework_rate", "error_rate", "churn",
                     "downtime", "response_time", "lead_time", "wait_time",
                     "denial_rate", "fraud_rate", "incident_rate",
                     "delinquency", "loss_ratio", "claims_cycle_days"}

    for i in range(n):
        row_idx = None
        if all_active[i] and isinstance(all_active[i], dict):
            row_idx = all_active[i].get("row")
        elif all_selected[i]:
            row_idx = all_selected[i][0]
        if row_idx is None:
            continue

        tbl_data = all_data[i]
        cols = all_columns[i]
        if not tbl_data or not cols or row_idx >= len(tbl_data):
            continue

        row = tbl_data[row_idx]
        col_list = [(c["id"], c["name"]) for c in cols]

        # ── Classify columns ─────────────────────────────────────────
        primary_id, primary_name = col_list[0]
        primary_val = str(row.get(primary_id, ""))

        metric_cols = []
        status_cols = []
        detail_cols = []
        for col_id, col_name in col_list[1:]:
            val = row.get(col_id, "")
            if _is_status_col(col_id):
                status_cols.append((col_id, col_name, val))
            elif _parse_num(val) is not None:
                metric_cols.append((col_id, col_name, val))
            else:
                detail_cols.append((col_id, col_name, val))

        # ── Determine severity & hero metric ─────────────────────────
        has_warning = any(
            str(v).lower() in _WARN_STATUSES for _, _, v in status_cols
        )
        status_str = str(status_cols[0][2]) if status_cols else ""
        status_lower = status_str.lower()

        # Pick hero metric: first metric col (or best ranked)
        hero_col_name = ""
        hero_val = ""
        hero_color = COLORS["blue"]
        hero_direction = "up"
        if metric_cols:
            hero_col_id, hero_col_name, hero_val = metric_cols[0]
            hero_num = _parse_num(hero_val)
            higher_better = hero_col_id.lower() not in _LOWER_BETTER
            rank, total = _rank_in_table(tbl_data, hero_col_id, hero_num, higher_better)
            if rank and rank <= max(1, total // 3):
                hero_color = COLORS["green"]
                hero_direction = "up"
            elif rank and rank > total - max(1, total // 3):
                hero_color = COLORS["red"]
                hero_direction = "down"
            else:
                hero_color = COLORS["blue"]
                hero_direction = "up"

        # ── Generate headline ────────────────────────────────────────
        if has_warning and status_lower in ("critical", "high"):
            headline = f"{primary_val} Flagged Critical — Immediate Review Required"
            severity_color = COLORS["red"]
        elif has_warning:
            headline = f"{primary_val} Showing Warning Indicators — Monitor Closely"
            severity_color = COLORS["yellow"]
        elif hero_direction == "up" and hero_color == COLORS["green"]:
            headline = f"{primary_val} Performing Above Average Across Key Metrics"
            severity_color = COLORS["green"]
        elif hero_direction == "down" and hero_color == COLORS["red"]:
            headline = f"{primary_val} Underperforming on {hero_col_name} — Action Needed"
            severity_color = COLORS["red"]
        else:
            headline = f"{primary_val} Operating Within Normal Parameters"
            severity_color = COLORS["blue"]

        # ── Generate narrative paragraph ─────────────────────────────
        metric_phrases = []
        for col_id, col_name, val in metric_cols[:6]:
            v_num = _parse_num(val)
            higher_better = col_id.lower() not in _LOWER_BETTER
            rank, total = _rank_in_table(tbl_data, col_id, v_num, higher_better)
            if rank and total > 1:
                pct = int((1 - (rank - 1) / (total - 1)) * 100) if total > 1 else 50
                if pct >= 75:
                    metric_phrases.append(
                        f"{col_name} of {val} (top {100 - pct}% across all rows)"
                    )
                elif pct <= 25:
                    metric_phrases.append(
                        f"{col_name} at {val} (bottom {pct + 1}% — below peers)"
                    )
                else:
                    metric_phrases.append(f"{col_name} at {val}")
            else:
                metric_phrases.append(f"{col_name} at {val}")

        # Build detail context from text columns
        context_parts = []
        for _, col_name, val in detail_cols[:3]:
            if val:
                context_parts.append(f"{col_name}: {val}")

        narrative = f"{primary_val} reports "
        narrative += ", ".join(metric_phrases[:4])
        narrative += "."
        if metric_phrases[4:]:
            narrative += f" Additionally, {', '.join(metric_phrases[4:])}."
        if context_parts:
            narrative += " " + " | ".join(context_parts) + "."
        if has_warning:
            narrative += (
                f" The current {status_cols[0][1].lower()} of \"{status_str}\""
                f" suggests this {primary_name.lower()} requires closer monitoring"
                f" and potential intervention."
            )

        # ── Generate action recommendation ───────────────────────────
        if has_warning and status_lower in ("critical", "high"):
            action = f"Escalate {primary_val} for immediate root-cause analysis and remediation"
        elif has_warning:
            action = f"Investigate {primary_val} warning signals and set up automated alerting"
        elif hero_color == COLORS["green"]:
            action = f"Document {primary_val} best practices for replication across peers"
        elif hero_color == COLORS["red"]:
            lowest_metric = metric_cols[-1][1] if metric_cols else "key metrics"
            action = f"Review {primary_val} performance on {lowest_metric} and identify improvement levers"
        else:
            action = f"Continue standard monitoring for {primary_val} with next review in 7 days"

        # ── Render insight card ──────────────────────────────────────
        arrow = "\u25b2" if hero_direction == "up" else "\u25bc"
        arrow_color = COLORS["green"] if hero_direction == "up" else COLORS["red"]

        insight = html.Div(
            style={"borderLeft": f"3px solid {severity_color}",
                   "backgroundColor": COLORS["panel"],
                   "border": f"1px solid {COLORS['border']}",
                   "borderLeftWidth": "3px",
                   "borderLeftStyle": "solid",
                   "borderLeftColor": severity_color,
                   "borderRadius": "12px", "padding": "20px 24px",
                   "marginBottom": "16px"},
            children=[
                # Headline + hero metric
                html.Div(
                    style={"display": "flex", "justifyContent": "space-between",
                           "alignItems": "flex-start", "marginBottom": "12px"},
                    children=[
                        html.Span(headline,
                                  style={"fontSize": "14px", "fontWeight": "600",
                                         "color": COLORS["white"], "flex": "1",
                                         "marginRight": "24px"}),
                        html.Div(
                            style={"display": "flex", "alignItems": "center",
                                   "gap": "6px", "flexShrink": "0"},
                            children=[
                                html.Span(str(hero_val),
                                          style={"fontSize": "22px", "fontWeight": "700",
                                                 "color": hero_color}),
                                html.Span(arrow,
                                          style={"color": arrow_color, "fontSize": "14px"}),
                            ],
                        ) if hero_val else html.Div(),
                    ],
                ),
                # Narrative paragraph
                html.P(narrative, style={
                    "fontSize": "13px", "color": COLORS["text_muted"],
                    "lineHeight": "1.65", "margin": "0 0 12px 0",
                }),
                # Action recommendation
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "6px"},
                    children=[
                        html.I(className="fa-solid fa-bolt",
                               style={"color": COLORS["yellow"], "fontSize": "11px"}),
                        html.Span(action,
                                  style={"fontSize": "12px", "color": COLORS["yellow"],
                                         "fontWeight": "600"}),
                    ],
                ),
            ],
        )

        # ── Metric strip below the insight ───────────────────────────
        _METRIC_ACCENTS = [COLORS["blue"], COLORS["green"], COLORS["purple"],
                           COLORS["cyan"], COLORS["orange"], COLORS["yellow"]]
        metric_cards = []
        for idx, (col_id, col_name, val) in enumerate(metric_cols):
            accent = _METRIC_ACCENTS[idx % len(_METRIC_ACCENTS)]
            metric_cards.append(
                html.Div(
                    style={"backgroundColor": COLORS["dark"],
                           "border": f"1px solid {COLORS['border']}",
                           "borderRadius": "10px", "padding": "12px 14px",
                           "borderTop": f"2px solid {accent}",
                           "textAlign": "center", "minWidth": "0"},
                    children=[
                        html.Div(str(val),
                                 style={"fontSize": "16px", "fontWeight": "700",
                                        "color": COLORS["white"],
                                        "marginBottom": "4px"}),
                        html.Div(col_name,
                                 style={"fontSize": "10px", "fontWeight": "600",
                                        "color": COLORS["text_muted"],
                                        "textTransform": "uppercase",
                                        "letterSpacing": "0.3px",
                                        "whiteSpace": "nowrap", "overflow": "hidden",
                                        "textOverflow": "ellipsis"}),
                    ],
                )
            )

        metrics_grid = html.Div(
            style={"display": "grid",
                   "gridTemplateColumns": f"repeat({min(len(metric_cards), 6)}, 1fr)",
                   "gap": "10px"},
            children=metric_cards,
        ) if metric_cards else None

        # ── Assemble ─────────────────────────────────────────────────
        panel_children = [insight]
        if metrics_grid:
            panel_children.append(metrics_grid)

        children_out[i] = panel_children
        styles_out[i] = {
            "display": "block",
            "padding": "20px 24px",
            "backgroundColor": f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.03)",
            "borderTop": f"2px solid {COLORS['blue']}",
            "animation": "pageFadeIn 0.2s ease-out",
        }

    return children_out, styles_out


# ===================================================================
#  Table Dropdown Filter Callback
# ===================================================================


@app.callback(
    Output({"type": "interactive-table", "index": ALL}, "data"),
    Input({"type": "table-col-filter", "index": ALL}, "value"),
    State({"type": "table-col-filter", "index": ALL}, "id"),
    State({"type": "table-store", "index": ALL}, "data"),
    State({"type": "interactive-table", "index": ALL}, "id"),
    prevent_initial_call=True,
)
def filter_table_by_dropdowns(filter_values, filter_ids, all_store_data, table_ids):
    """Filter table data based on dropdown selections.

    Each dropdown ID has index "N__col_id" where N is the table index.
    We parse these, group filters by table, and apply them to the
    original data stored in dcc.Store.
    """
    # Build a map: table_index -> {col_id: selected_values}
    table_filters = {}
    for fval, fid in zip(filter_values, filter_ids):
        parts = fid["index"].split("__", 1)
        if len(parts) != 2:
            continue
        tbl_idx = int(parts[0])
        col_id = parts[1]
        if fval:  # non-empty selection
            table_filters.setdefault(tbl_idx, {})[col_id] = set(fval)

    # Build table_index -> store_data map
    store_map = {}
    for i, tid in enumerate(table_ids):
        store_map[tid["index"]] = all_store_data[i] if i < len(all_store_data) else []

    # For each table, apply filters to its original store data
    result = []
    for tid in table_ids:
        idx = tid["index"]
        original = store_map.get(idx, [])
        active_filters = table_filters.get(idx, {})

        if not active_filters:
            result.append(original)
        else:
            filtered = []
            for row in original:
                match = True
                for col_id, allowed in active_filters.items():
                    if str(row.get(col_id, "")) not in allowed:
                        match = False
                        break
                if match:
                    filtered.append(row)
            result.append(filtered)

    return result


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
            is_error = msg.get("source") == "error"
            msg_cls = "genie-msg-ai genie-msg-error" if is_error else "genie-msg-ai"
            elements.append(html.Div(ai_children, className=msg_cls))

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

    # Cap chat history to prevent unbounded memory growth
    if len(chat_history) > 50:
        chat_history = chat_history[-50:]

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
