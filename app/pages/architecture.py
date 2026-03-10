"""Architecture Diagram page for ManufacturingIQ Databricks app.

Static page showing the system architecture as a Plotly Sankey/flow diagram
and a tech-stack reference table.
"""

from dash import html, dcc
import plotly.graph_objects as go

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
# Sankey diagram of data pipeline
# ===================================================================
_NODE_LABELS = [
    # 0-3  Sources
    "IoT Sensors",
    "MES / ERP",
    "Quality Systems",
    "Manual Logs",
    # 4  Bronze
    "Bronze Layer\n(Raw Ingestion)",
    # 5  Silver
    "Silver Layer\n(Cleaned & Enriched)",
    # 6  Gold
    "Gold Layer\n(Aggregated & Curated)",
    # 7-10  Consumers
    "Dash App\n(ManufacturingIQ)",
    "MLflow\n(Model Registry)",
    "Genie\n(AI / SQL)",
    "Alerts &\nNotifications",
]

_NODE_COLORS = [
    "#5E6AD2", "#5E6AD2", "#5E6AD2", "#5E6AD2",   # sources
    "#CD7F32",                                       # bronze
    "#C0C0C0",                                       # silver
    "#FFD700",                                       # gold
    ACCENT_BLUE, ACCENT_GREEN, "#7C5CFC", ACCENT_RED,  # consumers
]

_LINK_SOURCE = [0, 1, 2, 3,   4, 4,   5, 5,   6, 6, 6, 6,   8, 9]
_LINK_TARGET = [4, 4, 4, 4,   5, 8,   6, 8,   7, 8, 9, 10,  7, 7]
_LINK_VALUE  = [4, 3, 2, 1,   8, 1,   7, 1,   5, 2, 2, 1,   2, 1]
_LINK_COLORS = [
    "rgba(94,106,210,0.25)"]*4 + [
    "rgba(205,127,50,0.25)"]*2 + [
    "rgba(192,192,192,0.25)"]*2 + [
    "rgba(255,215,0,0.25)"]*4 + [
    "rgba(54,179,126,0.25)", "rgba(124,92,252,0.25)",
]


def _build_sankey():
    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            pad=30,
            thickness=22,
            line=dict(color=BORDER, width=1),
            label=_NODE_LABELS,
            color=_NODE_COLORS,
        ),
        link=dict(
            source=_LINK_SOURCE,
            target=_LINK_TARGET,
            value=_LINK_VALUE,
            color=_LINK_COLORS,
        ),
    ))
    fig.update_layout(
        paper_bgcolor=BG_CARD,
        font=dict(color=TEXT_PRIMARY, family="Inter, system-ui, sans-serif", size=12),
        margin=dict(l=20, r=20, t=50, b=20),
        title=dict(text="ManufacturingIQ Data Pipeline", font=dict(size=16)),
        height=460,
    )
    return fig


# ===================================================================
# Tech stack table
# ===================================================================
_TECH_STACK = [
    ("Data Ingestion", "Databricks Auto Loader, Delta Live Tables (DLT)"),
    ("Storage", "Delta Lake on Unity Catalog (Bronze / Silver / Gold)"),
    ("Orchestration", "Databricks Workflows, cron-scheduled Jobs"),
    ("ML Training", "Scikit-learn Isolation Forest, XGBoost regressor"),
    ("Model Registry", "MLflow on Databricks, champion/challenger promotion"),
    ("Model Serving", "Databricks Model Serving (real-time REST endpoint)"),
    ("Feature Store", "Databricks Feature Store (point-in-time lookups)"),
    ("AI / SQL", "Databricks Genie for natural-language queries"),
    ("Dashboard", "Dash 2.x (Plotly), deployed as Databricks App"),
    ("Alerting", "Databricks SQL Alerts \u2192 Slack / PagerDuty"),
    ("Monitoring", "Evidently AI for data drift, Lakehouse Monitoring"),
    ("Config", "YAML-driven per-environment configuration"),
]


def _tech_stack_table():
    header = html.Tr(
        children=[
            html.Th("Layer / Concern", style={
                "padding": "10px 16px", "textAlign": "left", "fontSize": "12px",
                "color": TEXT_MUTED, "textTransform": "uppercase",
                "letterSpacing": "0.5px", "borderBottom": f"2px solid {BORDER}",
                "backgroundColor": "#1A1D23",
            }),
            html.Th("Technology", style={
                "padding": "10px 16px", "textAlign": "left", "fontSize": "12px",
                "color": TEXT_MUTED, "textTransform": "uppercase",
                "letterSpacing": "0.5px", "borderBottom": f"2px solid {BORDER}",
                "backgroundColor": "#1A1D23",
            }),
        ],
    )

    rows = []
    for layer, tech in _TECH_STACK:
        rows.append(
            html.Tr(
                children=[
                    html.Td(layer, style={
                        "padding": "10px 16px", "fontSize": "13px",
                        "color": ACCENT_BLUE, "fontWeight": "600",
                        "borderBottom": f"1px solid {BORDER}",
                    }),
                    html.Td(tech, style={
                        "padding": "10px 16px", "fontSize": "13px",
                        "color": TEXT_PRIMARY,
                        "borderBottom": f"1px solid {BORDER}",
                    }),
                ],
            )
        )

    return html.Table(
        children=[html.Thead(header), html.Tbody(rows)],
        style={
            "width": "100%",
            "borderCollapse": "collapse",
            "backgroundColor": BG_CARD,
        },
    )


# ===================================================================
# ASCII pipeline diagram (supplemental quick-reference)
# ===================================================================
_ASCII_DIAGRAM = """\
\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510   \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510   \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510   \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510
\u2502  IoT Sensors  \u2502   \u2502  MES / ERP    \u2502   \u2502Quality Syst. \u2502   \u2502 Manual Logs  \u2502
\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518   \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518   \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518   \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
       \u2502               \u2502               \u2502               \u2502
       \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
               \u2502               \u2502
        \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510  Auto Loader / DLT
        \u2502 BRONZE LAYER  \u2502  (raw ingestion \u2192 Delta tables)
        \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
               \u2502
        \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510  Schema enforcement, dedup, joins
        \u2502 SILVER LAYER  \u2502  (cleaned & enriched)
        \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
               \u2502
        \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510  Aggregated KPIs, feature tables
        \u2502  GOLD LAYER   \u2502  (curated for analytics & ML)
        \u2514\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2518
            \u2502   \u2502   \u2502
    \u250c\u2500\u2500\u2500\u2500\u2500\u2534\u2500\u2510 \u2502 \u250c\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510
    \u2502 Dash  \u2502 \u2502 \u2502  Genie   \u2502
    \u2502 App   \u2502 \u2502 \u2502 (AI/SQL) \u2502
    \u2514\u2500\u2500\u2500\u252c\u2500\u2500\u2500\u2518 \u2502 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518
        \u2502     \u2502
        \u2502  \u250c\u2500\u2500\u2534\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510
        \u2514\u2500\u2500\u2524  MLflow     \u2502
           \u2502  Registry   \u2502
           \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\
"""


# ===================================================================
# Layout
# ===================================================================
def layout():
    """Return the Architecture Diagram page layout."""
    sankey_card = html.Div(
        children=[
            dcc.Graph(
                id="architecture-sankey",
                figure=_build_sankey(),
                config={"displayModeBar": False},
            ),
        ],
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "12px",
            "marginBottom": "24px",
        },
    )

    ascii_card = html.Div(
        children=[
            html.Div("Pipeline Quick-Reference", style={
                "fontSize": "14px", "fontWeight": "600", "color": TEXT_PRIMARY,
                "marginBottom": "12px",
            }),
            html.Pre(
                _ASCII_DIAGRAM,
                style={
                    "backgroundColor": "#0E1117",
                    "color": ACCENT_GREEN,
                    "padding": "20px",
                    "borderRadius": "4px",
                    "fontSize": "12px",
                    "lineHeight": "1.5",
                    "fontFamily": "JetBrains Mono, Menlo, monospace",
                    "overflowX": "auto",
                    "border": f"1px solid {BORDER}",
                    "margin": "0",
                },
            ),
        ],
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "20px",
            "marginBottom": "24px",
        },
    )

    tech_card = html.Div(
        children=[
            html.Div("Technology Stack", style={
                "fontSize": "15px", "fontWeight": "600", "color": TEXT_PRIMARY,
                "marginBottom": "16px",
            }),
            _tech_stack_table(),
        ],
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "20px",
        },
    )

    return html.Div(
        children=[
            html.H2("System Architecture", style={
                "color": TEXT_PRIMARY, "fontWeight": "700", "fontSize": "22px",
                "marginBottom": "20px",
            }),
            sankey_card,
            ascii_card,
            tech_card,
        ],
        style={"padding": "24px", "backgroundColor": BG_PRIMARY, "minHeight": "100vh"},
    )
