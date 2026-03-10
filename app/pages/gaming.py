"""
Page renderers for the Gaming vertical of Blueprint IQ v2.

Each public ``render_*`` function accepts a ``cfg`` dict (parsed from
gaming.yaml) and returns an ``html.Div`` that can be dropped into the
main content area.
"""

from dash import dcc, html
import plotly.graph_objects as go
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS

# ---------------------------------------------------------------------------
# Shared helper utilities (will migrate to a shared module later)
# ---------------------------------------------------------------------------

_ACCENT_ICONS = {
    "blue": "fa-chart-line",
    "purple": "fa-bolt",
    "green": "fa-arrow-trend-up",
    "red": "fa-triangle-exclamation",
    "yellow": "fa-circle-exclamation",
}


def _build_kpi_card(title, value_str, accent, icon, alert=False):
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
    return html.Div(
        style={"display": "flex", "justifyContent": "space-between", "padding": "6px 0",
               "borderBottom": f"1px solid {COLORS['border']}"},
        children=[
            html.Span(label, style={"fontSize": "13px", "color": COLORS["text_muted"]}),
            html.Span(str(value), style={"fontSize": "13px", "fontWeight": "500"}),
        ],
    )


# ---------------------------------------------------------------------------
# 1. Live Ops Command Center (Dashboard)
# ---------------------------------------------------------------------------

def render_dashboard(cfg):
    """Render the Live Ops Command Center dashboard page."""

    # -- KPI cards --
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("DAU", "2.4M", "blue", "fa-users"),
        _build_kpi_card("ARPDAU", "$0.118", "purple", "fa-dollar-sign"),
        _build_kpi_card("D7 Retention", "41%", "green", "fa-arrow-trend-up"),
        _build_kpi_card("High Churn Risk", "34.2K", "red", "fa-triangle-exclamation", alert=True),
    ])

    # -- Revenue by game title bar chart --
    titles = ["Stellar Conquest", "Shadow Realms", "Velocity Rush"]
    revenues = [142, 98, 44]

    fig = go.Figure(
        data=[
            go.Bar(
                x=titles,
                y=revenues,
                marker_color=[COLORS["blue"], COLORS["purple"], COLORS["green"]],
                text=[f"${v}K" for v in revenues],
                textposition="outside",
                textfont=dict(color=COLORS["white"], size=12),
            ),
        ],
    )
    fig.update_layout(
        title=dict(text="Revenue by Game Title", font=dict(size=14, color=COLORS["white"])),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT_FAMILY, color=COLORS["text_muted"]),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        yaxis=dict(
            showgrid=True, gridcolor=COLORS["border"], color=COLORS["text_muted"],
            title="Revenue ($K)",
        ),
        margin=dict(l=48, r=24, t=48, b=40),
        height=320,
    )

    chart_card = html.Div(className="card", children=[
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ])

    # -- Live Operations Summary table --
    ops_rows = [
        html.Tr([
            _td("Stellar Conquest", bold=True),
            _td("1.2M", mono=True),
            _td("184K", mono=True),
            _td("$0.142", mono=True, color=COLORS["green"]),
            _td("24.5K", mono=True),
        ]),
        html.Tr([
            _td("Shadow Realms", bold=True),
            _td("820K", mono=True),
            _td("112K", mono=True),
            _td("$0.108", mono=True, color=COLORS["green"]),
            _td("18.2K", mono=True),
        ]),
        html.Tr([
            _td("Velocity Rush", bold=True),
            _td("380K", mono=True),
            _td("52K", mono=True),
            _td("$0.094", mono=True, color=COLORS["yellow"]),
            _td("9.8K", mono=True),
        ]),
    ]

    table_card = html.Div(className="card", children=[
        html.H3("Live Operations Summary", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Game Title", "DAU", "Concurrent", "ARPDAU", "Events/sec"], ops_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Live Ops Command Center"),
            html.P("Real-time operational overview across all game titles"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            chart_card,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 2. Player Intelligence
# ---------------------------------------------------------------------------

def render_player_intel(cfg):
    """Render the Player Intelligence page."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Player LTV", "$47.80", "blue", "fa-chart-line"),
        _build_kpi_card("CLV", "$52.40", "purple", "fa-bolt"),
        _build_kpi_card("D1 Retention", "68%", "green", "fa-arrow-trend-up"),
        _build_kpi_card("Churn Risk High", "34.2K", "red", "fa-triangle-exclamation", alert=True),
    ])

    # -- Segment Breakdown table --
    segments = [
        ("Whale",        "82%", "64%", "45%", "$284.50", "Low"),
        ("Dolphin",      "74%", "48%", "28%", "$68.20",  "Medium"),
        ("Minnow",       "65%", "35%", "15%", "$12.40",  "High"),
        ("Free-to-Play", "58%", "28%", "8%",  "$2.10",   "Critical"),
    ]

    seg_rows = []
    for seg, d1, d7, d30, ltv, risk in segments:
        # Map risk levels to STATUS_COLORS keys
        risk_key_map = {"Low": "Low", "Medium": "Low", "High": "Critical", "Critical": "Critical"}
        seg_rows.append(html.Tr([
            _td(seg, bold=True),
            _td(d1, mono=True, color=COLORS["green"]),
            _td(d7, mono=True),
            _td(d30, mono=True),
            _td(ltv, mono=True, color=COLORS["purple"]),
            _status_td(risk, status_key=risk_key_map.get(risk, "Healthy")),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Segment Breakdown", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Segment", "D1", "D7", "D30", "LTV", "Churn Risk"], seg_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Player Intelligence"),
            html.P("Retention cohorts, lifetime value, and churn segmentation"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 3. Revenue & Monetization
# ---------------------------------------------------------------------------

def render_revenue(cfg):
    """Render the Revenue & Monetization page."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Daily Revenue", "$284K", "blue", "fa-dollar-sign"),
        _build_kpi_card("ARPDAU", "$0.118", "purple", "fa-chart-line"),
        _build_kpi_card("IAP Share", "72%", "green", "fa-cart-shopping"),
        _build_kpi_card("Ad Revenue Share", "18%", "yellow", "fa-rectangle-ad"),
    ])

    # -- Revenue by Title table --
    rev_data = [
        ("Stellar Conquest", "$142K", "$0.142", "$102K", "$25K", "$15K"),
        ("Shadow Realms",    "$98K",  "$0.108", "$71K",  "$18K", "$9K"),
        ("Velocity Rush",    "$44K",  "$0.094", "$30K",  "$8K",  "$6K"),
    ]

    rev_rows = []
    for title, rev, arpdau, iap, ad, bp in rev_data:
        rev_rows.append(html.Tr([
            _td(title, bold=True),
            _td(rev, mono=True, color=COLORS["green"]),
            _td(arpdau, mono=True),
            _td(iap, mono=True),
            _td(ad, mono=True),
            _td(bp, mono=True),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Revenue by Title", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Title", "Revenue", "ARPDAU", "IAP Rev", "Ad Rev", "Battle Pass"], rev_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Revenue & Monetization"),
            html.P("Daily revenue streams, IAP performance, and ad monetization metrics"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 4. User Acquisition & Growth
# ---------------------------------------------------------------------------

def render_ua_growth(cfg):
    """Render the User Acquisition & Growth page."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Avg ROAS", "1.52x", "blue", "fa-chart-line"),
        _build_kpi_card("CAC", "$3.80", "purple", "fa-bullseye"),
        _build_kpi_card("Active Campaigns", "24", "green", "fa-rocket"),
        _build_kpi_card("Monthly Spend", "$420K", "yellow", "fa-money-bill-trend-up"),
    ])

    # -- Campaign Performance table --
    campaign_data = [
        ("Google",      "$128K", "42.1K", "$3.04", "1.82x", "2.14x"),
        ("Meta",        "$104K", "31.8K", "$3.27", "1.64x", "1.98x"),
        ("TikTok",      "$72K",  "22.4K", "$3.21", "1.48x", "1.72x"),
        ("Apple",       "$58K",  "14.2K", "$4.08", "1.32x", "1.58x"),
        ("Organic",     "$0",    "18.6K", "$0.00", "N/A",   "N/A"),
        ("Cross-promo", "$12K",  "8.4K",  "$1.43", "2.84x", "3.42x"),
    ]

    camp_rows = []
    for channel, spend, installs, cpi, roas7, roas30 in campaign_data:
        camp_rows.append(html.Tr([
            _td(channel, bold=True),
            _td(spend, mono=True),
            _td(installs, mono=True),
            _td(cpi, mono=True),
            _td(roas7, mono=True, color=COLORS["green"] if roas7 != "N/A" else None),
            _td(roas30, mono=True, color=COLORS["green"] if roas30 != "N/A" else None),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Campaign Performance", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Channel", "Spend", "Installs", "CPI", "ROAS D7", "ROAS D30"], camp_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("User Acquisition & Growth"),
            html.P("Campaign spend, install volumes, and return on ad spend analysis"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 5. Game Development & Quality
# ---------------------------------------------------------------------------

def render_game_dev(cfg):
    """Render the Game Development & Quality page."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Milestone Completion", "87%", "blue", "fa-flag-checkered"),
        _build_kpi_card("Build Time", "4.2 hrs", "purple", "fa-clock"),
        _build_kpi_card("Bug Resolution", "94%", "green", "fa-bug"),
        _build_kpi_card("Review Score", "4.3/5", "yellow", "fa-star"),
    ])

    # -- Development Status by Title table --
    dev_data = [
        ("Stellar Conquest", "Season 8 Launch", "92%", "24", "96%", "4.5"),
        ("Shadow Realms",    "PvP Overhaul",    "84%", "41", "91%", "4.2"),
        ("Velocity Rush",    "Map Expansion",   "78%", "38", "88%", "4.1"),
    ]

    dev_rows = []
    for title, milestone, completion, bugs, resolution, score in dev_data:
        dev_rows.append(html.Tr([
            _td(title, bold=True),
            _td(milestone),
            _td(completion, mono=True, color=COLORS["green"]),
            _td(bugs, mono=True, color=COLORS["yellow"]),
            _td(resolution, mono=True),
            _td(score, mono=True),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Development Status by Title", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Title", "Milestone", "Completion", "Bugs Open", "Resolution Rate", "Review Score"], dev_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Game Development & Quality"),
            html.P("Milestone tracking, build pipelines, bug triage, and store review scores"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 6. Infrastructure & Ops
# ---------------------------------------------------------------------------

def render_infrastructure(cfg):
    """Render the Infrastructure & Ops page."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Server Uptime", "99.95%", "green", "fa-server"),
        _build_kpi_card("MTTR", "12.4 min", "blue", "fa-wrench"),
        _build_kpi_card("Avg Latency", "28ms", "purple", "fa-gauge-high"),
        _build_kpi_card("Time to Insight", "4.8 min", "yellow", "fa-magnifying-glass-chart"),
    ])

    # -- Regional Infrastructure table --
    infra_data = [
        ("US-East",   "99.98%", "8.2 min",  "22ms", "48ms",  "$4.2K"),
        ("US-West",   "99.97%", "9.1 min",  "24ms", "52ms",  "$3.8K"),
        ("EU-West",   "99.94%", "11.4 min", "32ms", "68ms",  "$3.6K"),
        ("EU-North",  "99.96%", "10.8 min", "30ms", "64ms",  "$2.9K"),
        ("Asia-East", "99.91%", "14.8 min", "38ms", "82ms",  "$4.8K"),
        ("Asia-SE",   "99.89%", "18.2 min", "42ms", "94ms",  "$3.1K"),
    ]

    infra_rows = []
    for region, uptime, mttr, latency, p99, cost in infra_data:
        infra_rows.append(html.Tr([
            _td(region, bold=True),
            _td(uptime, mono=True, color=COLORS["green"]),
            _td(mttr, mono=True),
            _td(latency, mono=True),
            _td(p99, mono=True, color=COLORS["yellow"]),
            _td(cost, mono=True),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Regional Infrastructure", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Region", "Uptime", "MTTR", "Latency", "P99 Latency", "Daily Cost"], infra_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Infrastructure & Ops"),
            html.P("Server health, latency monitoring, and regional cost analysis"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])
