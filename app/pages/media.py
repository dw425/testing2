"""Media & Entertainment vertical page renderers.

Provides six page-level rendering functions for the Media & Entertainment
vertical: Audience Command Center, Content Performance, Subscription & Revenue,
Advertising & Yield, Creative & Campaigns, and Platform & QoS.
"""

from dash import dcc, html
import plotly.graph_objects as go
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_ACCENT_ICONS = {
    "blue": "fa-chart-line", "purple": "fa-bolt", "green": "fa-arrow-trend-up",
    "red": "fa-triangle-exclamation", "yellow": "fa-circle-exclamation",
}


def _build_kpi_card(title, value_str, accent, icon, alert=False):
    accent_class = f"accent-{accent}"
    children = [
        html.Div(style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}, children=[
            html.Span(title, className="card-title"),
            html.I(className=f"fa-solid {icon}", style={"color": COLORS["text_muted"], "fontSize": "14px"}),
        ]),
        html.Div(value_str, className=f"card-value {accent_class}"),
        html.Div("Live", className="card-subtitle"),
    ]
    if alert:
        children.insert(1, html.Div(style={"position": "absolute", "top": "12px", "right": "12px"},
            children=html.Span("ALERT", style={"fontSize": "9px", "fontWeight": "700", "color": COLORS["red"],
                "backgroundColor": "rgba(239, 68, 68, 0.12)", "padding": "2px 6px", "borderRadius": "4px", "letterSpacing": "0.5px"})))
    return html.Div(className="card", style={"position": "relative"}, children=children)


def _build_table(headers, rows):
    th_style = {"padding": "10px 14px", "fontSize": "11px", "color": COLORS["text_muted"], "textAlign": "left",
                "borderBottom": f"1px solid {COLORS['border']}", "textTransform": "uppercase", "letterSpacing": "0.5px"}
    return html.Table(style={"width": "100%", "borderCollapse": "collapse"}, children=[
        html.Thead(html.Tr([html.Th(h, style=th_style) for h in headers])), html.Tbody(rows)])


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
    return html.Td(html.Span(status_text, className="status-badge",
        style={"backgroundColor": sc["bg"], "color": sc["text"], "border": f"1px solid {sc['border']}"}),
        style={"padding": "10px 14px"})


def _detail_row(label, value):
    return html.Div(style={"display": "flex", "justifyContent": "space-between", "padding": "6px 0",
        "borderBottom": f"1px solid {COLORS['border']}"}, children=[
        html.Span(label, style={"fontSize": "13px", "color": COLORS["text_muted"]}),
        html.Span(str(value), style={"fontSize": "13px", "fontWeight": "500"})])


# ---------------------------------------------------------------------------
# Plot layout defaults
# ---------------------------------------------------------------------------

_PLOT_LAYOUT = dict(
    paper_bgcolor=COLORS["panel"],
    plot_bgcolor="#0E1117",
    font=dict(color=COLORS["white"], family=FONT_FAMILY, size=12),
    margin=dict(l=50, r=30, t=45, b=45),
    xaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
    yaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
)


# ===================================================================
# 1. Audience Command Center
# ===================================================================

def render_dashboard(cfg):
    """Audience Command Center -- KPIs, regional bar chart, and audience tier table."""

    # --- KPI cards ---
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Monthly Active Viewers", "8.4M", "blue", "fa-users"),
        _build_kpi_card("Avg Watch Time", "47 min/day", "purple", "fa-clock"),
        _build_kpi_card("Subscriber LTV", "$142", "green", "fa-dollar-sign"),
        _build_kpi_card("Cross-platform Match", "74%", "yellow", "fa-link"),
    ])

    # --- Viewers by region bar chart ---
    regions = ["US", "UK", "EU", "APAC", "LATAM"]
    viewers = [3.2, 1.1, 1.6, 1.8, 0.7]

    fig = go.Figure(go.Bar(
        x=regions, y=viewers,
        marker=dict(color=[COLORS["blue"], COLORS["purple"], COLORS["green"],
                           COLORS["yellow"], COLORS["red"]],
                    line=dict(width=0)),
        text=[f"{v}M" for v in viewers],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig.update_layout(
        **_PLOT_LAYOUT,
        title=dict(text="Viewers by Region", font=dict(size=15)),
        yaxis_title="Viewers (Millions)",
        height=360,
    )

    chart = html.Div(
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
        style={"backgroundColor": COLORS["panel"], "border": f"1px solid {COLORS['border']}",
               "borderRadius": "12px", "padding": "12px", "marginBottom": "16px"},
    )

    # --- Audience by Subscription Tier table ---
    tier_data = [
        ("Premium", "1.2M", "62 min", "$18.40", "Low"),
        ("Standard", "3.8M", "48 min", "$9.20", "Medium"),
        ("Free", "2.8M", "32 min", "$2.40", "High"),
        ("Trial", "0.6M", "28 min", "$0", "Critical"),
    ]

    churn_status_map = {
        "Low": "Healthy",
        "Medium": "Low",
        "High": "Critical",
        "Critical": "Critical",
    }

    tier_rows = []
    for tier, viewers_str, watch_time, revenue, churn in tier_data:
        tier_rows.append(html.Tr([
            _td(tier, bold=True),
            _td(viewers_str, mono=True),
            _td(watch_time),
            _td(revenue, mono=True),
            _status_td(churn, churn_status_map.get(churn)),
        ]))

    table = _build_table(["Tier", "Viewers", "Watch Time", "Revenue/Viewer", "Churn Risk"], tier_rows)
    table_card = html.Div(className="card", children=[
        html.Span("Audience by Subscription Tier", className="card-title",
                   style={"display": "block", "marginBottom": "12px"}),
        table,
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Audience Command Center"),
            html.P("Real-time audience analytics across platforms and tiers"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, chart, table_card]),
    ])


# ===================================================================
# 2. Content Performance
# ===================================================================

def render_content(cfg):
    """Content Performance -- KPIs and genre-level content table."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Content ROI", "2.8x", "blue", "fa-chart-line"),
        _build_kpi_card("Catalog Utilization", "68%", "purple", "fa-film"),
        _build_kpi_card("Recommendation CTR", "12.4%", "green", "fa-bullseye"),
        _build_kpi_card("Total Titles", "4,200", "yellow", "fa-list"),
    ])

    genre_data = [
        ("Drama",       "820",  "1.4M", "78%", "3.2x", "4.5"),
        ("Action",      "640",  "1.8M", "72%", "2.9x", "4.3"),
        ("Comedy",      "580",  "1.1M", "81%", "2.6x", "4.2"),
        ("Documentary", "520",  "680K", "85%", "3.8x", "4.7"),
        ("Sports",      "440",  "2.2M", "62%", "2.4x", "4.1"),
        ("Reality",     "380",  "920K", "74%", "2.1x", "3.9"),
        ("News",        "820",  "540K", "58%", "1.8x", "3.6"),
    ]

    genre_rows = []
    for genre, titles, avg_views, completion, roi, rating in genre_data:
        roi_val = float(roi.replace("x", ""))
        roi_color = COLORS["green"] if roi_val >= 3.0 else (COLORS["yellow"] if roi_val >= 2.5 else COLORS["red"])
        genre_rows.append(html.Tr([
            _td(genre, bold=True),
            _td(titles),
            _td(avg_views, mono=True),
            _td(completion),
            _td(roi, mono=True, color=roi_color),
            _td(rating),
        ]))

    table = _build_table(["Genre", "Titles", "Avg Views", "Completion Rate", "ROI", "Rating"], genre_rows)
    table_card = html.Div(className="card", children=[
        html.Span("Content Performance by Genre", className="card-title",
                   style={"display": "block", "marginBottom": "12px"}),
        table,
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Content Performance"),
            html.P("Catalog analytics, engagement, and return on content investment"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, table_card]),
    ])


# ===================================================================
# 3. Subscription & Revenue
# ===================================================================

def render_subscriptions(cfg):
    """Subscription & Revenue -- KPIs and tier-level subscription metrics."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Subscriber LTV", "$142", "blue", "fa-dollar-sign"),
        _build_kpi_card("Monthly Churn", "4.2%", "red", "fa-arrow-trend-down", alert=True),
        _build_kpi_card("SVOD Revenue", "$4.8M/mo", "green", "fa-money-bill-trend-up"),
        _build_kpi_card("Paywall Conversion", "12%", "purple", "fa-lock-open"),
    ])

    sub_data = [
        ("Premium",  "1.2M", "$218", "$2.1M", "2.1%", "N/A"),
        ("Standard", "3.8M", "$142", "$1.8M", "3.8%", "8.4%"),
        ("Free",     "2.8M", "$38",  "$680K", "6.2%", "4.2%"),
        ("Trial",    "0.6M", "$0",   "$0",    "42%",  "18.6%"),
    ]

    churn_thresholds = {"2.1%": "Healthy", "3.8%": "Low", "6.2%": "Critical", "42%": "Critical"}

    sub_rows = []
    for tier, subs, ltv, mrr, churn, upgrade in sub_data:
        churn_key = churn_thresholds.get(churn, "Healthy")
        sub_rows.append(html.Tr([
            _td(tier, bold=True),
            _td(subs, mono=True),
            _td(ltv, mono=True),
            _td(mrr, mono=True),
            _status_td(churn, churn_key),
            _td(upgrade),
        ]))

    table = _build_table(["Tier", "Subscribers", "LTV", "MRR", "Churn Rate", "Upgrade Rate"], sub_rows)
    table_card = html.Div(className="card", children=[
        html.Span("Subscription Metrics by Tier", className="card-title",
                   style={"display": "block", "marginBottom": "12px"}),
        table,
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Subscription & Revenue"),
            html.P("Subscriber lifecycle, churn analysis, and revenue breakdown"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, table_card]),
    ])


# ===================================================================
# 4. Advertising & Yield
# ===================================================================

def render_advertising(cfg):
    """Advertising & Yield -- KPIs and ad format performance table."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Avg CPM", "$14.20", "blue", "fa-dollar-sign"),
        _build_kpi_card("Fill Rate", "92%", "green", "fa-gauge-high"),
        _build_kpi_card("ROAS", "3.4x", "purple", "fa-chart-line"),
        _build_kpi_card("Brand Safety", "98.2%", "green", "fa-shield-halved"),
    ])

    ad_data = [
        ("Pre-roll",  "24.2M", "$18.40", "2.8%", "94%",  "$445K"),
        ("Mid-roll",  "18.6M", "$16.20", "1.9%", "88%",  "$302K"),
        ("Display",   "42.1M", "$8.60",  "0.8%", "N/A",  "$362K"),
        ("Native",    "12.4M", "$22.80", "3.4%", "N/A",  "$283K"),
        ("CTV",       "8.8M",  "$28.40", "1.2%", "96%",  "$250K"),
    ]

    ad_rows = []
    for fmt, impressions, cpm, ctr, vcr, revenue in ad_data:
        cpm_val = float(cpm.replace("$", ""))
        cpm_color = COLORS["green"] if cpm_val >= 20 else None
        ad_rows.append(html.Tr([
            _td(fmt, bold=True),
            _td(impressions, mono=True),
            _td(cpm, mono=True, color=cpm_color),
            _td(ctr),
            _td(vcr),
            _td(revenue, mono=True),
        ]))

    table = _build_table(["Format", "Impressions", "CPM", "CTR", "VCR", "Revenue"], ad_rows)
    table_card = html.Div(className="card", children=[
        html.Span("Ad Performance by Format", className="card-title",
                   style={"display": "block", "marginBottom": "12px"}),
        table,
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Advertising & Yield"),
            html.P("Programmatic performance, fill rates, and yield optimization"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, table_card]),
    ])


# ===================================================================
# 5. Creative & Campaigns
# ===================================================================

def render_creative(cfg):
    """Creative & Campaigns -- KPIs and top campaigns table."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Campaign ROAS", "3.4x", "blue", "fa-chart-line"),
        _build_kpi_card("Creative Win Rate", "34%", "purple", "fa-trophy"),
        _build_kpi_card("A/B Tests Active", "12", "green", "fa-flask"),
        _build_kpi_card("Attribution Coverage", "89%", "yellow", "fa-crosshairs"),
    ])

    campaign_data = [
        ("Spring Originals Launch",  "StreamCo",       "$420K", "18.2M", "4.2x", "Multi-touch"),
        ("Live Sports Push",         "BetFair Media",   "$380K", "14.6M", "3.8x", "Last-click"),
        ("Family Bundle Promo",      "DigiHome",        "$240K", "9.8M",  "3.1x", "Multi-touch"),
        ("Documentary Series Drop",  "NatView",         "$180K", "7.2M",  "2.9x", "First-touch"),
        ("Holiday Binge Campaign",   "AdVantage Corp",  "$520K", "22.4M", "3.6x", "Multi-touch"),
    ]

    campaign_rows = []
    for campaign, advertiser, spend, impressions, roas, attribution in campaign_data:
        roas_val = float(roas.replace("x", ""))
        roas_color = COLORS["green"] if roas_val >= 3.5 else (COLORS["yellow"] if roas_val >= 3.0 else COLORS["red"])
        campaign_rows.append(html.Tr([
            _td(campaign, bold=True),
            _td(advertiser),
            _td(spend, mono=True),
            _td(impressions, mono=True),
            _td(roas, mono=True, color=roas_color),
            _td(attribution),
        ]))

    table = _build_table(["Campaign", "Advertiser", "Spend", "Impressions", "ROAS", "Attribution"], campaign_rows)
    table_card = html.Div(className="card", children=[
        html.Span("Top Campaigns", className="card-title",
                   style={"display": "block", "marginBottom": "12px"}),
        table,
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Creative & Campaigns"),
            html.P("Campaign performance, creative testing, and attribution analysis"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, table_card]),
    ])


# ===================================================================
# 6. Platform & QoS
# ===================================================================

def render_platform(cfg):
    """Platform & QoS -- KPIs and quality-of-service table by platform."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Buffering Rate", "0.8%", "green", "fa-spinner"),
        _build_kpi_card("Start Time", "1.8s", "blue", "fa-clock"),
        _build_kpi_card("Peak Concurrent", "1.24M", "purple", "fa-users"),
        _build_kpi_card("CDN Cost", "$42K/day", "yellow", "fa-server"),
    ])

    platform_data = [
        ("Web",      "0.6%", "1.4s", "8.2 Mbps", "0.12%", "2.4M"),
        ("iOS",      "0.4%", "1.2s", "9.6 Mbps", "0.08%", "1.8M"),
        ("Android",  "1.2%", "2.1s", "7.4 Mbps", "0.18%", "2.1M"),
        ("Smart TV", "1.4%", "2.8s", "12.4 Mbps", "0.22%", "1.6M"),
        ("Console",  "0.3%", "1.1s", "14.8 Mbps", "0.06%", "0.8M"),
    ]

    platform_rows = []
    for platform, buffering, start_time, bitrate, error_rate, sessions in platform_data:
        buf_val = float(buffering.replace("%", ""))
        if buf_val <= 0.5:
            buf_status = "Healthy"
        elif buf_val <= 1.0:
            buf_status = "Low"
        else:
            buf_status = "Critical"
        platform_rows.append(html.Tr([
            _td(platform, bold=True),
            _status_td(buffering, buf_status),
            _td(start_time, mono=True),
            _td(bitrate, mono=True),
            _td(error_rate),
            _td(sessions, mono=True),
        ]))

    table = _build_table(
        ["Platform", "Buffering %", "Start Time", "Bitrate", "Error Rate", "Sessions"],
        platform_rows,
    )
    table_card = html.Div(className="card", children=[
        html.Span("QoS by Platform", className="card-title",
                   style={"display": "block", "marginBottom": "12px"}),
        table,
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Platform & QoS"),
            html.P("Streaming quality, CDN performance, and platform health"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, table_card]),
    ])
