"""Telecommunications vertical page renderers.

Provides six page-level rendering functions for the Telecom dashboard:
Network Command Center, Customer Intelligence, Revenue & Growth,
Fraud & Security, Field Operations, and B2B & IoT.
"""

from dash import dcc, html
import plotly.graph_objects as go
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS

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
    if bold: style["fontWeight"] = "500"
    if mono: style["fontFamily"] = "monospace"; style["fontWeight"] = "600"
    if color: style["color"] = color
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
# Plot theme helper
# ---------------------------------------------------------------------------
_PLOT_LAYOUT = dict(
    paper_bgcolor=COLORS["panel"],
    plot_bgcolor=COLORS["dark"],
    font=dict(color=COLORS["white"], family=FONT_FAMILY, size=12),
    margin=dict(l=50, r=20, t=40, b=40),
    xaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
    yaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
)

# Table row border style
_ROW_BORDER = {"borderBottom": f"1px solid {COLORS['border']}"}


# ===================================================================
# 1. Network Command Center
# ===================================================================

def render_dashboard(cfg):
    """Network Command Center dashboard with KPIs, uptime chart, and network overview table."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Subscriber Base", "14.2M", "blue", "fa-users"),
        _build_kpi_card("Network Uptime", "99.97%", "green", "fa-signal"),
        _build_kpi_card("Monthly Churn", "1.8%", "yellow", "fa-arrow-right-from-bracket"),
        _build_kpi_card("Fraud Blocked 24h", "229", "red", "fa-shield-halved", alert=True),
    ])

    # Bar chart — network uptime by region
    regions = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
    uptimes = [99.98, 99.96, 99.97, 99.94, 99.99]
    bar_colors = [COLORS["green"] if u >= 99.97 else COLORS["yellow"] if u >= 99.95 else COLORS["red"]
                  for u in uptimes]

    fig = go.Figure(go.Bar(
        x=regions, y=uptimes,
        marker=dict(color=bar_colors, line=dict(width=0)),
        text=[f"{u}%" for u in uptimes],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig.update_layout(
        paper_bgcolor=COLORS["panel"],
        plot_bgcolor=COLORS["dark"],
        font=dict(color=COLORS["white"], family=FONT_FAMILY, size=12),
        margin=dict(l=50, r=20, t=40, b=40),
        title=dict(text="Network Uptime by Region", font=dict(size=15)),
        yaxis=dict(range=[99.90, 100.0], gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
        xaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
        height=340,
    )

    uptime_chart = html.Div(className="card", children=[
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ])

    # Network overview table
    network_data = [
        ("Northeast", "5G NR", "99.98%", "72%", "1.8 hrs", "1.2M"),
        ("Northeast", "LTE", "99.99%", "68%", "2.1 hrs", "2.8M"),
        ("Southeast", "5G NR", "99.96%", "78%", "2.4 hrs", "0.9M"),
        ("Southeast", "LTE", "99.97%", "74%", "2.8 hrs", "2.4M"),
        ("Midwest", "5G NR", "99.97%", "65%", "2.0 hrs", "0.8M"),
        ("Midwest", "LTE", "99.98%", "62%", "2.3 hrs", "1.9M"),
        ("Southwest", "5G NR", "99.94%", "81%", "3.1 hrs", "0.6M"),
        ("Southwest", "LTE", "99.95%", "76%", "3.4 hrs", "1.4M"),
        ("West", "5G NR", "99.99%", "70%", "1.5 hrs", "1.1M"),
        ("West", "LTE", "99.99%", "66%", "1.7 hrs", "2.1M"),
    ]

    table_rows = []
    for region, tech, uptime, cap_util, mttr, devices in network_data:
        # Determine status for uptime
        uptime_val = float(uptime.replace("%", ""))
        if uptime_val >= 99.97:
            status_key = "Healthy"
        elif uptime_val >= 99.95:
            status_key = "Low"
        else:
            status_key = "Critical"
        table_rows.append(html.Tr(style=_ROW_BORDER, children=[
            _td(region, bold=True),
            _td(tech),
            _status_td(uptime, status_key),
            _td(cap_util, mono=True),
            _td(mttr, mono=True),
            _td(devices, mono=True),
        ]))

    network_table = html.Div(className="card", children=[
        html.H3("Network Overview", style={"fontSize": "15px", "fontWeight": "600",
                 "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(["Region", "Technology", "Uptime", "Capacity Util", "MTTR", "Connected Devices"], table_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Network Command Center"),
            html.P("Real-time network performance and subscriber metrics"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, uptime_chart, network_table]),
    ])


# ===================================================================
# 2. Customer Intelligence
# ===================================================================

def render_customer(cfg):
    """Customer Intelligence page with segment analysis."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("NPS", "42", "blue", "fa-face-smile"),
        _build_kpi_card("CSAT", "4.1/5.0", "green", "fa-star"),
        _build_kpi_card("Churn Rate", "1.8%", "yellow", "fa-user-minus"),
        _build_kpi_card("ARPU", "$52.30", "purple", "fa-dollar-sign"),
    ])

    # Customer segments table
    segments = [
        ("Consumer", "8.2M", "$42.10", "2.1%", "38", "0.34"),
        ("SMB", "3.4M", "$68.40", "1.4%", "44", "0.52"),
        ("Enterprise", "2.1M", "$124.80", "0.8%", "48", "0.61"),
        ("Government", "0.5M", "$98.20", "0.4%", "52", "0.28"),
    ]

    table_rows = []
    for segment, subs, arpu, churn, nps, upsell in segments:
        churn_val = float(churn.replace("%", ""))
        if churn_val <= 0.8:
            churn_color = COLORS["green"]
        elif churn_val <= 1.5:
            churn_color = COLORS["yellow"]
        else:
            churn_color = COLORS["red"]

        nps_val = int(nps)
        if nps_val >= 50:
            nps_color = COLORS["green"]
        elif nps_val >= 40:
            nps_color = COLORS["blue"]
        else:
            nps_color = COLORS["yellow"]

        table_rows.append(html.Tr(style=_ROW_BORDER, children=[
            _td(segment, bold=True),
            _td(subs, mono=True),
            _td(arpu, mono=True),
            _td(churn, mono=True, color=churn_color),
            _td(nps, mono=True, color=nps_color),
            _td(upsell, mono=True),
        ]))

    segments_table = html.Div(className="card", children=[
        html.H3("Customer Segments", style={"fontSize": "15px", "fontWeight": "600",
                 "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(["Segment", "Subscribers", "ARPU", "Churn Rate", "NPS", "Upsell Score"], table_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Customer Intelligence"),
            html.P("Subscriber satisfaction, segmentation, and lifetime value analytics"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, segments_table]),
    ])


# ===================================================================
# 3. Revenue & Growth
# ===================================================================

def render_revenue(cfg):
    """Revenue & Growth page with plan-level revenue breakdown."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Total ARPU", "$52.30", "blue", "fa-dollar-sign"),
        _build_kpi_card("Subscriber Growth", "2.4% MoM", "green", "fa-arrow-trend-up"),
        _build_kpi_card("Contract Renewals", "89%", "purple", "fa-file-contract"),
        _build_kpi_card("Digital Adoption", "61%", "yellow", "fa-mobile-screen-button"),
    ])

    # Revenue by plan type table
    plans = [
        ("Postpaid", "6.8M", "$58.40", "$397.1M", "+1.8%"),
        ("Prepaid", "4.1M", "$32.20", "$132.0M", "+3.2%"),
        ("Family", "1.9M", "$72.60", "$137.9M", "+2.1%"),
        ("Business", "1.1M", "$112.50", "$123.8M", "+4.6%"),
        ("IoT", "0.3M", "$18.90", "$5.7M", "+12.4%"),
    ]

    table_rows = []
    for plan, subs, arpu, revenue, growth in plans:
        growth_val = float(growth.replace("%", "").replace("+", ""))
        if growth_val >= 4.0:
            growth_color = COLORS["green"]
        elif growth_val >= 2.0:
            growth_color = COLORS["blue"]
        else:
            growth_color = COLORS["yellow"]

        table_rows.append(html.Tr(style=_ROW_BORDER, children=[
            _td(plan, bold=True),
            _td(subs, mono=True),
            _td(arpu, mono=True),
            _td(revenue, mono=True),
            _td(growth, mono=True, color=growth_color),
        ]))

    revenue_table = html.Div(className="card", children=[
        html.H3("Revenue by Plan Type", style={"fontSize": "15px", "fontWeight": "600",
                 "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(["Plan", "Subscribers", "ARPU", "Revenue", "Growth"], table_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Revenue & Growth"),
            html.P("Plan-level revenue performance and subscriber growth trends"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, revenue_table]),
    ])


# ===================================================================
# 4. Fraud & Security
# ===================================================================

def render_fraud(cfg):
    """Fraud & Security page with detection metrics and event breakdown."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Detection Rate", "96.2%", "green", "fa-magnifying-glass"),
        _build_kpi_card("Loss Prevented", "$4.2M/mo", "blue", "fa-shield-halved"),
        _build_kpi_card("SIM Swap Attempts", "142/day", "yellow", "fa-sim-card"),
        _build_kpi_card("Subscription Fraud", "87/day", "red", "fa-user-secret", alert=True),
    ])

    # Fraud events table
    fraud_events = [
        ("SIM Swap", "142", "136", "95.8%", "$1,240"),
        ("Subscription", "87", "79", "90.8%", "$3,420"),
        ("IRSF", "64", "62", "96.9%", "$8,750"),
        ("Device Fraud", "38", "37", "97.4%", "$890"),
    ]

    table_rows = []
    for ftype, detected, blocked, block_rate, avg_amount in fraud_events:
        rate_val = float(block_rate.replace("%", ""))
        if rate_val >= 96.0:
            rate_color = COLORS["green"]
        elif rate_val >= 93.0:
            rate_color = COLORS["yellow"]
        else:
            rate_color = COLORS["red"]

        table_rows.append(html.Tr(style=_ROW_BORDER, children=[
            _td(ftype, bold=True),
            _td(detected, mono=True),
            _td(blocked, mono=True),
            _td(block_rate, mono=True, color=rate_color),
            _td(avg_amount, mono=True),
        ]))

    fraud_table = html.Div(className="card", children=[
        html.H3("Fraud Events", style={"fontSize": "15px", "fontWeight": "600",
                 "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(["Type", "Detected", "Blocked", "Block Rate", "Avg Amount at Risk"], table_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Fraud & Security"),
            html.P("Real-time fraud detection, prevention metrics, and threat intelligence"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, fraud_table]),
    ])


# ===================================================================
# 5. Field Operations
# ===================================================================

def render_field_ops(cfg):
    """Field Operations page with regional work order and SLA tracking."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("MTTR", "3.2 hrs", "blue", "fa-clock"),
        _build_kpi_card("FCR Rate", "78%", "green", "fa-check-double"),
        _build_kpi_card("SLA Achievement", "97.8%", "purple", "fa-clipboard-check"),
        _build_kpi_card("Truck Roll Rate", "32%", "yellow", "fa-truck"),
    ])

    # Field operations by region
    field_data = [
        ("Northeast", "1,842", "94.2%", "2.8 hrs", "98.1%", "81%"),
        ("Southeast", "2,156", "92.8%", "3.1 hrs", "97.4%", "76%"),
        ("Midwest", "1,634", "93.5%", "3.4 hrs", "97.9%", "79%"),
        ("Southwest", "1,298", "91.7%", "3.8 hrs", "96.8%", "74%"),
        ("West", "1,978", "95.1%", "2.6 hrs", "98.6%", "83%"),
    ]

    table_rows = []
    for region, orders, completion, mttr, sla, ftf in field_data:
        sla_val = float(sla.replace("%", ""))
        if sla_val >= 98.0:
            sla_status_key = "Healthy"
        elif sla_val >= 97.0:
            sla_status_key = "Low"
        else:
            sla_status_key = "Critical"

        table_rows.append(html.Tr(style=_ROW_BORDER, children=[
            _td(region, bold=True),
            _td(orders, mono=True),
            _td(completion, mono=True),
            _td(mttr, mono=True),
            _status_td(sla, sla_status_key),
            _td(ftf, mono=True),
        ]))

    ops_table = html.Div(className="card", children=[
        html.H3("Field Operations by Region", style={"fontSize": "15px", "fontWeight": "600",
                 "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(["Region", "Work Orders", "Completion Rate", "MTTR", "SLA Met", "First-Time Fix"], table_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Field Operations"),
            html.P("Work order management, SLA compliance, and technician performance"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, ops_table]),
    ])


# ===================================================================
# 6. B2B & IoT
# ===================================================================

def render_b2b_iot(cfg):
    """B2B & IoT page with product-level revenue and device metrics."""

    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("IoT Devices", "2.4M", "blue", "fa-microchip"),
        _build_kpi_card("Edge Revenue", "$8.2M/mo", "green", "fa-server"),
        _build_kpi_card("API Calls", "42M/mo", "purple", "fa-code"),
        _build_kpi_card("Uptime", "99.98%", "green", "fa-arrow-trend-up"),
    ])

    # B2B products table
    products = [
        ("IoT Platform", "1,240", "1.8M", "$3.4M", "+8.2%"),
        ("Edge Computing", "186", "42K", "$2.1M", "+14.6%"),
        ("Private 5G", "64", "128K", "$1.8M", "+22.3%"),
        ("API Services", "892", "420K", "$0.9M", "+6.1%"),
    ]

    table_rows = []
    for product, customers, devices, revenue, growth in products:
        growth_val = float(growth.replace("%", "").replace("+", ""))
        if growth_val >= 15.0:
            growth_color = COLORS["green"]
        elif growth_val >= 8.0:
            growth_color = COLORS["blue"]
        else:
            growth_color = COLORS["yellow"]

        table_rows.append(html.Tr(style=_ROW_BORDER, children=[
            _td(product, bold=True),
            _td(customers, mono=True),
            _td(devices, mono=True),
            _td(revenue, mono=True),
            _td(growth, mono=True, color=growth_color),
        ]))

    b2b_table = html.Div(className="card", children=[
        html.H3("B2B Products", style={"fontSize": "15px", "fontWeight": "600",
                 "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(["Product Type", "Customers", "Devices", "Monthly Revenue", "Growth"], table_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("B2B & IoT"),
            html.P("Enterprise connectivity, IoT platform, and edge computing analytics"),
        ]),
        html.Div(className="content-area", children=[kpi_cards, b2b_table]),
    ])
