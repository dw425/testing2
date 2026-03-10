"""Financial Services vertical pages for the expanded FinancialIQ application.

Covers Banking, Capital Markets, and Insurance sub-verticals with six page
renderers: dashboard, banking, capital_markets, insurance, fraud_compliance,
and customer.
"""

from dash import dcc, html
import plotly.graph_objects as go
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS

# ---------------------------------------------------------------------------
# Shared constants & helpers
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
# Shared layout helpers
# ---------------------------------------------------------------------------

_PLOT_LAYOUT = dict(
    paper_bgcolor=COLORS["panel"],
    plot_bgcolor="#0E1117",
    font=dict(color=COLORS["white"], family=FONT_FAMILY, size=12),
    margin=dict(l=50, r=20, t=40, b=40),
    xaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
    yaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
)

_ROW_STYLE = {"borderBottom": f"1px solid {COLORS['border']}"}

_SECTION_CARD_STYLE = {
    "backgroundColor": COLORS["panel"],
    "border": f"1px solid {COLORS['border']}",
    "borderRadius": "12px",
    "padding": "24px",
    "marginBottom": "24px",
}


def _page_header(title, subtitle):
    return html.Div(className="page-header", children=[
        html.H1(title),
        html.P(subtitle),
    ])


def _kpi_grid(cards):
    return html.Div(className="grid-4", style={"marginBottom": "24px"}, children=cards)


# ===================================================================
# 1. render_dashboard  –  Enterprise Risk Command Center
# ===================================================================

def render_dashboard(cfg):
    """Enterprise Risk Command Center spanning Banking, Capital Markets, and Insurance."""

    # KPI cards
    kpi_cards = _kpi_grid([
        _build_kpi_card("Transactions Today", "12.5M", "blue", "fa-chart-line"),
        _build_kpi_card("Fraud Blocked", "847", "red", "fa-triangle-exclamation", alert=True),
        _build_kpi_card("Portfolio VaR 95%", "$47M", "purple", "fa-bolt"),
        _build_kpi_card("Combined Ratio", "96.4%", "green", "fa-arrow-trend-up"),
    ])

    # Bar chart – key metrics by sub-vertical
    sub_verticals = ["Banking", "Capital Markets", "Insurance"]
    revenue_values = [2.1, 0.89, 1.4]
    risk_scores = [62, 38, 55]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=sub_verticals, y=revenue_values, name="Revenue ($B)",
        marker=dict(color=COLORS["blue"], cornerradius=4),
        text=[f"${v}B" for v in revenue_values],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig.add_trace(go.Bar(
        x=sub_verticals, y=risk_scores, name="Risk Score",
        marker=dict(color=COLORS["purple"], cornerradius=4),
        text=[str(v) for v in risk_scores],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
        yaxis="y2",
    ))
    fig.update_layout(
        paper_bgcolor=COLORS["panel"],
        plot_bgcolor="#0E1117",
        font=dict(color=COLORS["white"], family=FONT_FAMILY, size=12),
        margin=dict(l=50, r=20, t=40, b=40),
        title=dict(text="Key Metrics by Sub-Vertical", font=dict(size=15)),
        barmode="group",
        height=360,
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
        xaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
        yaxis=dict(title="Revenue ($B)", gridcolor="#1E2028", zerolinecolor="#1E2028"),
        yaxis2=dict(title="Risk Score", overlaying="y", side="right",
                    gridcolor="#1E2028", zerolinecolor="#1E2028"),
    )

    chart_section = html.Div(style=_SECTION_CARD_STYLE, children=[
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ])

    # Cross-Business Summary table
    summary_headers = ["Sub-Vertical", "Revenue", "Risk Score", "Compliance Status", "Key Metric"]
    summary_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("Banking", bold=True), _td("$2.1B", mono=True),
            _status_td("Medium", "Low"), _status_td("Compliant", "Healthy"),
            _td("NIM 3.1%"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Capital Markets", bold=True), _td("$890M", mono=True),
            _status_td("Low", "Healthy"), _status_td("Compliant", "Healthy"),
            _td("Sharpe 1.42"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Insurance", bold=True), _td("$1.4B", mono=True),
            _status_td("Medium", "Low"), _status_td("Under Review", "Low"),
            _td("CLR 96.4%"),
        ]),
    ]

    summary_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Cross-Business Summary", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(summary_headers, summary_rows),
    ])

    return html.Div(children=[
        _page_header("Enterprise Risk Command Center",
                     "Unified view across Banking, Capital Markets & Insurance operations"),
        html.Div(className="content-area", children=[kpi_cards, chart_section, summary_table]),
    ])


# ===================================================================
# 2. render_banking  –  Banking Intelligence
# ===================================================================

def render_banking(cfg):
    """Banking Intelligence page with NIM, fraud, LTV, and call-center metrics."""

    kpi_cards = _kpi_grid([
        _build_kpi_card("NIM", "3.1%", "blue", "fa-chart-line"),
        _build_kpi_card("Fraud Rate", "0.23%", "red", "fa-triangle-exclamation"),
        _build_kpi_card("Customer LTV", "$4,200", "green", "fa-arrow-trend-up"),
        _build_kpi_card("Digital Adoption", "67%", "purple", "fa-bolt"),
    ])

    # Banking Metrics by Line
    banking_headers = ["Line", "Portfolio", "Delinquency", "Fraud Blocked", "NPS", "LTV"]
    banking_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("Retail Banking", bold=True), _td("$48B", mono=True),
            _td("1.8%"), _td("312"), _td("34"), _td("$3,200"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Commercial Lending", bold=True), _td("$22B", mono=True),
            _td("0.9%"), _td("87"), _td("42"), _td("$18,400"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Wealth Management", bold=True), _td("$15B", mono=True),
            _td("0.3%"), _td("24"), _td("52"), _td("$42,000"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Cards", bold=True), _td("$8.4B", mono=True),
            _td("2.4%"), _td("424"), _td("28"), _td("$1,800"),
        ]),
    ]

    banking_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Banking Metrics by Line", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(banking_headers, banking_rows),
    ])

    # Call Center Metrics
    call_center_headers = ["Metric", "Value", "Target", "Status"]
    call_center_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("AHT", bold=True), _td("6.2 min", mono=True),
            _td("5.5 min"), _status_td("Above Target", "Low"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("FCR", bold=True), _td("74%", mono=True),
            _td("80%"), _status_td("Below Target", "Low"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("CSAT", bold=True), _td("4.1", mono=True),
            _td("4.5"), _status_td("Below Target", "Low"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("STP Rate", bold=True), _td("89%", mono=True),
            _td("92%"), _status_td("On Track", "Healthy"),
        ]),
    ]

    call_center_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Call Center Metrics", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(call_center_headers, call_center_rows),
    ])

    return html.Div(children=[
        _page_header("Banking Intelligence",
                     "Retail, commercial, wealth management & cards performance"),
        html.Div(className="content-area", children=[kpi_cards, banking_table, call_center_table]),
    ])


# ===================================================================
# 3. render_capital_markets  –  Capital Markets Analytics
# ===================================================================

def render_capital_markets(cfg):
    """Capital Markets Analytics with AUM, VaR, Sharpe, and alpha tracking."""

    kpi_cards = _kpi_grid([
        _build_kpi_card("AUM", "$24.5B", "blue", "fa-chart-line"),
        _build_kpi_card("VaR 95%", "$47M", "red", "fa-triangle-exclamation"),
        _build_kpi_card("Sharpe Ratio", "1.42", "green", "fa-arrow-trend-up"),
        _build_kpi_card("Alpha", "180 bps", "purple", "fa-bolt"),
    ])

    # Portfolio by Asset Class
    portfolio_headers = ["Asset Class", "AUM", "VaR Contribution", "Sharpe", "Daily P&L", "Positions"]
    portfolio_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("Equities", bold=True), _td("$8.2B", mono=True),
            _td("$14M", mono=True), _td("1.48", mono=True),
            _td("+$2.4M", color=COLORS["green"]), _td("2,140"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Fixed Income", bold=True), _td("$7.1B", mono=True),
            _td("$11M", mono=True), _td("1.39", mono=True),
            _td("-$0.8M", color=COLORS["red"]), _td("1,680"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Derivatives", bold=True), _td("$5.4B", mono=True),
            _td("$15M", mono=True), _td("1.35", mono=True),
            _td("+$1.2M", color=COLORS["green"]), _td("1,890"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Alternatives", bold=True), _td("$3.8B", mono=True),
            _td("$7M", mono=True), _td("1.52", mono=True),
            _td("+$0.6M", color=COLORS["green"]), _td("1,200"),
        ]),
    ]

    portfolio_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Portfolio by Asset Class", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(portfolio_headers, portfolio_rows),
    ])

    return html.Div(children=[
        _page_header("Capital Markets Analytics",
                     "Portfolio performance, risk attribution & position management"),
        html.Div(className="content-area", children=[kpi_cards, portfolio_table]),
    ])


# ===================================================================
# 4. render_insurance  –  Insurance Operations
# ===================================================================

def render_insurance(cfg):
    """Insurance Operations with combined ratio, loss ratio, claims, and retention."""

    kpi_cards = _kpi_grid([
        _build_kpi_card("Combined Ratio", "96.4%", "blue", "fa-chart-line"),
        _build_kpi_card("Loss Ratio", "62.1%", "red", "fa-triangle-exclamation"),
        _build_kpi_card("Claims Fraud", "8.2%", "yellow", "fa-circle-exclamation"),
        _build_kpi_card("Policy Retention", "87%", "green", "fa-arrow-trend-up"),
    ])

    # Insurance by Product
    insurance_headers = ["Product", "Premiums", "Loss Ratio", "Combined Ratio", "Claims", "Retention"]
    insurance_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("Auto", bold=True), _td("$420M", mono=True),
            _td("64.2%"), _td("97.8%"), _td("18,400"), _td("84%"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Home", bold=True), _td("$310M", mono=True),
            _td("58.4%"), _td("93.1%"), _td("12,200"), _td("89%"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Life", bold=True), _td("$280M", mono=True),
            _td("42.6%"), _td("88.2%"), _td("4,800"), _td("94%"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Commercial", bold=True), _td("$240M", mono=True),
            _td("71.3%"), _td("102.4%"), _td("8,600"), _td("82%"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Health", bold=True), _td("$180M", mono=True),
            _td("78.9%"), _td("104.1%"), _td("22,000"), _td("86%"),
        ]),
    ]

    insurance_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Insurance by Product", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(insurance_headers, insurance_rows),
    ])

    return html.Div(children=[
        _page_header("Insurance Operations",
                     "Underwriting performance, claims analytics & policy retention"),
        html.Div(className="content-area", children=[kpi_cards, insurance_table]),
    ])


# ===================================================================
# 5. render_fraud_compliance  –  Fraud & Compliance Hub
# ===================================================================

def render_fraud_compliance(cfg):
    """Fraud & Compliance Hub with detection metrics and regulatory status."""

    kpi_cards = _kpi_grid([
        _build_kpi_card("Total Fraud Blocked", "847", "red", "fa-triangle-exclamation"),
        _build_kpi_card("AML Alerts", "1,240/mo", "yellow", "fa-circle-exclamation"),
        _build_kpi_card("Claims Fraud Rate", "8.2%", "purple", "fa-bolt"),
        _build_kpi_card("Regulatory Capital", "14.2%", "green", "fa-arrow-trend-up"),
    ])

    # Fraud Detection by Type
    fraud_headers = ["Type", "Detected", "Blocked", "Rate", "Avg Amount", "Trend"]
    fraud_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("Transaction Fraud", bold=True), _td("1,240", mono=True),
            _td("1,180", mono=True), _td("95.2%"),
            _td("$2,400", mono=True), _td("\u2193 12%", color=COLORS["green"]),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Account Takeover", bold=True), _td("380", mono=True),
            _td("342", mono=True), _td("90.0%"),
            _td("$8,200", mono=True), _td("\u2191 8%", color=COLORS["red"]),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Synthetic ID", bold=True), _td("145", mono=True),
            _td("128", mono=True), _td("88.3%"),
            _td("$15,600", mono=True), _td("\u2191 22%", color=COLORS["red"]),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Claims Fraud", bold=True), _td("420", mono=True),
            _td("385", mono=True), _td("91.7%"),
            _td("$12,400", mono=True), _td("\u2193 5%", color=COLORS["green"]),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Market Manipulation", bold=True), _td("28", mono=True),
            _td("24", mono=True), _td("85.7%"),
            _td("$142,000", mono=True), _td("\u2192 0%", color=COLORS["text_muted"]),
        ]),
    ]

    fraud_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Fraud Detection by Type", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(fraud_headers, fraud_rows),
    ])

    # Compliance Status
    compliance_headers = ["Framework", "Status", "Capital Impact", "Last Review"]
    compliance_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("Basel III/IV", bold=True), _status_td("Compliant", "Healthy"),
            _td("$2.4B", mono=True), _td("2026-02-15"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Dodd-Frank", bold=True), _status_td("Compliant", "Healthy"),
            _td("$1.8B", mono=True), _td("2026-01-20"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("MiFID II", bold=True), _status_td("Under Review", "Low"),
            _td("$640M", mono=True), _td("2026-03-01"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Solvency II", bold=True), _status_td("Compliant", "Healthy"),
            _td("$920M", mono=True), _td("2026-02-28"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("CECL", bold=True), _status_td("Needs Review", "Critical"),
            _td("$1.1B", mono=True), _td("2025-12-10"),
        ]),
    ]

    compliance_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Compliance Status", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(compliance_headers, compliance_rows),
    ])

    return html.Div(children=[
        _page_header("Fraud & Compliance Hub",
                     "Cross-business fraud detection, AML monitoring & regulatory compliance"),
        html.Div(className="content-area", children=[kpi_cards, fraud_table, compliance_table]),
    ])


# ===================================================================
# 6. render_customer  –  Customer & Distribution
# ===================================================================

def render_customer(cfg):
    """Customer & Distribution page with segmentation, LTV, and churn analytics."""

    kpi_cards = _kpi_grid([
        _build_kpi_card("Customer LTV", "$4,200", "blue", "fa-chart-line"),
        _build_kpi_card("NPS", "38", "green", "fa-arrow-trend-up"),
        _build_kpi_card("Churn Risk", "12.4K", "red", "fa-triangle-exclamation"),
        _build_kpi_card("Digital Adoption", "67%", "purple", "fa-bolt"),
    ])

    # Customer Segments
    segment_headers = ["Segment", "Customers", "Avg LTV", "Products Held", "NPS", "Churn Risk"]
    segment_rows = [
        html.Tr(style=_ROW_STYLE, children=[
            _td("Mass", bold=True), _td("8.4M", mono=True),
            _td("$1,200", mono=True), _td("2.1"),
            _td("32"), _status_td("Medium", "Low"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Affluent", bold=True), _td("3.2M", mono=True),
            _td("$4,800", mono=True), _td("3.4"),
            _td("42"), _status_td("Low", "Healthy"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("HNW", bold=True), _td("0.8M", mono=True),
            _td("$24,500", mono=True), _td("5.2"),
            _td("48"), _status_td("Low", "Healthy"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("UHNW", bold=True), _td("0.1M", mono=True),
            _td("$142,000", mono=True), _td("7.8"),
            _td("52"), _status_td("Low", "Healthy"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("SMB", bold=True), _td("1.4M", mono=True),
            _td("$8,400", mono=True), _td("4.1"),
            _td("38"), _status_td("Medium", "Low"),
        ]),
        html.Tr(style=_ROW_STYLE, children=[
            _td("Corporate", bold=True), _td("0.3M", mono=True),
            _td("$48,000", mono=True), _td("6.4"),
            _td("44"), _status_td("Low", "Healthy"),
        ]),
    ]

    segment_table = html.Div(style=_SECTION_CARD_STYLE, children=[
        html.Div("Customer Segments", style={
            "fontSize": "15px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px"}),
        _build_table(segment_headers, segment_rows),
    ])

    return html.Div(children=[
        _page_header("Customer & Distribution",
                     "Segment analytics, lifetime value & distribution channel performance"),
        html.Div(className="content-area", children=[kpi_cards, segment_table]),
    ])
