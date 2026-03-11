"""Risk vertical pages for Blueprint IQ.

Seven page renderers covering enterprise risk management:
  - Dashboard (executive overview)
  - Enterprise Risk (heatmap & controls)
  - Credit Risk (portfolio table)
  - Market Risk (VaR forecast)
  - Operational Risk (incident alerts)
  - Compliance (regulatory table)
  - Cyber Risk (security grid)
"""

from app.page_styles import (
    dark_chart_layout, CHART_CONFIG, ACCENT_ICONS,
    page_header, hero_metric, compact_kpi, kpi_strip, filter_bar,
    tab_bar, info_banner, alert_card, progress_row, stat_card,
    breakdown_list,
    trend_indicator, use_case_badges, donut_figure,
    layout_executive, layout_table, layout_split, layout_alerts,
    layout_forecast, layout_grid,
    gauge_figure, sparkline_figure, metric_with_sparkline,
    _card, _hex_to_rgb,
)
from app.theme import COLORS, FONT_FAMILY
from dash import dcc, html
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
#  1. DASHBOARD  (Style A — layout_executive)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive risk dashboard with hero metrics, trend chart, and panels."""

    # --- Hero metrics -------------------------------------------------------
    heroes = [
        hero_metric("Total Value at Risk", "$284M",
                     trend_text="3.2% vs prior month", trend_dir="up",
                     accent="red"),
        hero_metric("Risk-Adjusted Capital", "$1.92B",
                     trend_text="1.8% improvement", trend_dir="up",
                     accent="blue"),
        hero_metric("Composite Risk Score", "72 / 100",
                     trend_text="4 pts from last quarter", trend_dir="down",
                     accent="yellow"),
    ]

    # --- Main chart: risk exposure trends -----------------------------------
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    credit_vals = [180, 195, 210, 205, 220, 215, 225, 240, 235, 250, 245, 260]
    market_vals = [120, 115, 130, 125, 140, 135, 145, 150, 142, 155, 148, 158]
    ops_vals    = [45, 50, 48, 55, 52, 60, 58, 62, 65, 60, 68, 72]

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=months, y=credit_vals, name="Credit Risk",
        line=dict(color=COLORS["blue"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=months, y=market_vals, name="Market Risk",
        line=dict(color=COLORS["purple"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['purple'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=months, y=ops_vals, name="Operational Risk",
        line=dict(color=COLORS["yellow"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['yellow'])}, 0.08)",
    ))
    fig_trend.update_layout(**dark_chart_layout(
        height=320,
        title=dict(text="Risk Exposure Trends ($M)", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Exposure ($M)", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_trend, config=CHART_CONFIG,
                           style={"height": "320px"})

    # --- Bottom panels ------------------------------------------------------
    # Panel 1: risk category donut
    donut = donut_figure(
        labels=["Credit", "Market", "Operational", "Compliance", "Cyber"],
        values=[42, 25, 15, 10, 8],
        colors=[COLORS["blue"], COLORS["purple"], COLORS["yellow"],
                COLORS["green"], COLORS["red"]],
        center_text="100%",
        title="Risk Distribution",
    )
    panel_donut = dcc.Graph(figure=donut, config=CHART_CONFIG,
                            style={"height": "280px"})

    # Panel 2: capital adequacy bar chart
    fig_cap = go.Figure()
    categories = ["Tier 1", "Tier 2", "Total Capital", "Leverage"]
    required  = [6.0, 2.0, 8.0, 3.0]
    actual    = [12.4, 3.8, 16.2, 5.1]
    fig_cap.add_trace(go.Bar(
        x=categories, y=required, name="Regulatory Min",
        marker_color=COLORS["border"], text=[f"{v}%" for v in required],
        textposition="outside", textfont=dict(color=COLORS["text_muted"], size=10),
    ))
    fig_cap.add_trace(go.Bar(
        x=categories, y=actual, name="Actual",
        marker_color=COLORS["green"], text=[f"{v}%" for v in actual],
        textposition="outside", textfont=dict(color=COLORS["green"], size=10),
    ))
    fig_cap.update_layout(**dark_chart_layout(
        height=280, barmode="group",
        title=dict(text="Capital Adequacy Ratios", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Ratio %", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    panel_capital = dcc.Graph(figure=fig_cap, config=CHART_CONFIG,
                              style={"height": "280px"})

    panels = [
        ("Risk Category Distribution", panel_donut),
        ("Capital Adequacy", panel_capital),
    ]

    return layout_executive(
        title="Risk Dashboard",
        subtitle="Enterprise-wide risk exposure and capital adequacy overview",
        heroes=heroes,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. ENTERPRISE RISK  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_enterprise_risk(cfg):
    """Enterprise risk view with heatmap tabs, stacked bar, and donut."""

    banner_text = (
        "Enterprise risk posture is ELEVATED. 3 risk categories exceed appetite "
        "thresholds: Credit concentration in commercial real estate (+12% over "
        "limit), Market volatility in emerging-market FX, and Cyber threat "
        "level raised to HIGH following Q1 threat intelligence briefing."
    )

    # ── TAB 1: Heatmap ──────────────────────────────────────────────────
    categories = ["Strategic", "Financial", "Operational", "Compliance", "Reputational"]
    high_vals   = [8, 12, 6, 4, 3]
    medium_vals = [15, 18, 14, 10, 7]
    low_vals    = [22, 10, 20, 16, 12]

    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        x=categories, y=high_vals, name="High",
        marker_color=COLORS["red"],
    ))
    fig_bar.add_trace(go.Bar(
        x=categories, y=medium_vals, name="Medium",
        marker_color=COLORS["yellow"],
    ))
    fig_bar.add_trace(go.Bar(
        x=categories, y=low_vals, name="Low",
        marker_color=COLORS["green"],
    ))
    fig_bar.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="Risk Events", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    heatmap_left = dcc.Graph(figure=fig_bar, config=CHART_CONFIG,
                             style={"height": "300px"})

    donut_sev = donut_figure(
        labels=["Critical", "High", "Medium", "Low", "Informational"],
        values=[8, 33, 64, 80, 25],
        colors=[COLORS["red"], "#FF6B6B", COLORS["yellow"],
                COLORS["green"], COLORS["blue"]],
        center_text="210",
        title="Risks by Severity",
    )
    heatmap_right = dcc.Graph(figure=donut_sev, config=CHART_CONFIG,
                              style={"height": "300px"})

    tab_heatmap = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Risk Events by Category", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), heatmap_left], padding="20px"),
            _card([html.Div("Severity Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), heatmap_right], padding="20px"),
        ],
    )

    # ── TAB 2: Trends ────────────────────────────────────────────────────
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar"]
    fig_trends = go.Figure()
    fig_trends.add_trace(go.Scatter(
        x=months, y=[180, 190, 195, 200, 205, 198, 208, 212, 210],
        name="Total Risk Events", mode="lines+markers",
        line=dict(color=COLORS["blue"], width=2), marker=dict(size=5),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_trends.add_trace(go.Scatter(
        x=months, y=[28, 32, 30, 35, 38, 36, 40, 42, 41],
        name="High + Critical", mode="lines+markers",
        line=dict(color=COLORS["red"], width=2),
        marker=dict(size=5),
    ))
    fig_trends.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Count", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    trends_left = dcc.Graph(figure=fig_trends, config=CHART_CONFIG,
                            style={"height": "300px"})

    # Risk appetite utilization by category
    fig_appetite = go.Figure()
    appetite_cats = ["Credit", "Market", "Operational", "Compliance", "Cyber"]
    utilization = [112, 94, 78, 65, 88]
    fig_appetite.add_trace(go.Bar(
        x=appetite_cats, y=utilization,
        marker_color=[COLORS["red"] if v > 100 else COLORS["yellow"] if v > 80
                      else COLORS["green"] for v in utilization],
        text=[f"{v}%" for v in utilization],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_appetite.add_hline(y=100, line_dash="dash", line_color=COLORS["red"],
                           opacity=0.6, annotation_text="Appetite Limit (100%)",
                           annotation_font_color=COLORS["red"],
                           annotation_font_size=10)
    fig_appetite.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="Appetite Utilization %", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"],
                   range=[0, 130]),
        margin=dict(l=48, r=24, t=24, b=48),
    ))
    trends_right = dcc.Graph(figure=fig_appetite, config=CHART_CONFIG,
                             style={"height": "300px"})

    tab_trends = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Risk Event Trend (9 Months)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), trends_left], padding="20px"),
            _card([html.Div("Risk Appetite Utilization", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), trends_right], padding="20px"),
        ],
    )

    # ── TAB 3: Controls ──────────────────────────────────────────────────
    control_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Effective", "Partially Effective", "Ineffective", "Not Tested"],
            values=[128, 38, 12, 6],
            colors=[COLORS["green"], COLORS["yellow"], COLORS["red"],
                    COLORS["text_muted"]],
            center_text="184",
            title="Control Effectiveness",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    # Control testing progress by quarter
    qtrs = ["Q1 '25", "Q2 '25", "Q3 '25", "Q4 '25", "Q1 '26"]
    fig_ctrl = go.Figure()
    fig_ctrl.add_trace(go.Bar(
        x=qtrs, y=[42, 48, 45, 49, 38], name="Controls Tested",
        marker_color=COLORS["blue"],
    ))
    fig_ctrl.add_trace(go.Scatter(
        x=qtrs, y=[88, 90, 92, 94, 93], name="Pass Rate %",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_ctrl.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Tests Conducted", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        yaxis2=dict(title="Pass Rate %", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"], range=[70, 100]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    controls_right = dcc.Graph(figure=fig_ctrl, config=CHART_CONFIG,
                               style={"height": "300px"})

    tab_controls = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Control Effectiveness Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), control_donut], padding="20px"),
            _card([html.Div("Control Testing & Pass Rate", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), controls_right], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Total Risks", "210", "blue"),
        ("Open Mitigations", "47", "yellow"),
        ("Controls Tested", "184", "green"),
        ("Appetite Breaches", "3", "red"),
        ("Avg Time to Close", "18 days", "purple"),
    ]

    return layout_split(
        title="Enterprise Risk Management",
        subtitle="Consolidated risk register and control effectiveness",
        tab_contents=[
            ("Heatmap", tab_heatmap),
            ("Trends", tab_trends),
            ("Controls", tab_controls),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. CREDIT RISK  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_credit_risk(cfg):
    """Credit risk portfolio table with filters and KPI strip."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Portfolio", "options": ["All Portfolios", "Commercial", "Retail", "Sovereign"]},
        {"label": "Rating", "options": ["All Ratings", "AAA-AA", "A-BBB", "BB-B", "CCC & Below"]},
        {"label": "Sector", "options": ["All Sectors", "Financials", "Energy", "Technology", "Healthcare"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Default Rate", "value": "1.24%", "accent": "red"},
        {"label": "Loss Given Default", "value": "42.8%", "accent": "yellow"},
        {"label": "Total Exposure", "value": "$48.3B", "accent": "blue"},
        {"label": "Coverage Ratio", "value": "156%", "accent": "green"},
        {"label": "NPL Ratio", "value": "2.1%", "accent": "purple"},
    ]

    # --- Table data ---------------------------------------------------------
    headers = ["Portfolio", "Exposure", "PD", "LGD", "EL",
               "Rating", "Risk Weight", "Status"]

    portfolios = [
        ("Commercial RE", "$8.4B", "2.30%", "45%", "$86.9M",
         "BBB+", 72, "Warning"),
        ("Corporate Lending", "$12.1B", "0.85%", "38%", "$39.1M",
         "A-", 88, "Healthy"),
        ("Retail Mortgages", "$15.6B", "0.42%", "22%", "$14.4M",
         "AA-", 95, "Healthy"),
        ("SME Portfolio", "$4.2B", "3.10%", "52%", "$67.7M",
         "BB+", 58, "Critical"),
        ("Sovereign Bonds", "$3.8B", "0.05%", "15%", "$0.3M",
         "AAA", 98, "Healthy"),
        ("Trade Finance", "$2.9B", "1.75%", "40%", "$20.3M",
         "BBB", 75, "Warning"),
        ("Consumer Credit", "$6.1B", "1.92%", "48%", "$56.2M",
         "BBB-", 65, "Warning"),
        ("Infrastructure", "$3.2B", "0.68%", "35%", "$7.6M",
         "A", 85, "Healthy"),
    ]

    columns = [
        {"name": "Portfolio", "id": "portfolio"},
        {"name": "Exposure", "id": "exposure"},
        {"name": "PD", "id": "pd"},
        {"name": "LGD", "id": "lgd"},
        {"name": "EL", "id": "el"},
        {"name": "Rating", "id": "rating"},
        {"name": "Risk Weight", "id": "risk_weight"},
        {"name": "Status", "id": "status"},
    ]

    data = []
    for name, exp, pd_val, lgd, el, rating, rw, status in portfolios:
        data.append({
            "portfolio": name,
            "exposure": exp,
            "pd": pd_val,
            "lgd": lgd,
            "el": el,
            "rating": rating,
            "risk_weight": f"{rw}%",
            "status": status,
        })

    return layout_table(
        title="Credit Risk",
        subtitle="Portfolio credit quality, default probabilities, and loss estimates",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. MARKET RISK  (Style E — layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_market_risk(cfg):
    """Market risk with VaR forecast, dual-axis chart, and breakdown."""

    # --- KPIs ---------------------------------------------------------------
    kpis = [
        {"label": "VaR (95%)", "value": "$142M", "accent": "red"},
        {"label": "VaR (99%)", "value": "$218M", "accent": "red"},
        {"label": "Expected Shortfall", "value": "$287M", "accent": "yellow"},
        {"label": "Portfolio Beta", "value": "1.12", "accent": "blue"},
    ]

    # --- Main chart: VaR vs actual P&L (dual axis) -------------------------
    days = list(range(1, 61))
    import math
    var_95 = [142 + 8 * math.sin(d / 5) + (d % 7) * 1.2 for d in days]
    actual_pnl = [
        -35 + 20 * math.sin(d / 3.5) + (d % 11 - 5) * 4.5
        for d in days
    ]

    fig_var = go.Figure()
    fig_var.add_trace(go.Scatter(
        x=days, y=var_95, name="VaR 95% ($M)",
        line=dict(color=COLORS["red"], width=2, dash="dash"),
        yaxis="y",
    ))
    fig_var.add_trace(go.Scatter(
        x=days, y=[-v for v in var_95], name="-VaR 95%",
        line=dict(color=COLORS["red"], width=1, dash="dot"),
        yaxis="y", showlegend=False,
    ))
    fig_var.add_trace(go.Bar(
        x=days, y=actual_pnl, name="Actual P&L ($M)",
        marker_color=[COLORS["green"] if v >= 0 else COLORS["red"] for v in actual_pnl],
        opacity=0.7, yaxis="y2",
    ))
    fig_var.update_layout(**dark_chart_layout(
        height=300,
        xaxis=dict(title="Trading Day", showgrid=False),
        yaxis=dict(title="VaR ($M)", showgrid=True, gridcolor=COLORS["border"],
                   side="left"),
        yaxis2=dict(title="P&L ($M)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
        barmode="overlay",
    ))
    main_chart = dcc.Graph(figure=fig_var, config=CHART_CONFIG,
                           style={"height": "300px"})

    # --- Side breakdown by asset class --------------------------------------
    side = breakdown_list([
        {"label": "Equities", "value": "$62M", "pct": 44, "color": COLORS["blue"]},
        {"label": "Fixed Income", "value": "$31M", "pct": 22, "color": COLORS["green"]},
        {"label": "FX", "value": "$24M", "pct": 17, "color": COLORS["purple"]},
        {"label": "Commodities", "value": "$15M", "pct": 11, "color": COLORS["yellow"]},
        {"label": "Derivatives", "value": "$10M", "pct": 7, "color": COLORS["red"]},
    ])

    return layout_forecast(
        title="Market Risk",
        subtitle="Value at Risk analysis and P&L back-testing",
        kpi_items=kpis,
        hero_value="$142M",
        hero_label="Daily Value at Risk (95% confidence)",
        hero_trend_text="+5.2% vs 30-day average",
        main_chart=main_chart,
        side_component=html.Div([
            html.Div("VaR Contribution by Asset Class",
                     style={"fontSize": "14px", "fontWeight": "600",
                            "color": COLORS["white"], "marginBottom": "16px"}),
            side,
        ]),
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. OPERATIONAL RISK  (Style D — layout_alerts)
# ═══════════════════════════════════════════════════════════════════════════

def render_operational_risk(cfg):
    """Operational risk incident cards with tabs and severity levels."""

    tabs = ["Active Incidents", "Investigating", "Closed"]

    summary_kpis = [
        {"label": "Open Incidents", "value": "14", "accent": "red"},
        {"label": "Under Investigation", "value": "8", "accent": "yellow"},
        {"label": "Avg Resolution", "value": "4.2 days", "accent": "blue"},
        {"label": "Total Losses YTD", "value": "$18.7M", "accent": "red"},
    ]

    alerts = [
        {
            "severity": "critical",
            "title": "Core Banking System Outage",
            "description": (
                "Primary transaction processing system experienced an unplanned "
                "outage impacting all retail and commercial banking channels. "
                "Failover to secondary data center initiated. Estimated 142,000 "
                "transactions affected."
            ),
            "impact": "Estimated Loss: $4.2M",
            "timestamp": "2 hours ago",
            "details": [
                ("Incident ID", "OPS-2024-0847"),
                ("Duration", "3h 42m (ongoing)"),
                ("Systems Affected", "Core Banking, ATM Network, Mobile App"),
                ("Recovery ETA", "Next 60 minutes"),
                ("Escalation", "Level 3 — CRO notified"),
            ],
        },
        {
            "severity": "critical",
            "title": "Unauthorized Wire Transfer Attempt",
            "description": (
                "Fraud detection flagged a series of wire transfer requests "
                "totaling $8.6M from three dormant corporate accounts. "
                "Transactions halted pending investigation. Potential social "
                "engineering attack on operations staff."
            ),
            "impact": "Potential Exposure: $8.6M",
            "timestamp": "5 hours ago",
            "details": [
                ("Incident ID", "OPS-2024-0846"),
                ("Accounts Involved", "3 corporate dormant accounts"),
                ("Transfer Destinations", "Offshore — under review"),
                ("Status", "Blocked — Forensics engaged"),
            ],
        },
        {
            "severity": "warning",
            "title": "Trade Settlement Process Error",
            "description": (
                "Automated trade settlement engine mismatched 47 equity trades "
                "due to a timestamp synchronization issue between matching "
                "engines. Manual reconciliation in progress."
            ),
            "impact": "Estimated Loss: $1.8M",
            "timestamp": "1 day ago",
            "details": [
                ("Incident ID", "OPS-2024-0843"),
                ("Trades Affected", "47 equity trades"),
                ("Root Cause", "NTP drift on matching engine cluster"),
                ("Status", "Reconciliation 68% complete"),
            ],
        },
        {
            "severity": "warning",
            "title": "Third-Party Vendor Data Leak",
            "description": (
                "Cloud analytics vendor reported a potential data exposure "
                "affecting customer segmentation data. 12,400 records may have "
                "been accessible via a misconfigured API endpoint for 72 hours."
            ),
            "impact": "Records Exposed: 12,400",
            "timestamp": "2 days ago",
            "details": [
                ("Incident ID", "OPS-2024-0841"),
                ("Vendor", "DataInsight Analytics (Tier-2)"),
                ("Data Type", "Customer segmentation — non-PII"),
                ("Regulatory Filing", "Pending legal review"),
            ],
        },
        {
            "severity": "info",
            "title": "Compliance Document Version Mismatch",
            "description": (
                "Automated compliance checking identified 3 policy documents "
                "running on deprecated v2.1 templates instead of current v3.0. "
                "Low impact but requires remediation before next audit cycle."
            ),
            "impact": "Audit Risk: Low",
            "timestamp": "3 days ago",
            "details": [
                ("Incident ID", "OPS-2024-0839"),
                ("Documents", "AML Policy, KYC Procedures, BCM Plan"),
                ("Remediation Deadline", "End of quarter"),
            ],
        },
    ]

    # ── Build tab contents using new API ───────────────────────────────
    active_cards = [alert_card(**a) for a in alerts
                    if a["severity"] in ("critical", "warning")]
    investigating_cards = [alert_card(**a) for a in alerts
                           if a["severity"] == "warning"] + [
        alert_card(severity="warning",
                   title="Suspicious Login Pattern — Wealth Management Portal",
                   description="Anomalous login activity detected from 3 IP addresses in Eastern Europe targeting high-net-worth client accounts. MFA challenges triggered; no successful breaches confirmed yet.",
                   timestamp="18 hours ago"),
        alert_card(severity="info",
                   title="Reconciliation Discrepancy — Custody Accounts",
                   description="Automated reconciliation identified $2.4M discrepancy across 12 custody accounts. Preliminary analysis points to timing differences in cross-border settlement. Under review.",
                   timestamp="1 day ago"),
    ]
    closed_cards = [
        alert_card(severity="healthy",
                   title="Payment Gateway Timeout — Resolved",
                   description="Intermittent timeouts on payment processing gateway traced to expired connection pool settings. Configuration updated and validated. No customer impact after fix deployed.",
                   timestamp="3 days ago"),
        alert_card(severity="healthy",
                   title="Data Center Power Event — Resolved",
                   description="Brief UPS switchover at secondary data center completed without service interruption. Generator tested and confirmed operational. Root cause: scheduled utility maintenance.",
                   timestamp="5 days ago"),
        alert_card(severity="healthy",
                   title="Regulatory Report Resubmission — Complete",
                   description="Corrected CCAR supplemental data submission accepted by the Federal Reserve. Original error was a rounding issue in projected loss estimates. No further action required.",
                   timestamp="1 week ago"),
        alert_card(severity="info",
                   title="Vendor Risk Assessment Cycle — Completed",
                   description="Annual vendor risk assessment for all Tier-1 and Tier-2 vendors completed on schedule. 3 vendors flagged for enhanced monitoring; remediation plans in place.",
                   timestamp="2 weeks ago"),
    ]

    tab_contents = [
        ("Active Incidents", html.Div(active_cards)),
        ("Investigating", html.Div(investigating_cards)),
        ("Closed", html.Div(closed_cards)),
    ]

    return layout_alerts(
        title="Operational Risk",
        subtitle="Active incidents, investigations, and operational loss tracking",
        tab_contents=tab_contents,
        summary_kpis=summary_kpis,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. COMPLIANCE  (Style B variant — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_compliance(cfg):
    """Regulatory compliance table with filters and progress tracking."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Regulation", "options": ["All Regulations", "Basel III/IV", "Dodd-Frank", "MiFID II", "GDPR", "SOX"]},
        {"label": "Status", "options": ["All Statuses", "Compliant", "In Progress", "Non-Compliant", "Under Review"]},
        {"label": "Department", "options": ["All Departments", "Risk", "Legal", "IT", "Operations", "Finance"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Compliance Score", "value": "94.2%", "accent": "green"},
        {"label": "Open Findings", "value": "23", "accent": "yellow"},
        {"label": "Remediation Rate", "value": "87%", "accent": "blue"},
        {"label": "Audit Status", "value": "On Track", "accent": "green"},
        {"label": "Regulatory Changes", "value": "12 Pending", "accent": "purple"},
    ]

    # --- Table data ---------------------------------------------------------
    headers = ["Regulation", "Requirement", "Owner", "Due Date",
               "Compliance", "Status"]

    requirements = [
        ("Basel III CET1", "Maintain minimum 4.5% CET1 ratio",
         "Capital Mgmt", "Ongoing", 100, "Healthy"),
        ("Basel III LCR", "Liquidity coverage ratio >= 100%",
         "Treasury", "Ongoing", 100, "Healthy"),
        ("Dodd-Frank 165", "Enhanced prudential standards — stress testing",
         "Risk Analytics", "2024-Q2", 92, "Healthy"),
        ("MiFID II RTS 25", "Best execution reporting obligations",
         "Trading Ops", "2024-03-31", 78, "Warning"),
        ("GDPR Art. 30", "Records of processing activities",
         "Data Privacy", "2024-06-30", 65, "Warning"),
        ("SOX Section 404", "Internal controls over financial reporting",
         "Internal Audit", "2024-Q1", 88, "Healthy"),
        ("AML / 6AMLD", "Enhanced due diligence for high-risk customers",
         "Compliance", "2024-04-15", 45, "Critical"),
        ("BCBS 239", "Risk data aggregation and reporting",
         "Risk IT", "2024-Q3", 58, "Warning"),
        ("DORA", "Digital operational resilience — ICT risk framework",
         "IT Security", "2025-01-17", 35, "Critical"),
        ("ESG / SFDR", "Sustainability-related disclosures",
         "ESG Office", "2024-06-30", 72, "Warning"),
    ]

    columns = [
        {"name": "Regulation", "id": "regulation"},
        {"name": "Requirement", "id": "requirement"},
        {"name": "Owner", "id": "owner"},
        {"name": "Due Date", "id": "due_date"},
        {"name": "Compliance", "id": "compliance"},
        {"name": "Status", "id": "status"},
    ]

    data = []
    for reg, req, owner, due, comp_pct, status in requirements:
        data.append({
            "regulation": reg,
            "requirement": req,
            "owner": owner,
            "due_date": due,
            "compliance": f"{comp_pct}%",
            "status": status,
        })

    return layout_table(
        title="Regulatory Compliance",
        subtitle="Compliance status, audit findings, and regulatory change management",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. CYBER RISK  (Style F — layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_cyber_risk(cfg):
    """Cyber risk grid with gauges, sparklines, and security metrics."""

    # --- Grid item 1: Threat Level gauge (tall, spans 2 rows) ---------------
    gauge_threat = gauge_figure(
        value=78, max_val=100, title="Threat Level Index",
        color=COLORS["red"],
    )
    threat_panel = html.Div([
        dcc.Graph(figure=gauge_threat, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "12px"},
            children=[
                html.Div("HIGH", style={
                    "fontSize": "18px", "fontWeight": "700",
                    "color": COLORS["red"], "letterSpacing": "1px",
                }),
                html.Div("Elevated since Mar 2", style={
                    "fontSize": "12px", "color": COLORS["text_muted"],
                    "marginTop": "4px",
                }),
            ],
        ),
        html.Div(
            style={"marginTop": "20px"},
            children=[
                progress_row("External Threats", "Critical", 85, COLORS["red"]),
                progress_row("Insider Threats", "Moderate", 45, COLORS["yellow"]),
                progress_row("Supply Chain", "Elevated", 62, COLORS["red"]),
            ],
        ),
    ])

    # --- Grid item 2: Attack Frequency sparkline ----------------------------
    attack_data = [120, 135, 142, 128, 155, 180, 210, 195, 230, 245,
                   220, 260, 275, 290, 265, 310, 285, 320, 305, 340]
    attack_panel = metric_with_sparkline(
        "Attack Frequency (24h)", "1,847",
        attack_data, accent="red",
    )

    # --- Grid item 3: Blocked Threats metric --------------------------------
    blocked_data = [98, 102, 110, 105, 115, 120, 118, 125, 130, 128,
                    135, 140, 138, 142, 148, 145, 150, 155, 152, 160]
    blocked_panel = metric_with_sparkline(
        "Blocked Threats (24h)", "1,694",
        blocked_data, accent="green",
    )

    # --- Grid item 4: Vulnerability summary (wide, spans 2 cols) -----------
    vuln_categories = ["Critical", "High", "Medium", "Low", "Informational"]
    vuln_counts     = [12, 47, 183, 342, 89]
    vuln_colors     = [COLORS["red"], "#FF6B6B", COLORS["yellow"],
                       COLORS["green"], COLORS["blue"]]

    fig_vuln = go.Figure(go.Bar(
        x=vuln_categories, y=vuln_counts,
        marker_color=vuln_colors,
        text=vuln_counts, textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_vuln.update_layout(**dark_chart_layout(
        height=200, margin=dict(l=40, r=20, t=36, b=36),
        title=dict(text="Open Vulnerabilities by Severity",
                   font=dict(size=13, color=COLORS["white"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"]),
    ))
    vuln_panel = html.Div([
        dcc.Graph(figure=fig_vuln, config=CHART_CONFIG,
                  style={"height": "200px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "marginTop": "8px", "padding": "0 4px"},
            children=[
                html.Div([
                    html.Span("Total: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("673", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["white"]}),
                ]),
                html.Div([
                    html.Span("SLA Breached: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("18", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["red"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 5: SIEM Alerts metric ------------------------------------
    siem_data = [450, 480, 520, 510, 490, 530, 560, 540, 580, 620,
                 600, 590, 640, 670, 650, 710, 690, 730, 720, 750]
    siem_panel = metric_with_sparkline(
        "SIEM Alerts (24h)", "4,218",
        siem_data, accent="yellow",
    )

    # --- Grid item 6: Incident Response Time --------------------------------
    response_gauge = gauge_figure(
        value=28, max_val=120, title="Avg Response Time (min)",
        color=COLORS["green"],
    )
    response_panel = html.Div([
        dcc.Graph(figure=response_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-around",
                   "marginTop": "8px"},
            children=[
                html.Div([
                    html.Div("Target", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("< 30 min", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Div("P95", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("52 min", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["yellow"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 7: Patch Compliance --------------------------------------
    patch_gauge = gauge_figure(
        value=91, max_val=100, title="Patch Compliance %",
        color=COLORS["blue"],
    )
    patch_panel = html.Div([
        dcc.Graph(figure=patch_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"marginTop": "12px"},
            children=[
                progress_row("Critical Patches", "98%", 98, COLORS["green"]),
                progress_row("High Patches", "94%", 94, COLORS["green"]),
                progress_row("Medium Patches", "87%", 87, COLORS["yellow"]),
                progress_row("Low Patches", "78%", 78, COLORS["yellow"]),
            ],
        ),
    ])

    # --- Assemble grid ------------------------------------------------------
    grid_items = [
        {"col_span": 1, "row_span": 2, "content": threat_panel},
        {"col_span": 1, "row_span": 1, "content": attack_panel},
        {"col_span": 1, "row_span": 1, "content": blocked_panel},
        {"col_span": 2, "row_span": 1, "content": vuln_panel},
        {"col_span": 1, "row_span": 1, "content": siem_panel},
        {"col_span": 1, "row_span": 1, "content": response_panel},
        {"col_span": 1, "row_span": 1, "content": patch_panel},
    ]

    return layout_grid(
        title="Cyber Risk",
        subtitle="Threat intelligence, vulnerability management, and security posture",
        grid_items=grid_items,
    )
