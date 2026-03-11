"""Financial Services vertical pages for the FinancialIQ application.

Covers Capital Markets with seven page renderers: dashboard,
investment_alpha, trading_advisory, risk_management, regulatory,
fraud_cyber, and operations.

Each renderer accepts a ``cfg`` dict and returns an ``html.Div``.
All charts call ``dark_chart_layout()`` for consistent dark theming.
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
# 1. DASHBOARD — layout_executive (Style A)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive dashboard with AUM, Revenue, Risk Score heroes,
    portfolio performance chart, asset allocation donut, and revenue
    breakdown bar chart."""

    # ── Hero metrics ──────────────────────────────────────────────────
    heroes = [
        hero_metric("Assets Under Management", "$847.3B",
                     trend_text="+3.2% QoQ", trend_dir="up", accent="blue"),
        hero_metric("Total Revenue", "$12.6B",
                     trend_text="+5.8% YoY", trend_dir="up", accent="green"),
        hero_metric("Composite Risk Score", "72 / 100",
                     trend_text="-4 pts from prior quarter", trend_dir="down",
                     accent="yellow"),
    ]

    # ── Main chart: portfolio performance line ────────────────────────
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    portfolio_vals = [100, 103.2, 101.8, 106.4, 109.1, 107.5,
                      112.3, 115.8, 113.6, 118.2, 121.7, 124.4]
    benchmark_vals = [100, 101.5, 100.9, 103.7, 105.2, 104.1,
                      107.8, 109.6, 108.4, 111.3, 113.1, 114.9]

    perf_fig = go.Figure()
    perf_fig.add_trace(go.Scatter(
        x=months, y=portfolio_vals, name="Portfolio",
        mode="lines+markers",
        line=dict(color=COLORS["blue"], width=2),
        marker=dict(size=5),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    perf_fig.add_trace(go.Scatter(
        x=months, y=benchmark_vals, name="S&P 500 Benchmark",
        mode="lines",
        line=dict(color=COLORS["text_muted"], width=1.5, dash="dash"),
    ))
    perf_fig.update_layout(**dark_chart_layout(
        height=320,
        title=dict(text="Portfolio Performance (Indexed to 100)",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="Index Value"),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=perf_fig, config=CHART_CONFIG,
                           style={"width": "100%"})

    # ── Panel 1: Asset allocation donut ───────────────────────────────
    alloc_labels = ["Equities", "Fixed Income", "Alternatives",
                    "Real Estate", "Cash"]
    alloc_values = [42, 28, 15, 10, 5]
    alloc_colors = [COLORS["blue"], COLORS["green"], COLORS["purple"],
                    COLORS["yellow"], COLORS["text_muted"]]
    alloc_fig = donut_figure(alloc_labels, alloc_values, alloc_colors,
                             center_text="$847B", title="Asset Allocation")
    alloc_chart = dcc.Graph(figure=alloc_fig, config=CHART_CONFIG,
                            style={"width": "100%"})

    # ── Panel 2: Revenue by business line bar chart ───────────────────
    lines = ["Wealth Mgmt", "IB Advisory", "Trading", "Asset Mgmt", "Other"]
    rev_vals = [3.8, 3.1, 2.9, 1.9, 0.9]
    rev_fig = go.Figure(go.Bar(
        x=lines, y=rev_vals,
        marker_color=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                      COLORS["yellow"], COLORS["text_muted"]],
        text=[f"${v}B" for v in rev_vals],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    rev_fig.update_layout(**dark_chart_layout(
        height=280,
        title=dict(text="Revenue by Business Line",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="$ Billions"),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        margin=dict(l=48, r=24, t=40, b=60),
    ))
    rev_chart = dcc.Graph(figure=rev_fig, config=CHART_CONFIG,
                          style={"width": "100%"})

    panels = [
        ("Asset Allocation", alloc_chart),
        ("Revenue by Business Line", rev_chart),
    ]

    return layout_executive(
        title="Financial Services Dashboard",
        subtitle="Enterprise-wide performance across Capital Markets, "
                 "Wealth Management, and Investment Banking",
        heroes=heroes,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2. INVESTMENT & ALPHA — layout_forecast (Style E)
# ═══════════════════════════════════════════════════════════════════════════

def render_investment_alpha(cfg):
    """Investment performance page with KPIs, hero YTD returns, dual-axis
    performance vs benchmark chart, and strategy breakdown sidebar."""

    # ── KPI strip ─────────────────────────────────────────────────────
    kpis = [
        {"label": "Alpha (bps)", "value": "+142", "accent": "green"},
        {"label": "Sharpe Ratio", "value": "1.87", "accent": "blue"},
        {"label": "AUM Deployed", "value": "$623.5B", "accent": "purple"},
        {"label": "Annualised Return", "value": "14.8%", "accent": "green"},
    ]

    # ── Dual-axis chart: performance vs benchmark ─────────────────────
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    fund_ret = [1.2, 0.8, 1.5, -0.3, 2.1, 1.7,
                0.9, 1.4, -0.6, 2.3, 1.1, 1.3]
    bench_ret = [0.9, 0.5, 1.1, -0.1, 1.6, 1.2,
                 0.6, 1.0, -0.4, 1.7, 0.8, 0.9]
    cumul_alpha = []
    running = 0.0
    for f, b in zip(fund_ret, bench_ret):
        running += (f - b)
        cumul_alpha.append(round(running, 2))

    chart_fig = go.Figure()
    chart_fig.add_trace(go.Bar(
        x=months, y=fund_ret, name="Fund Return %",
        marker_color=COLORS["blue"], opacity=0.85,
        yaxis="y",
    ))
    chart_fig.add_trace(go.Bar(
        x=months, y=bench_ret, name="Benchmark %",
        marker_color=COLORS["text_muted"], opacity=0.55,
        yaxis="y",
    ))
    chart_fig.add_trace(go.Scatter(
        x=months, y=cumul_alpha, name="Cumulative Alpha (bps x100)",
        mode="lines+markers",
        line=dict(color=COLORS["green"], width=2),
        marker=dict(size=5),
        yaxis="y2",
    ))
    chart_fig.update_layout(**dark_chart_layout(
        height=320, barmode="group",
        yaxis=dict(title="Monthly Return %", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        yaxis2=dict(title="Cumulative Alpha", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"]),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=chart_fig, config=CHART_CONFIG,
                           style={"width": "100%"})

    # ── Side breakdown by strategy ────────────────────────────────────
    side = breakdown_list([
        {"label": "Long / Short Equity", "value": "+18.2%", "pct": 34,
         "color": COLORS["blue"]},
        {"label": "Global Macro", "value": "+14.6%", "pct": 22,
         "color": COLORS["green"]},
        {"label": "Quant Systematic", "value": "+12.1%", "pct": 18,
         "color": COLORS["purple"]},
        {"label": "Event-Driven", "value": "+9.7%", "pct": 14,
         "color": COLORS["yellow"]},
        {"label": "Fixed Income Arb", "value": "+6.3%", "pct": 12,
         "color": COLORS["text_muted"]},
    ])

    return layout_forecast(
        title="Investment & Alpha Generation",
        subtitle="Fund performance, alpha attribution, and strategy "
                 "breakdown across all managed portfolios",
        kpi_items=kpis,
        hero_value="12.4%",
        hero_label="Year-to-Date Returns",
        hero_trend_text="+3.6% vs benchmark",
        main_chart=main_chart,
        side_component=side,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 3. TRADING & ADVISORY — layout_table (Style B)
# ═══════════════════════════════════════════════════════════════════════════

def render_trading_advisory(cfg):
    """Trading desk view with filters, KPIs, and a rich table of active
    trades showing PnL progress bars and status badges."""

    # ── Filters ───────────────────────────────────────────────────────
    filters = [
        {"label": "Desk", "options": ["All Desks", "Equities", "FICC",
                                       "FX", "Derivatives"]},
        {"label": "Asset Class", "options": ["All Classes", "Equity",
                                              "Fixed Income", "FX",
                                              "Commodities"]},
        {"label": "Strategy", "options": ["All Strategies", "Market Making",
                                           "Prop", "Client Flow",
                                           "Hedging"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────
    kpis = [
        {"label": "Daily Volume", "value": "$24.7B", "accent": "blue"},
        {"label": "Daily PnL", "value": "+$18.4M", "accent": "green"},
        {"label": "Win Rate", "value": "68.3%", "accent": "purple"},
        {"label": "Avg Latency", "value": "0.42ms", "accent": "yellow"},
        {"label": "Fill Rate", "value": "99.7%", "accent": "green"},
    ]

    # ── Trade table data ──────────────────────────────────────────────
    table_columns = [
        {"name": "Trade ID", "id": "trade_id"},
        {"name": "Desk", "id": "desk"},
        {"name": "Instrument", "id": "instrument"},
        {"name": "Side", "id": "side"},
        {"name": "Notional", "id": "notional"},
        {"name": "PnL", "id": "pnl"},
        {"name": "PnL %", "id": "pnl_pct"},
        {"name": "Status", "id": "status"},
    ]

    trade_rows = [
        ("TRD-90124", "Equities", "AAPL 250C 03/21", "Buy",
         "$14.2M", "+$842K", 72, "Healthy"),
        ("TRD-90118", "FICC", "UST 10Y Future", "Sell",
         "$250.0M", "+$1.3M", 85, "Healthy"),
        ("TRD-90115", "FX", "EUR/USD Spot", "Buy",
         "$78.5M", "-$124K", 28, "Warning"),
        ("TRD-90112", "Derivatives", "SPX 5200P 03/28", "Sell",
         "$32.7M", "+$567K", 64, "Healthy"),
        ("TRD-90109", "Equities", "NVDA Equity Swap", "Buy",
         "$95.0M", "+$2.1M", 91, "Healthy"),
        ("TRD-90103", "FICC", "IG CDX Series 42", "Buy",
         "$180.0M", "-$340K", 18, "Critical"),
        ("TRD-90098", "FX", "USD/JPY 1M Fwd", "Sell",
         "$45.0M", "+$215K", 55, "Healthy"),
        ("TRD-90091", "Derivatives", "VIX Call Spread", "Buy",
         "$8.4M", "+$93K", 48, "Low"),
        ("TRD-90087", "Equities", "MSFT Block Trade", "Sell",
         "$62.3M", "+$1.8M", 88, "Healthy"),
        ("TRD-90082", "FICC", "MBS TBA Pool", "Buy",
         "$320.0M", "-$780K", 22, "Warning"),
    ]

    table_data = []
    for tid, desk, instr, side, notional, pnl, pnl_pct, status in trade_rows:
        table_data.append({
            "trade_id": tid,
            "desk": desk,
            "instrument": instr,
            "side": side,
            "notional": notional,
            "pnl": pnl,
            "pnl_pct": f"{pnl_pct}%",
            "status": status,
        })

    return layout_table(
        title="Trading & Advisory",
        subtitle="Real-time trading desk activity, PnL attribution, "
                 "and execution quality metrics",
        filters=filters,
        kpi_items=kpis,
        table_columns=table_columns,
        table_data=table_data,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 4. RISK MANAGEMENT — layout_alerts (Style D)
# ═══════════════════════════════════════════════════════════════════════════

def render_risk_management(cfg):
    """Risk management page with tabs, summary KPIs, and alert cards
    for VaR breaches, concentration risk, and stress test failures."""

    tabs = ["Active Risks", "Breaches", "Historical"]

    summary_kpis = [
        {"label": "Active Alerts", "value": "14", "accent": "red"},
        {"label": "VaR (99% 1D)", "value": "$48.2M", "accent": "yellow"},
        {"label": "Stress Loss", "value": "-$312M", "accent": "red"},
        {"label": "Capital Utilisation", "value": "73.4%", "accent": "blue"},
    ]

    alerts = [
        {
            "severity": "critical",
            "title": "VaR Limit Breach — Equity Desk",
            "description": (
                "The 99th-percentile 1-day VaR for the Equity Trading desk "
                "has exceeded the $25M internal limit by $7.3M.  Current "
                "VaR stands at $32.3M driven by concentrated NVDA and AAPL "
                "delta exposure."
            ),
            "impact": "Potential Loss: $32.3M",
            "details": [
                ("Desk", "Equities — US Large Cap"),
                ("VaR Limit", "$25.0M"),
                ("Current VaR", "$32.3M (129%)"),
                ("Primary Driver", "Single-name delta concentration"),
                ("Breach Duration", "2h 14m"),
            ],
            "timestamp": "14 min ago",
        },
        {
            "severity": "critical",
            "title": "Stress Test Failure — 2008 Replay Scenario",
            "description": (
                "The firm-wide stress test under the 2008 GFC replay "
                "scenario projects a loss of $312M, exceeding the $280M "
                "board-approved threshold by 11.4%.  Credit spread widening "
                "in the IG book is the dominant factor."
            ),
            "impact": "Potential Loss: $312M",
            "details": [
                ("Scenario", "2008 GFC Replay"),
                ("Threshold", "$280M"),
                ("Projected Loss", "$312M (111%)"),
                ("Key Factor", "IG credit spread +180 bps"),
            ],
            "timestamp": "43 min ago",
        },
        {
            "severity": "warning",
            "title": "Concentration Risk — Sector Exposure",
            "description": (
                "Technology sector exposure has reached 34.2% of total "
                "AUM, above the 30% soft limit.  Current allocation is "
                "driven by strong momentum positioning in semiconductor "
                "names across multiple desks."
            ),
            "impact": "Exposure: $289.8B (34.2%)",
            "details": [
                ("Sector", "Information Technology"),
                ("Limit", "30% of AUM"),
                ("Current", "34.2% ($289.8B)"),
                ("Top Holdings", "NVDA, AAPL, MSFT, AVGO"),
            ],
            "timestamp": "1h 12m ago",
        },
        {
            "severity": "warning",
            "title": "Counterparty Credit Downgrade — Lehman RE Fund III",
            "description": (
                "Moody's has placed Lehman RE Fund III on negative watch "
                "following deterioration of underlying CRE collateral.  "
                "Current exposure is $84M in secured lending facilities."
            ),
            "impact": "Exposure: $84M",
            "details": [
                ("Counterparty", "Lehman RE Fund III"),
                ("Rating Action", "Baa2 → Negative Watch"),
                ("Exposure Type", "Secured Lending"),
                ("Collateral LTV", "78% → 91%"),
            ],
            "timestamp": "2h 38m ago",
        },
        {
            "severity": "info",
            "title": "Liquidity Coverage Ratio — Approaching Floor",
            "description": (
                "The 30-day LCR has declined to 118%, approaching the "
                "110% internal floor.  Seasonal outflows in money-market "
                "funds and increased repo activity are contributing to "
                "the drawdown of HQLA buffers."
            ),
            "impact": "LCR: 118% (floor 110%)",
            "details": [
                ("Current LCR", "118%"),
                ("Regulatory Min", "100%"),
                ("Internal Floor", "110%"),
                ("Trend", "Down 7 pts in 5 days"),
            ],
            "timestamp": "3h 05m ago",
        },
    ]

    return layout_alerts(
        title="Risk Management",
        subtitle="Enterprise risk monitoring — VaR, stress testing, "
                 "concentration limits, and counterparty exposure",
        tabs=tabs,
        alerts=alerts,
        summary_kpis=summary_kpis,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 5. REGULATORY & COMPLIANCE — layout_split (Style C)
# ═══════════════════════════════════════════════════════════════════════════

def render_regulatory(cfg):
    """Regulatory compliance with tabs, info banner, capital ratios bar
    chart on the left, compliance status donut on the right, and bottom
    summary stats."""

    tabs = ["Capital", "Reporting", "Audit"]

    banner_text = (
        "All Basel III / IV capital ratios are above regulatory minimums "
        "as of the latest quarterly filing.  Next CCAR submission due in "
        "18 days — pre-filing review is 94% complete."
    )

    # ── Left panel: stacked bar of capital ratios over time ───────────
    quarters = ["Q1 '25", "Q2 '25", "Q3 '25", "Q4 '25"]
    cet1 = [13.2, 13.5, 13.8, 14.1]
    tier1 = [1.8, 1.7, 1.9, 1.8]
    tier2 = [1.5, 1.6, 1.5, 1.4]

    cap_fig = go.Figure()
    cap_fig.add_trace(go.Bar(
        x=quarters, y=cet1, name="CET1",
        marker_color=COLORS["blue"],
    ))
    cap_fig.add_trace(go.Bar(
        x=quarters, y=tier1, name="AT1",
        marker_color=COLORS["purple"],
    ))
    cap_fig.add_trace(go.Bar(
        x=quarters, y=tier2, name="Tier 2",
        marker_color=COLORS["green"],
    ))
    cap_fig.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="Capital Ratio %", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    # Add regulatory minimum line
    cap_fig.add_hline(y=10.5, line_dash="dash",
                      line_color=COLORS["red"], opacity=0.6,
                      annotation_text="Min Total Capital (10.5%)",
                      annotation_font_color=COLORS["red"],
                      annotation_font_size=10)

    left_chart = dcc.Graph(figure=cap_fig, config=CHART_CONFIG,
                           style={"width": "100%"})

    # ── Right panel: compliance status donut ──────────────────────────
    comp_labels = ["Compliant", "Under Review", "Remediation", "Past Due"]
    comp_values = [142, 18, 7, 3]
    comp_colors = [COLORS["green"], COLORS["blue"], COLORS["yellow"],
                   COLORS["red"]]
    comp_fig = donut_figure(comp_labels, comp_values, comp_colors,
                            center_text="170", title="Compliance Items")
    right_chart = dcc.Graph(figure=comp_fig, config=CHART_CONFIG,
                            style={"width": "100%"})

    # ── Bottom stats ──────────────────────────────────────────────────
    bottom_stats = [
        ("CET1 Ratio", "14.1%", "blue"),
        ("Total Capital", "17.3%", "green"),
        ("Leverage Ratio", "6.2%", "purple"),
        ("CCAR Progress", "94%", "blue"),
        ("Open Findings", "3", "red"),
    ]

    return layout_split(
        title="Regulatory & Compliance",
        subtitle="Capital adequacy, regulatory reporting, and audit "
                 "readiness across all jurisdictions",
        tabs=tabs,
        banner_text=banner_text,
        left_panel=("Capital Ratios Over Time", left_chart),
        right_panel=("Compliance Status", right_chart),
        bottom_stats=bottom_stats,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 6. FRAUD & CYBER SECURITY — layout_grid (Style F)
# ═══════════════════════════════════════════════════════════════════════════

def render_fraud_cyber(cfg):
    """Grid-based fraud and cyber security dashboard with gauge, sparkline,
    metric cards, and monitoring panels."""

    # ── Grid item 1: Threat level gauge (1x1) ─────────────────────────
    threat_gauge = gauge_figure(72, 100, title="Threat Level", color=COLORS["red"])
    gauge_panel = html.Div([
        dcc.Graph(figure=threat_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div("ELEVATED", style={
            "textAlign": "center", "fontSize": "13px", "fontWeight": "700",
            "color": COLORS["yellow"], "letterSpacing": "1px",
            "marginTop": "4px",
        }),
    ])

    # ── Grid item 2: Transaction monitoring sparkline (1x1) ──────────
    txn_values = [1240, 1380, 1190, 1560, 1720, 1480, 1890, 2010,
                  1760, 1940, 2130, 1870, 2240, 2080, 2310, 1960,
                  2450, 2280, 2590, 2370]
    txn_panel = metric_with_sparkline(
        "Transaction Monitoring",
        "2,370 / min",
        txn_values,
        accent="blue",
    )

    # ── Grid item 3: Fraud detected metric (tall, 1x2) ───────────────
    fraud_detail_children = [
        html.Div("Fraud Detected (MTD)", style={
            "fontSize": "12px", "color": COLORS["text_muted"],
            "textTransform": "uppercase", "letterSpacing": "0.4px",
            "marginBottom": "12px", "fontWeight": "600",
        }),
        html.Div("$4.8M", style={
            "fontSize": "36px", "fontWeight": "700",
            "color": COLORS["red"], "marginBottom": "4px",
        }),
        html.Div([
            html.Span("847 incidents", style={
                "fontSize": "13px", "color": COLORS["white"],
                "fontWeight": "500",
            }),
            trend_indicator("down", "-12% vs prior month"),
        ], style={"marginBottom": "20px"}),
        progress_row("Card Fraud", "$2.1M", 44, COLORS["red"]),
        progress_row("Wire Fraud", "$1.4M", 29, COLORS["yellow"]),
        progress_row("Account Takeover", "$0.8M", 17, COLORS["purple"]),
        progress_row("Identity Theft", "$0.5M", 10, COLORS["blue"]),
    ]
    fraud_panel = html.Div(fraud_detail_children)

    # ── Grid item 4: False positive rate (1x1) ───────────────────────
    fp_sparkline_vals = [8.2, 7.9, 8.5, 7.4, 6.8, 7.1, 6.5, 6.2,
                         5.9, 5.6, 5.8, 5.3, 4.9, 5.1, 4.7, 4.4]
    fp_panel = metric_with_sparkline(
        "False Positive Rate",
        "4.4%",
        fp_sparkline_vals,
        accent="green",
    )

    # ── Grid item 5: Blocked transactions (wide, 2x1) ────────────────
    blocked_months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
                      "Jan", "Feb", "Mar"]
    blocked_counts = [3420, 3890, 4210, 3780, 4560, 5120, 4870, 5340, 5680]
    blocked_fig = go.Figure(go.Bar(
        x=blocked_months, y=blocked_counts,
        marker_color=COLORS["purple"],
        text=[f"{v:,}" for v in blocked_counts],
        textposition="outside",
        textfont=dict(color=COLORS["text_muted"], size=10),
    ))
    blocked_fig.update_layout(**dark_chart_layout(
        height=160, margin=dict(l=40, r=16, t=28, b=28),
        title=dict(text="Blocked Transactions (Monthly)",
                   font=dict(size=12, color=COLORS["text_muted"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
    ))
    blocked_panel = html.Div([
        dcc.Graph(figure=blocked_fig, config=CHART_CONFIG,
                  style={"height": "160px"}),
        html.Div(style={"display": "flex", "justifyContent": "space-between",
                         "marginTop": "8px"}, children=[
            html.Div([
                html.Div("Total Blocked (MTD)", style={
                    "fontSize": "11px", "color": COLORS["text_muted"],
                    "textTransform": "uppercase"}),
                html.Div("5,680", style={
                    "fontSize": "20px", "fontWeight": "700",
                    "color": COLORS["purple"]}),
            ]),
            html.Div([
                html.Div("Block Rate", style={
                    "fontSize": "11px", "color": COLORS["text_muted"],
                    "textTransform": "uppercase"}),
                html.Div("99.2%", style={
                    "fontSize": "20px", "fontWeight": "700",
                    "color": COLORS["green"]}),
            ]),
        ]),
    ])

    # ── Grid item 6: Investigation backlog (1x1) ─────────────────────
    backlog_children = [
        html.Div("Investigation Backlog", style={
            "fontSize": "11px", "color": COLORS["text_muted"],
            "textTransform": "uppercase", "letterSpacing": "0.4px",
            "marginBottom": "8px", "fontWeight": "600",
        }),
        html.Div("127", style={
            "fontSize": "28px", "fontWeight": "700",
            "color": COLORS["yellow"], "marginBottom": "12px",
        }),
        progress_row("Critical", "12 cases", 9, COLORS["red"]),
        progress_row("High", "34 cases", 27, COLORS["yellow"]),
        progress_row("Medium", "51 cases", 40, COLORS["blue"]),
        progress_row("Low", "30 cases", 24, COLORS["text_muted"]),
    ]
    backlog_panel = html.Div(backlog_children)

    # ── Assemble grid ─────────────────────────────────────────────────
    grid_items = [
        {"col_span": 1, "row_span": 1, "content": gauge_panel},
        {"col_span": 1, "row_span": 1, "content": txn_panel},
        {"col_span": 1, "row_span": 2, "content": fraud_panel},
        {"col_span": 1, "row_span": 1, "content": fp_panel},
        {"col_span": 2, "row_span": 1, "content": blocked_panel},
        {"col_span": 1, "row_span": 1, "content": backlog_panel},
    ]

    return layout_grid(
        title="Fraud & Cyber Security",
        subtitle="Real-time threat monitoring, fraud detection, and "
                 "investigation pipeline across all channels",
        grid_items=grid_items,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 7. OPERATIONS — layout_table (Style B variant)
# ═══════════════════════════════════════════════════════════════════════════

def render_operations(cfg):
    """Operational systems view with filters, KPIs, and a rich table of
    systems showing health progress bars and status badges."""

    # ── Filters ───────────────────────────────────────────────────────
    filters = [
        {"label": "Region", "options": ["All Regions", "Americas", "EMEA",
                                         "APAC"]},
        {"label": "System", "options": ["All Systems", "Core Banking",
                                         "Trading Platform", "Settlement",
                                         "Risk Engine"]},
        {"label": "Priority", "options": ["All", "P1", "P2", "P3", "P4"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────
    kpis = [
        {"label": "STP Rate", "value": "97.8%", "accent": "green"},
        {"label": "Settlement T+1", "value": "99.2%", "accent": "blue"},
        {"label": "System Uptime", "value": "99.97%", "accent": "green"},
        {"label": "Open Incidents", "value": "7", "accent": "yellow"},
        {"label": "Avg MTTR", "value": "23 min", "accent": "purple"},
    ]

    # ── Operations table data ─────────────────────────────────────────
    table_columns = [
        {"name": "System", "id": "system"},
        {"name": "Region", "id": "region"},
        {"name": "Health", "id": "health"},
        {"name": "STP %", "id": "stp"},
        {"name": "Uptime", "id": "uptime"},
        {"name": "Incidents", "id": "incidents"},
        {"name": "Last Incident", "id": "last_incident"},
        {"name": "Status", "id": "status"},
    ]

    systems = [
        ("Core Banking Engine", "Americas", 98, "99.4%", "99.99%",
         "0", "12 days ago", "Healthy"),
        ("Equities Trading Platform", "Americas", 94, "98.7%", "99.98%",
         "1", "3h ago", "Healthy"),
        ("FICC Trading Engine", "Americas", 87, "97.2%", "99.95%",
         "2", "1h 15m ago", "Warning"),
        ("FX Settlement Gateway", "EMEA", 96, "99.1%", "99.99%",
         "0", "8 days ago", "Healthy"),
        ("Risk Calc Engine", "Americas", 72, "95.8%", "99.91%",
         "3", "28 min ago", "Warning"),
        ("Margin & Collateral", "EMEA", 91, "98.3%", "99.97%",
         "1", "6h ago", "Healthy"),
        ("Regulatory Reporting", "APAC", 88, "97.6%", "99.96%",
         "1", "4h 20m ago", "Healthy"),
        ("Client Onboarding", "EMEA", 95, "98.9%", "99.98%",
         "0", "18 days ago", "Healthy"),
        ("Payment Processing", "APAC", 64, "93.2%", "99.84%",
         "4", "14 min ago", "Critical"),
        ("Data Warehouse / ETL", "Americas", 82, "96.5%", "99.93%",
         "2", "2h 40m ago", "Low"),
        ("Anti-Money Laundering", "EMEA", 93, "98.5%", "99.97%",
         "0", "21 days ago", "Healthy"),
        ("Market Data Feed", "APAC", 76, "96.1%", "99.89%",
         "3", "45 min ago", "Warning"),
    ]

    table_data = []
    for (sys_name, region, health, stp, uptime,
         incidents, last_inc, status) in systems:
        table_data.append({
            "system": sys_name,
            "region": region,
            "health": f"{health}%",
            "stp": stp,
            "uptime": uptime,
            "incidents": incidents,
            "last_incident": last_inc,
            "status": status,
        })

    return layout_table(
        title="Operations & Infrastructure",
        subtitle="System health, settlement processing, and incident "
                 "management across all regions and platforms",
        filters=filters,
        kpi_items=kpis,
        table_columns=table_columns,
        table_data=table_data,
    )
