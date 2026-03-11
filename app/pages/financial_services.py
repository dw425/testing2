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
    insight_card, morning_briefing,
    trend_indicator, use_case_badges, donut_figure,
    layout_executive, layout_table, layout_split, layout_alerts,
    layout_forecast, layout_grid,
    gauge_figure, sparkline_figure, metric_with_sparkline,
    _card, _hex_to_rgb,
)
from app.theme import COLORS, FONT_FAMILY, get_vertical_theme
from dash import dcc, html
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
# 1. DASHBOARD — layout_executive (Style A)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive dashboard with AUM, Revenue, Risk Score heroes,
    portfolio performance chart, asset allocation donut, and revenue
    breakdown bar chart."""

    # ── Morning briefing ──────────────────────────────────────────────
    briefing = morning_briefing(
        title="Financial Services Morning Briefing",
        summary_text=(
            "Net Interest Margin held steady at 3.42%, outperforming the "
            "peer median by 18 bps as the rate environment stabilises. "
            "Risk-adjusted returns climbed to 8.7% on improved credit "
            "selection in the IG book, while capital efficiency improved "
            "quarter-over-quarter with RoRWA reaching 1.64%. Overnight "
            "funding markets remain orderly; no stress signals detected."
        ),
        signals=[
            {"label": "Net Interest Margin", "status": "green",
             "detail": "3.42% — stable, +18 bps vs peer median"},
            {"label": "Risk-Adjusted Returns", "status": "green",
             "detail": "8.7% — up 40 bps QoQ on IG credit alpha"},
            {"label": "Capital Efficiency (RoRWA)", "status": "green",
             "detail": "1.64% — above 1.50% internal target"},
            {"label": "Liquidity Coverage Ratio", "status": "amber",
             "detail": "118% — trending toward 110% internal floor"},
        ],
    )

    # ── Hero metrics — strategic North Stars + existing ────────────────
    heroes = [
        hero_metric("Net Interest Margin", "3.42%",
                     trend_text="+18 bps vs peers", trend_dir="up",
                     accent="green",
                     href="/financial_services/investment_alpha"),
        hero_metric("Risk-Adjusted Return", "8.7%",
                     trend_text="+40 bps QoQ", trend_dir="up",
                     accent="blue",
                     href="/financial_services/risk_management"),
        hero_metric("Assets Under Management", "$847.3B",
                     trend_text="+3.2% QoQ", trend_dir="up", accent="blue",
                     href="/financial_services/investment_alpha"),
        hero_metric("Total Revenue", "$12.6B",
                     trend_text="+5.8% YoY", trend_dir="up", accent="green",
                     href="/financial_services/operations"),
        hero_metric("Composite Risk Score", "72 / 100",
                     trend_text="-4 pts from prior quarter", trend_dir="down",
                     accent="yellow",
                     href="/financial_services/risk_management"),
    ]

    # ── Insight card — market risk / capital adequacy ──────────────────
    risk_insight = insight_card(
        headline="Market Risk Anomaly — Equity Vol Surface",
        metric_value="VaR +12%",
        direction="up",
        narrative=(
            "Implied volatility skew on 3-month S&P options has steepened "
            "beyond 2-sigma historical norms, pushing parametric VaR up 12% "
            "intraday. The anomaly correlates with elevated dealer gamma "
            "positioning ahead of quarterly OpEx. Capital adequacy ratios "
            "remain well above minimums (CET1 at 14.1%), but the trend "
            "warrants monitoring as the stress-test buffer narrows to 36 bps "
            "above the internal floor."
        ),
        action_text="Review VaR decomposition in Risk Management",
        sparkline_values=[42, 44, 43, 46, 48, 47, 51, 53, 55, 58, 61, 65],
        severity="warning",
        accent_color=COLORS["yellow"],
    )

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
        vertical="financial_services",
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
        vertical="financial_services",
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
        ("Asset Allocation", alloc_chart, "/financial_services/investment_alpha"),
        ("Revenue by Business Line", rev_chart, "/financial_services/operations"),
    ]

    # ── Layout assembly with briefing and insight ─────────────────────
    dashboard = layout_executive(
        title="Financial Services Dashboard",
        subtitle="Enterprise-wide performance across Capital Markets, "
                 "Wealth Management, and Investment Banking",
        heroes=heroes,
        main_chart=main_chart,
        panels=panels,
    )

    # Prepend briefing and append insight into the content area
    content_area = dashboard.children[1]  # the content-area div
    content_area.children.insert(0, briefing)
    content_area.children.append(risk_insight)

    return dashboard


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

    investment_insight = insight_card(
        headline="APAC Equities Fund Outperforming Benchmark",
        metric_value="+240bps",
        direction="up",
        narrative=(
            "APAC equities fund outperforming benchmark by 240bps this quarter"
        ),
        action_text="Review APAC allocation strategy",
        severity="healthy",
    )

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
        insight=investment_insight,
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

    trading_insight = insight_card(
        headline="FX Desk Leading Performance This Month",
        metric_value="72%",
        direction="up",
        narrative=(
            "FX desk showing 72% win rate \u2014 best performing desk this month"
        ),
        action_text="Review FX desk strategy breakdown",
        severity="healthy",
    )

    return layout_table(
        title="Trading & Advisory",
        subtitle="Real-time trading desk activity, PnL attribution, "
                 "and execution quality metrics",
        filters=filters,
        kpi_items=kpis,
        table_columns=table_columns,
        table_data=table_data,
        insight=trading_insight,
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

    # ── Build tab contents using new API ───────────────────────────────
    active_cards = [alert_card(**a) for a in alerts
                    if a["severity"] in ("critical", "warning")]
    breach_cards = [alert_card(**a) for a in alerts
                    if a["severity"] == "critical"] + [
        alert_card(severity="critical",
                   title="Intraday PnL Limit Breach — FICC Desk",
                   description="Daily PnL drawdown on the FICC desk has hit -$18.4M, exceeding the -$15M intraday stop-loss limit. Position reduction directive issued to desk head.",
                   timestamp="58 min ago"),
    ]
    historical_cards = [
        alert_card(severity="healthy",
                   title="Q4 VaR Backtesting — 0 Exceptions",
                   description="99% 1-day VaR model produced zero backtesting exceptions in Q4. Model remains well-calibrated with a p-value of 0.42 under Kupiec test.",
                   timestamp="Jan 15, 2026"),
        alert_card(severity="healthy",
                   title="CCAR Stress Test — Passed",
                   description="All nine Fed-mandated scenarios passed with CET1 never falling below 9.8% (minimum 4.5%). Board approved capital distribution plan.",
                   timestamp="Dec 18, 2025"),
        alert_card(severity="info",
                   title="Counterparty Exposure Rebalanced — Q3",
                   description="Top-10 counterparty concentration reduced from 42% to 34% of total exposure following quarterly rebalancing. All within risk appetite.",
                   timestamp="Oct 1, 2025"),
        alert_card(severity="info",
                   title="Market Risk Model Recalibration Complete",
                   description="Annual recalibration of parametric VaR model completed. Volatility surface updated with 2025 regime data. No material impact to capital requirements.",
                   timestamp="Sep 12, 2025"),
    ]

    tab_contents = [
        ("Active Risks", html.Div(active_cards)),
        ("Breaches", html.Div(breach_cards)),
        ("Historical", html.Div(historical_cards)),
    ]

    risk_insight = insight_card(
        headline="VaR Breach Frequency Declining",
        metric_value="2 vs 7",
        direction="down",
        narrative=(
            "VaR breach frequency declining \u2014 2 events vs 7 last quarter"
        ),
        action_text="Review VaR model performance",
        severity="healthy",
    )

    return layout_alerts(
        title="Risk Management",
        subtitle="Enterprise risk monitoring \u2014 VaR, stress testing, "
                 "concentration limits, and counterparty exposure",
        tab_contents=tab_contents,
        summary_kpis=summary_kpis,
        insight=risk_insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 5. REGULATORY & COMPLIANCE — layout_split (Style C)
# ═══════════════════════════════════════════════════════════════════════════

def render_regulatory(cfg):
    """Regulatory compliance with tabs, info banner, capital ratios bar
    chart on the left, compliance status donut on the right, and bottom
    summary stats."""

    banner_text = (
        "All Basel III / IV capital ratios are above regulatory minimums "
        "as of the latest quarterly filing.  Next CCAR submission due in "
        "18 days — pre-filing review is 94% complete."
    )

    # ── TAB 1: Capital ───────────────────────────────────────────────
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
    cap_fig.add_hline(y=10.5, line_dash="dash",
                      line_color=COLORS["red"], opacity=0.6,
                      annotation_text="Min Total Capital (10.5%)",
                      annotation_font_color=COLORS["red"],
                      annotation_font_size=10)
    cap_left = dcc.Graph(figure=cap_fig, config=CHART_CONFIG, style={"width": "100%"})

    comp_fig = donut_figure(
        ["Compliant", "Under Review", "Remediation", "Past Due"],
        [142, 18, 7, 3],
        [COLORS["green"], COLORS["blue"], COLORS["yellow"], COLORS["red"]],
        center_text="170", title="Compliance Items",
    )
    cap_right = dcc.Graph(figure=comp_fig, config=CHART_CONFIG, style={"width": "100%"})

    tab_capital = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Capital Ratios Over Time", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), cap_left], padding="20px"),
            _card([html.Div("Compliance Status", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), cap_right], padding="20px"),
        ],
    )

    # ── TAB 2: Reporting ─────────────────────────────────────────────
    report_types = ["CCAR / DFAST", "Basel III Pillar 3", "Call Reports",
                    "MiFID II RTS", "AML / SAR", "ESG / SFDR"]
    report_status = [94, 100, 88, 78, 82, 65]
    fig_report = go.Figure()
    fig_report.add_trace(go.Bar(
        y=report_types, x=report_status,
        orientation="h",
        marker_color=[COLORS["green"] if v >= 90 else COLORS["yellow"] if v >= 70
                      else COLORS["red"] for v in report_status],
        text=[f"{v}%" for v in report_status],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_report.add_vline(x=100, line_dash="dash", line_color=COLORS["green"],
                         opacity=0.4)
    fig_report.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        margin=dict(l=140, r=60, t=24, b=24),
        xaxis=dict(title="Completion %", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], range=[0, 110]),
        yaxis=dict(showgrid=False),
    ))
    report_left = dcc.Graph(figure=fig_report, config=CHART_CONFIG)

    # Submission timeline
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    fig_timeline = go.Figure()
    fig_timeline.add_trace(go.Bar(
        x=months, y=[8, 12, 6, 14, 10, 8], name="Reports Filed",
        marker_color=COLORS["blue"],
    ))
    fig_timeline.add_trace(go.Scatter(
        x=months, y=[1, 0, 2, 1, 0, 0], name="Late Submissions",
        mode="lines+markers", line=dict(color=COLORS["red"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_timeline.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Filed Count", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        yaxis2=dict(title="Late", overlaying="y", side="right",
                    showgrid=False, color=COLORS["red"], range=[0, 5]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    report_right = dcc.Graph(figure=fig_timeline, config=CHART_CONFIG)

    tab_reporting = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Regulatory Report Completion", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), report_left], padding="20px"),
            _card([html.Div("Filing Timeline & Late Submissions", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), report_right], padding="20px"),
        ],
    )

    # ── TAB 3: Audit ─────────────────────────────────────────────────
    audit_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Closed", "In Progress", "Not Started", "Overdue"],
            values=[84, 12, 6, 3],
            colors=[COLORS["green"], COLORS["blue"], COLORS["text_muted"], COLORS["red"]],
            center_text="105", title="Audit Findings",
        ),
        config=CHART_CONFIG,
    )

    # Audit score by department
    departments = ["Risk", "Trading", "Operations", "IT", "Legal"]
    scores = [96, 88, 92, 78, 94]
    fig_audit = go.Figure(go.Bar(
        x=departments, y=scores,
        marker_color=[COLORS["green"] if s >= 90 else COLORS["yellow"] if s >= 80
                      else COLORS["red"] for s in scores],
        text=[f"{s}%" for s in scores],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_audit.add_hline(y=90, line_dash="dash", line_color=COLORS["green"],
                        opacity=0.5, annotation_text="Target (90%)",
                        annotation_font_color=COLORS["green"],
                        annotation_font_size=10)
    fig_audit.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="Audit Score %", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], range=[0, 105]),
        margin=dict(l=48, r=24, t=24, b=48),
    ))
    audit_right = dcc.Graph(figure=fig_audit, config=CHART_CONFIG)

    tab_audit = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Audit Findings Status", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), audit_donut], padding="20px"),
            _card([html.Div("Audit Score by Department", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), audit_right], padding="20px"),
        ],
    )

    # ── Bottom stats ──────────────────────────────────────────────────
    bottom_stats = [
        ("CET1 Ratio", "14.1%", "blue"),
        ("Total Capital", "17.3%", "green"),
        ("Leverage Ratio", "6.2%", "purple"),
        ("CCAR Progress", "94%", "blue"),
        ("Open Findings", "3", "red"),
    ]

    regulatory_insight = insight_card(
        headline="CET1 Ratio Provides Strong Capital Buffer",
        metric_value="14.1%",
        direction="up",
        narrative=(
            "CET1 ratio at 14.1% provides 310bps buffer above regulatory minimum"
        ),
        action_text="Review capital adequacy projections",
        severity="healthy",
    )

    return layout_split(
        title="Regulatory & Compliance",
        subtitle="Capital adequacy, regulatory reporting, and audit "
                 "readiness across all jurisdictions",
        tab_contents=[
            ("Capital", tab_capital),
            ("Reporting", tab_reporting),
            ("Audit", tab_audit),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=regulatory_insight,
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

    fraud_insight = insight_card(
        headline="False Positive Rate Reduction",
        metric_value="4.4%",
        direction="down",
        narrative=(
            "False positive rate down to 4.4% \u2014 reducing investigation backlog"
        ),
        action_text="Review detection model tuning",
        severity="healthy",
    )

    return layout_grid(
        title="Fraud & Cyber Security",
        subtitle="Real-time threat monitoring, fraud detection, and "
                 "investigation pipeline across all channels",
        grid_items=grid_items,
        insight=fraud_insight,
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

    ops_insight = insight_card(
        headline="Cross-Border STP Rate Improving",
        metric_value="98.2%",
        direction="up",
        narrative=(
            "STP rate for cross-border payments improved to 98.2%"
        ),
        action_text="Review cross-border processing pipeline",
        severity="healthy",
    )

    return layout_table(
        title="Operations & Infrastructure",
        subtitle="System health, settlement processing, and incident "
                 "management across all regions and platforms",
        filters=filters,
        kpi_items=kpis,
        table_columns=table_columns,
        table_data=table_data,
        insight=ops_insight,
    )
