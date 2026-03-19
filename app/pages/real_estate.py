"""Real Estate vertical pages for Blueprint IQ.

Seven page renderers covering commercial real estate portfolio management:
  - Dashboard (portfolio overview)
  - Portfolio Analytics (performance, allocation, risk)
  - Leasing & Occupancy (tenant table)
  - Market Intelligence (cap rate forecast)
  - Acquisitions Pipeline (deal table)
  - Asset Management (maintenance, capex, tenant ops)
  - ESG & Sustainability (grid metrics)
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
#  1. DASHBOARD  (Style A — layout_executive)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive portfolio dashboard with hero metrics, NOI trend, and panels."""
    vt = get_vertical_theme("real_estate")

    # --- Morning Briefing (AI Narrative Center) ------------------------------
    briefing = morning_briefing(
        title="Portfolio Morning Briefing",
        summary_text=(
            "Portfolio NOI is tracking at $285M annualized, 3.2% above "
            "underwriting. Occupancy stands at 92.1%, steady versus last "
            "quarter, but office assets in San Francisco are under pressure "
            "with 14.8% vacancy — 280bps above portfolio average. Rent "
            "growth across the portfolio is +3.4% YoY, led by industrial "
            "and data center assets at +5.6% and +7.2% respectively. "
            "DSCR is comfortable at 1.45x but two properties are below the "
            "1.25x covenant threshold. Recommend reviewing SF office "
            "repositioning strategy and accelerating leasing at the "
            "Chicago mixed-use development."
        ),
        signals=[
            {"label": "NOI Performance", "status": "green", "detail": "$285M annualized — 3.2% above underwriting"},
            {"label": "Occupancy", "status": "amber", "detail": "92.1% portfolio — SF office at 85.2% requires attention"},
            {"label": "Rent Growth", "status": "green", "detail": "+3.4% YoY — industrial and data center leading"},
            {"label": "Debt Covenants", "status": "red", "detail": "2 properties below 1.25x DSCR threshold"},
        ],
    )

    # --- North Star hero metric ---------------------------------------------
    north_star = hero_metric("Portfolio NOI", "$285M",
                              trend_text="3.2% above underwriting", trend_dir="up",
                              accent="green", href="/real_estate/portfolio_analytics")

    # --- Hero metrics -------------------------------------------------------
    heroes = [
        north_star,
        hero_metric("Portfolio Value", "$4.2B",
                     trend_text="+6.8% vs prior year", trend_dir="up",
                     accent="blue", href="/real_estate/portfolio_analytics"),
        hero_metric("Occupancy Rate", "92.1%",
                     trend_text="Flat vs prior quarter", trend_dir="flat",
                     accent="yellow", href="/real_estate/leasing"),
        hero_metric("Wtd Avg Cap Rate", "5.8%",
                     trend_text="-20bps vs last year", trend_dir="down",
                     accent="purple", href="/real_estate/market_intel"),
    ]

    # --- Main chart: NOI / revenue trends -----------------------------------
    quarters = ["Q1'24", "Q2'24", "Q3'24", "Q4'24", "Q1'25", "Q2'25",
                "Q3'25", "Q4'25", "Q1'26"]
    noi_vals = [62.1, 64.8, 66.2, 68.5, 69.4, 70.8, 71.5, 72.3, 73.1]
    revenue_vals = [98.5, 102.1, 104.8, 108.2, 110.4, 112.6, 114.2, 116.8, 118.5]
    opex_vals = [36.4, 37.3, 38.6, 39.7, 41.0, 41.8, 42.7, 44.5, 45.4]

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=quarters, y=revenue_vals, name="Gross Revenue ($M)",
        line=dict(color=COLORS["blue"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=quarters, y=noi_vals, name="NOI ($M)",
        line=dict(color=COLORS["green"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['green'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=quarters, y=opex_vals, name="OpEx ($M)",
        line=dict(color=COLORS["yellow"], width=2, dash="dash"),
    ))
    fig_trend.update_layout(**dark_chart_layout(
        vertical="real_estate",
        height=320,
        title=dict(text="Revenue, NOI & Operating Expense Trends ($M)",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="$ Millions", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_trend, config=CHART_CONFIG,
                           style={"height": "320px"})

    # --- Bottom panels ------------------------------------------------------
    # Panel 1: portfolio allocation donut
    donut = donut_figure(
        labels=["Office", "Industrial", "Multifamily", "Retail", "Mixed-Use", "Data Center"],
        values=[32, 24, 20, 12, 7, 5],
        colors=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                COLORS["yellow"], COLORS["red"], "#4FC3F7"],
        center_text="$4.2B",
        title="Portfolio Allocation by Type",
    )
    panel_donut = dcc.Graph(figure=donut, config=CHART_CONFIG,
                            style={"height": "280px"})

    # Panel 2: occupancy by property type bar chart
    fig_occ = go.Figure()
    prop_types = ["Office", "Industrial", "Multifamily", "Retail", "Mixed-Use", "Data Center"]
    occupancy = [87.2, 96.4, 94.8, 89.5, 91.2, 98.1]
    target_occ = [92.0, 95.0, 93.0, 91.0, 90.0, 97.0]
    fig_occ.add_trace(go.Bar(
        x=prop_types, y=occupancy, name="Actual",
        marker_color=[COLORS["red"] if v < t else COLORS["green"]
                      for v, t in zip(occupancy, target_occ)],
        text=[f"{v}%" for v in occupancy],
        textposition="outside", textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_occ.add_trace(go.Scatter(
        x=prop_types, y=target_occ, name="Target",
        mode="markers+lines",
        line=dict(color=COLORS["yellow"], width=1, dash="dash"),
        marker=dict(size=6, color=COLORS["yellow"]),
    ))
    fig_occ.update_layout(**dark_chart_layout(
        vertical="real_estate",
        height=280, barmode="group",
        title=dict(text="Occupancy vs Target by Type",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Occupancy %", showgrid=True,
                   gridcolor=COLORS["border"], range=[80, 102]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    panel_occ = dcc.Graph(figure=fig_occ, config=CHART_CONFIG,
                          style={"height": "280px"})

    # --- Insight card -------------------------------------------------------
    portfolio_insight = insight_card(
        headline="SF Office Repositioning Urgency",
        metric_value="14.8%",
        direction="up",
        narrative=(
            "San Francisco office vacancy has climbed to 14.8%, now 280bps "
            "above portfolio average. Three anchor tenants with 142,000 SF "
            "combined have leases expiring within 18 months. Market rent "
            "growth in SF office is flat at +0.2% YoY compared to +3.4% "
            "portfolio-wide. Conversion to life-science or mixed-use should "
            "be evaluated to preserve asset value."
        ),
        action_text="Initiate repositioning study for 3 SF office assets",
        severity="critical",
        sparkline_values=[9.2, 10.1, 10.8, 11.4, 12.0, 12.6, 13.1, 13.8, 14.2, 14.8],
    )

    panels = [
        ("Portfolio Allocation", panel_donut, "/real_estate/portfolio_analytics"),
        ("Occupancy by Type", panel_occ, "/real_estate/leasing"),
        ("AI Insight", portfolio_insight),
    ]

    return layout_executive(
        title="Portfolio Overview",
        subtitle="Enterprise-wide property performance and portfolio health",
        heroes=heroes,
        briefing=briefing,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. PORTFOLIO ANALYTICS  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_portfolio_analytics(cfg):
    """Portfolio analytics with performance, allocation, and risk tabs."""

    banner_text = (
        "Portfolio return is tracking at 8.4% unlevered, exceeding the 7.5% "
        "benchmark by 90bps. Allocation is tilted toward industrial (+4% "
        "overweight) which has driven outperformance. Geographic concentration "
        "risk is elevated with 38% exposure in the top 2 markets (New York "
        "and San Francisco). Value-add assets are contributing 42% of total "
        "return despite comprising only 28% of portfolio value."
    )

    # ── TAB 1: Performance ──────────────────────────────────────────────
    quarters = ["Q1'24", "Q2'24", "Q3'24", "Q4'24", "Q1'25", "Q2'25",
                "Q3'25", "Q4'25", "Q1'26"]
    portfolio_return = [1.8, 2.1, 2.0, 2.3, 2.2, 2.4, 2.1, 2.5, 2.3]
    benchmark_return = [1.6, 1.9, 1.8, 2.0, 1.9, 2.1, 1.9, 2.2, 2.0]

    fig_perf = go.Figure()
    fig_perf.add_trace(go.Bar(
        x=quarters, y=portfolio_return, name="Portfolio Return",
        marker_color=COLORS["blue"],
        text=[f"{v}%" for v in portfolio_return],
        textposition="outside", textfont=dict(color=COLORS["white"], size=9),
    ))
    fig_perf.add_trace(go.Scatter(
        x=quarters, y=benchmark_return, name="NCREIF Benchmark",
        mode="lines+markers",
        line=dict(color=COLORS["yellow"], width=2, dash="dash"),
        marker=dict(size=5),
    ))
    fig_perf.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Quarterly Return %", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    perf_left = dcc.Graph(figure=fig_perf, config=CHART_CONFIG,
                          style={"height": "300px"})

    # NOI growth by property type
    fig_noi_growth = go.Figure()
    types = ["Data Center", "Industrial", "Multifamily", "Office", "Retail", "Mixed-Use"]
    noi_growth = [7.2, 5.6, 4.1, 1.8, 2.4, 3.2]
    fig_noi_growth.add_trace(go.Bar(
        x=types, y=noi_growth,
        marker_color=[COLORS["green"] if v > 3 else COLORS["yellow"] if v > 1.5
                      else COLORS["red"] for v in noi_growth],
        text=[f"+{v}%" for v in noi_growth],
        textposition="outside", textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_noi_growth.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="NOI Growth YoY %", showgrid=True,
                   gridcolor=COLORS["border"]),
    ))
    perf_right = dcc.Graph(figure=fig_noi_growth, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_performance = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Portfolio vs Benchmark Returns", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), perf_left], padding="20px"),
            _card([html.Div("NOI Growth by Property Type", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), perf_right], padding="20px"),
        ],
    )

    # ── TAB 2: Allocation ──────────────────────────────────────────────
    alloc_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Office", "Industrial", "Multifamily", "Retail", "Mixed-Use", "Data Center"],
            values=[32, 24, 20, 12, 7, 5],
            colors=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                    COLORS["yellow"], COLORS["red"], "#4FC3F7"],
            center_text="$4.2B",
            title="By Property Type",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    geo_donut = dcc.Graph(
        figure=donut_figure(
            labels=["New York", "San Francisco", "Chicago", "Austin", "Miami", "Seattle", "Denver", "Atlanta"],
            values=[22, 16, 14, 12, 11, 10, 8, 7],
            colors=[COLORS["blue"], COLORS["purple"], COLORS["green"],
                    COLORS["yellow"], "#FF6B6B", "#4FC3F7",
                    COLORS["red"], COLORS["text_muted"]],
            center_text="8 Mkts",
            title="By Market",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    tab_allocation = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Allocation by Property Type", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), alloc_donut], padding="20px"),
            _card([html.Div("Geographic Allocation", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), geo_donut], padding="20px"),
        ],
    )

    # ── TAB 3: Risk ────────────────────────────────────────────────────
    markets = ["New York", "San Francisco", "Chicago", "Austin", "Miami",
               "Seattle", "Denver", "Atlanta"]
    concentration = [22, 16, 14, 12, 11, 10, 8, 7]
    fig_risk = go.Figure()
    fig_risk.add_trace(go.Bar(
        x=markets, y=concentration,
        marker_color=[COLORS["red"] if v > 15 else COLORS["yellow"] if v > 10
                      else COLORS["green"] for v in concentration],
        text=[f"{v}%" for v in concentration],
        textposition="outside", textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_risk.add_hline(y=15, line_dash="dash", line_color=COLORS["red"],
                       opacity=0.6, annotation_text="Concentration Limit (15%)",
                       annotation_font_color=COLORS["red"],
                       annotation_font_size=10)
    fig_risk.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="Portfolio Weight %", showgrid=True,
                   gridcolor=COLORS["border"], range=[0, 28]),
    ))
    risk_left = dcc.Graph(figure=fig_risk, config=CHART_CONFIG,
                          style={"height": "300px"})

    # LTV distribution
    ltv_buckets = ["< 50%", "50-60%", "60-70%", "70-75%", "> 75%"]
    ltv_counts = [8, 14, 18, 6, 2]
    fig_ltv = go.Figure()
    fig_ltv.add_trace(go.Bar(
        x=ltv_buckets, y=ltv_counts,
        marker_color=[COLORS["green"], COLORS["green"], COLORS["yellow"],
                      COLORS["red"], COLORS["red"]],
        text=ltv_counts, textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_ltv.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="Property Count", showgrid=True,
                   gridcolor=COLORS["border"]),
    ))
    risk_right = dcc.Graph(figure=fig_ltv, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_risk = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Geographic Concentration Risk", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), risk_left], padding="20px"),
            _card([html.Div("LTV Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), risk_right], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Portfolio Value", "$4.2B", "blue"),
        ("Unlevered Return", "8.4%", "green"),
        ("Levered Return", "12.1%", "green"),
        ("Active Properties", "48", "purple"),
        ("Avg Hold Period", "6.2 yrs", "blue"),
    ]

    insight = insight_card(
        headline="Industrial Allocation Driving Alpha",
        metric_value="+90bps",
        direction="up",
        narrative=(
            "Industrial overweight of 4% vs benchmark is contributing 90bps "
            "of outperformance. Data center exposure at 5% is small but "
            "generating 7.2% NOI growth — highest across all property types."
        ),
        severity="healthy",
    )

    return layout_split(
        title="Portfolio Analytics",
        subtitle="Performance attribution, allocation analysis, and portfolio risk",
        tab_contents=[
            ("Performance", tab_performance),
            ("Allocation", tab_allocation),
            ("Risk", tab_risk),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. LEASING & OCCUPANCY  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_leasing(cfg):
    """Leasing and occupancy table with tenant data, filters, and KPIs."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Property Type", "options": ["All Types", "Office", "Industrial", "Multifamily", "Retail", "Mixed-Use", "Data Center"]},
        {"label": "Market", "options": ["All Markets", "New York", "San Francisco", "Chicago", "Austin", "Miami", "Seattle", "Denver", "Atlanta"]},
        {"label": "Lease Status", "options": ["All Statuses", "Active", "Expiring <12mo", "Expiring 12-24mo", "Month-to-Month"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Occupancy", "value": "92.1%", "accent": "green"},
        {"label": "Leasing Velocity", "value": "14.2 days", "accent": "blue"},
        {"label": "Retention Rate", "value": "82%", "accent": "green"},
        {"label": "Avg Rent PSF", "value": "$42.80", "accent": "blue"},
        {"label": "Net Effective Rent", "value": "$38.50", "accent": "purple"},
    ]

    # --- Table data ---------------------------------------------------------
    tenants = [
        ("Meridian Tech Corp", "Office", "New York", "48,200 SF",
         "$62.40", "Jun 2027", "A", "Active"),
        ("Blue Harbor Logistics", "Industrial", "Chicago", "124,500 SF",
         "$12.80", "Mar 2029", "BBB+", "Active"),
        ("Vertex Healthcare", "Office", "San Francisco", "32,100 SF",
         "$78.50", "Dec 2025", "AA-", "Expiring <12mo"),
        ("Cascade Retail Group", "Retail", "Miami", "18,400 SF",
         "$45.20", "Sep 2026", "BBB", "Active"),
        ("Summit Cloud Services", "Data Center", "Austin", "62,000 SF",
         "$185.00", "Jan 2031", "A+", "Active"),
        ("Prairie Living LLC", "Multifamily", "Denver", "86 units",
         "$2,180/mo", "Rolling", "N/A", "Active"),
        ("Ironworks Manufacturing", "Industrial", "Atlanta", "210,800 SF",
         "$9.40", "Aug 2025", "BB+", "Expiring <12mo"),
        ("NovaTech Solutions", "Office", "Seattle", "28,600 SF",
         "$54.80", "Feb 2026", "A-", "Expiring 12-24mo"),
        ("Metro Urban Living", "Mixed-Use", "New York", "42,300 SF",
         "$52.10", "Nov 2027", "BBB+", "Active"),
        ("Pacific Warehousing", "Industrial", "San Francisco", "178,200 SF",
         "$14.60", "Apr 2028", "A", "Active"),
        ("Oakridge Senior Care", "Office", "Chicago", "22,400 SF",
         "$38.90", "Jul 2025", "BBB-", "Expiring <12mo"),
        ("DataVault Inc.", "Data Center", "Seattle", "44,000 SF",
         "$192.50", "Mar 2032", "AA", "Active"),
    ]

    columns = [
        {"name": "Tenant", "id": "tenant"},
        {"name": "Type", "id": "type"},
        {"name": "Market", "id": "market"},
        {"name": "Size", "id": "size"},
        {"name": "Rent", "id": "rent"},
        {"name": "Expiry", "id": "expiry"},
        {"name": "Credit", "id": "credit"},
        {"name": "Status", "id": "status"},
    ]

    data = []
    for name, ptype, mkt, size, rent, exp, credit, status in tenants:
        data.append({
            "tenant": name,
            "type": ptype,
            "market": mkt,
            "size": size,
            "rent": rent,
            "expiry": exp,
            "credit": credit,
            "status": status,
        })

    insight = insight_card(
        headline="Lease Expiry Concentration Risk",
        metric_value="18.4%",
        direction="up",
        narrative=(
            "18.4% of total leased square footage expires within the next "
            "12 months, concentrated in 3 office assets. Early renewal "
            "discussions should be initiated with Vertex Healthcare (32.1K SF) "
            "and Ironworks Manufacturing (210.8K SF) to reduce rollover risk."
        ),
        action_text="Initiate early renewal outreach for top 5 expiring tenants",
        severity="warning",
        sparkline_values=[12.1, 13.0, 13.8, 14.5, 15.2, 16.0, 16.8, 17.4, 18.0, 18.4],
    )

    return layout_table(
        title="Leasing & Occupancy",
        subtitle="Tenant roster, lease expirations, and occupancy metrics",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. MARKET INTELLIGENCE  (Style E — layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_market_intel(cfg):
    """Market intelligence with cap rate trends, market KPIs, and breakdown."""

    # --- KPIs ---------------------------------------------------------------
    kpis = [
        {"label": "Market Cap Rate", "value": "6.1%", "accent": "blue"},
        {"label": "Vacancy Rate", "value": "12.8%", "accent": "yellow"},
        {"label": "Net Absorption", "value": "2.84M SF", "accent": "green"},
        {"label": "Construction Pipeline", "value": "5.2M SF", "accent": "purple"},
    ]

    # --- Main chart: cap rate trends by property type -----------------------
    quarters = ["Q1'23", "Q2'23", "Q3'23", "Q4'23", "Q1'24", "Q2'24",
                "Q3'24", "Q4'24", "Q1'25", "Q2'25", "Q3'25", "Q4'25", "Q1'26"]
    office_caps = [5.8, 5.9, 6.1, 6.3, 6.5, 6.6, 6.8, 6.9, 7.0, 6.9, 6.8, 6.7, 6.6]
    industrial_caps = [4.8, 4.9, 5.0, 5.1, 5.2, 5.3, 5.2, 5.1, 5.0, 4.9, 4.8, 4.8, 4.7]
    multifamily_caps = [4.5, 4.6, 4.7, 4.8, 4.9, 5.0, 5.1, 5.1, 5.0, 4.9, 4.9, 4.8, 4.8]
    retail_caps = [6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.6, 6.5, 6.4, 6.3, 6.2, 6.1, 6.0]

    fig_caps = go.Figure()
    fig_caps.add_trace(go.Scatter(
        x=quarters, y=office_caps, name="Office",
        line=dict(color=COLORS["blue"], width=2),
        mode="lines+markers", marker=dict(size=4),
    ))
    fig_caps.add_trace(go.Scatter(
        x=quarters, y=industrial_caps, name="Industrial",
        line=dict(color=COLORS["green"], width=2),
        mode="lines+markers", marker=dict(size=4),
    ))
    fig_caps.add_trace(go.Scatter(
        x=quarters, y=multifamily_caps, name="Multifamily",
        line=dict(color=COLORS["purple"], width=2),
        mode="lines+markers", marker=dict(size=4),
    ))
    fig_caps.add_trace(go.Scatter(
        x=quarters, y=retail_caps, name="Retail",
        line=dict(color=COLORS["yellow"], width=2),
        mode="lines+markers", marker=dict(size=4),
    ))
    fig_caps.update_layout(**dark_chart_layout(
        height=300,
        xaxis=dict(title="Quarter", showgrid=False),
        yaxis=dict(title="Cap Rate %", showgrid=True, gridcolor=COLORS["border"],
                   range=[4.0, 7.5]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_caps, config=CHART_CONFIG,
                           style={"height": "300px"})

    # --- Side breakdown by market -------------------------------------------
    side = breakdown_list([
        {"label": "Austin", "value": "+5.6%", "pct": 56, "color": COLORS["green"]},
        {"label": "Miami", "value": "+4.8%", "pct": 48, "color": COLORS["green"]},
        {"label": "Seattle", "value": "+3.9%", "pct": 39, "color": COLORS["green"]},
        {"label": "Denver", "value": "+3.2%", "pct": 32, "color": COLORS["blue"]},
        {"label": "Atlanta", "value": "+2.8%", "pct": 28, "color": COLORS["blue"]},
        {"label": "Chicago", "value": "+1.4%", "pct": 14, "color": COLORS["yellow"]},
        {"label": "New York", "value": "+0.8%", "pct": 8, "color": COLORS["yellow"]},
        {"label": "San Francisco", "value": "-0.2%", "pct": 2, "color": COLORS["red"]},
    ])

    insight = insight_card(
        headline="Industrial Cap Rate Compression Accelerating",
        metric_value="4.7%",
        direction="down",
        narrative=(
            "Industrial cap rates have compressed to 4.7%, a 50bps decline "
            "over 12 months driven by e-commerce demand and limited new "
            "supply. Spreads vs office have widened to 190bps — the largest "
            "gap in a decade. Consider increasing industrial allocation "
            "while valuations in office stabilize."
        ),
        action_text="Evaluate increasing industrial allocation by 3-5%",
        severity="healthy",
        sparkline_values=[5.2, 5.3, 5.2, 5.1, 5.0, 4.9, 4.8, 4.8, 4.7, 4.7],
    )

    return layout_forecast(
        title="Market Intelligence",
        subtitle="Cap rate trends, market fundamentals, and rent growth analytics",
        kpi_items=kpis,
        hero_value="6.1%",
        hero_label="Market Weighted Average Cap Rate",
        hero_trend_text="-30bps vs 12 months ago",
        main_chart=main_chart,
        side_component=html.Div([
            html.Div("Rent Growth by Market (YoY)",
                     style={"fontSize": "14px", "fontWeight": "600",
                            "color": COLORS["white"], "marginBottom": "16px"}),
            side,
        ]),
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. ACQUISITIONS PIPELINE  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_acquisitions(cfg):
    """Acquisition pipeline table with deal tracking, filters, and KPIs."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Strategy", "options": ["All Strategies", "Core", "Core-Plus", "Value-Add", "Opportunistic"]},
        {"label": "Property Type", "options": ["All Types", "Office", "Industrial", "Multifamily", "Retail", "Mixed-Use", "Data Center"]},
        {"label": "Stage", "options": ["All Stages", "Screening", "LOI Submitted", "Under Contract", "Due Diligence", "Closing"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Pipeline Value", "value": "$892M", "accent": "blue"},
        {"label": "Deals in Pipeline", "value": "14", "accent": "purple"},
        {"label": "Avg Target IRR", "value": "14.2%", "accent": "green"},
        {"label": "YTD Deployed", "value": "$324M", "accent": "blue"},
        {"label": "Annual Target", "value": "$600M", "accent": "yellow"},
    ]

    # --- Table data ---------------------------------------------------------
    deals = [
        ("One Liberty Plaza", "Office", "New York", "$185M",
         "6.2%", "12.8%", "Core-Plus", "Due Diligence"),
        ("Pinnacle Distribution Hub", "Industrial", "Atlanta", "$94M",
         "5.4%", "15.2%", "Value-Add", "Under Contract"),
        ("Bayshore Apartments", "Multifamily", "Miami", "$128M",
         "4.8%", "13.6%", "Core", "LOI Submitted"),
        ("TechPark Austin", "Mixed-Use", "Austin", "$210M",
         "5.9%", "16.4%", "Value-Add", "Screening"),
        ("Pacific Gateway DC", "Data Center", "Seattle", "$156M",
         "5.1%", "18.2%", "Opportunistic", "LOI Submitted"),
        ("Lakeshore Retail Center", "Retail", "Chicago", "$62M",
         "7.1%", "14.8%", "Value-Add", "Screening"),
        ("Riverside Industrial Park", "Industrial", "Denver", "$78M",
         "5.6%", "15.8%", "Core-Plus", "Under Contract"),
        ("Metropolitan Tower", "Office", "San Francisco", "$142M",
         "6.8%", "11.2%", "Opportunistic", "Due Diligence"),
        ("SunBelt Logistics Center", "Industrial", "Austin", "$88M",
         "5.2%", "16.1%", "Value-Add", "Closing"),
        ("Harbor Point Mixed-Use", "Mixed-Use", "Miami", "$115M",
         "5.5%", "14.4%", "Core-Plus", "Screening"),
    ]

    columns = [
        {"name": "Property", "id": "property"},
        {"name": "Type", "id": "type"},
        {"name": "Market", "id": "market"},
        {"name": "Price", "id": "price"},
        {"name": "Cap Rate", "id": "cap_rate"},
        {"name": "Target IRR", "id": "irr"},
        {"name": "Strategy", "id": "strategy"},
        {"name": "Stage", "id": "stage"},
    ]

    data = []
    for name, ptype, mkt, price, cap, irr, strat, stage in deals:
        data.append({
            "property": name,
            "type": ptype,
            "market": mkt,
            "price": price,
            "cap_rate": cap,
            "irr": irr,
            "strategy": strat,
            "stage": stage,
        })

    insight = insight_card(
        headline="Deployment Pace Below Annual Target",
        metric_value="54%",
        direction="down",
        narrative=(
            "YTD capital deployment at $324M is 54% of the $600M annual "
            "target with 9 months elapsed. The pipeline contains $892M in "
            "potential deals but only $172M are in advanced stages (due "
            "diligence or closing). Accelerating the SunBelt Logistics "
            "closing and advancing the Pinnacle Distribution Hub would "
            "bring deployment to 62% of target."
        ),
        action_text="Fast-track 2 advanced-stage deals to close within 60 days",
        severity="warning",
        sparkline_values=[42, 44, 46, 48, 49, 50, 51, 52, 53, 54],
    )

    return layout_table(
        title="Acquisitions Pipeline",
        subtitle="Deal flow, underwriting metrics, and deployment tracking",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. ASSET MANAGEMENT  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_asset_management(cfg):
    """Asset management with maintenance, capex, and tenant ops tabs."""

    banner_text = (
        "Asset management operations are performing well overall with "
        "98.2% planned maintenance completion and CapEx tracking 4% under "
        "budget. However, tenant satisfaction at the two SF office properties "
        "has dropped to 3.2/5.0, driven by HVAC complaints and elevator "
        "downtime. The deferred maintenance backlog across the portfolio "
        "has grown 12% to $18.4M, concentrated in aging retail assets."
    )

    # ── TAB 1: Maintenance ─────────────────────────────────────────────
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar"]
    fig_maint = go.Figure()
    fig_maint.add_trace(go.Bar(
        x=months, y=[142, 158, 136, 164, 148, 172, 155, 168, 145],
        name="Work Orders Completed",
        marker_color=COLORS["green"],
    ))
    fig_maint.add_trace(go.Bar(
        x=months, y=[12, 8, 15, 10, 14, 6, 11, 9, 13],
        name="Work Orders Open",
        marker_color=COLORS["yellow"],
    ))
    fig_maint.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="Work Orders", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    maint_left = dcc.Graph(figure=fig_maint, config=CHART_CONFIG,
                           style={"height": "300px"})

    maint_donut = dcc.Graph(
        figure=donut_figure(
            labels=["HVAC", "Plumbing", "Electrical", "Elevator", "Structural", "Other"],
            values=[28, 18, 16, 14, 12, 12],
            colors=[COLORS["blue"], COLORS["green"], COLORS["yellow"],
                    COLORS["purple"], COLORS["red"], COLORS["text_muted"]],
            center_text="1,413",
            title="Work Orders by Category",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    tab_maintenance = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Work Order Volume Trend", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), maint_left], padding="20px"),
            _card([html.Div("Work Order Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), maint_donut], padding="20px"),
        ],
    )

    # ── TAB 2: CapEx ───────────────────────────────────────────────────
    categories = ["Roof/Envelope", "HVAC Upgrades", "Lobby Renovations",
                  "Elevator Mod", "Parking", "TI Allowances"]
    budget = [4.2, 3.8, 2.6, 2.1, 1.4, 5.8]
    actual = [3.8, 3.6, 2.8, 1.9, 1.2, 5.4]

    fig_capex = go.Figure()
    fig_capex.add_trace(go.Bar(
        x=categories, y=budget, name="Budget ($M)",
        marker_color=COLORS["border"],
        text=[f"${v}M" for v in budget],
        textposition="outside", textfont=dict(color=COLORS["text_muted"], size=9),
    ))
    fig_capex.add_trace(go.Bar(
        x=categories, y=actual, name="Actual ($M)",
        marker_color=COLORS["blue"],
        text=[f"${v}M" for v in actual],
        textposition="outside", textfont=dict(color=COLORS["blue"], size=9),
    ))
    fig_capex.update_layout(**dark_chart_layout(
        height=300, barmode="group",
        yaxis=dict(title="$ Millions", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    capex_left = dcc.Graph(figure=fig_capex, config=CHART_CONFIG,
                           style={"height": "300px"})

    # Deferred maintenance by property type
    def_types = ["Office", "Retail", "Industrial", "Multifamily", "Mixed-Use"]
    def_vals = [6.2, 4.8, 3.1, 2.4, 1.9]
    fig_deferred = go.Figure()
    fig_deferred.add_trace(go.Bar(
        x=def_types, y=def_vals,
        marker_color=[COLORS["red"] if v > 5 else COLORS["yellow"] if v > 3
                      else COLORS["green"] for v in def_vals],
        text=[f"${v}M" for v in def_vals],
        textposition="outside", textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_deferred.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="Deferred Maint ($M)", showgrid=True,
                   gridcolor=COLORS["border"]),
    ))
    capex_right = dcc.Graph(figure=fig_deferred, config=CHART_CONFIG,
                            style={"height": "300px"})

    tab_capex = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("CapEx Budget vs Actual", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), capex_left], padding="20px"),
            _card([html.Div("Deferred Maintenance Backlog", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), capex_right], padding="20px"),
        ],
    )

    # ── TAB 3: Tenant Satisfaction ─────────────────────────────────────
    props = ["Bayshore Apts", "Summit DC", "Pinnacle Dist", "Metro Tower",
             "One Liberty", "Pacific GW", "Lakeshore Retail", "SF Office 1", "SF Office 2"]
    satisfaction = [4.6, 4.5, 4.4, 4.2, 4.0, 3.9, 3.6, 3.2, 3.1]
    fig_sat = go.Figure()
    fig_sat.add_trace(go.Bar(
        x=props, y=satisfaction,
        marker_color=[COLORS["green"] if v >= 4.0 else COLORS["yellow"] if v >= 3.5
                      else COLORS["red"] for v in satisfaction],
        text=[f"{v}" for v in satisfaction],
        textposition="outside", textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_sat.add_hline(y=4.0, line_dash="dash", line_color=COLORS["green"],
                      opacity=0.6, annotation_text="Target (4.0)",
                      annotation_font_color=COLORS["green"],
                      annotation_font_size=10)
    fig_sat.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="Score (1-5)", showgrid=True,
                   gridcolor=COLORS["border"], range=[2.5, 5.2]),
        xaxis=dict(tickangle=-30),
    ))
    sat_left = dcc.Graph(figure=fig_sat, config=CHART_CONFIG,
                         style={"height": "300px"})

    complaint_donut = dcc.Graph(
        figure=donut_figure(
            labels=["HVAC/Climate", "Elevator", "Parking", "Cleaning", "Security", "Noise"],
            values=[32, 22, 16, 14, 10, 6],
            colors=[COLORS["red"], COLORS["yellow"], COLORS["blue"],
                    COLORS["green"], COLORS["purple"], COLORS["text_muted"]],
            center_text="248",
            title="Complaints by Category",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    tab_tenant = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Tenant Satisfaction by Property", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), sat_left], padding="20px"),
            _card([html.Div("Complaint Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), complaint_donut], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Properties Managed", "48", "blue"),
        ("CapEx Spend YTD", "$18.7M", "green"),
        ("Maintenance Completion", "98.2%", "green"),
        ("Deferred Backlog", "$18.4M", "red"),
        ("Avg Satisfaction", "3.9/5.0", "yellow"),
    ]

    insight = insight_card(
        headline="SF Office Tenant Satisfaction Critical",
        metric_value="3.2",
        direction="down",
        narrative=(
            "Tenant satisfaction at SF office properties has dropped to "
            "3.2/5.0 from 3.8 last quarter. HVAC and elevator complaints "
            "account for 54% of all service requests. Two anchor tenants "
            "have cited facility conditions in early lease renegotiations."
        ),
        action_text="Allocate emergency CapEx for HVAC and elevator upgrades at SF offices",
        severity="critical",
    )

    return layout_split(
        title="Asset Management",
        subtitle="Property operations, capital expenditures, and tenant experience",
        tab_contents=[
            ("Maintenance", tab_maintenance),
            ("CapEx", tab_capex),
            ("Tenant Ops", tab_tenant),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. ESG & SUSTAINABILITY  (Style F — layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_sustainability(cfg):
    """ESG and sustainability grid with gauges, sparklines, and progress."""

    # --- Grid item 1: Energy Star Score gauge (tall, spans 2 rows) ----------
    gauge_energy = gauge_figure(
        value=78, max_val=100, title="Energy Star Score",
        color=COLORS["green"],
    )
    energy_panel = html.Div([
        dcc.Graph(figure=gauge_energy, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "12px"},
            children=[
                html.Div("ABOVE AVERAGE", style={
                    "fontSize": "18px", "fontWeight": "700",
                    "color": COLORS["green"], "letterSpacing": "1px",
                }),
                html.Div("Portfolio weighted average", style={
                    "fontSize": "12px", "color": COLORS["text_muted"],
                    "marginTop": "4px",
                }),
            ],
        ),
        html.Div(
            style={"marginTop": "20px"},
            children=[
                progress_row("Office Assets", "72", 72, COLORS["yellow"]),
                progress_row("Industrial", "81", 81, COLORS["green"]),
                progress_row("Multifamily", "76", 76, COLORS["green"]),
                progress_row("Data Center", "84", 84, COLORS["green"]),
            ],
        ),
    ])

    # --- Grid item 2: Carbon Intensity sparkline ----------------------------
    carbon_data = [12.1, 11.8, 11.4, 11.0, 10.6, 10.2, 9.8, 9.5, 9.1, 8.8,
                   8.6, 8.4, 8.4, 8.4, 8.5, 8.4, 8.4, 8.3, 8.4, 8.4]
    carbon_panel = metric_with_sparkline(
        "Carbon Intensity (kgCO2e/SF)", "8.4",
        carbon_data, accent="green",
    )

    # --- Grid item 3: LEED Certification metric -----------------------------
    leed_data = [28, 30, 31, 33, 34, 35, 36, 37, 38, 39,
                 39, 40, 40, 41, 41, 42, 42, 42, 42, 42]
    leed_panel = metric_with_sparkline(
        "LEED Certified (%)", "42%",
        leed_data, accent="blue",
    )

    # --- Grid item 4: GRESB Performance (wide, spans 2 cols) ----------------
    gresb_categories = ["Management", "Performance", "Development", "Resilience", "Overall"]
    gresb_scores = [82, 74, 68, 71, 76]
    gresb_benchmark = [78, 72, 65, 68, 72]

    fig_gresb = go.Figure()
    fig_gresb.add_trace(go.Bar(
        x=gresb_categories, y=gresb_scores, name="Portfolio",
        marker_color=COLORS["blue"],
        text=gresb_scores, textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_gresb.add_trace(go.Bar(
        x=gresb_categories, y=gresb_benchmark, name="Peer Benchmark",
        marker_color=COLORS["border"],
        text=gresb_benchmark, textposition="outside",
        textfont=dict(color=COLORS["text_muted"], size=11),
    ))
    fig_gresb.update_layout(**dark_chart_layout(
        height=200, barmode="group",
        margin=dict(l=40, r=20, t=36, b=36),
        title=dict(text="GRESB Score vs Peer Benchmark",
                   font=dict(size=13, color=COLORS["white"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"], range=[0, 100]),
        legend=dict(orientation="h", y=-0.3, x=0.5, xanchor="center"),
    ))
    gresb_panel = html.Div([
        dcc.Graph(figure=fig_gresb, config=CHART_CONFIG,
                  style={"height": "200px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "marginTop": "8px", "padding": "0 4px"},
            children=[
                html.Div([
                    html.Span("GRESB Rating: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("3 Star", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Span("Percentile: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("68th", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["blue"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 5: Water Usage Reduction ---------------------------------
    water_data = [5, 6, 8, 9, 10, 11, 12, 13, 14, 15,
                  15, 16, 16, 17, 17, 17, 18, 18, 18, 18]
    water_panel = metric_with_sparkline(
        "Water Usage Reduction (%)", "18%",
        water_data, accent="blue",
    )

    # --- Grid item 6: Renewable Energy gauge --------------------------------
    gauge_renewable = gauge_figure(
        value=34, max_val=100, title="Renewable Energy %",
        color=COLORS["green"],
    )
    renewable_panel = html.Div([
        dcc.Graph(figure=gauge_renewable, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-around",
                   "marginTop": "8px"},
            children=[
                html.Div([
                    html.Div("2030 Target", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("50%", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["yellow"]}),
                ]),
                html.Div([
                    html.Div("Solar Installed", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("2.4 MW", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["green"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 7: Green Lease Adoption ----------------------------------
    green_gauge = gauge_figure(
        value=58, max_val=100, title="Green Lease Adoption %",
        color=COLORS["blue"],
    )
    green_lease_panel = html.Div([
        dcc.Graph(figure=green_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"marginTop": "12px"},
            children=[
                progress_row("Office Leases", "72%", 72, COLORS["green"]),
                progress_row("Industrial", "48%", 48, COLORS["yellow"]),
                progress_row("Retail", "42%", 42, COLORS["yellow"]),
                progress_row("Multifamily", "65%", 65, COLORS["green"]),
            ],
        ),
    ])

    # --- Assemble grid ------------------------------------------------------
    grid_items = [
        {"col_span": 1, "row_span": 2, "content": energy_panel},
        {"col_span": 1, "row_span": 1, "content": carbon_panel},
        {"col_span": 1, "row_span": 1, "content": leed_panel},
        {"col_span": 2, "row_span": 1, "content": gresb_panel},
        {"col_span": 1, "row_span": 1, "content": water_panel},
        {"col_span": 1, "row_span": 1, "content": renewable_panel},
        {"col_span": 1, "row_span": 1, "content": green_lease_panel},
    ]

    insight = insight_card(
        headline="Carbon Reduction on Track for 2030 Target",
        metric_value="-31%",
        direction="down",
        narrative=(
            "Portfolio carbon intensity has decreased 31% from the 2019 "
            "baseline to 8.4 kgCO2e/SF. On track for the 50% reduction "
            "target by 2030. Key drivers: LED retrofits across 82% of "
            "properties, on-site solar at 12 assets, and HVAC optimization "
            "programs. Next priority: expanding renewable energy from "
            "34% to 50% through additional PPAs and rooftop solar."
        ),
        action_text="Evaluate PPA options for 8 properties without renewable energy",
        severity="healthy",
        sparkline_values=[12.1, 11.4, 10.6, 10.2, 9.8, 9.5, 9.1, 8.8, 8.6, 8.4],
    )

    return layout_grid(
        title="ESG & Sustainability",
        subtitle="Environmental metrics, GRESB benchmarking, and sustainability progress",
        grid_items=grid_items,
        insight=insight,
    )
