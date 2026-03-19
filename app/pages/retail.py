"""Retail vertical pages for Blueprint IQ.

Seven page renderers covering omnichannel retail analytics:
  - Dashboard (executive overview)
  - Merchandising & Assortment (category performance)
  - Customer Analytics (segments & loyalty)
  - Store Operations (performance forecast)
  - Supply Chain & Inventory (inventory table)
  - E-Commerce & Digital (digital channels)
  - Loss Prevention & Shrink (security grid)
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
    """Executive retail dashboard with hero metrics, revenue trends, and panels."""
    vt = get_vertical_theme("retail")

    # --- Morning Briefing (AI Narrative Center) ------------------------------
    briefing = morning_briefing(
        title="Retail Morning Briefing",
        summary_text=(
            "Total revenue is pacing at $148M month-to-date, 4.2% ahead of comp "
            "stores year-over-year. Gross margin held steady at 38.2% despite "
            "aggressive promotional activity in Electronics (-120 bps). E-commerce "
            "share reached 34%, a new high, driven by a 22% surge in mobile app "
            "orders. However, shrinkage rate ticked up to 1.4% — 20 bps above "
            "target — with organized retail crime incidents up 18% in the Southwest "
            "region. Stockout rate of 3.1% in Grocery requires immediate attention "
            "ahead of the weekend demand spike. Recommend reallocating safety stock "
            "from overstocked Home & Garden categories."
        ),
        signals=[
            {"label": "Comp Sales", "status": "green", "detail": "+4.2% YoY — Apparel and Beauty leading growth"},
            {"label": "Gross Margin", "status": "green", "detail": "38.2% — stable despite promotional pressure"},
            {"label": "Shrinkage", "status": "red", "detail": "1.4% of sales — ORC up 18% in Southwest"},
            {"label": "Stockouts", "status": "amber", "detail": "3.1% rate — Grocery critical ahead of weekend"},
        ],
    )

    # --- North Star hero metric ---------------------------------------------
    north_star = hero_metric("Revenue MTD", "$148M",
                              trend_text="+4.2% vs prior year", trend_dir="up",
                              accent="blue", href="/retail/merchandising")

    # --- Hero metrics -------------------------------------------------------
    heroes = [
        north_star,
        hero_metric("Gross Margin", "38.2%",
                     trend_text="Flat vs prior month", trend_dir="flat",
                     accent="green", href="/retail/merchandising"),
        hero_metric("Conversion Rate", "3.4%",
                     trend_text="+0.3 pts vs prior month", trend_dir="up",
                     accent="blue", href="/retail/store_ops"),
        hero_metric("NPS", "42",
                     trend_text="+3 pts vs last quarter", trend_dir="up",
                     accent="green", href="/retail/customer_analytics"),
    ]

    # --- Main chart: revenue trends by channel -------------------------------
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    instore_rev  = [78, 82, 80, 85, 110, 125, 72, 74, 79, 81, 84, 88]
    ecomm_rev    = [28, 30, 32, 35, 52, 58, 30, 32, 35, 38, 40, 44]
    mobile_rev   = [12, 14, 15, 17, 24, 28, 16, 18, 20, 22, 24, 26]

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=months, y=instore_rev, name="In-Store",
        line=dict(color=COLORS["blue"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=months, y=ecomm_rev, name="E-Commerce",
        line=dict(color=COLORS["purple"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['purple'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=months, y=mobile_rev, name="Mobile App",
        line=dict(color=COLORS["green"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['green'])}, 0.08)",
    ))
    fig_trend.update_layout(**dark_chart_layout(
        vertical="retail",
        height=320,
        title=dict(text="Revenue by Channel ($M)", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Revenue ($M)", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_trend, config=CHART_CONFIG,
                           style={"height": "320px"})

    # --- Bottom panels --------------------------------------------------------
    # Panel 1: department revenue donut
    donut = donut_figure(
        labels=["Apparel", "Electronics", "Home & Garden", "Grocery", "Beauty", "Sports"],
        values=[28, 22, 18, 16, 10, 6],
        colors=[COLORS["blue"], COLORS["purple"], COLORS["green"],
                COLORS["yellow"], COLORS["red"], COLORS["text_muted"]],
        center_text="100%",
        title="Revenue by Department",
    )
    panel_donut = dcc.Graph(figure=donut, config=CHART_CONFIG,
                            style={"height": "280px"})

    # Panel 2: store format performance bar chart
    fig_format = go.Figure()
    formats = ["Flagship", "Mall", "Outlet", "Express", "Dark Store"]
    sales_sqft = [685, 520, 410, 380, 0]
    conv_rate = [4.8, 3.6, 3.2, 2.9, 0]
    fig_format.add_trace(go.Bar(
        x=formats, y=sales_sqft, name="Sales/Sq Ft ($)",
        marker_color=COLORS["blue"],
        text=[f"${v}" for v in sales_sqft],
        textposition="outside", textfont=dict(color=COLORS["blue"], size=10),
    ))
    fig_format.add_trace(go.Scatter(
        x=formats, y=[v * 100 for v in conv_rate], name="Conv Rate %",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_format.update_layout(**dark_chart_layout(
        vertical="retail",
        height=280,
        title=dict(text="Store Format Performance", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Sales / Sq Ft ($)", showgrid=True, gridcolor=COLORS["border"]),
        yaxis2=dict(title="Conversion %", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    panel_format = dcc.Graph(figure=fig_format, config=CHART_CONFIG,
                              style={"height": "280px"})

    # --- Insight card ---------------------------------------------------------
    retail_insight = insight_card(
        headline="Grocery Stockouts Threaten Weekend Revenue",
        metric_value="3.1%",
        direction="up",
        narrative=(
            "Grocery stockout rate has climbed to 3.1%, up from 1.8% last month, "
            "driven by supplier delays in fresh produce and dairy. With weekend "
            "foot traffic projected at 48K visitors — 15% above average — every "
            "1% stockout costs an estimated $420K in lost sales. Home & Garden "
            "is carrying 42 days of supply, 14 days above target, creating a "
            "rebalancing opportunity."
        ),
        action_text="Reallocate safety stock from Home & Garden to Grocery",
        severity="critical",
        sparkline_values=[1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.1],
    )

    panels = [
        ("Revenue by Department", panel_donut, "/retail/merchandising"),
        ("Store Format Performance", panel_format, "/retail/store_ops"),
        ("AI Insight", retail_insight),
    ]

    return layout_executive(
        title="Retail Command Center",
        subtitle="Omnichannel revenue, margin, and operational performance overview",
        heroes=heroes,
        briefing=briefing,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. MERCHANDISING & ASSORTMENT  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_merchandising(cfg):
    """Merchandising view with category performance, pricing, and assortment tabs."""

    banner_text = (
        "Merchandising performance is STRONG. Apparel comp sales +6.8% driven "
        "by spring collection sell-through at 72%. Electronics margin pressure "
        "continues at 24.1% gross margin (-120 bps) due to competitive pricing. "
        "Private label penetration reached 18.4%, a quarterly high. Markdown "
        "optimizer recommends accelerating Beauty clearance by 2 weeks to capture "
        "$2.1M in margin recovery before end-of-season."
    )

    # ── TAB 1: Category Performance ──────────────────────────────────────
    departments = ["Apparel", "Electronics", "Home & Garden", "Grocery", "Beauty", "Sports"]
    revenue_vals = [41.4, 32.6, 26.6, 23.7, 14.8, 8.9]
    margin_vals  = [48.2, 24.1, 42.6, 28.4, 52.8, 38.6]

    fig_cat = go.Figure()
    fig_cat.add_trace(go.Bar(
        x=departments, y=revenue_vals, name="Revenue ($M)",
        marker_color=COLORS["blue"],
        text=[f"${v}M" for v in revenue_vals],
        textposition="outside", textfont=dict(color=COLORS["blue"], size=10),
    ))
    fig_cat.add_trace(go.Scatter(
        x=departments, y=margin_vals, name="Gross Margin %",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_cat.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Revenue ($M)", showgrid=True, gridcolor=COLORS["border"]),
        yaxis2=dict(title="Gross Margin %", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"], range=[0, 60]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    cat_left = dcc.Graph(figure=fig_cat, config=CHART_CONFIG,
                          style={"height": "300px"})

    donut_sell = donut_figure(
        labels=["Sold at Full Price", "Markdown", "Clearance", "Unsold"],
        values=[58, 22, 12, 8],
        colors=[COLORS["green"], COLORS["yellow"], COLORS["red"],
                COLORS["text_muted"]],
        center_text="72%",
        title="Sell-Through Analysis",
    )
    cat_right = dcc.Graph(figure=donut_sell, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_category = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Category Revenue & Margin", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), cat_left], padding="20px"),
            _card([html.Div("Sell-Through Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), cat_right], padding="20px"),
        ],
    )

    # ── TAB 2: Price Optimization ────────────────────────────────────────
    weeks = ["Wk1", "Wk2", "Wk3", "Wk4", "Wk5", "Wk6", "Wk7", "Wk8"]
    fig_price = go.Figure()
    fig_price.add_trace(go.Scatter(
        x=weeks, y=[67.40, 66.80, 68.10, 67.90, 69.20, 68.50, 70.10, 69.80],
        name="AOV ($)", mode="lines+markers",
        line=dict(color=COLORS["blue"], width=2), marker=dict(size=5),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_price.add_trace(go.Scatter(
        x=weeks, y=[3.8, 3.7, 3.9, 3.8, 4.0, 3.9, 4.1, 4.0],
        name="Basket Size (items)", mode="lines+markers",
        line=dict(color=COLORS["green"], width=2), marker=dict(size=5),
        yaxis="y2",
    ))
    fig_price.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="AOV ($)", showgrid=True, gridcolor=COLORS["border"]),
        yaxis2=dict(title="Basket Size", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    price_left = dcc.Graph(figure=fig_price, config=CHART_CONFIG,
                            style={"height": "300px"})

    # Markdown impact by department
    fig_md = go.Figure()
    md_depts = ["Apparel", "Electronics", "Home & Garden", "Grocery", "Beauty", "Sports"]
    md_depth = [22, 18, 28, 8, 32, 15]
    md_lift  = [14, 8, 10, 5, 18, 12]
    fig_md.add_trace(go.Bar(
        x=md_depts, y=md_depth, name="Avg Markdown %",
        marker_color=COLORS["red"],
    ))
    fig_md.add_trace(go.Bar(
        x=md_depts, y=md_lift, name="Unit Lift %",
        marker_color=COLORS["green"],
    ))
    fig_md.update_layout(**dark_chart_layout(
        height=300, barmode="group",
        yaxis=dict(title="Percentage", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    price_right = dcc.Graph(figure=fig_md, config=CHART_CONFIG,
                             style={"height": "300px"})

    tab_price = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("AOV & Basket Size Trend", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), price_left], padding="20px"),
            _card([html.Div("Markdown Depth vs Unit Lift", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), price_right], padding="20px"),
        ],
    )

    # ── TAB 3: Assortment ────────────────────────────────────────────────
    assort_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Core", "Seasonal", "Trend", "Private Label", "Exclusive"],
            values=[42, 22, 15, 18, 3],
            colors=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                    COLORS["yellow"], COLORS["red"]],
            center_text="14.2K",
            title="Active SKU Mix",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    # SKU productivity by department
    qtrs = ["Q1 '25", "Q2 '25", "Q3 '25", "Q4 '25", "Q1 '26"]
    fig_sku = go.Figure()
    fig_sku.add_trace(go.Bar(
        x=qtrs, y=[4.2, 4.5, 4.3, 4.8, 5.1], name="Sales per SKU ($K)",
        marker_color=COLORS["blue"],
    ))
    fig_sku.add_trace(go.Scatter(
        x=qtrs, y=[14.0, 13.8, 14.2, 13.6, 13.4], name="Active SKUs (K)",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_sku.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Sales per SKU ($K)", showgrid=True,
                   gridcolor=COLORS["border"]),
        yaxis2=dict(title="Active SKUs (K)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    sku_right = dcc.Graph(figure=fig_sku, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_assort = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Active SKU Mix Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), assort_donut], padding="20px"),
            _card([html.Div("SKU Productivity & Rationalization", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), sku_right], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Revenue MTD", "$148M", "blue"),
        ("Gross Margin", "38.2%", "green"),
        ("Sell-Through", "72%", "blue"),
        ("Private Label", "18.4%", "purple"),
        ("Markdown Rate", "22%", "yellow"),
    ]

    insight = insight_card(
        headline="Beauty Clearance Window Closing",
        metric_value="$2.1M",
        direction="up",
        narrative=(
            "Markdown optimizer projects $2.1M in margin recovery if Beauty "
            "clearance is accelerated by 2 weeks. Current sell-through at 58% "
            "with 6 weeks remaining in season — historical pattern shows 80% "
            "of recovery value is captured in the first week of markdown."
        ),
        severity="warning",
    )

    return layout_split(
        title="Merchandising & Assortment",
        subtitle="Category performance, pricing strategy, and assortment productivity",
        tab_contents=[
            ("Category Performance", tab_category),
            ("Price Optimization", tab_price),
            ("Assortment", tab_assort),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. CUSTOMER ANALYTICS  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_customer_analytics(cfg):
    """Customer analytics table with segment performance and loyalty metrics."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Segment", "options": ["All Segments", "Loyalty Platinum", "Loyalty Gold", "Loyalty Silver", "Casual", "New"]},
        {"label": "Region", "options": ["All Regions", "Northeast", "Southeast", "Midwest", "West Coast", "Southwest"]},
        {"label": "Channel", "options": ["All Channels", "In-Store", "E-Commerce", "Mobile App", "Social Commerce"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Total Customers", "value": "2.4M", "accent": "blue"},
        {"label": "Loyalty Penetration", "value": "58%", "accent": "green"},
        {"label": "Avg CLV", "value": "$1,240", "accent": "blue"},
        {"label": "Repeat Rate", "value": "42%", "accent": "green"},
        {"label": "Churn Risk", "value": "12.4%", "accent": "red"},
    ]

    # --- Table data ---------------------------------------------------------
    segments = [
        ("Loyalty Platinum", "142K", "$2,840", "78%", "4.2", "68",
         "$403M", "2.1%", "Healthy"),
        ("Loyalty Gold", "318K", "$1,680", "62%", "3.6", "52",
         "$534M", "5.8%", "Healthy"),
        ("Loyalty Silver", "486K", "$920", "48%", "2.8", "44",
         "$447M", "9.2%", "Warning"),
        ("Casual", "724K", "$480", "24%", "1.6", "32",
         "$348M", "18.6%", "Warning"),
        ("New (< 90 days)", "312K", "$120", "12%", "1.2", "38",
         "$37M", "28.4%", "Critical"),
        ("Lapsed (> 180 days)", "418K", "$640", "0%", "0", "22",
         "$0M", "72.1%", "Critical"),
    ]

    columns = [
        {"name": "Segment", "id": "segment"},
        {"name": "Customers", "id": "customers"},
        {"name": "Avg CLV", "id": "clv"},
        {"name": "Repeat Rate", "id": "repeat_rate"},
        {"name": "Visits/Yr", "id": "visits"},
        {"name": "NPS", "id": "nps"},
        {"name": "Revenue", "id": "revenue"},
        {"name": "Churn Risk", "id": "churn_risk"},
        {"name": "Status", "id": "status"},
    ]

    data = []
    for seg, cust, clv, repeat, visits, nps, rev, churn, status in segments:
        data.append({
            "segment": seg,
            "customers": cust,
            "clv": clv,
            "repeat_rate": repeat,
            "visits": visits,
            "nps": nps,
            "revenue": rev,
            "churn_risk": churn,
            "status": status,
        })

    insight = insight_card(
        headline="Silver-to-Gold Upgrade Opportunity",
        metric_value="$48M",
        direction="up",
        narrative=(
            "Churn model identifies 62K Loyalty Silver members with high upgrade "
            "propensity. If 30% convert to Gold, projected annual revenue lift is "
            "$48M with $760 incremental CLV per customer. Recommended trigger: "
            "personalized offer after third purchase in 60 days."
        ),
        severity="healthy",
    )

    return layout_table(
        title="Customer Analytics",
        subtitle="Customer segment performance, lifetime value, and loyalty metrics",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. STORE OPERATIONS  (Style E — layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_store_ops(cfg):
    """Store operations with performance forecast, KPIs, and regional breakdown."""

    # --- KPIs ---------------------------------------------------------------
    kpis = [
        {"label": "Sales / Sq Ft", "value": "$524", "accent": "blue"},
        {"label": "Conversion Rate", "value": "3.4%", "accent": "green"},
        {"label": "Sales / Labor Hr", "value": "$142.60", "accent": "blue"},
        {"label": "CSAT Score", "value": "82", "accent": "green"},
    ]

    # --- Main chart: store performance trend with forecast -----------------
    import math
    days = list(range(1, 61))
    daily_rev = [
        2.4 + 0.5 * math.sin(d / 7 * 3.14) + (d % 7 == 6) * 0.8
        + (d % 7 == 0) * 1.1 for d in days
    ]
    # Forecast line (last 15 days are projected)
    actual = daily_rev[:45]
    forecast = [None] * 44 + daily_rev[44:]
    upper_band = [None] * 44 + [v + 0.4 for v in daily_rev[44:]]
    lower_band = [None] * 44 + [max(v - 0.4, 0) for v in daily_rev[44:]]

    fig_perf = go.Figure()
    fig_perf.add_trace(go.Scatter(
        x=days[:45], y=actual, name="Actual Revenue",
        line=dict(color=COLORS["blue"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_perf.add_trace(go.Scatter(
        x=days, y=forecast, name="Forecast",
        line=dict(color=COLORS["purple"], width=2, dash="dash"),
    ))
    fig_perf.add_trace(go.Scatter(
        x=days, y=upper_band, name="Upper CI",
        line=dict(color=COLORS["purple"], width=0),
        showlegend=False,
    ))
    fig_perf.add_trace(go.Scatter(
        x=days, y=lower_band, name="Lower CI",
        line=dict(color=COLORS["purple"], width=0),
        fill="tonexty", fillcolor=f"rgba({_hex_to_rgb(COLORS['purple'])}, 0.12)",
        showlegend=False,
    ))
    fig_perf.update_layout(**dark_chart_layout(
        height=300,
        xaxis=dict(title="Day", showgrid=False),
        yaxis=dict(title="Daily Revenue ($M)", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_perf, config=CHART_CONFIG,
                           style={"height": "300px"})

    # --- Side breakdown by region -------------------------------------------
    side = breakdown_list([
        {"label": "Northeast", "value": "$38.2M", "pct": 26, "color": COLORS["blue"]},
        {"label": "Southeast", "value": "$32.6M", "pct": 22, "color": COLORS["green"]},
        {"label": "West Coast", "value": "$31.1M", "pct": 21, "color": COLORS["purple"]},
        {"label": "Midwest", "value": "$28.4M", "pct": 19, "color": COLORS["yellow"]},
        {"label": "Southwest", "value": "$17.7M", "pct": 12, "color": COLORS["red"]},
    ])

    insight = insight_card(
        headline="Weekend Revenue Forecast Above Target",
        metric_value="+8.2%",
        direction="up",
        narrative=(
            "Demand forecast projects weekend revenue at $12.4M across all stores — "
            "8.2% above plan. Flagship and Mall formats expected to over-index due "
            "to spring promotion launch. Recommend increasing floor staff by 12% "
            "on Saturday to maximize conversion opportunity."
        ),
        severity="healthy",
    )

    return layout_forecast(
        title="Store Operations",
        subtitle="Store performance forecasting and regional analytics",
        kpi_items=kpis,
        hero_value="$524",
        hero_label="Average Sales per Square Foot (Trailing 12M)",
        hero_trend_text="+3.8% vs prior year",
        main_chart=main_chart,
        side_component=html.Div([
            html.Div("Revenue by Region",
                     style={"fontSize": "14px", "fontWeight": "600",
                            "color": COLORS["white"], "marginBottom": "16px"}),
            side,
        ]),
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. SUPPLY CHAIN & INVENTORY  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_supply_chain(cfg):
    """Supply chain and inventory table with fill rates and supplier metrics."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Department", "options": ["All Departments", "Apparel", "Electronics", "Home & Garden", "Grocery", "Beauty", "Sports"]},
        {"label": "Region", "options": ["All Regions", "Northeast", "Southeast", "Midwest", "West Coast", "Southwest"]},
        {"label": "Status", "options": ["All Statuses", "Healthy", "Warning", "Critical"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Fill Rate", "value": "96.4%", "accent": "green"},
        {"label": "Stockout Rate", "value": "3.1%", "accent": "red"},
        {"label": "Days of Supply", "value": "28.4", "accent": "blue"},
        {"label": "Supplier OTIF", "value": "91.8%", "accent": "green"},
        {"label": "Inv. Turnover", "value": "8.2x", "accent": "blue"},
    ]

    # --- Table data ---------------------------------------------------------
    inventory = [
        ("Apparel", "96.8%", "2.4%", "32", "93.2%", "9.4x",
         "$42.1M", "Healthy"),
        ("Electronics", "94.2%", "4.8%", "18", "89.6%", "12.1x",
         "$28.4M", "Warning"),
        ("Home & Garden", "97.4%", "1.8%", "42", "94.8%", "5.2x",
         "$38.6M", "Warning"),
        ("Grocery", "92.1%", "6.2%", "8", "88.4%", "24.6x",
         "$12.8M", "Critical"),
        ("Beauty", "98.2%", "1.2%", "36", "96.1%", "6.8x",
         "$18.2M", "Healthy"),
        ("Sports", "95.6%", "3.1%", "24", "91.4%", "8.4x",
         "$14.6M", "Healthy"),
    ]

    columns = [
        {"name": "Department", "id": "department"},
        {"name": "Fill Rate", "id": "fill_rate"},
        {"name": "Stockout Rate", "id": "stockout_rate"},
        {"name": "Days of Supply", "id": "days_supply"},
        {"name": "Supplier OTIF", "id": "otif"},
        {"name": "Inv. Turns", "id": "turns"},
        {"name": "Inv. Value", "id": "inv_value"},
        {"name": "Status", "id": "status"},
    ]

    data = []
    for dept, fill, stockout, dos, otif, turns, inv_val, status in inventory:
        data.append({
            "department": dept,
            "fill_rate": fill,
            "stockout_rate": stockout,
            "days_supply": dos,
            "otif": otif,
            "turns": turns,
            "inv_value": inv_val,
            "status": status,
        })

    insight = insight_card(
        headline="Grocery Fresh Produce Supply Risk",
        metric_value="6.2%",
        direction="up",
        narrative=(
            "Grocery stockout rate has surged to 6.2%, driven by two key suppliers "
            "missing OTIF targets — FreshFarms at 78% and Valley Produce at 82%. "
            "Days of supply for perishables dropped to 3.2 days against a 5-day "
            "target. Recommend activating backup supplier agreements and diverting "
            "shipments from overstocked regions."
        ),
        severity="critical",
    )

    return layout_table(
        title="Supply Chain & Inventory",
        subtitle="Inventory health, supplier performance, and fulfillment metrics",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. E-COMMERCE & DIGITAL  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_ecommerce(cfg):
    """E-commerce dashboard with traffic, conversion, and fulfillment tabs."""

    banner_text = (
        "Digital channels are at an all-time high with 34% of total revenue. "
        "Mobile app conversion reached 2.8%, up 40 bps from last quarter. "
        "Cart abandonment remains elevated at 68.4% — checkout friction on "
        "mobile is the primary driver. BOPIS adoption grew 28% QoQ and drives "
        "a 22% higher basket size versus ship-to-home. Same-day fulfillment "
        "SLA compliance dropped to 87% in the Southwest region."
    )

    # ── TAB 1: Traffic ──────────────────────────────────────────────────
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar"]
    fig_traffic = go.Figure()
    fig_traffic.add_trace(go.Scatter(
        x=months, y=[2.1, 2.3, 2.4, 2.6, 3.8, 4.2, 2.2, 2.4, 2.6],
        name="Desktop Sessions (M)", mode="lines+markers",
        line=dict(color=COLORS["blue"], width=2), marker=dict(size=5),
    ))
    fig_traffic.add_trace(go.Scatter(
        x=months, y=[3.4, 3.6, 3.8, 4.1, 5.6, 6.2, 3.8, 4.0, 4.4],
        name="Mobile Sessions (M)", mode="lines+markers",
        line=dict(color=COLORS["purple"], width=2), marker=dict(size=5),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['purple'])}, 0.08)",
    ))
    fig_traffic.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Sessions (M)", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    traffic_left = dcc.Graph(figure=fig_traffic, config=CHART_CONFIG,
                              style={"height": "300px"})

    traffic_donut = donut_figure(
        labels=["Organic", "Paid Search", "Social", "Email", "Direct", "Referral"],
        values=[32, 24, 18, 12, 10, 4],
        colors=[COLORS["green"], COLORS["blue"], COLORS["purple"],
                COLORS["yellow"], COLORS["red"], COLORS["text_muted"]],
        center_text="7.0M",
        title="Traffic Source Mix",
    )
    traffic_right = dcc.Graph(figure=traffic_donut, config=CHART_CONFIG,
                               style={"height": "300px"})

    tab_traffic = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Session Volume by Device", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), traffic_left], padding="20px"),
            _card([html.Div("Traffic Source Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), traffic_right], padding="20px"),
        ],
    )

    # ── TAB 2: Conversion ────────────────────────────────────────────────
    funnel_stages = ["Product View", "Add to Cart", "Checkout Start", "Payment", "Confirmation"]
    funnel_vals   = [100, 42, 28, 18, 12]
    funnel_colors = [COLORS["blue"], COLORS["purple"], COLORS["yellow"],
                     COLORS["green"], COLORS["green"]]

    fig_funnel = go.Figure(go.Bar(
        x=funnel_vals, y=funnel_stages, orientation="h",
        marker_color=funnel_colors,
        text=[f"{v}%" for v in funnel_vals],
        textposition="auto",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_funnel.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        xaxis=dict(title="% of Sessions", showgrid=True,
                   gridcolor=COLORS["border"]),
        margin=dict(l=120, r=24, t=24, b=48),
    ))
    conv_left = dcc.Graph(figure=fig_funnel, config=CHART_CONFIG,
                           style={"height": "300px"})

    # Conversion rate by device
    fig_conv_device = go.Figure()
    devices = ["Desktop", "Mobile App", "Mobile Web", "Tablet"]
    conv_rates = [4.2, 2.8, 1.6, 3.4]
    aov_vals = [82, 64, 58, 76]
    fig_conv_device.add_trace(go.Bar(
        x=devices, y=conv_rates, name="Conversion %",
        marker_color=COLORS["blue"],
        text=[f"{v}%" for v in conv_rates],
        textposition="outside", textfont=dict(color=COLORS["blue"], size=10),
    ))
    fig_conv_device.add_trace(go.Scatter(
        x=devices, y=aov_vals, name="AOV ($)",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_conv_device.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Conversion %", showgrid=True,
                   gridcolor=COLORS["border"]),
        yaxis2=dict(title="AOV ($)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    conv_right = dcc.Graph(figure=fig_conv_device, config=CHART_CONFIG,
                            style={"height": "300px"})

    tab_conversion = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Purchase Funnel", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), conv_left], padding="20px"),
            _card([html.Div("Conversion & AOV by Device", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), conv_right], padding="20px"),
        ],
    )

    # ── TAB 3: Fulfillment ───────────────────────────────────────────────
    fulfill_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Ship to Home", "BOPIS", "Curbside", "Same-Day", "Store Ship"],
            values=[48, 22, 12, 10, 8],
            colors=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                    COLORS["yellow"], COLORS["red"]],
            center_text="100%",
            title="Fulfillment Mix",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    # Fulfillment SLA by method
    methods = ["Ship to Home", "BOPIS", "Curbside", "Same-Day"]
    sla_met = [94, 97, 92, 87]
    fig_sla = go.Figure(go.Bar(
        x=methods, y=sla_met,
        marker_color=[COLORS["green"] if v >= 95 else COLORS["yellow"]
                      if v >= 90 else COLORS["red"] for v in sla_met],
        text=[f"{v}%" for v in sla_met],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_sla.add_hline(y=95, line_dash="dash", line_color=COLORS["green"],
                       opacity=0.6, annotation_text="SLA Target (95%)",
                       annotation_font_color=COLORS["green"],
                       annotation_font_size=10)
    fig_sla.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="SLA Met %", showgrid=True,
                   gridcolor=COLORS["border"], range=[70, 105]),
        margin=dict(l=48, r=24, t=24, b=48),
    ))
    fulfill_right = dcc.Graph(figure=fig_sla, config=CHART_CONFIG,
                               style={"height": "300px"})

    tab_fulfillment = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Fulfillment Method Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), fulfill_donut], padding="20px"),
            _card([html.Div("Fulfillment SLA Compliance", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), fulfill_right], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Digital Revenue", "$50.3M", "blue"),
        ("Conversion Rate", "2.8%", "green"),
        ("Cart Abandon", "68.4%", "red"),
        ("BOPIS Share", "22%", "purple"),
        ("Avg Ship Time", "2.4 days", "blue"),
    ]

    insight = insight_card(
        headline="Mobile Checkout Friction Costing $3.8M",
        metric_value="68.4%",
        direction="up",
        narrative=(
            "Cart abandonment on mobile web is 74.2% versus 58.1% on desktop — "
            "the gap widened by 6 pts this quarter. Checkout session recordings "
            "show 42% of drop-offs occur at the payment step. Estimated revenue "
            "loss: $3.8M per month. A/B test for one-tap checkout projected to "
            "recover 18-22% of abandoned carts."
        ),
        severity="critical",
    )

    return layout_split(
        title="E-Commerce & Digital",
        subtitle="Digital traffic, conversion funnel, and omnichannel fulfillment",
        tab_contents=[
            ("Traffic", tab_traffic),
            ("Conversion", tab_conversion),
            ("Fulfillment", tab_fulfillment),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. LOSS PREVENTION & SHRINK  (Style F — layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_loss_prevention(cfg):
    """Loss prevention grid with gauges, sparklines, and shrinkage metrics."""

    # --- Grid item 1: Shrinkage Rate gauge (tall, spans 2 rows) ------------
    gauge_shrink = gauge_figure(
        value=1.4, max_val=3.0, title="Shrinkage Rate %",
        color=COLORS["red"],
    )
    shrink_panel = html.Div([
        dcc.Graph(figure=gauge_shrink, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "12px"},
            children=[
                html.Div("ELEVATED", style={
                    "fontSize": "18px", "fontWeight": "700",
                    "color": COLORS["red"], "letterSpacing": "1px",
                }),
                html.Div("Target: 1.2% | Industry Avg: 1.6%", style={
                    "fontSize": "12px", "color": COLORS["text_muted"],
                    "marginTop": "4px",
                }),
            ],
        ),
        html.Div(
            style={"marginTop": "20px"},
            children=[
                progress_row("External Theft", "$12.4M", 52, COLORS["red"]),
                progress_row("Internal Theft", "$4.8M", 20, COLORS["yellow"]),
                progress_row("Admin Error", "$3.6M", 15, COLORS["blue"]),
                progress_row("Vendor Fraud", "$3.1M", 13, COLORS["purple"]),
            ],
        ),
    ])

    # --- Grid item 2: ORC Incidents sparkline ------------------------------
    orc_data = [18, 22, 20, 25, 28, 24, 32, 30, 35, 38,
                34, 42, 40, 45, 44, 48, 46, 52, 50, 54]
    orc_panel = metric_with_sparkline(
        "ORC Incidents (30d)", "54",
        orc_data, accent="red",
    )

    # --- Grid item 3: Exception Alerts metric ------------------------------
    exception_data = [120, 135, 128, 142, 138, 150, 145, 162, 158, 170,
                      164, 178, 172, 185, 180, 192, 188, 198, 194, 204]
    exception_panel = metric_with_sparkline(
        "Exception Alerts (7d)", "204",
        exception_data, accent="yellow",
    )

    # --- Grid item 4: Shrinkage by department (wide, spans 2 cols) --------
    dept_cats = ["Apparel", "Electronics", "Beauty", "Grocery", "Sports", "Home & Garden"]
    dept_shrink = [1.8, 2.4, 1.6, 0.8, 1.2, 0.6]
    dept_colors = [COLORS["red"] if v > 1.5 else COLORS["yellow"]
                   if v > 1.0 else COLORS["green"] for v in dept_shrink]

    fig_dept = go.Figure(go.Bar(
        x=dept_cats, y=dept_shrink,
        marker_color=dept_colors,
        text=[f"{v}%" for v in dept_shrink],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_dept.add_hline(y=1.2, line_dash="dash", line_color=COLORS["yellow"],
                        opacity=0.6, annotation_text="Target (1.2%)",
                        annotation_font_color=COLORS["yellow"],
                        annotation_font_size=10)
    fig_dept.update_layout(**dark_chart_layout(
        height=200, margin=dict(l=40, r=20, t=36, b=36),
        title=dict(text="Shrinkage Rate by Department",
                   font=dict(size=13, color=COLORS["white"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"], range=[0, 3.0]),
    ))
    dept_panel = html.Div([
        dcc.Graph(figure=fig_dept, config=CHART_CONFIG,
                  style={"height": "200px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "marginTop": "8px", "padding": "0 4px"},
            children=[
                html.Div([
                    html.Span("Total Loss: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("$23.9M", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["white"]}),
                ]),
                html.Div([
                    html.Span("YoY Change: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("+8.2%", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["red"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 5: Case Resolution Rate metric --------------------------
    resolution_data = [82, 84, 83, 86, 85, 88, 87, 90, 89, 91,
                       90, 92, 91, 93, 92, 94, 93, 95, 94, 96]
    resolution_panel = metric_with_sparkline(
        "Case Resolution Rate", "96%",
        resolution_data, accent="green",
    )

    # --- Grid item 6: Recovery Rate gauge ---------------------------------
    recovery_gauge = gauge_figure(
        value=34, max_val=100, title="Recovery Rate %",
        color=COLORS["yellow"],
    )
    recovery_panel = html.Div([
        dcc.Graph(figure=recovery_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-around",
                   "marginTop": "8px"},
            children=[
                html.Div([
                    html.Div("Recovered", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("$8.1M", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Div("Total Loss", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("$23.9M", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["red"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 7: Camera / EAS Coverage --------------------------------
    coverage_gauge = gauge_figure(
        value=88, max_val=100, title="Surveillance Coverage %",
        color=COLORS["blue"],
    )
    coverage_panel = html.Div([
        dcc.Graph(figure=coverage_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"marginTop": "12px"},
            children=[
                progress_row("Camera Coverage", "92%", 92, COLORS["green"]),
                progress_row("EAS Tag Compliance", "88%", 88, COLORS["yellow"]),
                progress_row("Self-Checkout Audit", "78%", 78, COLORS["yellow"]),
                progress_row("High-Value Lock Cases", "95%", 95, COLORS["green"]),
            ],
        ),
    ])

    # --- Assemble grid ------------------------------------------------------
    grid_items = [
        {"col_span": 1, "row_span": 2, "content": shrink_panel},
        {"col_span": 1, "row_span": 1, "content": orc_panel},
        {"col_span": 1, "row_span": 1, "content": exception_panel},
        {"col_span": 2, "row_span": 1, "content": dept_panel},
        {"col_span": 1, "row_span": 1, "content": resolution_panel},
        {"col_span": 1, "row_span": 1, "content": recovery_panel},
        {"col_span": 1, "row_span": 1, "content": coverage_panel},
    ]

    insight = insight_card(
        headline="Southwest ORC Ring Identified",
        metric_value="+18%",
        direction="up",
        narrative=(
            "Loss prevention analytics detected a coordinated organized retail "
            "crime pattern across 8 Southwest stores — matching suspect profiles, "
            "merchandise selection, and timing windows. Estimated exposure: $1.2M "
            "over the past 90 days. Law enforcement referral initiated and "
            "high-value merchandise lockdown protocols activated in affected stores."
        ),
        severity="critical",
    )

    return layout_grid(
        title="Loss Prevention & Shrink",
        subtitle="Shrinkage analytics, organized retail crime tracking, and asset protection",
        grid_items=grid_items,
        insight=insight,
    )
