"""Energy & Utilities vertical pages for Blueprint IQ.

Seven page renderers covering grid operations and energy transition:
  - Dashboard (grid command center)
  - Generation & Dispatch (fuel mix, capacity, dispatch)
  - Grid Operations (outage table, reliability metrics)
  - Customer Programs (demand response forecast)
  - Asset Health & Reliability (condition table)
  - Energy Transition (renewables, carbon, DER)
  - Regulatory & Rates (gauges, sparklines)
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
    """Grid command center with hero metrics, generation mix, and panels."""
    vt = get_vertical_theme("energy")

    # --- Morning Briefing (AI Narrative Center) ------------------------------
    briefing = morning_briefing(
        title="Grid Morning Briefing",
        summary_text=(
            "System load currently at 68.4 GW, 3.2% above day-ahead forecast "
            "driven by an unexpected heat dome across the Southeast territory. "
            "Reserve margin has tightened to 14.8% — still above the 13% "
            "emergency threshold but trending down. Renewable penetration hit "
            "a new daily record of 31.2% at solar peak yesterday. SAIDI is "
            "trending 8% worse than target due to three vegetation-related "
            "outages on aging feeders in the Northeast. Recommend pre-staging "
            "mutual aid crews for Southeast heat event and accelerating "
            "vegetation management on flagged circuits NE-2247 and NE-3391."
        ),
        signals=[
            {"label": "System Load", "status": "amber", "detail": "68.4 GW — 3.2% above forecast, Southeast heat dome driving overage"},
            {"label": "Reserve Margin", "status": "amber", "detail": "14.8% margin — approaching 13% emergency threshold by Friday"},
            {"label": "Reliability (SAIDI)", "status": "red", "detail": "108.2 min — 8% worse than 100 min target, vegetation events"},
            {"label": "Renewable Output", "status": "green", "detail": "31.2% penetration — new daily record, battery storage at 94% SOC"},
        ],
    )

    # --- North Star hero metric ---------------------------------------------
    north_star = hero_metric("System Load", "68.4 GW",
                              trend_text="3.2% above forecast", trend_dir="up",
                              accent="yellow", href="/energy/grid_ops")

    # --- Hero metrics -------------------------------------------------------
    heroes = [
        north_star,
        hero_metric("Reserve Margin", "14.8%",
                     trend_text="Down from 16.1% last week", trend_dir="down",
                     accent="yellow", href="/energy/grid_ops"),
        hero_metric("Renewable Penetration", "31.2%",
                     trend_text="New daily record", trend_dir="up",
                     accent="green", href="/energy/energy_transition"),
        hero_metric("Avg Cost / MWh", "$42.60",
                     trend_text="2.1% below budget", trend_dir="down",
                     accent="blue", href="/energy/regulatory"),
    ]

    # --- Main chart: generation mix / load trends ----------------------------
    hours = [f"{h:02d}:00" for h in range(24)]
    solar_vals  = [0, 0, 0, 0, 0, 0.2, 1.8, 5.4, 9.2, 12.6, 14.8, 15.2,
                   14.6, 13.1, 10.8, 7.4, 3.6, 0.8, 0, 0, 0, 0, 0, 0]
    wind_vals   = [8.2, 7.8, 8.4, 9.1, 9.6, 9.2, 7.8, 6.4, 5.2, 4.8, 5.1,
                   5.6, 6.2, 6.8, 7.4, 8.2, 9.1, 9.8, 10.4, 10.1, 9.6, 9.2,
                   8.8, 8.4]
    gas_vals    = [18.4, 17.2, 16.1, 15.8, 16.2, 18.6, 22.4, 24.8, 22.6,
                   20.1, 18.4, 17.8, 18.2, 19.4, 21.6, 24.2, 26.8, 28.4,
                   27.2, 25.6, 23.4, 21.8, 20.2, 19.1]
    nuclear_vals = [14.2] * 24
    hydro_vals  = [4.8, 4.8, 4.6, 4.4, 4.6, 5.0, 5.4, 5.8, 5.6, 5.2, 5.0,
                   4.8, 4.6, 4.8, 5.2, 5.6, 5.8, 5.4, 5.0, 4.8, 4.6, 4.8,
                   4.8, 4.8]

    fig_gen = go.Figure()
    for name, vals, color in [
        ("Solar", solar_vals, "#FBBF24"),
        ("Wind", wind_vals, "#60A5FA"),
        ("Natural Gas", gas_vals, "#F97316"),
        ("Nuclear", nuclear_vals, "#A78BFA"),
        ("Hydro", hydro_vals, "#34D399"),
    ]:
        fig_gen.add_trace(go.Scatter(
            x=hours, y=vals, name=name, stackgroup="gen",
            line=dict(width=0.5, color=color),
            fillcolor=f"rgba({_hex_to_rgb(color)}, 0.7)",
        ))
    # Overlay total load line
    total_load = [sum(x) for x in zip(solar_vals, wind_vals, gas_vals,
                                       nuclear_vals, hydro_vals)]
    fig_gen.add_trace(go.Scatter(
        x=hours, y=total_load, name="Total Load",
        line=dict(color=COLORS["white"], width=2, dash="dot"),
        mode="lines",
    ))
    fig_gen.update_layout(**dark_chart_layout(
        vertical="energy",
        height=320,
        title=dict(text="Generation Dispatch Stack (GW) — Today",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="GW", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_gen, config=CHART_CONFIG,
                           style={"height": "320px"})

    # --- Bottom panels -------------------------------------------------------
    # Panel 1: generation mix donut
    donut = donut_figure(
        labels=["Natural Gas", "Nuclear", "Wind", "Solar", "Hydro", "Battery"],
        values=[34, 21, 18, 14, 8, 5],
        colors=["#F97316", "#A78BFA", "#60A5FA", "#FBBF24", "#34D399",
                COLORS["green"]],
        center_text="100%",
        title="Generation Mix (Today)",
    )
    panel_donut = dcc.Graph(figure=donut, config=CHART_CONFIG,
                            style={"height": "280px"})

    # Panel 2: reliability bar chart (SAIDI by territory)
    fig_rel = go.Figure()
    territories = ["Northeast", "Southeast", "Midwest", "Western", "ERCOT"]
    saidi_vals = [128.4, 94.6, 102.1, 88.3, 112.7]
    target_val = [100] * 5
    fig_rel.add_trace(go.Bar(
        x=territories, y=saidi_vals, name="Actual SAIDI",
        marker_color=[COLORS["red"] if v > 100 else COLORS["green"]
                      for v in saidi_vals],
        text=[f"{v}" for v in saidi_vals],
        textposition="outside", textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_rel.add_trace(go.Scatter(
        x=territories, y=target_val, name="Target (100 min)",
        line=dict(color=COLORS["yellow"], width=2, dash="dash"),
        mode="lines",
    ))
    fig_rel.update_layout(**dark_chart_layout(
        vertical="energy",
        height=280,
        title=dict(text="SAIDI by Territory (minutes)",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Minutes", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    panel_reliability = dcc.Graph(figure=fig_rel, config=CHART_CONFIG,
                                   style={"height": "280px"})

    # --- Insight card -------------------------------------------------------
    grid_insight = insight_card(
        headline="Southeast Heat Dome: Load Forecast Risk",
        metric_value="92.1 GW",
        direction="up",
        narrative=(
            "Peak demand forecast for Friday approaches 92.1 GW, within 2% of "
            "all-time record. Southeast territory driving the surge with "
            "temperatures 8-12F above seasonal normal. Battery storage "
            "resources at 94% state of charge — recommend pre-positioning "
            "demand response dispatch for Thursday evening and coordinating "
            "with neighboring balancing authorities for emergency interchange."
        ),
        action_text="Pre-stage demand response and mutual aid for Southeast",
        severity="warning",
        sparkline_values=[62, 64, 65, 68, 71, 74, 78, 82, 86, 92],
    )

    panels = [
        ("Generation Mix", panel_donut, "/energy/generation"),
        ("Reliability by Territory", panel_reliability, "/energy/grid_ops"),
        ("AI Insight", grid_insight),
    ]

    return layout_executive(
        title="Grid Command Center",
        subtitle="Real-time grid operations, generation dispatch, and reliability overview",
        heroes=heroes,
        briefing=briefing,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. GENERATION & DISPATCH  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_generation(cfg):
    """Generation dispatch view with tabs for dispatch, fuel mix, and capacity."""

    banner_text = (
        "Generation fleet operating at 61% capacity factor. Natural gas units "
        "dispatched at elevated levels due to Southeast heat event. Solar "
        "curtailment reached 420 MW today — interconnection queue backlog "
        "limiting additional renewable integration. Nuclear fleet at 98.2% "
        "availability. Battery storage cycled 1.4 times today, above the "
        "1.0 cycle target, indicating grid stress."
    )

    # ── TAB 1: Dispatch Stack ─────────────────────────────────────────
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    gas_monthly   = [38, 35, 32, 28, 30, 36, 42, 44, 40, 34, 32, 36]
    nuclear_month = [21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21]
    wind_monthly  = [14, 15, 16, 18, 17, 14, 12, 11, 13, 16, 17, 15]
    solar_monthly = [6, 8, 10, 13, 15, 16, 17, 16, 14, 10, 7, 5]
    hydro_monthly = [8, 9, 10, 9, 8, 7, 6, 5, 6, 7, 8, 9]

    fig_stack = go.Figure()
    for name, vals, color in [
        ("Natural Gas", gas_monthly, "#F97316"),
        ("Nuclear", nuclear_month, "#A78BFA"),
        ("Wind", wind_monthly, "#60A5FA"),
        ("Solar", solar_monthly, "#FBBF24"),
        ("Hydro", hydro_monthly, "#34D399"),
    ]:
        fig_stack.add_trace(go.Bar(
            x=months, y=vals, name=name, marker_color=color,
        ))
    fig_stack.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="Generation (GW)", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    dispatch_left = dcc.Graph(figure=fig_stack, config=CHART_CONFIG,
                               style={"height": "300px"})

    donut_dispatch = donut_figure(
        labels=["Natural Gas", "Nuclear", "Wind", "Solar", "Hydro", "Battery"],
        values=[34, 21, 18, 14, 8, 5],
        colors=["#F97316", "#A78BFA", "#60A5FA", "#FBBF24", "#34D399",
                COLORS["green"]],
        center_text="112.8 GW",
        title="Installed Capacity",
    )
    dispatch_right = dcc.Graph(figure=donut_dispatch, config=CHART_CONFIG,
                                style={"height": "300px"})

    tab_dispatch = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Monthly Generation by Fuel (GW)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), dispatch_left], padding="20px"),
            _card([html.Div("Installed Capacity Mix", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), dispatch_right], padding="20px"),
        ],
    )

    # ── TAB 2: Fuel Mix Trends ─────────────────────────────────────────
    years = ["2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"]
    fig_fuel = go.Figure()
    fig_fuel.add_trace(go.Scatter(
        x=years, y=[48, 44, 41, 38, 36, 34, 32, 30],
        name="Natural Gas %", mode="lines+markers",
        line=dict(color="#F97316", width=2), marker=dict(size=5),
    ))
    fig_fuel.add_trace(go.Scatter(
        x=years, y=[8, 10, 14, 18, 22, 26, 29, 31],
        name="Renewables %", mode="lines+markers",
        line=dict(color=COLORS["green"], width=2), marker=dict(size=5),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['green'])}, 0.08)",
    ))
    fig_fuel.add_trace(go.Scatter(
        x=years, y=[22, 22, 21, 21, 21, 21, 21, 21],
        name="Nuclear %", mode="lines+markers",
        line=dict(color="#A78BFA", width=2), marker=dict(size=5),
    ))
    fig_fuel.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Share (%)", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    fuel_left = dcc.Graph(figure=fig_fuel, config=CHART_CONFIG,
                           style={"height": "300px"})

    # Heat rate efficiency
    fig_heat = go.Figure()
    fig_heat.add_trace(go.Bar(
        x=["Combined Cycle", "Combustion Turbine", "Steam", "Fleet Avg"],
        y=[6800, 9200, 10400, 7420],
        marker_color=[COLORS["green"], COLORS["yellow"], COLORS["red"],
                      COLORS["blue"]],
        text=["6,800", "9,200", "10,400", "7,420"],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_heat.add_hline(y=7000, line_dash="dash", line_color=COLORS["green"],
                        opacity=0.6, annotation_text="Best-in-class (7,000)",
                        annotation_font_color=COLORS["green"],
                        annotation_font_size=10)
    fig_heat.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="BTU/kWh", showgrid=True, gridcolor=COLORS["border"],
                   range=[0, 12000]),
        margin=dict(l=48, r=24, t=24, b=48),
    ))
    fuel_right = dcc.Graph(figure=fig_heat, config=CHART_CONFIG,
                            style={"height": "300px"})

    tab_fuel = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Fuel Mix Evolution (8 Years)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), fuel_left], padding="20px"),
            _card([html.Div("Heat Rate by Plant Type (BTU/kWh)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), fuel_right], padding="20px"),
        ],
    )

    # ── TAB 3: Capacity & Availability ─────────────────────────────────
    cap_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Available", "Planned Outage", "Forced Outage", "Derated"],
            values=[82, 8, 6, 4],
            colors=[COLORS["green"], COLORS["blue"], COLORS["red"],
                    COLORS["yellow"]],
            center_text="82%",
            title="Fleet Availability",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    qtrs = ["Q1 '25", "Q2 '25", "Q3 '25", "Q4 '25", "Q1 '26"]
    fig_cap = go.Figure()
    fig_cap.add_trace(go.Bar(
        x=qtrs, y=[112.8, 114.2, 115.6, 116.4, 118.2], name="Total Capacity (GW)",
        marker_color=COLORS["blue"],
    ))
    fig_cap.add_trace(go.Scatter(
        x=qtrs, y=[60, 61, 62, 60, 61], name="Capacity Factor %",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_cap.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Capacity (GW)", showgrid=True,
                   gridcolor=COLORS["border"]),
        yaxis2=dict(title="Capacity Factor %", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"], range=[40, 80]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    cap_right = dcc.Graph(figure=fig_cap, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_capacity = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Fleet Availability Breakdown", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), cap_donut], padding="20px"),
            _card([html.Div("Capacity Growth & Factor Trend", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), cap_right], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Capacity", "112.8 GW", "blue"),
        ("Capacity Factor", "61%", "green"),
        ("Heat Rate", "7,420 BTU/kWh", "yellow"),
        ("Curtailment", "420 MW", "red"),
        ("Battery Cycles", "1.4/day", "purple"),
    ]

    insight = insight_card(
        headline="Solar Curtailment Trending Up",
        metric_value="420 MW",
        direction="up",
        narrative=(
            "Solar curtailment reached 420 MW today, a 35% increase from last "
            "month. Interconnection queue backlog of 8.2 GW limits new "
            "transmission capacity. Battery storage co-location could absorb "
            "~60% of curtailed energy."
        ),
        severity="warning",
    )

    return layout_split(
        title="Generation & Dispatch",
        subtitle="Fleet performance, fuel mix, and dispatch economics",
        tab_contents=[
            ("Dispatch", tab_dispatch),
            ("Fuel Mix", tab_fuel),
            ("Capacity", tab_capacity),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. GRID OPERATIONS  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_grid_ops(cfg):
    """Grid operations table with outage data, filters, and reliability KPIs."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Territory", "options": ["All Territories", "Northeast Grid", "Southeast Grid", "Midwest Grid", "Western Interconnect", "Texas ERCOT"]},
        {"label": "Outage Cause", "options": ["All Causes", "Vegetation", "Equipment Failure", "Weather", "Animal Contact", "Planned"]},
        {"label": "Status", "options": ["All Statuses", "Active", "Crew Dispatched", "Restored", "Under Investigation"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "SAIDI", "value": "108.2 min", "accent": "red"},
        {"label": "SAIFI", "value": "1.04", "accent": "yellow"},
        {"label": "CAIDI", "value": "104.0 min", "accent": "yellow"},
        {"label": "T&D Loss", "value": "5.4%", "accent": "blue"},
        {"label": "Active Outages", "value": "23", "accent": "red"},
    ]

    # --- Table data ---------------------------------------------------------
    columns = [
        {"name": "Circuit", "id": "circuit"},
        {"name": "Territory", "id": "territory"},
        {"name": "Cause", "id": "cause"},
        {"name": "Customers Out", "id": "customers_out"},
        {"name": "Duration", "id": "duration"},
        {"name": "CMI", "id": "cmi"},
        {"name": "Crew Status", "id": "crew_status"},
        {"name": "Status", "id": "status"},
    ]

    outages = [
        ("NE-2247", "Northeast Grid", "Vegetation", "4,218", "3h 42m",
         "938,916", "En Route", "Active"),
        ("SE-1104", "Southeast Grid", "Equipment Failure", "2,840",
         "1h 18m", "221,520", "On Site", "Active"),
        ("NE-3391", "Northeast Grid", "Vegetation", "1,926", "5h 06m",
         "590,316", "On Site", "Active"),
        ("MW-0782", "Midwest Grid", "Weather — Ice", "6,412", "8h 22m",
         "3,222,264", "Multiple Crews", "Active"),
        ("TX-4401", "Texas ERCOT", "Animal Contact", "842", "0h 48m",
         "40,416", "Dispatched", "Crew Dispatched"),
        ("WI-0215", "Western Interconnect", "Equipment Failure", "1,284",
         "2h 14m", "172,056", "On Site", "Active"),
        ("SE-2208", "Southeast Grid", "Planned", "3,100", "4h 00m",
         "744,000", "Scheduled", "Planned"),
        ("MW-1456", "Midwest Grid", "Vegetation", "2,108", "1h 52m",
         "236,096", "En Route", "Active"),
        ("NE-0891", "Northeast Grid", "Equipment Failure", "956", "0h 34m",
         "32,504", "Dispatched", "Crew Dispatched"),
        ("TX-3317", "Texas ERCOT", "Weather — Wind", "5,624", "6h 48m",
         "2,294,592", "Multiple Crews", "Active"),
    ]

    data = []
    for circuit, territory, cause, cust, duration, cmi, crew, status in outages:
        data.append({
            "circuit": circuit,
            "territory": territory,
            "cause": cause,
            "customers_out": cust,
            "duration": duration,
            "cmi": cmi,
            "crew_status": crew,
            "status": status,
        })

    insight = insight_card(
        headline="Vegetation Outages Driving SAIDI Overage",
        metric_value="108.2 min",
        direction="up",
        narrative=(
            "SAIDI at 108.2 minutes, 8% above the 100-minute regulatory "
            "target. Three vegetation-related outages on aging feeders in the "
            "Northeast territory account for 62% of total customer minutes "
            "interrupted this month. Predictive vegetation risk model flags "
            "14 additional circuits for accelerated trimming."
        ),
        action_text="Accelerate vegetation management on flagged NE circuits",
        severity="critical",
    )

    return layout_table(
        title="Grid Operations",
        subtitle="Real-time outage management, reliability metrics, and crew dispatch",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. CUSTOMER PROGRAMS  (Style E — layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_customer_programs(cfg):
    """Customer programs with demand response forecast, KPIs, and breakdown."""

    # --- KPIs ---------------------------------------------------------------
    kpis = [
        {"label": "DR Enrolled", "value": "3,850 MW", "accent": "green"},
        {"label": "Avg Residential Bill", "value": "$142.30", "accent": "blue"},
        {"label": "EV Stations", "value": "12,480", "accent": "purple"},
        {"label": "Net Metering Customers", "value": "284K", "accent": "green"},
    ]

    # --- Main chart: demand response dispatch and enrollment trends ----------
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    import math
    dr_dispatched = [2800, 3200, 2400, 1200, 800, 600,
                     500, 600, 1000, 1800, 2600, 3400]
    dr_enrolled   = [3200, 3300, 3400, 3450, 3500, 3550,
                     3600, 3650, 3700, 3750, 3800, 3850]

    fig_dr = go.Figure()
    fig_dr.add_trace(go.Bar(
        x=months, y=dr_dispatched, name="DR Dispatched (MW)",
        marker_color=COLORS["blue"], opacity=0.8,
    ))
    fig_dr.add_trace(go.Scatter(
        x=months, y=dr_enrolled, name="DR Enrolled (MW)",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_dr.update_layout(**dark_chart_layout(
        height=300,
        xaxis=dict(title="Month", showgrid=False),
        yaxis=dict(title="Dispatched MW", showgrid=True,
                   gridcolor=COLORS["border"], side="left"),
        yaxis2=dict(title="Enrolled MW", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
        barmode="overlay",
    ))
    main_chart = dcc.Graph(figure=fig_dr, config=CHART_CONFIG,
                           style={"height": "300px"})

    # --- Side breakdown by customer class -----------------------------------
    side = breakdown_list([
        {"label": "Residential", "value": "1,840 MW", "pct": 48, "color": COLORS["blue"]},
        {"label": "Commercial", "value": "920 MW", "pct": 24, "color": COLORS["green"]},
        {"label": "Industrial", "value": "690 MW", "pct": 18, "color": "#A78BFA"},
        {"label": "Municipal", "value": "280 MW", "pct": 7, "color": "#FBBF24"},
        {"label": "EV Charging", "value": "120 MW", "pct": 3, "color": "#F97316"},
    ])

    insight = insight_card(
        headline="EV Charging Demand Growing Faster Than Grid Capacity",
        metric_value="+34%",
        direction="up",
        narrative=(
            "EV charging station count grew 34% year-over-year to 12,480 "
            "stations. Peak EV charging load now reaches 1.2 GW during "
            "evening hours, coinciding with residential peak. Time-of-use "
            "rate adoption among EV owners at only 28% — managed charging "
            "programs could shift 400 MW to off-peak if enrollment doubles."
        ),
        action_text="Expand managed EV charging incentives and TOU enrollment",
        severity="warning",
    )

    return layout_forecast(
        title="Customer Programs",
        subtitle="Demand response, energy efficiency, and customer engagement analytics",
        kpi_items=kpis,
        hero_value="3,850 MW",
        hero_label="Total Demand Response Enrolled Capacity",
        hero_trend_text="+8.2% vs prior year",
        main_chart=main_chart,
        side_component=html.Div([
            html.Div("DR Enrollment by Customer Class",
                     style={"fontSize": "14px", "fontWeight": "600",
                            "color": COLORS["white"], "marginBottom": "16px"}),
            side,
        ]),
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. ASSET HEALTH & RELIABILITY  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_asset_health(cfg):
    """Asset condition table with health indices, filters, and KPIs."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Asset Type", "options": ["All Types", "Transmission Lines", "Substations", "Distribution Feeders", "Smart Meters", "DER Inverters"]},
        {"label": "Condition", "options": ["All Conditions", "Good", "Fair", "Poor", "Very Poor", "End of Life"]},
        {"label": "Territory", "options": ["All Territories", "Northeast Grid", "Southeast Grid", "Midwest Grid", "Western Interconnect", "Texas ERCOT"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Assets Monitored", "value": "48,240", "accent": "blue"},
        {"label": "Avg Health Index", "value": "72.4", "accent": "green"},
        {"label": "Critical Alerts", "value": "18", "accent": "red"},
        {"label": "Predictive Alerts", "value": "42", "accent": "yellow"},
        {"label": "Maint. Backlog", "value": "$284M", "accent": "purple"},
    ]

    # --- Table data ---------------------------------------------------------
    columns = [
        {"name": "Asset ID", "id": "asset_id"},
        {"name": "Type", "id": "type"},
        {"name": "Territory", "id": "territory"},
        {"name": "Age (yrs)", "id": "age"},
        {"name": "Health Index", "id": "health_index"},
        {"name": "Last Inspection", "id": "last_inspection"},
        {"name": "Risk Score", "id": "risk_score"},
        {"name": "Condition", "id": "condition"},
    ]

    assets = [
        ("SUB-NE-042", "Substation", "Northeast Grid", "38", "42",
         "2025-08-14", "High", "Poor"),
        ("TL-SE-118", "Transmission Line", "Southeast Grid", "22", "78",
         "2025-11-02", "Low", "Good"),
        ("DF-MW-2247", "Distribution Feeder", "Midwest Grid", "31", "56",
         "2025-06-28", "Medium", "Fair"),
        ("SM-TX-88412", "Smart Meter", "Texas ERCOT", "8", "91",
         "2026-01-15", "Low", "Good"),
        ("SUB-WI-017", "Substation", "Western Interconnect", "45", "28",
         "2025-04-22", "Critical", "Very Poor"),
        ("DER-NE-3341", "DER Inverter", "Northeast Grid", "4", "88",
         "2026-02-10", "Low", "Good"),
        ("TL-MW-204", "Transmission Line", "Midwest Grid", "42", "34",
         "2025-07-18", "High", "Poor"),
        ("DF-SE-1891", "Distribution Feeder", "Southeast Grid", "28", "62",
         "2025-09-05", "Medium", "Fair"),
        ("SUB-TX-091", "Substation", "Texas ERCOT", "18", "82",
         "2025-12-20", "Low", "Good"),
        ("TL-NE-067", "Transmission Line", "Northeast Grid", "52", "18",
         "2025-03-12", "Critical", "End of Life"),
    ]

    data = []
    for aid, atype, territory, age, hi, insp, risk, condition in assets:
        data.append({
            "asset_id": aid,
            "type": atype,
            "territory": territory,
            "age": age,
            "health_index": hi,
            "last_inspection": insp,
            "risk_score": risk,
            "condition": condition,
        })

    insight = insight_card(
        headline="Aging Substations Driving Reliability Risk",
        metric_value="14%",
        direction="up",
        narrative=(
            "14% of substations (62 units) now rated Poor or Very Poor "
            "condition, up from 11% last year. Average substation age in the "
            "Northeast is 38 years — well above the 30-year design life. "
            "Predictive model flags 8 transformers with dissolved gas analysis "
            "anomalies indicating accelerated degradation. Estimated $284M "
            "capital investment needed to address backlog."
        ),
        action_text="Prioritize capital plan for NE and MW substation replacements",
        severity="critical",
    )

    return layout_table(
        title="Asset Health & Reliability",
        subtitle="Infrastructure condition monitoring, predictive maintenance, and capital planning",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. ENERGY TRANSITION  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_energy_transition(cfg):
    """Energy transition view with tabs for renewables, carbon, and DER."""

    banner_text = (
        "Renewable penetration at 31.2% against a 50% target by 2030. Carbon "
        "intensity declined 18% over 5 years to 720 lbs/MWh. DER "
        "interconnection queue stands at 8.2 GW, with average wait time of "
        "22 months. Methane leak rate at 0.24%, below the 0.5% EPA threshold "
        "but above the voluntary 0.1% industry pledge. Wildfire risk elevated "
        "on 3,800 miles of transmission corridors in the Western Interconnect."
    )

    # ── TAB 1: Renewables Progress ─────────────────────────────────────
    years = ["2020", "2021", "2022", "2023", "2024", "2025", "2026",
             "2027", "2028", "2029", "2030"]
    actual_re  = [12, 16, 20, 24, 28, 31, None, None, None, None, None]
    target_re  = [12, 16, 20, 25, 30, 35, 38, 41, 44, 47, 50]
    projected  = [None, None, None, None, None, 31, 34, 37, 40, 43, 46]

    fig_re = go.Figure()
    fig_re.add_trace(go.Scatter(
        x=years, y=target_re, name="Regulatory Target",
        line=dict(color=COLORS["red"], width=2, dash="dash"),
        mode="lines", connectgaps=True,
    ))
    fig_re.add_trace(go.Scatter(
        x=years, y=actual_re, name="Actual",
        line=dict(color=COLORS["green"], width=3),
        mode="lines+markers", marker=dict(size=7), connectgaps=False,
    ))
    fig_re.add_trace(go.Scatter(
        x=years, y=projected, name="Projected (Current Pace)",
        line=dict(color=COLORS["yellow"], width=2, dash="dot"),
        mode="lines+markers", marker=dict(size=5), connectgaps=True,
    ))
    fig_re.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Renewable %", showgrid=True,
                   gridcolor=COLORS["border"], range=[0, 55]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    re_left = dcc.Graph(figure=fig_re, config=CHART_CONFIG,
                         style={"height": "300px"})

    # Renewable capacity additions by type
    fig_adds = go.Figure()
    add_years = ["2022", "2023", "2024", "2025", "2026"]
    fig_adds.add_trace(go.Bar(
        x=add_years, y=[1200, 1800, 2400, 3100, 3600], name="Solar MW",
        marker_color="#FBBF24",
    ))
    fig_adds.add_trace(go.Bar(
        x=add_years, y=[800, 1200, 1400, 1800, 2200], name="Wind MW",
        marker_color="#60A5FA",
    ))
    fig_adds.add_trace(go.Bar(
        x=add_years, y=[200, 400, 800, 1400, 2000], name="Battery MW",
        marker_color=COLORS["green"],
    ))
    fig_adds.update_layout(**dark_chart_layout(
        height=300, barmode="group",
        yaxis=dict(title="MW Added", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    re_right = dcc.Graph(figure=fig_adds, config=CHART_CONFIG,
                          style={"height": "300px"})

    tab_renewables = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Renewable Penetration Trajectory", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), re_left], padding="20px"),
            _card([html.Div("Annual Capacity Additions (MW)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), re_right], padding="20px"),
        ],
    )

    # ── TAB 2: Carbon Reduction ─────────────────────────────────────────
    carbon_years = ["2018", "2019", "2020", "2021", "2022", "2023",
                    "2024", "2025", "2026"]
    fig_carbon = go.Figure()
    fig_carbon.add_trace(go.Scatter(
        x=carbon_years, y=[920, 880, 840, 810, 780, 760, 740, 720, 700],
        name="Carbon Intensity (lbs/MWh)", mode="lines+markers",
        line=dict(color=COLORS["blue"], width=3), marker=dict(size=6),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_carbon.add_hline(y=500, line_dash="dash", line_color=COLORS["green"],
                          opacity=0.6, annotation_text="2035 Target (500)",
                          annotation_font_color=COLORS["green"],
                          annotation_font_size=10)
    fig_carbon.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="lbs CO2/MWh", showgrid=True,
                   gridcolor=COLORS["border"], range=[0, 1000]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    carbon_left = dcc.Graph(figure=fig_carbon, config=CHART_CONFIG,
                             style={"height": "300px"})

    # Emissions by source
    emissions_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Natural Gas", "Methane Leaks", "Fleet Vehicles",
                    "SF6 Losses", "Other"],
            values=[72, 12, 8, 5, 3],
            colors=["#F97316", COLORS["red"], COLORS["yellow"],
                    "#A78BFA", COLORS["text_muted"]],
            center_text="720",
            title="Emission Sources",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    tab_carbon = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Carbon Intensity Trend (lbs/MWh)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), carbon_left], padding="20px"),
            _card([html.Div("Emission Source Breakdown", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), emissions_donut], padding="20px"),
        ],
    )

    # ── TAB 3: DER & Interconnection ────────────────────────────────────
    der_months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    fig_der = go.Figure()
    fig_der.add_trace(go.Bar(
        x=der_months, y=[320, 340, 380, 420, 480, 520, 560, 540, 500, 460,
                         400, 360],
        name="DER Applications",
        marker_color=COLORS["blue"], opacity=0.8,
    ))
    fig_der.add_trace(go.Bar(
        x=der_months, y=[180, 200, 220, 260, 300, 340, 380, 360, 320, 280,
                         240, 200],
        name="Completed Interconnections",
        marker_color=COLORS["green"], opacity=0.8,
    ))
    fig_der.update_layout(**dark_chart_layout(
        height=300, barmode="group",
        yaxis=dict(title="Count", showgrid=True,
                   gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    der_left = dcc.Graph(figure=fig_der, config=CHART_CONFIG,
                          style={"height": "300px"})

    # Queue backlog
    fig_queue = go.Figure()
    q_years = ["2021", "2022", "2023", "2024", "2025", "2026"]
    fig_queue.add_trace(go.Bar(
        x=q_years, y=[2.1, 3.4, 4.8, 6.2, 7.4, 8.2],
        name="Queue Backlog (GW)",
        marker_color=COLORS["red"],
        text=["2.1", "3.4", "4.8", "6.2", "7.4", "8.2"],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_queue.add_trace(go.Scatter(
        x=q_years, y=[14, 16, 18, 20, 21, 22],
        name="Avg Wait (months)", mode="lines+markers",
        line=dict(color=COLORS["yellow"], width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_queue.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="GW in Queue", showgrid=True,
                   gridcolor=COLORS["border"]),
        yaxis2=dict(title="Avg Wait (months)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["yellow"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    der_right = dcc.Graph(figure=fig_queue, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_der = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("DER Applications vs Completions", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), der_left], padding="20px"),
            _card([html.Div("Interconnection Queue Backlog", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), der_right], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Renewable %", "31.2%", "green"),
        ("Carbon Intensity", "720 lbs/MWh", "blue"),
        ("DER Queue", "8.2 GW", "red"),
        ("Methane Leak Rate", "0.24%", "yellow"),
        ("Wildfire Risk Miles", "3,800", "red"),
    ]

    insight = insight_card(
        headline="Renewable Target Gap Widening",
        metric_value="4 pts",
        direction="up",
        narrative=(
            "At current pace, projected to reach 46% renewable by 2030 vs "
            "the 50% regulatory mandate — a 4-point gap. Interconnection "
            "queue backlog of 8.2 GW is the primary bottleneck. Accelerating "
            "battery storage co-location and transmission upgrades could "
            "close the gap by 2029."
        ),
        severity="warning",
    )

    return layout_split(
        title="Energy Transition",
        subtitle="Renewable progress, decarbonization trajectory, and DER integration",
        tab_contents=[
            ("Renewables", tab_renewables),
            ("Carbon", tab_carbon),
            ("DER", tab_der),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. REGULATORY & RATES  (Style F — layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_regulatory(cfg):
    """Regulatory and rates grid with gauges, sparklines, and rate metrics."""

    # --- Grid item 1: ROE gauge (tall, spans 2 rows) -------------------------
    gauge_roe = gauge_figure(
        value=9.4, max_val=12, title="Earned ROE (%)",
        color=COLORS["yellow"],
    )
    roe_panel = html.Div([
        dcc.Graph(figure=gauge_roe, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "12px"},
            children=[
                html.Div("9.4%", style={
                    "fontSize": "18px", "fontWeight": "700",
                    "color": COLORS["yellow"], "letterSpacing": "1px",
                }),
                html.Div("Authorized: 10.2%", style={
                    "fontSize": "12px", "color": COLORS["text_muted"],
                    "marginTop": "4px",
                }),
            ],
        ),
        html.Div(
            style={"marginTop": "20px"},
            children=[
                progress_row("Regulatory Lag", "14.2 months", 71, COLORS["red"]),
                progress_row("Rate Case Status", "Filed", 45, COLORS["yellow"]),
                progress_row("PBR Compliance", "92%", 92, COLORS["green"]),
            ],
        ),
    ])

    # --- Grid item 2: Revenue per MWh sparkline ------------------------------
    rev_data = [64.2, 65.1, 65.8, 66.4, 67.0, 67.3, 67.8, 68.0, 68.2,
                68.4, 68.1, 67.9, 68.2, 68.4, 68.6, 68.8, 69.0, 68.4,
                68.2, 68.4]
    rev_panel = metric_with_sparkline(
        "Revenue per MWh", "$68.40",
        rev_data, accent="green",
    )

    # --- Grid item 3: O&M Cost sparkline ------------------------------------
    opex_data = [268, 272, 275, 278, 280, 282, 284, 286, 284, 282,
                 280, 278, 280, 282, 284, 286, 288, 286, 284, 284]
    opex_panel = metric_with_sparkline(
        "O&M per Customer", "$284.50",
        opex_data, accent="yellow",
    )

    # --- Grid item 4: Rate comparison bar (wide, spans 2 cols) ---------------
    fig_rates = go.Figure()
    rate_categories = ["Residential", "Commercial", "Industrial",
                       "Regional Avg", "National Median"]
    rate_values = [14.2, 12.8, 8.4, 13.6, 12.1]
    rate_colors = [COLORS["blue"], COLORS["blue"], COLORS["blue"],
                   COLORS["yellow"], COLORS["text_muted"]]

    fig_rates.add_trace(go.Bar(
        x=rate_categories, y=rate_values,
        marker_color=rate_colors,
        text=[f"${v:.1f}" for v in rate_values],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_rates.update_layout(**dark_chart_layout(
        height=200, margin=dict(l=40, r=20, t=36, b=36),
        title=dict(text="Rates (cents/kWh) vs Benchmarks",
                   font=dict(size=13, color=COLORS["white"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   title="cents/kWh"),
    ))
    rates_panel = html.Div([
        dcc.Graph(figure=fig_rates, config=CHART_CONFIG,
                  style={"height": "200px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "marginTop": "8px", "padding": "0 4px"},
            children=[
                html.Div([
                    html.Span("Avg Bill: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("$142.30", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["white"]}),
                ]),
                html.Div([
                    html.Span("YoY Change: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("+3.2%", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["yellow"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 5: CapEx sparkline ----------------------------------------
    capex_data = [3.2, 3.4, 3.6, 3.8, 3.9, 4.0, 4.1, 4.0, 4.1, 4.2,
                  4.1, 4.0, 4.1, 4.2, 4.2, 4.3, 4.2, 4.1, 4.2, 4.2]
    capex_panel = metric_with_sparkline(
        "Annual CapEx", "$4.2B",
        capex_data, accent="blue",
    )

    # --- Grid item 6: Rate Base gauge ----------------------------------------
    gauge_rb = gauge_figure(
        value=28.5, max_val=40, title="Rate Base ($B)",
        color=COLORS["blue"],
    )
    rb_panel = html.Div([
        dcc.Graph(figure=gauge_rb, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-around",
                   "marginTop": "8px"},
            children=[
                html.Div([
                    html.Div("Growth", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("+6.2% YoY", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Div("ROA", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("3.8%", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["yellow"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 7: Regulatory Lag gauge -----------------------------------
    gauge_lag = gauge_figure(
        value=14.2, max_val=24, title="Regulatory Lag (months)",
        color=COLORS["red"],
    )
    lag_panel = html.Div([
        dcc.Graph(figure=gauge_lag, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"marginTop": "12px"},
            children=[
                progress_row("Rate Case Filed", "Active", 45, COLORS["yellow"]),
                progress_row("Fuel Adj. Clause", "Current", 95, COLORS["green"]),
                progress_row("Storm Recovery", "Pending", 30, COLORS["red"]),
                progress_row("Rider Mechanisms", "4 Active", 80, COLORS["green"]),
            ],
        ),
    ])

    # --- Assemble grid -------------------------------------------------------
    grid_items = [
        {"col_span": 1, "row_span": 2, "content": roe_panel},
        {"col_span": 1, "row_span": 1, "content": rev_panel},
        {"col_span": 1, "row_span": 1, "content": opex_panel},
        {"col_span": 2, "row_span": 1, "content": rates_panel},
        {"col_span": 1, "row_span": 1, "content": capex_panel},
        {"col_span": 1, "row_span": 1, "content": rb_panel},
        {"col_span": 1, "row_span": 1, "content": lag_panel},
    ]

    insight = insight_card(
        headline="Regulatory Lag Eroding Earned Returns",
        metric_value="80 bps",
        direction="down",
        narrative=(
            "Earned ROE of 9.4% lags the 10.2% authorized return by 80 basis "
            "points, driven by 14.2 months of regulatory lag. Rate case filed "
            "in Q4 2025 with $1.8B test-year rate base increase request. "
            "Performance-based ratemaking mechanisms recovering 62% of the "
            "lag, but accelerating capital deployment outpaces rider recovery."
        ),
        action_text="Expedite interim rate relief filing and rider expansion",
        severity="warning",
    )

    return layout_grid(
        title="Regulatory & Rates",
        subtitle="Rate case status, earned returns, and regulatory compliance",
        grid_items=grid_items,
        insight=insight,
    )
