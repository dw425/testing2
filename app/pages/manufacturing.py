"""Manufacturing vertical page renderers for Blueprint IQ.

Provides seven page-level rendering functions for the Manufacturing
vertical: Dashboard, Production Analytics, Quality Control, Supply Chain,
Predictive Maintenance, Energy & Sustainability, and Workforce Ops.

Each public ``render_*`` function accepts a ``cfg`` dict and returns an
``html.Div`` that can be dropped into the main content area.
"""

from app.page_styles import (
    dark_chart_layout, CHART_CONFIG, ACCENT_ICONS,
    page_header, hero_metric, compact_kpi, kpi_strip, filter_bar,
    tab_bar, info_banner, alert_card, progress_row, stat_card,
    rich_table, td, status_td, progress_td, breakdown_list,
    trend_indicator, use_case_badges, donut_figure,
    layout_executive, layout_table, layout_split, layout_alerts,
    layout_forecast, layout_grid,
    gauge_figure, sparkline_figure, metric_with_sparkline,
    insight_card, morning_briefing,
    _card, _hex_to_rgb,
)
from app.theme import COLORS, FONT_FAMILY, get_vertical_theme
from dash import dcc, html
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
#  1. render_dashboard  —  Style A (layout_executive)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive manufacturing dashboard with hero KPIs and production trends."""

    # ── Morning briefing ───────────────────────────────────────────────────
    briefing = morning_briefing(
        title="Manufacturing Morning Briefing",
        summary_text=(
            "OEE remains our North Star metric at 87.4%, up 2.1% month-over-month. "
            "Margin protection is holding steady with net margin per asset at $12.4K. "
            "Equipment availability across all four facilities is at 97.2%, "
            "though Berlin CNC-A1 spindle shows early wear signatures. "
            "Focus today: sustain OEE gains while monitoring Tokyo welding line quality drift."
        ),
        signals=[
            {"label": "OEE (North Star)", "status": "green",
             "detail": "87.4% — above 85% target, trending up"},
            {"label": "Margin Protection", "status": "green",
             "detail": "Net margin per asset $12.4K — within plan"},
            {"label": "Equipment Availability", "status": "amber",
             "detail": "97.2% overall — CNC-A1 spindle flagged for inspection"},
        ],
    )

    # ── Hero metrics ──────────────────────────────────────────────────────
    heroes = [
        hero_metric(
            "Overall Equipment Effectiveness",
            "87.4%",
            trend_text="+2.1% vs last month",
            trend_dir="up",
            accent="blue",
            href="/manufacturing/production_analytics",
        ),
        hero_metric(
            "Net Margin per Asset",
            "$12.4K",
            trend_text="+$0.8K vs last quarter",
            trend_dir="up",
            accent="green",
            href="/manufacturing/production_analytics",
        ),
        hero_metric(
            "First Pass Yield",
            "94.8%",
            trend_text="+0.6% vs last month",
            trend_dir="up",
            accent="green",
            href="/manufacturing/quality_control",
        ),
        hero_metric(
            "Production Rate",
            "1,247 u/hr",
            trend_text="-3.2% vs last month",
            trend_dir="down",
            accent="purple",
            href="/manufacturing/production_analytics",
        ),
    ]

    # ── Main chart: production output trend ───────────────────────────────
    months = ["Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"]
    berlin = [4200, 4350, 4500, 4100, 4600, 4750, 4900, 5050, 5200, 5100, 5350, 5500]
    detroit = [3800, 3950, 4100, 3900, 4200, 4350, 4450, 4600, 4700, 4550, 4800, 4950]
    tokyo = [3200, 3400, 3550, 3300, 3600, 3750, 3900, 4050, 4100, 3950, 4200, 4350]
    shanghai = [5100, 5300, 5450, 5200, 5600, 5800, 5950, 6100, 6250, 6100, 6400, 6600]

    fig_main = go.Figure()
    fig_main.add_trace(go.Scatter(
        x=months, y=berlin, name="Berlin",
        mode="lines+markers", line=dict(color=COLORS["blue"], width=2),
        marker=dict(size=5),
    ))
    fig_main.add_trace(go.Scatter(
        x=months, y=detroit, name="Detroit",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=5),
    ))
    fig_main.add_trace(go.Scatter(
        x=months, y=tokyo, name="Tokyo",
        mode="lines+markers", line=dict(color=COLORS["purple"], width=2),
        marker=dict(size=5),
    ))
    fig_main.add_trace(go.Scatter(
        x=months, y=shanghai, name="Shanghai",
        mode="lines+markers", line=dict(color=COLORS["yellow"], width=2),
        marker=dict(size=5),
    ))
    fig_main.update_layout(**dark_chart_layout(
        vertical="manufacturing",
        height=340,
        title=dict(text="Production Output by Facility (units/day)",
                   font=dict(size=14, color=COLORS["white"]),
                   x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_main, config=CHART_CONFIG,
                           style={"height": "340px"})

    # ── Bottom panel 1: output by product line (donut) ────────────────────
    product_labels = ["Automotive Parts", "Electronics", "Heavy Machinery", "Consumer Goods"]
    product_values = [3400, 2800, 1900, 1300]
    product_colors = [COLORS["blue"], COLORS["green"], COLORS["purple"], COLORS["yellow"]]
    fig_donut = donut_figure(product_labels, product_values, product_colors,
                             center_text="9.4K", title="Output by Product Line")
    donut_chart = dcc.Graph(figure=fig_donut, config=CHART_CONFIG,
                            style={"height": "280px"})

    # ── Bottom panel 2: downtime trend chart ──────────────────────────────
    weeks = [f"W{w}" for w in range(1, 13)]
    planned = [4.2, 3.8, 5.1, 4.5, 3.9, 4.0, 3.6, 4.8, 3.5, 4.1, 3.3, 3.7]
    unplanned = [2.1, 2.8, 1.9, 3.2, 2.5, 1.8, 2.0, 1.5, 2.3, 1.7, 2.1, 1.4]

    fig_downtime = go.Figure()
    fig_downtime.add_trace(go.Bar(
        x=weeks, y=planned, name="Planned",
        marker_color=COLORS["blue"], opacity=0.85,
    ))
    fig_downtime.add_trace(go.Bar(
        x=weeks, y=unplanned, name="Unplanned",
        marker_color=COLORS["red"], opacity=0.85,
    ))
    fig_downtime.update_layout(**dark_chart_layout(
        vertical="manufacturing",
        height=280, barmode="stack",
        title=dict(text="Downtime Hours (weekly)",
                   font=dict(size=13, color=COLORS["white"]),
                   x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
        margin=dict(l=48, r=24, t=40, b=40),
    ))
    downtime_chart = dcc.Graph(figure=fig_downtime, config=CHART_CONFIG,
                               style={"height": "280px"})

    # ── Insight card ─────────────────────────────────────────────────────
    insight = insight_card(
        headline="Production Line Anomaly Detected",
        metric_value="-12% throughput",
        direction="down",
        narrative=(
            "Welding-C1 in Tokyo is showing a sustained throughput decline over "
            "the past 72 hours, coinciding with a 34% rise in surface finish defects. "
            "Supply chain data also flags a pending delay on replacement carbide inserts "
            "for CNC-A2 (Sandvik Coromant), which could cascade to Berlin production "
            "if not resolved within 5 days."
        ),
        action_text="Review Welding-C1 diagnostics",
        severity="warning",
    )

    # ── Assemble with layout_executive ────────────────────────────────────
    dashboard = layout_executive(
        title="Manufacturing Dashboard",
        subtitle="Real-time production performance across all facilities",
        heroes=heroes,
        main_chart=main_chart,
        panels=[
            ("Output by Product Line", donut_chart, "/manufacturing/production_analytics"),
            ("Downtime Trend", downtime_chart, "/manufacturing/predictive_maintenance"),
        ],
    )

    # Wrap with briefing and insight card at the top
    return html.Div([briefing, insight, dashboard])


# ═══════════════════════════════════════════════════════════════════════════
#  2. render_production_analytics  —  Style B (layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_production_analytics(cfg):
    """Production analytics with filters, KPIs, and a rich production-line table."""

    # ── Filters ───────────────────────────────────────────────────────────
    filters = [
        {"label": "Facility", "options": ["All Facilities", "Berlin", "Detroit", "Tokyo", "Shanghai"]},
        {"label": "Line", "options": ["All Lines", "CNC-A1", "CNC-A2", "Assembly-B1", "Assembly-B2"]},
        {"label": "Shift", "options": ["All Shifts", "Morning", "Afternoon", "Night"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────────
    kpi_items = [
        {"label": "Units Produced", "value": "42,850", "accent": "blue"},
        {"label": "Avg Cycle Time", "value": "3.2 min", "accent": "purple"},
        {"label": "Throughput", "value": "1,247 u/hr", "accent": "green"},
        {"label": "Utilization", "value": "87.4%", "accent": "blue"},
        {"label": "Scrap Rate", "value": "2.3%", "accent": "red"},
    ]

    # ── Production line table ─────────────────────────────────────────────
    columns = [
        {"name": "Line", "id": "line"},
        {"name": "Facility", "id": "facility"},
        {"name": "Shift", "id": "shift"},
        {"name": "Units", "id": "units"},
        {"name": "Cycle Time", "id": "cycle_time"},
        {"name": "Utilization", "id": "utilization"},
        {"name": "Status", "id": "status"},
    ]

    data = [
        {"line": "CNC-A1", "facility": "Berlin", "shift": "Morning", "units": "1,842", "cycle_time": "2.8 min", "utilization": "94%", "status": "Healthy"},
        {"line": "CNC-A2", "facility": "Berlin", "shift": "Morning", "units": "1,690", "cycle_time": "3.1 min", "utilization": "88%", "status": "Healthy"},
        {"line": "Assembly-B1", "facility": "Detroit", "shift": "Afternoon", "units": "2,105", "cycle_time": "2.4 min", "utilization": "92%", "status": "Healthy"},
        {"line": "Assembly-B2", "facility": "Detroit", "shift": "Afternoon", "units": "1,956", "cycle_time": "2.6 min", "utilization": "86%", "status": "Nominal"},
        {"line": "Welding-C1", "facility": "Tokyo", "shift": "Night", "units": "1,425", "cycle_time": "3.8 min", "utilization": "78%", "status": "Warning"},
        {"line": "Welding-C2", "facility": "Tokyo", "shift": "Night", "units": "1,380", "cycle_time": "4.0 min", "utilization": "72%", "status": "Warning"},
        {"line": "Stamping-D1", "facility": "Shanghai", "shift": "Morning", "units": "2,540", "cycle_time": "1.9 min", "utilization": "96%", "status": "Healthy"},
        {"line": "Stamping-D2", "facility": "Shanghai", "shift": "Morning", "units": "2,410", "cycle_time": "2.1 min", "utilization": "91%", "status": "Healthy"},
        {"line": "Paint-E1", "facility": "Berlin", "shift": "Afternoon", "units": "1,150", "cycle_time": "4.5 min", "utilization": "68%", "status": "Critical"},
        {"line": "Paint-E2", "facility": "Detroit", "shift": "Night", "units": "1,280", "cycle_time": "4.2 min", "utilization": "74%", "status": "Warning"},
        {"line": "Assembly-F1", "facility": "Shanghai", "shift": "Afternoon", "units": "2,320", "cycle_time": "2.3 min", "utilization": "93%", "status": "Healthy"},
        {"line": "CNC-G1", "facility": "Tokyo", "shift": "Morning", "units": "1,752", "cycle_time": "3.0 min", "utilization": "85%", "status": "Nominal"},
    ]

    insight = insight_card(
        headline="Line 7 Below OEE Target",
        metric_value="72.1%",
        direction="down",
        narrative="Line 7 OEE at 72.1% — below 85% target. Downtime analysis shows bearing wear as root cause.",
        severity="warning",
    )

    return layout_table(
        title="Production Analytics",
        subtitle="Line-level production performance and utilization metrics",
        filters=filters,
        kpi_items=kpi_items,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. render_quality_control  —  Style C (layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_quality_control(cfg):
    """Quality control view with defect analysis, SPC, and tolerance tabs."""

    banner_text = (
        "Quality insight: Welding-C1 line in Tokyo shows a 34% increase in "
        "surface finish defects over the past 2 weeks. Root cause analysis "
        "suggests worn tooling on station WC1-07. Recommend immediate inspection."
    )

    # ── TAB 1: Defects ───────────────────────────────────────────────────
    defect_lines = ["CNC-A1", "Assembly-B1", "Welding-C1", "Stamping-D1", "Paint-E1"]
    dimensional = [12, 8, 5, 18, 3]
    surface_finish = [6, 4, 22, 7, 15]
    structural = [3, 2, 8, 4, 2]
    cosmetic = [2, 5, 3, 2, 24]

    fig_defects = go.Figure()
    fig_defects.add_trace(go.Bar(
        y=defect_lines, x=dimensional, name="Dimensional",
        orientation="h", marker_color=COLORS["blue"],
    ))
    fig_defects.add_trace(go.Bar(
        y=defect_lines, x=surface_finish, name="Surface Finish",
        orientation="h", marker_color=COLORS["purple"],
    ))
    fig_defects.add_trace(go.Bar(
        y=defect_lines, x=structural, name="Structural",
        orientation="h", marker_color=COLORS["red"],
    ))
    fig_defects.add_trace(go.Bar(
        y=defect_lines, x=cosmetic, name="Cosmetic",
        orientation="h", marker_color=COLORS["yellow"],
    ))
    fig_defects.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
        margin=dict(l=100, r=24, t=24, b=48),
        xaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   title=dict(text="Defect Count", font=dict(size=11))),
        yaxis=dict(showgrid=False),
    ))
    defects_left = dcc.Graph(figure=fig_defects, config=CHART_CONFIG,
                             style={"height": "300px"})

    qc_labels = ["Pass", "Fail", "Rework"]
    qc_values = [8940, 312, 498]
    qc_colors = [COLORS["green"], COLORS["red"], COLORS["yellow"]]
    fig_qc = donut_figure(qc_labels, qc_values, qc_colors,
                          center_text="94.8%", title="Inspection Results")
    defects_right = dcc.Graph(figure=fig_qc, config=CHART_CONFIG,
                              style={"height": "300px"})

    tab_defects = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Defect Types by Production Line", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), defects_left], padding="20px"),
            _card([html.Div("Inspection Result Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), defects_right], padding="20px"),
        ],
    )

    # ── TAB 2: SPC ───────────────────────────────────────────────────────
    import random
    random.seed(42)
    samples = list(range(1, 31))
    ucl, lcl, target = 10.05, 9.95, 10.00
    measurements = [10.00 + (random.gauss(0, 0.015)) for _ in samples]
    # Introduce two out-of-control points
    measurements[12] = 10.06
    measurements[22] = 9.93

    fig_spc = go.Figure()
    fig_spc.add_trace(go.Scatter(
        x=samples, y=measurements, name="Measurement",
        mode="lines+markers",
        line=dict(color=COLORS["blue"], width=2),
        marker=dict(
            size=6,
            color=[COLORS["red"] if m > ucl or m < lcl else COLORS["blue"]
                   for m in measurements],
        ),
    ))
    fig_spc.add_hline(y=ucl, line_dash="dash", line_color=COLORS["red"],
                      opacity=0.7, annotation_text="UCL (10.05)",
                      annotation_font_color=COLORS["red"],
                      annotation_font_size=10)
    fig_spc.add_hline(y=lcl, line_dash="dash", line_color=COLORS["red"],
                      opacity=0.7, annotation_text="LCL (9.95)",
                      annotation_font_color=COLORS["red"],
                      annotation_font_size=10)
    fig_spc.add_hline(y=target, line_dash="dot", line_color=COLORS["green"],
                      opacity=0.5)
    fig_spc.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Dimension (mm)", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"],
                   range=[9.92, 10.08]),
        xaxis=dict(title="Sample #", showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    spc_left = dcc.Graph(figure=fig_spc, config=CHART_CONFIG,
                         style={"height": "300px"})

    # Cpk trend over time
    weeks = [f"W{w}" for w in range(1, 13)]
    cpk_vals = [1.42, 1.38, 1.45, 1.33, 1.28, 1.35, 1.40, 1.31, 1.44, 1.37, 1.46, 1.41]
    fig_cpk = go.Figure()
    fig_cpk.add_trace(go.Scatter(
        x=weeks, y=cpk_vals, name="Cpk",
        mode="lines+markers", line=dict(color=COLORS["purple"], width=2),
        marker=dict(size=5),
    ))
    fig_cpk.add_hline(y=1.33, line_dash="dash", line_color=COLORS["yellow"],
                      opacity=0.6, annotation_text="Min Cpk (1.33)",
                      annotation_font_color=COLORS["yellow"],
                      annotation_font_size=10)
    fig_cpk.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Cpk Value", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], range=[1.0, 1.6]),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    spc_right = dcc.Graph(figure=fig_cpk, config=CHART_CONFIG,
                          style={"height": "300px"})

    tab_spc = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("X-bar Control Chart (CNC-A1)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), spc_left], padding="20px"),
            _card([html.Div("Process Capability (Cpk) Trend", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), spc_right], padding="20px"),
        ],
    )

    # ── TAB 3: Tolerance ─────────────────────────────────────────────────
    features = ["Bore Diameter", "Surface Flatness", "Thread Pitch",
                "Wall Thickness", "Concentricity"]
    within_tol = [96.2, 93.8, 98.1, 91.4, 94.7]
    out_of_tol = [3.8, 6.2, 1.9, 8.6, 5.3]

    fig_tol = go.Figure()
    fig_tol.add_trace(go.Bar(
        x=features, y=within_tol, name="Within Tolerance %",
        marker_color=COLORS["green"],
    ))
    fig_tol.add_trace(go.Bar(
        x=features, y=out_of_tol, name="Out of Tolerance %",
        marker_color=COLORS["red"],
    ))
    fig_tol.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="%", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
        margin=dict(l=48, r=24, t=16, b=48),
    ))
    tol_left = dcc.Graph(figure=fig_tol, config=CHART_CONFIG,
                         style={"height": "300px"})

    tol_donut = dcc.Graph(
        figure=donut_figure(
            labels=["GD&T Pass", "Minor Deviation", "Major Deviation", "Reject"],
            values=[82, 10, 5, 3],
            colors=[COLORS["green"], COLORS["yellow"], COLORS["red"], COLORS["text_muted"]],
            center_text="82%",
            title="GD&T Compliance",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    tab_tolerance = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Tolerance Analysis by Feature", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), tol_left], padding="20px"),
            _card([html.Div("GD&T Compliance Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), tol_donut], padding="20px"),
        ],
    )

    # ── Bottom stats ──────────────────────────────────────────────────────
    bottom_stats = [
        ("First Pass Yield", "94.8%", "green"),
        ("DPMO", "2,340", "red"),
        ("Sigma Level", "4.32\u03c3", "purple"),
        ("Inspection Rate", "100%", "blue"),
        ("CAPA Open", "7", "yellow"),
    ]

    insight = insight_card(
        headline="Cpk Improved to 1.41",
        metric_value="1.41",
        direction="up",
        narrative="Process capability index improved to 1.41, within Six Sigma target range of 1.33+.",
        severity="healthy",
    )

    return layout_split(
        title="Quality Control",
        subtitle="Defect analysis and statistical process control",
        tab_contents=[
            ("Defects", tab_defects),
            ("SPC", tab_spc),
            ("Tolerance", tab_tolerance),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. render_supply_chain  —  Style D (layout_alerts)
# ═══════════════════════════════════════════════════════════════════════════

def render_supply_chain(cfg):
    """Supply chain risk view with alert cards for disruptions."""

    tabs = ["Critical", "Delayed", "On Track"]

    alerts = [
        {
            "severity": "critical",
            "title": "Semiconductor Chip Shortage \u2014 Supplier: NXP Semiconductors",
            "description": (
                "Lead time extended from 12 to 26 weeks for MCU modules. "
                "Affects CNC controller boards at Berlin and Detroit facilities. "
                "Current buffer stock covers 3.5 weeks of production."
            ),
            "impact": "Est. cost impact: $2.4M / month",
            "timestamp": "2 hours ago",
            "details": [
                ("Affected SKUs", "MCU-4410, MCU-4412, MCU-4420"),
                ("Production Lines", "CNC-A1, CNC-A2, CNC-G1"),
                ("Buffer Remaining", "3.5 weeks (critically low)"),
                ("Alt. Supplier Status", "STMicro \u2014 qualification in progress"),
            ],
        },
        {
            "severity": "critical",
            "title": "Logistics Disruption \u2014 Shanghai Port Congestion",
            "description": (
                "Vessel delays averaging 8\u201312 days at Shanghai Yangshan port. "
                "Inbound raw aluminum and steel shipments impacted. "
                "Three containers currently held at dock."
            ),
            "impact": "Est. cost impact: $890K / week",
            "timestamp": "5 hours ago",
            "details": [
                ("Containers Held", "3 of 12 (COSCO, Maersk)"),
                ("Materials Affected", "Aluminum 6061-T6, Steel AISI 4140"),
                ("Facility Impact", "Shanghai stamping lines D1, D2"),
                ("Mitigation", "Air freight for critical components authorized"),
            ],
        },
        {
            "severity": "warning",
            "title": "Raw Material Price Surge \u2014 Copper +18% (30-day)",
            "description": (
                "LME copper futures surged 18% in the past 30 days. "
                "Wire harness and motor winding costs projected to increase "
                "$340K next quarter if spot prices hold."
            ),
            "impact": "Projected margin impact: -1.2%",
            "timestamp": "1 day ago",
            "details": [
                ("Current Spot", "$9,840 / tonne"),
                ("Contracted Rate", "$8,320 / tonne (expires Q3)"),
                ("Hedge Coverage", "65% through Q2"),
                ("Components Affected", "Motor assemblies, wire harnesses"),
            ],
        },
        {
            "severity": "warning",
            "title": "Quality Hold \u2014 Supplier Batch Rejection (Bosch Rexroth)",
            "description": (
                "Hydraulic valve batch HR-2024-0891 failed incoming inspection. "
                "12 of 200 units showed pressure seal defects beyond tolerance. "
                "Supplier notified; replacement batch ETA 10 days."
            ),
            "impact": "Production delay: 3\u20135 days on Assembly-B1",
            "timestamp": "6 hours ago",
            "details": [
                ("Batch ID", "HR-2024-0891"),
                ("Defect Rate", "6.0% (threshold 1.5%)"),
                ("Affected Line", "Assembly-B1 (Detroit)"),
                ("CAPA Ref", "CAPA-2024-0147"),
            ],
        },
        {
            "severity": "info",
            "title": "New Supplier Onboarded \u2014 Precision Castings GmbH",
            "description": (
                "Qualification complete for aluminum die-cast housings. "
                "First production order placed: 5,000 units, delivery W14. "
                "Dual-sourcing now active for housing components."
            ),
            "impact": "Risk reduction: single-source dependency eliminated",
            "timestamp": "2 days ago",
            "details": [
                ("Supplier Rating", "A (ISO 9001, IATF 16949)"),
                ("Component", "Die-cast housing DH-440"),
                ("Lead Time", "6 weeks (vs 8 weeks incumbent)"),
                ("Cost Delta", "-4.2% vs current supplier"),
            ],
        },
        {
            "severity": "healthy",
            "title": "Steel Supply Stable \u2014 ThyssenKrupp Contract Renewed",
            "description": (
                "Annual contract for AISI 4140 and 1018 steel renewed with "
                "2.5% price lock through Q4. Delivery reliability at 98.7% "
                "over the last 12 months."
            ),
            "impact": "Savings: $180K annualized",
            "timestamp": "3 days ago",
            "details": [
                ("Contract Term", "12 months (auto-renew)"),
                ("Volume", "2,400 tonnes / quarter"),
                ("Delivery Score", "98.7%"),
                ("Price Lock", "2.5% below current market"),
            ],
        },
    ]

    summary_kpis = [
        {"label": "Active Suppliers", "value": "148", "accent": "blue"},
        {"label": "Open Risks", "value": "12", "accent": "red"},
        {"label": "On-Time Delivery", "value": "91.3%", "accent": "green"},
        {"label": "Avg Lead Time", "value": "18.4 days", "accent": "purple"},
    ]

    # ── Build tab contents using new API ───────────────────────────────
    critical_cards = [alert_card(**a) for a in alerts
                      if a["severity"] == "critical"]
    delayed_cards = [alert_card(**a) for a in alerts
                     if a["severity"] == "warning"] + [
        alert_card(severity="warning",
                   title="Tooling Delivery Delayed \u2014 Sandvik Coromant",
                   description="Replacement carbide inserts for CNC-A2 lathe turret delayed by 8 days due to customs hold in Rotterdam. Current inserts at 85% wear limit.",
                   timestamp="3 days ago"),
    ]
    on_track_cards = [alert_card(**a) for a in alerts
                      if a["severity"] in ("info", "healthy")] + [
        alert_card(severity="healthy",
                   title="Bearing Supply Stable \u2014 SKF Contract Active",
                   description="Quarterly delivery of precision bearings arrived on schedule. All 1,200 units passed incoming inspection. Next shipment: W18.",
                   timestamp="1 week ago"),
        alert_card(severity="healthy",
                   title="Hydraulic Fluid Inventory \u2014 Above Safety Stock",
                   description="ISO VG 46 hydraulic fluid reserves at 142% of safety stock across all facilities. Consumption rate trending 3% below forecast.",
                   timestamp="5 days ago"),
    ]

    tab_contents = [
        ("Critical", html.Div(critical_cards)),
        ("Delayed", html.Div(delayed_cards)),
        ("On Track", html.Div(on_track_cards)),
    ]

    insight = insight_card(
        headline="Titanium Supplier Lead Time Extended",
        metric_value="+3 wks",
        direction="up",
        narrative="Primary titanium supplier lead time extended 3 weeks. Recommend activating secondary source.",
        severity="critical",
    )

    return layout_alerts(
        title="Supply Chain Risk Monitor",
        subtitle="Supplier disruptions, material shortages, and logistics alerts",
        tab_contents=tab_contents,
        summary_kpis=summary_kpis,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. render_predictive_maintenance  —  Style E (layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_predictive_maintenance(cfg):
    """Predictive maintenance with equipment health forecasting."""

    # ── KPI strip ─────────────────────────────────────────────────────────
    kpi_items = [
        {"label": "MTBF", "value": "842 hrs", "accent": "blue"},
        {"label": "MTTR", "value": "4.2 hrs", "accent": "purple"},
        {"label": "Uptime", "value": "97.2%", "accent": "green"},
        {"label": "Open Work Orders", "value": "23", "accent": "yellow"},
    ]

    # ── Hero value ────────────────────────────────────────────────────────
    hero_value = "97.2%"
    hero_label = "Equipment Availability (All Facilities)"
    hero_trend = "+1.8% vs prior quarter"

    # ── Dual-axis chart: failure predictions ──────────────────────────────
    weeks = [f"W{w}" for w in range(1, 25)]
    predicted_failures = [
        3, 2, 4, 3, 5, 4, 3, 6, 4, 5, 7, 5,
        4, 6, 8, 5, 7, 6, 9, 7, 8, 10, 7, 6,
    ]
    actual_failures = [
        2, 3, 3, 4, 4, 5, 2, 5, 5, 4, 6, 6,
        3, 5, 7, 6, 6, 7, 8, 8, 7, 9, None, None,
    ]
    confidence = [
        92, 91, 93, 90, 88, 89, 94, 87, 90, 88, 85, 86,
        91, 87, 84, 88, 86, 85, 82, 83, 84, 80, 78, 76,
    ]

    fig_forecast = go.Figure()
    fig_forecast.add_trace(go.Bar(
        x=weeks, y=predicted_failures, name="Predicted Failures",
        marker_color=COLORS["blue"], opacity=0.7, yaxis="y",
    ))
    fig_forecast.add_trace(go.Scatter(
        x=weeks, y=actual_failures, name="Actual Failures",
        mode="lines+markers", line=dict(color=COLORS["red"], width=2),
        marker=dict(size=5), yaxis="y",
        connectgaps=False,
    ))
    fig_forecast.add_trace(go.Scatter(
        x=weeks, y=confidence, name="Model Confidence %",
        mode="lines", line=dict(color=COLORS["green"], width=2, dash="dot"),
        yaxis="y2",
    ))
    fig_forecast.update_layout(**dark_chart_layout(
        height=320,
        yaxis=dict(
            title=dict(text="Failure Count", font=dict(size=11)),
            showgrid=True, gridcolor=COLORS["border"],
            color=COLORS["text_muted"],
        ),
        yaxis2=dict(
            title=dict(text="Confidence %", font=dict(size=11)),
            overlaying="y", side="right",
            showgrid=False, color=COLORS["green"],
            range=[50, 100],
        ),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
        margin=dict(l=48, r=56, t=24, b=48),
    ))
    main_chart = dcc.Graph(figure=fig_forecast, config=CHART_CONFIG,
                           style={"height": "320px"})

    # ── Side panel: breakdown by equipment type ───────────────────────────
    equipment_breakdown = breakdown_list([
        {"label": "CNC Machines", "value": "6", "pct": 35, "color": COLORS["blue"]},
        {"label": "Hydraulic Press", "value": "4", "pct": 24, "color": COLORS["purple"]},
        {"label": "Conveyor Systems", "value": "3", "pct": 18, "color": COLORS["green"]},
        {"label": "Welding Robots", "value": "2", "pct": 12, "color": COLORS["yellow"]},
        {"label": "Paint Booths", "value": "1", "pct": 6, "color": COLORS["red"]},
        {"label": "HVAC / Utilities", "value": "1", "pct": 5, "color": COLORS["blue"]},
    ])
    side_content = html.Div([
        html.Div("Pending Failures by Equipment Type",
                 style={"fontSize": "14px", "fontWeight": "600",
                        "color": COLORS["white"], "marginBottom": "16px"}),
        equipment_breakdown,
    ])

    # ── Bottom table: upcoming work orders ────────────────────────────────
    wo_headers = ["Work Order", "Equipment", "Facility", "Priority", "Due Date", "Status"]
    wo_col_widths = ["14%", "20%", "14%", "12%", "14%", "12%"]
    wo_data = [
        ("WO-4821", "CNC-A1 Spindle Motor", "Berlin", "Critical", "Mar 12", "Critical"),
        ("WO-4822", "Hydraulic Press HP-03", "Detroit", "High", "Mar 14", "Warning"),
        ("WO-4823", "Conveyor Belt CB-12", "Shanghai", "Medium", "Mar 18", "Nominal"),
        ("WO-4824", "Welding Robot WR-07", "Tokyo", "High", "Mar 15", "Warning"),
        ("WO-4825", "Paint Booth PB-02", "Berlin", "Low", "Mar 22", "Healthy"),
        ("WO-4826", "CNC-G1 Coolant Pump", "Tokyo", "Medium", "Mar 20", "Nominal"),
    ]
    wo_rows = []
    for wo_id, equip, facility, priority, due, status in wo_data:
        wo_rows.append(html.Tr([
            td(wo_id, bold=True, color=COLORS["blue"]),
            td(equip, bold=True),
            td(facility),
            td(priority, color=COLORS["red"] if priority == "Critical"
               else COLORS["yellow"] if priority == "High"
               else COLORS["text_muted"]),
            td(due, mono=True),
            status_td(status),
        ]))
    bottom_table = rich_table(wo_headers, wo_rows, col_widths=wo_col_widths)

    insight = insight_card(
        headline="4 Bearing Failures Predicted",
        metric_value="4",
        direction="up",
        narrative="ML model predicting 4 bearing failures in next 14 days with 92% confidence.",
        severity="warning",
    )

    return layout_forecast(
        title="Predictive Maintenance",
        subtitle="AI-driven failure prediction and maintenance scheduling",
        kpi_items=kpi_items,
        hero_value=hero_value,
        hero_label=hero_label,
        hero_trend_text=hero_trend,
        main_chart=main_chart,
        side_component=side_content,
        bottom_table=bottom_table,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. render_energy_sustainability  —  Style F (layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_energy_sustainability(cfg):
    """Energy and sustainability metrics across manufacturing facilities."""

    # ── Grid item 1: Energy Efficiency gauge ──────────────────────────────
    fig_gauge = gauge_figure(82.5, 100, title="Energy Efficiency Index", color=COLORS["green"])
    gauge_panel = html.Div([
        dcc.Graph(figure=fig_gauge, config=CHART_CONFIG, style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "8px"},
            children=[
                html.Span("Target: 85%", style={"fontSize": "12px",
                                                  "color": COLORS["text_muted"]}),
                trend_indicator("up", "+3.1% vs Q3"),
            ],
        ),
    ])

    # ── Grid item 2: Carbon emissions sparkline ───────────────────────────
    carbon_monthly = [420, 410, 395, 388, 375, 362, 350, 345, 338, 332, 328, 315]
    carbon_panel = metric_with_sparkline(
        "Carbon Emissions (tonnes CO2e)",
        "315 t",
        carbon_monthly,
        accent="green",
    )

    # ── Grid item 3: kWh per unit (tall panel) ────────────────────────────
    facilities = ["Berlin", "Detroit", "Tokyo", "Shanghai"]
    kwh_values = [4.8, 5.2, 3.9, 6.1]
    kwh_targets = [4.5, 4.5, 4.5, 4.5]

    fig_kwh = go.Figure()
    fig_kwh.add_trace(go.Bar(
        y=facilities, x=kwh_values, name="Actual",
        orientation="h", marker_color=COLORS["blue"], opacity=0.85,
    ))
    fig_kwh.add_trace(go.Bar(
        y=facilities, x=kwh_targets, name="Target",
        orientation="h", marker_color=COLORS["green"], opacity=0.5,
    ))
    fig_kwh.update_layout(**dark_chart_layout(
        height=320, barmode="group",
        title=dict(text="Energy per Unit (kWh/unit)",
                   font=dict(size=13, color=COLORS["white"]),
                   x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"),
        margin=dict(l=80, r=24, t=40, b=40),
        xaxis=dict(showgrid=True, gridcolor=COLORS["border"]),
        yaxis=dict(showgrid=False),
    ))
    kwh_panel = html.Div([
        dcc.Graph(figure=fig_kwh, config=CHART_CONFIG, style={"height": "320px"}),
    ])

    # ── Grid item 4: Water usage metric ───────────────────────────────────
    water_monthly = [180, 175, 172, 168, 165, 160, 158, 155, 150, 148, 145, 142]
    water_panel = metric_with_sparkline(
        "Water Usage (m\u00b3/day)",
        "142 m\u00b3",
        water_monthly,
        accent="blue",
    )

    # ── Grid item 5: Waste recycling rate ─────────────────────────────────
    fig_recycle = gauge_figure(73.8, 100, title="Waste Recycling Rate", color=COLORS["purple"])
    recycle_panel = html.Div([
        dcc.Graph(figure=fig_recycle, config=CHART_CONFIG, style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "8px"},
            children=[
                html.Span("Target: 80%", style={"fontSize": "12px",
                                                  "color": COLORS["text_muted"]}),
                trend_indicator("up", "+5.4% YoY"),
            ],
        ),
    ])

    # ── Grid item 6: Sustainability score card ────────────────────────────
    score_panel = html.Div([
        html.Div("Sustainability Score",
                 style={"fontSize": "11px", "color": COLORS["text_muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.4px",
                        "marginBottom": "8px"}),
        html.Div("B+", style={"fontSize": "48px", "fontWeight": "700",
                               "color": COLORS["green"], "lineHeight": "1.1",
                               "marginBottom": "12px"}),
        html.Div(
            style={"display": "flex", "flexDirection": "column", "gap": "8px"},
            children=[
                progress_row("ISO 14001 Compliance", "94%", 94, COLORS["green"]),
                progress_row("Scope 1 Reduction", "68%", 68, COLORS["blue"]),
                progress_row("Scope 2 Reduction", "52%", 52, COLORS["purple"]),
                progress_row("Zero Waste to Landfill", "73%", 73, COLORS["yellow"]),
            ],
        ),
    ])

    # ── Assemble grid items ───────────────────────────────────────────────
    grid_items = [
        {"col_span": 1, "row_span": 1, "content": gauge_panel},
        {"col_span": 1, "row_span": 1, "content": carbon_panel},
        {"col_span": 1, "row_span": 2, "content": kwh_panel},
        {"col_span": 1, "row_span": 1, "content": water_panel},
        {"col_span": 1, "row_span": 1, "content": recycle_panel},
        {"col_span": 2, "row_span": 1, "content": score_panel},
    ]

    insight = insight_card(
        headline="Carbon Emissions Down 8% YoY",
        metric_value="-8%",
        direction="down",
        narrative="Carbon emissions per unit down 8% year-over-year. On track for ESG targets.",
        severity="healthy",
    )

    return layout_grid(
        title="Energy & Sustainability",
        subtitle="Environmental performance and resource efficiency tracking",
        grid_items=grid_items,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. render_workforce_ops  —  Style B variant (layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_workforce_ops(cfg):
    """Workforce operations with department metrics and safety tracking."""

    # ── Filters ───────────────────────────────────────────────────────────
    filters = [
        {"label": "Facility", "options": ["All Facilities", "Berlin", "Detroit", "Tokyo", "Shanghai"]},
        {"label": "Department", "options": ["All Departments", "Production", "Quality", "Maintenance",
                                             "Logistics", "Engineering"]},
        {"label": "Shift", "options": ["All Shifts", "Morning", "Afternoon", "Night"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────────
    kpi_items = [
        {"label": "Total Headcount", "value": "2,847", "accent": "blue"},
        {"label": "Overtime Hours", "value": "12.4%", "accent": "yellow"},
        {"label": "Safety Score", "value": "96.1", "accent": "green"},
        {"label": "Training Completion", "value": "88.5%", "accent": "purple"},
        {"label": "Turnover Rate", "value": "4.2%", "accent": "red"},
    ]

    # ── Workforce table by department ─────────────────────────────────────
    columns = [
        {"name": "Department", "id": "department"},
        {"name": "Facility", "id": "facility"},
        {"name": "Headcount", "id": "headcount"},
        {"name": "Overtime %", "id": "overtime"},
        {"name": "Safety Score", "id": "safety_score"},
        {"name": "Training", "id": "training"},
        {"name": "Status", "id": "status"},
    ]

    data = [
        {"department": "Production", "facility": "Berlin", "headcount": "342", "overtime": "14.2%", "safety_score": "97%", "training": "92%", "status": "Healthy"},
        {"department": "Production", "facility": "Detroit", "headcount": "318", "overtime": "16.8%", "safety_score": "94%", "training": "86%", "status": "Nominal"},
        {"department": "Production", "facility": "Tokyo", "headcount": "286", "overtime": "11.5%", "safety_score": "98%", "training": "94%", "status": "Healthy"},
        {"department": "Production", "facility": "Shanghai", "headcount": "410", "overtime": "18.3%", "safety_score": "91%", "training": "82%", "status": "Warning"},
        {"department": "Quality", "facility": "Berlin", "headcount": "84", "overtime": "8.1%", "safety_score": "99%", "training": "96%", "status": "Healthy"},
        {"department": "Quality", "facility": "Detroit", "headcount": "76", "overtime": "9.4%", "safety_score": "97%", "training": "90%", "status": "Healthy"},
        {"department": "Quality", "facility": "Tokyo", "headcount": "68", "overtime": "6.2%", "safety_score": "99%", "training": "97%", "status": "Healthy"},
        {"department": "Quality", "facility": "Shanghai", "headcount": "92", "overtime": "10.5%", "safety_score": "95%", "training": "84%", "status": "Nominal"},
        {"department": "Maintenance", "facility": "Berlin", "headcount": "56", "overtime": "22.4%", "safety_score": "93%", "training": "88%", "status": "Warning"},
        {"department": "Maintenance", "facility": "Detroit", "headcount": "52", "overtime": "24.1%", "safety_score": "90%", "training": "78%", "status": "Warning"},
        {"department": "Maintenance", "facility": "Tokyo", "headcount": "44", "overtime": "15.8%", "safety_score": "96%", "training": "91%", "status": "Healthy"},
        {"department": "Maintenance", "facility": "Shanghai", "headcount": "62", "overtime": "26.5%", "safety_score": "88%", "training": "72%", "status": "Critical"},
        {"department": "Logistics", "facility": "Berlin", "headcount": "68", "overtime": "10.2%", "safety_score": "96%", "training": "90%", "status": "Healthy"},
        {"department": "Logistics", "facility": "Detroit", "headcount": "72", "overtime": "12.6%", "safety_score": "94%", "training": "85%", "status": "Nominal"},
        {"department": "Logistics", "facility": "Tokyo", "headcount": "58", "overtime": "8.4%", "safety_score": "97%", "training": "93%", "status": "Healthy"},
        {"department": "Logistics", "facility": "Shanghai", "headcount": "86", "overtime": "14.8%", "safety_score": "92%", "training": "80%", "status": "Warning"},
        {"department": "Engineering", "facility": "Berlin", "headcount": "124", "overtime": "6.5%", "safety_score": "98%", "training": "95%", "status": "Healthy"},
        {"department": "Engineering", "facility": "Detroit", "headcount": "110", "overtime": "7.8%", "safety_score": "97%", "training": "92%", "status": "Healthy"},
        {"department": "Engineering", "facility": "Tokyo", "headcount": "96", "overtime": "5.2%", "safety_score": "99%", "training": "96%", "status": "Healthy"},
        {"department": "Engineering", "facility": "Shanghai", "headcount": "143", "overtime": "8.9%", "safety_score": "96%", "training": "88%", "status": "Nominal"},
    ]

    insight = insight_card(
        headline="Safety Score at Record High",
        metric_value="96.1",
        direction="up",
        narrative="Safety score at 96.1 — best quarter in 2 years. Zero lost-time incidents this month.",
        severity="healthy",
    )

    return layout_table(
        title="Workforce Operations",
        subtitle="Staffing, safety, and training metrics by facility and department",
        filters=filters,
        kpi_items=kpi_items,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )
