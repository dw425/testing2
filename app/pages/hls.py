"""
Page renderers for the Healthcare & Life Sciences (HLS) vertical.

Covers sub-verticals: Healthcare Ops, Network & Quality, BioPharma Intelligence,
Supply Chain & Manufacturing, MedTech & Digital Surgery, and Patient Outcomes.
Each public ``render_*`` function accepts a ``cfg`` dict and returns an
``html.Div`` that can be dropped into the main content area.
"""

from app.page_styles import (
    dark_chart_layout, CHART_CONFIG, ACCENT_ICONS,
    page_header, hero_metric, compact_kpi, kpi_strip, filter_bar,
    tab_bar, info_banner, alert_card, progress_row, stat_card,
    breakdown_list, donut_figure,
    layout_executive, layout_table, layout_split, layout_alerts,
    layout_forecast, layout_grid,
    gauge_figure, sparkline_figure, metric_with_sparkline,
    _card, _hex_to_rgb,
    insight_card, morning_briefing,
)
from app.theme import COLORS, FONT_FAMILY, get_vertical_theme
from dash import dcc, html
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
#  1. DASHBOARD  (Style A — layout_executive)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive HLS dashboard with hero metrics, patient volume trend,
    department revenue donut and readmission trend chart."""

    # ── Morning briefing ──────────────────────────────────────────────
    briefing = morning_briefing(
        title="HLS Morning Briefing",
        summary_text=(
            "Operating Margin Resilience remains strong at 12.4%, holding above "
            "the 10% target for the third consecutive quarter despite rising supply "
            "costs. Labor stability continues to improve with the Labor Stability "
            "Index at 87%, driven by reduced RN turnover and expanded retention "
            "programs. Patient outcomes are trending favorably\u2014readmission rates "
            "fell to 8.2% and the Patient Outcomes Score reached 94.2%, reflecting "
            "sustained gains in clinical quality across all departments."
        ),
        signals=[
            {"label": "Operating Margin", "status": "green",
             "detail": "12.4% \u2014 above 10% target for Q3 running"},
            {"label": "Labor Stability Index", "status": "green",
             "detail": "87% \u2014 RN turnover down 4.1 pts YoY"},
            {"label": "Patient Outcomes", "status": "green",
             "detail": "94.2% composite score, readmissions at 8.2%"},
            {"label": "Supply Chain", "status": "amber",
             "detail": "Epinephrine shortage \u2014 18 days stock remaining"},
        ],
    )

    # ── Hero metrics ──────────────────────────────────────────────────
    heroes = [
        hero_metric("North Star: Operating Margin", "12.4%",
                     trend_text="+1.2 pts vs prior quarter", trend_dir="up",
                     accent="green", href="/hls/healthcare_ops"),
        hero_metric("Labor Stability Index", "87%",
                     trend_text="+4.1 pts vs prior year", trend_dir="up",
                     accent="blue", href="/hls/healthcare_ops"),
        hero_metric("Patient Outcomes Score", "94.2%",
                     trend_text="+2.1% vs last quarter", trend_dir="up",
                     accent="green", href="/hls/patient_outcomes"),
        hero_metric("Bed Utilization", "87.6%",
                     trend_text="+3.4% vs last month", trend_dir="up",
                     accent="blue", href="/hls/healthcare_ops"),
        hero_metric("Clinical Trial Success", "68.3%",
                     trend_text="+5.7% vs prior year", trend_dir="up",
                     accent="purple", href="/hls/biopharma_intel"),
    ]

    # ── Main chart: patient volume trend ──────────────────────────────
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    inpatient = [4120, 4350, 4580, 4410, 4720, 4950,
                 5100, 5280, 5060, 5340, 5520, 5680]
    outpatient = [6800, 7100, 7350, 7200, 7580, 7900,
                  8150, 8400, 8100, 8550, 8780, 9020]
    emergency = [2100, 2250, 2180, 2320, 2450, 2380,
                 2520, 2600, 2480, 2700, 2650, 2810]

    vol_fig = go.Figure()
    vol_fig.add_trace(go.Scatter(
        x=months, y=inpatient, name="Inpatient",
        mode="lines+markers", line=dict(color=COLORS["blue"], width=2),
        marker=dict(size=5),
    ))
    vol_fig.add_trace(go.Scatter(
        x=months, y=outpatient, name="Outpatient",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=5),
    ))
    vol_fig.add_trace(go.Scatter(
        x=months, y=emergency, name="Emergency",
        mode="lines+markers", line=dict(color=COLORS["red"], width=2),
        marker=dict(size=5),
    ))
    vol_fig.update_layout(**dark_chart_layout(
        vertical="hls",
        height=320,
        title=dict(text="Patient Volume Trends (2025)",
                   font=dict(size=14, color=COLORS["white"]),
                   x=0.5, xanchor="center"),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="Patients"),
    ))
    main_chart = dcc.Graph(figure=vol_fig, config=CHART_CONFIG,
                           style={"height": "320px"})

    # ── Panel 1: department revenue donut ─────────────────────────────
    dept_labels = ["Cardiology", "Oncology", "Orthopedics",
                   "Neurology", "Pediatrics", "Other"]
    dept_values = [28, 22, 18, 14, 10, 8]
    dept_colors = [COLORS["blue"], COLORS["green"], COLORS["purple"],
                   COLORS["red"], COLORS["yellow"], COLORS["text_muted"]]
    dept_fig = donut_figure(dept_labels, dept_values, dept_colors,
                            center_text="$248M", title="Revenue by Department")
    dept_chart = dcc.Graph(figure=dept_fig, config=CHART_CONFIG,
                           style={"height": "280px"})

    # ── Panel 2: readmission trend ────────────────────────────────────
    readmit_fig = go.Figure()
    readmit_rates = [12.4, 11.8, 11.2, 10.9, 10.5, 10.1,
                     9.8, 9.4, 9.1, 8.7, 8.5, 8.2]
    readmit_fig.add_trace(go.Scatter(
        x=months, y=readmit_rates, name="Readmission %",
        mode="lines+markers", line=dict(color=COLORS["red"], width=2),
        marker=dict(size=5),
    ))
    target_line = [10.0] * 12
    readmit_fig.add_trace(go.Scatter(
        x=months, y=target_line, name="Target",
        mode="lines", line=dict(color=COLORS["yellow"], width=1, dash="dash"),
    ))
    readmit_fig.update_layout(**dark_chart_layout(
        vertical="hls",
        height=280,
        title=dict(text="30-Day Readmission Rate",
                   font=dict(size=14, color=COLORS["white"]),
                   x=0.5, xanchor="center"),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="%", range=[0, 15]),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
    ))
    readmit_chart = dcc.Graph(figure=readmit_fig, config=CHART_CONFIG,
                              style={"height": "280px"})

    # ── Insight card ──────────────────────────────────────────────────
    staffing_insight = insight_card(
        headline="Staffing Risk: ICU Weekend Coverage",
        metric_value="74%",
        direction="down",
        narrative=(
            "ICU weekend nurse-to-patient ratios have dipped below the 80% "
            "staffing threshold for 3 of the last 4 weekends. This correlates "
            "with a 1.6-point uptick in weekend readmission rates and may "
            "pressure operating margin if overtime costs continue to rise."
        ),
        action_text="Review ICU staffing plan",
        severity="warning",
    )

    panels = [
        ("Department Revenue", dept_chart, "/hls/healthcare_ops"),
        ("Readmission Trend", readmit_chart, "/hls/patient_outcomes"),
    ]

    # ── Layout assembly with briefing and insight ─────────────────────
    dashboard = layout_executive(
        title="Healthcare & Life Sciences Dashboard",
        subtitle="Executive overview of clinical performance, utilization, and financials",
        heroes=heroes,
        main_chart=main_chart,
        panels=panels,
        briefing=briefing,
    )

    # Append insight into the content area
    content_area = dashboard.children[1]  # the content-area div
    content_area.children.append(staffing_insight)

    return dashboard


# ═══════════════════════════════════════════════════════════════════════════
#  2. HEALTHCARE OPS  (Style F — layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_healthcare_ops(cfg):
    """Multi-panel operations grid: bed occupancy gauge, ER wait sparkline,
    surgeries completed, staff utilization, equipment uptime, patient
    throughput."""

    # ── Bed Occupancy gauge ───────────────────────────────────────────
    bed_gauge = gauge_figure(87.6, 100, title="Bed Occupancy %",
                             color=COLORS["blue"])
    bed_panel = html.Div([
        dcc.Graph(figure=bed_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "6px"},
            children=[
                html.Span("1,314 / 1,500 beds occupied",
                           style={"fontSize": "12px",
                                  "color": COLORS["text_muted"]}),
            ],
        ),
    ])

    # ── ER Wait Time sparkline ────────────────────────────────────────
    er_vals = [42, 38, 45, 51, 47, 39, 36, 41, 44, 38, 35, 33,
               37, 40, 43, 39, 36, 34, 31, 29, 32, 35, 30, 28]
    er_panel = metric_with_sparkline("ER Wait Time", "28 min",
                                      er_vals, accent="green")

    # ── Surgeries Completed ───────────────────────────────────────────
    surg_vals = [82, 91, 87, 95, 88, 93, 97, 102, 99, 105, 108, 112]
    surg_panel = metric_with_sparkline("Surgeries This Month", "1,247",
                                        surg_vals, accent="blue")

    # ── Staff Utilization ─────────────────────────────────────────────
    staff_panel = html.Div([
        html.Div("Staff Utilization", style={
            "fontSize": "11px", "color": COLORS["text_muted"],
            "textTransform": "uppercase", "letterSpacing": "0.4px",
            "marginBottom": "12px"}),
        progress_row("Physicians", "92%", 92, COLORS["blue"]),
        progress_row("Nurses", "96%", 96, COLORS["green"]),
        progress_row("Technicians", "84%", 84, COLORS["purple"]),
        progress_row("Support Staff", "78%", 78, COLORS["yellow"]),
    ])

    # ── Equipment Uptime ──────────────────────────────────────────────
    equip_gauge = gauge_figure(98.4, 100, title="Equipment Uptime %",
                               color=COLORS["green"])
    equip_panel = html.Div([
        dcc.Graph(figure=equip_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-around",
                    "marginTop": "8px"},
            children=[
                html.Div([
                    html.Div("MRI", style={"fontSize": "11px",
                                            "color": COLORS["text_muted"]}),
                    html.Div("99.1%", style={"fontSize": "14px",
                                              "fontWeight": "600",
                                              "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Div("CT", style={"fontSize": "11px",
                                           "color": COLORS["text_muted"]}),
                    html.Div("97.8%", style={"fontSize": "14px",
                                              "fontWeight": "600",
                                              "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Div("Ventilators", style={"fontSize": "11px",
                                                     "color": COLORS["text_muted"]}),
                    html.Div("98.2%", style={"fontSize": "14px",
                                              "fontWeight": "600",
                                              "color": COLORS["yellow"]}),
                ]),
            ],
        ),
    ])

    # ── Patient Throughput ────────────────────────────────────────────
    tp_months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    tp_admits = [4850, 5120, 4980, 5280, 5450, 5680]
    tp_discharges = [4720, 4980, 5050, 5190, 5380, 5610]
    tp_fig = go.Figure()
    tp_fig.add_trace(go.Bar(
        x=tp_months, y=tp_admits, name="Admissions",
        marker_color=COLORS["blue"], opacity=0.85,
    ))
    tp_fig.add_trace(go.Bar(
        x=tp_months, y=tp_discharges, name="Discharges",
        marker_color=COLORS["green"], opacity=0.85,
    ))
    tp_fig.update_layout(**dark_chart_layout(
        height=220, barmode="group",
        title=dict(text="Patient Throughput", font=dict(size=12,
                   color=COLORS["text_muted"]), x=0.5, xanchor="center"),
        margin=dict(l=40, r=16, t=36, b=28),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
    ))
    tp_panel = dcc.Graph(figure=tp_fig, config=CHART_CONFIG,
                         style={"height": "220px"})

    grid_items = [
        {"col_span": 1, "row_span": 1, "content": bed_panel},
        {"col_span": 1, "row_span": 1, "content": er_panel},
        {"col_span": 1, "row_span": 2, "content": staff_panel},
        {"col_span": 2, "row_span": 1, "content": tp_panel},
        {"col_span": 1, "row_span": 1, "content": surg_panel},
        {"col_span": 1, "row_span": 1, "content": equip_panel},
    ]

    insight = insight_card(
        headline="ER Wait Times Down 12%",
        metric_value="-12%",
        direction="down",
        narrative="ER wait times decreased 12% after triage protocol update. Average wait now 28 minutes.",
        severity="healthy",
    )

    return layout_grid(
        title="Healthcare Operations",
        subtitle="Real-time operational metrics across facilities",
        grid_items=grid_items,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. NETWORK & QUALITY  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_network_quality(cfg):
    """Provider network quality view with filters, KPI strip, and provider
    performance table with quality progress bars."""

    # ── Filters ───────────────────────────────────────────────────────
    filters = [
        {"label": "Region", "options": ["All Regions", "Northeast",
                                         "Southeast", "Midwest", "West"]},
        {"label": "Facility", "options": ["All Facilities", "Acute Care",
                                           "Ambulatory", "Rehab", "SNF"]},
        {"label": "Specialty", "options": ["All Specialties", "Cardiology",
                                            "Oncology", "Orthopedics",
                                            "Primary Care"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────
    kpi_items = [
        {"label": "Star Rating", "value": "4.6 / 5.0", "accent": "blue"},
        {"label": "HEDIS Score", "value": "91.3%", "accent": "green"},
        {"label": "Provider Count", "value": "2,847", "accent": "purple"},
        {"label": "Member Satisfaction", "value": "88.7%", "accent": "blue"},
        {"label": "Claims Ratio", "value": "82.4%", "accent": "yellow"},
    ]

    # ── Provider table ────────────────────────────────────────────────
    columns = [
        {"name": "Provider Network", "id": "provider_network"},
        {"name": "Region", "id": "region"},
        {"name": "Providers", "id": "providers"},
        {"name": "Star Rating", "id": "star_rating"},
        {"name": "HEDIS", "id": "hedis"},
        {"name": "Quality Score", "id": "quality_score"},
        {"name": "Status", "id": "status"},
    ]

    network_data = [
        ("Northeast Health Alliance", "Northeast", "412",
         "4.8", "94.1%", 94, "Healthy"),
        ("Southeast Medical Group", "Southeast", "387",
         "4.5", "91.8%", 88, "Healthy"),
        ("Midwest Care Partners", "Midwest", "298",
         "4.3", "89.2%", 82, "Healthy"),
        ("Pacific Provider Network", "West", "524",
         "4.7", "93.5%", 91, "Healthy"),
        ("Great Lakes Health System", "Midwest", "345",
         "4.1", "86.7%", 76, "Warning"),
        ("Southern Specialty Associates", "Southeast", "276",
         "3.9", "84.3%", 71, "Warning"),
        ("Mountain West Medical", "West", "189",
         "4.4", "90.6%", 85, "Healthy"),
        ("Atlantic Primary Care", "Northeast", "416",
         "4.6", "92.4%", 89, "Healthy"),
    ]

    data = []
    for name, region, provs, star, hedis, q_pct, status in network_data:
        data.append({
            "provider_network": name,
            "region": region,
            "providers": provs,
            "star_rating": star,
            "hedis": hedis,
            "quality_score": f"{q_pct}%",
            "status": status,
        })

    insight = insight_card(
        headline="3 Providers Below HEDIS Threshold",
        metric_value="3",
        direction="up",
        narrative="Three providers fell below HEDIS compliance thresholds this quarter. Network contract reviews recommended.",
        severity="warning",
    )

    return layout_table(
        title="Network & Quality",
        subtitle="Provider network performance, HEDIS compliance, and quality metrics",
        filters=filters,
        kpi_items=kpi_items,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. BIOPHARMA INTELLIGENCE  (Style E — layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_biopharma_intel(cfg):
    """BioPharma pipeline analysis with hero value, dual-axis trial
    progression chart, and therapeutic area breakdown."""

    # ── KPIs ──────────────────────────────────────────────────────────
    kpi_items = [
        {"label": "Pipeline Value", "value": "$4.2B", "accent": "blue"},
        {"label": "Trials Active", "value": "147", "accent": "green"},
        {"label": "Approval Rate", "value": "23.6%", "accent": "purple"},
        {"label": "Time-to-Market", "value": "8.2 yrs", "accent": "yellow"},
    ]

    # ── Dual-axis trial progression chart ─────────────────────────────
    phases = ["Preclinical", "Phase I", "Phase II", "Phase III",
              "NDA/BLA", "Approved"]
    trial_counts = [42, 38, 31, 22, 8, 6]
    success_rates = [100, 63.2, 48.4, 30.7, 18.5, 14.1]

    trial_fig = go.Figure()
    trial_fig.add_trace(go.Bar(
        x=phases, y=trial_counts, name="Active Trials",
        marker_color=COLORS["blue"], opacity=0.85, yaxis="y",
    ))
    trial_fig.add_trace(go.Scatter(
        x=phases, y=success_rates, name="Cumulative Success %",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=7, color=COLORS["green"]),
        yaxis="y2",
    ))
    trial_fig.update_layout(**dark_chart_layout(
        height=300,
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        yaxis=dict(title="Trial Count", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        yaxis2=dict(title="Success Rate %", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"],
                    range=[0, 110]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
        margin=dict(l=48, r=48, t=16, b=60),
    ))
    main_chart = dcc.Graph(figure=trial_fig, config=CHART_CONFIG,
                           style={"height": "300px"})

    # ── Side breakdown by therapeutic area ────────────────────────────
    side = html.Div([
        html.Div("Pipeline by Therapeutic Area", style={
            "fontSize": "14px", "fontWeight": "600",
            "color": COLORS["white"], "marginBottom": "16px"}),
        breakdown_list([
            {"label": "Oncology", "value": "$1.4B", "pct": 33,
             "color": COLORS["blue"]},
            {"label": "Immunology", "value": "$890M", "pct": 21,
             "color": COLORS["green"]},
            {"label": "Neuroscience", "value": "$720M", "pct": 17,
             "color": COLORS["purple"]},
            {"label": "Rare Disease", "value": "$540M", "pct": 13,
             "color": COLORS["yellow"]},
            {"label": "Cardiovascular", "value": "$410M", "pct": 10,
             "color": COLORS["red"]},
            {"label": "Infectious Disease", "value": "$240M", "pct": 6,
             "color": COLORS["text_muted"]},
        ]),
    ])

    insight = insight_card(
        headline="Phase III Success Rate Trending Up",
        metric_value="68%",
        direction="up",
        narrative="Phase III success rate at 68%, up from 61% last year. Oncology pipeline strongest performer.",
        severity="healthy",
    )

    return layout_forecast(
        title="BioPharma Intelligence",
        subtitle="Drug pipeline analytics, clinical trial tracking, and market forecast",
        kpi_items=kpi_items,
        hero_value="$4.2B",
        hero_label="Total Pipeline Value",
        hero_trend_text="+12.8% vs prior year",
        main_chart=main_chart,
        side_component=side,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. SUPPLY CHAIN  (Style D — layout_alerts)
# ═══════════════════════════════════════════════════════════════════════════

def render_supply_chain(cfg):
    """Supply chain alert view with critical drug shortages, equipment
    delays, cold chain breaks, and supplier issues."""

    tabs = ["Critical", "Warning", "Normal"]

    alerts = [
        {
            "severity": "critical",
            "title": "Epinephrine Auto-Injector Shortage",
            "description": ("National supply of epinephrine auto-injectors "
                            "has dropped below 30-day safety threshold. "
                            "Manufacturer reports production line maintenance "
                            "causing 6-week delay in resupply."),
            "impact": "Cost Impact: $2.4M | 847 patients affected",
            "timestamp": "2 hours ago",
            "details": [
                ("Current Stock", "12,400 units (18 days)"),
                ("Reorder ETA", "March 28, 2026"),
                ("Affected Facilities", "14 hospitals, 38 clinics"),
                ("Backup Supplier", "Pending qualification"),
            ],
        },
        {
            "severity": "critical",
            "title": "MRI Coil Replacement Delayed",
            "description": ("Critical MRI gradient coil replacement for "
                            "Northeast Regional Medical Center delayed due "
                            "to component shortage from primary manufacturer."),
            "impact": "Cost Impact: $890K | 120 scans/week affected",
            "timestamp": "5 hours ago",
            "details": [
                ("Equipment", "Siemens MAGNETOM Vida 3T"),
                ("Original ETA", "Feb 15, 2026"),
                ("Revised ETA", "Apr 10, 2026"),
                ("Workaround", "Redirecting to satellite facility"),
            ],
        },
        {
            "severity": "warning",
            "title": "Cold Chain Deviation — Vaccine Shipment",
            "description": ("Temperature excursion detected in mRNA vaccine "
                            "shipment ID VX-20260308. Sensors recorded 2.1C "
                            "above threshold for 47 minutes during transit."),
            "impact": "Cost Impact: $340K | 5,200 doses at risk",
            "timestamp": "8 hours ago",
            "details": [
                ("Shipment ID", "VX-20260308"),
                ("Max Temp Recorded", "-67.9C (limit: -70C)"),
                ("Duration", "47 min excursion"),
                ("QA Decision", "Pending potency analysis"),
            ],
        },
        {
            "severity": "warning",
            "title": "Surgical Glove Supplier Quality Issue",
            "description": ("Lot GL-2026-1142 from Medline Industries failed "
                            "AQL sampling — pinhole defect rate at 3.2%% "
                            "exceeds the 1.5%% acceptance threshold."),
            "impact": "Cost Impact: $156K | 280K gloves quarantined",
            "timestamp": "12 hours ago",
            "details": [
                ("Supplier", "Medline Industries"),
                ("Lot Number", "GL-2026-1142"),
                ("Defect Rate", "3.2% (limit: 1.5%)"),
                ("Action", "Lot quarantined, CAPA initiated"),
            ],
        },
        {
            "severity": "info",
            "title": "Infusion Pump Firmware Update Available",
            "description": ("Baxter SIGMA Spectrum pumps eligible for "
                            "firmware v4.8.2 addressing drug library sync "
                            "latency. Non-critical but recommended within "
                            "60-day window."),
            "impact": "218 pumps across 6 facilities",
            "timestamp": "1 day ago",
            "details": [
                ("Model", "Baxter SIGMA Spectrum"),
                ("Firmware", "v4.7.1 -> v4.8.2"),
                ("Deadline", "May 10, 2026"),
                ("Estimated Downtime", "12 min per unit"),
            ],
        },
    ]

    summary_kpis = [
        {"label": "Open Alerts", "value": "23", "accent": "red"},
        {"label": "Critical Items", "value": "5", "accent": "red"},
        {"label": "Avg Resolution", "value": "4.2 days", "accent": "yellow"},
        {"label": "Supply Risk Score", "value": "72 / 100", "accent": "blue"},
    ]

    # ── Build tab contents using new API ───────────────────────────────
    critical_cards = [alert_card(**a) for a in alerts
                      if a["severity"] == "critical"]
    warning_cards = [alert_card(**a) for a in alerts
                     if a["severity"] == "warning"]
    normal_cards = [alert_card(**a) for a in alerts
                    if a["severity"] == "info"] + [
        alert_card(severity="healthy",
                   title="PPE Inventory Levels — Fully Stocked",
                   description="All PPE categories (N95 masks, gloves, gowns, face shields) are above 90-day safety stock thresholds across all 14 hospitals and 38 clinics.",
                   timestamp="Updated daily"),
        alert_card(severity="healthy",
                   title="Pharmaceutical Distribution — On Schedule",
                   description="All scheduled pharmaceutical shipments for the current week have been delivered on time. Cold chain integrity verified for all temperature-sensitive items.",
                   timestamp="12 hours ago"),
        alert_card(severity="healthy",
                   title="Surgical Supply Contract Renewed — Ethicon",
                   description="Annual contract for sutures and wound closure products renewed with 3.1% volume discount. Delivery reliability at 99.4% over the past 12 months.",
                   timestamp="2 days ago"),
    ]

    tab_contents = [
        ("Critical", html.Div(critical_cards)),
        ("Warning", html.Div(warning_cards)),
        ("Normal", html.Div(normal_cards)),
    ]

    insight = insight_card(
        headline="Epinephrine Supply Critical",
        metric_value="5 days",
        direction="down",
        narrative="Epinephrine supply at 5-day coverage — well below 14-day safety threshold. Expedited order placed.",
        severity="critical",
    )

    return layout_alerts(
        title="Supply Chain & Manufacturing",
        subtitle="Drug shortages, equipment logistics, cold chain, and supplier monitoring",
        tab_contents=tab_contents,
        summary_kpis=summary_kpis,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. MEDTECH & DIGITAL SURGERY  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_medtech_surgery(cfg):
    """Split view of surgical outcomes, device utilization, and training
    metrics with tabs, info banner, and bottom stats."""

    banner_text = (
        "Robotic-assisted procedures show 23% reduction in average length "
        "of stay and 31% fewer post-operative complications compared to "
        "traditional approaches across Q4 2025 cohort (n=4,218)."
    )

    # ── TAB 1: Outcomes ──────────────────────────────────────────────
    procedures = ["Cardiac\nBypass", "Hip\nReplacement", "Spinal\nFusion",
                  "Knee\nArthroscopy", "Lap\nCholecystectomy",
                  "Robotic\nProstatectomy"]
    success_rates = [96.2, 97.8, 94.5, 98.1, 99.2, 97.4]
    complication_rates = [3.8, 2.2, 5.5, 1.9, 0.8, 2.6]

    outcome_fig = go.Figure()
    outcome_fig.add_trace(go.Bar(
        x=procedures, y=success_rates, name="Success Rate %",
        marker_color=COLORS["green"], opacity=0.85,
    ))
    outcome_fig.add_trace(go.Bar(
        x=procedures, y=complication_rates, name="Complication Rate %",
        marker_color=COLORS["red"], opacity=0.85,
    ))
    outcome_fig.update_layout(**dark_chart_layout(
        height=300, barmode="group",
        margin=dict(l=48, r=24, t=16, b=60),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"],
                   tickfont=dict(size=10)),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="%", range=[0, 105]),
        legend=dict(orientation="h", y=-0.25, x=0.5, xanchor="center"),
    ))
    outcomes_left = dcc.Graph(figure=outcome_fig, config=CHART_CONFIG,
                              style={"height": "300px"})

    # Average length of stay trend
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar"]
    fig_los = go.Figure()
    fig_los.add_trace(go.Scatter(
        x=months, y=[5.8, 5.6, 5.4, 5.2, 4.9, 4.7, 4.5, 4.3, 4.1],
        name="Robotic-Assisted", mode="lines+markers",
        line=dict(color=COLORS["green"], width=2), marker=dict(size=5),
    ))
    fig_los.add_trace(go.Scatter(
        x=months, y=[7.2, 7.1, 7.0, 6.9, 6.8, 6.8, 6.7, 6.6, 6.5],
        name="Traditional", mode="lines+markers",
        line=dict(color=COLORS["text_muted"], width=2, dash="dash"),
        marker=dict(size=4),
    ))
    fig_los.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Avg LOS (days)", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    outcomes_right = dcc.Graph(figure=fig_los, config=CHART_CONFIG,
                               style={"height": "300px"})

    tab_outcomes = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Procedure Outcomes by Type", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), outcomes_left], padding="20px"),
            _card([html.Div("Length of Stay: Robotic vs Traditional", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), outcomes_right], padding="20px"),
        ],
    )

    # ── TAB 2: Devices ───────────────────────────────────────────────
    device_labels = ["Da Vinci Xi", "Mako SmartRobotics",
                     "ROSA Spine", "Ion Bronchoscopy",
                     "Monarch Platform"]
    device_values = [34, 26, 18, 12, 10]
    device_colors = [COLORS["blue"], COLORS["green"], COLORS["purple"],
                     COLORS["yellow"], COLORS["red"]]
    device_fig = donut_figure(device_labels, device_values, device_colors,
                              center_text="2,847", title="Procedures by Device")
    devices_left = dcc.Graph(figure=device_fig, config=CHART_CONFIG,
                             style={"height": "300px"})

    # Device uptime & utilization bar chart
    devices = ["Da Vinci Xi", "Mako", "ROSA Spine", "Ion", "Monarch"]
    fig_uptime = go.Figure()
    fig_uptime.add_trace(go.Bar(
        x=devices, y=[99.4, 98.8, 97.2, 99.1, 96.5], name="Uptime %",
        marker_color=COLORS["green"],
    ))
    fig_uptime.add_trace(go.Bar(
        x=devices, y=[82, 76, 68, 71, 58], name="Utilization %",
        marker_color=COLORS["blue"],
    ))
    fig_uptime.update_layout(**dark_chart_layout(
        height=300, barmode="group",
        yaxis=dict(title="%", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], range=[0, 110]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
        margin=dict(l=48, r=24, t=16, b=48),
    ))
    devices_right = dcc.Graph(figure=fig_uptime, config=CHART_CONFIG,
                              style={"height": "300px"})

    tab_devices = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Procedures by Device Platform", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), devices_left], padding="20px"),
            _card([html.Div("Device Uptime & Utilization", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), devices_right], padding="20px"),
        ],
    )

    # ── TAB 3: Training ──────────────────────────────────────────────
    roles = ["Surgeons", "OR Nurses", "Technicians", "Residents", "Fellows"]
    fig_training = go.Figure()
    fig_training.add_trace(go.Bar(
        y=roles,
        x=[92, 88, 84, 72, 68],
        orientation="h", name="Certified %",
        marker_color=[COLORS["green"] if v >= 85 else COLORS["yellow"] if v >= 70
                      else COLORS["red"] for v in [92, 88, 84, 72, 68]],
        text=["92%", "88%", "84%", "72%", "68%"],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_training.add_vline(x=85, line_dash="dash", line_color=COLORS["green"],
                           opacity=0.5)
    fig_training.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        margin=dict(l=100, r=60, t=24, b=24),
        xaxis=dict(title="Certification %", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"],
                   range=[0, 105]),
        yaxis=dict(showgrid=False),
    ))
    training_left = dcc.Graph(figure=fig_training, config=CHART_CONFIG,
                              style={"height": "300px"})

    # Training hours trend
    qtrs = ["Q1 '25", "Q2 '25", "Q3 '25", "Q4 '25", "Q1 '26"]
    fig_hours = go.Figure()
    fig_hours.add_trace(go.Bar(
        x=qtrs, y=[1240, 1380, 1520, 1680, 1420], name="Simulation Hours",
        marker_color=COLORS["purple"],
    ))
    fig_hours.add_trace(go.Bar(
        x=qtrs, y=[680, 720, 810, 890, 760], name="Proctored Cases",
        marker_color=COLORS["blue"],
    ))
    fig_hours.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="Hours", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    training_right = dcc.Graph(figure=fig_hours, config=CHART_CONFIG,
                               style={"height": "300px"})

    tab_training = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Certification by Role", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), training_left], padding="20px"),
            _card([html.Div("Training Hours by Quarter", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), training_right], padding="20px"),
        ],
    )

    # ── Bottom stats ──────────────────────────────────────────────────
    bottom_stats = [
        ("Procedures YTD", "12,483", "blue"),
        ("Avg Op Time", "142 min", "purple"),
        ("Complication Rate", "2.4%", "green"),
        ("Training Certified", "89%", "blue"),
        ("Device Uptime", "99.1%", "green"),
    ]

    insight = insight_card(
        headline="Robotic-Assisted Procedures Outperforming",
        metric_value="-0.8%",
        direction="down",
        narrative="Robotic-assisted procedures showing 0.8% lower complication rate vs traditional methods.",
        severity="healthy",
    )

    return layout_split(
        title="MedTech & Digital Surgery",
        subtitle="Surgical outcomes, robotic device utilization, and training compliance",
        tab_contents=[
            ("Outcomes", tab_outcomes),
            ("Devices", tab_devices),
            ("Training", tab_training),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. PATIENT OUTCOMES  (Style B variant — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_patient_outcomes(cfg):
    """Patient outcomes table view with filters for department, condition,
    and timeframe, KPI strip, and outcomes table with progress bars."""

    # ── Filters ───────────────────────────────────────────────────────
    filters = [
        {"label": "Department", "options": ["All Departments", "Cardiology",
                                             "Oncology", "Orthopedics",
                                             "Neurology", "Pulmonology"]},
        {"label": "Condition", "options": ["All Conditions", "Heart Failure",
                                            "COPD", "Pneumonia", "Sepsis",
                                            "Stroke", "Hip Fracture"]},
        {"label": "Timeframe", "options": ["Last 12 Months", "Last 6 Months",
                                            "Last Quarter", "YTD"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────
    kpi_items = [
        {"label": "Mortality Rate", "value": "1.8%", "accent": "green"},
        {"label": "Readmission", "value": "8.2%", "accent": "blue"},
        {"label": "Satisfaction", "value": "92.4%", "accent": "blue"},
        {"label": "Avg LOS", "value": "4.1 days", "accent": "purple"},
        {"label": "Infection Rate", "value": "0.6%", "accent": "green"},
    ]

    # ── Outcomes table ────────────────────────────────────────────────
    columns = [
        {"name": "Condition", "id": "condition"},
        {"name": "Cases", "id": "cases"},
        {"name": "Mortality", "id": "mortality"},
        {"name": "Readmit %", "id": "readmit_pct"},
        {"name": "Avg LOS", "id": "avg_los"},
        {"name": "Recovery Score", "id": "recovery_score"},
        {"name": "Trend", "id": "trend"},
    ]

    condition_data = [
        ("Acute MI", "1,842", "2.1%", "9.4%", "5.2 days", 91,
         "up", "+2.3%"),
        ("Heart Failure", "2,364", "3.4%", "12.1%", "6.8 days", 78,
         "up", "+1.8%"),
        ("COPD Exacerbation", "1,567", "1.2%", "14.6%", "4.3 days", 82,
         "down", "-0.5%"),
        ("Community Pneumonia", "2,108", "1.8%", "7.2%", "3.9 days", 88,
         "up", "+3.1%"),
        ("Sepsis", "987", "8.7%", "6.3%", "9.4 days", 72,
         "up", "+4.2%"),
        ("Ischemic Stroke", "1,243", "4.2%", "8.8%", "7.1 days", 76,
         "up", "+1.4%"),
        ("Hip Fracture", "892", "1.9%", "5.1%", "5.6 days", 85,
         "up", "+2.7%"),
        ("Knee Replacement", "1,456", "0.3%", "2.8%", "2.4 days", 94,
         "up", "+1.1%"),
        ("Coronary Bypass", "634", "2.8%", "7.6%", "8.2 days", 83,
         "up", "+3.5%"),
        ("Appendectomy", "1,120", "0.1%", "1.4%", "1.8 days", 97,
         "up", "+0.4%"),
    ]

    data = []
    for (cond, cases, mort, readmit, los,
         recovery, trend_dir, trend_pct) in condition_data:
        trend_arrow = "\u2191" if trend_dir == "up" else "\u2193"
        data.append({
            "condition": cond,
            "cases": cases,
            "mortality": mort,
            "readmit_pct": readmit,
            "avg_los": los,
            "recovery_score": f"{recovery}%",
            "trend": f"{trend_arrow} {trend_pct}",
        })

    insight = insight_card(
        headline="Cardiac Care Readmission Improved",
        metric_value="-2.1pts",
        direction="down",
        narrative="Cardiac care readmission rate improved 2.1 points this quarter to 6.1%.",
        severity="healthy",
    )

    return layout_table(
        title="Patient Outcomes",
        subtitle="Condition-level outcome analysis with recovery scoring and trend tracking",
        filters=filters,
        kpi_items=kpi_items,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )
