"""
Page renderers for the Health & Life Sciences vertical of Blueprint IQ v2.

Covers four sub-verticals: Providers, Health Plans, BioPharma, and MedTech.
Each public ``render_*`` function accepts a ``cfg`` dict (parsed from
hls.yaml) and returns an ``html.Div`` that can be dropped into the
main content area.
"""

from dash import dcc, html
import plotly.graph_objects as go
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS

# ---------------------------------------------------------------------------
# Shared helper utilities
# ---------------------------------------------------------------------------

_ACCENT_ICONS = {
    "blue": "fa-chart-line",
    "purple": "fa-bolt",
    "green": "fa-arrow-trend-up",
    "red": "fa-triangle-exclamation",
    "yellow": "fa-circle-exclamation",
}


def _build_kpi_card(title, value_str, accent, icon, alert=False):
    accent_class = f"accent-{accent}"
    children = [
        html.Div(
            style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"},
            children=[
                html.Span(title, className="card-title"),
                html.I(className=f"fa-solid {icon}", style={"color": COLORS["text_muted"], "fontSize": "14px"}),
            ],
        ),
        html.Div(value_str, className=f"card-value {accent_class}"),
        html.Div("Live", className="card-subtitle"),
    ]
    if alert:
        children.insert(1, html.Div(
            style={"position": "absolute", "top": "12px", "right": "12px"},
            children=html.Span(
                "ALERT",
                style={
                    "fontSize": "9px", "fontWeight": "700", "color": COLORS["red"],
                    "backgroundColor": "rgba(239, 68, 68, 0.12)", "padding": "2px 6px",
                    "borderRadius": "4px", "letterSpacing": "0.5px",
                },
            ),
        ))
    return html.Div(className="card", style={"position": "relative"}, children=children)


def _build_table(headers, rows):
    th_style = {
        "padding": "10px 14px", "fontSize": "11px", "color": COLORS["text_muted"],
        "textAlign": "left", "borderBottom": f"1px solid {COLORS['border']}",
        "textTransform": "uppercase", "letterSpacing": "0.5px",
    }
    return html.Table(
        style={"width": "100%", "borderCollapse": "collapse"},
        children=[
            html.Thead(html.Tr([html.Th(h, style=th_style) for h in headers])),
            html.Tbody(rows),
        ],
    )


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
    return html.Td(
        html.Span(
            status_text,
            className="status-badge",
            style={"backgroundColor": sc["bg"], "color": sc["text"], "border": f"1px solid {sc['border']}"},
        ),
        style={"padding": "10px 14px"},
    )


def _detail_row(label, value):
    return html.Div(
        style={"display": "flex", "justifyContent": "space-between", "padding": "6px 0",
               "borderBottom": f"1px solid {COLORS['border']}"},
        children=[
            html.Span(label, style={"fontSize": "13px", "color": COLORS["text_muted"]}),
            html.Span(str(value), style={"fontSize": "13px", "fontWeight": "500"}),
        ],
    )


# ---------------------------------------------------------------------------
# 1. HLS Command Center (Dashboard)
# ---------------------------------------------------------------------------

def render_dashboard(cfg):
    """Render the HLS Command Center dashboard page."""

    # -- KPI cards --
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Bed Utilization", "87.3%", "blue", "fa-bed"),
        _build_kpi_card("Readmission Rate", "8.2%", "purple", "fa-rotate-left"),
        _build_kpi_card("Star Rating", "4.2", "green", "fa-star"),
        _build_kpi_card("Claims FWA Rate", "4.7%", "red", "fa-triangle-exclamation", alert=True),
    ])

    # -- Key metrics by sub-vertical bar chart --
    sub_verticals = ["Providers", "Health Plans", "BioPharma", "MedTech"]
    metric_values = [87.3, 84.2, 78.0, 96.8]

    fig = go.Figure(
        data=[
            go.Bar(
                x=sub_verticals,
                y=metric_values,
                marker_color=[COLORS["blue"], COLORS["purple"], COLORS["green"], COLORS["yellow"]],
                text=[f"{v}%" for v in metric_values],
                textposition="outside",
                textfont=dict(color=COLORS["white"], size=12),
            ),
        ],
    )
    fig.update_layout(
        title=dict(text="Key Metrics by Sub-Vertical", font=dict(size=14, color=COLORS["white"])),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT_FAMILY, color=COLORS["text_muted"]),
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        yaxis=dict(
            showgrid=True, gridcolor=COLORS["border"], color=COLORS["text_muted"],
            title="Performance (%)",
        ),
        margin=dict(l=48, r=24, t=48, b=40),
        height=320,
    )

    chart_card = html.Div(className="card", children=[
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ])

    # -- Cross-Sub-Vertical Summary table --
    summary_data = [
        ("Providers",    "Bed Util 87.3%", "Good",         "Stable",    "Healthy"),
        ("Health Plans", "MLR 84.2%",      "Good",         "Improving", "Healthy"),
        ("BioPharma",    "Trial Enrollment 78%", "Below Target", "Declining", "Low"),
        ("MedTech",      "Mfg Yield 96.8%", "Excellent",   "Stable",    "Healthy"),
    ]

    summary_rows = []
    for sv, metric, perf, trend, status in summary_data:
        trend_color = COLORS["green"] if trend == "Improving" else (COLORS["red"] if trend == "Declining" else None)
        summary_rows.append(html.Tr([
            _td(sv, bold=True),
            _td(metric, mono=True),
            _td(perf),
            _td(trend, color=trend_color),
            _status_td(status),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Cross-Sub-Vertical Summary", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Sub-Vertical", "Key Metric", "Performance", "Trend", "Status"], summary_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("HLS Command Center"),
            html.P("Real-time operational overview across Providers, Health Plans, BioPharma, and MedTech"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            chart_card,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 2. Provider Operations
# ---------------------------------------------------------------------------

def render_provider_ops(cfg):
    """Render the Provider Operations page."""

    # -- KPI cards --
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Bed Utilization", "87.3%", "blue", "fa-bed"),
        _build_kpi_card("ED Wait Time", "34 min", "purple", "fa-clock"),
        _build_kpi_card("Avg LOS", "4.2 days", "green", "fa-calendar-days"),
        _build_kpi_card("Denial Rate", "6.8%", "red", "fa-ban"),
    ])

    # -- Facility Operations table --
    facility_data = [
        ("Metro General", 142, "28 min", "91.2%", "3.8 d", "5.4%"),
        ("Westside",       98, "38 min", "84.1%", "4.4 d", "7.2%"),
        ("Eastview",       45, "22 min", "72.5%", "4.8 d", "8.1%"),
    ]

    facility_rows = []
    for name, admissions, ed_wait, bed_util, los, denial in facility_data:
        facility_rows.append(html.Tr([
            _td(name, bold=True),
            _td(admissions, mono=True),
            _td(ed_wait, mono=True),
            _td(bed_util, mono=True, color=COLORS["green"]),
            _td(los, mono=True),
            _td(denial, mono=True, color=COLORS["yellow"]),
        ]))

    facility_card = html.Div(className="card", children=[
        html.H3("Facility Operations", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Facility", "Admissions", "ED Wait", "Bed Util", "LOS", "Denial Rate"], facility_rows),
    ])

    # -- Revenue Cycle Summary card --
    rev_cycle_card = html.Div(className="card", children=[
        html.H3("Revenue Cycle Summary", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _detail_row("A/R Days", "42"),
        _detail_row("Clean Claim Rate", "91%"),
        _detail_row("Denial Rate", "6.8%"),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Provider Operations"),
            html.P("Facility throughput, emergency department performance, and revenue cycle metrics"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            facility_card,
            rev_cycle_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 3. Clinical Quality & Outcomes
# ---------------------------------------------------------------------------

def render_clinical_quality(cfg):
    """Render the Clinical Quality & Outcomes page."""

    # -- KPI cards --
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("HCAHPS Score", "82/100", "blue", "fa-hospital"),
        _build_kpi_card("Readmission Rate", "8.2%", "purple", "fa-rotate-left"),
        _build_kpi_card("Sepsis Compliance", "91%", "green", "fa-shield-virus"),
        _build_kpi_card("HAC Rate", "1.2%", "red", "fa-triangle-exclamation"),
    ])

    # -- Quality Measures table --
    quality_data = [
        ("Sepsis Bundle",    "91%",  "90%",  "Above", "Improving"),
        ("HEDIS Diabetes",   "84%",  "85%",  "Below", "Stable"),
        ("CMS Star Rating",  "4.2",  "4.0",  "Above", "Improving"),
        ("HAC Prevention",   "98.8%", "98%", "Above", "Stable"),
        ("Readmission",      "8.2%", "9.0%", "Above", "Improving"),
    ]

    quality_rows = []
    for measure, compliance, benchmark, vs_bench, trend in quality_data:
        vs_color = COLORS["green"] if vs_bench == "Above" else COLORS["red"]
        trend_color = COLORS["green"] if trend == "Improving" else None
        quality_rows.append(html.Tr([
            _td(measure, bold=True),
            _td(compliance, mono=True),
            _td(benchmark, mono=True),
            _td(vs_bench, color=vs_color),
            _td(trend, color=trend_color),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Quality Measures", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Measure", "Compliance", "Benchmark", "vs Benchmark", "Trend"], quality_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Clinical Quality & Outcomes"),
            html.P("HCAHPS scores, readmission rates, sepsis compliance, and hospital-acquired conditions"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 4. Health Plan Analytics
# ---------------------------------------------------------------------------

def render_health_plans(cfg):
    """Render the Health Plan Analytics page."""

    # -- KPI cards --
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("MLR", "84.2%", "blue", "fa-chart-pie"),
        _build_kpi_card("Claims Processing", "4.8 days", "purple", "fa-file-invoice"),
        _build_kpi_card("Prior Auth", "18 hrs", "green", "fa-clock"),
        _build_kpi_card("FWA Detection", "4.7%", "red", "fa-triangle-exclamation"),
    ])

    # -- Plan Performance table --
    plan_data = [
        ("HMO",                "2.4M", "82.1%", "4,200", "3.2 d", "4.4"),
        ("PPO",                "3.8M", "86.4%", "6,800", "5.1 d", "4.0"),
        ("Medicare Advantage", "1.2M", "85.2%", "3,100", "4.4 d", "4.2"),
        ("Medicaid",           "0.8M", "88.1%", "2,400", "6.2 d", "3.8"),
    ]

    plan_rows = []
    for plan, members, mlr, claims_day, proc_time, star in plan_data:
        plan_rows.append(html.Tr([
            _td(plan, bold=True),
            _td(members, mono=True),
            _td(mlr, mono=True, color=COLORS["green"]),
            _td(claims_day, mono=True),
            _td(proc_time, mono=True),
            _td(star, mono=True, color=COLORS["purple"]),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Plan Performance", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Plan Type", "Members", "MLR", "Claims/Day", "Processing Time", "Star Rating"], plan_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("Health Plan Analytics"),
            html.P("Medical loss ratios, claims processing efficiency, prior authorization, and fraud detection"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 5. BioPharma Intelligence
# ---------------------------------------------------------------------------

def render_biopharma(cfg):
    """Render the BioPharma Intelligence page."""

    # -- KPI cards --
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Trial Enrollment", "78%", "blue", "fa-flask"),
        _build_kpi_card("Time to Market", "14.2 mo", "purple", "fa-clock"),
        _build_kpi_card("Mfg Yield", "94.6%", "green", "fa-industry"),
        _build_kpi_card("HCP Engagement", "3.8/5", "yellow", "fa-user-doctor"),
    ])

    # -- Clinical Trials table --
    trial_data = [
        ("ONCO-2024",   "Phase III", "Oncology",    "82%", 42, "2.1%"),
        ("CARDIO-2024", "Phase II",  "Cardiology",  "74%", 28, "1.8%"),
        ("NEURO-2025",  "Phase I",   "Neurology",   "68%", 14, "3.2%"),
        ("IMMUNO-2024", "Phase III", "Immunology",  "91%", 56, "1.4%"),
    ]

    trial_rows = []
    for trial, phase, area, enrollment, sites, ae_rate in trial_data:
        enroll_val = int(enrollment.replace("%", ""))
        enroll_color = COLORS["green"] if enroll_val >= 80 else (COLORS["yellow"] if enroll_val >= 70 else COLORS["red"])
        trial_rows.append(html.Tr([
            _td(trial, bold=True),
            _td(phase),
            _td(area),
            _td(enrollment, mono=True, color=enroll_color),
            _td(sites, mono=True),
            _td(ae_rate, mono=True),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Clinical Trials", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Trial", "Phase", "Therapeutic Area", "Enrollment %", "Sites", "AE Rate"], trial_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("BioPharma Intelligence"),
            html.P("Clinical trial enrollment, time-to-market tracking, manufacturing yield, and HCP engagement"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])


# ---------------------------------------------------------------------------
# 6. MedTech & Supply Chain
# ---------------------------------------------------------------------------

def render_medtech(cfg):
    """Render the MedTech & Supply Chain page."""

    # -- KPI cards --
    kpi_cards = html.Div(className="grid-4", style={"marginBottom": "16px"}, children=[
        _build_kpi_card("Pipeline Value", "$1.2B", "blue", "fa-chart-line"),
        _build_kpi_card("Mfg Yield", "96.8%", "green", "fa-industry"),
        _build_kpi_card("Field Service", "4.2 hrs", "purple", "fa-wrench"),
        _build_kpi_card("Procedure Growth", "12% YoY", "yellow", "fa-arrow-trend-up"),
    ])

    # -- Product Portfolio table --
    product_data = [
        ("Surgical",       42, "Launched",     "97.2%", 3,  "14%"),
        ("Diagnostic",     28, "Launched",     "96.4%", 5,  "11%"),
        ("Implant",        18, "Regulatory",   "95.8%", 8,  "8%"),
        ("Digital Health", 12, "Development",  "98.1%", 2,  "22%"),
    ]

    product_rows = []
    for category, products, stage, yield_pct, capa, rev_growth in product_data:
        growth_val = int(rev_growth.replace("%", ""))
        growth_color = COLORS["green"] if growth_val >= 10 else COLORS["yellow"]
        product_rows.append(html.Tr([
            _td(category, bold=True),
            _td(products, mono=True),
            _td(stage),
            _td(yield_pct, mono=True, color=COLORS["green"]),
            _td(capa, mono=True, color=COLORS["yellow"] if capa >= 5 else None),
            _td(rev_growth, mono=True, color=growth_color),
        ]))

    table_card = html.Div(className="card", children=[
        html.H3("Product Portfolio", style={
            "fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "16px",
        }),
        _build_table(["Category", "Products", "Pipeline Stage", "Yield", "CAPA Open", "Revenue Growth"], product_rows),
    ])

    return html.Div([
        html.Div(className="page-header", children=[
            html.H1("MedTech & Supply Chain"),
            html.P("Product pipeline, manufacturing yield, field service response, and procedure volume growth"),
        ]),
        html.Div(className="content-area", children=[
            kpi_cards,
            table_card,
        ]),
    ])
