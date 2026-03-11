"""Telecommunications vertical page renderers.

Provides seven public render functions for the Telecom dashboard:
  1. render_dashboard        -- layout_executive  (Style A)
  2. render_consumer_cx      -- layout_split      (Style C)
  3. render_b2b_enterprise   -- layout_forecast   (Style E)
  4. render_network_ops      -- layout_grid        (Style F)
  5. render_field_energy      -- layout_table       (Style B)
  6. render_fraud_prevention  -- layout_alerts      (Style D)
  7. render_cyber_security    -- layout_grid        (Style F variant)
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
    _card, _hex_to_rgb,
)
from app.theme import COLORS, FONT_FAMILY
from dash import dcc, html
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
#  1. TELECOM OUTCOME HUB  (Style A -- layout_executive)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive dashboard with subscriber, NPS, and uptime heroes,
    a subscriber-growth line chart, and two bottom panels (revenue donut
    + churn trend)."""

    # ── Hero metrics ──────────────────────────────────────────────────
    heroes = [
        hero_metric("Total Subscribers", "48.7M",
                     trend_text="+3.2% YoY", trend_dir="up", accent="blue"),
        hero_metric("Net Promoter Score", "62",
                     trend_text="+4 pts vs Q3", trend_dir="up", accent="green"),
        hero_metric("Network Uptime", "99.97%",
                     trend_text="0.01% improvement", trend_dir="up", accent="purple"),
    ]

    # ── Main chart: subscriber growth (18 months) ─────────────────────
    months = ["Jan '25", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan '26", "Feb", "Mar"]
    subs = [44.1, 44.5, 44.9, 45.2, 45.6, 46.0,
            46.3, 46.8, 47.1, 47.5, 47.8, 48.0,
            48.2, 48.5, 48.7]

    fig_growth = go.Figure()
    fig_growth.add_trace(go.Scatter(
        x=months, y=subs, mode="lines+markers",
        name="Subscribers (M)",
        line=dict(color=COLORS["blue"], width=3),
        marker=dict(size=5, color=COLORS["blue"]),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_growth.update_layout(**dark_chart_layout(
        height=320,
        title=dict(text="Subscriber Growth", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Subscribers (M)", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
    ))

    main_chart = dcc.Graph(figure=fig_growth, config=CHART_CONFIG,
                           style={"width": "100%"})

    # ── Bottom panel 1: revenue by segment donut ──────────────────────
    rev_labels = ["Consumer Mobile", "Enterprise", "Broadband", "Wholesale", "IoT"]
    rev_values = [4200, 2800, 1900, 1100, 650]
    rev_colors = [COLORS["blue"], COLORS["purple"], COLORS["green"],
                  COLORS["yellow"], COLORS["red"]]
    fig_rev = donut_figure(rev_labels, rev_values, rev_colors,
                           center_text="$10.7B", title="Revenue by Segment")
    panel_rev = dcc.Graph(figure=fig_rev, config=CHART_CONFIG)

    # ── Bottom panel 2: churn trend line chart ────────────────────────
    churn_months = ["Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"]
    churn_pct = [1.42, 1.38, 1.35, 1.31, 1.28, 1.25, 1.21]

    fig_churn = go.Figure()
    fig_churn.add_trace(go.Scatter(
        x=churn_months, y=churn_pct, mode="lines+markers",
        name="Monthly Churn %",
        line=dict(color=COLORS["red"], width=2, dash="dot"),
        marker=dict(size=6, color=COLORS["red"]),
    ))
    fig_churn.update_layout(**dark_chart_layout(
        height=280,
        title=dict(text="Churn Trend", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Churn %", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"],
                   range=[1.0, 1.6]),
    ))
    panel_churn = dcc.Graph(figure=fig_churn, config=CHART_CONFIG)

    # ── Assemble ──────────────────────────────────────────────────────
    return layout_executive(
        title="Telecom Outcome Hub",
        subtitle="Enterprise-wide KPIs and performance trends",
        heroes=heroes,
        main_chart=main_chart,
        panels=[
            ("Revenue by Segment", panel_rev),
            ("Churn Trend", panel_churn),
        ],
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. CONSUMER CX & GROWTH  (Style C -- layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_consumer_cx(cfg):
    """Split view with satisfaction bar chart, complaint donut, tabs,
    info banner, and bottom stat cards."""

    # ── Left panel: satisfaction by touchpoint (bar chart) ────────────
    touchpoints = ["Store Visit", "Call Center", "App", "Web Chat",
                   "Social Media", "Self-Service", "Field Tech"]
    scores = [82, 71, 88, 76, 69, 84, 79]
    bar_colors = [COLORS["green"] if s >= 80 else
                  COLORS["yellow"] if s >= 70 else COLORS["red"]
                  for s in scores]

    fig_sat = go.Figure()
    fig_sat.add_trace(go.Bar(
        x=touchpoints, y=scores,
        marker=dict(color=bar_colors, cornerradius=4),
        text=scores, textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_sat.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="CSAT Score", range=[0, 100],
                   showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        xaxis=dict(tickangle=-30, color=COLORS["text_muted"]),
    ))
    left_chart = dcc.Graph(figure=fig_sat, config=CHART_CONFIG)

    # ── Right panel: complaint category donut ─────────────────────────
    comp_labels = ["Billing Errors", "Network Coverage", "Speed Issues",
                   "Customer Service", "Contract Disputes", "Other"]
    comp_values = [28, 22, 18, 15, 10, 7]
    comp_colors = [COLORS["red"], COLORS["yellow"], COLORS["purple"],
                   COLORS["blue"], COLORS["green"], COLORS["text_muted"]]
    fig_comp = donut_figure(comp_labels, comp_values, comp_colors,
                            center_text="4.2K", title="Complaint Categories")
    right_chart = dcc.Graph(figure=fig_comp, config=CHART_CONFIG)

    # ── Bottom stats ──────────────────────────────────────────────────
    bottom = [
        ("Avg CSAT", "78.4", "blue"),
        ("First Contact Res.", "68%", "green"),
        ("Avg Handle Time", "6.2 min", "yellow"),
        ("Escalation Rate", "11.3%", "red"),
    ]

    return layout_split(
        title="Consumer CX & Growth",
        subtitle="Customer satisfaction, journey analytics, and NPS drivers",
        tabs=["Satisfaction", "Journey", "NPS"],
        banner_text=(
            "CX Insight: App channel satisfaction rose 5 pts after the Q1 UX "
            "refresh. Call center remains the lowest-scoring touchpoint -- "
            "consider AI-assisted routing to reduce wait times."
        ),
        left_panel=("Satisfaction by Touchpoint", left_chart),
        right_panel=("Complaint Breakdown", right_chart),
        bottom_stats=bottom,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. B2B / SMB / ENTERPRISE  (Style E -- layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_b2b_enterprise(cfg):
    """Forecast layout with KPI strip, hero quarterly revenue, dual-axis
    chart (revenue + win rate), and side breakdown of revenue by service."""

    # ── KPIs ──────────────────────────────────────────────────────────
    kpis = [
        {"label": "Enterprise Revenue", "value": "$168M", "accent": "blue"},
        {"label": "Deal Pipeline", "value": "$94M", "accent": "purple"},
        {"label": "Win Rate", "value": "38.5%", "accent": "green"},
        {"label": "Avg Contract Value", "value": "$2.4M", "accent": "yellow"},
    ]

    # ── Dual-axis chart: quarterly revenue bars + win-rate line ───────
    quarters = ["Q2 '24", "Q3 '24", "Q4 '24", "Q1 '25", "Q2 '25",
                "Q3 '25", "Q4 '25", "Q1 '26"]
    revenue = [34, 36, 38, 39, 40, 41, 42, 42]
    win_rate = [32.1, 33.5, 34.8, 35.6, 36.2, 37.0, 37.8, 38.5]

    fig_dual = go.Figure()
    fig_dual.add_trace(go.Bar(
        x=quarters, y=revenue, name="Revenue ($M)",
        marker=dict(color=COLORS["blue"], cornerradius=4),
        yaxis="y",
    ))
    fig_dual.add_trace(go.Scatter(
        x=quarters, y=win_rate, name="Win Rate %",
        mode="lines+markers",
        line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6, color=COLORS["green"]),
        yaxis="y2",
    ))
    fig_dual.update_layout(**dark_chart_layout(
        height=280,
        yaxis=dict(title="Revenue ($M)", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        yaxis2=dict(title="Win Rate %", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"],
                    range=[25, 45]),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_dual, config=CHART_CONFIG)

    # ── Side panel: revenue breakdown by service ──────────────────────
    side = breakdown_list([
        {"label": "MPLS / SD-WAN", "value": "$52M", "pct": 31, "color": COLORS["blue"]},
        {"label": "Managed Security", "value": "$38M", "pct": 23, "color": COLORS["purple"]},
        {"label": "Unified Comms", "value": "$29M", "pct": 17, "color": COLORS["green"]},
        {"label": "Cloud Connect", "value": "$24M", "pct": 14, "color": COLORS["yellow"]},
        {"label": "IoT Solutions", "value": "$15M", "pct": 9, "color": COLORS["red"]},
        {"label": "Professional Svcs", "value": "$10M", "pct": 6, "color": COLORS["text_muted"]},
    ])

    return layout_forecast(
        title="B2B / Enterprise",
        subtitle="Enterprise revenue pipeline, win rates, and service mix",
        kpi_items=kpis,
        hero_value="$42M",
        hero_label="Q1 2026 Enterprise Revenue",
        hero_trend_text="+8.3% vs prior quarter",
        main_chart=main_chart,
        side_component=html.Div([
            html.Div("Revenue by Service",
                     style={"fontSize": "14px", "fontWeight": "600",
                            "color": COLORS["white"], "marginBottom": "16px"}),
            side,
        ]),
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. NETWORK OPERATIONS  (Style F -- layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_network_ops(cfg):
    """Multi-panel grid with gauge, sparkline, and assorted metric cards
    covering availability, latency, throughput, cell sites, incidents,
    and SLA compliance."""

    # ── Panel 1: Network Availability gauge ───────────────────────────
    fig_avail = gauge_figure(99.97, 100, title="Network Availability %",
                             color=COLORS["green"])
    panel_avail = html.Div([
        dcc.Graph(figure=fig_avail, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div("Target: 99.95%",
                 style={"textAlign": "center", "fontSize": "11px",
                        "color": COLORS["text_muted"], "marginTop": "4px"}),
    ])

    # ── Panel 2: Latency sparkline ────────────────────────────────────
    latency_vals = [12, 14, 11, 13, 15, 12, 10, 11, 13, 14, 12, 11,
                    10, 9, 11, 12, 10, 9, 10, 11, 10, 9, 8, 10]
    panel_latency = metric_with_sparkline(
        "Avg Latency", "10 ms", latency_vals, accent="blue",
    )

    # ── Panel 3: Throughput metric ────────────────────────────────────
    throughput_vals = [320, 335, 340, 350, 345, 360, 370, 365,
                      380, 390, 385, 395, 400, 410, 405, 420]
    panel_throughput = metric_with_sparkline(
        "Avg Throughput", "420 Gbps", throughput_vals, accent="purple",
    )

    # ── Panel 4: Cell Sites Active ────────────────────────────────────
    panel_cells = html.Div([
        html.Div("Cell Sites Active",
                 style={"fontSize": "11px", "color": COLORS["text_muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.4px",
                        "marginBottom": "4px"}),
        html.Div("72,841",
                 style={"fontSize": "28px", "fontWeight": "700",
                        "color": COLORS["green"], "marginBottom": "8px"}),
        html.Div(
            style={"display": "flex", "gap": "16px", "marginTop": "12px"},
            children=[
                html.Div([
                    html.Div("5G NR", style={"fontSize": "11px",
                                              "color": COLORS["text_muted"]}),
                    html.Div("18,420", style={"fontSize": "16px",
                                               "fontWeight": "600",
                                               "color": COLORS["blue"]}),
                ]),
                html.Div([
                    html.Div("4G LTE", style={"fontSize": "11px",
                                               "color": COLORS["text_muted"]}),
                    html.Div("41,316", style={"fontSize": "16px",
                                               "fontWeight": "600",
                                               "color": COLORS["purple"]}),
                ]),
                html.Div([
                    html.Div("3G/Other", style={"fontSize": "11px",
                                                 "color": COLORS["text_muted"]}),
                    html.Div("13,105", style={"fontSize": "16px",
                                               "fontWeight": "600",
                                               "color": COLORS["text_muted"]}),
                ]),
            ],
        ),
    ])

    # ── Panel 5: Incident Count ───────────────────────────────────────
    panel_incidents = html.Div([
        html.Div("Active Incidents",
                 style={"fontSize": "11px", "color": COLORS["text_muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.4px",
                        "marginBottom": "4px"}),
        html.Div("23",
                 style={"fontSize": "28px", "fontWeight": "700",
                        "color": COLORS["red"], "marginBottom": "8px"}),
        progress_row("P1 Critical", "3", 13, COLORS["red"]),
        progress_row("P2 Major", "8", 35, COLORS["yellow"]),
        progress_row("P3 Minor", "12", 52, COLORS["blue"]),
    ])

    # ── Panel 6: SLA Compliance ───────────────────────────────────────
    fig_sla = gauge_figure(98.4, 100, title="SLA Compliance %",
                           color=COLORS["blue"])
    panel_sla = html.Div([
        dcc.Graph(figure=fig_sla, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "marginTop": "8px"},
            children=[
                html.Span("Target: 98.0%",
                           style={"fontSize": "11px",
                                  "color": COLORS["text_muted"]}),
                html.Span("Met",
                           style={"fontSize": "11px", "fontWeight": "600",
                                  "color": COLORS["green"]}),
            ],
        ),
    ])

    grid_items = [
        {"col_span": 1, "row_span": 1, "content": panel_avail},
        {"col_span": 1, "row_span": 1, "content": panel_latency},
        {"col_span": 1, "row_span": 2, "content": panel_cells},
        {"col_span": 1, "row_span": 1, "content": panel_throughput},
        {"col_span": 1, "row_span": 1, "content": panel_incidents},
        {"col_span": 1, "row_span": 1, "content": panel_sla},
    ]

    return layout_grid(
        title="Network Operations",
        subtitle="Real-time network health, capacity, and incident monitoring",
        grid_items=grid_items,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. FIELD OPS & ENERGY  (Style B -- layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_field_energy(cfg):
    """Table view with filters, KPI strip, and a rich work-order table
    containing status badges and priority progress bars."""

    # ── Filters ───────────────────────────────────────────────────────
    filters = [
        {"label": "Region", "options": ["All Regions", "Northeast", "Southeast",
                                         "Midwest", "West", "Southwest"]},
        {"label": "Priority", "options": ["All", "P1", "P2", "P3", "P4"]},
        {"label": "Type", "options": ["All Types", "Install", "Repair",
                                       "Maintenance", "Upgrade", "Emergency"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────
    kpis = [
        {"label": "Open Tickets", "value": "1,247", "accent": "blue"},
        {"label": "SLA Met", "value": "94.2%", "accent": "green"},
        {"label": "First Fix Rate", "value": "78%", "accent": "purple"},
        {"label": "Active Technicians", "value": "3,842", "accent": "yellow"},
    ]

    # ── Work order table rows ─────────────────────────────────────────
    orders = [
        ("WO-20261042", "Cell Tower Repair", "Northeast", "P1",
         "In Progress", 72, COLORS["red"]),
        ("WO-20261038", "Fiber Splice - Main St Hub", "Southeast", "P2",
         "Dispatched", 55, COLORS["yellow"]),
        ("WO-20261035", "5G Antenna Install", "West", "P2",
         "In Progress", 88, COLORS["blue"]),
        ("WO-20261031", "Customer Premise Install", "Midwest", "P3",
         "Scheduled", 30, COLORS["blue"]),
        ("WO-20261027", "Generator Maintenance", "Southwest", "P3",
         "In Progress", 65, COLORS["green"]),
        ("WO-20261024", "Cabinet Battery Replace", "Northeast", "P4",
         "Completed", 100, COLORS["green"]),
        ("WO-20261019", "Microwave Link Align", "West", "P2",
         "In Progress", 40, COLORS["yellow"]),
        ("WO-20261015", "Emergency Power Restore", "Southeast", "P1",
         "Dispatched", 15, COLORS["red"]),
        ("WO-20261011", "Backhaul Upgrade", "Midwest", "P3",
         "Scheduled", 0, COLORS["text_muted"]),
        ("WO-20261007", "Small Cell Deploy", "Southwest", "P2",
         "In Progress", 60, COLORS["purple"]),
    ]

    table_columns = [
        {"name": h, "id": h.lower().replace(" ", "_")}
        for h in ["WO ID", "Description", "Region", "Priority",
                   "Status", "SLA Progress"]
    ]
    table_data = [
        {
            "wo_id": wo_id,
            "description": desc,
            "region": region,
            "priority": priority,
            "status": status,
            "sla_progress": f"{pct}%",
        }
        for wo_id, desc, region, priority, status, pct, color in orders
    ]

    return layout_table(
        title="Field Ops & Energy",
        subtitle="Work order management, SLA tracking, and technician dispatch",
        filters=filters,
        kpi_items=kpis,
        table_columns=table_columns,
        table_data=table_data,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. FRAUD PREVENTION  (Style D -- layout_alerts)
# ═══════════════════════════════════════════════════════════════════════════

def render_fraud_prevention(cfg):
    """Alert card layout with tabs for active/investigating/resolved,
    summary KPIs, and detailed alert cards for SIM swap, billing, and
    roaming fraud."""

    # ── Summary KPIs ──────────────────────────────────────────────────
    summary = [
        {"label": "Active Alerts", "value": "47", "accent": "red"},
        {"label": "Under Investigation", "value": "23", "accent": "yellow"},
        {"label": "Blocked (MTD)", "value": "1,842", "accent": "green"},
        {"label": "Est. Savings", "value": "$4.6M", "accent": "blue"},
    ]

    # ── Alert cards ───────────────────────────────────────────────────
    alerts = [
        {
            "severity": "critical",
            "title": "SIM Swap Fraud Cluster Detected",
            "description": (
                "Coordinated SIM swap activity detected across 14 accounts in "
                "the Southeast region. Pattern matches organized fraud ring "
                "behavior with rapid sequential port-out requests."
            ),
            "impact": "Financial Impact: $287K estimated",
            "timestamp": "12 min ago",
            "details": [
                ("Affected Accounts", "14"),
                ("Region", "Southeast -- Atlanta, Miami, Charlotte"),
                ("Attack Vector", "Social engineering + dealer portal"),
                ("Risk Score", "97 / 100"),
                ("IMSI Changes", "14 in last 45 min"),
            ],
        },
        {
            "severity": "critical",
            "title": "Billing System Anomaly -- Revenue Leakage",
            "description": (
                "Automated anomaly detection flagged abnormal CDR patterns "
                "indicating potential revenue bypass. Approximately 3,200 calls "
                "routed through a compromised interconnect gateway are not "
                "generating billing records."
            ),
            "impact": "Revenue Leakage: $142K / day",
            "timestamp": "38 min ago",
            "details": [
                ("Gateway ID", "GW-ATL-0042"),
                ("Unbilled CDRs", "3,200+ daily"),
                ("Duration", "Ongoing ~72 hours"),
                ("Carrier", "Interconnect Partner C"),
                ("Pattern", "Zero-rated international trunk"),
            ],
        },
        {
            "severity": "warning",
            "title": "International Roaming Fraud -- Premium Rate",
            "description": (
                "Spike in premium-rate SMS origination from roaming subscribers "
                "in Eastern Europe. 8 devices generating abnormal volumes "
                "consistent with IRSF (International Revenue Share Fraud)."
            ),
            "impact": "Financial Impact: $68K accrued",
            "timestamp": "1 hr ago",
            "details": [
                ("Devices Flagged", "8"),
                ("Roaming Partner", "Operator EU-East-12"),
                ("Premium Destinations", "Moldova, Latvia, Bosnia"),
                ("SMS Volume", "24,000 msgs in 6 hrs"),
                ("Avg Cost / msg", "$2.84"),
            ],
        },
        {
            "severity": "warning",
            "title": "Subscription Fraud -- Synthetic Identity",
            "description": (
                "Credit-check bypass detected for 6 new enterprise accounts "
                "opened in the last 48 hours. Identity verification scores "
                "are unusually clustered, suggesting synthetic identities."
            ),
            "impact": "Potential Exposure: $180K device subsidy",
            "timestamp": "2 hr ago",
            "details": [
                ("Accounts Flagged", "6"),
                ("Devices Acquired", "42 handsets"),
                ("Avg Credit Score", "710 (synthetic cluster)"),
                ("Dealer Channel", "Online -- self-serve"),
            ],
        },
        {
            "severity": "info",
            "title": "Wangiri Callback Scheme -- Low Volume",
            "description": (
                "Low-volume Wangiri (one-ring) activity detected from a block "
                "of Togolese numbers targeting prepaid subscribers. Auto-block "
                "rule engaged; no customer impact so far."
            ),
            "impact": "Blocked -- $0 customer impact",
            "timestamp": "3 hr ago",
            "details": [
                ("Source Range", "+228 9XXX XXXX"),
                ("Calls Attempted", "340"),
                ("Calls Blocked", "340 (100%)"),
                ("Callback Attempts", "0"),
            ],
        },
    ]

    return layout_alerts(
        title="Fraud Prevention",
        subtitle="Real-time fraud detection, investigation queue, and financial impact",
        tabs=["Active Alerts", "Investigating", "Resolved"],
        alerts=alerts,
        summary_kpis=summary,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. CYBER SECURITY  (Style F variant -- layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_cyber_security(cfg):
    """Grid layout variant with threat-level gauge, attack-trend sparkline,
    and metric panels for blocked attacks, firewall rules, vulnerabilities,
    and SIEM alerts."""

    # ── Panel 1: Threat Level gauge ───────────────────────────────────
    fig_threat = gauge_figure(72, 100, title="Threat Level Index",
                              color=COLORS["yellow"])
    panel_threat = html.Div([
        dcc.Graph(figure=fig_threat, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "center",
                   "gap": "16px", "marginTop": "4px"},
            children=[
                html.Span("Elevated",
                           style={"fontSize": "12px", "fontWeight": "600",
                                  "color": COLORS["yellow"]}),
                html.Span("|", style={"color": COLORS["border"]}),
                html.Span("Updated 2 min ago",
                           style={"fontSize": "11px",
                                  "color": COLORS["text_muted"]}),
            ],
        ),
    ])

    # ── Panel 2: Attack Trend sparkline ───────────────────────────────
    attack_hourly = [120, 145, 132, 198, 210, 185, 240, 310, 275,
                     420, 380, 295, 260, 230, 200, 190, 175, 160,
                     155, 180, 210, 245, 220, 195]
    panel_attacks = metric_with_sparkline(
        "Attacks / Hour", "195", attack_hourly, accent="red",
    )

    # ── Panel 3: Blocked Attacks ──────────────────────────────────────
    panel_blocked = html.Div([
        html.Div("Blocked Attacks (24h)",
                 style={"fontSize": "11px", "color": COLORS["text_muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.4px",
                        "marginBottom": "4px"}),
        html.Div("48,291",
                 style={"fontSize": "28px", "fontWeight": "700",
                        "color": COLORS["green"], "marginBottom": "12px"}),
        progress_row("DDoS Mitigation", "18,420", 38, COLORS["red"]),
        progress_row("Intrusion Attempts", "14,830", 31, COLORS["yellow"]),
        progress_row("Malware / Phishing", "9,612", 20, COLORS["purple"]),
        progress_row("Brute Force", "5,429", 11, COLORS["blue"]),
    ])

    # ── Panel 4: Firewall Rules ───────────────────────────────────────
    panel_firewall = html.Div([
        html.Div("Active Firewall Rules",
                 style={"fontSize": "11px", "color": COLORS["text_muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.4px",
                        "marginBottom": "4px"}),
        html.Div("12,847",
                 style={"fontSize": "28px", "fontWeight": "700",
                        "color": COLORS["blue"], "marginBottom": "8px"}),
        html.Div(
            style={"display": "flex", "gap": "16px", "marginTop": "8px"},
            children=[
                html.Div([
                    html.Div("Auto-Generated", style={"fontSize": "11px",
                                                       "color": COLORS["text_muted"]}),
                    html.Div("8,234", style={"fontSize": "16px",
                                              "fontWeight": "600",
                                              "color": COLORS["purple"]}),
                ]),
                html.Div([
                    html.Div("Manual", style={"fontSize": "11px",
                                               "color": COLORS["text_muted"]}),
                    html.Div("4,613", style={"fontSize": "16px",
                                              "fontWeight": "600",
                                              "color": COLORS["blue"]}),
                ]),
            ],
        ),
        html.Div(
            style={"marginTop": "12px"},
            children=[
                html.Div("Last rule update: 14 min ago",
                         style={"fontSize": "11px",
                                "color": COLORS["text_muted"]}),
            ],
        ),
    ])

    # ── Panel 5: Open Vulnerabilities ─────────────────────────────────
    panel_vulns = html.Div([
        html.Div("Open Vulnerabilities",
                 style={"fontSize": "11px", "color": COLORS["text_muted"],
                        "textTransform": "uppercase", "letterSpacing": "0.4px",
                        "marginBottom": "4px"}),
        html.Div("164",
                 style={"fontSize": "28px", "fontWeight": "700",
                        "color": COLORS["yellow"], "marginBottom": "12px"}),
        progress_row("Critical (CVE 9.0+)", "12", 7, COLORS["red"]),
        progress_row("High (CVE 7.0-8.9)", "38", 23, COLORS["yellow"]),
        progress_row("Medium (CVE 4.0-6.9)", "67", 41, COLORS["blue"]),
        progress_row("Low (CVE < 4.0)", "47", 29, COLORS["green"]),
    ])

    # ── Panel 6: SIEM Alerts ─────────────────────────────────────────
    siem_vals = [45, 52, 38, 60, 72, 55, 48, 65, 58, 42, 50, 63,
                 70, 62, 54, 47, 68, 75, 59, 44, 56, 61, 53, 49]
    panel_siem = html.Div([
        metric_with_sparkline(
            "SIEM Alerts (24h)", "1,312", siem_vals, accent="purple",
        ),
        html.Div(
            style={"display": "flex", "gap": "12px", "marginTop": "12px"},
            children=[
                html.Div([
                    html.Div("Correlated", style={"fontSize": "11px",
                                                   "color": COLORS["text_muted"]}),
                    html.Div("847", style={"fontSize": "14px",
                                            "fontWeight": "600",
                                            "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Div("Pending Triage", style={"fontSize": "11px",
                                                       "color": COLORS["text_muted"]}),
                    html.Div("312", style={"fontSize": "14px",
                                            "fontWeight": "600",
                                            "color": COLORS["yellow"]}),
                ]),
                html.Div([
                    html.Div("Escalated", style={"fontSize": "11px",
                                                  "color": COLORS["text_muted"]}),
                    html.Div("153", style={"fontSize": "14px",
                                            "fontWeight": "600",
                                            "color": COLORS["red"]}),
                ]),
            ],
        ),
    ])

    grid_items = [
        {"col_span": 1, "row_span": 1, "content": panel_threat},
        {"col_span": 1, "row_span": 1, "content": panel_attacks},
        {"col_span": 1, "row_span": 2, "content": panel_blocked},
        {"col_span": 1, "row_span": 1, "content": panel_firewall},
        {"col_span": 1, "row_span": 1, "content": panel_vulns},
        {"col_span": 1, "row_span": 1, "content": panel_siem},
    ]

    return layout_grid(
        title="Cyber Security",
        subtitle="Threat intelligence, attack surface monitoring, and SOC metrics",
        grid_items=grid_items,
    )
