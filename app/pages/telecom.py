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
    insight_card, morning_briefing,
    trend_indicator, use_case_badges, donut_figure,
    data_table, interactive_tabs,
    layout_executive, layout_table, layout_split, layout_alerts,
    layout_forecast, layout_grid,
    gauge_figure, sparkline_figure, metric_with_sparkline,
    _card, _hex_to_rgb,
)
from app.theme import COLORS, FONT_FAMILY, get_vertical_theme
from dash import dcc, html
import plotly.graph_objects as go


# ═══════════════════════════════════════════════════════════════════════════
#  1. TELECOM OUTCOME HUB  (Style A -- layout_executive)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive dashboard with subscriber, NPS, and uptime heroes,
    a subscriber-growth line chart, and two bottom panels (revenue donut
    + churn trend)."""
    vt = get_vertical_theme("telecom")

    # ── Morning Briefing (AI Narrative Center) ────────────────────────
    briefing = morning_briefing(
        title="Morning Briefing",
        summary_text=(
            "Customer Lifetime Value climbed to $847, up 6% QoQ, driven by premium "
            "bundle adoption in urban markets. Network reliability holds at 99.97% "
            "with zero P1 incidents this month. Churn continues its downward trend "
            "at 1.21%, but a localized anomaly in the Southeast region warrants "
            "monitoring — 340 base stations reported intermittent latency spikes "
            "correlating with a 0.8% uptick in support calls."
        ),
        signals=[
            {"label": "CLV", "status": "green", "detail": "$847 — +6% QoQ, premium bundles driving growth"},
            {"label": "Network Reliability", "status": "green", "detail": "99.97% uptime — zero P1 incidents"},
            {"label": "Churn", "status": "green", "detail": "1.21% monthly — 7th consecutive month of decline"},
            {"label": "SE Region Latency", "status": "amber", "detail": "340 base stations — intermittent spikes detected"},
        ],
    )

    # ── Hero metrics — Strategic North Stars ──────────────────────────
    heroes = [
        hero_metric("Customer Lifetime Value", "$847",
                     trend_text="+6% QoQ", trend_dir="up", accent="green",
                     href="/telecom/consumer_cx"),
        hero_metric("Service Reliability Index", "99.97%",
                     trend_text="0 P1 incidents", trend_dir="up", accent="purple",
                     href="/telecom/network_ops"),
        hero_metric("Total Subscribers", "48.7M",
                     trend_text="+3.2% YoY", trend_dir="up", accent="blue",
                     href="/telecom/consumer_cx"),
        hero_metric("Net Promoter Score", "62",
                     trend_text="+4 pts vs Q3", trend_dir="up", accent="green",
                     href="/telecom/consumer_cx"),
        hero_metric("Network Uptime", "99.97%",
                     trend_text="0.01% improvement", trend_dir="up", accent="purple",
                     href="/telecom/network_ops"),
    ]

    # ── Main chart: subscriber growth (18 months) ─────────────────────
    months = ["Jan '25", "Feb '25", "Mar '25", "Apr '25", "May '25", "Jun '25",
              "Jul '25", "Aug '25", "Sep '25", "Oct '25", "Nov '25", "Dec '25",
              "Jan '26", "Feb '26", "Mar '26"]
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
        vertical="telecom",
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
        vertical="telecom",
        height=280,
        title=dict(text="Churn Trend", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Churn %", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"],
                   range=[1.0, 1.6]),
    ))
    panel_churn = dcc.Graph(figure=fig_churn, config=CHART_CONFIG)

    # ── Insight card: network anomaly ─────────────────────────────────
    anomaly_card = insight_card(
        headline="SE Region Latency Anomaly",
        metric_value="340",
        direction="up",
        narrative=(
            "340 base stations in the Southeast region reported intermittent latency "
            "spikes over the past 48 hours, correlating with a 0.8% uptick in support "
            "calls. Root cause analysis points to a firmware update conflict on Ericsson "
            "RAN units deployed in Q4. Churn risk for affected subscribers estimated at 2.3x baseline."
        ),
        action_text="Escalate to Network Ops for firmware rollback assessment",
        severity="warning",
        sparkline_values=[12, 14, 11, 15, 18, 22, 45, 112, 210, 340],
    )

    # ── Assemble ──────────────────────────────────────────────────────
    panels = [
        ("Revenue by Segment", panel_rev, "/telecom/b2b_enterprise"),
        ("Churn Trend", panel_churn, "/telecom/consumer_cx"),
        ("AI Insight", anomaly_card),
    ]
    return layout_executive(
        title="Telecom Outcome Hub",
        subtitle="Enterprise-wide KPIs and performance trends",
        heroes=[briefing] + heroes,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. CONSUMER CX & GROWTH  (Style C -- layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_consumer_cx(cfg):
    """Split view with three real tabs: Satisfaction, Journey, and NPS,
    plus info banner and bottom stat cards."""

    # ══════════════════════════════════════════════════════════════════
    #  TAB 1: Satisfaction -- bar chart + donut side-by-side
    # ══════════════════════════════════════════════════════════════════

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

    tab_satisfaction = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([
                html.Div("Satisfaction by Touchpoint",
                         style={"fontSize": "14px", "fontWeight": "600",
                                "color": COLORS["white"], "marginBottom": "12px"}),
                left_chart,
            ], padding="20px"),
            _card([
                html.Div("Complaint Breakdown",
                         style={"fontSize": "14px", "fontWeight": "600",
                                "color": COLORS["white"], "marginBottom": "12px"}),
                right_chart,
            ], padding="20px"),
        ],
    )

    # ══════════════════════════════════════════════════════════════════
    #  TAB 2: Journey -- funnel bar chart of customer journey stages
    # ══════════════════════════════════════════════════════════════════

    journey_stages = ["Awareness", "Consideration", "Purchase",
                      "Retention", "Advocacy"]
    journey_rates = [100, 64, 38, 29, 12]
    journey_colors = [COLORS["blue"], COLORS["purple"], COLORS["green"],
                      COLORS["yellow"], COLORS["red"]]

    fig_journey = go.Figure()
    fig_journey.add_trace(go.Bar(
        x=journey_stages, y=journey_rates,
        marker=dict(color=journey_colors, cornerradius=4),
        text=[f"{r}%" for r in journey_rates], textposition="outside",
        textfont=dict(color=COLORS["white"], size=12),
    ))
    fig_journey.update_layout(**dark_chart_layout(
        height=320,
        title=dict(text="Customer Journey Funnel",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Conversion Rate %", range=[0, 115],
                   showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        xaxis=dict(color=COLORS["text_muted"]),
    ))
    chart_journey = dcc.Graph(figure=fig_journey, config=CHART_CONFIG)

    # Stage-to-stage drop-off breakdown
    dropoff_items = [
        {"label": "Awareness -> Consideration", "value": "64%",
         "pct": 64, "color": COLORS["purple"]},
        {"label": "Consideration -> Purchase", "value": "59%",
         "pct": 59, "color": COLORS["green"]},
        {"label": "Purchase -> Retention", "value": "76%",
         "pct": 76, "color": COLORS["yellow"]},
        {"label": "Retention -> Advocacy", "value": "41%",
         "pct": 41, "color": COLORS["red"]},
    ]

    tab_journey = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([
                html.Div("Journey Funnel",
                         style={"fontSize": "14px", "fontWeight": "600",
                                "color": COLORS["white"], "marginBottom": "12px"}),
                chart_journey,
            ], padding="20px"),
            _card([
                html.Div("Stage Conversion Rates",
                         style={"fontSize": "14px", "fontWeight": "600",
                                "color": COLORS["white"], "marginBottom": "16px"}),
                breakdown_list(dropoff_items),
                html.Div(
                    style={"marginTop": "20px", "padding": "12px",
                           "backgroundColor": f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
                           "borderRadius": "8px"},
                    children=[
                        html.Div("Key Insight",
                                 style={"fontSize": "11px", "fontWeight": "700",
                                        "color": COLORS["blue"],
                                        "textTransform": "uppercase",
                                        "marginBottom": "4px"}),
                        html.Div(
                            "Retention-to-Advocacy has the largest drop-off (41%). "
                            "Loyalty programs and referral incentives could improve "
                            "this conversion.",
                            style={"fontSize": "12px", "color": COLORS["text_muted"],
                                   "lineHeight": "1.5"}),
                    ],
                ),
            ], padding="20px"),
        ],
    )

    # ══════════════════════════════════════════════════════════════════
    #  TAB 3: NPS -- distribution bar + channel breakdown
    # ══════════════════════════════════════════════════════════════════

    nps_categories = ["Detractors (0-6)", "Passives (7-8)", "Promoters (9-10)"]
    nps_values = [18, 20, 62]
    nps_colors = [COLORS["red"], COLORS["yellow"], COLORS["green"]]

    fig_nps = go.Figure()
    fig_nps.add_trace(go.Bar(
        x=nps_categories, y=nps_values,
        marker=dict(color=nps_colors, cornerradius=4),
        text=[f"{v}%" for v in nps_values], textposition="outside",
        textfont=dict(color=COLORS["white"], size=13),
        width=0.5,
    ))
    fig_nps.update_layout(**dark_chart_layout(
        height=300,
        title=dict(text="NPS Distribution",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Percentage of Respondents", range=[0, 80],
                   showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        xaxis=dict(color=COLORS["text_muted"]),
    ))
    chart_nps = dcc.Graph(figure=fig_nps, config=CHART_CONFIG)

    # NPS by channel breakdown
    nps_channel_items = [
        {"label": "Mobile App", "value": "NPS 74",
         "pct": 74, "color": COLORS["green"]},
        {"label": "Self-Service Portal", "value": "NPS 68",
         "pct": 68, "color": COLORS["green"]},
        {"label": "In-Store", "value": "NPS 61",
         "pct": 61, "color": COLORS["blue"]},
        {"label": "Web Chat", "value": "NPS 55",
         "pct": 55, "color": COLORS["blue"]},
        {"label": "Social Media", "value": "NPS 48",
         "pct": 48, "color": COLORS["yellow"]},
        {"label": "Call Center", "value": "NPS 39",
         "pct": 39, "color": COLORS["red"]},
    ]

    tab_nps = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([
                html.Div("NPS Distribution",
                         style={"fontSize": "14px", "fontWeight": "600",
                                "color": COLORS["white"], "marginBottom": "12px"}),
                chart_nps,
                html.Div(
                    style={"display": "flex", "justifyContent": "center",
                           "alignItems": "center", "marginTop": "8px", "gap": "8px"},
                    children=[
                        html.Span("Overall NPS:",
                                  style={"fontSize": "13px",
                                         "color": COLORS["text_muted"]}),
                        html.Span("+44",
                                  style={"fontSize": "20px", "fontWeight": "700",
                                         "color": COLORS["green"]}),
                        html.Span("(+4 pts vs Q3)",
                                  style={"fontSize": "11px",
                                         "color": COLORS["text_muted"]}),
                    ],
                ),
            ], padding="20px"),
            _card([
                html.Div("NPS by Channel",
                         style={"fontSize": "14px", "fontWeight": "600",
                                "color": COLORS["white"], "marginBottom": "16px"}),
                breakdown_list(nps_channel_items),
            ], padding="20px"),
        ],
    )

    # ── Bottom stats ──────────────────────────────────────────────────
    bottom = [
        ("Avg CSAT", "78.4", "blue"),
        ("First Contact Res.", "68%", "green"),
        ("Avg Handle Time", "6.2 min", "yellow"),
        ("Escalation Rate", "11.3%", "red"),
    ]

    cx_insight = insight_card(
        headline="Call Center Lowest-Scoring Touchpoint",
        metric_value="6.2/10",
        direction="down",
        narrative="Call center remains lowest-scoring touchpoint at 6.2/10 CSAT. "
                  "Recommend AI-assisted routing to reduce wait times and improve first-call resolution.",
        action_text="Implement AI-assisted call routing",
        severity="warning",
    )

    return layout_split(
        title="Consumer CX & Growth",
        subtitle="Customer satisfaction, journey analytics, and NPS drivers",
        tab_contents=[
            ("Satisfaction", tab_satisfaction),
            ("Journey", tab_journey),
            ("NPS", tab_nps),
        ],
        banner_text=(
            "CX Insight: App channel satisfaction rose 5 pts after the Q1 UX "
            "refresh. Call center remains the lowest-scoring touchpoint -- "
            "consider AI-assisted routing to reduce wait times."
        ),
        bottom_stats=bottom,
        insight=cx_insight,
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

    b2b_insight = insight_card(
        headline="Enterprise ARR Growing 32%",
        metric_value="+32%",
        direction="up",
        narrative="Enterprise segment ARR growing 32% YoY driven by SD-WAN adoption. "
                  "Top 10 accounts represent 44% of B2B revenue — concentration risk moderate.",
        action_text="Expand SD-WAN pipeline to mid-market",
        severity="healthy",
    )

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
        insight=b2b_insight,
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
        # Regional coverage breakdown to fill 2-row span
        html.Div(style={"marginTop": "16px", "borderTop": f"1px solid {COLORS['border']}",
                         "paddingTop": "12px"},
                 children=[
            html.Div("Regional Coverage", style={"fontSize": "11px", "fontWeight": "700",
                                                    "color": COLORS["text_muted"],
                                                    "textTransform": "uppercase",
                                                    "letterSpacing": "0.4px", "marginBottom": "8px"}),
            progress_row("Northeast", "99.8%", 92, COLORS["green"]),
            progress_row("Southeast", "99.6%", 88, COLORS["green"]),
            progress_row("Midwest", "98.9%", 82, COLORS["blue"]),
            progress_row("West Coast", "99.4%", 86, COLORS["green"]),
        ]),
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

    netops_insight = insight_card(
        headline="EU-West Core Link at 87% Utilization",
        metric_value="87%",
        direction="up",
        narrative="EU-West core link approaching capacity threshold. Current utilization at 87% "
                  "with peak-hour bursts hitting 94%. Capacity expansion recommended within 60 days.",
        action_text="Initiate EU-West capacity expansion",
        severity="warning",
    )

    return layout_grid(
        title="Network Operations",
        subtitle="Real-time network health, capacity, and incident monitoring",
        grid_items=grid_items,
        insight=netops_insight,
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

    field_insight = insight_card(
        headline="Southeast Power Cost Efficiency Leading",
        metric_value="$0.08/kWh",
        direction="down",
        narrative="Southeast region showing best power cost efficiency at $0.08/kWh — "
                  "34% below network average. Renewable energy contracts driving savings.",
        action_text="Replicate Southeast energy strategy to other regions",
        severity="healthy",
    )

    return layout_table(
        title="Field Ops & Energy",
        subtitle="Work order management, SLA tracking, and technician dispatch",
        filters=filters,
        kpi_items=kpis,
        table_columns=table_columns,
        table_data=table_data,
        insight=field_insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. FRAUD PREVENTION  (Style D -- layout_alerts)
# ═══════════════════════════════════════════════════════════════════════════

def render_fraud_prevention(cfg):
    """Alert card layout with three real tabs -- Active Alerts,
    Investigating, and Resolved -- each with unique alert content,
    plus summary KPIs."""

    # ── Summary KPIs ──────────────────────────────────────────────────
    summary = [
        {"label": "Active Alerts", "value": "47", "accent": "red"},
        {"label": "Under Investigation", "value": "23", "accent": "yellow"},
        {"label": "Blocked (MTD)", "value": "1,842", "accent": "green"},
        {"label": "Est. Savings", "value": "$4.6M", "accent": "blue"},
    ]

    # ══════════════════════════════════════════════════════════════════
    #  TAB 1: Active Alerts -- critical + warning alerts requiring action
    # ══════════════════════════════════════════════════════════════════

    tab_active = html.Div(children=[
        alert_card(
            severity="critical",
            title="SIM Swap Fraud Cluster Detected",
            description=(
                "Coordinated SIM swap activity detected across 14 accounts in "
                "the Southeast region. Pattern matches organized fraud ring "
                "behavior with rapid sequential port-out requests."
            ),
            impact="Financial Impact: $287K estimated",
            timestamp="12 min ago",
            details=[
                ("Affected Accounts", "14"),
                ("Region", "Southeast -- Atlanta, Miami, Charlotte"),
                ("Attack Vector", "Social engineering + dealer portal"),
                ("Risk Score", "97 / 100"),
                ("IMSI Changes", "14 in last 45 min"),
            ],
        ),
        alert_card(
            severity="critical",
            title="Billing System Anomaly -- Revenue Leakage",
            description=(
                "Automated anomaly detection flagged abnormal CDR patterns "
                "indicating potential revenue bypass. Approximately 3,200 calls "
                "routed through a compromised interconnect gateway are not "
                "generating billing records."
            ),
            impact="Revenue Leakage: $142K / day",
            timestamp="38 min ago",
            details=[
                ("Gateway ID", "GW-ATL-0042"),
                ("Unbilled CDRs", "3,200+ daily"),
                ("Duration", "Ongoing ~72 hours"),
                ("Carrier", "Interconnect Partner C"),
                ("Pattern", "Zero-rated international trunk"),
            ],
        ),
        alert_card(
            severity="warning",
            title="International Roaming Fraud -- Premium Rate",
            description=(
                "Spike in premium-rate SMS origination from roaming subscribers "
                "in Eastern Europe. 8 devices generating abnormal volumes "
                "consistent with IRSF (International Revenue Share Fraud)."
            ),
            impact="Financial Impact: $68K accrued",
            timestamp="1 hr ago",
            details=[
                ("Devices Flagged", "8"),
                ("Roaming Partner", "Operator EU-East-12"),
                ("Premium Destinations", "Moldova, Latvia, Bosnia"),
                ("SMS Volume", "24,000 msgs in 6 hrs"),
                ("Avg Cost / msg", "$2.84"),
            ],
        ),
        alert_card(
            severity="warning",
            title="Subscription Fraud -- Synthetic Identity",
            description=(
                "Credit-check bypass detected for 6 new enterprise accounts "
                "opened in the last 48 hours. Identity verification scores "
                "are unusually clustered, suggesting synthetic identities."
            ),
            impact="Potential Exposure: $180K device subsidy",
            timestamp="2 hr ago",
            details=[
                ("Accounts Flagged", "6"),
                ("Devices Acquired", "42 handsets"),
                ("Avg Credit Score", "710 (synthetic cluster)"),
                ("Dealer Channel", "Online -- self-serve"),
            ],
        ),
    ])

    # ══════════════════════════════════════════════════════════════════
    #  TAB 2: Investigating -- ongoing investigation cases
    # ══════════════════════════════════════════════════════════════════

    tab_investigating = html.Div(children=[
        alert_card(
            severity="warning",
            title="Dealer Collusion Ring -- Northeast Region",
            description=(
                "Internal audit flagged suspicious activation patterns at 3 "
                "authorized dealer locations in New Jersey. Over 240 device "
                "activations in 72 hours with identical credit profiles and "
                "sequential IMEI numbers suggest organized subsidy fraud."
            ),
            impact="Potential Exposure: $420K in device subsidies",
            timestamp="Investigation opened 2 days ago",
            details=[
                ("Dealers Flagged", "3 locations (NJ-0147, NJ-0152, NJ-0168)"),
                ("Activations Under Review", "243"),
                ("Lead Investigator", "Fraud Analytics Team -- Region NE"),
                ("Evidence Status", "Device IMEI logs & CCTV under review"),
                ("Est. Completion", "5-7 business days"),
            ],
        ),
        alert_card(
            severity="info",
            title="Compromised API Credentials -- Partner Portal",
            description=(
                "Security team detected unusual API call patterns from MVNO "
                "Partner Delta's integration credentials. Traffic volume spiked "
                "8x normal levels with queries targeting subscriber PII endpoints. "
                "Credentials rotated; forensic analysis underway."
            ),
            impact="Data Exposure Risk: ~12K subscriber records queried",
            timestamp="Investigation opened 4 days ago",
            details=[
                ("Partner", "MVNO Delta (Partner ID: P-00482)"),
                ("Anomalous API Calls", "34,200 in 6-hour window"),
                ("Endpoints Accessed", "/subscriber/lookup, /account/details"),
                ("Credentials", "Rotated and re-issued"),
                ("Forensic Status", "Log analysis 60% complete"),
            ],
        ),
    ])

    # ══════════════════════════════════════════════════════════════════
    #  TAB 3: Resolved -- closed past incidents
    # ══════════════════════════════════════════════════════════════════

    tab_resolved = html.Div(children=[
        alert_card(
            severity="healthy",
            title="PBX Toll Fraud -- Enterprise Client Acme Corp",
            description=(
                "Compromised PBX system at Acme Corp was exploited to route "
                "premium-rate international calls through their SIP trunk. "
                "Fraud was detected within 4 hours. SIP trunk suspended, "
                "client PBX patched, and $38K in fraudulent charges reversed."
            ),
            impact="Resolved -- $38K recovered",
            timestamp="Closed 3 days ago",
            details=[
                ("Duration", "4 hours (detection to containment)"),
                ("Fraudulent Calls", "1,247 to premium numbers"),
                ("Client Action", "PBX firmware updated, SIP ACL tightened"),
                ("Financial Recovery", "$38K reversed, $0 customer liability"),
            ],
        ),
        alert_card(
            severity="healthy",
            title="Wangiri Callback Scheme -- West Africa Block",
            description=(
                "Large-scale Wangiri (one-ring) campaign from Togolese number "
                "block targeting 18,000 prepaid subscribers. Auto-block rules "
                "engaged immediately. Zero customer callbacks recorded. "
                "Number range permanently blacklisted."
            ),
            impact="Blocked -- $0 customer impact",
            timestamp="Closed 5 days ago",
            details=[
                ("Source Range", "+228 9XXX XXXX (Togo)"),
                ("Calls Attempted", "18,340"),
                ("Calls Blocked", "18,340 (100%)"),
                ("Customer Callbacks", "0"),
                ("Action Taken", "Number range permanently blacklisted"),
            ],
        ),
        alert_card(
            severity="healthy",
            title="SIM Box Fraud -- Wholesale Voice Bypass",
            description=(
                "Network analytics identified a SIM box operation in the "
                "Midwest terminating international calls as local traffic, "
                "bypassing interconnect fees. 48 SIMs deactivated, gateway "
                "equipment seized in coordination with law enforcement."
            ),
            impact="Resolved -- $165K revenue leakage stopped",
            timestamp="Closed 8 days ago",
            details=[
                ("SIMs Deactivated", "48"),
                ("Bypass Duration", "~21 days before detection"),
                ("Revenue Recovered", "$165K in retroactive billing"),
                ("Law Enforcement", "FBI IC3 referral filed"),
                ("Preventive Measure", "Enhanced velocity checks on bulk SIM activations"),
            ],
        ),
    ])

    fraud_insight = insight_card(
        headline="SIM Swap Attacks Up 23%",
        metric_value="+23%",
        direction="up",
        narrative="SIM swap attacks increased 23% this month with losses at $480K. "
                  "Recommend tightening verification thresholds and adding biometric step.",
        action_text="Tighten SIM swap verification thresholds",
        severity="critical",
    )

    return layout_alerts(
        title="Fraud Prevention",
        subtitle="Real-time fraud detection, investigation queue, and financial impact",
        tab_contents=[
            ("Active Alerts", tab_active),
            ("Investigating", tab_investigating),
            ("Resolved", tab_resolved),
        ],
        summary_kpis=summary,
        insight=fraud_insight,
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

    cyber_insight = insight_card(
        headline="Zero-Day Patching Response Improved",
        metric_value="4.2h",
        direction="down",
        narrative="Zero-day vulnerability patching response improved to 4.2 hours from "
                  "12.8 hours last quarter. Automated SIEM integration driving faster detection.",
        action_text="Continue automated SIEM integration rollout",
        severity="healthy",
    )

    return layout_grid(
        title="Cyber Security",
        subtitle="Threat intelligence, attack surface monitoring, and SOC metrics",
        grid_items=grid_items,
        insight=cyber_insight,
    )
