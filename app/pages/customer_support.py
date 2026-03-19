"""Customer Support vertical pages for Blueprint IQ.

Seven page renderers covering contact center intelligence and customer success:
  - Dashboard (support command center)
  - Contact Center (channel analytics & queue management)
  - Ticket Analytics (ticket intelligence & SLA tracking)
  - Agent Performance (workforce KPIs & trends)
  - Quality Assurance (QA scores & compliance)
  - Customer Health (health scores, churn risk, sentiment)
  - Self-Service & AI (deflection, chatbot, knowledge base)
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
    """Executive support dashboard with hero metrics, volume chart, and panels."""
    vt = get_vertical_theme("customer_support")

    # --- Morning Briefing (AI Narrative Center) ------------------------------
    briefing = morning_briefing(
        title="Support Morning Briefing",
        summary_text=(
            "Customer Satisfaction holds steady at 4.32/5 but ticket backlog has "
            "grown 18% week-over-week to 1,284 open items, driven by a surge in "
            "billing-related inquiries following the March pricing update. First "
            "Contact Resolution dipped to 74.2% from 78.1% as Tier 2 escalations "
            "increased 22%. Self-service deflection improved to 38.4% after the "
            "knowledge base refresh, saving an estimated $42K in agent costs this "
            "month. Recommend prioritizing backlog reduction with targeted staffing "
            "and expediting the billing FAQ automation deployment."
        ),
        signals=[
            {"label": "Customer Satisfaction", "status": "green", "detail": "CSAT 4.32/5 — stable, above 4.2 target"},
            {"label": "Ticket Backlog", "status": "red", "detail": "1,284 open tickets — 18% increase WoW, billing surge"},
            {"label": "First Contact Resolution", "status": "amber", "detail": "74.2% FCR — below 78% target, escalations up 22%"},
            {"label": "Self-Service", "status": "green", "detail": "Deflection at 38.4% — up 4.1 pts after KB refresh"},
        ],
    )

    # --- North Star hero metric ---------------------------------------------
    north_star = hero_metric("Customer Satisfaction", "4.32 / 5",
                              trend_text="Stable vs prior month", trend_dir="up",
                              accent="green", href="/customer_support/customer_health")

    # --- Hero metrics -------------------------------------------------------
    heroes = [
        north_star,
        hero_metric("Ticket Backlog", "1,284",
                     trend_text="18% increase WoW", trend_dir="up",
                     accent="red", href="/customer_support/ticket_analytics"),
        hero_metric("First Contact Resolution", "74.2%",
                     trend_text="3.9 pts below target", trend_dir="down",
                     accent="yellow", href="/customer_support/contact_center"),
        hero_metric("Net Promoter Score", "+47",
                     trend_text="2 pts up from last quarter", trend_dir="up",
                     accent="blue", href="/customer_support/customer_health"),
    ]

    # --- Main chart: ticket volume trends -----------------------------------
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    phone_vals   = [820, 790, 810, 850, 880, 920, 890, 860, 940, 910, 870, 900]
    chat_vals    = [540, 580, 620, 650, 690, 710, 740, 780, 820, 800, 760, 790]
    email_vals   = [380, 360, 350, 340, 330, 310, 320, 300, 310, 290, 280, 270]
    social_vals  = [120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 310, 320]

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=months, y=phone_vals, name="Phone",
        line=dict(color=COLORS["blue"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=months, y=chat_vals, name="Chat",
        line=dict(color=COLORS["green"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['green'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=months, y=email_vals, name="Email",
        line=dict(color=COLORS["purple"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['purple'])}, 0.08)",
    ))
    fig_trend.add_trace(go.Scatter(
        x=months, y=social_vals, name="Social Media",
        line=dict(color=COLORS["yellow"], width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['yellow'])}, 0.08)",
    ))
    fig_trend.update_layout(**dark_chart_layout(
        vertical="customer_support",
        height=320,
        title=dict(text="Ticket Volume by Channel (Monthly)", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Tickets", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_trend, config=CHART_CONFIG,
                           style={"height": "320px"})

    # --- Bottom panels ------------------------------------------------------
    # Panel 1: category distribution donut
    donut = donut_figure(
        labels=["Billing", "Technical", "Account", "Returns", "Product Info", "Complaints"],
        values=[28, 24, 18, 14, 10, 6],
        colors=[COLORS["blue"], COLORS["purple"], COLORS["green"],
                COLORS["yellow"], COLORS["red"], COLORS["text_muted"]],
        center_text="100%",
        title="Ticket Category Mix",
    )
    panel_donut = dcc.Graph(figure=donut, config=CHART_CONFIG,
                            style={"height": "280px"})

    # Panel 2: SLA compliance by priority
    fig_sla = go.Figure()
    priorities = ["P1 Critical", "P2 High", "P3 Medium", "P4 Low"]
    sla_target = [95.0, 90.0, 85.0, 80.0]
    sla_actual = [88.2, 91.4, 93.8, 96.1]
    fig_sla.add_trace(go.Bar(
        x=priorities, y=sla_target, name="SLA Target",
        marker_color=COLORS["border"], text=[f"{v}%" for v in sla_target],
        textposition="outside", textfont=dict(color=COLORS["text_muted"], size=10),
    ))
    fig_sla.add_trace(go.Bar(
        x=priorities, y=sla_actual, name="Actual",
        marker_color=[COLORS["red"] if a < t else COLORS["green"]
                      for a, t in zip(sla_actual, sla_target)],
        text=[f"{v}%" for v in sla_actual],
        textposition="outside", textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_sla.update_layout(**dark_chart_layout(
        vertical="customer_support",
        height=280, barmode="group",
        title=dict(text="SLA Compliance by Priority", font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="Compliance %", showgrid=True, gridcolor=COLORS["border"],
                   range=[70, 105]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    panel_sla = dcc.Graph(figure=fig_sla, config=CHART_CONFIG,
                          style={"height": "280px"})

    # --- Insight card -------------------------------------------------------
    support_insight = insight_card(
        headline="Billing Ticket Surge Post Pricing Update",
        metric_value="+42%",
        direction="up",
        narrative=(
            "Billing-related tickets spiked 42% following the March 1st pricing "
            "structure change. 68% of these tickets are first-time contacts asking "
            "about new tier differences. Self-service deflection for billing topics "
            "is only 22% vs 38% overall, indicating knowledge base gaps. Deploying "
            "the billing FAQ chatbot flow could deflect an estimated 340 tickets/week."
        ),
        action_text="Deploy billing FAQ automation and update knowledge base articles",
        severity="critical",
        sparkline_values=[180, 195, 210, 220, 240, 265, 290, 310, 335, 355],
    )

    panels = [
        ("Ticket Category Distribution", panel_donut, "/customer_support/ticket_analytics"),
        ("SLA Performance", panel_sla, "/customer_support/contact_center"),
        ("AI Insight", support_insight),
    ]

    return layout_executive(
        title="Support Command Center",
        subtitle="Contact center performance, customer satisfaction, and operational health overview",
        heroes=heroes,
        briefing=briefing,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. CONTACT CENTER  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_contact_center(cfg):
    """Contact center analytics with volume, wait times, and channel mix tabs."""

    banner_text = (
        "Contact center operating at 92.1% SLA compliance. Phone channel experiencing "
        "elevated wait times averaging 4.8 minutes during peak hours (10AM-2PM), "
        "exceeding the 3-minute target. Chat channel performing strongly with 1.2 min "
        "average wait time and 94% CSAT. Recommend shifting 15% of phone capacity to "
        "chat during peak windows and accelerating IVR self-service expansion."
    )

    # ── TAB 1: Volume Analysis ───────────────────────────────────────────
    channels = ["Phone", "Chat", "Email", "Social", "Self-Service", "Video"]
    daily_vol = [940, 820, 310, 280, 1090, 45]
    csat_by_ch = [4.18, 4.52, 4.28, 3.95, 4.41, 4.68]

    fig_vol = go.Figure()
    fig_vol.add_trace(go.Bar(
        x=channels, y=daily_vol, name="Daily Volume",
        marker_color=COLORS["blue"],
        text=daily_vol, textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_vol.add_trace(go.Scatter(
        x=channels, y=csat_by_ch, name="CSAT Score",
        mode="lines+markers", line=dict(color=COLORS["green"], width=2),
        marker=dict(size=7), yaxis="y2",
    ))
    fig_vol.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Daily Tickets", showgrid=True, gridcolor=COLORS["border"]),
        yaxis2=dict(title="CSAT (1-5)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["green"], range=[3.5, 5.0]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    vol_left = dcc.Graph(figure=fig_vol, config=CHART_CONFIG,
                         style={"height": "300px"})

    donut_channel = donut_figure(
        labels=["Phone", "Chat", "Email", "Social", "Self-Service", "Video"],
        values=[27, 24, 9, 8, 31, 1],
        colors=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                COLORS["yellow"], COLORS["red"], COLORS["text_muted"]],
        center_text="3,485",
        title="Channel Distribution",
    )
    vol_right = dcc.Graph(figure=donut_channel, config=CHART_CONFIG,
                          style={"height": "300px"})

    tab_volume = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Volume & Satisfaction by Channel", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), vol_left], padding="20px"),
            _card([html.Div("Channel Mix", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), vol_right], padding="20px"),
        ],
    )

    # ── TAB 2: Wait Times ────────────────────────────────────────────────
    hours = [f"{h}:00" for h in range(6, 22)]
    phone_wait = [1.8, 2.1, 3.2, 4.1, 4.8, 4.6, 3.9, 3.2, 3.8, 4.5,
                  4.2, 3.6, 2.8, 2.2, 1.9, 1.5]
    chat_wait = [0.4, 0.5, 0.8, 1.1, 1.4, 1.2, 1.0, 0.8, 1.0, 1.3,
                 1.1, 0.9, 0.7, 0.5, 0.4, 0.3]

    fig_wait = go.Figure()
    fig_wait.add_trace(go.Scatter(
        x=hours, y=phone_wait, name="Phone Wait (min)",
        line=dict(color=COLORS["blue"], width=2), mode="lines+markers",
        marker=dict(size=5),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_wait.add_trace(go.Scatter(
        x=hours, y=chat_wait, name="Chat Wait (min)",
        line=dict(color=COLORS["green"], width=2), mode="lines+markers",
        marker=dict(size=5),
    ))
    fig_wait.add_hline(y=3.0, line_dash="dash", line_color=COLORS["red"],
                       opacity=0.6, annotation_text="Target (3 min)",
                       annotation_font_color=COLORS["red"],
                       annotation_font_size=10)
    fig_wait.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Wait Time (min)", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    wait_left = dcc.Graph(figure=fig_wait, config=CHART_CONFIG,
                          style={"height": "300px"})

    # Abandonment by hour
    abandon_vals = [2.1, 2.8, 4.2, 5.8, 7.1, 6.4, 5.2, 4.1, 5.0, 6.5,
                    5.8, 4.6, 3.4, 2.6, 2.0, 1.8]
    fig_abandon = go.Figure()
    fig_abandon.add_trace(go.Bar(
        x=hours, y=abandon_vals,
        marker_color=[COLORS["red"] if v > 5.0 else COLORS["yellow"] if v > 3.5
                      else COLORS["green"] for v in abandon_vals],
        text=[f"{v}%" for v in abandon_vals],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=9),
    ))
    fig_abandon.add_hline(y=5.0, line_dash="dash", line_color=COLORS["red"],
                          opacity=0.6, annotation_text="Threshold (5%)",
                          annotation_font_color=COLORS["red"],
                          annotation_font_size=10)
    fig_abandon.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        yaxis=dict(title="Abandonment %", showgrid=True, gridcolor=COLORS["border"],
                   range=[0, 10]),
        margin=dict(l=48, r=24, t=24, b=48),
    ))
    wait_right = dcc.Graph(figure=fig_abandon, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_wait = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Wait Time by Hour of Day", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), wait_left], padding="20px"),
            _card([html.Div("Abandonment Rate by Hour", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), wait_right], padding="20px"),
        ],
    )

    # ── TAB 3: Channel Mix Trends ─────────────────────────────────────────
    qtrs = ["Q1 '25", "Q2 '25", "Q3 '25", "Q4 '25", "Q1 '26"]
    fig_mix = go.Figure()
    fig_mix.add_trace(go.Bar(x=qtrs, y=[34, 32, 30, 28, 27], name="Phone",
                             marker_color=COLORS["blue"]))
    fig_mix.add_trace(go.Bar(x=qtrs, y=[20, 21, 22, 23, 24], name="Chat",
                             marker_color=COLORS["green"]))
    fig_mix.add_trace(go.Bar(x=qtrs, y=[14, 12, 11, 10, 9], name="Email",
                             marker_color=COLORS["purple"]))
    fig_mix.add_trace(go.Bar(x=qtrs, y=[5, 6, 7, 8, 8], name="Social",
                             marker_color=COLORS["yellow"]))
    fig_mix.add_trace(go.Bar(x=qtrs, y=[26, 28, 29, 30, 31], name="Self-Service",
                             marker_color=COLORS["red"]))
    fig_mix.add_trace(go.Bar(x=qtrs, y=[1, 1, 1, 1, 1], name="Video",
                             marker_color=COLORS["text_muted"]))
    fig_mix.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="Channel Share %", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    mix_left = dcc.Graph(figure=fig_mix, config=CHART_CONFIG,
                         style={"height": "300px"})

    # FCR by channel donut
    fcr_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Phone", "Chat", "Email", "Social", "Video"],
            values=[72, 81, 68, 64, 88],
            colors=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                    COLORS["yellow"], COLORS["text_muted"]],
            center_text="74.2%",
            title="FCR by Channel",
        ),
        config=CHART_CONFIG,
        style={"height": "300px"},
    )

    tab_mix = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Channel Mix Evolution (Quarterly)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), mix_left], padding="20px"),
            _card([html.Div("First Contact Resolution by Channel", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), fcr_donut], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Daily Volume", "3,485", "blue"),
        ("Avg Wait Time", "3.2 min", "yellow"),
        ("Abandonment", "5.4%", "red"),
        ("SLA Compliance", "92.1%", "green"),
        ("Avg Handle Time", "8.4 min", "purple"),
    ]

    insight = insight_card(
        headline="Chat Channel Outperforming Phone",
        metric_value="+34 pts",
        direction="up",
        narrative=(
            "Chat CSAT of 4.52 exceeds phone CSAT of 4.18 by 34 basis points, "
            "with 40% lower cost per interaction. Chat FCR at 81% vs phone at 72%. "
            "Shifting 15% of phone volume to chat could save $18K/month."
        ),
        severity="healthy",
    )

    return layout_split(
        title="Contact Center Analytics",
        subtitle="Channel performance, queue management, and service level tracking",
        tab_contents=[
            ("Volume", tab_volume),
            ("Wait Times", tab_wait),
            ("Channel Mix", tab_mix),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. TICKET ANALYTICS  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_ticket_analytics(cfg):
    """Ticket intelligence table with filters, KPIs, and SLA tracking."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Category", "options": ["All Categories", "Billing", "Technical", "Account", "Returns", "Product Info", "Complaints"]},
        {"label": "Priority", "options": ["All Priorities", "P1 Critical", "P2 High", "P3 Medium", "P4 Low"]},
        {"label": "Status", "options": ["All Statuses", "Open", "In Progress", "Pending Customer", "Escalated", "Resolved"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Open Tickets", "value": "1,284", "accent": "red"},
        {"label": "Avg Resolution", "value": "4.2 hrs", "accent": "blue"},
        {"label": "SLA Compliance", "value": "92.1%", "accent": "green"},
        {"label": "Escalation Rate", "value": "13.8%", "accent": "yellow"},
        {"label": "Reopen Rate", "value": "6.4%", "accent": "purple"},
    ]

    # --- Table data ---------------------------------------------------------
    columns = [
        {"name": "Ticket ID", "id": "ticket_id"},
        {"name": "Category", "id": "category"},
        {"name": "Priority", "id": "priority"},
        {"name": "Product", "id": "product"},
        {"name": "Age (hrs)", "id": "age"},
        {"name": "Assigned To", "id": "assigned"},
        {"name": "SLA Status", "id": "sla_status"},
        {"name": "Status", "id": "status"},
    ]

    tickets = [
        ("TKT-28401", "Billing", "P1 Critical", "Enterprise Suite",
         "18.4", "Sarah Chen", "Breached", "Escalated"),
        ("TKT-28397", "Technical", "P1 Critical", "API Services",
         "12.1", "Marcus Williams", "At Risk", "In Progress"),
        ("TKT-28412", "Billing", "P2 High", "Core Platform",
         "8.6", "Priya Patel", "On Track", "In Progress"),
        ("TKT-28388", "Complaints", "P2 High", "Mobile App",
         "22.3", "James Rodriguez", "Breached", "Escalated"),
        ("TKT-28420", "Technical", "P2 High", "Enterprise Suite",
         "4.2", "Lisa Thompson", "On Track", "Open"),
        ("TKT-28415", "Account", "P3 Medium", "Core Platform",
         "6.8", "David Kim", "On Track", "In Progress"),
        ("TKT-28422", "Returns", "P3 Medium", "Add-ons",
         "3.1", "Emma Wilson", "On Track", "Open"),
        ("TKT-28418", "Product Info", "P3 Medium", "Mobile App",
         "5.4", "Alex Nakamura", "On Track", "Pending Customer"),
        ("TKT-28425", "Technical", "P4 Low", "Core Platform",
         "2.0", "Unassigned", "On Track", "Open"),
        ("TKT-28419", "Billing", "P4 Low", "Add-ons",
         "7.2", "Carlos Mendez", "On Track", "In Progress"),
    ]

    data = []
    for tid, cat, pri, prod, age, assigned, sla, status in tickets:
        data.append({
            "ticket_id": tid,
            "category": cat,
            "priority": pri,
            "product": prod,
            "age": age,
            "assigned": assigned,
            "sla_status": sla,
            "status": status,
        })

    insight = insight_card(
        headline="P1 SLA Breach Rate Climbing",
        metric_value="11.8%",
        direction="up",
        narrative=(
            "P1 Critical ticket SLA breach rate has increased to 11.8% from 7.2% "
            "last month. Root cause analysis shows 62% of breaches originate from "
            "billing category tickets lacking auto-routing to Tier 2 specialists. "
            "Enabling ML-based ticket routing could reduce P1 breach rate by 40%."
        ),
        severity="warning",
    )

    return layout_table(
        title="Ticket Intelligence",
        subtitle="Real-time ticket tracking, SLA monitoring, and backlog analysis",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. AGENT PERFORMANCE  (Style E — layout_forecast)
# ═══════════════════════════════════════════════════════════════════════════

def render_agent_performance(cfg):
    """Agent performance with KPI trends, dual-axis chart, and team breakdown."""

    # --- KPIs ---------------------------------------------------------------
    kpis = [
        {"label": "Active Agents", "value": "342", "accent": "blue"},
        {"label": "Avg CSAT", "value": "4.32", "accent": "green"},
        {"label": "Avg Handle Time", "value": "8.4 min", "accent": "yellow"},
        {"label": "Adherence Rate", "value": "91%", "accent": "green"},
    ]

    # --- Main chart: agent performance trends (dual axis) -------------------
    weeks = [f"W{w}" for w in range(1, 13)]
    import math
    csat_trend = [4.28, 4.31, 4.25, 4.30, 4.35, 4.29, 4.33, 4.38, 4.32,
                  4.36, 4.34, 4.32]
    aht_trend = [9.2, 8.9, 9.1, 8.8, 8.6, 8.7, 8.5, 8.3, 8.4, 8.2, 8.3, 8.4]

    fig_perf = go.Figure()
    fig_perf.add_trace(go.Scatter(
        x=weeks, y=csat_trend, name="CSAT Score",
        line=dict(color=COLORS["green"], width=2), mode="lines+markers",
        marker=dict(size=6), yaxis="y",
    ))
    fig_perf.add_trace(go.Bar(
        x=weeks, y=aht_trend, name="Avg Handle Time (min)",
        marker_color=COLORS["blue"], opacity=0.7, yaxis="y2",
    ))
    fig_perf.update_layout(**dark_chart_layout(
        height=300,
        xaxis=dict(title="Week", showgrid=False),
        yaxis=dict(title="CSAT Score", showgrid=True, gridcolor=COLORS["border"],
                   side="left", range=[4.0, 4.6]),
        yaxis2=dict(title="AHT (min)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["text_muted"], range=[7.0, 10.0]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
        barmode="overlay",
    ))
    main_chart = dcc.Graph(figure=fig_perf, config=CHART_CONFIG,
                           style={"height": "300px"})

    # --- Side breakdown by team ---------------------------------------------
    side = breakdown_list([
        {"label": "Team Alpha", "value": "4.48 CSAT", "pct": 92, "color": COLORS["green"]},
        {"label": "Team Bravo", "value": "4.35 CSAT", "pct": 85, "color": COLORS["green"]},
        {"label": "Team Charlie", "value": "4.28 CSAT", "pct": 78, "color": COLORS["blue"]},
        {"label": "Team Delta", "value": "4.22 CSAT", "pct": 72, "color": COLORS["yellow"]},
        {"label": "Team Echo", "value": "4.14 CSAT", "pct": 65, "color": COLORS["yellow"]},
        {"label": "Team Foxtrot", "value": "4.08 CSAT", "pct": 58, "color": COLORS["red"]},
    ])

    insight = insight_card(
        headline="Top Quartile Agents Drive 2x FCR",
        metric_value="86.4%",
        direction="up",
        narrative=(
            "Top-quartile agents achieve 86.4% first contact resolution vs 61.2% "
            "for the bottom quartile. Key differentiators: knowledge base usage "
            "(3.2x more frequent), active listening markers in transcripts, and "
            "avg 14 months more tenure. Pairing bottom-quartile agents with top "
            "performers in a buddy program could lift overall FCR by 5-8 pts."
        ),
        severity="healthy",
    )

    return layout_forecast(
        title="Agent Performance",
        subtitle="Workforce KPIs, team comparisons, and performance optimization",
        kpi_items=kpis,
        hero_value="4.32",
        hero_label="Average Customer Satisfaction Score",
        hero_trend_text="+0.04 pts vs 12-week average",
        main_chart=main_chart,
        side_component=html.Div([
            html.Div("Team Performance Ranking",
                     style={"fontSize": "14px", "fontWeight": "600",
                            "color": COLORS["white"], "marginBottom": "16px"}),
            side,
        ]),
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. QUALITY ASSURANCE  (Style B — layout_table)
# ═══════════════════════════════════════════════════════════════════════════

def render_quality_assurance(cfg):
    """Quality assurance scores table with filters, KPIs, and compliance tracking."""

    # --- Filters ------------------------------------------------------------
    filters = [
        {"label": "Team", "options": ["All Teams", "Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot"]},
        {"label": "Score Range", "options": ["All Scores", "90-100 (Excellent)", "80-89 (Good)", "70-79 (Needs Improvement)", "Below 70 (Action Required)"]},
        {"label": "Period", "options": ["Last 30 Days", "Last 60 Days", "Last 90 Days", "This Quarter", "Last Quarter"]},
    ]

    # --- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "Avg QA Score", "value": "87.4", "accent": "green"},
        {"label": "Evaluations Done", "value": "1,248", "accent": "blue"},
        {"label": "Compliance Rate", "value": "96%", "accent": "green"},
        {"label": "Calibration Variance", "value": "2.8%", "accent": "yellow"},
        {"label": "Action Plans Active", "value": "18", "accent": "purple"},
    ]

    # --- Table data ---------------------------------------------------------
    columns = [
        {"name": "Agent", "id": "agent"},
        {"name": "Team", "id": "team"},
        {"name": "Overall Score", "id": "overall"},
        {"name": "Greeting", "id": "greeting"},
        {"name": "Empathy", "id": "empathy"},
        {"name": "Resolution", "id": "resolution"},
        {"name": "Compliance", "id": "compliance"},
        {"name": "Status", "id": "status"},
    ]

    evaluations = [
        ("Sarah Chen", "Alpha", 94, 96, 92, 95, 98, "Healthy"),
        ("Marcus Williams", "Alpha", 91, 90, 93, 88, 96, "Healthy"),
        ("Priya Patel", "Bravo", 89, 92, 88, 86, 94, "Healthy"),
        ("Lisa Thompson", "Bravo", 88, 85, 90, 87, 92, "Healthy"),
        ("David Kim", "Charlie", 84, 88, 82, 80, 90, "Healthy"),
        ("Emma Wilson", "Charlie", 82, 80, 84, 78, 88, "Warning"),
        ("Alex Nakamura", "Delta", 79, 82, 76, 75, 86, "Warning"),
        ("Carlos Mendez", "Delta", 76, 78, 72, 74, 84, "Warning"),
        ("James Rodriguez", "Echo", 72, 70, 74, 68, 80, "Critical"),
        ("Nina Kowalski", "Foxtrot", 68, 65, 70, 64, 78, "Critical"),
    ]

    data = []
    for agent, team, overall, greet, emp, res, comp, status in evaluations:
        data.append({
            "agent": agent,
            "team": team,
            "overall": f"{overall}%",
            "greeting": f"{greet}%",
            "empathy": f"{emp}%",
            "resolution": f"{res}%",
            "compliance": f"{comp}%",
            "status": status,
        })

    insight = insight_card(
        headline="Empathy Scores Correlate with CSAT",
        metric_value="r=0.84",
        direction="up",
        narrative=(
            "Statistical analysis reveals a strong correlation (r=0.84) between "
            "agent empathy scores and customer satisfaction ratings. Agents scoring "
            "above 90% on empathy achieve CSAT of 4.5+ vs 4.0 for those below 75%. "
            "Targeted empathy training for bottom two quartiles could yield a "
            "0.15-0.20 pt CSAT improvement enterprise-wide."
        ),
        severity="healthy",
    )

    return layout_table(
        title="Quality Assurance",
        subtitle="QA evaluation scores, compliance adherence, and coaching insights",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. CUSTOMER HEALTH  (Style C — layout_split)
# ═══════════════════════════════════════════════════════════════════════════

def render_customer_health(cfg):
    """Customer health with health scores, churn risk, and sentiment tabs."""

    banner_text = (
        "Customer health portfolio showing 68% of accounts in Green status, 22% "
        "in Amber, and 10% in Red. Churn prediction model flagged 47 accounts "
        "with >70% churn probability — 18 of these have contract renewals within "
        "90 days representing $2.4M in ARR. Overall sentiment trending positive "
        "at 68% but declining in the Enterprise Suite segment post v4.2 release."
    )

    # ── TAB 1: Health Scores ─────────────────────────────────────────────
    segments = ["Enterprise", "Mid-Market", "SMB", "Startup", "Government"]
    green_pct = [72, 68, 62, 58, 78]
    amber_pct = [18, 22, 24, 26, 16]
    red_pct   = [10, 10, 14, 16, 6]

    fig_health = go.Figure()
    fig_health.add_trace(go.Bar(
        x=segments, y=green_pct, name="Healthy",
        marker_color=COLORS["green"],
    ))
    fig_health.add_trace(go.Bar(
        x=segments, y=amber_pct, name="At Risk",
        marker_color=COLORS["yellow"],
    ))
    fig_health.add_trace(go.Bar(
        x=segments, y=red_pct, name="Critical",
        marker_color=COLORS["red"],
    ))
    fig_health.update_layout(**dark_chart_layout(
        height=300, barmode="stack",
        yaxis=dict(title="Account Share %", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center"),
    ))
    health_left = dcc.Graph(figure=fig_health, config=CHART_CONFIG,
                            style={"height": "300px"})

    donut_health = donut_figure(
        labels=["Healthy (>80)", "Moderate (60-80)", "At Risk (40-60)", "Critical (<40)"],
        values=[68, 18, 10, 4],
        colors=[COLORS["green"], COLORS["yellow"], COLORS["red"], "#FF6B6B"],
        center_text="1,847",
        title="Health Score Distribution",
    )
    health_right = dcc.Graph(figure=donut_health, config=CHART_CONFIG,
                             style={"height": "300px"})

    tab_health = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Health by Customer Segment", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), health_left], padding="20px"),
            _card([html.Div("Overall Health Distribution", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), health_right], padding="20px"),
        ],
    )

    # ── TAB 2: Churn Risk ────────────────────────────────────────────────
    months = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
              "Jan", "Feb", "Mar"]
    fig_churn = go.Figure()
    fig_churn.add_trace(go.Scatter(
        x=months, y=[3.8, 4.1, 3.6, 4.2, 4.5, 4.0, 4.8, 5.2, 5.6],
        name="Churn Rate %", mode="lines+markers",
        line=dict(color=COLORS["red"], width=2), marker=dict(size=5),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['red'])}, 0.08)",
    ))
    fig_churn.add_trace(go.Scatter(
        x=months, y=[12, 14, 11, 15, 18, 16, 22, 28, 34],
        name="High-Risk Accounts", mode="lines+markers",
        line=dict(color=COLORS["yellow"], width=2),
        marker=dict(size=5), yaxis="y2",
    ))
    fig_churn.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Churn Rate %", showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"]),
        yaxis2=dict(title="High-Risk Count", overlaying="y", side="right",
                    showgrid=False, color=COLORS["yellow"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    churn_left = dcc.Graph(figure=fig_churn, config=CHART_CONFIG,
                           style={"height": "300px"})

    # Churn drivers
    drivers = ["Low Product Usage", "Repeat Escalations", "Billing Disputes",
               "Poor CSAT Trend", "Competitor Mentions"]
    driver_impact = [34, 22, 18, 15, 11]
    fig_drivers = go.Figure(go.Bar(
        x=driver_impact, y=drivers, orientation="h",
        marker_color=[COLORS["red"], COLORS["red"], COLORS["yellow"],
                      COLORS["yellow"], COLORS["blue"]],
        text=[f"{v}%" for v in driver_impact],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_drivers.update_layout(**dark_chart_layout(
        height=300, showlegend=False,
        xaxis=dict(title="Impact Weight %", showgrid=True, gridcolor=COLORS["border"],
                   range=[0, 45]),
        margin=dict(l=140, r=40, t=24, b=48),
    ))
    churn_right = dcc.Graph(figure=fig_drivers, config=CHART_CONFIG,
                            style={"height": "300px"})

    tab_churn = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Churn Rate & High-Risk Accounts", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), churn_left], padding="20px"),
            _card([html.Div("Top Churn Drivers (ML Model)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), churn_right], padding="20px"),
        ],
    )

    # ── TAB 3: Sentiment ─────────────────────────────────────────────────
    months_full = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
                   "Jan", "Feb", "Mar"]
    fig_sent = go.Figure()
    fig_sent.add_trace(go.Scatter(
        x=months_full, y=[65, 67, 66, 69, 71, 70, 68, 67, 68],
        name="Positive %", mode="lines+markers",
        line=dict(color=COLORS["green"], width=2), marker=dict(size=5),
    ))
    fig_sent.add_trace(go.Scatter(
        x=months_full, y=[22, 21, 22, 20, 18, 19, 20, 21, 20],
        name="Neutral %", mode="lines+markers",
        line=dict(color=COLORS["blue"], width=2), marker=dict(size=5),
    ))
    fig_sent.add_trace(go.Scatter(
        x=months_full, y=[13, 12, 12, 11, 11, 11, 12, 12, 12],
        name="Negative %", mode="lines+markers",
        line=dict(color=COLORS["red"], width=2), marker=dict(size=5),
    ))
    fig_sent.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(title="Percentage", showgrid=True, gridcolor=COLORS["border"],
                   range=[0, 80]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    sent_left = dcc.Graph(figure=fig_sent, config=CHART_CONFIG,
                          style={"height": "300px"})

    # Sentiment by product
    products = ["Core Platform", "Mobile App", "Enterprise Suite", "API Services", "Add-ons"]
    pos_pct = [72, 65, 58, 74, 70]
    neg_pct = [8, 14, 18, 6, 10]
    fig_prod_sent = go.Figure()
    fig_prod_sent.add_trace(go.Bar(
        x=products, y=pos_pct, name="Positive",
        marker_color=COLORS["green"],
    ))
    fig_prod_sent.add_trace(go.Bar(
        x=products, y=neg_pct, name="Negative",
        marker_color=COLORS["red"],
    ))
    fig_prod_sent.update_layout(**dark_chart_layout(
        height=300, barmode="group",
        yaxis=dict(title="Sentiment %", showgrid=True, gridcolor=COLORS["border"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    sent_right = dcc.Graph(figure=fig_prod_sent, config=CHART_CONFIG,
                           style={"height": "300px"})

    tab_sentiment = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
        children=[
            _card([html.Div("Sentiment Trend (9 Months)", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), sent_left], padding="20px"),
            _card([html.Div("Sentiment by Product", style={"fontSize": "14px", "fontWeight": "600", "color": COLORS["white"], "marginBottom": "12px"}), sent_right], padding="20px"),
        ],
    )

    # --- Bottom stats -------------------------------------------------------
    bottom_stats = [
        ("Total Accounts", "1,847", "blue"),
        ("NPS", "+47", "green"),
        ("CSAT (90d)", "4.32", "green"),
        ("Churn Rate", "5.6%", "red"),
        ("At-Risk ARR", "$2.4M", "yellow"),
    ]

    insight = insight_card(
        headline="Enterprise Suite Sentiment Declining",
        metric_value="-14 pts",
        direction="down",
        narrative=(
            "Positive sentiment for Enterprise Suite dropped 14 points since the "
            "v4.2 release, driven by performance complaints and migration friction. "
            "18% negative sentiment is the highest across all products. 12 Enterprise "
            "accounts with combined $1.8M ARR now flagged as churn risk."
        ),
        severity="critical",
    )

    return layout_split(
        title="Customer Health",
        subtitle="Customer health scoring, churn prediction, and sentiment analysis",
        tab_contents=[
            ("Health Scores", tab_health),
            ("Churn Risk", tab_churn),
            ("Sentiment", tab_sentiment),
        ],
        banner_text=banner_text,
        bottom_stats=bottom_stats,
        insight=insight,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. SELF-SERVICE & AI  (Style F — layout_grid)
# ═══════════════════════════════════════════════════════════════════════════

def render_self_service(cfg):
    """Self-service and AI grid with gauges, sparklines, and deflection metrics."""

    # --- Grid item 1: Deflection Rate gauge (tall, spans 2 rows) -----------
    gauge_deflection = gauge_figure(
        value=38.4, max_val=100, title="Self-Service Deflection Rate",
        color=COLORS["green"],
    )
    deflection_panel = html.Div([
        dcc.Graph(figure=gauge_deflection, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"textAlign": "center", "marginTop": "12px"},
            children=[
                html.Div("38.4%", style={
                    "fontSize": "18px", "fontWeight": "700",
                    "color": COLORS["green"], "letterSpacing": "1px",
                }),
                html.Div("Target: 45% by Q3", style={
                    "fontSize": "12px", "color": COLORS["text_muted"],
                    "marginTop": "4px",
                }),
            ],
        ),
        html.Div(
            style={"marginTop": "20px"},
            children=[
                progress_row("Knowledge Base", "Active", 42, COLORS["green"]),
                progress_row("AI Chatbot", "Growing", 34, COLORS["blue"]),
                progress_row("IVR Self-Service", "Moderate", 28, COLORS["yellow"]),
            ],
        ),
    ])

    # --- Grid item 2: Chatbot Sessions sparkline ---------------------------
    chatbot_data = [280, 310, 340, 360, 380, 420, 450, 470, 510, 540,
                    560, 590, 620, 650, 680, 720, 740, 780, 810, 840]
    chatbot_panel = metric_with_sparkline(
        "AI Chatbot Sessions (24h)", "2,847",
        chatbot_data, accent="blue",
    )

    # --- Grid item 3: Resolution Without Agent metric ----------------------
    resolved_data = [62, 64, 63, 66, 68, 67, 70, 72, 71, 74,
                     73, 75, 76, 78, 77, 80, 79, 81, 82, 84]
    resolved_panel = metric_with_sparkline(
        "Resolved Without Agent", "72.4%",
        resolved_data, accent="green",
    )

    # --- Grid item 4: Intent Recognition (wide, spans 2 cols) ---------------
    intents = ["Billing FAQ", "Password Reset", "Order Status", "Returns", "Feature Help", "Upgrade"]
    intent_counts = [840, 620, 510, 380, 290, 180]
    resolution_rates = [88, 95, 72, 64, 58, 42]
    intent_colors = [COLORS["green"] if r >= 75 else COLORS["yellow"] if r >= 60
                     else COLORS["red"] for r in resolution_rates]

    fig_intent = go.Figure()
    fig_intent.add_trace(go.Bar(
        x=intents, y=intent_counts,
        marker_color=intent_colors,
        text=[f"{c} ({r}%)" for c, r in zip(intent_counts, resolution_rates)],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=10),
    ))
    fig_intent.update_layout(**dark_chart_layout(
        height=200, margin=dict(l=40, r=20, t=36, b=36),
        title=dict(text="Top Intents by Volume (Resolution Rate %)",
                   font=dict(size=13, color=COLORS["white"])),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"]),
    ))
    intent_panel = html.Div([
        dcc.Graph(figure=fig_intent, config=CHART_CONFIG,
                  style={"height": "200px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "marginTop": "8px", "padding": "0 4px"},
            children=[
                html.Div([
                    html.Span("Total Intents: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("2,820", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["white"]}),
                ]),
                html.Div([
                    html.Span("Escalated: ", style={"fontSize": "12px",
                              "color": COLORS["text_muted"]}),
                    html.Span("784", style={"fontSize": "14px",
                              "fontWeight": "700", "color": COLORS["red"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 5: Knowledge Base Metrics --------------------------------
    kb_data = [4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000,
               6200, 6400, 6600, 6800, 7000, 7200, 7400, 7600, 7800, 8000]
    kb_panel = metric_with_sparkline(
        "KB Article Views (24h)", "8,420",
        kb_data, accent="purple",
    )

    # --- Grid item 6: Chatbot Confidence Gauge ------------------------------
    confidence_gauge = gauge_figure(
        value=87, max_val=100, title="Chatbot Confidence Score",
        color=COLORS["blue"],
    )
    confidence_panel = html.Div([
        dcc.Graph(figure=confidence_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-around",
                   "marginTop": "8px"},
            children=[
                html.Div([
                    html.Div("Threshold", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("> 80%", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["green"]}),
                ]),
                html.Div([
                    html.Div("Low Confidence", style={"fontSize": "11px",
                             "color": COLORS["text_muted"],
                             "textTransform": "uppercase"}),
                    html.Div("13%", style={"fontSize": "14px",
                             "fontWeight": "600", "color": COLORS["yellow"]}),
                ]),
            ],
        ),
    ])

    # --- Grid item 7: Cost Savings ------------------------------------------
    savings_gauge = gauge_figure(
        value=42, max_val=80, title="Monthly Cost Savings ($K)",
        color=COLORS["green"],
    )
    savings_panel = html.Div([
        dcc.Graph(figure=savings_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"marginTop": "12px"},
            children=[
                progress_row("KB Deflection", "$24K", 60, COLORS["green"]),
                progress_row("Chatbot Deflection", "$12K", 30, COLORS["blue"]),
                progress_row("IVR Automation", "$6K", 15, COLORS["purple"]),
            ],
        ),
    ])

    # --- Assemble grid ------------------------------------------------------
    grid_items = [
        {"col_span": 1, "row_span": 2, "content": deflection_panel},
        {"col_span": 1, "row_span": 1, "content": chatbot_panel},
        {"col_span": 1, "row_span": 1, "content": resolved_panel},
        {"col_span": 2, "row_span": 1, "content": intent_panel},
        {"col_span": 1, "row_span": 1, "content": kb_panel},
        {"col_span": 1, "row_span": 1, "content": confidence_panel},
        {"col_span": 1, "row_span": 1, "content": savings_panel},
    ]

    insight = insight_card(
        headline="AI Chatbot Deflection Growing Rapidly",
        metric_value="+34%",
        direction="up",
        narrative=(
            "AI chatbot deflection rate increased 34% quarter-over-quarter, now "
            "handling 2,847 sessions daily with 72.4% resolution without agent "
            "escalation. Estimated monthly savings of $42K. Expanding chatbot to "
            "cover returns and upgrade intents (currently 42% and 64% resolution) "
            "could add $15K in monthly savings."
        ),
        severity="healthy",
    )

    return layout_grid(
        title="Self-Service & AI",
        subtitle="Deflection analytics, chatbot performance, and knowledge base effectiveness",
        grid_items=grid_items,
        insight=insight,
    )
