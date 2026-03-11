"""Media & Entertainment vertical page renderers.

Provides seven page-level rendering functions for the Media & Entertainment
vertical: Dashboard, Content Strategy, Audience Intelligence,
Subscription Intelligence, Ad Yield & Monetization, Platform & Delivery,
and Personalization & AI.

Each function accepts a ``cfg`` dict and returns an ``html.Div``.
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
#  1.  DASHBOARD  –  layout_executive  (Style A)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive media dashboard with hero metrics, viewership trend, and
    bottom panels for genre and platform breakdowns."""

    # -- hero metrics -------------------------------------------------------
    heroes = [
        hero_metric("Total Subscribers", "48.2M",
                     trend_text="+6.4% vs last quarter", trend_dir="up",
                     accent="blue"),
        hero_metric("Content Engagement", "73.8%",
                     trend_text="+2.1pp vs last month", trend_dir="up",
                     accent="green"),
        hero_metric("Ad Revenue", "$18.7M",
                     trend_text="+11.3% YoY", trend_dir="up",
                     accent="purple"),
    ]

    # -- main area chart: viewership trends ---------------------------------
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    fig_main = go.Figure()
    fig_main.add_trace(go.Scatter(
        x=months,
        y=[31.2, 33.5, 35.1, 34.8, 37.4, 40.2,
           42.1, 44.6, 43.8, 45.9, 47.1, 48.2],
        mode="lines",
        name="Total Viewers (M)",
        line=dict(color=COLORS["blue"], width=2.5),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.10)",
    ))
    fig_main.add_trace(go.Scatter(
        x=months,
        y=[18.4, 19.1, 20.5, 21.0, 22.8, 24.3,
           25.7, 27.1, 26.9, 28.4, 29.2, 30.5],
        mode="lines",
        name="Premium Viewers (M)",
        line=dict(color=COLORS["purple"], width=2, dash="dash"),
    ))
    fig_main.add_trace(go.Scatter(
        x=months,
        y=[12.8, 14.4, 14.6, 13.8, 14.6, 15.9,
           16.4, 17.5, 16.9, 17.5, 17.9, 17.7],
        mode="lines",
        name="Ad-Supported (M)",
        line=dict(color=COLORS["green"], width=2, dash="dot"),
    ))
    fig_main.update_layout(**dark_chart_layout(
        height=340,
        title=dict(text="Monthly Viewership Trends", font=dict(size=14,
                   color=COLORS["white"]), x=0.0),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_main, config=CHART_CONFIG)

    # -- bottom panel 1: content genre donut --------------------------------
    genre_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Drama", "Comedy", "Sci-Fi", "Documentary", "Reality",
                    "Sports"],
            values=[28, 22, 16, 14, 12, 8],
            colors=[COLORS["blue"], COLORS["purple"], COLORS["green"],
                    COLORS["yellow"], COLORS["red"], COLORS["text_muted"]],
            center_text="6",
            title="Content by Genre",
        ),
        config=CHART_CONFIG,
    )

    # -- bottom panel 2: platform split bar chart ---------------------------
    platforms = ["Smart TV", "Mobile", "Web", "Tablet", "Console"]
    fig_plat = go.Figure()
    fig_plat.add_trace(go.Bar(
        x=platforms,
        y=[38, 27, 18, 10, 7],
        marker_color=[COLORS["blue"], COLORS["green"], COLORS["purple"],
                      COLORS["yellow"], COLORS["red"]],
        text=["38%", "27%", "18%", "10%", "7%"],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_plat.update_layout(**dark_chart_layout(
        height=280,
        title=dict(text="Viewership by Platform", font=dict(size=14,
                   color=COLORS["white"]), x=0.0),
        showlegend=False,
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="Share %"),
    ))
    plat_chart = dcc.Graph(figure=fig_plat, config=CHART_CONFIG)

    return layout_executive(
        title="Media Outcome Hub",
        subtitle="Streaming performance at a glance",
        heroes=heroes,
        main_chart=main_chart,
        panels=[
            ("Genre Distribution", genre_donut),
            ("Platform Split", plat_chart),
        ],
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2.  CONTENT STRATEGY  –  layout_split  (Style C)
# ═══════════════════════════════════════════════════════════════════════════

def render_content_strategy(cfg):
    """Content strategy view with performance by genre, investment donut, and
    bottom stats."""

    # -- left panel: stacked bar of content performance by genre -------------
    genres = ["Drama", "Comedy", "Sci-Fi", "Documentary", "Reality"]
    fig_perf = go.Figure()
    fig_perf.add_trace(go.Bar(
        name="Completion Rate",
        x=genres,
        y=[82, 74, 79, 68, 63],
        marker_color=COLORS["blue"],
    ))
    fig_perf.add_trace(go.Bar(
        name="Engagement Score",
        x=genres,
        y=[71, 68, 75, 62, 58],
        marker_color=COLORS["purple"],
    ))
    fig_perf.add_trace(go.Bar(
        name="Retention Impact",
        x=genres,
        y=[65, 52, 61, 55, 41],
        marker_color=COLORS["green"],
    ))
    fig_perf.update_layout(**dark_chart_layout(
        height=320, barmode="stack",
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="Score"),
    ))
    left_chart = dcc.Graph(figure=fig_perf, config=CHART_CONFIG)

    # -- right panel: donut of investment allocation ------------------------
    right_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Original Series", "Licensed Content", "Live Sports",
                    "News & Talk", "Docs & Specials"],
            values=[42, 24, 18, 9, 7],
            colors=[COLORS["blue"], COLORS["green"], COLORS["yellow"],
                    COLORS["purple"], COLORS["red"]],
            center_text="$3.8B",
            title="Investment Allocation",
        ),
        config=CHART_CONFIG,
    )

    # -- bottom stats -------------------------------------------------------
    bottom = [
        ("Titles in Library", "14,280", "blue"),
        ("Avg Cost / Hour", "$1.2M", "purple"),
        ("Content ROI", "3.4x", "green"),
        ("Pipeline Titles", "312", "yellow"),
    ]

    return layout_split(
        title="Content Strategy",
        subtitle="Performance, pipeline, and investment analysis",
        tabs=["Performance", "Pipeline", "Licensing"],
        banner_text=(
            "Original series drive 3.4x higher subscriber retention than "
            "licensed content. Consider increasing original slate by 15% "
            "next quarter to maximize LTV."
        ),
        left_panel=("Content Performance by Genre", left_chart),
        right_panel=("Investment Allocation", right_donut),
        bottom_stats=bottom,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3.  AUDIENCE INTELLIGENCE  –  layout_table  (Style B)
# ═══════════════════════════════════════════════════════════════════════════

def render_audience_intel(cfg):
    """Audience intelligence view with filters, KPI strip, and a detailed
    audience segment table."""

    # -- filters ------------------------------------------------------------
    filters = [
        {"label": "Platform", "options": ["All Platforms"]},
        {"label": "Segment", "options": ["All Segments"]},
        {"label": "Region", "options": ["Global"]},
    ]

    # -- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "MAU", "value": "42.6M", "accent": "blue"},
        {"label": "Avg Watch Time", "value": "2h 14m", "accent": "green"},
        {"label": "Engagement Score", "value": "73.8", "accent": "purple"},
        {"label": "Demographics 18-34", "value": "38%", "accent": "yellow"},
    ]

    # -- audience segment table ---------------------------------------------
    columns = [
        {"name": "Segment", "id": "segment"},
        {"name": "Users", "id": "users"},
        {"name": "Watch Time", "id": "watch_time"},
        {"name": "Engagement", "id": "engagement"},
        {"name": "Top Genre", "id": "top_genre"},
        {"name": "Retention", "id": "retention"},
        {"name": "Status", "id": "status"},
    ]

    segments = [
        ("Binge Watchers", "8.4M", "4h 32m",  91, "Drama",    94, "Healthy"),
        ("Casual Viewers",  "14.2M", "1h 05m", 52, "Comedy",   68, "Low"),
        ("Sports Fans",     "6.8M", "2h 48m",  78, "Sports",   82, "Healthy"),
        ("Family Accounts", "5.1M", "3h 10m",  70, "Animation", 76, "Healthy"),
        ("News Seekers",    "3.9M", "0h 45m",  61, "News",     59, "Warning"),
        ("Music & Podcast", "2.7M", "1h 22m",  65, "Music",    71, "Low"),
        ("Late-Night",      "1.5M", "1h 48m",  58, "Reality",  53, "Critical"),
    ]

    data = [
        {
            "segment": seg,
            "users": users,
            "watch_time": wt,
            "engagement": f"{eng}%",
            "top_genre": genre,
            "retention": f"{ret}%",
            "status": status,
        }
        for seg, users, wt, eng, genre, ret, status in segments
    ]

    return layout_table(
        title="Audience Intelligence",
        subtitle="Segment behavior, demographics, and retention analysis",
        filters=filters,
        kpi_items=kpis,
        table_columns=columns,
        table_data=data,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4.  SUBSCRIPTION INTELLIGENCE  –  layout_forecast  (Style E)
# ═══════════════════════════════════════════════════════════════════════════

def render_subscription_intel(cfg):
    """Subscription intelligence with MRR hero, dual-axis growth/churn chart,
    and tier breakdown."""

    # -- KPI strip ----------------------------------------------------------
    kpis = [
        {"label": "MRR", "value": "$12.4M", "accent": "blue"},
        {"label": "Churn Rate", "value": "4.2%", "accent": "red"},
        {"label": "Avg LTV", "value": "$284", "accent": "green"},
        {"label": "Trial Conversion", "value": "62%", "accent": "purple"},
    ]

    # -- dual-axis chart: subscriber growth + churn -------------------------
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    subs_growth = [38.1, 39.4, 40.2, 40.8, 41.5, 42.3,
                   43.1, 44.0, 44.6, 45.8, 47.0, 48.2]
    churn_rate = [5.1, 4.9, 4.8, 4.7, 4.5, 4.4,
                  4.3, 4.4, 4.3, 4.2, 4.1, 4.2]

    fig_sub = go.Figure()
    fig_sub.add_trace(go.Bar(
        x=months, y=subs_growth, name="Subscribers (M)",
        marker_color=COLORS["blue"],
        yaxis="y",
    ))
    fig_sub.add_trace(go.Scatter(
        x=months, y=churn_rate, name="Churn %",
        mode="lines+markers",
        line=dict(color=COLORS["red"], width=2),
        marker=dict(size=6, color=COLORS["red"]),
        yaxis="y2",
    ))
    fig_sub.update_layout(**dark_chart_layout(
        height=300,
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="Subscribers (M)"),
        yaxis2=dict(showgrid=False, color=COLORS["red"],
                    overlaying="y", side="right",
                    title="Churn %", range=[0, 10]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_sub, config=CHART_CONFIG)

    # -- side panel: tier breakdown -----------------------------------------
    side = breakdown_list([
        {"label": "Premium 4K", "value": "$18.99", "pct": 34,
         "color": COLORS["blue"]},
        {"label": "Standard HD", "value": "$12.99", "pct": 41,
         "color": COLORS["green"]},
        {"label": "Basic w/ Ads", "value": "$6.99", "pct": 18,
         "color": COLORS["yellow"]},
        {"label": "Student Plan", "value": "$4.99", "pct": 5,
         "color": COLORS["purple"]},
        {"label": "Family Bundle", "value": "$24.99", "pct": 2,
         "color": COLORS["red"]},
    ])

    return layout_forecast(
        title="Subscription Intelligence",
        subtitle="Revenue, churn, and tier performance",
        kpi_items=kpis,
        hero_value="$12.4M",
        hero_label="Monthly Recurring Revenue",
        hero_trend_text="+8.3% vs prior quarter",
        main_chart=main_chart,
        side_component=side,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5.  AD YIELD & MONETIZATION  –  layout_grid  (Style F)
# ═══════════════════════════════════════════════════════════════════════════

def render_ad_yield(cfg):
    """Ad yield grid with gauge, sparklines, and key monetisation metrics."""

    # -- gauge: fill rate ---------------------------------------------------
    fill_gauge = html.Div(children=[
        html.Div("Ad Fill Rate", style={
            "fontSize": "13px", "fontWeight": "600",
            "color": COLORS["white"], "marginBottom": "8px"}),
        dcc.Graph(
            figure=gauge_figure(92.4, 100, title="", color=COLORS["green"]),
            config=CHART_CONFIG,
            style={"height": "180px"},
        ),
    ])

    # -- sparkline: CPM trend -----------------------------------------------
    cpm_vals = [8.2, 8.5, 8.1, 8.9, 9.3, 9.0, 9.6, 10.1, 9.8, 10.4,
                10.8, 11.2]
    cpm_spark = metric_with_sparkline(
        "CPM Trend", "$11.20", cpm_vals, accent="blue")

    # -- metric: ad impressions ---------------------------------------------
    impressions_card = html.Div(children=[
        html.Div("Ad Impressions", style={
            "fontSize": "11px", "color": COLORS["text_muted"],
            "textTransform": "uppercase", "letterSpacing": "0.4px",
            "marginBottom": "6px"}),
        html.Div("2.84B", style={
            "fontSize": "28px", "fontWeight": "700",
            "color": COLORS["blue"]}),
        html.Div(children=[
            trend_indicator("up", "+14.2% MoM"),
        ], style={"marginTop": "6px"}),
    ])

    # -- metric: eCPM -------------------------------------------------------
    ecpm_card = html.Div(children=[
        html.Div("Effective CPM", style={
            "fontSize": "11px", "color": COLORS["text_muted"],
            "textTransform": "uppercase", "letterSpacing": "0.4px",
            "marginBottom": "6px"}),
        html.Div("$14.60", style={
            "fontSize": "28px", "fontWeight": "700",
            "color": COLORS["green"]}),
        html.Div(children=[
            trend_indicator("up", "+7.8% QoQ"),
        ], style={"marginTop": "6px"}),
    ])

    # -- metric: viewability ------------------------------------------------
    viewability_card = html.Div(children=[
        html.Div("Viewability Rate", style={
            "fontSize": "11px", "color": COLORS["text_muted"],
            "textTransform": "uppercase", "letterSpacing": "0.4px",
            "marginBottom": "6px"}),
        html.Div("87.3%", style={
            "fontSize": "28px", "fontWeight": "700",
            "color": COLORS["purple"]}),
        progress_row("Above benchmark", "87.3%", 87, COLORS["purple"]),
    ])

    # -- sparkline: programmatic % ------------------------------------------
    prog_vals = [62, 64, 65, 67, 68, 70, 71, 72, 74, 73, 75, 76]
    prog_spark = metric_with_sparkline(
        "Programmatic %", "76%", prog_vals, accent="green")

    # -- metric: direct sold ------------------------------------------------
    direct_card = html.Div(children=[
        html.Div("Direct Sold Revenue", style={
            "fontSize": "11px", "color": COLORS["text_muted"],
            "textTransform": "uppercase", "letterSpacing": "0.4px",
            "marginBottom": "6px"}),
        html.Div("$4.5M", style={
            "fontSize": "28px", "fontWeight": "700",
            "color": COLORS["yellow"]}),
        html.Div(children=[
            trend_indicator("up", "+3.1% MoM"),
        ], style={"marginTop": "6px"}),
        html.Div("24% of total ad revenue", style={
            "fontSize": "12px", "color": COLORS["text_muted"],
            "marginTop": "8px"}),
    ])

    # -- revenue breakdown bar ----------------------------------------------
    fig_rev = go.Figure()
    ad_types = ["Pre-Roll", "Mid-Roll", "Display", "Native", "Sponsored"]
    fig_rev.add_trace(go.Bar(
        x=ad_types,
        y=[6.8, 5.2, 3.1, 2.4, 1.2],
        marker_color=[COLORS["blue"], COLORS["purple"], COLORS["green"],
                      COLORS["yellow"], COLORS["red"]],
        text=["$6.8M", "$5.2M", "$3.1M", "$2.4M", "$1.2M"],
        textposition="outside",
        textfont=dict(color=COLORS["white"], size=11),
    ))
    fig_rev.update_layout(**dark_chart_layout(
        height=210,
        showlegend=False,
        margin=dict(l=40, r=20, t=30, b=40),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="Revenue ($M)"),
    ))
    rev_chart_card = html.Div(children=[
        html.Div("Revenue by Ad Format", style={
            "fontSize": "13px", "fontWeight": "600",
            "color": COLORS["white"], "marginBottom": "8px"}),
        dcc.Graph(figure=fig_rev, config=CHART_CONFIG,
                  style={"height": "210px"}),
    ])

    grid_items = [
        {"col_span": 1, "row_span": 1, "content": fill_gauge},
        {"col_span": 1, "row_span": 1, "content": cpm_spark},
        {"col_span": 1, "row_span": 2, "content": viewability_card},
        {"col_span": 1, "row_span": 1, "content": impressions_card},
        {"col_span": 1, "row_span": 1, "content": ecpm_card},
        {"col_span": 2, "row_span": 1, "content": rev_chart_card},
        {"col_span": 1, "row_span": 1, "content": direct_card},
        {"col_span": 1, "row_span": 1, "content": prog_spark},
    ]

    return layout_grid(
        title="Ad Yield & Monetization",
        subtitle="Fill rate, CPM trends, and revenue optimization",
        grid_items=grid_items,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6.  PLATFORM & DELIVERY  –  layout_alerts  (Style D)
# ═══════════════════════════════════════════════════════════════════════════

def render_platform_delivery(cfg):
    """Platform delivery alerts showing CDN issues, buffering spikes, quality
    degradation, and latency warnings."""

    summary_kpis = [
        {"label": "Uptime", "value": "99.94%", "accent": "green"},
        {"label": "Active Alerts", "value": "4", "accent": "red"},
        {"label": "Avg Latency", "value": "38ms", "accent": "blue"},
        {"label": "Buffer Ratio", "value": "0.12%", "accent": "yellow"},
    ]

    alerts = [
        {
            "severity": "critical",
            "title": "CDN Edge Node Failure — US-East-1",
            "description": (
                "Primary edge node in us-east-1 region is returning 5xx "
                "errors at an elevated rate.  Failover to secondary node "
                "has been triggered automatically but viewer experience "
                "is degraded for approximately 2.1M active sessions."
            ),
            "impact": "Impact: ~2.1M viewers | Est. Revenue Loss: $42K/hr",
            "details": [
                ("Region", "US-East-1 (Virginia)"),
                ("Error Rate", "8.4% of requests (5xx)"),
                ("Start Time", "2026-03-10 14:23 UTC"),
                ("Failover Status", "Active — Secondary Node"),
            ],
            "timestamp": "12 min ago",
        },
        {
            "severity": "critical",
            "title": "Buffering Spike — Live Sports Stream",
            "description": (
                "Re-buffer rate spiked to 3.8% during the live basketball "
                "broadcast, exceeding the 1.5% SLA threshold.  Root cause "
                "identified as insufficient origin-shield capacity."
            ),
            "impact": "Impact: ~820K concurrent viewers",
            "details": [
                ("Event", "NBA — Lakers vs Celtics"),
                ("Re-buffer Rate", "3.8%  (SLA: <1.5%)"),
                ("Bitrate Drop", "4K to 1080p adaptive"),
                ("Concurrent Viewers", "820,400"),
            ],
            "timestamp": "34 min ago",
        },
        {
            "severity": "warning",
            "title": "Video Quality Degradation — EU-West Region",
            "description": (
                "Average delivered bitrate has dropped 22% in the EU-West "
                "region over the last 90 minutes.  ISP-level congestion "
                "detected on two major carriers."
            ),
            "impact": "Impact: ~1.4M viewers, quality downgraded",
            "details": [
                ("Region", "EU-West (Frankfurt / London)"),
                ("Avg Bitrate", "5.2 Mbps  (baseline 6.7 Mbps)"),
                ("Affected ISPs", "Deutsche Telekom, BT Group"),
                ("Resolution ETA", "~45 min (ISP mitigation)"),
            ],
            "timestamp": "1h 12min ago",
        },
        {
            "severity": "warning",
            "title": "API Latency Warning — Recommendation Service",
            "description": (
                "p99 latency for the recommendation API has risen to 480ms, "
                "above the 300ms warning threshold.  The personalization "
                "engine is experiencing increased load from A/B test cohort "
                "expansion."
            ),
            "impact": "Impact: Slower content discovery, -2% CTR",
            "details": [
                ("Service", "recommendation-api-v3"),
                ("p99 Latency", "480ms  (threshold: 300ms)"),
                ("Error Rate", "0.3%  (within SLA)"),
                ("Root Cause", "A/B test cohort expansion"),
            ],
            "timestamp": "2h 5min ago",
        },
        {
            "severity": "info",
            "title": "Scheduled CDN Cache Purge — APAC Region",
            "description": (
                "A planned cache purge for the APAC region is scheduled in "
                "2 hours.  Temporary increase in origin requests expected.  "
                "Auto-scaling rules are pre-configured."
            ),
            "impact": "Expected: +35% origin load for ~15 min",
            "details": [
                ("Region", "APAC (Tokyo / Sydney)"),
                ("Scheduled", "2026-03-10 18:00 UTC"),
                ("Duration", "~15 min"),
            ],
            "timestamp": "Scheduled",
        },
    ]

    return layout_alerts(
        title="Platform & Delivery",
        subtitle="Infrastructure health, CDN performance, and incident tracking",
        tabs=["Active", "Resolved", "Monitoring"],
        alerts=alerts,
        summary_kpis=summary_kpis,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7.  PERSONALIZATION & AI  –  layout_split  (Style C variant)
# ═══════════════════════════════════════════════════════════════════════════

def render_personalization_ai(cfg):
    """Personalization & AI view with A/B test results, recommendation engine
    click-through donut, and model performance stats."""

    # -- left panel: bar chart of A/B test results --------------------------
    tests = ["Hero Carousel\nv2", "Autoplay\nThreshold", "Thumbnail\nAI",
             "Row Ordering\nML", "Skip Intro\nTiming"]
    control = [4.2, 12.8, 6.1, 18.4, 32.0]
    variant = [5.8, 14.5, 8.9, 22.1, 28.6]

    fig_ab = go.Figure()
    fig_ab.add_trace(go.Bar(
        name="Control",
        x=tests,
        y=control,
        marker_color=COLORS["text_muted"],
        text=[f"{v}%" for v in control],
        textposition="outside",
        textfont=dict(color=COLORS["text_muted"], size=10),
    ))
    fig_ab.add_trace(go.Bar(
        name="Variant",
        x=tests,
        y=variant,
        marker_color=COLORS["blue"],
        text=[f"{v}%" for v in variant],
        textposition="outside",
        textfont=dict(color=COLORS["blue"], size=10),
    ))
    fig_ab.update_layout(**dark_chart_layout(
        height=320,
        barmode="group",
        legend=dict(orientation="h", y=-0.25, x=0.5, xanchor="center"),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"],
                   color=COLORS["text_muted"], title="Conversion %",
                   range=[0, 40]),
        xaxis=dict(tickangle=0),
        margin=dict(l=48, r=24, t=36, b=60),
    ))
    left_chart = dcc.Graph(figure=fig_ab, config=CHART_CONFIG)

    # -- right panel: donut of recommendation engine clicks -----------------
    right_donut = dcc.Graph(
        figure=donut_figure(
            labels=["Collaborative Filter", "Content-Based", "Trending",
                    "Editorial Picks", "Deep Learning"],
            values=[34, 24, 19, 13, 10],
            colors=[COLORS["blue"], COLORS["green"], COLORS["yellow"],
                    COLORS["purple"], COLORS["red"]],
            center_text="81%",
            title="Recommendation Click-Through",
        ),
        config=CHART_CONFIG,
    )

    # -- bottom stats -------------------------------------------------------
    bottom = [
        ("Model Accuracy", "94.2%", "green"),
        ("Personalization Lift", "+18.6%", "blue"),
        ("A/B Tests Active", "12", "purple"),
        ("Avg Inference", "23ms", "yellow"),
    ]

    return layout_split(
        title="Personalization & AI",
        subtitle="A/B testing, recommendation engines, and ML model performance",
        tabs=["A/B Tests", "Recommendations", "Segments"],
        banner_text=(
            "ML-driven personalization is delivering an 18.6% lift in "
            "engagement over rule-based recommendations.  The v3 deep-learning "
            "model shows 94.2% accuracy on next-watch prediction, up from "
            "89.7% on the prior version."
        ),
        left_panel=("A/B Test Results", left_chart),
        right_panel=("Recommendation Engine Clicks", right_donut),
        bottom_stats=bottom,
    )
