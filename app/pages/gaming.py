"""
Page renderers for the Gaming vertical of Blueprint IQ v2.

Each public ``render_*`` function accepts a ``cfg`` dict and returns an
``html.Div`` that Dash can render into the main content area.

Style assignments:
  render_dashboard     -> layout_executive  (Style A)
  render_know_player   -> layout_table      (Style B)
  render_grow_playerbase -> layout_split    (Style C)
  render_grow_revenue  -> layout_forecast   (Style E)
  render_build_games   -> layout_alerts     (Style D)
  render_live_ops      -> layout_grid       (Style F)
  render_efficient_ops -> layout_table      (Style B variant)
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
#  1. DASHBOARD  —  layout_executive  (Style A)
# ═══════════════════════════════════════════════════════════════════════════

def render_dashboard(cfg):
    """Executive gaming dashboard: DAU, Revenue, Retention heroes + charts."""

    # ── Hero metrics ──────────────────────────────────────────────────────
    heroes = [
        hero_metric("Daily Active Users", "1.24M",
                     trend_text="+6.3% vs last week", trend_dir="up",
                     accent="blue"),
        hero_metric("Daily Revenue", "$284K",
                     trend_text="+11.7% vs last week", trend_dir="up",
                     accent="green"),
        hero_metric("D7 Retention", "38.2%",
                     trend_text="-1.4% vs last week", trend_dir="down",
                     accent="purple"),
    ]

    # ── Main chart: Player trend (30 days) ────────────────────────────────
    days = [f"Feb {d}" for d in range(9, 28)] + [f"Mar {d}" for d in range(1, 11)]
    dau_values = [
        1.08, 1.11, 1.09, 1.14, 1.16, 1.12, 1.10, 1.15, 1.18, 1.17,
        1.13, 1.15, 1.19, 1.21, 1.18, 1.20, 1.22, 1.19, 1.17, 1.21,
        1.23, 1.20, 1.18, 1.22, 1.25, 1.23, 1.21, 1.24, 1.26, 1.24,
    ]
    new_users = [
        82, 87, 79, 91, 95, 84, 80, 93, 98, 96,
        88, 92, 99, 103, 94, 97, 105, 101, 93, 102,
        108, 100, 96, 104, 112, 106, 98, 107, 115, 110,
    ]

    fig_main = go.Figure()
    fig_main.add_trace(go.Scatter(
        x=days, y=dau_values, name="DAU (M)",
        mode="lines+markers",
        line=dict(color=COLORS["blue"], width=2),
        marker=dict(size=4),
        fill="tozeroy",
        fillcolor=f"rgba({_hex_to_rgb(COLORS['blue'])}, 0.08)",
    ))
    fig_main.add_trace(go.Scatter(
        x=days, y=[v / 1000 for v in new_users], name="New Users (K)",
        mode="lines",
        line=dict(color=COLORS["green"], width=2, dash="dot"),
        yaxis="y2",
    ))
    fig_main.update_layout(**dark_chart_layout(
        height=340,
        title=dict(text="Player Trends  —  Last 30 Days",
                   font=dict(size=14, color=COLORS["white"])),
        yaxis=dict(title="DAU (millions)", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        yaxis2=dict(title="New Users (K)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_main, config=CHART_CONFIG)

    # ── Bottom panel 1: Revenue breakdown donut ───────────────────────────
    rev_labels = ["In-App Purchases", "Ad Revenue", "Subscriptions", "Battle Pass"]
    rev_values = [142, 68, 48, 26]
    rev_colors = [COLORS["blue"], COLORS["green"], COLORS["purple"], COLORS["yellow"]]
    fig_donut = donut_figure(rev_labels, rev_values, rev_colors,
                             center_text="$284K", title="Revenue Mix")
    panel_donut = dcc.Graph(figure=fig_donut, config=CHART_CONFIG)

    # ── Bottom panel 2: Retention cohort chart ────────────────────────────
    cohort_days = ["D1", "D3", "D7", "D14", "D30"]
    cohort_organic = [62, 48, 38, 28, 18]
    cohort_paid = [55, 40, 31, 22, 13]

    fig_cohort = go.Figure()
    fig_cohort.add_trace(go.Scatter(
        x=cohort_days, y=cohort_organic, name="Organic",
        mode="lines+markers",
        line=dict(color=COLORS["green"], width=2),
        marker=dict(size=6),
    ))
    fig_cohort.add_trace(go.Scatter(
        x=cohort_days, y=cohort_paid, name="Paid",
        mode="lines+markers",
        line=dict(color=COLORS["purple"], width=2),
        marker=dict(size=6),
    ))
    fig_cohort.update_layout(**dark_chart_layout(
        height=280,
        yaxis=dict(title="Retention %", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.18, x=0.5, xanchor="center"),
    ))
    panel_cohort = dcc.Graph(figure=fig_cohort, config=CHART_CONFIG)

    # ── Assemble ──────────────────────────────────────────────────────────
    panels = [
        ("Revenue Breakdown", panel_donut),
        ("Retention Cohorts", panel_cohort),
    ]
    return layout_executive(
        title=cfg.get("title", "Gaming Dashboard"),
        subtitle=cfg.get("subtitle", "Real-time player and revenue overview"),
        heroes=heroes,
        main_chart=main_chart,
        panels=panels,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  2. KNOW YOUR PLAYER  —  layout_table  (Style B)
# ═══════════════════════════════════════════════════════════════════════════

def render_know_player(cfg):
    """Player segmentation table with LTV, engagement KPIs."""

    # ── Filters ───────────────────────────────────────────────────────────
    filters = [
        {"label": "Game", "options": ["All Games", "Clash Arena", "Farm Empire", "Space Drift"]},
        {"label": "Region", "options": ["Global", "NA", "EU", "APAC", "LATAM"]},
        {"label": "Segment", "options": ["All", "Whales", "Dolphins", "Minnows", "Free"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────────
    kpi_items = [
        {"label": "Avg LTV", "value": "$42.80", "accent": "blue"},
        {"label": "Avg CLV", "value": "$127.50", "accent": "green"},
        {"label": "Churn Rate", "value": "4.3%", "accent": "red"},
        {"label": "Avg Sessions/Day", "value": "3.7", "accent": "purple"},
        {"label": "Engagement Score", "value": "72/100", "accent": "yellow"},
    ]

    # ── Player segment table ──────────────────────────────────────────────
    headers = ["Segment", "Players", "Avg LTV", "ARPDAU", "Sessions/Day",
               "Engagement", "Churn", "Status"]

    segment_data = [
        ("Whales", "12,400", "$312.00", "$4.85", "6.2", 94, "1.1%", "Healthy"),
        ("Dolphins", "87,600", "$68.50", "$1.22", "4.8", 78, "2.8%", "Healthy"),
        ("Minnows", "342,000", "$18.20", "$0.34", "3.1", 62, "5.2%", "Warning"),
        ("Social Players", "198,000", "$8.40", "$0.12", "4.5", 71, "3.9%", "Healthy"),
        ("Free Riders", "604,000", "$1.10", "$0.02", "1.8", 38, "8.7%", "Critical"),
        ("Lapsed Whales", "4,200", "$285.00", "$0.00", "0.3", 12, "22.4%", "Critical"),
        ("New Users (7d)", "156,000", "$3.60", "$0.45", "2.9", 55, "N/A", "Info"),
    ]

    table_columns = [{"name": h, "id": h.lower().replace(" ", "_").replace("/", "_")} for h in headers]
    table_data = [
        {
            "segment": seg,
            "players": players,
            "avg_ltv": ltv,
            "arpdau": arpdau,
            "sessions_day": sessions,
            "engagement": str(engagement),
            "churn": churn,
            "status": status,
        }
        for seg, players, ltv, arpdau, sessions, engagement, churn, status in segment_data
    ]

    return layout_table(
        title=cfg.get("title", "Know Your Player"),
        subtitle=cfg.get("subtitle", "Player segmentation, LTV analysis, and engagement tracking"),
        filters=filters,
        kpi_items=kpi_items,
        table_columns=table_columns,
        table_data=table_data,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  3. GROW YOUR PLAYERBASE  —  layout_split  (Style C)
# ═══════════════════════════════════════════════════════════════════════════

def render_grow_playerbase(cfg):
    """Acquisition analytics with channel mix and campaign insights."""

    # ── Tabs ──────────────────────────────────────────────────────────────
    tabs = ["Acquisition", "Viral Growth", "Campaigns"]

    # ── Info banner ───────────────────────────────────────────────────────
    banner_text = (
        "TikTok install volume surged 34% this week while CPI dropped to $1.82. "
        "Consider shifting 15% of Google UAC budget to TikTok for improved ROI. "
        "Organic installs are trending up driven by recent App Store featuring."
    )

    # ── Left panel: Stacked bar chart (installs by channel) ───────────────
    weeks = ["W1 Feb", "W2 Feb", "W3 Feb", "W4 Feb", "W1 Mar"]
    channels = {
        "Organic":   [42, 45, 48, 51, 56],
        "Google UAC": [38, 36, 34, 33, 31],
        "TikTok":    [18, 22, 26, 31, 42],
        "Unity Ads": [15, 14, 16, 14, 15],
        "Facebook":  [24, 22, 20, 19, 18],
    }
    ch_colors = {
        "Organic": COLORS["green"],
        "Google UAC": COLORS["blue"],
        "TikTok": COLORS["purple"],
        "Unity Ads": COLORS["yellow"],
        "Facebook": COLORS["red"],
    }

    fig_bar = go.Figure()
    for ch_name, ch_vals in channels.items():
        fig_bar.add_trace(go.Bar(
            x=weeks, y=[v * 1000 for v in ch_vals], name=ch_name,
            marker_color=ch_colors[ch_name],
        ))
    fig_bar.update_layout(**dark_chart_layout(
        height=320, barmode="stack",
        yaxis=dict(title="Installs", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center",
                    font=dict(size=11)),
    ))
    left_chart = dcc.Graph(figure=fig_bar, config=CHART_CONFIG)

    # ── Right panel: Donut (channel mix this week) ────────────────────────
    mix_labels = list(channels.keys())
    mix_values = [ch[-1] for ch in channels.values()]
    mix_colors = [ch_colors[n] for n in mix_labels]
    fig_donut = donut_figure(mix_labels, mix_values, mix_colors,
                             center_text="162K", title="This Week")
    right_chart = dcc.Graph(figure=fig_donut, config=CHART_CONFIG)

    # ── Bottom stats ──────────────────────────────────────────────────────
    bottom_stats = [
        ("Total Installs", "162K", "blue"),
        ("Avg CPI", "$2.14", "green"),
        ("Organic Share", "34.6%", "purple"),
        ("D1 Install Retention", "48%", "yellow"),
    ]

    return layout_split(
        title=cfg.get("title", "Grow Your Playerbase"),
        subtitle=cfg.get("subtitle", "Acquisition channels, viral loops, and campaign performance"),
        tabs=tabs,
        banner_text=banner_text,
        left_panel=("Installs by Channel  —  Weekly", left_chart),
        right_panel=("Channel Mix  —  Current Week", right_chart),
        bottom_stats=bottom_stats,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4. GROW REVENUE  —  layout_forecast  (Style E)
# ═══════════════════════════════════════════════════════════════════════════

def render_grow_revenue(cfg):
    """Revenue analytics with ARPDAU trends and source breakdown."""

    # ── KPI strip ─────────────────────────────────────────────────────────
    kpi_items = [
        {"label": "ARPDAU", "value": "$0.229", "accent": "blue"},
        {"label": "IAP Revenue", "value": "$142K", "accent": "green"},
        {"label": "Ad Revenue", "value": "$68K", "accent": "purple"},
        {"label": "ARPPU", "value": "$18.40", "accent": "yellow"},
    ]

    # ── Main chart: dual-axis bar (revenue) + line (ARPDAU) ──────────────
    days_14 = [f"Feb {d}" for d in range(25, 29)] + [f"Mar {d}" for d in range(1, 11)]
    iap_rev = [138, 141, 135, 144, 148, 142, 139, 146, 152, 149,
               143, 147, 155, 142]
    ad_rev = [62, 64, 59, 66, 68, 65, 63, 67, 71, 69,
              64, 68, 73, 68]
    arpdau = [0.208, 0.213, 0.202, 0.218, 0.225, 0.215, 0.210, 0.222,
              0.232, 0.227, 0.216, 0.224, 0.237, 0.229]

    fig_rev = go.Figure()
    fig_rev.add_trace(go.Bar(
        x=days_14, y=iap_rev, name="IAP Revenue ($K)",
        marker_color=COLORS["blue"], opacity=0.85,
    ))
    fig_rev.add_trace(go.Bar(
        x=days_14, y=ad_rev, name="Ad Revenue ($K)",
        marker_color=COLORS["green"], opacity=0.85,
    ))
    fig_rev.add_trace(go.Scatter(
        x=days_14, y=arpdau, name="ARPDAU ($)",
        mode="lines+markers",
        line=dict(color=COLORS["yellow"], width=2),
        marker=dict(size=5),
        yaxis="y2",
    ))
    fig_rev.update_layout(**dark_chart_layout(
        height=320, barmode="stack",
        yaxis=dict(title="Revenue ($K)", showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        yaxis2=dict(title="ARPDAU ($)", overlaying="y", side="right",
                    showgrid=False, color=COLORS["text_muted"]),
        legend=dict(orientation="h", y=-0.22, x=0.5, xanchor="center"),
    ))
    main_chart = dcc.Graph(figure=fig_rev, config=CHART_CONFIG)

    # ── Side: Revenue source breakdown ────────────────────────────────────
    side = breakdown_list([
        {"label": "Gem Packs", "value": "$82K", "pct": 29, "color": COLORS["blue"]},
        {"label": "Battle Pass", "value": "$38K", "pct": 13, "color": COLORS["purple"]},
        {"label": "Cosmetics", "value": "$22K", "pct": 8, "color": COLORS["green"]},
        {"label": "Rewarded Ads", "value": "$42K", "pct": 15, "color": COLORS["yellow"]},
        {"label": "Interstitial Ads", "value": "$26K", "pct": 9, "color": COLORS["red"]},
        {"label": "Subscriptions", "value": "$48K", "pct": 17, "color": COLORS["blue"]},
        {"label": "Season Pass", "value": "$26K", "pct": 9, "color": COLORS["purple"]},
    ])

    return layout_forecast(
        title=cfg.get("title", "Grow Revenue"),
        subtitle=cfg.get("subtitle", "Revenue optimization, ARPDAU trends, and monetization mix"),
        kpi_items=kpi_items,
        hero_value="$284K",
        hero_label="Daily Revenue",
        hero_trend_text="+11.7% vs last week",
        main_chart=main_chart,
        side_component=html.Div([
            html.Div("Revenue Sources", style={
                "fontSize": "14px", "fontWeight": "600",
                "color": COLORS["white"], "marginBottom": "16px",
            }),
            side,
        ]),
    )


# ═══════════════════════════════════════════════════════════════════════════
#  5. BUILD GAMES  —  layout_alerts  (Style D)
# ═══════════════════════════════════════════════════════════════════════════

def render_build_games(cfg):
    """Build health dashboard showing failures, crashes, QA issues."""

    # ── Tabs ──────────────────────────────────────────────────────────────
    tabs = ["Active Issues", "Resolved", "All"]

    # ── Summary KPIs ──────────────────────────────────────────────────────
    summary_kpis = [
        {"label": "Open Issues", "value": "14", "accent": "red"},
        {"label": "Crash Rate", "value": "0.42%", "accent": "yellow"},
        {"label": "Build Success", "value": "94.1%", "accent": "green"},
        {"label": "Avg Fix Time", "value": "3.2h", "accent": "blue"},
    ]

    # ── Alert cards ───────────────────────────────────────────────────────
    alerts = [
        {
            "severity": "critical",
            "title": "Clash Arena  —  Android Build Failure",
            "description": (
                "The release/2.8.1 branch fails during the NDK linking step. "
                "Affected modules: libgame_core.so, libnetwork.so. The CI pipeline "
                "has been blocked for 4 hours. Rollback to 2.8.0 recommended if "
                "unresolved within 1 hour."
            ),
            "impact": "Est. revenue loss: $18K/hr (blocked release)",
            "details": [
                ("Build ID", "CA-20260310-7841"),
                ("Pipeline", "android-release-arm64"),
                ("Failing Step", "ndk-link-r26b"),
                ("Logs", "/ci/clash-arena/build-7841.log"),
                ("Assigned", "Platform Team — J. Martinez"),
            ],
            "timestamp": "2h 14m ago",
        },
        {
            "severity": "critical",
            "title": "Farm Empire  —  Crash Spike (iOS 18.3)",
            "description": (
                "Crash rate jumped from 0.08% to 1.2% after iOS 18.3 update. "
                "Stack traces point to Metal shader compilation in the rendering "
                "pipeline. Affects iPhone 15 Pro and iPad Pro M4 devices."
            ),
            "impact": "~42K users impacted — 1-star reviews increasing",
            "details": [
                ("Crash Group", "FE-CRASH-9182"),
                ("OS Version", "iOS 18.3 (22D60)"),
                ("Devices", "iPhone 15 Pro, iPad Pro M4"),
                ("Top Frame", "MetalShaderCompiler::linkProgram()"),
                ("Sessions Affected", "42,381"),
            ],
            "timestamp": "48m ago",
        },
        {
            "severity": "warning",
            "title": "Space Drift  —  QA Regression in Matchmaking",
            "description": (
                "Automated QA suite detected 12 failing tests in the matchmaking "
                "module after the skill-based ranking refactor. Players may be "
                "matched with opponents 3+ tiers apart, leading to poor experience."
            ),
            "impact": "Est. DAU impact: -2.4% if shipped",
            "details": [
                ("Test Suite", "matchmaking_integration_v4"),
                ("Failing Tests", "12 / 87"),
                ("Branch", "feature/sbmm-v3"),
                ("Regression Since", "commit a3f821c"),
            ],
            "timestamp": "1h 32m ago",
        },
        {
            "severity": "warning",
            "title": "Clash Arena  —  Memory Leak in Replay System",
            "description": (
                "Long sessions (>45 min) show increasing heap allocation in the "
                "replay buffer. Memory grows ~8 MB/min after the 30-minute mark. "
                "Low-end Android devices trigger OOM kills."
            ),
            "impact": "Session length capped; affects 18% of Android DAU",
            "details": [
                ("Issue", "CA-MEM-4421"),
                ("Repro Rate", "100% after 45m"),
                ("Heap Growth", "~8 MB/min"),
                ("Affected Devices", "RAM <= 4 GB"),
            ],
            "timestamp": "3h 5m ago",
        },
        {
            "severity": "info",
            "title": "Farm Empire  —  Asset Bundle Size Exceeds Target",
            "description": (
                "The Spring Festival asset bundle is 124 MB, exceeding the 100 MB "
                "target. This may increase download abandonment on cellular. "
                "Texture compression and mesh LOD pass recommended."
            ),
            "impact": "Projected +3.8% download abandonment",
            "details": [
                ("Bundle", "spring_festival_2026_v2"),
                ("Current Size", "124 MB"),
                ("Target", "100 MB"),
                ("Largest Assets", "env_blossom_4k.ktx2 (31 MB)"),
            ],
            "timestamp": "5h 18m ago",
        },
    ]

    return layout_alerts(
        title=cfg.get("title", "Build Games"),
        subtitle=cfg.get("subtitle", "Build health, crash reports, and QA pipeline status"),
        tabs=tabs,
        alerts=alerts,
        summary_kpis=summary_kpis,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  6. LIVE OPS  —  layout_grid  (Style F)
# ═══════════════════════════════════════════════════════════════════════════

def render_live_ops(cfg):
    """Live operations grid: events, battle pass, server health."""

    # ── Grid item 1: Event participation gauge ────────────────────────────
    fig_gauge = gauge_figure(
        value=73.4, max_val=100,
        title="Event Participation %",
        color=COLORS["green"],
    )
    gauge_panel = html.Div([
        html.Div("Spring Festival Event", style={
            "fontSize": "14px", "fontWeight": "600",
            "color": COLORS["white"], "marginBottom": "12px",
        }),
        dcc.Graph(figure=fig_gauge, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div("912K / 1.24M players active", style={
            "fontSize": "12px", "color": COLORS["text_muted"],
            "textAlign": "center", "marginTop": "4px",
        }),
    ])

    # ── Grid item 2: Events/sec sparkline ─────────────────────────────────
    events_sec_values = [
        4200, 4350, 4180, 4500, 4620, 4480, 4310, 4550, 4700, 4650,
        4420, 4580, 4720, 4800, 4680, 4750, 4820, 4900, 4780, 4850,
    ]
    events_panel = metric_with_sparkline(
        title="Events / Second",
        value="4,850",
        sparkline_values=events_sec_values,
        accent="blue",
    )

    # ── Grid item 3: Content ROI (tall, spans 2 rows) ────────────────────
    content_items = [
        ("Spring Festival", "$186K", 92, COLORS["green"]),
        ("Battle Pass S14", "$142K", 78, COLORS["blue"]),
        ("Weekend Tourney", "$67K", 64, COLORS["purple"]),
        ("Flash Sale (gems)", "$48K", 55, COLORS["yellow"]),
        ("New Hero: Zephyr", "$34K", 42, COLORS["blue"]),
        ("Clan Wars Event", "$28K", 38, COLORS["green"]),
    ]
    content_roi = html.Div([
        html.Div("Content ROI Ranking", style={
            "fontSize": "14px", "fontWeight": "600",
            "color": COLORS["white"], "marginBottom": "16px",
        }),
    ] + [progress_row(label, val, pct, color)
         for label, val, pct, color in content_items])

    # ── Grid item 4: Battle pass progress (wide, 2 cols) ─────────────────
    tiers = [f"T{i}" for i in range(1, 21)]
    tier_completion = [
        98, 96, 94, 91, 88, 84, 80, 76, 71, 66,
        60, 54, 48, 42, 36, 30, 24, 18, 13, 8,
    ]
    fig_bp = go.Figure()
    fig_bp.add_trace(go.Bar(
        x=tiers, y=tier_completion,
        marker=dict(
            color=tier_completion,
            colorscale=[[0, COLORS["red"]], [0.5, COLORS["yellow"]], [1, COLORS["green"]]],
        ),
        text=[f"{v}%" for v in tier_completion],
        textposition="outside",
        textfont=dict(size=10, color=COLORS["text_muted"]),
    ))
    fig_bp.update_layout(**dark_chart_layout(
        height=200,
        yaxis=dict(title="% Reached", range=[0, 110], showgrid=True,
                   gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        title=dict(text="Battle Pass S14  —  Tier Completion",
                   font=dict(size=13, color=COLORS["white"])),
        margin=dict(l=48, r=24, t=44, b=36),
    ))
    bp_panel = html.Div([
        dcc.Graph(figure=fig_bp, config=CHART_CONFIG, style={"height": "200px"}),
        html.Div(
            style={"display": "flex", "gap": "24px", "marginTop": "8px"},
            children=[
                html.Span("Avg Tier: 11.4", style={
                    "fontSize": "12px", "color": COLORS["text_muted"]}),
                html.Span("Premium: 28%", style={
                    "fontSize": "12px", "color": COLORS["purple"]}),
                html.Span("Revenue: $142K", style={
                    "fontSize": "12px", "color": COLORS["green"]}),
            ],
        ),
    ])

    # ── Grid item 5: Live event revenue metric ────────────────────────────
    live_rev_values = [
        186, 192, 188, 196, 204, 198, 194, 202, 210, 208,
        196, 204, 212, 218, 214, 220, 226, 222, 218, 224,
    ]
    live_rev_panel = metric_with_sparkline(
        title="Live Event Revenue",
        value="$224K",
        sparkline_values=live_rev_values,
        accent="green",
    )

    # ── Grid item 6: Server uptime gauge ──────────────────────────────────
    fig_uptime = gauge_figure(
        value=99.94, max_val=100,
        title="Server Uptime %",
        color=COLORS["blue"],
    )
    uptime_panel = html.Div([
        dcc.Graph(figure=fig_uptime, config=CHART_CONFIG,
                  style={"height": "180px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                    "marginTop": "4px"},
            children=[
                html.Span("SLA: 99.9%", style={
                    "fontSize": "11px", "color": COLORS["text_muted"]}),
                html.Span("Incidents: 1", style={
                    "fontSize": "11px", "color": COLORS["yellow"]}),
            ],
        ),
    ])

    # ── Assemble grid ─────────────────────────────────────────────────────
    grid_items = [
        {"col_span": 1, "row_span": 1, "content": gauge_panel},
        {"col_span": 1, "row_span": 1, "content": events_panel},
        {"col_span": 1, "row_span": 2, "content": content_roi},
        {"col_span": 2, "row_span": 1, "content": bp_panel},
        {"col_span": 1, "row_span": 1, "content": live_rev_panel},
        {"col_span": 1, "row_span": 1, "content": uptime_panel},
    ]

    return layout_grid(
        title=cfg.get("title", "Live Ops"),
        subtitle=cfg.get("subtitle", "Live events, battle pass, content ROI, and server health"),
        grid_items=grid_items,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  7. EFFICIENT OPS  —  layout_table  (Style B variant)
# ═══════════════════════════════════════════════════════════════════════════

def render_efficient_ops(cfg):
    """Infrastructure operations: uptime, latency, cost per region."""

    # ── Filters ───────────────────────────────────────────────────────────
    filters = [
        {"label": "Region", "options": ["All Regions", "US-East", "US-West", "EU-West",
                                         "EU-North", "APAC-SE", "APAC-NE"]},
        {"label": "Service", "options": ["All Services", "Game Servers", "Matchmaking",
                                          "Auth", "Analytics", "CDN"]},
    ]

    # ── KPI strip ─────────────────────────────────────────────────────────
    kpi_items = [
        {"label": "Global Uptime", "value": "99.94%", "accent": "green"},
        {"label": "MTTR", "value": "4.2 min", "accent": "blue"},
        {"label": "Cost / User", "value": "$0.0031", "accent": "purple"},
        {"label": "P99 Latency", "value": "42ms", "accent": "yellow"},
        {"label": "Cache Hit Rate", "value": "94.7%", "accent": "green"},
    ]

    # ── Server region table ───────────────────────────────────────────────
    headers = ["Region", "Status", "Uptime", "Capacity", "P99 Latency",
               "Cost/hr", "Utilization", "Instances"]

    region_data = [
        ("US-East-1", "Healthy", "99.98%", "82K CCU", "28ms", "$142/hr", 78, "48"),
        ("US-West-2", "Healthy", "99.96%", "64K CCU", "32ms", "$118/hr", 72, "38"),
        ("EU-West-1", "Healthy", "99.99%", "96K CCU", "24ms", "$168/hr", 84, "56"),
        ("EU-North-1", "Warning", "99.87%", "38K CCU", "38ms", "$86/hr", 91, "24"),
        ("APAC-SE-1", "Healthy", "99.95%", "124K CCU", "36ms", "$198/hr", 68, "64"),
        ("APAC-NE-1", "Healthy", "99.92%", "108K CCU", "42ms", "$172/hr", 74, "52"),
        ("SA-East-1", "Warning", "99.82%", "22K CCU", "58ms", "$48/hr", 88, "14"),
        ("ME-South-1", "Healthy", "99.91%", "18K CCU", "46ms", "$42/hr", 62, "12"),
    ]

    table_columns = [{"name": h, "id": h.lower().replace(" ", "_").replace("/", "_")} for h in headers]
    table_data = [
        {
            "region": region,
            "status": status,
            "uptime": uptime,
            "capacity": capacity,
            "p99_latency": latency,
            "cost_hr": cost,
            "utilization": str(util_pct),
            "instances": instances,
        }
        for region, status, uptime, capacity, latency, cost, util_pct, instances in region_data
    ]

    return layout_table(
        title=cfg.get("title", "Efficient Ops"),
        subtitle=cfg.get("subtitle", "Infrastructure health, cost efficiency, and regional performance"),
        filters=filters,
        kpi_items=kpi_items,
        table_columns=table_columns,
        table_data=table_data,
    )
