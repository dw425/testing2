"""Shared page layout builders for Blueprint IQ.

Six distinct page styles inspired by the Lakehouse Optimizer app:
  A (Executive)  - hero metrics, main chart, side panels
  B (Table)      - filter bar, KPI strip, interactive DataTable
  C (Split)      - real tabs (dcc.Tabs), info banner, side-by-side panels
  D (Alerts)     - real tabs, expandable alert/incident cards
  E (Forecast)   - KPI strip, hero number, dual-axis chart
  F (Grid)       - multi-panel grid with varied sizes
"""

from dash import dcc, html, dash_table
import plotly.graph_objects as go
from app.theme import COLORS, FONT_FAMILY, STATUS_COLORS

# ── Chart helpers ───────────────────────────────────────────────────────────

CHART_CONFIG = {
    "displayModeBar": "hover",
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
}

ACCENT_ICONS = {
    "blue": "fa-chart-line",
    "purple": "fa-bolt",
    "green": "fa-arrow-trend-up",
    "red": "fa-triangle-exclamation",
    "yellow": "fa-circle-exclamation",
}


def dark_chart_layout(**overrides):
    """Plotly layout dict for dark-themed charts."""
    base = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=FONT_FAMILY, color=COLORS["text_muted"]),
        margin=dict(l=48, r=24, t=36, b=36),
        height=300,
        xaxis=dict(showgrid=False, color=COLORS["text_muted"]),
        yaxis=dict(showgrid=True, gridcolor=COLORS["border"], color=COLORS["text_muted"]),
        legend=dict(font=dict(color=COLORS["text_muted"], size=11)),
    )
    base.update(overrides)
    return base


# ── Component builders ──────────────────────────────────────────────────────

def page_header(title, subtitle=""):
    children = [html.H1(title, style={"fontSize": "22px", "fontWeight": "700",
                                       "color": COLORS["white"], "margin": "0"})]
    if subtitle:
        children.append(html.P(subtitle, style={"fontSize": "13px",
                                                  "color": COLORS["text_muted"], "margin": "4px 0 0 0"}))
    return html.Div(className="page-header", children=children)


def hero_metric(title, value, trend_text="", trend_dir="up", accent="blue", href=None):
    """Large hero metric card for executive dashboards.

    When *href* is provided the card becomes a clickable link.
    """
    accent_color = COLORS.get(accent, COLORS["blue"])
    arrow = "\u25b2" if trend_dir == "up" else "\u25bc"
    trend_color = COLORS["green"] if trend_dir == "up" else COLORS["red"]

    children = [
        html.Div(title, style={"fontSize": "12px", "color": COLORS["text_muted"],
                                "textTransform": "uppercase", "letterSpacing": "0.5px",
                                "marginBottom": "8px", "fontWeight": "600"}),
        html.Div(value, style={"fontSize": "36px", "fontWeight": "700",
                                "color": accent_color, "lineHeight": "1.1"}),
    ]
    if trend_text:
        children.append(html.Div(
            children=[
                html.Span(f"{arrow} ", style={"color": trend_color}),
                html.Span(trend_text, style={"color": trend_color, "fontWeight": "600"}),
            ],
            style={"fontSize": "13px", "marginTop": "8px"},
        ))

    cls = "card clickable-card" if href else "card"
    card = html.Div(className=cls, style={"padding": "24px", "minHeight": "130px"}, children=children)

    if href:
        return dcc.Link(card, href=href, style={"textDecoration": "none", "color": "inherit"})
    return card


def compact_kpi(label, value, accent="blue"):
    """Small inline KPI chip."""
    accent_color = COLORS.get(accent, COLORS["blue"])
    return html.Div(
        style={"backgroundColor": COLORS["panel"], "border": f"1px solid {COLORS['border']}",
               "borderRadius": "10px", "padding": "14px 18px", "flex": "1", "minWidth": "120px"},
        children=[
            html.Div(label, style={"fontSize": "11px", "color": COLORS["text_muted"],
                                    "textTransform": "uppercase", "letterSpacing": "0.4px",
                                    "marginBottom": "4px"}),
            html.Div(value, style={"fontSize": "20px", "fontWeight": "700", "color": accent_color}),
        ],
    )


def kpi_strip(items):
    """Horizontal row of compact KPIs. items: [{label, value, accent}]"""
    return html.Div(
        style={"display": "flex", "gap": "12px", "marginBottom": "20px", "flexWrap": "wrap"},
        children=[compact_kpi(i["label"], i["value"], i.get("accent", "blue")) for i in items],
    )


def filter_bar(filters):
    """Interactive filter bar using dcc.Dropdown. filters: [{label, options}]"""
    children = []
    for i, f in enumerate(filters):
        opts = f.get("options", ["All"])
        options = [{"label": o, "value": o} for o in opts]
        children.append(html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "8px", "minWidth": "180px"},
            children=[
                html.Span(f["label"], style={"fontSize": "12px", "color": COLORS["text_muted"],
                                              "fontWeight": "500", "whiteSpace": "nowrap"}),
                dcc.Dropdown(
                    options=options,
                    value=opts[0] if opts else "All",
                    clearable=False,
                    style={"width": "150px", "fontSize": "12px"},
                ),
            ],
        ))
    return html.Div(
        style={"display": "flex", "gap": "12px", "marginBottom": "16px", "flexWrap": "wrap"},
        children=children,
    )


def interactive_tabs(tab_contents):
    """Real tab switching using dcc.Tabs.
    tab_contents: list of (label, component) tuples
    """
    _tab_style = {
        "backgroundColor": "transparent",
        "border": "none",
        "borderBottom": "2px solid transparent",
        "color": COLORS["text_muted"],
        "padding": "10px 20px",
        "fontFamily": FONT_FAMILY,
        "fontSize": "13px",
        "fontWeight": "600",
    }
    _selected_style = dict(_tab_style)
    _selected_style.update({
        "color": COLORS["white"],
        "borderBottom": f"2px solid {COLORS['blue']}",
    })

    children = []
    for i, (label, content) in enumerate(tab_contents):
        children.append(dcc.Tab(
            label=label,
            value=f"tab-{i}",
            children=[html.Div(content, style={"paddingTop": "20px"})],
            style=_tab_style,
            selected_style=_selected_style,
        ))

    return dcc.Tabs(
        value="tab-0",
        children=children,
        style={"borderBottom": f"1px solid {COLORS['border']}"},
        colors={"border": COLORS["border"], "primary": COLORS["blue"],
                "background": "transparent"},
    )


# Keep old tab_bar for backward compat but mark deprecated
def tab_bar(tabs, active_idx=0):
    """DEPRECATED: Use interactive_tabs() instead."""
    return interactive_tabs([(t, html.Div()) for t in tabs])


def info_banner(text, icon="fa-circle-info"):
    """Colored info/insight callout banner."""
    return html.Div(
        style={"backgroundColor": "rgba(75, 123, 245, 0.08)",
               "border": "1px solid rgba(75, 123, 245, 0.25)",
               "borderLeft": f"3px solid {COLORS['blue']}",
               "borderRadius": "8px", "padding": "14px 18px", "marginBottom": "20px",
               "display": "flex", "alignItems": "flex-start", "gap": "12px"},
        children=[
            html.I(className=f"fa-solid {icon}",
                   style={"color": COLORS["blue"], "fontSize": "14px", "marginTop": "2px"}),
            html.Span(text, style={"fontSize": "13px", "color": COLORS["white"],
                                    "lineHeight": "1.5"}),
        ],
    )


def alert_card(severity, title, description, impact=None, details=None, timestamp=None):
    """Alert/incident card with severity indicator."""
    sev_colors = {"critical": COLORS["red"], "warning": COLORS["yellow"],
                  "info": COLORS["blue"], "healthy": COLORS["green"]}
    sev_color = sev_colors.get(severity, COLORS["blue"])
    sev_bg = f"rgba({_hex_to_rgb(sev_color)}, 0.08)"

    header_children = [
        html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "10px", "flex": "1"},
            children=[
                html.Div(style={"width": "10px", "height": "10px", "borderRadius": "50%",
                                 "backgroundColor": sev_color, "flexShrink": "0",
                                 "boxShadow": f"0 0 6px {sev_color}50"}),
                html.Span(title, style={"fontSize": "14px", "fontWeight": "600",
                                         "color": COLORS["white"]}),
            ],
        ),
    ]
    if timestamp:
        header_children.append(html.Span(timestamp, style={"fontSize": "11px",
                                                             "color": COLORS["text_muted"]}))

    card_children = [
        html.Div(style={"display": "flex", "justifyContent": "space-between",
                         "alignItems": "center", "marginBottom": "8px"},
                 children=header_children),
        html.P(description, style={"fontSize": "13px", "color": COLORS["text_muted"],
                                    "lineHeight": "1.5", "margin": "0 0 8px 20px"}),
    ]

    if impact:
        impact_style = {"fontSize": "12px", "fontWeight": "600", "color": sev_color,
                        "backgroundColor": sev_bg, "padding": "4px 10px",
                        "borderRadius": "6px", "display": "inline-block", "marginLeft": "20px"}
        card_children.append(html.Span(impact, style=impact_style))

    if details:
        detail_children = []
        for row in details:
            detail_children.append(html.Tr([
                html.Td(row[0], style={"padding": "6px 12px", "fontSize": "12px",
                                        "color": COLORS["text_muted"]}),
                html.Td(row[1], style={"padding": "6px 12px", "fontSize": "12px",
                                        "color": COLORS["white"], "fontWeight": "500"}),
            ]))
        card_children.append(html.Table(
            style={"width": "100%", "borderCollapse": "collapse", "marginTop": "12px",
                    "marginLeft": "20px"},
            children=[html.Tbody(detail_children)],
        ))

    return html.Div(
        className="card",
        style={"borderLeft": f"3px solid {sev_color}", "marginBottom": "12px",
               "padding": "18px 20px"},
        children=card_children,
    )


def progress_row(label, value_text, pct, color):
    """Progress bar with label."""
    return html.Div(
        style={"marginBottom": "14px"},
        children=[
            html.Div(
                style={"display": "flex", "justifyContent": "space-between", "marginBottom": "6px"},
                children=[
                    html.Span(label, style={"fontSize": "13px", "color": COLORS["white"],
                                             "fontWeight": "500"}),
                    html.Span(value_text, style={"fontSize": "13px", "color": color,
                                                  "fontWeight": "600", "fontFamily": "monospace"}),
                ],
            ),
            html.Div(
                style={"width": "100%", "height": "8px", "backgroundColor": COLORS["border"],
                        "borderRadius": "4px", "overflow": "hidden"},
                children=[html.Div(style={"width": f"{pct}%", "height": "100%",
                                           "backgroundColor": color, "borderRadius": "4px"})],
            ),
        ],
    )


def stat_card(label, value, accent="blue"):
    """Small stat card for bottom stat rows."""
    accent_color = COLORS.get(accent, COLORS["blue"])
    return html.Div(
        style={"backgroundColor": COLORS["panel"], "border": f"1px solid {COLORS['border']}",
               "borderRadius": "10px", "padding": "16px", "textAlign": "center", "flex": "1"},
        children=[
            html.Div(value, style={"fontSize": "22px", "fontWeight": "700", "color": accent_color}),
            html.Div(label, style={"fontSize": "11px", "color": COLORS["text_muted"],
                                    "textTransform": "uppercase", "letterSpacing": "0.4px",
                                    "marginTop": "4px"}),
        ],
    )


_data_table_counter = {"n": 0}


def data_table(columns, data, page_size=10, style_conditions=None):
    """Interactive DataTable with native sort/filter/pagination.
    columns: list of {"name": "Display Name", "id": "col_id"} dicts
    data: list of row dicts

    Auto-applies color coding to status, severity, and risk_grade columns.
    Each table gets a unique pattern-matching ID for row-click callbacks.
    """
    if not data or not columns:
        return html.Div(
            className="card",
            style={"padding": "40px", "textAlign": "center"},
            children=[
                html.I(className="fa-solid fa-table",
                       style={"fontSize": "32px", "color": COLORS["text_muted"],
                              "marginBottom": "12px", "display": "block"}),
                html.Div("No data available",
                         style={"color": COLORS["text_muted"], "fontSize": "14px"}),
            ],
        )
    col_ids = {c["id"] for c in columns} if columns else set()

    conditions = [
        {"if": {"row_index": "odd"}, "backgroundColor": "rgba(255,255,255,0.03)"},
    ]

    # Auto-style status-like columns (status, risk_status, health)
    _STATUS_MAP = {
        "Healthy": COLORS["green"],
        "Nominal": COLORS["blue"],
        "Warning": COLORS["yellow"],
        "Critical": COLORS["red"],
        "Low": COLORS["text_muted"],
        "Info": COLORS["blue"],
    }
    for col in ("status", "risk_status", "health"):
        if col in col_ids:
            for val, color in _STATUS_MAP.items():
                conditions.append({
                    "if": {"filter_query": f'{{{col}}} = "{val}"', "column_id": col},
                    "color": color, "fontWeight": "600",
                })
            # Subtle row tint for Critical rows
            conditions.append({
                "if": {"filter_query": f'{{{col}}} = "Critical"'},
                "backgroundColor": "rgba(248, 113, 113, 0.06)",
            })

    # Auto-style severity columns
    _SEVERITY_MAP = {
        "Critical": COLORS["red"], "High": COLORS["red"],
        "Medium": COLORS["yellow"], "Low": COLORS["green"],
    }
    if "severity" in col_ids:
        for val, color in _SEVERITY_MAP.items():
            conditions.append({
                "if": {"filter_query": f'{{severity}} = "{val}"', "column_id": "severity"},
                "color": color, "fontWeight": "600",
            })

    # Auto-style risk_grade columns
    _GRADE_MAP = {"A": COLORS["green"], "B": COLORS["blue"],
                  "C": COLORS["yellow"], "D": COLORS["red"]}
    if "risk_grade" in col_ids:
        for val, color in _GRADE_MAP.items():
            conditions.append({
                "if": {"filter_query": f'{{risk_grade}} = "{val}"', "column_id": "risk_grade"},
                "color": color, "fontWeight": "600",
            })

    if style_conditions:
        conditions.extend(style_conditions)

    _data_table_counter["n"] += 1
    table_id = {"type": "interactive-table", "index": _data_table_counter["n"]}

    return html.Div(
        className="card",
        style={"padding": "0", "overflow": "hidden"},
        children=[
            dash_table.DataTable(
            id=table_id,
            columns=columns,
            data=data,
            sort_action="native",
            filter_action="native",
            page_action="native",
            page_size=page_size,
            row_selectable="single",
            export_format="csv",
            export_headers="display",
            style_table={"overflowX": "auto"},
            style_cell={
                "backgroundColor": COLORS["panel"],
                "color": COLORS["white"],
                "border": f"1px solid {COLORS['border']}",
                "fontSize": "13px",
                "fontFamily": FONT_FAMILY,
                "padding": "10px 14px",
                "textAlign": "left",
                "minWidth": "80px",
                "maxWidth": "250px",
                "whiteSpace": "normal",
            },
            style_header={
                "backgroundColor": COLORS["dark"],
                "color": COLORS["text_muted"],
                "fontWeight": "600",
                "fontSize": "11px",
                "textTransform": "uppercase",
                "letterSpacing": "0.5px",
                "border": f"1px solid {COLORS['border']}",
                "padding": "12px 14px",
            },
            style_filter={
                "backgroundColor": COLORS["dark"],
                "color": COLORS["white"],
                "border": f"1px solid {COLORS['border']}",
                "padding": "4px 8px",
            },
            style_data_conditional=conditions,
            style_as_list_view=True,
        ),
        # Row detail panel (hidden by default, populated by callback)
        html.Div(
            id={"type": "row-detail-panel", "index": _data_table_counter["n"]},
            style={"display": "none"},
        ),
    ])


# Keep old rich_table for backward compat
def rich_table(headers, rows, col_widths=None):
    """Static styled table (use data_table for interactive version)."""
    th_style = {
        "padding": "12px 14px", "fontSize": "11px", "color": COLORS["text_muted"],
        "textAlign": "left", "borderBottom": f"1px solid {COLORS['border']}",
        "textTransform": "uppercase", "letterSpacing": "0.5px", "fontWeight": "600",
    }
    header_cells = []
    for i, h in enumerate(headers):
        s = dict(th_style)
        if col_widths and i < len(col_widths):
            s["width"] = col_widths[i]
        header_cells.append(html.Th(h, style=s))

    return html.Div(
        className="card",
        style={"padding": "0", "overflow": "hidden"},
        children=[html.Table(
            style={"width": "100%", "borderCollapse": "collapse"},
            children=[
                html.Thead(html.Tr(header_cells)),
                html.Tbody(rows),
            ],
        )],
    )


def td(text, bold=False, mono=False, color=None):
    """Styled table cell."""
    style = {"padding": "12px 14px", "fontSize": "13px",
             "borderBottom": f"1px solid {COLORS['border']}"}
    if bold:
        style["fontWeight"] = "600"
    if mono:
        style["fontFamily"] = "monospace"
        style["fontWeight"] = "600"
    style["color"] = color or COLORS["white"]
    return html.Td(str(text), style=style)


def status_td(status_text, status_key=None):
    """Status badge table cell."""
    key = status_key or status_text
    sc = STATUS_COLORS.get(key, STATUS_COLORS["Healthy"])
    return html.Td(
        html.Span(status_text, className="status-badge",
                   style={"backgroundColor": sc["bg"], "color": sc["text"],
                           "border": f"1px solid {sc['border']}"}),
        style={"padding": "12px 14px", "borderBottom": f"1px solid {COLORS['border']}"},
    )


def progress_td(pct, color):
    """Table cell with inline progress bar."""
    return html.Td(
        html.Div(
            style={"display": "flex", "alignItems": "center", "gap": "8px"},
            children=[
                html.Div(
                    style={"flex": "1", "height": "6px", "backgroundColor": COLORS["border"],
                           "borderRadius": "3px", "overflow": "hidden", "minWidth": "60px"},
                    children=[html.Div(style={"width": f"{pct}%", "height": "100%",
                                               "backgroundColor": color, "borderRadius": "3px"})],
                ),
                html.Span(f"{pct}%", style={"fontSize": "12px", "color": color,
                                              "fontWeight": "600", "fontFamily": "monospace",
                                              "minWidth": "40px"}),
            ],
        ),
        style={"padding": "12px 14px", "borderBottom": f"1px solid {COLORS['border']}"},
    )


def breakdown_list(items):
    """Vertical breakdown list. items: [{label, value, pct, color}]"""
    children = []
    for item in items:
        children.append(html.Div(
            style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                    "padding": "10px 0", "borderBottom": f"1px solid {COLORS['border']}"},
            children=[
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "10px"},
                    children=[
                        html.Div(style={"width": "8px", "height": "8px", "borderRadius": "2px",
                                         "backgroundColor": item["color"]}),
                        html.Span(item["label"], style={"fontSize": "13px",
                                                         "color": COLORS["white"]}),
                    ],
                ),
                html.Div(
                    style={"display": "flex", "alignItems": "center", "gap": "12px"},
                    children=[
                        html.Span(item["value"], style={"fontSize": "13px", "fontWeight": "600",
                                                          "color": COLORS["white"],
                                                          "fontFamily": "monospace"}),
                        html.Span(f"{item['pct']}%", style={"fontSize": "12px",
                                                              "color": COLORS["text_muted"],
                                                              "minWidth": "40px",
                                                              "textAlign": "right"}),
                    ],
                ),
            ],
        ))
    return html.Div(style={"padding": "0 4px"}, children=children)


def trend_indicator(direction, pct_text):
    """Small inline trend arrow."""
    arrow = "\u25b2" if direction == "up" else "\u25bc"
    color = COLORS["green"] if direction == "up" else COLORS["red"]
    return html.Span(
        f" {arrow} {pct_text}",
        style={"fontSize": "11px", "color": color, "marginLeft": "6px", "fontWeight": "600"},
    )


def use_case_badges(use_cases):
    """Badge pills for use cases."""
    badge_style = {
        "display": "inline-block", "fontSize": "11px", "fontWeight": "600",
        "color": COLORS["blue"], "backgroundColor": "rgba(75, 123, 245, 0.12)",
        "padding": "4px 12px", "borderRadius": "12px", "marginRight": "8px",
        "marginBottom": "8px", "letterSpacing": "0.3px",
    }
    return html.Div(
        className="card",
        children=[
            html.H3("Use Cases", style={"fontSize": "14px", "fontWeight": "600",
                                         "color": COLORS["white"], "marginBottom": "12px"}),
            html.Div(style={"display": "flex", "flexWrap": "wrap"},
                     children=[html.Span(uc, style=badge_style) for uc in use_cases]),
        ],
    )


def donut_figure(labels, values, colors, center_text="", title=""):
    """Create a Plotly donut chart figure."""
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.65,
        marker=dict(colors=colors, line=dict(color=COLORS["dark"], width=2)),
        textinfo="none", hovertemplate="%{label}: %{value}<extra></extra>",
    ))
    annotations = []
    if center_text:
        annotations.append(dict(text=center_text, x=0.5, y=0.5, font_size=20,
                                font_color=COLORS["white"], font_family=FONT_FAMILY,
                                showarrow=False))
    fig.update_layout(**dark_chart_layout(
        height=280, margin=dict(l=20, r=20, t=40, b=20),
        showlegend=True,
        legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center",
                    font=dict(color=COLORS["text_muted"], size=11)),
        annotations=annotations,
        title=dict(text=title, font=dict(size=14, color=COLORS["white"]),
                   x=0.5, xanchor="center") if title else None,
    ))
    return fig


# ── Utility ─────────────────────────────────────────────────────────────────

def _hex_to_rgb(hex_color):
    """Convert #RRGGBB to 'R, G, B' string for use in rgba()."""
    h = hex_color.lstrip("#")
    return f"{int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}"


def _card(children, **style_overrides):
    """Shorthand for a card div."""
    style = {"backgroundColor": COLORS["panel"], "border": f"1px solid {COLORS['border']}",
             "borderRadius": "12px", "padding": "24px", "marginBottom": "16px"}
    style.update(style_overrides)
    return html.Div(style=style, children=children)


# ═══════════════════════════════════════════════════════════════════════════
#  LAYOUT BUILDERS — each creates a fundamentally different page structure
# ═══════════════════════════════════════════════════════════════════════════


def layout_executive(title, subtitle, heroes, main_chart, panels):
    """Style A: Executive Dashboard.

    heroes: list of hero_metric() components
    main_chart: dcc.Graph or html component
    panels: list of (title_str, component) or (title_str, component, href) tuples
    """
    hero_count = len(heroes)
    hero_cols = min(hero_count, 4)

    content = [
        html.Div(
            style={"display": "grid",
                   "gridTemplateColumns": f"repeat({hero_cols}, 1fr)",
                   "gap": "16px", "marginBottom": "20px"},
            children=heroes,
        ),
        _card([main_chart], padding="20px"),
    ]

    if panels:
        panel_divs = []
        for panel in panels:
            ptitle, pcomp = panel[0], panel[1]
            phref = panel[2] if len(panel) > 2 else None

            header_children = [
                html.Div(ptitle, style={"fontSize": "14px", "fontWeight": "600",
                                         "color": COLORS["white"]}),
            ]
            if phref:
                header_children.append(
                    dcc.Link("View Details \u2192", href=phref,
                             style={"fontSize": "12px", "color": COLORS["blue"],
                                    "textDecoration": "none"})
                )
            header = html.Div(
                style={"display": "flex", "justifyContent": "space-between",
                       "alignItems": "center", "marginBottom": "16px"},
                children=header_children,
            )
            panel_divs.append(_card([header, pcomp], padding="20px"))
        content.append(html.Div(
            style={"display": "grid",
                   "gridTemplateColumns": f"repeat({min(len(panels), 3)}, 1fr)",
                   "gap": "16px", "marginTop": "4px"},
            children=panel_divs,
        ))

    return html.Div([page_header(title, subtitle),
                      html.Div(className="content-area", children=content)])


def layout_table(title, subtitle, filters, kpi_items, table_columns=None, table_data=None,
                 page_size=10, style_conditions=None, table_component=None):
    """Style B: Data Table View.

    New API (interactive DataTable with native sort/filter/pagination):
        table_columns: list of {"name": "...", "id": "..."} for DataTable
        table_data: list of row dicts for DataTable

    Legacy API (backward compatible):
        table_component: pre-built html component (e.g. rich_table)
    """
    content = []
    # filters parameter is accepted for backward compatibility but no longer rendered;
    # the DataTable's native filter_action="native" provides real filtering.
    if kpi_items:
        content.append(kpi_strip(kpi_items))

    # Hint text directing users to the DataTable's built-in column filtering
    _hint = html.Div(
        "Use column headers to sort and filter",
        style={"fontSize": "12px", "color": COLORS["text_muted"],
               "marginBottom": "8px", "fontStyle": "italic"},
    )

    if table_component is not None:
        content.append(_hint)
        content.append(table_component)
    elif table_columns and table_data is not None:
        content.append(_hint)
        content.append(data_table(table_columns, table_data, page_size, style_conditions))

    return html.Div([page_header(title, subtitle),
                      html.Div(className="content-area", children=content)])


def layout_split(title, subtitle, tab_contents=None, banner_text=None, bottom_stats=None,
                 tabs=None, left_panel=None, right_panel=None):
    """Style C: Split Analytics with real tab switching.

    New API:
        tab_contents: list of (tab_label, tab_component) tuples

    Legacy API (backward compatible):
        tabs: list of tab label strings
        left_panel: (title_str, component) tuple
        right_panel: (title_str, component) tuple
        -> Auto-converts to tab_contents with side-by-side layout in first tab
    """
    # Convert legacy API to new tab_contents
    if tab_contents is None and tabs is not None:
        side_by_side = html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
            children=[
                _card([
                    html.Div(left_panel[0] if left_panel else "",
                             style={"fontSize": "14px", "fontWeight": "600",
                                    "color": COLORS["white"], "marginBottom": "12px"}),
                    left_panel[1] if left_panel else html.Div(),
                ], padding="20px"),
                _card([
                    html.Div(right_panel[0] if right_panel else "",
                             style={"fontSize": "14px", "fontWeight": "600",
                                    "color": COLORS["white"], "marginBottom": "12px"}),
                    right_panel[1] if right_panel else html.Div(),
                ], padding="20px"),
            ],
        )
        tab_contents = [(tabs[0], side_by_side)]
        for label in tabs[1:]:
            tab_contents.append((label, html.Div(
                style={"padding": "40px", "textAlign": "center"},
                children=[html.Div(f"{label} analytics coming soon",
                                   style={"color": COLORS["text_muted"], "fontSize": "14px"})],
            )))

    content = []
    if banner_text:
        content.append(info_banner(banner_text))
    if tab_contents:
        content.append(interactive_tabs(tab_contents))

    if bottom_stats:
        content.append(html.Div(
            style={"display": "flex", "gap": "12px", "marginTop": "16px"},
            children=[stat_card(s[0], s[1], s[2] if len(s) > 2 else "blue") for s in bottom_stats],
        ))

    return html.Div([page_header(title, subtitle),
                      html.Div(className="content-area", children=content)])


def layout_alerts(title, subtitle, tab_contents=None, summary_kpis=None,
                  tabs=None, alerts=None):
    """Style D: Alert/Incident Cards with real tab switching.

    New API:
        tab_contents: list of (tab_label, alerts_component) tuples

    Legacy API (backward compatible):
        tabs: list of tab label strings
        alerts: list of alert dicts with severity, title, description, etc.
        -> Auto-groups alerts into tabs by severity
    """
    # Convert legacy API to new tab_contents
    if tab_contents is None and tabs is not None and alerts is not None:
        # Build alert cards from dicts
        all_cards = []
        for a in alerts:
            all_cards.append(alert_card(
                severity=a.get("severity", "info"),
                title=a.get("title", ""),
                description=a.get("description", ""),
                impact=a.get("impact"),
                details=a.get("details"),
                timestamp=a.get("timestamp"),
            ))

        # Split into groups for tabs
        critical_warning = [c for c, a in zip(all_cards, alerts)
                           if a.get("severity") in ("critical", "warning")]
        info_other = [c for c, a in zip(all_cards, alerts)
                     if a.get("severity") not in ("critical", "warning")]

        tab_contents = []
        if len(tabs) >= 1:
            tab_contents.append((tabs[0], html.Div(critical_warning or all_cards)))
        if len(tabs) >= 2:
            tab_contents.append((tabs[1], html.Div(info_other or [
                html.Div("No items in this view",
                         style={"color": COLORS["text_muted"], "padding": "40px",
                                "textAlign": "center", "fontSize": "14px"}),
            ])))
        if len(tabs) >= 3:
            tab_contents.append((tabs[2], html.Div(all_cards)))

    content = []
    if summary_kpis:
        content.append(kpi_strip(summary_kpis))
    if tab_contents:
        content.append(interactive_tabs(tab_contents))

    return html.Div([page_header(title, subtitle),
                      html.Div(className="content-area", children=content)])


def layout_forecast(title, subtitle, kpi_items, hero_value, hero_label,
                    hero_trend_text, main_chart, side_component, bottom_table=None):
    """Style E: Forecast / Cost Analysis.

    hero_value: string like "$14.2M"
    hero_trend_text: string like "+8.3% vs prior quarter"
    main_chart: dcc.Graph component
    side_component: component for right sidebar (e.g., breakdown_list)
    """
    content = []
    if kpi_items:
        content.append(kpi_strip(kpi_items))

    hero_panel = _card([
        html.Div(hero_label, style={"fontSize": "13px", "color": COLORS["text_muted"],
                                     "marginBottom": "8px", "fontWeight": "500"}),
        html.Div(hero_value, style={"fontSize": "42px", "fontWeight": "700",
                                     "color": COLORS["blue"], "lineHeight": "1.1"}),
        html.Div(
            children=[
                html.Span("\u25b2 " if hero_trend_text.startswith("+") else "\u25bc ",
                           style={"color": COLORS["green"] if hero_trend_text.startswith("+") else COLORS["red"]}),
                html.Span(hero_trend_text,
                           style={"color": COLORS["green"] if hero_trend_text.startswith("+") else COLORS["red"],
                                  "fontWeight": "600"}),
            ],
            style={"fontSize": "13px", "marginTop": "8px", "marginBottom": "16px"},
        ),
        main_chart,
    ], padding="24px")

    side_panel = _card([side_component], padding="20px")

    content.append(html.Div(
        style={"display": "grid", "gridTemplateColumns": "2fr 1fr", "gap": "16px"},
        children=[hero_panel, side_panel],
    ))

    if bottom_table:
        content.append(bottom_table)

    return html.Div([page_header(title, subtitle),
                      html.Div(className="content-area", children=content)])


def layout_grid(title, subtitle, grid_items):
    """Style F: Operations Grid with varied panel sizes.

    grid_items: list of dicts {col_span, row_span, content}
    """
    children = []
    for item in grid_items:
        col_span = item.get("col_span", 1)
        row_span = item.get("row_span", 1)
        style = {
            "backgroundColor": COLORS["panel"],
            "border": f"1px solid {COLORS['border']}",
            "borderRadius": "12px",
            "padding": "20px",
            "gridColumn": f"span {col_span}" if col_span > 1 else "auto",
            "gridRow": f"span {row_span}" if row_span > 1 else "auto",
        }
        children.append(html.Div(style=style, children=[item["content"]]))

    content = html.Div(
        style={"display": "grid", "gridTemplateColumns": "1fr 1fr 1fr",
               "gap": "16px", "gridAutoRows": "minmax(140px, auto)"},
        children=children,
    )

    return html.Div([page_header(title, subtitle),
                      html.Div(className="content-area", children=[content])])


# ── Gauge / mini chart helpers ──────────────────────────────────────────────

def gauge_figure(value, max_val, title="", color=None):
    """Create a gauge indicator figure."""
    color = color or COLORS["blue"]
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        gauge=dict(
            axis=dict(range=[0, max_val], tickcolor=COLORS["text_muted"]),
            bar=dict(color=color),
            bgcolor=COLORS["border"],
            borderwidth=0,
        ),
        number=dict(font=dict(color=COLORS["white"], size=28)),
    ))
    fig.update_layout(**dark_chart_layout(
        height=180, margin=dict(l=20, r=20, t=40, b=10),
        title=dict(text=title, font=dict(size=12, color=COLORS["text_muted"]),
                   x=0.5, xanchor="center") if title else None,
    ))
    return fig


def sparkline_figure(values, color=None, height=60):
    """Create a small sparkline figure."""
    color = color or COLORS["blue"]
    fig = go.Figure(go.Scatter(
        y=values, mode="lines", line=dict(color=color, width=2),
        fill="tozeroy", fillcolor=f"rgba({_hex_to_rgb(color)}, 0.1)",
    ))
    fig.update_layout(**dark_chart_layout(
        height=height, margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(showgrid=False, visible=False),
        yaxis=dict(showgrid=False, visible=False),
    ))
    return fig


def metric_with_sparkline(title, value, sparkline_values, accent="blue"):
    """A metric card with an embedded sparkline."""
    accent_color = COLORS.get(accent, COLORS["blue"])
    fig = sparkline_figure(sparkline_values, accent_color, height=50)
    return html.Div(children=[
        html.Div(title, style={"fontSize": "11px", "color": COLORS["text_muted"],
                                "textTransform": "uppercase", "letterSpacing": "0.4px",
                                "marginBottom": "4px"}),
        html.Div(value, style={"fontSize": "24px", "fontWeight": "700",
                                "color": accent_color, "marginBottom": "8px"}),
        dcc.Graph(figure=fig, config=CHART_CONFIG,
                  style={"height": "50px", "width": "100%"}),
    ])
