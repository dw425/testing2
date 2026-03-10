"""Quality & Tolerance page for ManufacturingIQ Databricks app.

Displays parts-inspected KPIs, tolerance deviation distribution chart,
and inspection method breakdown.
"""

import random

from dash import html, dcc, dash_table, Input, Output
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Theme constants
# ---------------------------------------------------------------------------
try:
    from app.theme import (
        BG_PRIMARY, BG_CARD, BORDER, TEXT_PRIMARY, TEXT_MUTED,
        ACCENT_BLUE, ACCENT_GREEN, ACCENT_YELLOW, ACCENT_RED,
    )
except ImportError:
    BG_PRIMARY = "#0D0F12"
    BG_CARD = "#16181D"
    BORDER = "#272A31"
    TEXT_PRIMARY = "#EAEBF0"
    TEXT_MUTED = "#8A8D98"
    ACCENT_BLUE = "#4C9AFF"
    ACCENT_GREEN = "#36B37E"
    ACCENT_YELLOW = "#FFAB00"
    ACCENT_RED = "#FF5630"

# ---------------------------------------------------------------------------
# Data access – demo fallback
# ---------------------------------------------------------------------------
try:
    from app.data_access import (
        get_quality_kpis,
        get_tolerance_deviations,
        get_inspection_methods,
    )
except ImportError:
    def get_quality_kpis():
        return {
            "parts_inspected": 2_854_000,
            "deviations_detected": 14,
        }

    def get_tolerance_deviations():
        """Return simulated deviation measurements for distribution chart."""
        random.seed(7)
        # Most readings near zero (within spec); a few outliers
        deviations = [random.gauss(0, 0.012) for _ in range(500)]
        # Inject a handful of out-of-tolerance points
        deviations += [random.gauss(0.06, 0.008) for _ in range(8)]
        deviations += [random.gauss(-0.055, 0.007) for _ in range(6)]
        part_ids = [f"P-{10000 + i}" for i in range(len(deviations))]
        return {
            "part_id": part_ids,
            "deviation_mm": deviations,
        }

    def get_inspection_methods():
        return [
            {"method": "CMM (Coordinate Measuring Machine)", "count": 1_420_000,
             "pass_rate": 99.97, "avg_cycle_s": 18.4},
            {"method": "Vision System (2D/3D)", "count": 980_000,
             "pass_rate": 99.99, "avg_cycle_s": 2.1},
            {"method": "Laser Profilometer", "count": 310_000,
             "pass_rate": 99.95, "avg_cycle_s": 5.6},
            {"method": "Ultrasonic Thickness", "count": 102_000,
             "pass_rate": 99.91, "avg_cycle_s": 8.3},
            {"method": "Manual Gauge Inspection", "count": 42_000,
             "pass_rate": 99.82, "avg_cycle_s": 45.0},
        ]


# ===================================================================
# Plot theme
# ===================================================================
_PLOT_LAYOUT = dict(
    paper_bgcolor=BG_CARD,
    plot_bgcolor="#0E1117",
    font=dict(color=TEXT_PRIMARY, family="Inter, system-ui, sans-serif", size=12),
    margin=dict(l=50, r=30, t=45, b=45),
    xaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
    yaxis=dict(gridcolor="#1E2028", zerolinecolor="#1E2028"),
)


# ===================================================================
# Charts
# ===================================================================
def _build_deviation_histogram():
    data = get_tolerance_deviations()
    devs = data["deviation_mm"]

    # Colour each bar: green if within +/- 0.04 mm, red otherwise
    fig = go.Figure()
    in_spec = [d for d in devs if abs(d) <= 0.04]
    out_spec = [d for d in devs if abs(d) > 0.04]

    fig.add_trace(go.Histogram(
        x=in_spec, nbinsx=60,
        marker=dict(color=ACCENT_GREEN, line=dict(width=0.5, color="#0E1117")),
        name="Within Spec",
        opacity=0.85,
    ))
    fig.add_trace(go.Histogram(
        x=out_spec, nbinsx=20,
        marker=dict(color=ACCENT_RED, line=dict(width=0.5, color="#0E1117")),
        name="Out of Spec",
        opacity=0.90,
    ))

    # Tolerance limit lines
    for lim in [-0.04, 0.04]:
        fig.add_vline(
            x=lim, line=dict(color=ACCENT_YELLOW, dash="dash", width=1.5),
            annotation_text=f"{lim:+.3f} mm",
            annotation=dict(font=dict(size=10, color=ACCENT_YELLOW), showarrow=False),
        )

    fig.update_layout(
        **_PLOT_LAYOUT,
        title=dict(text="Tolerance Deviation Distribution", font=dict(size=15)),
        xaxis_title="Deviation (mm)",
        yaxis_title="Part Count",
        barmode="overlay",
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11),
                    x=0.01, y=0.98),
        height=380,
    )
    return fig


def _build_inspection_breakdown():
    methods = get_inspection_methods()
    names = [m["method"] for m in methods]
    counts = [m["count"] for m in methods]

    fig = go.Figure(go.Pie(
        labels=names,
        values=counts,
        hole=0.50,
        marker=dict(colors=[ACCENT_BLUE, ACCENT_GREEN, ACCENT_YELLOW,
                            "#7C5CFC", ACCENT_RED],
                    line=dict(color=BG_CARD, width=2)),
        textinfo="percent+label",
        textfont=dict(size=11),
        hovertemplate="%{label}<br>Count: %{value:,.0f}<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor=BG_CARD,
        plot_bgcolor=BG_CARD,
        font=dict(color=TEXT_PRIMARY, family="Inter, system-ui, sans-serif", size=12),
        margin=dict(l=20, r=20, t=45, b=20),
        title=dict(text="Inspection Method Breakdown", font=dict(size=15)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
        height=380,
    )
    return fig


# ===================================================================
# Stat card helper (reusable on this page)
# ===================================================================
def _stat_card(title, value, subtitle, accent):
    return html.Div(
        children=[
            html.Div(
                style={
                    "position": "absolute", "left": "0", "top": "0", "bottom": "0",
                    "width": "4px", "backgroundColor": accent,
                    "borderRadius": "4px 0 0 4px",
                },
            ),
            html.Div(
                children=[
                    html.Div(title, style={
                        "fontSize": "13px", "fontWeight": "500", "color": TEXT_MUTED,
                        "textTransform": "uppercase", "letterSpacing": "0.5px",
                        "marginBottom": "8px",
                    }),
                    html.Div(value, style={
                        "fontSize": "32px", "fontWeight": "700", "color": TEXT_PRIMARY,
                        "lineHeight": "1.2", "marginBottom": "6px",
                    }),
                    html.Div(subtitle, style={
                        "fontSize": "12px", "color": "#6C6F7A",
                    }),
                ],
                style={"paddingLeft": "16px"},
            ),
        ],
        style={
            "position": "relative",
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "20px 20px 20px 8px",
            "flex": "1",
            "minWidth": "240px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.25)",
        },
    )


# ===================================================================
# Layout
# ===================================================================
def layout():
    """Return the Quality & Tolerance page layout."""
    try:
        kpis = get_quality_kpis()
    except Exception:
        kpis = {"parts_inspected": 0, "deviations_detected": 0}

    try:
        methods_data = get_inspection_methods()
    except Exception:
        methods_data = []

    stat_row = html.Div(
        children=[
            _stat_card(
                "Parts Inspected",
                f"{kpis['parts_inspected']:,}",
                "Last 30 days across all lines",
                ACCENT_BLUE,
            ),
            _stat_card(
                "Deviations Detected",
                str(kpis["deviations_detected"]),
                "Out-of-tolerance events flagged",
                ACCENT_RED,
            ),
        ],
        style={
            "display": "flex", "gap": "16px", "flexWrap": "wrap",
            "marginBottom": "24px",
        },
    )

    charts_row = html.Div(
        children=[
            html.Div(
                dcc.Graph(id="quality-deviation-hist",
                          figure=_build_deviation_histogram(),
                          config={"displayModeBar": False}),
                style={
                    "backgroundColor": BG_CARD,
                    "border": f"1px solid {BORDER}",
                    "borderRadius": "6px",
                    "padding": "12px",
                    "flex": "1.2",
                    "minWidth": "420px",
                },
            ),
            html.Div(
                dcc.Graph(id="quality-inspection-pie",
                          figure=_build_inspection_breakdown(),
                          config={"displayModeBar": False}),
                style={
                    "backgroundColor": BG_CARD,
                    "border": f"1px solid {BORDER}",
                    "borderRadius": "6px",
                    "padding": "12px",
                    "flex": "0.8",
                    "minWidth": "340px",
                },
            ),
        ],
        style={"display": "flex", "gap": "16px", "flexWrap": "wrap",
               "marginBottom": "24px"},
    )

    # Inspection methods detail table
    methods_table = html.Div(
        children=[
            html.Div("Inspection Methods Detail", style={
                "fontSize": "15px", "fontWeight": "600", "color": TEXT_PRIMARY,
                "marginBottom": "12px",
            }),
            dash_table.DataTable(
                id="quality-methods-table",
                columns=[
                    {"name": "Method", "id": "method"},
                    {"name": "Inspections", "id": "count", "type": "numeric"},
                    {"name": "Pass Rate %", "id": "pass_rate", "type": "numeric"},
                    {"name": "Avg Cycle (s)", "id": "avg_cycle_s", "type": "numeric"},
                ],
                data=methods_data,
                page_size=10,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#1A1D23",
                    "color": TEXT_MUTED,
                    "fontWeight": "600",
                    "fontSize": "12px",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.5px",
                    "border": f"1px solid {BORDER}",
                    "padding": "10px 14px",
                },
                style_cell={
                    "backgroundColor": BG_CARD,
                    "color": TEXT_PRIMARY,
                    "border": f"1px solid {BORDER}",
                    "fontSize": "13px",
                    "padding": "10px 14px",
                    "fontFamily": "Inter, system-ui, sans-serif",
                },
            ),
        ],
        style={
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "20px",
        },
    )

    return html.Div(
        children=[
            html.H2("Quality & Tolerance Analysis", style={
                "color": TEXT_PRIMARY, "fontWeight": "700", "fontSize": "22px",
                "marginBottom": "20px",
            }),
            stat_row,
            charts_row,
            methods_table,
        ],
        style={"padding": "24px", "backgroundColor": BG_PRIMARY, "minHeight": "100vh"},
    )


# ===================================================================
# Callbacks
# ===================================================================
def register_callbacks(app):
    """Register Dash callbacks for the Quality page.

    Currently a no-op because the page is driven by static demo data.
    Add interactive filters here when connecting to live Databricks tables.
    """
    pass
