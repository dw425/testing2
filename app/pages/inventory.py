"""Inventory & Forecasting page for ManufacturingIQ Databricks app.

Displays a searchable component inventory table with status badges, days
remaining, predicted shortages, and a Monte Carlo simulation trigger.
"""

from dash import html, dcc, dash_table, Input, Output, State, callback, no_update

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
    from app.data_access import get_inventory_data
except ImportError:
    def get_inventory_data():
        return [
            {
                "component": "Spindle Bearing SKF-7210",
                "site": "Plant A \u2013 Austin",
                "current_stock": 342,
                "days_remaining": 47,
                "status": "Healthy",
                "predicted_shortage": "\u2014",
                "ramp_impact": "None",
            },
            {
                "component": "Servo Motor FANUC \u03b1i",
                "site": "Plant A \u2013 Austin",
                "current_stock": 18,
                "days_remaining": 9,
                "status": "Critical",
                "predicted_shortage": "2026-03-19",
                "ramp_impact": "Line 3 shutdown",
            },
            {
                "component": "Hydraulic Pump Rexroth A10V",
                "site": "Plant B \u2013 Dresden",
                "current_stock": 85,
                "days_remaining": 22,
                "status": "Low",
                "predicted_shortage": "2026-04-01",
                "ramp_impact": "Reduced throughput \u221215%",
            },
            {
                "component": "Coolant Filter Pall HH8656",
                "site": "Plant B \u2013 Dresden",
                "current_stock": 1240,
                "days_remaining": 120,
                "status": "Healthy",
                "predicted_shortage": "\u2014",
                "ramp_impact": "None",
            },
            {
                "component": "PLC Module Siemens S7-1500",
                "site": "Plant C \u2013 Nagoya",
                "current_stock": 6,
                "days_remaining": 5,
                "status": "Critical",
                "predicted_shortage": "2026-03-15",
                "ramp_impact": "Full cell offline",
            },
            {
                "component": "Linear Guide THK SHS-25",
                "site": "Plant A \u2013 Austin",
                "current_stock": 64,
                "days_remaining": 30,
                "status": "Low",
                "predicted_shortage": "2026-04-09",
                "ramp_impact": "Maintenance delay",
            },
            {
                "component": "Encoder Heidenhain ERN 1387",
                "site": "Plant C \u2013 Nagoya",
                "current_stock": 210,
                "days_remaining": 58,
                "status": "Healthy",
                "predicted_shortage": "\u2014",
                "ramp_impact": "None",
            },
            {
                "component": "Ball Screw NSK W3206",
                "site": "Plant B \u2013 Dresden",
                "current_stock": 12,
                "days_remaining": 8,
                "status": "Critical",
                "predicted_shortage": "2026-03-18",
                "ramp_impact": "Line 1 partial halt",
            },
            {
                "component": "VFD ABB ACS580",
                "site": "Plant A \u2013 Austin",
                "current_stock": 42,
                "days_remaining": 35,
                "status": "Healthy",
                "predicted_shortage": "\u2014",
                "ramp_impact": "None",
            },
            {
                "component": "Pneumatic Cylinder SMC CQ2",
                "site": "Plant C \u2013 Nagoya",
                "current_stock": 28,
                "days_remaining": 14,
                "status": "Low",
                "predicted_shortage": "2026-03-24",
                "ramp_impact": "Assembly bottleneck",
            },
        ]


# ===================================================================
# Status badge helper
# ===================================================================
_STATUS_COLORS = {
    "Healthy": ACCENT_GREEN,
    "Low": ACCENT_YELLOW,
    "Critical": ACCENT_RED,
}


# ===================================================================
# Layout
# ===================================================================
def layout():
    """Return the Inventory & Forecasting page layout."""
    try:
        rows = get_inventory_data()
    except Exception:
        rows = []

    # Search input
    search_bar = html.Div(
        children=[
            dcc.Input(
                id="inventory-search",
                type="text",
                placeholder="Search components, sites\u2026",
                debounce=True,
                style={
                    "backgroundColor": "#1A1D23",
                    "color": TEXT_PRIMARY,
                    "border": f"1px solid {BORDER}",
                    "borderRadius": "4px",
                    "padding": "10px 14px",
                    "width": "320px",
                    "fontSize": "13px",
                    "outline": "none",
                },
            ),
            html.Button(
                "Run Monte Carlo Simulation",
                id="inventory-mc-btn",
                n_clicks=0,
                style={
                    "backgroundColor": ACCENT_BLUE,
                    "color": "#fff",
                    "border": "none",
                    "borderRadius": "4px",
                    "padding": "10px 20px",
                    "fontSize": "13px",
                    "fontWeight": "600",
                    "cursor": "pointer",
                    "marginLeft": "12px",
                    "transition": "background-color 0.2s",
                },
            ),
        ],
        style={
            "display": "flex",
            "alignItems": "center",
            "marginBottom": "16px",
        },
    )

    # Monte Carlo result area (placeholder)
    mc_result = html.Div(
        id="inventory-mc-result",
        style={"marginBottom": "16px"},
    )

    # Table
    table = dash_table.DataTable(
        id="inventory-table",
        columns=[
            {"name": "Component", "id": "component"},
            {"name": "Site", "id": "site"},
            {"name": "Current Stock", "id": "current_stock", "type": "numeric"},
            {"name": "Days Remaining", "id": "days_remaining", "type": "numeric"},
            {"name": "Status", "id": "status"},
            {"name": "Predicted Shortage", "id": "predicted_shortage"},
            {"name": "Ramp Impact", "id": "ramp_impact"},
        ],
        data=rows,
        page_size=12,
        sort_action="native",
        filter_action="none",
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
        style_data_conditional=[
            # Status badge colours
            {
                "if": {"filter_query": '{status} = "Healthy"', "column_id": "status"},
                "color": ACCENT_GREEN,
                "fontWeight": "700",
            },
            {
                "if": {"filter_query": '{status} = "Low"', "column_id": "status"},
                "color": ACCENT_YELLOW,
                "fontWeight": "700",
            },
            {
                "if": {"filter_query": '{status} = "Critical"', "column_id": "status"},
                "color": ACCENT_RED,
                "fontWeight": "700",
            },
            # Highlight entire Critical rows
            {
                "if": {"filter_query": '{status} = "Critical"'},
                "backgroundColor": "rgba(255, 86, 48, 0.06)",
            },
            # Days remaining red when <= 10
            {
                "if": {
                    "filter_query": "{days_remaining} <= 10",
                    "column_id": "days_remaining",
                },
                "color": ACCENT_RED,
                "fontWeight": "700",
            },
        ],
    )

    return html.Div(
        children=[
            html.H2("Inventory & Forecasting", style={
                "color": TEXT_PRIMARY, "fontWeight": "700", "fontSize": "22px",
                "marginBottom": "20px",
            }),
            search_bar,
            mc_result,
            html.Div(
                table,
                style={
                    "backgroundColor": BG_CARD,
                    "border": f"1px solid {BORDER}",
                    "borderRadius": "6px",
                    "padding": "20px",
                },
            ),
        ],
        style={"padding": "24px", "backgroundColor": BG_PRIMARY, "minHeight": "100vh"},
    )


# ===================================================================
# Callbacks
# ===================================================================
def register_callbacks(app):
    """Register Dash callbacks for search and Monte Carlo simulation."""

    @app.callback(
        Output("inventory-table", "data"),
        Input("inventory-search", "value"),
    )
    def filter_inventory(search_value):
        try:
            rows = get_inventory_data()
        except Exception:
            rows = []

        if not search_value:
            return rows

        term = search_value.lower()
        return [
            r for r in rows
            if term in r.get("component", "").lower()
            or term in r.get("site", "").lower()
            or term in r.get("status", "").lower()
            or term in r.get("ramp_impact", "").lower()
        ]

    @app.callback(
        Output("inventory-mc-result", "children"),
        Input("inventory-mc-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def run_monte_carlo(n_clicks):
        if not n_clicks:
            return no_update
        # Placeholder result
        return html.Div(
            children=[
                html.Span("\u2713 ", style={"color": ACCENT_GREEN, "fontWeight": "700"}),
                html.Span(
                    "Monte Carlo simulation complete \u2013 95th-percentile shortage "
                    "window: 11\u201318 days for 3 critical components. "
                    "(Placeholder \u2013 connect to Databricks Jobs for real run.)",
                    style={"color": TEXT_PRIMARY, "fontSize": "13px"},
                ),
            ],
            style={
                "backgroundColor": BG_CARD,
                "border": f"1px solid {BORDER}",
                "borderRadius": "4px",
                "padding": "12px 16px",
            },
        )
