"""Real-Time Build Tracking page for ManufacturingIQ Databricks app.

Displays a filterable, auto-refreshing table of build tracking events with
dropdown filters for batch_id and station, colour-coded status, and an export
placeholder button.
"""

import datetime
import random

from dash import html, dcc, dash_table, Input, Output, no_update

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
    from app.data_access import get_build_tracking_data
except ImportError:
    def get_build_tracking_data():
        """Return demo build tracking rows."""
        random.seed(99)
        base = datetime.datetime.now()

        known_batches = [
            {
                "timestamp": (base - datetime.timedelta(minutes=3)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "batch_id": "B-9982-XYZ",
                "station": "Station 4 \u2013 Final Assembly",
                "status": "Complete",
                "defect_flag": "None",
            },
            {
                "timestamp": (base - datetime.timedelta(minutes=7)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "batch_id": "A-1102-MDF",
                "station": "Station 2 \u2013 CNC Machining",
                "status": "Defect",
                "defect_flag": "Vibration anomaly",
            },
            {
                "timestamp": (base - datetime.timedelta(minutes=1)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "batch_id": "C-4421-ALP",
                "station": "Station 3 \u2013 Heat Treatment",
                "status": "In Progress",
                "defect_flag": "None",
            },
        ]

        stations = [
            "Station 1 \u2013 Raw Material Intake",
            "Station 2 \u2013 CNC Machining",
            "Station 3 \u2013 Heat Treatment",
            "Station 4 \u2013 Final Assembly",
            "Station 5 \u2013 QC & Packaging",
        ]
        statuses = ["In Progress", "Complete", "Complete", "Complete", "Queued"]
        extra_batches = [f"D-{random.randint(1000,9999)}-{chr(random.randint(65,90))}{chr(random.randint(65,90))}{chr(random.randint(65,90))}"
                         for _ in range(12)]

        rows = list(known_batches)
        for i, bid in enumerate(extra_batches):
            ts = (base - datetime.timedelta(minutes=random.randint(10, 180))).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            st = random.choice(stations)
            status = random.choice(statuses)
            defect = "None"
            if random.random() < 0.10:
                status = "Defect"
                defect = random.choice([
                    "Surface scratch", "Dimensional out-of-spec",
                    "Hardness deviation", "Misalignment",
                ])
            rows.append({
                "timestamp": ts,
                "batch_id": bid,
                "station": st,
                "status": status,
                "defect_flag": defect,
            })

        # Sort newest first
        rows.sort(key=lambda r: r["timestamp"], reverse=True)
        return rows


# ===================================================================
# Helpers
# ===================================================================
def _unique_values(rows, key):
    """Extract sorted unique values for a given key."""
    return sorted({r[key] for r in rows})


# ===================================================================
# Layout
# ===================================================================
def layout():
    """Return the Real-Time Build Tracking page layout."""
    try:
        rows = get_build_tracking_data()
    except Exception:
        rows = []

    batch_ids = _unique_values(rows, "batch_id")
    stations = _unique_values(rows, "station")

    filter_row = html.Div(
        children=[
            # Batch ID dropdown
            html.Div(
                children=[
                    html.Label("Batch ID", style={
                        "fontSize": "12px", "color": TEXT_MUTED, "marginBottom": "4px",
                        "display": "block", "textTransform": "uppercase",
                        "letterSpacing": "0.5px",
                    }),
                    dcc.Dropdown(
                        id="tracking-batch-filter",
                        options=[{"label": b, "value": b} for b in batch_ids],
                        value=None,
                        multi=True,
                        placeholder="All batches",
                        style={"minWidth": "260px", "fontSize": "13px"},
                        className="dash-dropdown-dark",
                    ),
                ],
                style={"flex": "1", "minWidth": "240px"},
            ),
            # Station dropdown
            html.Div(
                children=[
                    html.Label("Station", style={
                        "fontSize": "12px", "color": TEXT_MUTED, "marginBottom": "4px",
                        "display": "block", "textTransform": "uppercase",
                        "letterSpacing": "0.5px",
                    }),
                    dcc.Dropdown(
                        id="tracking-station-filter",
                        options=[{"label": s, "value": s} for s in stations],
                        value=None,
                        multi=True,
                        placeholder="All stations",
                        style={"minWidth": "280px", "fontSize": "13px"},
                        className="dash-dropdown-dark",
                    ),
                ],
                style={"flex": "1", "minWidth": "260px"},
            ),
            # Export button
            html.Div(
                html.Button(
                    "Export CSV",
                    id="tracking-export-btn",
                    n_clicks=0,
                    style={
                        "backgroundColor": "#2A2D35",
                        "color": TEXT_PRIMARY,
                        "border": f"1px solid {BORDER}",
                        "borderRadius": "4px",
                        "padding": "10px 20px",
                        "fontSize": "13px",
                        "fontWeight": "600",
                        "cursor": "pointer",
                        "transition": "background-color 0.2s",
                        "marginTop": "20px",
                    },
                ),
                style={"display": "flex", "alignItems": "flex-end"},
            ),
        ],
        style={
            "display": "flex",
            "gap": "16px",
            "flexWrap": "wrap",
            "marginBottom": "16px",
            "backgroundColor": BG_CARD,
            "border": f"1px solid {BORDER}",
            "borderRadius": "6px",
            "padding": "16px 20px",
        },
    )

    # Export placeholder message
    export_msg = html.Div(id="tracking-export-msg", style={"marginBottom": "12px"})

    # Table
    table = dash_table.DataTable(
        id="tracking-table",
        columns=[
            {"name": "Timestamp", "id": "timestamp"},
            {"name": "Batch ID", "id": "batch_id"},
            {"name": "Station", "id": "station"},
            {"name": "Status", "id": "status"},
            {"name": "Defect Flag", "id": "defect_flag"},
        ],
        data=rows,
        page_size=15,
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
            "fontFamily": "JetBrains Mono, monospace",
        },
        style_data_conditional=[
            # Status colouring
            {
                "if": {"filter_query": '{status} = "Complete"', "column_id": "status"},
                "color": ACCENT_GREEN,
                "fontWeight": "700",
            },
            {
                "if": {"filter_query": '{status} = "In Progress"', "column_id": "status"},
                "color": ACCENT_BLUE,
                "fontWeight": "700",
            },
            {
                "if": {"filter_query": '{status} = "Queued"', "column_id": "status"},
                "color": TEXT_MUTED,
                "fontWeight": "600",
            },
            {
                "if": {"filter_query": '{status} = "Defect"', "column_id": "status"},
                "color": ACCENT_RED,
                "fontWeight": "700",
            },
            # Highlight defect rows
            {
                "if": {"filter_query": '{status} = "Defect"'},
                "backgroundColor": "rgba(255, 86, 48, 0.06)",
            },
            # Defect flag column
            {
                "if": {
                    "filter_query": '{defect_flag} != "None"',
                    "column_id": "defect_flag",
                },
                "color": ACCENT_RED,
                "fontWeight": "600",
            },
        ],
    )

    # Auto-refresh interval
    interval = dcc.Interval(
        id="tracking-interval",
        interval=8000,  # 8 seconds
        n_intervals=0,
    )

    return html.Div(
        children=[
            interval,
            html.H2("Real-Time Build Tracking", style={
                "color": TEXT_PRIMARY, "fontWeight": "700", "fontSize": "22px",
                "marginBottom": "20px",
            }),
            filter_row,
            export_msg,
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
    """Register Dash callbacks for filtering and auto-refresh."""

    @app.callback(
        Output("tracking-table", "data"),
        Input("tracking-batch-filter", "value"),
        Input("tracking-station-filter", "value"),
        Input("tracking-interval", "n_intervals"),
    )
    def update_table(batch_filter, station_filter, _n):
        try:
            rows = get_build_tracking_data()
        except Exception:
            rows = []

        if batch_filter:
            rows = [r for r in rows if r["batch_id"] in batch_filter]
        if station_filter:
            rows = [r for r in rows if r["station"] in station_filter]
        return rows

    @app.callback(
        Output("tracking-export-msg", "children"),
        Input("tracking-export-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def export_csv(n_clicks):
        if not n_clicks:
            return no_update
        return html.Div(
            children=[
                html.Span("\u2193 ", style={"fontWeight": "700", "color": ACCENT_BLUE}),
                html.Span(
                    "Export triggered \u2013 CSV download will start shortly. "
                    "(Placeholder \u2013 connect to Databricks file export.)",
                    style={"color": TEXT_PRIMARY, "fontSize": "13px"},
                ),
            ],
            style={
                "backgroundColor": BG_CARD,
                "border": f"1px solid {BORDER}",
                "borderRadius": "4px",
                "padding": "10px 16px",
            },
        )
