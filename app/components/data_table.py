"""Reusable dark-themed DataTable component for ManufacturingIQ Databricks app."""

from dash import dash_table


def make_data_table(id, columns, data, highlight_column=None, highlight_rules=None):
    """Create a dark-themed, sortable, filterable DataTable.

    Args:
        id: The Dash component id for the table.
        columns: List of dicts with keys 'name' and 'id' for column definitions.
        data: List of dicts representing the table rows.
        highlight_column: Optional column id to apply conditional formatting.
        highlight_rules: Optional dict mapping cell values to colors, e.g.
            {"OK": "#2ECC71", "Warning": "#F1C40F", "Critical": "#E74C3C"}.
            If None, defaults are used for common status values.

    Returns:
        A dash DataTable component with full dark theme styling.
    """

    # Default conditional formatting rules for status-type columns
    default_rules = {
        # Green statuses
        "OK": "#2ECC71",
        "Normal": "#2ECC71",
        "Good": "#2ECC71",
        "Pass": "#2ECC71",
        "Active": "#2ECC71",
        "Healthy": "#2ECC71",
        "Running": "#2ECC71",
        "Completed": "#2ECC71",
        # Yellow / warning statuses
        "Warning": "#F1C40F",
        "Caution": "#F1C40F",
        "Degraded": "#F1C40F",
        "Pending": "#F1C40F",
        "Review": "#F1C40F",
        # Red / critical statuses
        "Critical": "#E74C3C",
        "Fail": "#E74C3C",
        "Error": "#E74C3C",
        "Down": "#E74C3C",
        "Anomaly": "#E74C3C",
        "Alert": "#E74C3C",
        "Offline": "#E74C3C",
    }

    rules = highlight_rules if highlight_rules is not None else default_rules

    # Build conditional style rules
    style_data_conditional = [
        # Striped rows
        {
            "if": {"row_index": "odd"},
            "backgroundColor": "#1A1C22",
        },
        # Hover effect
        {
            "if": {"state": "active"},
            "backgroundColor": "#22252E",
            "border": "1px solid #3A7BF7",
        },
    ]

    # Add highlight rules for the specified column
    if highlight_column:
        for value, color in rules.items():
            style_data_conditional.append(
                {
                    "if": {
                        "filter_query": '{{{col}}} = "{val}"'.format(
                            col=highlight_column, val=value
                        ),
                        "column_id": highlight_column,
                    },
                    "color": color,
                    "fontWeight": "600",
                }
            )

    table = dash_table.DataTable(
        id=id,
        columns=columns,
        data=data,
        # Sorting and filtering
        sort_action="native",
        sort_mode="multi",
        filter_action="native",
        # Pagination
        page_action="native",
        page_size=15,
        # Cell styling
        style_cell={
            "backgroundColor": "#16181D",
            "color": "#CCCFD8",
            "border": "1px solid #272A31",
            "fontFamily": "'Inter', 'Segoe UI', -apple-system, sans-serif",
            "fontSize": "13px",
            "padding": "10px 14px",
            "textAlign": "left",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            "maxWidth": "200px",
        },
        # Header styling
        style_header={
            "backgroundColor": "#1E2A45",
            "color": "#EAEBF0",
            "fontWeight": "600",
            "fontSize": "12px",
            "textTransform": "uppercase",
            "letterSpacing": "0.5px",
            "border": "1px solid #272A31",
            "borderBottom": "2px solid #3A7BF7",
            "padding": "12px 14px",
        },
        # Filter row styling
        style_filter={
            "backgroundColor": "#1A1C22",
            "color": "#CCCFD8",
            "border": "1px solid #272A31",
            "padding": "6px 10px",
        },
        # Table-level styling
        style_table={
            "borderRadius": "6px",
            "overflow": "hidden",
            "border": "1px solid #272A31",
        },
        # Data cells
        style_data={
            "border": "1px solid #272A31",
        },
        # Conditional formatting
        style_data_conditional=style_data_conditional,
        # CSS for the overall component
        css=[
            # Style the pagination buttons
            {
                "selector": ".previous-next-container",
                "rule": "color: #CCCFD8;",
            },
            {
                "selector": ".page-number",
                "rule": "color: #CCCFD8; background-color: #16181D;",
            },
            # Filter input placeholder
            {
                "selector": "input::placeholder",
                "rule": "color: #6C6F7A !important;",
            },
        ],
    )

    return table
