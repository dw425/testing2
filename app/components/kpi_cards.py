"""Reusable KPI card component for ManufacturingIQ Databricks app."""

from dash import html


def make_kpi_card(title, value, subtitle, accent_color, alert=False):
    """Create a styled KPI card with dark Blueprint theme.

    Args:
        title: Header text displayed above the value (muted).
        value: The main metric value displayed prominently.
        subtitle: Smaller context text below the value.
        accent_color: CSS color string for the left accent bar.
        alert: If True, render a pulsing red indicator dot.

    Returns:
        A dash html.Div component representing the KPI card.
    """

    # Pulsing red alert indicator
    alert_indicator = []
    if alert:
        alert_indicator = [
            html.Span(
                style={
                    "display": "inline-block",
                    "width": "10px",
                    "height": "10px",
                    "borderRadius": "50%",
                    "backgroundColor": "#FF4D4F",
                    "marginLeft": "8px",
                    "verticalAlign": "middle",
                    "boxShadow": "0 0 8px 2px rgba(255, 77, 79, 0.6)",
                    "animation": "pulse 1.5s ease-in-out infinite",
                },
            ),
            # Inject keyframes via a hidden style element
            html.Div(
                children=[],
                style={"display": "none"},
                **{
                    "data-style": (
                        "@keyframes pulse {"
                        "0% { box-shadow: 0 0 4px 1px rgba(255,77,79,0.4); opacity: 1; }"
                        "50% { box-shadow: 0 0 12px 4px rgba(255,77,79,0.8); opacity: 0.7; }"
                        "100% { box-shadow: 0 0 4px 1px rgba(255,77,79,0.4); opacity: 1; }"
                        "}"
                    )
                },
            ),
        ]

    card = html.Div(
        children=[
            # Left accent bar
            html.Div(
                style={
                    "position": "absolute",
                    "left": "0",
                    "top": "0",
                    "bottom": "0",
                    "width": "4px",
                    "backgroundColor": accent_color,
                    "borderRadius": "4px 0 0 4px",
                },
            ),
            # Card content
            html.Div(
                children=[
                    # Title row with optional alert
                    html.Div(
                        children=[
                            html.Span(
                                title,
                                style={
                                    "fontSize": "13px",
                                    "fontWeight": "500",
                                    "color": "#8A8D98",
                                    "textTransform": "uppercase",
                                    "letterSpacing": "0.5px",
                                },
                            ),
                        ]
                        + alert_indicator,
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "marginBottom": "8px",
                        },
                    ),
                    # Value
                    html.Div(
                        value,
                        style={
                            "fontSize": "32px",
                            "fontWeight": "700",
                            "color": "#EAEBF0",
                            "lineHeight": "1.2",
                            "marginBottom": "6px",
                        },
                    ),
                    # Subtitle
                    html.Div(
                        subtitle,
                        style={
                            "fontSize": "12px",
                            "fontWeight": "400",
                            "color": "#6C6F7A",
                        },
                    ),
                ],
                style={"paddingLeft": "16px"},
            ),
        ],
        style={
            "position": "relative",
            "backgroundColor": "#16181D",
            "border": "1px solid #272A31",
            "borderRadius": "6px",
            "padding": "20px 20px 20px 8px",
            "minWidth": "200px",
            "flex": "1",
            "boxShadow": "0 2px 8px rgba(0, 0, 0, 0.25)",
            "transition": "box-shadow 0.2s ease",
        },
    )

    return card
