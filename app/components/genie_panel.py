"""Genie NLQ chat panel component for ManufacturingIQ Databricks app."""

from dash import html, dcc


def genie_panel(sample_questions):
    """Create a Genie natural-language query chat panel.

    The panel features a greeting header, sample question pill buttons,
    a scrollable chat message area, and a bottom input bar. It uses a
    white/light background to contrast with the dark main content area.

    Args:
        sample_questions: List of sample question strings to display as
            clickable pill buttons.

    Returns:
        A dash html.Div component representing the full Genie panel.
    """

    # Sample question pill buttons
    question_pills = []
    for i, question in enumerate(sample_questions):
        pill = html.Button(
            children=question,
            id={"type": "genie-sample-question", "index": i},
            n_clicks=0,
            style={
                "backgroundColor": "#F0F4FF",
                "color": "#2C5CC5",
                "border": "1px solid #D4DFFA",
                "borderRadius": "20px",
                "padding": "8px 16px",
                "fontSize": "13px",
                "fontWeight": "500",
                "cursor": "pointer",
                "display": "inline-block",
                "margin": "4px",
                "transition": "background-color 0.2s ease, border-color 0.2s ease",
                "whiteSpace": "nowrap",
                "fontFamily": "'Inter', 'Segoe UI', -apple-system, sans-serif",
            },
        )
        question_pills.append(pill)

    # Greeting header
    greeting = html.Div(
        children=[
            html.Div(
                children=[
                    html.I(
                        className="fa fa-magic",
                        style={
                            "fontSize": "24px",
                            "color": "#3A7BF7",
                            "marginRight": "12px",
                        },
                    ),
                    html.Span(
                        "Hello",
                        style={
                            "fontSize": "22px",
                            "fontWeight": "700",
                            "color": "#1A1D26",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "marginBottom": "4px",
                },
            ),
            html.Div(
                "Ask me anything about your manufacturing data.",
                style={
                    "fontSize": "14px",
                    "color": "#6C6F7A",
                    "marginBottom": "16px",
                },
            ),
        ],
        style={"padding": "20px 20px 0 20px"},
    )

    # Sample questions section
    sample_section = html.Div(
        children=[
            html.Div(
                "Try asking:",
                style={
                    "fontSize": "12px",
                    "fontWeight": "600",
                    "color": "#8A8D98",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.5px",
                    "marginBottom": "8px",
                },
            ),
            html.Div(
                children=question_pills,
                style={
                    "display": "flex",
                    "flexWrap": "wrap",
                    "gap": "4px",
                },
            ),
        ],
        style={
            "padding": "8px 20px 16px 20px",
            "borderBottom": "1px solid #E8EAF0",
        },
    )

    # Chat message area (scrollable)
    chat_area = html.Div(
        id="genie-chat-messages",
        children=[
            # Placeholder for when no messages exist yet
            html.Div(
                children=[
                    html.I(
                        className="fa fa-comments-o",
                        style={
                            "fontSize": "32px",
                            "color": "#D0D3DC",
                            "marginBottom": "8px",
                        },
                    ),
                    html.Div(
                        "Your conversation will appear here.",
                        style={
                            "fontSize": "13px",
                            "color": "#A0A4B0",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "flexDirection": "column",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "height": "100%",
                    "minHeight": "120px",
                },
            ),
        ],
        style={
            "flex": "1",
            "overflowY": "auto",
            "padding": "16px 20px",
            "display": "flex",
            "flexDirection": "column",
            "gap": "12px",
        },
    )

    # Input bar at bottom
    input_bar = html.Div(
        children=[
            html.Div(
                children=[
                    dcc.Input(
                        id="genie-input",
                        type="text",
                        placeholder="Ask a question about your manufacturing data...",
                        debounce=True,
                        style={
                            "flex": "1",
                            "border": "none",
                            "outline": "none",
                            "fontSize": "14px",
                            "color": "#1A1D26",
                            "backgroundColor": "transparent",
                            "padding": "10px 14px",
                            "fontFamily": "'Inter', 'Segoe UI', -apple-system, sans-serif",
                        },
                    ),
                    html.Button(
                        children=[
                            html.I(
                                className="fa fa-paper-plane",
                                style={
                                    "fontSize": "14px",
                                    "color": "#FFFFFF",
                                },
                            ),
                        ],
                        id="genie-send-btn",
                        n_clicks=0,
                        style={
                            "backgroundColor": "#3A7BF7",
                            "border": "none",
                            "borderRadius": "8px",
                            "width": "38px",
                            "height": "38px",
                            "cursor": "pointer",
                            "display": "flex",
                            "alignItems": "center",
                            "justifyContent": "center",
                            "flexShrink": "0",
                            "transition": "background-color 0.2s ease",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "backgroundColor": "#F5F6F8",
                    "borderRadius": "10px",
                    "border": "1px solid #E0E2E9",
                    "padding": "4px 6px 4px 4px",
                },
            ),
        ],
        style={
            "padding": "12px 20px 16px 20px",
            "borderTop": "1px solid #E8EAF0",
        },
    )

    # Full Genie panel
    panel = html.Div(
        id="genie-panel",
        children=[
            greeting,
            sample_section,
            chat_area,
            input_bar,
        ],
        style={
            "width": "360px",
            "minWidth": "360px",
            "height": "100vh",
            "backgroundColor": "#FFFFFF",
            "borderLeft": "1px solid #E0E2E9",
            "display": "flex",
            "flexDirection": "column",
            "fontFamily": "'Inter', 'Segoe UI', -apple-system, sans-serif",
            "position": "sticky",
            "top": "0",
            "boxShadow": "-2px 0 12px rgba(0, 0, 0, 0.06)",
        },
    )

    return panel
