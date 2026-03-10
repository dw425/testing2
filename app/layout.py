"""
Master 3-panel layout for Blueprint IQ.

Creates the outer shell of the application:
  - Left: collapsible Genie AI chat panel (white background, matching HTML mockup)
  - Middle: sidebar navigation
  - Right: routed page content area

The layout is built once at startup via ``build_layout(use_case)``.
"""

from dash import dcc, html

from app.data_access import get_config
from app.theme import COLORS, FONT_FAMILY


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USE_CASE_ICONS = {
    "manufacturing": "fa-industry",
    "risk": "fa-shield-halved",
    "healthcare": "fa-heart-pulse",
    "gaming": "fa-gamepad",
    "financial_services": "fa-building-columns",
}

_USE_CASE_GENIE_DESC = {
    "manufacturing": "Ask questions about your manufacturing data",
    "risk": "Ask questions about risk and compliance data",
    "healthcare": "Ask questions about clinical operations data",
    "gaming": "Ask questions about player and game analytics",
    "financial_services": "Ask questions about financial risk data",
}


def _build_genie_panel(questions: list, use_case: str = "manufacturing") -> html.Div:
    """Genie AI chat panel styled to match the HTML mockup.

    White background, blue header text, bordered question cards,
    scrollable chat area, and pill-shaped input bar with send button.
    """
    # Question cards -- styled like the HTML mockup: blue border, uppercase, bold
    question_cards = []
    for q in questions:
        question_cards.append(
            html.Button(
                q,
                id={"type": "genie-q", "index": q[:20]},
                n_clicks=0,
                className="genie-question-card",
                style={
                    "width": "100%",
                    "textAlign": "left",
                    "fontSize": "11px",
                    "fontWeight": "700",
                    "color": COLORS["blue"],
                    "border": f"2px solid rgba(35, 62, 216, 0.30)",
                    "borderRadius": "6px",
                    "padding": "14px",
                    "marginBottom": "10px",
                    "backgroundColor": "transparent",
                    "cursor": "pointer",
                    "textTransform": "uppercase",
                    "lineHeight": "1.5",
                    "letterSpacing": "0.3px",
                    "fontFamily": FONT_FAMILY,
                    "transition": "background-color 0.15s ease",
                },
            )
        )

    return html.Div(
        id="genie-panel",
        className="genie-panel-mockup",
        style={
            "width": "288px",
            "minWidth": "288px",
            "flexShrink": "0",
            "backgroundColor": "#FFFFFF",
            "borderRight": "1px solid #E5E7EB",
            "height": "100vh",
            "display": "flex",
            "flexDirection": "column",
            "overflow": "hidden",
            "transition": "width 0.3s ease, opacity 0.3s ease",
        },
        children=[
            # ---- Chat Header ----
            html.Div(
                style={
                    "padding": "24px 20px 16px 20px",
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "flexShrink": "0",
                },
                children=[
                    html.Span(
                        "Hello",
                        style={
                            "fontWeight": "700",
                            "color": COLORS["blue"],
                            "fontSize": "18px",
                            "fontFamily": FONT_FAMILY,
                        },
                    ),
                    html.Button(
                        html.I(className="fa-solid fa-chevron-down", style={"fontSize": "11px"}),
                        id="genie-close-btn",
                        n_clicks=0,
                        style={
                            "backgroundColor": "#F3F4F6",
                            "border": "none",
                            "borderRadius": "6px",
                            "padding": "8px 10px",
                            "color": "#6B7280",
                            "cursor": "pointer",
                            "transition": "background-color 0.15s",
                            "display": "flex",
                            "alignItems": "center",
                            "justifyContent": "center",
                        },
                    ),
                ],
            ),

            # ---- Scrollable chat area: questions + response history ----
            html.Div(
                id="genie-chat-area",
                style={
                    "flex": "1",
                    "overflowY": "auto",
                    "padding": "0 16px 16px 16px",
                },
                children=[
                    # Suggested question cards
                    html.Div(
                        id="genie-questions-container",
                        children=question_cards,
                    ),
                    # Chat response area (messages will be appended here)
                    html.Div(
                        id="genie-response",
                        style={"marginTop": "12px"},
                    ),
                ],
            ),

            # ---- Input bar at bottom ----
            html.Div(
                style={
                    "padding": "12px 16px",
                    "backgroundColor": "#FFFFFF",
                    "borderTop": "1px solid #F3F4F6",
                    "flexShrink": "0",
                },
                children=[
                    html.Div(
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "backgroundColor": "#F3F4F6",
                            "borderRadius": "9999px",
                            "border": "1px solid #D1D5DB",
                            "padding": "3px 3px 3px 16px",
                        },
                        children=[
                            dcc.Input(
                                id="genie-input",
                                type="text",
                                placeholder="Ask a question...",
                                debounce=True,
                                n_submit=0,
                                style={
                                    "flex": "1",
                                    "border": "none",
                                    "outline": "none",
                                    "backgroundColor": "transparent",
                                    "fontSize": "13px",
                                    "color": "#1F2937",
                                    "fontFamily": FONT_FAMILY,
                                    "padding": "6px 0",
                                },
                            ),
                            html.Button(
                                html.I(className="fa-solid fa-paper-plane", style={"fontSize": "11px"}),
                                id="genie-send-btn",
                                n_clicks=0,
                                style={
                                    "width": "32px",
                                    "height": "32px",
                                    "borderRadius": "50%",
                                    "backgroundColor": "#FFFFFF",
                                    "border": "1px solid #E5E7EB",
                                    "color": "#9CA3AF",
                                    "cursor": "pointer",
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                    "flexShrink": "0",
                                    "boxShadow": "0 1px 2px rgba(0,0,0,0.05)",
                                    "transition": "color 0.15s",
                                    "padding": "0",
                                },
                            ),
                        ],
                    ),
                ],
            ),

            # ---- Store for chat history ----
            dcc.Store(id="genie-chat-history", data=[]),
        ],
    )


def _build_sidebar(pages: list, app_title: str, app_subtitle: str, use_case: str = "manufacturing") -> html.Div:
    """Vertical sidebar with brand header and navigation links."""
    nav_links = []
    for page in pages:
        if not page.get("enabled", True):
            continue
        icon_class = f"fa-solid {page.get('icon', 'fa-circle')}"
        nav_links.append(
            dcc.Link(
                [
                    html.I(className=icon_class),
                    html.Span(page["label"]),
                ],
                href=f"/{page['id']}",
                className="nav-link",
                id={"type": "nav-link", "index": page["id"]},
            )
        )

    return html.Div(
        className="sidebar",
        style={"width": "220px", "minWidth": "220px", "flexShrink": "0"},
        children=[
            # Brand header
            html.Div(
                className="sidebar-brand",
                children=[
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "8px"},
                        children=[
                            html.Div(
                                style={
                                    "width": "28px",
                                    "height": "28px",
                                    "borderRadius": "6px",
                                    "backgroundColor": COLORS["blue"],
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                },
                                children=html.I(
                                    className=f"fa-solid {_USE_CASE_ICONS.get(use_case, 'fa-cube')}",
                                    style={"color": COLORS["white"], "fontSize": "13px"},
                                ),
                            ),
                            html.H2(app_title),
                        ],
                    ),
                    html.P(app_subtitle),
                ],
            ),
            # Genie toggle button (shown when panel is hidden)
            html.Div(
                html.Button(
                    [html.I(className="fa-solid fa-robot", style={"marginRight": "6px"}), "Genie"],
                    id="genie-open-btn",
                    className="genie-toggle",
                    n_clicks=0,
                    style={"width": "calc(100% - 32px)", "margin": "12px 16px", "textAlign": "left"},
                ),
                id="genie-open-wrapper",
                style={"display": "none"},
            ),
            # Nav links
            html.Nav(nav_links, style={"marginTop": "4px"}),
            # Footer
            html.Div(
                style={
                    "position": "absolute",
                    "bottom": "16px",
                    "left": "0",
                    "width": "220px",
                    "padding": "0 20px",
                },
                children=[
                    html.Div(
                        style={"borderTop": f"1px solid {COLORS['border']}", "paddingTop": "12px"},
                        children=[
                            html.P(
                                "Powered by Databricks",
                                style={"fontSize": "10px", "color": COLORS["text_muted"], "margin": "0"},
                            ),
                            html.P(
                                "Blueprint AI Platform",
                                style={"fontSize": "10px", "color": COLORS["text_muted"], "margin": "2px 0 0 0"},
                            ),
                        ],
                    )
                ],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_layout(use_case: str = "manufacturing") -> html.Div:
    """Construct the full application layout.

    Parameters
    ----------
    use_case:
        Name of the YAML config to load from ``config/<use_case>.yaml``.
        Defaults to ``"manufacturing"``.

    Returns
    -------
    html.Div
        Root Div wrapping URL routing, interval timer, and the 3-panel shell.
    """
    cfg = get_config()

    app_title = cfg["app"].get("title", "ManufacturingIQ")
    app_subtitle = cfg["app"].get("subtitle", "Production Analysis")
    pages = cfg.get("pages", [])
    genie_questions = cfg.get("genie", {}).get("sample_questions", [])

    return html.Div(
        style={"fontFamily": FONT_FAMILY},
        children=[
            # URL routing
            dcc.Location(id="url", refresh=False),
            # Auto-refresh interval (5 seconds)
            dcc.Interval(id="interval-refresh", interval=5_000, n_intervals=0),
            # Store for genie panel visibility
            dcc.Store(id="genie-visible", data=True),
            # Outer flex container
            html.Div(
                style={
                    "display": "flex",
                    "height": "100vh",
                    "overflow": "hidden",
                    "backgroundColor": COLORS["dark"],
                },
                children=[
                    # Genie panel (left)
                    _build_genie_panel(genie_questions, use_case),
                    # Sidebar (middle)
                    _build_sidebar(pages, app_title, app_subtitle, use_case),
                    # Content area (right)
                    html.Div(
                        id="page-content",
                        style={
                            "flex": "1",
                            "overflowY": "auto",
                            "backgroundColor": COLORS["dark"],
                        },
                    ),
                ],
            ),
        ],
    )
