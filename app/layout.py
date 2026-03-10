"""
Master 3-panel layout for ManufacturingIQ.

Creates the outer shell of the application:
  - Left: collapsible Genie AI question panel
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
    """Collapsible Genie AI question panel (left-most column)."""
    question_cards = []
    for q in questions:
        question_cards.append(
            html.Div(
                q,
                className="genie-question",
                id={"type": "genie-q", "index": q[:20]},
            )
        )

    return html.Div(
        id="genie-panel",
        className="genie-panel",
        style={"width": "280px", "minWidth": "280px", "flexShrink": "0"},
        children=[
            html.Div(
                style={"display": "flex", "alignItems": "center", "justifyContent": "space-between", "marginBottom": "16px"},
                children=[
                    html.H3(
                        [html.I(className="fa-solid fa-robot", style={"marginRight": "8px"}), "Genie AI"],
                        style={"margin": "0"},
                    ),
                    html.Button(
                        html.I(className="fa-solid fa-xmark"),
                        id="genie-close-btn",
                        className="genie-toggle",
                        n_clicks=0,
                    ),
                ],
            ),
            html.P(
                _USE_CASE_GENIE_DESC.get(use_case, "Ask questions about your data"),
                style={"fontSize": "12px", "color": COLORS["text_muted"], "marginBottom": "16px"},
            ),
            html.Div(
                dcc.Input(
                    id="genie-input",
                    type="text",
                    placeholder="Type a question...",
                    style={
                        "width": "100%",
                        "padding": "10px 14px",
                        "borderRadius": "8px",
                        "border": f"1px solid {COLORS['border']}",
                        "backgroundColor": COLORS["dark"],
                        "color": COLORS["white"],
                        "fontSize": "13px",
                        "outline": "none",
                        "fontFamily": FONT_FAMILY,
                    },
                ),
                style={"marginBottom": "20px"},
            ),
            html.Div(
                [html.P("Suggested questions", style={"fontSize": "11px", "color": COLORS["text_muted"], "marginBottom": "8px", "textTransform": "uppercase", "letterSpacing": "0.5px"})],
            ),
            html.Div(question_cards),
            # Response area
            html.Div(id="genie-response", style={"marginTop": "20px"}),
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
