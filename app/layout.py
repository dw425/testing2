"""
Master layout for Blueprint IQ.

Creates the outer shell of the application:
  - Left: sidebar navigation
  - Right: routed page content area
  - Floating: chat icon button (bottom-right) that opens a popup modal

The layout is built once at startup via ``build_layout()`` which creates a
dynamic skeleton with placeholder containers.  The sidebar content is
populated at runtime by callbacks in ``main.py`` using ``build_sidebar()``.
The Genie chat modal is toggled via a floating button.
"""

from dash import dcc, html

from app.data_access import get_config_for
from app.theme import COLORS, FONT_FAMILY


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USE_CASE_ICONS = {
    "gaming": "fa-gamepad",
    "telecom": "fa-tower-cell",
    "media": "fa-film",
    "financial_services": "fa-building-columns",
    "hls": "fa-heart-pulse",
    "manufacturing": "fa-industry",
    "risk": "fa-shield-halved",
}


# ---------------------------------------------------------------------------
# Public helpers -- called by main.py callbacks
# ---------------------------------------------------------------------------


def build_sidebar(vertical: str, active_page: str = "") -> html.Div:
    """Build the sidebar navigation for the given *vertical*."""
    cfg = get_config_for(vertical)
    app_title = cfg["app"].get("title", "Blueprint IQ")
    app_subtitle = cfg["app"].get("subtitle", "")
    pages = cfg.get("pages", [])
    brand_color = cfg.get("brand", {}).get("primary_color", COLORS["blue"])

    nav_links = []
    current_group = None
    for page in pages:
        if not page.get("enabled", True):
            continue
        group = page.get("group", "")
        if group and group != current_group:
            current_group = group
            nav_links.append(
                html.Div(
                    group.upper(),
                    style={
                        "fontSize": "9px",
                        "fontWeight": "700",
                        "color": COLORS.get("text_muted", "#9CA3AF"),
                        "textTransform": "uppercase",
                        "letterSpacing": "1.2px",
                        "padding": "14px 20px 4px 20px",
                        "opacity": "0.7",
                    },
                )
            )
        icon_class = f"fa-solid {page.get('icon', 'fa-circle')}"
        nav_links.append(
            dcc.Link(
                [
                    html.I(className=icon_class),
                    html.Span(page["label"]),
                ],
                href=f"/{vertical}/{page['id']}",
                className="nav-link",
                id={"type": "nav-link", "index": page["id"]},
            )
        )

    return html.Div(
        className="sidebar",
        style={"width": "220px", "minWidth": "220px", "flexShrink": "0"},
        children=[
            # Back to Demo Hub link
            dcc.Link(
                [
                    html.I(
                        className="fa-solid fa-arrow-left",
                        style={"marginRight": "6px", "fontSize": "11px"},
                    ),
                    html.Span("Back to Demo Hub"),
                ],
                href="/hub",
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "padding": "10px 20px",
                    "fontSize": "12px",
                    "color": COLORS.get("text_muted", "#9CA3AF"),
                    "textDecoration": "none",
                    "borderBottom": f"1px solid {COLORS.get('border', '#374151')}",
                    "fontFamily": FONT_FAMILY,
                },
            ),
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
                                    "backgroundColor": brand_color,
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                },
                                children=html.I(
                                    className=f"fa-solid {_USE_CASE_ICONS.get(vertical, 'fa-cube')}",
                                    style={"color": COLORS["white"], "fontSize": "13px"},
                                ),
                            ),
                            html.H2(app_title),
                        ],
                    ),
                    html.P(app_subtitle),
                ],
            ),
            # Nav links + shared reference pages
            html.Nav(nav_links + [
                html.Div(
                    "REFERENCE",
                    style={
                        "fontSize": "9px", "fontWeight": "700",
                        "color": COLORS.get("text_muted", "#9CA3AF"),
                        "textTransform": "uppercase", "letterSpacing": "1.2px",
                        "padding": "14px 20px 4px 20px", "opacity": "0.7",
                    },
                ),
                dcc.Link(
                    [html.I(className="fa-solid fa-layer-group"),
                     html.Span("Architecture")],
                    href=f"/{vertical}/architecture",
                    className="nav-link",
                    id={"type": "nav-link", "index": "architecture"},
                ),
                dcc.Link(
                    [html.I(className="fa-solid fa-circle-info"),
                     html.Span("Details")],
                    href=f"/{vertical}/details",
                    className="nav-link",
                    id={"type": "nav-link", "index": "details"},
                ),
            ], style={"marginTop": "4px"}),
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


def build_layout() -> html.Div:
    """Construct the full application layout.

    Two-panel shell (sidebar + content) with a floating chat button
    and popup modal for Genie AI.
    """
    return html.Div(
        style={"fontFamily": FONT_FAMILY},
        children=[
            # URL routing
            dcc.Location(id="url", refresh=False),
            # Store for tracking the active vertical
            dcc.Store(id="active-vertical", data=None),
            # Store for genie chat history
            dcc.Store(id="genie-chat-history", data=[]),
            # Store for chat modal visibility
            dcc.Store(id="chat-modal-open", data=False),
            # Store for tracking which vertical the genie chat is configured for
            dcc.Store(id="genie-current-vertical", data=None),
            # Outer flex container
            html.Div(
                style={
                    "display": "flex",
                    "height": "100vh",
                    "overflow": "hidden",
                    "backgroundColor": COLORS["dark"],
                },
                children=[
                    # Sidebar placeholder -- populated by callback
                    html.Div(id="sidebar-container"),
                    # Content area
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

            # ---- Floating chat button (bottom-right) ----
            html.Button(
                html.I(className="fa-solid fa-comment-dots", style={"fontSize": "22px"}),
                id="chat-fab",
                n_clicks=0,
                style={
                    "position": "fixed",
                    "bottom": "24px",
                    "right": "24px",
                    "width": "56px",
                    "height": "56px",
                    "borderRadius": "50%",
                    "backgroundColor": COLORS["blue"],
                    "color": "#FFFFFF",
                    "border": "none",
                    "cursor": "pointer",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "boxShadow": "0 4px 16px rgba(35, 62, 216, 0.4)",
                    "zIndex": "1000",
                    "transition": "transform 0.2s ease, box-shadow 0.2s ease",
                },
            ),

            # ---- Chat modal (hidden by default) ----
            html.Div(
                id="chat-modal",
                style={
                    "display": "none",
                    "position": "fixed",
                    "bottom": "92px",
                    "right": "24px",
                    "width": "380px",
                    "height": "520px",
                    "backgroundColor": "#FFFFFF",
                    "borderRadius": "16px",
                    "boxShadow": "0 8px 32px rgba(0, 0, 0, 0.2)",
                    "zIndex": "1001",
                    "flexDirection": "column",
                    "overflow": "hidden",
                    "fontFamily": FONT_FAMILY,
                },
                children=[
                    # Modal header
                    html.Div(
                        style={
                            "padding": "16px 20px",
                            "display": "flex",
                            "justifyContent": "space-between",
                            "alignItems": "center",
                            "borderBottom": "1px solid #E5E7EB",
                            "backgroundColor": COLORS["blue"],
                            "borderRadius": "16px 16px 0 0",
                        },
                        children=[
                            html.Div(
                                style={"display": "flex", "alignItems": "center", "gap": "8px"},
                                children=[
                                    html.I(
                                        className="fa-solid fa-robot",
                                        style={"fontSize": "16px", "color": "#FFFFFF"},
                                    ),
                                    html.Span(
                                        "Genie AI",
                                        style={
                                            "fontWeight": "700",
                                            "color": "#FFFFFF",
                                            "fontSize": "15px",
                                        },
                                    ),
                                ],
                            ),
                            html.Button(
                                html.I(className="fa-solid fa-xmark", style={"fontSize": "14px"}),
                                id="chat-close-btn",
                                n_clicks=0,
                                style={
                                    "backgroundColor": "rgba(255,255,255,0.15)",
                                    "border": "none",
                                    "borderRadius": "6px",
                                    "padding": "6px 8px",
                                    "color": "#FFFFFF",
                                    "cursor": "pointer",
                                    "display": "flex",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                },
                            ),
                        ],
                    ),

                    # Chat messages area with loading spinner
                    html.Div(
                        style={"flex": "1", "overflow": "hidden", "position": "relative"},
                        children=dcc.Loading(
                            type="circle",
                            color=COLORS["blue"],
                            children=html.Div(
                                id="genie-response",
                                style={
                                    "height": "100%",
                                    "overflowY": "auto",
                                    "padding": "16px",
                                    "display": "flex",
                                    "flexDirection": "column",
                                    "gap": "8px",
                                },
                                children=[
                                    html.Div(
                                        style={"textAlign": "center", "padding": "32px 16px"},
                                        children=[
                                            html.I(
                                                className="fa-solid fa-robot",
                                                style={"fontSize": "28px", "color": COLORS["blue"],
                                                       "marginBottom": "8px", "display": "block"},
                                            ),
                                            html.Div(
                                                "Hi! I'm Genie AI",
                                                style={"fontSize": "15px", "fontWeight": "600",
                                                       "color": "#1F2937", "marginBottom": "4px"},
                                            ),
                                            html.Div(
                                                "Ask me anything about your data.",
                                                style={"fontSize": "12px", "color": "#9CA3AF"},
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                        ),
                    ),

                    # Input bar
                    html.Div(
                        style={
                            "padding": "12px 16px",
                            "borderTop": "1px solid #E5E7EB",
                        },
                        children=[
                            html.Div(
                                style={
                                    "display": "flex",
                                    "alignItems": "center",
                                    "backgroundColor": "#F3F4F6",
                                    "borderRadius": "24px",
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
                                            "padding": "8px 0",
                                        },
                                    ),
                                    html.Button(
                                        html.I(className="fa-solid fa-paper-plane", style={"fontSize": "11px"}),
                                        id="genie-send-btn",
                                        n_clicks=0,
                                        style={
                                            "width": "34px",
                                            "height": "34px",
                                            "borderRadius": "50%",
                                            "backgroundColor": COLORS["blue"],
                                            "border": "none",
                                            "color": "#FFFFFF",
                                            "cursor": "pointer",
                                            "display": "flex",
                                            "alignItems": "center",
                                            "justifyContent": "center",
                                            "flexShrink": "0",
                                        },
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
