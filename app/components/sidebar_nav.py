"""Collapsible sidebar navigation component for ManufacturingIQ Databricks app."""

from dash import html, dcc


def make_sidebar(pages, active_page):
    """Create a collapsible sidebar navigation with dark Blueprint theme.

    Args:
        pages: List of page config dicts, each with keys:
            - id: Unique page identifier (used for URL path).
            - label: Display text for the nav item.
            - icon: FontAwesome icon class (e.g. 'fa-chart-line').
        active_page: The id of the currently active page.

    Returns:
        A dash html.Div component representing the full sidebar.
    """

    # Build navigation items
    nav_items = []
    for page in pages:
        page_id = page.get("id", "")
        label = page.get("label", "")
        icon = page.get("icon", "fa-circle")
        is_active = page_id == active_page

        # Active state styles
        bg_color = "#22252E" if is_active else "transparent"
        text_color = "#EAEBF0" if is_active else "#8A8D98"
        icon_color = "#3A7BF7" if is_active else "#6C6F7A"
        left_accent = "3px solid #3A7BF7" if is_active else "3px solid transparent"
        font_weight = "600" if is_active else "400"

        nav_item = dcc.Link(
            children=html.Div(
                children=[
                    html.I(
                        className=f"fa {icon}",
                        style={
                            "fontSize": "16px",
                            "color": icon_color,
                            "width": "24px",
                            "textAlign": "center",
                            "flexShrink": "0",
                            "transition": "color 0.2s ease",
                        },
                    ),
                    html.Span(
                        label,
                        className="sidebar-label",
                        style={
                            "fontSize": "14px",
                            "color": text_color,
                            "fontWeight": font_weight,
                            "marginLeft": "12px",
                            "whiteSpace": "nowrap",
                            "overflow": "hidden",
                            "transition": "opacity 0.2s ease, color 0.2s ease",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "padding": "10px 16px",
                    "borderLeft": left_accent,
                    "backgroundColor": bg_color,
                    "borderRadius": "0 6px 6px 0",
                    "marginRight": "8px",
                    "transition": "background-color 0.2s ease",
                    "cursor": "pointer",
                },
            ),
            href=f"/{page_id}" if page_id else "/",
            style={
                "textDecoration": "none",
                "display": "block",
                "marginBottom": "2px",
            },
        )
        nav_items.append(nav_item)

    # Collapse/Expand toggle button
    collapse_button = html.Button(
        children=[
            html.I(
                className="fa fa-chevron-left",
                id="sidebar-collapse-icon",
                style={
                    "fontSize": "14px",
                    "color": "#8A8D98",
                    "transition": "transform 0.3s ease",
                },
            ),
        ],
        id="sidebar-collapse-btn",
        n_clicks=0,
        style={
            "backgroundColor": "transparent",
            "border": "1px solid #272A31",
            "borderRadius": "6px",
            "padding": "6px 10px",
            "cursor": "pointer",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center",
            "width": "32px",
            "height": "32px",
        },
    )

    # Blueprint logo / branding area
    branding = html.Div(
        children=[
            html.Div(
                children=[
                    # Logo icon
                    html.Div(
                        children=[
                            html.I(
                                className="fa fa-industry",
                                style={
                                    "fontSize": "20px",
                                    "color": "#3A7BF7",
                                },
                            ),
                        ],
                        style={
                            "width": "36px",
                            "height": "36px",
                            "borderRadius": "8px",
                            "backgroundColor": "rgba(58, 123, 247, 0.1)",
                            "display": "flex",
                            "alignItems": "center",
                            "justifyContent": "center",
                            "flexShrink": "0",
                        },
                    ),
                    # Brand text
                    html.Div(
                        children=[
                            html.Div(
                                "ManufacturingIQ",
                                className="sidebar-label",
                                style={
                                    "fontSize": "15px",
                                    "fontWeight": "700",
                                    "color": "#EAEBF0",
                                    "lineHeight": "1.2",
                                    "whiteSpace": "nowrap",
                                    "overflow": "hidden",
                                },
                            ),
                            html.Div(
                                "Blueprint",
                                className="sidebar-label",
                                style={
                                    "fontSize": "11px",
                                    "fontWeight": "400",
                                    "color": "#6C6F7A",
                                    "whiteSpace": "nowrap",
                                    "overflow": "hidden",
                                },
                            ),
                        ],
                        style={"marginLeft": "10px"},
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                },
            ),
        ],
        style={
            "padding": "20px 16px 16px 16px",
        },
    )

    # Header row with branding and collapse button
    header = html.Div(
        children=[
            branding,
            html.Div(
                collapse_button,
                style={
                    "padding": "0 16px 12px 16px",
                    "display": "flex",
                    "justifyContent": "flex-end",
                },
            ),
        ],
    )

    # Divider
    divider = html.Hr(
        style={
            "border": "none",
            "borderTop": "1px solid #272A31",
            "margin": "0 16px 12px 16px",
        },
    )

    # Section label
    section_label = html.Div(
        "NAVIGATION",
        className="sidebar-label",
        style={
            "fontSize": "10px",
            "fontWeight": "600",
            "color": "#6C6F7A",
            "letterSpacing": "1px",
            "padding": "4px 16px 8px 20px",
            "textTransform": "uppercase",
        },
    )

    # Full sidebar container
    sidebar = html.Div(
        id="sidebar",
        children=[
            header,
            divider,
            section_label,
            html.Nav(
                children=nav_items,
                style={"flex": "1", "overflowY": "auto"},
            ),
            # Footer area
            html.Div(
                children=[
                    html.Hr(
                        style={
                            "border": "none",
                            "borderTop": "1px solid #272A31",
                            "margin": "0 16px 12px 16px",
                        },
                    ),
                    html.Div(
                        children=[
                            html.I(
                                className="fa fa-database",
                                style={
                                    "fontSize": "12px",
                                    "color": "#2ECC71",
                                    "marginRight": "8px",
                                },
                            ),
                            html.Span(
                                "Databricks Connected",
                                className="sidebar-label",
                                style={
                                    "fontSize": "11px",
                                    "color": "#6C6F7A",
                                    "whiteSpace": "nowrap",
                                    "overflow": "hidden",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "padding": "0 16px 16px 20px",
                        },
                    ),
                ],
            ),
        ],
        style={
            "width": "240px",
            "minWidth": "240px",
            "height": "100vh",
            "backgroundColor": "#16181D",
            "borderRight": "1px solid #272A31",
            "display": "flex",
            "flexDirection": "column",
            "fontFamily": "'Inter', 'Segoe UI', -apple-system, sans-serif",
            "transition": "width 0.3s ease, min-width 0.3s ease",
            "overflow": "hidden",
            "position": "sticky",
            "top": "0",
        },
    )

    return sidebar
