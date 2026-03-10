"""
Blueprint brand theme for ManufacturingIQ Dash application.

Provides centralized color palette, status colors, typography, and the
base CSS stylesheet for the dark industrial theme.
"""

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------

COLORS = {
    "blue": "#233ED8",
    "blue_hover": "#1D35BB",
    "dark": "#0F1115",
    "panel": "#16181D",
    "border": "#272A31",
    "text_muted": "#A0A4B0",
    "white": "#FFFFFF",
    "green": "#22C55E",
    "yellow": "#EAB308",
    "red": "#EF4444",
    "purple": "#8B5CF6",
}

# ---------------------------------------------------------------------------
# Status badge colors  (bg, text, border)
# ---------------------------------------------------------------------------

STATUS_COLORS = {
    "Healthy": {
        "bg": "rgba(34, 197, 94, 0.12)",
        "text": "#22C55E",
        "border": "rgba(34, 197, 94, 0.30)",
    },
    "Low": {
        "bg": "rgba(234, 179, 8, 0.12)",
        "text": "#EAB308",
        "border": "rgba(234, 179, 8, 0.30)",
    },
    "Critical": {
        "bg": "rgba(239, 68, 68, 0.12)",
        "text": "#EF4444",
        "border": "rgba(239, 68, 68, 0.30)",
    },
    "Nominal": {
        "bg": "rgba(34, 197, 94, 0.12)",
        "text": "#22C55E",
        "border": "rgba(34, 197, 94, 0.30)",
    },
    "Defect": {
        "bg": "rgba(239, 68, 68, 0.12)",
        "text": "#EF4444",
        "border": "rgba(239, 68, 68, 0.30)",
    },
}

# ---------------------------------------------------------------------------
# Typography
# ---------------------------------------------------------------------------

FONT_FAMILY = "'Inter', 'Segoe UI', system-ui, sans-serif"

# ---------------------------------------------------------------------------
# Base stylesheet
# ---------------------------------------------------------------------------


def get_base_stylesheet() -> str:
    """Return a CSS string that applies the dark Blueprint theme globally."""
    return f"""
    /* ---------- reset & base ---------- */
    * {{
        box-sizing: border-box;
        margin: 0;
        padding: 0;
    }}

    body, html {{
        background-color: {COLORS["dark"]};
        color: {COLORS["white"]};
        font-family: {FONT_FAMILY};
        font-size: 14px;
        line-height: 1.5;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
    }}

    /* ---------- scrollbar ---------- */
    ::-webkit-scrollbar {{
        width: 6px;
        height: 6px;
    }}
    ::-webkit-scrollbar-track {{
        background: {COLORS["dark"]};
    }}
    ::-webkit-scrollbar-thumb {{
        background: {COLORS["border"]};
        border-radius: 3px;
    }}

    /* ---------- card ---------- */
    .card {{
        background-color: {COLORS["panel"]};
        border: 1px solid {COLORS["border"]};
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
    }}

    .card-title {{
        font-size: 13px;
        font-weight: 500;
        color: {COLORS["text_muted"]};
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 8px;
    }}

    .card-value {{
        font-size: 32px;
        font-weight: 700;
        line-height: 1.1;
    }}

    .card-subtitle {{
        font-size: 12px;
        color: {COLORS["text_muted"]};
        margin-top: 4px;
    }}

    /* ---------- KPI accent colors ---------- */
    .accent-blue {{ color: {COLORS["blue"]}; }}
    .accent-purple {{ color: {COLORS["purple"]}; }}
    .accent-green {{ color: {COLORS["green"]}; }}
    .accent-red {{ color: {COLORS["red"]}; }}
    .accent-yellow {{ color: {COLORS["yellow"]}; }}

    /* ---------- status badge ---------- */
    .status-badge {{
        display: inline-block;
        padding: 2px 10px;
        border-radius: 9999px;
        font-size: 12px;
        font-weight: 600;
        line-height: 20px;
    }}

    /* ---------- sidebar ---------- */
    .sidebar {{
        background-color: {COLORS["panel"]};
        border-right: 1px solid {COLORS["border"]};
        height: 100vh;
        overflow-y: auto;
        padding: 16px 0;
    }}

    .sidebar-brand {{
        padding: 12px 20px 24px 20px;
        border-bottom: 1px solid {COLORS["border"]};
        margin-bottom: 8px;
    }}

    .sidebar-brand h2 {{
        font-size: 16px;
        font-weight: 700;
        color: {COLORS["white"]};
        margin: 0;
    }}

    .sidebar-brand p {{
        font-size: 11px;
        color: {COLORS["text_muted"]};
        margin: 2px 0 0 0;
    }}

    .nav-link {{
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 10px 20px;
        color: {COLORS["text_muted"]};
        text-decoration: none;
        font-size: 13px;
        font-weight: 500;
        border-radius: 0;
        transition: background-color 0.15s, color 0.15s;
        cursor: pointer;
    }}

    .nav-link:hover {{
        background-color: rgba(35, 62, 216, 0.08);
        color: {COLORS["white"]};
    }}

    .nav-link.active {{
        background-color: rgba(35, 62, 216, 0.15);
        color: {COLORS["blue"]};
        border-left: 3px solid {COLORS["blue"]};
    }}

    .nav-link i {{
        width: 18px;
        text-align: center;
        font-size: 14px;
    }}

    /* ---------- genie panel ---------- */
    .genie-panel {{
        background-color: {COLORS["panel"]};
        border-right: 1px solid {COLORS["border"]};
        height: 100vh;
        overflow-y: auto;
        padding: 20px;
        transition: width 0.3s ease, opacity 0.3s ease;
    }}

    .genie-panel h3 {{
        font-size: 14px;
        font-weight: 600;
        color: {COLORS["white"]};
        margin-bottom: 16px;
    }}

    .genie-question {{
        background-color: rgba(35, 62, 216, 0.06);
        border: 1px solid {COLORS["border"]};
        border-radius: 8px;
        padding: 10px 14px;
        margin-bottom: 8px;
        font-size: 12px;
        color: {COLORS["text_muted"]};
        cursor: pointer;
        transition: border-color 0.15s, color 0.15s;
    }}

    .genie-question:hover {{
        border-color: {COLORS["blue"]};
        color: {COLORS["white"]};
    }}

    /* ---------- page header ---------- */
    .page-header {{
        padding: 24px 32px 16px 32px;
        border-bottom: 1px solid {COLORS["border"]};
        margin-bottom: 24px;
    }}

    .page-header h1 {{
        font-size: 22px;
        font-weight: 700;
        color: {COLORS["white"]};
        margin: 0;
    }}

    .page-header p {{
        font-size: 13px;
        color: {COLORS["text_muted"]};
        margin: 4px 0 0 0;
    }}

    /* ---------- content area ---------- */
    .content-area {{
        padding: 0 32px 32px 32px;
        overflow-y: auto;
        height: calc(100vh - 80px);
    }}

    /* ---------- data table ---------- */
    .dash-table {{
        font-family: {FONT_FAMILY};
    }}

    /* ---------- toggle button ---------- */
    .genie-toggle {{
        background: none;
        border: 1px solid {COLORS["border"]};
        border-radius: 8px;
        color: {COLORS["text_muted"]};
        padding: 6px 10px;
        cursor: pointer;
        font-size: 14px;
        transition: color 0.15s, border-color 0.15s;
    }}

    .genie-toggle:hover {{
        color: {COLORS["white"]};
        border-color: {COLORS["blue"]};
    }}

    /* ---------- utilities ---------- */
    .text-muted {{ color: {COLORS["text_muted"]}; }}
    .text-white {{ color: {COLORS["white"]}; }}
    .text-blue {{ color: {COLORS["blue"]}; }}
    .mt-0 {{ margin-top: 0; }}
    .mb-0 {{ margin-bottom: 0; }}
    .mb-8 {{ margin-bottom: 8px; }}
    .mb-16 {{ margin-bottom: 16px; }}
    .mb-24 {{ margin-bottom: 24px; }}
    .flex {{ display: flex; }}
    .flex-wrap {{ flex-wrap: wrap; }}
    .gap-16 {{ gap: 16px; }}
    .grid-4 {{
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
    }}
    @media (max-width: 1200px) {{
        .grid-4 {{
            grid-template-columns: repeat(2, 1fr);
        }}
    }}
    @media (max-width: 768px) {{
        .grid-4 {{
            grid-template-columns: 1fr;
        }}
    }}
    """
