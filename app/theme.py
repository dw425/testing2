"""
Blueprint brand theme for ManufacturingIQ Dash application.

Provides centralized color palette, status colors, typography, and the
base CSS stylesheet for the dark industrial theme.
"""

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------

COLORS = {
    "blue": "#4B7BF5",
    "blue_hover": "#3D6BE0",
    # 4-tier dark surface hierarchy (GitHub Dark-inspired)
    "bg_base": "#0D1117",       # Deepest layer — page canvas
    "dark": "#131620",          # Legacy alias (≈ bg_base variant)
    "surface": "#161B22",       # Card backgrounds, content containers
    "panel": "#1A1F2E",         # Legacy alias (≈ surface)
    "surface_elevated": "#21262D",  # Modals, dropdowns, hover states
    "surface_highest": "#30363D",   # Active selections, pressed states
    # Borders — semi-transparent for theme coherence
    "border_subtle": "rgba(255,255,255,0.06)",
    "border": "#272D3F",        # Default border
    "border_strong": "rgba(255,255,255,0.12)",
    # Text hierarchy
    "text_primary": "#E6EDF3",  # Main text — off-white, not pure white
    "white": "#E8ECF4",         # Legacy alias
    "text_secondary": "#8B949E",  # Descriptions, secondary info
    "text_muted": "#8892A7",    # Legacy alias
    "text_tertiary": "#484F58", # Disabled, very subtle labels
    # Semantic accents
    "green": "#34D399",
    "yellow": "#FBBF24",
    "red": "#F87171",
    "purple": "#A78BFA",
    "cyan": "#00E5FF",
    "orange": "#FF6D00",
    "coral": "#FF5252",
}

# ---------------------------------------------------------------------------
# Per-vertical color themes
# ---------------------------------------------------------------------------

VERTICAL_THEMES = {
    "gaming": {
        "accent_primary": "#00E5FF",
        "accent_secondary": "#FF007F",
        "bg_base": "#050816",
        "chart_colorway": ["#00E5FF", "#FF007F", "#A78BFA", "#34D399", "#FBBF24", "#F87171"],
    },
    "telecom": {
        "accent_primary": "#0091D5",
        "accent_secondary": "#26A69A",
        "bg_base": "#0F1923",
        "chart_colorway": ["#0091D5", "#26A69A", "#4B7BF5", "#34D399", "#FBBF24", "#F87171"],
    },
    "financial_services": {
        "accent_primary": "#1C4E80",
        "accent_secondary": "#288CFA",
        "bg_base": "#0D1117",
        "chart_colorway": ["#288CFA", "#34D399", "#F87171", "#FBBF24", "#A78BFA", "#1C4E80"],
    },
    "hls": {
        "accent_primary": "#00897B",
        "accent_secondary": "#1976D2",
        "bg_base": "#FAFAFA",       # Light theme for healthcare
        "is_light": True,
        "chart_colorway": ["#00897B", "#1976D2", "#F87171", "#FBBF24", "#A78BFA", "#34D399"],
    },
    "manufacturing": {
        "accent_primary": "#FF6D00",
        "accent_secondary": "#34D399",
        "bg_base": "#1B2631",
        "chart_colorway": ["#FF6D00", "#34D399", "#4B7BF5", "#FBBF24", "#F87171", "#A78BFA"],
    },
    "media": {
        "accent_primary": "#7C4DFF",
        "accent_secondary": "#FF5252",
        "bg_base": "#121212",
        "chart_colorway": ["#7C4DFF", "#FF5252", "#34D399", "#FBBF24", "#4B7BF5", "#A78BFA"],
    },
    "risk": {
        "accent_primary": "#1A237E",
        "accent_secondary": "#4B7BF5",
        "bg_base": "#0D1117",
        "chart_colorway": ["#4B7BF5", "#F87171", "#FBBF24", "#34D399", "#A78BFA", "#1A237E"],
    },
}


def get_vertical_theme(vertical_name: str) -> dict:
    """Return the theme dict for a given vertical, falling back to defaults."""
    return VERTICAL_THEMES.get(vertical_name, {
        "accent_primary": COLORS["blue"],
        "accent_secondary": COLORS["purple"],
        "bg_base": COLORS["bg_base"],
        "chart_colorway": [COLORS["blue"], COLORS["green"], COLORS["red"],
                           COLORS["yellow"], COLORS["purple"], COLORS["cyan"]],
    })

# ---------------------------------------------------------------------------
# Status badge colors  (bg, text, border)
# ---------------------------------------------------------------------------

STATUS_COLORS = {
    "Healthy": {
        "bg": "rgba(52, 211, 153, 0.12)",
        "text": "#34D399",
        "border": "rgba(52, 211, 153, 0.30)",
    },
    "Low": {
        "bg": "rgba(251, 191, 36, 0.12)",
        "text": "#FBBF24",
        "border": "rgba(251, 191, 36, 0.30)",
    },
    "Critical": {
        "bg": "rgba(248, 113, 113, 0.12)",
        "text": "#F87171",
        "border": "rgba(248, 113, 113, 0.30)",
    },
    "Nominal": {
        "bg": "rgba(52, 211, 153, 0.12)",
        "text": "#34D399",
        "border": "rgba(52, 211, 153, 0.30)",
    },
    "Defect": {
        "bg": "rgba(248, 113, 113, 0.12)",
        "text": "#F87171",
        "border": "rgba(248, 113, 113, 0.30)",
    },
    "Warning": {
        "bg": "rgba(251, 191, 36, 0.12)",
        "text": "#FBBF24",
        "border": "rgba(251, 191, 36, 0.30)",
    },
    "Info": {
        "bg": "rgba(75, 123, 245, 0.12)",
        "text": "#4B7BF5",
        "border": "rgba(75, 123, 245, 0.30)",
    },
}

# ---------------------------------------------------------------------------
# Typography
# ---------------------------------------------------------------------------

FONT_FAMILY = "'DM Sans', 'Inter', 'Segoe UI', system-ui, sans-serif"

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
        background-color: {COLORS["surface"]};
        border: 1px solid {COLORS["border"]};
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
        transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
    }}
    .card:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
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

    .clickable-card {{
        cursor: pointer;
        transition: border-color 0.2s ease, box-shadow 0.2s ease;
    }}
    .clickable-card:hover {{
        border-color: {COLORS["blue"]} !important;
        box-shadow: 0 0 0 1px rgba(75, 123, 245, 0.2);
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
        background-color: rgba(75, 123, 245, 0.08);
        color: {COLORS["white"]};
    }}

    .nav-link.active {{
        background-color: rgba(75, 123, 245, 0.15);
        color: {COLORS["blue"]};
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
        background-color: rgba(75, 123, 245, 0.06);
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
        scroll-behavior: smooth;
    }}

    /* ---------- page transition ---------- */
    @keyframes pageFadeIn {{
        from {{ opacity: 0; transform: translateY(6px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    #page-content > * {{
        animation: pageFadeIn 0.25s ease-out;
    }}

    /* Dark loading overlay for page transitions */
    .dash-loading .dash-spinner-container {{
        background-color: rgba(19, 22, 32, 0.7) !important;
    }}

    /* ---------- staggered card entry ---------- */
    @keyframes fadeInUp {{
        from {{ opacity: 0; transform: translateY(12px); }}
        to {{ opacity: 1; transform: translateY(0); }}
    }}
    .content-area .card {{
        animation: fadeInUp 0.35s ease-out both;
    }}
    .content-area .card:nth-child(1) {{ animation-delay: 0s; }}
    .content-area .card:nth-child(2) {{ animation-delay: 0.05s; }}
    .content-area .card:nth-child(3) {{ animation-delay: 0.1s; }}
    .content-area .card:nth-child(4) {{ animation-delay: 0.15s; }}
    .content-area .card:nth-child(5) {{ animation-delay: 0.2s; }}
    .content-area .card:nth-child(6) {{ animation-delay: 0.25s; }}

    /* ---------- status health pulse ---------- */
    @keyframes statusPulse {{
        0%, 100% {{ opacity: 1; }}
        50% {{ opacity: 0.5; }}
    }}
    .status-pulse {{
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        animation: statusPulse 2s ease-in-out infinite;
    }}
    .status-pulse-green {{ background-color: {COLORS["green"]}; box-shadow: 0 0 6px {COLORS["green"]}50; }}
    .status-pulse-amber {{ background-color: {COLORS["yellow"]}; box-shadow: 0 0 6px {COLORS["yellow"]}50; }}
    .status-pulse-red {{ background-color: {COLORS["red"]}; box-shadow: 0 0 6px {COLORS["red"]}50; }}

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

    /* ---------- genie panel (mockup-style white bg) ---------- */
    .genie-panel-mockup {{
        /* Base styles set via inline, this adds scrollbar overrides */
    }}

    .genie-panel-mockup ::-webkit-scrollbar-track {{
        background: transparent;
    }}
    .genie-panel-mockup ::-webkit-scrollbar-thumb {{
        background: #D1D5DB;
        border-radius: 3px;
    }}

    .genie-question-card:hover {{
        background-color: #EFF6FF !important;
    }}

    /* Genie send button hover */
    #genie-send-btn:hover {{
        color: {COLORS["blue"]} !important;
        border-color: {COLORS["blue"]} !important;
    }}

    /* Genie close button hover */
    #genie-close-btn:hover {{
        background-color: #E5E7EB !important;
    }}

    /* ---------- chat message bubbles ---------- */
    .genie-msg-user {{
        background-color: {COLORS["blue"]};
        color: #FFFFFF;
        padding: 10px 14px;
        border-radius: 12px 12px 4px 12px;
        font-size: 13px;
        line-height: 1.5;
        max-width: 85%;
        margin-left: auto;
        margin-bottom: 8px;
        word-wrap: break-word;
    }}

    .genie-msg-ai {{
        background-color: #F3F4F6;
        color: #1F2937;
        padding: 12px 14px;
        border-radius: 12px 12px 12px 4px;
        font-size: 13px;
        line-height: 1.6;
        max-width: 95%;
        margin-bottom: 8px;
        word-wrap: break-word;
    }}

    .genie-msg-ai strong {{
        color: #111827;
    }}

    .genie-msg-error {{
        border-left: 3px solid #F87171;
        background-color: #FEF2F2 !important;
    }}

    .genie-msg-sql {{
        background-color: {COLORS["dark"]};
        color: #E2E8F0;
        padding: 10px 12px;
        border-radius: 8px;
        font-family: 'Courier New', Courier, monospace;
        font-size: 11px;
        line-height: 1.5;
        overflow-x: auto;
        margin-top: 8px;
        border: 1px solid {COLORS["border"]};
    }}

    .genie-msg-source {{
        font-size: 10px;
        color: #9CA3AF;
        text-align: right;
        margin-top: 4px;
        margin-bottom: 12px;
    }}

    .genie-msg-divider {{
        border: none;
        border-top: 1px solid #E5E7EB;
        margin: 16px 0;
    }}

    /* ---------- landing overlay ---------- */
    .landing-overlay {{
        height: 100vh; display: flex; flex-direction: column;
        align-items: center; justify-content: center;
        background: linear-gradient(135deg, {COLORS["dark"]} 0%, #1a1d28 50%, {COLORS["dark"]} 100%);
    }}
    .landing-title {{ font-size: 48px; font-weight: 700; color: white; margin-bottom: 8px; }}
    .landing-subtitle {{ font-size: 18px; color: {COLORS["text_muted"]}; margin-bottom: 40px; }}
    .landing-enter-btn {{
        background: {COLORS["blue"]}; color: white; border: none; padding: 16px 48px;
        border-radius: 12px; font-size: 16px; font-weight: 600; cursor: pointer;
        transition: background 0.2s; text-decoration: none;
    }}
    .landing-enter-btn:hover {{ background: {COLORS["blue_hover"]}; color: white; }}

    /* ---------- hub grid ---------- */
    .hub-header {{ text-align: center; padding: 48px 32px 32px; }}
    .hub-title {{ font-size: 32px; font-weight: 700; color: white; }}
    .hub-subtitle {{ font-size: 16px; color: {COLORS["text_muted"]}; margin-top: 8px; }}
    .hub-grid {{
        display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        gap: 24px; padding: 0 48px 48px; max-width: 1400px; margin: 0 auto;
    }}
    .vertical-card {{
        background: {COLORS["panel"]}; border: 1px solid {COLORS["border"]}; border-radius: 16px;
        padding: 32px; cursor: pointer; transition: all 0.2s; text-decoration: none; display: block;
    }}
    .vertical-card:hover {{ border-color: {COLORS["blue"]}; transform: translateY(-4px); box-shadow: 0 8px 32px rgba(35,62,216,0.15); }}
    .vertical-card-icon {{ width: 56px; height: 56px; border-radius: 12px; display: flex; align-items: center; justify-content: center; margin-bottom: 20px; font-size: 24px; }}
    .vertical-card-title {{ font-size: 20px; font-weight: 700; color: white; margin-bottom: 4px; }}
    .vertical-card-subtitle {{ font-size: 14px; color: {COLORS["text_muted"]}; margin-bottom: 16px; }}
    .vertical-card-stats {{ display: flex; gap: 24px; }}
    .vertical-card-stat {{ font-size: 12px; color: {COLORS["text_muted"]}; }}
    .vertical-card-stat strong {{ color: white; font-size: 14px; display: block; }}

    /* ---------- back to hub link ---------- */
    .back-to-hub {{
        display: flex; align-items: center; gap: 8px; padding: 12px 20px; margin-bottom: 8px;
        color: {COLORS["text_muted"]}; text-decoration: none; font-size: 13px; font-weight: 500;
        border-bottom: 1px solid {COLORS["border"]}; transition: color 0.15s;
    }}
    .back-to-hub:hover {{ color: {COLORS["blue"]}; }}

    /* smooth sidebar transitions */
    .sidebar {{
        transition: width 0.2s ease;
    }}
    .nav-link {{
        position: relative;
        overflow: hidden;
    }}
    .nav-link.active::before {{
        content: '';
        position: absolute;
        left: 0;
        top: 0;
        bottom: 0;
        width: 3px;
        background: {COLORS["blue"]};
        border-radius: 0 2px 2px 0;
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

    /* ---------- dark dcc.Dropdown ---------- */
    .Select-control {{
        background-color: {COLORS["panel"]} !important;
        border-color: {COLORS["border"]} !important;
        border-radius: 8px !important;
        cursor: pointer !important;
    }}
    .Select-menu-outer {{
        background-color: {COLORS["surface_elevated"]} !important;
        border-color: {COLORS["border"]} !important;
        border-radius: 0 0 8px 8px !important;
        z-index: 100 !important;
    }}
    .Select-option {{
        background-color: {COLORS["surface_elevated"]} !important;
        color: {COLORS["white"]} !important;
    }}
    .Select-option.is-focused {{
        background-color: rgba(75, 123, 245, 0.15) !important;
    }}
    .Select-value-label, .Select-value {{
        color: {COLORS["white"]} !important;
    }}
    .Select-placeholder {{
        color: {COLORS["text_muted"]} !important;
    }}
    .Select-input input {{
        color: {COLORS["white"]} !important;
    }}
    .Select-arrow {{
        border-color: {COLORS["text_muted"]} transparent transparent !important;
    }}
    .Select.is-open > .Select-control {{
        border-color: {COLORS["blue"]} !important;
    }}
    .Select-clear-zone .Select-clear {{
        color: {COLORS["text_muted"]} !important;
    }}
    .has-value.Select--single > .Select-control .Select-value .Select-value-label {{
        color: {COLORS["white"]} !important;
    }}

    /* ---------- dark DataTable ---------- */
    .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner input {{
        color: {COLORS["white"]} !important;
        background-color: {COLORS["dark"]} !important;
    }}
    /* Hide native filter row -- replaced by dropdown filters above table */
    .dash-table-container .dash-spreadsheet-container .dash-filter {{
        display: none !important;
    }}
    .dash-table-container .previous-next-container {{
        background-color: {COLORS["panel"]} !important;
        border: 1px solid {COLORS["border"]} !important;
        border-top: none !important;
        padding: 8px !important;
    }}
    .dash-table-container .previous-next-container button {{
        color: {COLORS["white"]} !important;
        background-color: {COLORS["panel"]} !important;
        border: 1px solid {COLORS["border"]} !important;
        border-radius: 6px !important;
        cursor: pointer !important;
    }}
    .dash-table-container .previous-next-container button:hover {{
        background-color: rgba(75, 123, 245, 0.15) !important;
        border-color: {COLORS["blue"]} !important;
    }}

    /* ---------- dcc.Tabs dark theme ---------- */
    .tab-container {{
        border-bottom: 1px solid {COLORS["border"]} !important;
    }}
    .tab {{
        border: none !important;
        background-color: transparent !important;
    }}
    .tab--selected {{
        border-bottom: 2px solid {COLORS["blue"]} !important;
        color: {COLORS["white"]} !important;
    }}

    /* ---------- row selection highlight ---------- */
    .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner td.cell--selected {{
        background-color: rgba(75, 123, 245, 0.15) !important;
        border-color: {COLORS["blue"]} !important;
    }}

    /* ---------- sample question chips ---------- */
    .sample-question-chip {{
        display: block;
        width: 100%;
        text-align: left;
        background-color: #F9FAFB;
        border: 1px solid #E5E7EB;
        border-radius: 10px;
        padding: 10px 14px;
        margin-bottom: 6px;
        font-size: 12px;
        color: #374151;
        cursor: pointer;
        font-family: {FONT_FAMILY};
        line-height: 1.4;
        transition: border-color 0.15s, background-color 0.15s;
    }}
    .sample-question-chip:hover {{
        border-color: {COLORS["blue"]};
        background-color: #EFF6FF;
    }}

    /* ---------- DataTable export button ---------- */
    .dash-table-container .export {{
        color: {COLORS["text_muted"]} !important;
        background-color: {COLORS["panel"]} !important;
        border: 1px solid {COLORS["border"]} !important;
        border-radius: 6px !important;
        padding: 4px 12px !important;
        font-size: 11px !important;
        cursor: pointer !important;
        margin: 8px !important;
        font-family: {FONT_FAMILY} !important;
    }}
    .dash-table-container .export:hover {{
        background-color: rgba(75, 123, 245, 0.15) !important;
        border-color: {COLORS["blue"]} !important;
        color: {COLORS["white"]} !important;
    }}

    /* ---------- AI message list styling ---------- */
    .genie-msg-ai ul, .genie-msg-ai ol {{
        margin: 4px 0;
        padding-left: 20px;
    }}
    .genie-msg-ai li {{
        margin-bottom: 2px;
    }}

    /* ---------- responsive breakpoints ---------- */
    @media (max-width: 1200px) {{
        .content-area {{
            padding: 0 20px 20px 20px;
        }}
        .hub-grid {{
            padding: 0 24px 24px;
        }}
    }}
    @media (max-width: 768px) {{
        .content-area {{
            padding: 0 12px 12px 12px;
        }}
        .page-header {{
            padding: 16px 12px 12px 12px;
        }}
        .hub-grid {{
            grid-template-columns: 1fr !important;
            padding: 0 12px 24px;
        }}
        .sidebar {{
            display: none;
        }}
    }}

    /* ---------- chart transition smoothing ---------- */
    .js-plotly-plot .plotly .main-svg {{
        transition: opacity 0.3s ease;
    }}

    /* ---------- insight card accent border animation ---------- */
    @keyframes insightPulse {{
        0%, 100% {{ opacity: 1; }}
        50% {{ opacity: 0.7; }}
    }}

    /* ---------- reduce motion for accessibility ---------- */
    @media (prefers-reduced-motion: reduce) {{
        *, *::before, *::after {{
            animation-duration: 0.01ms !important;
            animation-iteration-count: 1 !important;
            transition-duration: 0.01ms !important;
        }}
    }}
    """
