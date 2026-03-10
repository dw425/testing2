"""Tests for config structure and page rendering across all verticals."""

import os
import pytest
import yaml


V2_VERTICALS = ["gaming", "telecom", "media", "financial_services", "hls"]
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")


def test_all_verticals_config_exists():
    for vertical in V2_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        assert os.path.exists(path), f"Missing config: {vertical}.yaml"


def test_old_verticals_removed():
    for old in ["manufacturing", "risk", "healthcare"]:
        path = os.path.join(CONFIG_DIR, f"{old}.yaml")
        assert not os.path.exists(path), f"Old config should be removed: {old}.yaml"


def test_config_has_required_sections():
    for vertical in V2_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        required = ["app", "brand", "data", "ml", "pages", "genie", "dashboard"]
        for section in required:
            assert section in cfg, f"{vertical}: Missing config section: {section}"


def test_pages_have_required_fields():
    for vertical in V2_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        for page in cfg["pages"]:
            assert "id" in page, f"{vertical}: page missing 'id': {page}"
            assert "label" in page, f"{vertical}: page missing 'label': {page}"
            assert "icon" in page, f"{vertical}: page missing 'icon': {page}"


def test_dashboard_kpis_have_required_fields():
    for vertical in V2_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        kpis = cfg.get("dashboard", {}).get("kpis", [])
        assert len(kpis) >= 4, f"{vertical}: should have at least 4 KPIs"
        for kpi in kpis:
            assert "title" in kpi, f"{vertical}: KPI missing 'title': {kpi}"
            assert "accent" in kpi, f"{vertical}: KPI missing 'accent': {kpi}"


def test_genie_has_tables_and_questions():
    for vertical in V2_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        genie = cfg.get("genie", {})
        assert len(genie.get("tables", [])) >= 4, f"{vertical}: should have >= 4 genie tables"
        assert len(genie.get("sample_questions", [])) == 10, f"{vertical}: should have 10 questions"


def test_page_renderers_importable():
    from app.pages import gaming, telecom, media, financial_services, hls
    modules = {
        "gaming": gaming,
        "telecom": telecom,
        "media": media,
        "financial_services": financial_services,
        "hls": hls,
    }
    for name, mod in modules.items():
        funcs = [f for f in dir(mod) if f.startswith("render_")]
        assert len(funcs) == 6, f"{name}: should have 6 render functions, got {len(funcs)}: {funcs}"


def test_all_pages_render():
    from app.data_access import get_config_for, set_active_vertical
    from app.pages import gaming, telecom, media, financial_services, hls
    modules = {
        "gaming": gaming,
        "telecom": telecom,
        "media": media,
        "financial_services": financial_services,
        "hls": hls,
    }
    for name, mod in modules.items():
        cfg = get_config_for(name)
        set_active_vertical(name)
        for fn_name in [f for f in dir(mod) if f.startswith("render_")]:
            fn = getattr(mod, fn_name)
            result = fn(cfg)
            assert result is not None, f"{name}.{fn_name} returned None"
