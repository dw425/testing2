"""Tests for config structure and page rendering across all verticals."""

import os
import pytest
import yaml


ALL_VERTICALS = ["gaming", "telecom", "media", "financial_services", "hls",
                 "manufacturing", "risk"]
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "config")


def test_all_verticals_config_exists():
    for vertical in ALL_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        assert os.path.exists(path), f"Missing config: {vertical}.yaml"


def test_old_verticals_removed():
    for old in ["healthcare"]:
        path = os.path.join(CONFIG_DIR, f"{old}.yaml")
        assert not os.path.exists(path), f"Old config should be removed: {old}.yaml"


def test_config_has_required_sections():
    for vertical in ALL_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        required = ["app", "brand", "data", "ml", "pages", "genie", "dashboard"]
        for section in required:
            assert section in cfg, f"{vertical}: Missing config section: {section}"


def test_pages_have_required_fields():
    for vertical in ALL_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        for page in cfg["pages"]:
            assert "id" in page, f"{vertical}: page missing 'id': {page}"
            assert "label" in page, f"{vertical}: page missing 'label': {page}"
            assert "icon" in page, f"{vertical}: page missing 'icon': {page}"


def test_dashboard_kpis_have_required_fields():
    for vertical in ALL_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        kpis = cfg.get("dashboard", {}).get("kpis", [])
        assert len(kpis) >= 8, f"{vertical}: should have at least 8 KPIs"
        for kpi in kpis:
            assert "title" in kpi, f"{vertical}: KPI missing 'title': {kpi}"
            assert "accent" in kpi, f"{vertical}: KPI missing 'accent': {kpi}"


def test_genie_has_tables_and_questions():
    for vertical in ALL_VERTICALS:
        path = os.path.join(CONFIG_DIR, f"{vertical}.yaml")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        genie = cfg.get("genie", {})
        assert len(genie.get("tables", [])) >= 4, f"{vertical}: should have >= 4 genie tables"
        assert len(genie.get("sample_questions", [])) == 100, f"{vertical}: should have 100 questions"


def test_page_renderers_importable():
    from app.pages import gaming, telecom, media, financial_services, hls
    from app.pages import manufacturing, risk
    expected_funcs = {
        "gaming": ["render_build_games", "render_dashboard", "render_efficient_ops",
                    "render_grow_playerbase", "render_grow_revenue", "render_know_player", "render_live_ops"],
        "telecom": ["render_b2b_enterprise", "render_consumer_cx", "render_cyber_security",
                     "render_dashboard", "render_field_energy", "render_fraud_prevention", "render_network_ops"],
        "media": ["render_ad_yield", "render_audience_intel", "render_content_strategy",
                   "render_dashboard", "render_personalization_ai", "render_platform_delivery", "render_subscription_intel"],
        "financial_services": ["render_dashboard", "render_fraud_cyber", "render_investment_alpha",
                                "render_operations", "render_regulatory", "render_risk_management", "render_trading_advisory"],
        "hls": ["render_biopharma_intel", "render_dashboard", "render_healthcare_ops",
                 "render_medtech_surgery", "render_network_quality", "render_patient_outcomes", "render_supply_chain"],
        "manufacturing": ["render_dashboard", "render_energy_sustainability", "render_predictive_maintenance",
                           "render_production_analytics", "render_quality_control", "render_supply_chain", "render_workforce_ops"],
        "risk": ["render_compliance", "render_credit_risk", "render_cyber_risk",
                  "render_dashboard", "render_enterprise_risk", "render_market_risk", "render_operational_risk"],
    }
    modules = {
        "gaming": gaming,
        "telecom": telecom,
        "media": media,
        "financial_services": financial_services,
        "hls": hls,
        "manufacturing": manufacturing,
        "risk": risk,
    }
    for name, mod in modules.items():
        funcs = sorted([f for f in dir(mod) if f.startswith("render_")])
        assert len(funcs) == 7, f"{name}: should have 7 render functions, got {len(funcs)}: {funcs}"
        assert funcs == expected_funcs[name], f"{name}: unexpected functions {funcs}"


def test_all_pages_render():
    from app.data_access import get_config_for, set_active_vertical
    from app.pages import gaming, telecom, media, financial_services, hls
    from app.pages import manufacturing, risk
    modules = {
        "gaming": gaming,
        "telecom": telecom,
        "media": media,
        "financial_services": financial_services,
        "hls": hls,
        "manufacturing": manufacturing,
        "risk": risk,
    }
    for name, mod in modules.items():
        cfg = get_config_for(name)
        set_active_vertical(name)
        for fn_name in [f for f in dir(mod) if f.startswith("render_")]:
            fn = getattr(mod, fn_name)
            result = fn(cfg)
            assert result is not None, f"{name}.{fn_name} returned None"
