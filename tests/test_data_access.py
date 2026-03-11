"""Tests for data access layer - validates config loading and demo mode."""

import os
import pytest

# Force demo mode for testing
os.environ["USE_DEMO_DATA"] = "true"
os.environ["USE_CASE"] = "gaming"


def test_get_config_for_all_verticals():
    from app.data_access import get_config_for
    verticals = ["gaming", "telecom", "media", "financial_services", "hls", "manufacturing", "risk"]
    for v in verticals:
        cfg = get_config_for(v)
        assert cfg is not None
        assert "app" in cfg
        assert "pages" in cfg
        assert "genie" in cfg


def test_set_active_vertical():
    from app.data_access import set_active_vertical, _active_vertical
    set_active_vertical("telecom")
    assert _active_vertical.get() == "telecom"
    set_active_vertical("gaming")
    assert _active_vertical.get() == "gaming"


def test_config_has_app_metadata():
    from app.data_access import get_config_for
    for v in ["gaming", "telecom", "media", "financial_services", "hls", "manufacturing", "risk"]:
        cfg = get_config_for(v)
        assert "name" in cfg["app"]
        assert "title" in cfg["app"]
        assert "catalog" in cfg["app"]


def test_config_has_ml_models():
    from app.data_access import get_config_for
    for v in ["gaming", "telecom", "media", "financial_services", "hls", "manufacturing", "risk"]:
        cfg = get_config_for(v)
        ml = cfg.get("ml", {})
        assert len(ml) >= 2, f"{v} should have at least 2 ML models"


def test_config_has_genie_questions():
    from app.data_access import get_config_for
    for v in ["gaming", "telecom", "media", "financial_services", "hls", "manufacturing", "risk"]:
        cfg = get_config_for(v)
        questions = cfg["genie"].get("sample_questions", [])
        assert len(questions) == 100, f"{v} should have 100 sample questions"


def test_config_has_six_pages():
    from app.data_access import get_config_for
    for v in ["gaming", "telecom", "media", "financial_services", "hls", "manufacturing", "risk"]:
        cfg = get_config_for(v)
        pages = cfg.get("pages", [])
        assert len(pages) == 7, f"{v} should have 7 pages, got {len(pages)}"


def test_is_demo_mode():
    from app.data_access import is_demo_mode
    assert is_demo_mode() is True
