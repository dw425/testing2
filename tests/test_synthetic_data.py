"""Tests for synthetic data generator - validates config loading and data shapes."""

import os
import pytest
import yaml


def test_manufacturing_config_loads():
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "manufacturing.yaml")
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    assert cfg["app"]["name"] == "ManufacturingIQ"
    assert cfg["app"]["catalog"] == "manufacturing_iq"


def test_config_has_required_sections():
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "manufacturing.yaml")
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    required = ["app", "brand", "data", "ml", "pages", "genie", "dashboard"]
    for section in required:
        assert section in cfg, f"Missing config section: {section}"


def test_all_verticals_load():
    config_dir = os.path.join(os.path.dirname(__file__), "..", "config")
    verticals = ["manufacturing", "risk", "healthcare", "gaming", "financial_services"]
    for vertical in verticals:
        path = os.path.join(config_dir, f"{vertical}.yaml")
        assert os.path.exists(path), f"Missing config: {vertical}.yaml"
        with open(path) as f:
            cfg = yaml.safe_load(f)
        assert "app" in cfg
        assert "pages" in cfg
        assert len(cfg["pages"]) >= 4


def test_data_sites_defined():
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "manufacturing.yaml")
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    sites = cfg["data"]["sites"]
    assert len(sites) == 3
    assert "Berlin" in sites
    assert "Detroit" in sites
    assert "Tokyo" in sites


def test_pages_have_required_fields():
    config_path = os.path.join(os.path.dirname(__file__), "..", "config", "manufacturing.yaml")
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    for page in cfg["pages"]:
        assert "id" in page
        assert "label" in page
        assert "icon" in page
        assert "enabled" in page
