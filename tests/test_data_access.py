"""Tests for data access layer - validates demo mode returns expected data."""

import os
import pytest

# Force demo mode for testing
os.environ["USE_DEMO_DATA"] = "true"
os.environ["USE_CASE"] = "manufacturing"


def test_get_production_kpis():
    from app.data_access import get_production_kpis
    kpis = get_production_kpis()
    assert kpis is not None
    assert "model_f1_score" in kpis
    assert kpis["model_f1_score"] == 0.947


def test_get_anomaly_scatter_data():
    from app.data_access import get_anomaly_scatter_data
    data = get_anomaly_scatter_data()
    assert isinstance(data, list)
    assert len(data) > 0
    first = data[0]
    assert "vibration_hz" in first
    assert "temp_c" in first
    assert "is_anomaly" in first or "is_anomalous" in first


def test_get_shap_importance():
    from app.data_access import get_shap_importance
    shap = get_shap_importance()
    assert shap is not None
    assert isinstance(shap, list)
    assert any(s.get("feature") == "vibration_hz" for s in shap)


def test_get_live_inference_feed():
    from app.data_access import get_live_inference_feed
    feed = get_live_inference_feed()
    assert isinstance(feed, list)
    assert len(feed) > 0


def test_get_inventory_status():
    from app.data_access import get_inventory_status
    inventory = get_inventory_status()
    assert isinstance(inventory, list)
    assert len(inventory) > 0
    # Should have Critical status item
    statuses = [item.get("stock_status", item.get("status", "")) for item in inventory]
    assert any(s in ("Critical", "critical") for s in statuses)


def test_get_quality_summary():
    from app.data_access import get_quality_summary
    quality = get_quality_summary()
    assert quality is not None


def test_get_build_tracking():
    from app.data_access import get_build_tracking
    tracking = get_build_tracking()
    assert isinstance(tracking, list)
    assert len(tracking) > 0
