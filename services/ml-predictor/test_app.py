"""Tests for ml-predictor's prediction logic and HTTP surface.

These run against the real model artifacts committed under
services/ml-predictor/models/ (the same ones the service loads at
startup) rather than a fabricated fixture, since that's the actual
contract this service depends on. Kafka/Redis are never touched — the
consumer loop (kafka_consumer_loop) is not started by importing app, and
Redis is a lazily-connecting client, so importing the module does no
network I/O.
"""

import os

# app.py defaults MODEL_DIR to /app/models (the container path). Point it
# at the real committed model artifacts next to this test file *before*
# importing app, since model loading happens at import time.
os.environ.setdefault("MODEL_DIR", os.path.join(os.path.dirname(__file__), "models"))

import app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(app.app)


def test_predict_xg_returns_probability_in_bounds():
    xg = app.predict_xg({"x": 88, "y": 45, "detail": "on_target"})
    assert xg is not None
    assert 0.0 <= xg <= 1.0


def test_predict_xg_uses_defaults_for_missing_fields():
    # No x/y/detail supplied at all — predict_xg documents defaults of
    # x=80, y=50, shot_type="foot" rather than raising.
    xg = app.predict_xg({})
    assert xg is not None
    assert 0.0 <= xg <= 1.0


def test_predict_xg_shot_type_variants_do_not_crash():
    for detail in ["on_target", "off_target", "header", "freekick", "penalty", "unknown_detail"]:
        xg = app.predict_xg({"x": 85, "y": 50, "detail": detail})
        assert xg is not None
        assert 0.0 <= xg <= 1.0


def test_predict_xg_returns_none_when_model_not_loaded():
    original = app.xg_model
    try:
        app.xg_model = None
        assert app.predict_xg({"x": 85, "y": 50}) is None
    finally:
        app.xg_model = original


def test_predict_win_probability_returns_all_three_outcomes_summing_to_one():
    stats = {
        "minute": 60,
        "home_goals": 1,
        "away_goals": 0,
        "home_shots": 8,
        "away_shots": 4,
        "home_shots_on_target": 3,
        "away_shots_on_target": 1,
        "home_corners": 4,
        "away_corners": 2,
        "home_fouls": 5,
        "away_fouls": 6,
        "home_yellow_cards": 1,
        "away_yellow_cards": 2,
        "home_red_cards": 0,
        "away_red_cards": 0,
    }
    result = app.predict_win_probability(stats)

    assert result is not None
    assert set(result.keys()) == {"home_win", "draw", "away_win"}
    for prob in result.values():
        assert 0.0 <= prob <= 1.0
    assert abs(sum(result.values()) - 1.0) < 0.01


def test_predict_win_probability_handles_missing_stats_fields():
    # An empty stats dict should fall back to the documented .get(..., 0)
    # defaults rather than raising a KeyError.
    result = app.predict_win_probability({})
    assert result is not None
    assert set(result.keys()) == {"home_win", "draw", "away_win"}


def test_predict_win_probability_returns_none_when_model_not_loaded():
    original = app.win_model
    try:
        app.win_model = None
        assert app.predict_win_probability({}) is None
    finally:
        app.win_model = original


def test_health_endpoint_reports_model_load_status():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "ml-predictor"
    assert "xg_model_loaded" in body
    assert "win_model_loaded" in body


def test_predict_xg_endpoint_returns_valid_response_shape():
    resp = client.get("/predict/xg", params={"x": 88, "y": 45, "shot_type": "on_target"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["x"] == 88
    assert body["y"] == 45
    assert 0.0 <= body["xg"] <= 1.0


def test_predict_xg_endpoint_uses_defaults_when_no_params_given():
    resp = client.get("/predict/xg")
    assert resp.status_code == 200
    body = resp.json()
    assert 0.0 <= body["xg"] <= 1.0


def test_metrics_endpoint_exposes_prometheus_format():
    resp = client.get("/metrics")
    assert resp.status_code == 200
    # Prometheus text exposition format: our own metric names should
    # appear once the app has been imported (Counters/Gauges are
    # registered at import time even before any prediction has run).
    assert "ml_predictor_predictions_total" in resp.text
    assert "ml_predictor_processing_errors_total" in resp.text
