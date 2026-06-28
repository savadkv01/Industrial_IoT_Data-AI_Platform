"""Serving API tests (Phase 9).

Fakes the MLflow registry with in-memory estimators so the API surface, feature
alignment, traceability fields and metrics can be tested without a running MLflow
server. Model loading itself is exercised by the Phase 6/7 ML tests.
"""

from __future__ import annotations

import numpy as np
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import LoadedModel, ModelCache, ModelNotReady, score_records
from app.schemas import FeatureRecord
from ml.common.tasks import (
    ANOMALY_DETECTION,
    BATTERY_HEALTH,
    PREDICTIVE_MAINTENANCE,
)

FEATURES = ("motor_temp_mean", "vibration_mean", "cpu_load_mean")


class _FakeClassifier:
    feature_names_in_ = np.array(FEATURES)

    def predict_proba(self, frame):
        # Probability rises with motor temperature; second column is the positive class.
        p = (frame["motor_temp_mean"].to_numpy() / 100.0).clip(0, 1)
        return np.column_stack([1 - p, p])


class _FakeAnomaly:
    feature_names_in_ = np.array(FEATURES)

    def score_samples(self, frame):
        # Isolation-Forest-like: more negative = more anomalous.
        return -frame["vibration_mean"].to_numpy()


class _FakeRegressor:
    feature_names_in_ = np.array(FEATURES)

    def predict(self, frame):
        return 100.0 - frame["cpu_load_mean"].to_numpy()


class _FakeCache(ModelCache):
    """ModelCache that serves in-memory fakes instead of hitting the registry."""

    _FAKES = {
        PREDICTIVE_MAINTENANCE: _FakeClassifier(),
        ANOMALY_DETECTION: _FakeAnomaly(),
        BATTERY_HEALTH: _FakeRegressor(),
    }

    def get(self, task: str) -> LoadedModel:
        if task not in self._FAKES:
            raise ModelNotReady(f"no fake for '{task}'")
        return LoadedModel(
            model=self._FAKES[task], name=f"iiot_{task}", alias="production", version="7"
        )


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        test_client.app.state.cache = _FakeCache()
        yield test_client


def _payload() -> dict:
    return {
        "records": [
            {
                "machine_id": "machine-001",
                "event_timestamp": "2026-01-01T00:00:00Z",
                "features": {"motor_temp_mean": 90.0, "vibration_mean": 3.0, "cpu_load_mean": 40.0},
            }
        ]
    }


def test_health_reports_loaded_models(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    tasks = {m["task"] for m in body["models"]}
    assert tasks == {PREDICTIVE_MAINTENANCE, ANOMALY_DETECTION, BATTERY_HEALTH}
    assert all(m["loaded"] for m in body["models"])


def test_predict_maintenance_returns_probability_and_version(client: TestClient) -> None:
    resp = client.post("/predict/maintenance", json=_payload())
    assert resp.status_code == 200
    body = resp.json()
    assert body["task"] == PREDICTIVE_MAINTENANCE
    assert body["model_version"] == "7"
    assert body["output"] == "failure_probability"
    score = body["predictions"][0]["score"]
    assert 0.0 <= score <= 1.0
    assert body["predictions"][0]["machine_id"] == "machine-001"


def test_predict_battery_and_anomaly(client: TestClient) -> None:
    battery = client.post("/predict/battery", json=_payload())
    assert battery.status_code == 200
    assert battery.json()["output"] == "battery_soh_prediction"

    anomaly = client.post("/predict/anomaly", json=_payload())
    assert anomaly.status_code == 200
    assert anomaly.json()["output"] == "anomaly_score"


def test_empty_features_rejected_with_422(client: TestClient) -> None:
    bad = {"records": [{"machine_id": "m1", "features": {}}]}
    resp = client.post("/predict/maintenance", json=bad)
    assert resp.status_code == 422


def test_missing_features_filled_not_rejected(client: TestClient) -> None:
    # Only one of the three trained features supplied; the rest are filled, no crash.
    partial = {"records": [{"machine_id": "m1", "features": {"motor_temp_mean": 50.0}}]}
    resp = client.post("/predict/maintenance", json=partial)
    assert resp.status_code == 200


def test_model_not_ready_returns_503(client: TestClient) -> None:
    class _Down(ModelCache):
        def get(self, task: str) -> LoadedModel:
            raise ModelNotReady("registry down")

    client.app.state.cache = _Down()
    resp = client.post("/predict/maintenance", json=_payload())
    assert resp.status_code == 503


def test_metrics_endpoint_exposes_counters(client: TestClient) -> None:
    client.post("/predict/maintenance", json=_payload())
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "serving_requests_total" in resp.text


def test_score_records_aligns_to_trained_feature_space() -> None:
    cache = _FakeCache()
    records = [
        FeatureRecord(
            machine_id="m1",
            features={"vibration_mean": 5.0, "unused_extra": 999.0},
        )
    ]
    loaded, scores = score_records(cache, ANOMALY_DETECTION, records, fill=0.0)
    assert loaded.version == "7"
    # Extra feature dropped, missing ones filled. The fake returns -vibration_mean and
    # score_records negates it, so anomaly score = vibration_mean.
    assert scores[0] == pytest.approx(5.0)
