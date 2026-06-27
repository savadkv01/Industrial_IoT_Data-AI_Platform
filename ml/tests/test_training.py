"""End-to-end training smoke tests on synthetic features (no MLflow side effects)."""

from __future__ import annotations

import math

from ml.common.data import LABEL_COLUMN, select_feature_columns
from ml.common.synthetic import make_synthetic_feature_frame
from ml.config import MLConfig
from ml.predictive_maintenance.train import train as train_pdm
from ml.anomaly_detection.train import train as train_anomaly
from ml.battery_health.train import train as train_battery


def _frame():
    return make_synthetic_feature_frame(n_machines=40, n_steps=60, seed=11)


def test_select_feature_columns_excludes_label_and_ids() -> None:
    frame = _frame()
    cols = select_feature_columns(frame)
    for forbidden in ("machine_id", "event_timestamp", "created_timestamp", LABEL_COLUMN):
        assert forbidden not in cols
    assert len(cols) > 10


def test_predictive_maintenance_learns_signal() -> None:
    result = train_pdm(frame=_frame(), cfg=MLConfig())
    assert set(["roc_auc", "pr_auc", "f1"]).issubset(result.metrics)
    # The synthetic label is driven by wear/vibration/temp → the model must beat chance.
    assert result.metrics["roc_auc"] > 0.6


def test_anomaly_detection_runs_and_scores() -> None:
    result = train_anomaly(frame=_frame(), cfg=MLConfig())
    assert "precision_at_k" in result.metrics
    assert not math.isnan(result.metrics["score_auc"])


def test_battery_health_excludes_soh_inputs_and_fits() -> None:
    result = train_battery(frame=_frame(), cfg=MLConfig())
    assert result.target == "battery_soh_mean_5m"
    # No battery_soh_* column may be used as an input (leakage guard).
    assert not any(c.startswith("battery_soh") for c in result.feature_columns)
    assert math.isfinite(result.metrics["rmse"])
