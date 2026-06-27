"""Shared task specifications for the three model families (Phase 7).

Centralizes everything the MLOps layer needs to treat the three models uniformly:
the registered-model name, primary promotion metric and its direction, the MLflow
flavor used to load the artifact, how to turn a loaded model into scores, and how to
select the feature columns at inference time. Both :mod:`ml.pipeline` and
:mod:`ml.inference.batch` consume these specs so training and serving stay in sync.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from ml.common.data import LABEL_COLUMN, select_feature_columns
from ml.config import MLConfig

PREDICTIVE_MAINTENANCE = "predictive_maintenance"
ANOMALY_DETECTION = "anomaly_detection"
BATTERY_HEALTH = "battery_health"


def _pdm_features(frame: pd.DataFrame) -> list[str]:
    return select_feature_columns(frame, label_col=LABEL_COLUMN)


def _anomaly_features(frame: pd.DataFrame) -> list[str]:
    return select_feature_columns(frame, label_col=LABEL_COLUMN)


def _battery_features(frame: pd.DataFrame) -> list[str]:
    # Exclude every battery_soh_* column from inputs to avoid target leakage.
    battery_cols = [c for c in frame.columns if c.startswith("battery_soh")]
    return [
        c for c in select_feature_columns(frame, label_col=LABEL_COLUMN) if c not in battery_cols
    ]


@dataclass(frozen=True)
class TaskSpec:
    """Static description of one model family for the MLOps + serving layers."""

    key: str
    run_name: str
    # Primary metric used for promotion + the direction that counts as "better".
    primary_metric: str
    higher_is_better: bool
    # MLflow flavor used to load the artifact ("xgboost" or "sklearn").
    flavor: str
    # How a loaded model turns features into scores ("proba" | "anomaly" | "regression").
    score_kind: str
    # Output column name written by batch inference.
    output_column: str
    select_features: Callable[[pd.DataFrame], list[str]]

    def experiment(self, cfg: MLConfig) -> str:
        return getattr(cfg, f"experiment_{self.key}")

    def registered_model(self, cfg: MLConfig) -> str:
        return getattr(cfg, f"registered_{self.key}")

    def gate(self, cfg: MLConfig) -> float:
        return _GATES[self.key](cfg)


_GATES: dict[str, Callable[[MLConfig], float]] = {
    PREDICTIVE_MAINTENANCE: lambda cfg: cfg.pdm_min_auc,
    ANOMALY_DETECTION: lambda cfg: cfg.anomaly_min_score_auc,
    BATTERY_HEALTH: lambda cfg: cfg.battery_max_rmse,
}


TASKS: dict[str, TaskSpec] = {
    PREDICTIVE_MAINTENANCE: TaskSpec(
        key=PREDICTIVE_MAINTENANCE,
        run_name="xgb-baseline",
        primary_metric="roc_auc",
        higher_is_better=True,
        flavor="xgboost",
        score_kind="proba",
        output_column="failure_probability",
        select_features=_pdm_features,
    ),
    ANOMALY_DETECTION: TaskSpec(
        key=ANOMALY_DETECTION,
        run_name="iforest-baseline",
        primary_metric="score_auc",
        higher_is_better=True,
        flavor="sklearn",
        score_kind="anomaly",
        output_column="anomaly_score",
        select_features=_anomaly_features,
    ),
    BATTERY_HEALTH: TaskSpec(
        key=BATTERY_HEALTH,
        run_name="gbr-baseline",
        primary_metric="rmse",
        higher_is_better=False,
        flavor="sklearn",
        score_kind="regression",
        output_column="battery_soh_prediction",
        select_features=_battery_features,
    ),
}


def get_task(key: str) -> TaskSpec:
    if key not in TASKS:
        raise KeyError(f"unknown task '{key}'; expected one of {sorted(TASKS)}")
    return TASKS[key]
