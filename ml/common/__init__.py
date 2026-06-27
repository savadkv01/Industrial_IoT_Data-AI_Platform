"""Shared ML utilities: data loading, time-aware splits, metrics, MLflow tracking."""

from __future__ import annotations

from ml.common.data import (
    LABEL_COLUMN,
    NON_FEATURE_COLUMNS,
    load_feature_frame,
    select_feature_columns,
)
from ml.common.metrics import classification_metrics, ranking_metrics, regression_metrics
from ml.common.splits import time_train_test_split, time_train_val_test_split
from ml.common.synthetic import make_synthetic_feature_frame
from ml.common.tracking import start_run

__all__ = [
    "LABEL_COLUMN",
    "NON_FEATURE_COLUMNS",
    "load_feature_frame",
    "select_feature_columns",
    "classification_metrics",
    "ranking_metrics",
    "regression_metrics",
    "time_train_test_split",
    "time_train_val_test_split",
    "make_synthetic_feature_frame",
    "start_run",
]
