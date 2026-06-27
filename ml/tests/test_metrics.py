"""Metric helper tests."""

from __future__ import annotations

import numpy as np

from ml.common.metrics import classification_metrics, ranking_metrics, regression_metrics


def test_classification_metrics_perfect_separation() -> None:
    y_true = [0, 0, 1, 1]
    y_score = [0.1, 0.2, 0.8, 0.9]
    m = classification_metrics(y_true, y_score)
    assert m["roc_auc"] == 1.0
    assert m["pr_auc"] == 1.0
    assert m["f1"] == 1.0


def test_classification_metrics_single_class_is_nan_not_crash() -> None:
    m = classification_metrics([0, 0, 0], [0.1, 0.2, 0.3])
    assert np.isnan(m["roc_auc"])
    assert np.isnan(m["pr_auc"])
    assert m["positive_rate"] == 0.0


def test_ranking_metrics_top_k_precision() -> None:
    # Highest scores are the true positives → precision@k should be perfect.
    y_true = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1]
    y_score = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.95, 0.99]
    m = ranking_metrics(y_true, y_score, k=0.2)
    assert m["n_flagged"] == 2.0
    assert m["precision_at_k"] == 1.0
    assert m["recall_at_k"] == 1.0


def test_regression_metrics_perfect_fit() -> None:
    y = [0.1, 0.5, 0.9]
    m = regression_metrics(y, y)
    assert m["rmse"] == 0.0
    assert m["mae"] == 0.0
    assert m["r2"] == 1.0
