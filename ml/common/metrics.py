"""Evaluation metrics for the platform's three baseline model families."""

from __future__ import annotations

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)


def _as_array(values) -> np.ndarray:
    return np.asarray(values).ravel()


def classification_metrics(
    y_true,
    y_score,
    threshold: float = 0.5,
) -> dict[str, float]:
    """Binary-classification metrics from predicted scores/probabilities.

    Returns AUC and PR-AUC (threshold-free) plus F1/precision/recall at ``threshold``.
    Robust to a single-class slice (AUC/PR-AUC fall back to NaN, not a crash).
    """
    y_true = _as_array(y_true).astype(int)
    y_score = _as_array(y_score).astype(float)
    y_pred = (y_score >= threshold).astype(int)

    if y_true.size == 0:
        return {
            "roc_auc": float("nan"),
            "pr_auc": float("nan"),
            "f1": float("nan"),
            "precision": float("nan"),
            "recall": float("nan"),
            "positive_rate": float("nan"),
        }

    single_class = len(np.unique(y_true)) < 2
    return {
        "roc_auc": float("nan") if single_class else float(roc_auc_score(y_true, y_score)),
        "pr_auc": float("nan")
        if single_class
        else float(average_precision_score(y_true, y_score)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "positive_rate": float(y_true.mean()),
    }


def ranking_metrics(y_true, y_score, k: float = 0.05) -> dict[str, float]:
    """Top-k anomaly metrics: flag the highest ``k`` fraction of scores as anomalies.

    Used for unsupervised models where ``y_true`` is a held-out label proxy
    (e.g. failure flag). ``k`` is the fraction of the population alerted on.
    """
    y_true = _as_array(y_true).astype(int)
    y_score = _as_array(y_score).astype(float)
    n = len(y_score)
    if n == 0:
        return {"precision_at_k": float("nan"), "recall_at_k": float("nan"), "k": float(k)}

    n_flagged = max(1, int(round(k * n)))
    top_idx = np.argsort(-y_score)[:n_flagged]
    flagged = np.zeros(n, dtype=int)
    flagged[top_idx] = 1

    true_positives = int(((flagged == 1) & (y_true == 1)).sum())
    total_positives = int((y_true == 1).sum())
    return {
        "precision_at_k": true_positives / n_flagged,
        "recall_at_k": (true_positives / total_positives) if total_positives else float("nan"),
        "k": float(k),
        "n_flagged": float(n_flagged),
    }


def regression_metrics(y_true, y_pred) -> dict[str, float]:
    """Regression metrics: RMSE, MAE, R²."""
    y_true = _as_array(y_true).astype(float)
    y_pred = _as_array(y_pred).astype(float)
    if y_true.size == 0:
        return {"rmse": float("nan"), "mae": float("nan"), "r2": float("nan")}
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    return {
        "rmse": rmse,
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)) if len(np.unique(y_true)) > 1 else float("nan"),
    }
