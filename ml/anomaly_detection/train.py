"""Anomaly detection training pipeline (Phase 6).

Fits an unsupervised Isolation Forest on the feature vectors (no labels used for
fitting). The failure label is used **only** for evaluation as a proxy for "abnormal",
via top-k precision/recall and the ranking AUC of the anomaly score. Fitting on the
earlier time window and scoring the later one keeps the evaluation leak-free.

Run:  ``python -m ml.anomaly_detection.train``
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

import mlflow
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ml.common.data import LABEL_COLUMN, load_or_synthesize, select_feature_columns
from ml.common.metrics import classification_metrics, ranking_metrics
from ml.common.splits import time_train_test_split
from ml.common.tracking import start_run
from ml.config import MLConfig


@dataclass
class TrainResult:
    metrics: dict[str, float]
    feature_columns: list[str]
    model: Pipeline
    source: str
    cutoffs: dict[str, str]


def _anomaly_scores(model: Pipeline, X: pd.DataFrame) -> pd.Series:
    """Higher score = more anomalous (negate ``score_samples``)."""
    raw = model.named_steps["iforest"].score_samples(
        model.named_steps["scaler"].transform(model.named_steps["imputer"].transform(X))
    )
    return pd.Series(-raw, index=X.index)


def train(frame: pd.DataFrame | None = None, cfg: MLConfig | None = None) -> TrainResult:
    """Fit Isolation Forest and evaluate on the most recent time slice."""
    cfg = cfg or MLConfig()
    source = "provided"
    if frame is None:
        frame, source = load_or_synthesize(cfg.offline_features_path)

    feature_cols = select_feature_columns(frame, label_col=LABEL_COLUMN)
    train_df, test_df = time_train_test_split(
        frame, time_col=cfg.time_col, test_fraction=cfg.test_fraction
    )

    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "iforest",
                IsolationForest(
                    n_estimators=200,
                    contamination="auto",
                    random_state=cfg.random_state,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    # Unsupervised fit — labels are never passed to the model.
    model.fit(train_df[feature_cols])

    scores = (
        _anomaly_scores(model, test_df[feature_cols])
        if len(test_df) > 0
        else pd.Series(dtype=float)
    )

    metrics: dict[str, float] = {"n_train": float(len(train_df)), "n_test": float(len(test_df))}
    if len(test_df) > 0 and LABEL_COLUMN in test_df.columns:
        label_mask = test_df[LABEL_COLUMN].notna().to_numpy()
        if label_mask.any():
            y_eval = test_df.loc[label_mask, LABEL_COLUMN].astype(bool).astype(int)
            s_eval = scores.to_numpy()[label_mask]
            metrics.update(ranking_metrics(y_eval, s_eval, k=0.05))
            # Threshold-free separation of the anomaly score vs the label proxy.
            metrics["score_auc"] = classification_metrics(y_eval, s_eval)["roc_auc"]

    cutoffs = {
        "train_end": str(train_df[cfg.time_col].max()),
        "test_end": str(test_df[cfg.time_col].max()),
    }
    return TrainResult(metrics, feature_cols, model, source, cutoffs)


def main() -> int:
    cfg = MLConfig()
    result = train(cfg=cfg)

    with start_run(cfg.experiment_anomaly_detection, run_name="iforest-baseline", cfg=cfg):
        mlflow.log_params(
            {
                "model": "IsolationForest",
                "n_estimators": 200,
                "contamination": "auto",
                "n_features": len(result.feature_columns),
                "feature_source": result.source,
                "split": "time_aware",
            }
        )
        mlflow.log_params({f"cutoff_{k}": v for k, v in result.cutoffs.items()})
        mlflow.log_metrics({k: v for k, v in result.metrics.items() if v == v})
        mlflow.sklearn.log_model(
            result.model, name="model", serialization_format="cloudpickle"
        )

    print(
        f"[anomaly] feature_source={result.source}  "
        f"precision@k={result.metrics.get('precision_at_k', float('nan')):.3f}  "
        f"score_auc={result.metrics.get('score_auc', float('nan')):.3f}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
