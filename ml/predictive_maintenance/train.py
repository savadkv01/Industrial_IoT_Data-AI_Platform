"""Predictive maintenance training pipeline (Phase 6).

Trains an XGBoost classifier to predict ``label_failure_within_horizon`` from the
Phase 5 feature vectors. Uses a **time-aware** train/val/test split (no shuffling) so
the test metrics reflect future-scoring behaviour, handles class imbalance with
``scale_pos_weight``, and logs everything to MLflow.

Run:  ``python -m ml.predictive_maintenance.train``
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

import mlflow
import pandas as pd
from xgboost import XGBClassifier

from ml.common.data import LABEL_COLUMN, load_or_synthesize, select_feature_columns
from ml.common.metrics import classification_metrics
from ml.common.splits import time_train_val_test_split
from ml.common.tracking import start_run
from ml.config import MLConfig


@dataclass
class TrainResult:
    metrics: dict[str, float]
    feature_columns: list[str]
    model: XGBClassifier
    source: str
    cutoffs: dict[str, str]


def _scale_pos_weight(y: pd.Series) -> float:
    positives = float((y == 1).sum())
    negatives = float((y == 0).sum())
    if positives <= 0:
        return 1.0
    return max(1.0, negatives / positives)


def train(frame: pd.DataFrame | None = None, cfg: MLConfig | None = None) -> TrainResult:
    """Train and evaluate the predictive-maintenance classifier (no MLflow side effects)."""
    cfg = cfg or MLConfig()
    source = "provided"
    if frame is None:
        frame, source = load_or_synthesize(cfg.offline_features_path)

    if LABEL_COLUMN not in frame.columns:
        raise ValueError(f"label column '{LABEL_COLUMN}' missing from feature frame")

    # Supervised learning needs a known label; drop rows where it is null (real Gold
    # data can have unlabeled windows from the feature left-join).
    frame = frame.dropna(subset=[LABEL_COLUMN]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("no labeled rows available for training")

    feature_cols = select_feature_columns(frame, label_col=LABEL_COLUMN)
    train_df, val_df, test_df = time_train_val_test_split(
        frame,
        time_col=cfg.time_col,
        val_fraction=cfg.val_fraction,
        test_fraction=cfg.test_fraction,
    )

    y_train = train_df[LABEL_COLUMN].astype(bool).astype(int)
    y_val = val_df[LABEL_COLUMN].astype(bool).astype(int)
    y_test = test_df[LABEL_COLUMN].astype(bool).astype(int)

    model = XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        scale_pos_weight=_scale_pos_weight(y_train),
        eval_metric="aucpr",
        early_stopping_rounds=30,
        random_state=cfg.random_state,
        n_jobs=-1,
    )
    if len(val_df) > 0:
        model.fit(
            train_df[feature_cols],
            y_train,
            eval_set=[(val_df[feature_cols], y_val)],
            verbose=False,
        )
    else:
        # No validation slice (tiny dataset) — disable early stopping.
        model.set_params(early_stopping_rounds=None)
        model.fit(train_df[feature_cols], y_train, verbose=False)

    test_scores = (
        model.predict_proba(test_df[feature_cols])[:, 1]
        if len(test_df) > 0
        else []
    )
    metrics = classification_metrics(y_test, test_scores)
    metrics["n_train"] = float(len(train_df))
    metrics["n_val"] = float(len(val_df))
    metrics["n_test"] = float(len(test_df))

    cutoffs = {
        "train_end": str(train_df[cfg.time_col].max()),
        "val_end": str(val_df[cfg.time_col].max()),
        "test_end": str(test_df[cfg.time_col].max()),
    }
    return TrainResult(metrics, feature_cols, model, source, cutoffs)


def main() -> int:
    cfg = MLConfig()
    result = train(cfg=cfg)

    with start_run(cfg.experiment_predictive_maintenance, run_name="xgb-baseline", cfg=cfg):
        mlflow.log_params(
            {
                "model": "XGBClassifier",
                "n_estimators": 300,
                "max_depth": 5,
                "learning_rate": 0.05,
                "n_features": len(result.feature_columns),
                "feature_source": result.source,
                "split": "time_aware",
            }
        )
        mlflow.log_params({f"cutoff_{k}": v for k, v in result.cutoffs.items()})
        mlflow.log_metrics({k: v for k, v in result.metrics.items() if v == v})
        mlflow.xgboost.log_model(result.model, name="model")

    auc = result.metrics["roc_auc"]
    print(
        f"[pdm] feature_source={result.source}  "
        f"AUC={auc:.3f}  PR-AUC={result.metrics['pr_auc']:.3f}  "
        f"F1={result.metrics['f1']:.3f}  (gate AUC>={cfg.pdm_min_auc})",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
