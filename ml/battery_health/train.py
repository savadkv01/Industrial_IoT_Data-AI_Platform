"""Battery health training pipeline (Phase 6).

Trains a gradient-boosted regressor to estimate battery **state of health**
(``battery_soh_mean_5m``) from operational signals (vibration, motor temperature,
CPU, error/usage counts). All ``battery_soh_*`` columns are excluded from the inputs
so the model learns the *relationship* between operating stress and battery wear rather
than trivially copying the target. Time-aware split, RMSE/MAE/R² metrics, MLflow logging.

Run:  ``python -m ml.battery_health.train``
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

import mlflow
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from ml.common.data import LABEL_COLUMN, load_or_synthesize, select_feature_columns
from ml.common.metrics import regression_metrics
from ml.common.splits import time_train_val_test_split
from ml.common.tracking import start_run
from ml.config import MLConfig

TARGET_COLUMN = "battery_soh_mean_5m"


@dataclass
class TrainResult:
    metrics: dict[str, float]
    feature_columns: list[str]
    target: str
    model: Pipeline
    source: str
    cutoffs: dict[str, str]


def train(frame: pd.DataFrame | None = None, cfg: MLConfig | None = None) -> TrainResult:
    """Train and evaluate the battery SoH regressor."""
    cfg = cfg or MLConfig()
    source = "provided"
    if frame is None:
        frame, source = load_or_synthesize(cfg.offline_features_path)

    if TARGET_COLUMN not in frame.columns:
        raise ValueError(f"target column '{TARGET_COLUMN}' missing from feature frame")

    # Regression needs a known target; drop rows where it is null.
    frame = frame.dropna(subset=[TARGET_COLUMN]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("no rows with a battery SoH target available for training")

    # Exclude every battery_soh_* column from inputs to avoid target leakage.
    battery_cols = [c for c in frame.columns if c.startswith("battery_soh")]
    feature_cols = [
        c
        for c in select_feature_columns(frame, label_col=LABEL_COLUMN)
        if c not in battery_cols
    ]

    train_df, val_df, test_df = time_train_val_test_split(
        frame,
        time_col=cfg.time_col,
        val_fraction=cfg.val_fraction,
        test_fraction=cfg.test_fraction,
    )

    model = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            (
                "gbr",
                GradientBoostingRegressor(
                    n_estimators=300,
                    max_depth=3,
                    learning_rate=0.05,
                    subsample=0.9,
                    random_state=cfg.random_state,
                ),
            ),
        ]
    )
    model.fit(train_df[feature_cols], train_df[TARGET_COLUMN])

    preds = model.predict(test_df[feature_cols]) if len(test_df) > 0 else []
    metrics = regression_metrics(test_df[TARGET_COLUMN] if len(test_df) > 0 else [], preds)
    metrics["n_train"] = float(len(train_df))
    metrics["n_val"] = float(len(val_df))
    metrics["n_test"] = float(len(test_df))

    cutoffs = {
        "train_end": str(train_df[cfg.time_col].max()),
        "test_end": str(test_df[cfg.time_col].max()),
    }
    return TrainResult(metrics, feature_cols, TARGET_COLUMN, model, source, cutoffs)


def main() -> int:
    cfg = MLConfig()
    result = train(cfg=cfg)

    with start_run(cfg.experiment_battery_health, run_name="gbr-baseline", cfg=cfg):
        mlflow.log_params(
            {
                "model": "GradientBoostingRegressor",
                "target": result.target,
                "n_estimators": 300,
                "max_depth": 3,
                "learning_rate": 0.05,
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
        f"[battery] feature_source={result.source}  "
        f"RMSE={result.metrics['rmse']:.4f}  MAE={result.metrics['mae']:.4f}  "
        f"R2={result.metrics['r2']:.3f}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
