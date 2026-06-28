"""Batch inference — score a feature frame with a registered model (Phase 7).

Loads a model by registry **alias** (default ``production``) and scores a feature
parquet, writing predictions to ``ml/predictions/<task>.parquet`` by default. This is the
scheduled, offline counterpart to the real-time FastAPI service (Phase 9); both read the
exact same registered model so batch and online scores stay consistent.

Run:
    python -m ml.inference.batch --task predictive_maintenance
    python -m ml.inference.batch --task battery_health --alias staging --output out.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

from ml.common.data import load_feature_frame
from ml.common.tasks import TASKS, TaskSpec, get_task
from ml.common.tracking import configure_tracking
from ml.config import MLConfig

ID_COLUMNS = ("machine_id", "event_timestamp")


def load_model(task: str, alias: str | None = None, cfg: MLConfig | None = None) -> Any:
    """Load the registered model for ``task`` behind ``alias`` (default: production).

    Resolves the alias to its backing run and loads via the ``runs:/`` URI rather than
    ``models:/<name>@<alias>``. The latter trips an MLflow Windows bug where the local
    artifact path (``c:\\...``) is misparsed as a URI scheme; the run artifact URI is a
    proper ``file://`` location, so it loads reliably on every platform.
    """
    cfg = configure_tracking(cfg)
    spec = get_task(task)
    alias = alias or cfg.production_alias
    client = MlflowClient(
        tracking_uri=cfg.mlflow_tracking_uri, registry_uri=cfg.mlflow_tracking_uri
    )
    version = client.get_model_version_by_alias(spec.registered_model(cfg), alias)
    # MLflow 3.x uses LoggedModel storage: source is `models:/m-<id>`.
    # Use source directly when available (works with both MinIO and file:// backends),
    # falling back to the run artifact URI for older registrations.
    source = getattr(version, "source", None)
    if source and source.startswith("models:/m-"):
        uri = source
    else:
        uri = f"runs:/{version.run_id}/model"
    if spec.flavor == "xgboost":
        return mlflow.xgboost.load_model(uri)
    return mlflow.sklearn.load_model(uri)


def score(spec: TaskSpec, model: Any, frame: pd.DataFrame) -> pd.Series:
    """Turn a loaded model + feature frame into the task's score Series."""
    features = frame[spec.select_features(frame)]
    if spec.score_kind == "proba":
        values = model.predict_proba(features)[:, 1]
    elif spec.score_kind == "anomaly":
        # Higher = more anomalous (negate the Isolation Forest log-likelihood).
        values = -model.score_samples(features)
    elif spec.score_kind == "regression":
        values = model.predict(features)
    else:  # pragma: no cover - guarded by TaskSpec construction
        raise ValueError(f"unknown score_kind '{spec.score_kind}'")
    return pd.Series(values, index=frame.index, name=spec.output_column)


def batch_score(
    task: str,
    frame: pd.DataFrame | None = None,
    *,
    alias: str | None = None,
    cfg: MLConfig | None = None,
) -> pd.DataFrame:
    """Score ``frame`` (or the configured offline parquet) and return id + prediction cols."""
    cfg = configure_tracking(cfg)
    spec = get_task(task)
    if frame is None:
        frame = load_feature_frame(cfg.offline_features_path)
    model = load_model(task, alias=alias, cfg=cfg)
    scores = score(spec, model, frame)

    id_cols = [c for c in ID_COLUMNS if c in frame.columns]
    out = frame[id_cols].copy()
    out[spec.output_column] = scores.to_numpy()
    out["model"] = spec.registered_model(cfg)
    out["alias"] = alias or cfg.production_alias
    return out


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch-score features with a registered model.")
    parser.add_argument("--task", required=True, choices=sorted(TASKS))
    parser.add_argument("--alias", default=None, help="Registry alias (default: production).")
    parser.add_argument("--input", default=None, help="Feature parquet (default: offline store).")
    parser.add_argument("--output", default=None, help="Output parquet path.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    cfg = MLConfig()
    frame = load_feature_frame(args.input) if args.input else None
    predictions = batch_score(args.task, frame, alias=args.alias, cfg=cfg)

    output = Path(args.output) if args.output else cfg.predictions_dir / f"{args.task}.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_parquet(output, index=False)

    print(
        f"[batch] task={args.task} rows={len(predictions)} -> {output}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
