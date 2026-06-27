"""Thin MLflow helper so every training entrypoint logs runs consistently.

Defaults to a local ``file:`` tracking store (see :class:`ml.config.MLConfig`), so
training works fully offline in Phase 6. Phase 7 sets ``MLFLOW_TRACKING_URI`` to the
MLflow server and reuses the exact same code path.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import mlflow

from ml.config import MLConfig


def configure_tracking(cfg: MLConfig | None = None) -> MLConfig:
    """Point MLflow's tracking + registry at the configured store and return the cfg."""
    cfg = cfg or MLConfig()
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_registry_uri(cfg.mlflow_tracking_uri)
    return cfg


@contextmanager
def start_run(
    experiment: str,
    run_name: str | None = None,
    cfg: MLConfig | None = None,
) -> Iterator[mlflow.ActiveRun]:
    """Context manager that configures tracking + experiment and opens a run."""
    cfg = configure_tracking(cfg)
    # Keep artifacts beside the local tracking DB when no server is configured.
    if mlflow.get_experiment_by_name(experiment) is None:
        mlflow.create_experiment(experiment, artifact_location=cfg.artifact_location)
    mlflow.set_experiment(experiment)
    with mlflow.start_run(run_name=run_name) as run:
        yield run


def log_model(model: Any, name: str = "model") -> None:
    """Log a model under the active run, dispatching to the right MLflow flavor.

    XGBoost estimators use the ``xgboost`` flavor; everything else (sklearn pipelines,
    estimators) uses ``sklearn`` with cloudpickle serialization (skops rejects some
    numpy dtypes used by the platform's pipelines).
    """
    try:
        from xgboost import XGBModel

        if isinstance(model, XGBModel):
            mlflow.xgboost.log_model(model, name=name)
            return
    except Exception:  # pragma: no cover - xgboost always present in this project
        pass
    mlflow.sklearn.log_model(model, name=name, serialization_format="cloudpickle")
