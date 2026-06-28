"""MLflow model loading, caching and scoring for the serving service (Phase 9).

Models are loaded from the MLflow registry **once** (warm start or first request) and
cached in memory keyed by task; subsequent requests reuse the in-memory artifact for
low latency. :meth:`ModelCache.reload` re-resolves the alias to pick up a newly promoted
version without restarting the service.

Scoring reuses the shared :mod:`ml.common.tasks` specs so online and batch/offline paths
score with identical model identity and semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

from ml.common.tasks import TASKS, TaskSpec, get_task
from ml.common.tracking import configure_tracking
from ml.config import MLConfig

from app.config import ServingConfig
from app.schemas import FeatureRecord


class ModelNotReady(RuntimeError):
    """Raised when a task's model cannot be resolved from the registry."""


@dataclass(frozen=True)
class LoadedModel:
    """A loaded registry model plus the identity needed for traceability."""

    model: Any
    name: str
    alias: str
    version: str


def _load_from_registry(spec: TaskSpec, alias: str, ml_cfg: MLConfig) -> LoadedModel:
    """Resolve ``alias`` to a concrete version and load the backing artifact.

    Loads via the version ``source`` (``models:/m-<id>`` for MLflow 3.x LoggedModels)
    when available, falling back to the run artifact URI. This mirrors
    :func:`ml.inference.batch.load_model` so online and batch loaders behave identically.
    """
    client = MlflowClient(
        tracking_uri=ml_cfg.mlflow_tracking_uri, registry_uri=ml_cfg.mlflow_tracking_uri
    )
    name = spec.registered_model(ml_cfg)
    version = client.get_model_version_by_alias(name, alias)
    source = getattr(version, "source", None)
    uri = source if source and source.startswith("models:/m-") else f"runs:/{version.run_id}/model"
    if spec.flavor == "xgboost":
        model = mlflow.xgboost.load_model(uri)
    else:
        model = mlflow.sklearn.load_model(uri)
    return LoadedModel(model=model, name=name, alias=alias, version=str(version.version))


class ModelCache:
    """In-memory cache of one loaded model per task."""

    def __init__(
        self,
        serving_cfg: ServingConfig | None = None,
        ml_cfg: MLConfig | None = None,
    ) -> None:
        self._serving_cfg = serving_cfg or ServingConfig()
        self._ml_cfg = configure_tracking(ml_cfg)
        self._cache: dict[str, LoadedModel] = {}

    @property
    def alias(self) -> str:
        return self._serving_cfg.model_alias

    @property
    def ml_cfg(self) -> MLConfig:
        return self._ml_cfg

    def get(self, task: str) -> LoadedModel:
        """Return the cached model for ``task``, loading it on first use."""
        if task not in self._cache:
            spec = get_task(task)
            try:
                self._cache[task] = _load_from_registry(spec, self.alias, self._ml_cfg)
            except Exception as exc:  # registry miss, no production alias, MinIO down…
                raise ModelNotReady(f"model for task '{task}' is not available: {exc}") from exc
        return self._cache[task]

    def reload(self, task: str | None = None) -> None:
        """Drop cached model(s) so the next request re-resolves the alias."""
        if task is None:
            self._cache.clear()
        else:
            self._cache.pop(task, None)

    def warm(self) -> dict[str, str | None]:
        """Best-effort load of every task; never raises. Returns task -> error (or None)."""
        errors: dict[str, str | None] = {}
        for task in TASKS:
            try:
                self.get(task)
                errors[task] = None
            except ModelNotReady as exc:
                errors[task] = str(exc)
        return errors


def _align_features(loaded: LoadedModel, frame: pd.DataFrame, fill: float) -> pd.DataFrame:
    """Reindex an incoming feature frame to the model's trained feature columns.

    Prefers the estimator's own ``feature_names_in_`` (set when it was fit on a
    DataFrame) so request features map to the exact trained space; features absent from
    the request are added and filled with ``fill``, and unknown extras are dropped. This
    keeps inputs in parity with training and prevents id/timestamp leakage.
    """
    names = getattr(loaded.model, "feature_names_in_", None)
    if names is not None:
        frame = frame.reindex(columns=list(names))
    return frame.fillna(fill)


def score_records(
    cache: ModelCache,
    task: str,
    records: list[FeatureRecord],
    *,
    fill: float = 0.0,
) -> tuple[LoadedModel, list[float]]:
    """Score a batch of feature records, returning the model and the per-record scores."""
    spec = get_task(task)
    loaded = cache.get(task)
    frame = pd.DataFrame([record.features for record in records])
    features = _align_features(loaded, frame, fill)

    if spec.score_kind == "proba":
        values = loaded.model.predict_proba(features)[:, 1]
    elif spec.score_kind == "anomaly":
        # Higher = more anomalous (negate the Isolation Forest log-likelihood).
        values = -loaded.model.score_samples(features)
    elif spec.score_kind == "regression":
        values = loaded.model.predict(features)
    else:  # pragma: no cover - guarded by TaskSpec construction
        raise ValueError(f"unknown score_kind '{spec.score_kind}'")

    return loaded, [float(v) for v in values]
