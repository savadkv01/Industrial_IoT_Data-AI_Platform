"""Runtime configuration for the FastAPI serving service (Phase 9).

Reads from environment variables (and ``.env`` when present). The registry/model
identity lives in :class:`ml.config.MLConfig` so serving, training and batch inference
all resolve the exact same registered models; this config only adds serving-specific
knobs (which alias to serve, whether to warm-load at startup).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

try:  # best-effort .env loading
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class ServingConfig:
    """Serving-specific settings, layered on top of :class:`ml.config.MLConfig`."""

    # Registry alias to serve (mlflow 3.x replaced stages with aliases).
    model_alias: str = os.getenv("SERVING_MODEL_ALIAS", "production")
    # Warm-load every model at startup so the first request isn't penalised.
    warm_start: bool = _as_bool(os.getenv("SERVING_WARM_START"), True)
    # Value used to fill features absent from a request before scoring.
    missing_feature_fill: float = float(os.getenv("SERVING_MISSING_FEATURE_FILL", "0.0"))
