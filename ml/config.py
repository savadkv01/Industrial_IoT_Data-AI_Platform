"""Runtime configuration for ML training (Phases 6-7).

Reads from environment variables (and ``.env`` when present). Defaults keep
training fully local and offline: MLflow logs to a ``file:`` store under ``ml/mlruns``
and features are read from the Phase 5 offline parquet. Phase 7 overrides
``MLFLOW_TRACKING_URI`` to point at the MLflow server.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:  # best-effort .env loading
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


def _ml_root() -> Path:
    return Path(__file__).resolve().parent


def _default_offline_features() -> Path:
    """Default to the Phase 5 Feast offline dataset, resolved relative to the repo."""
    repo_root = _ml_root().parent
    return (
        repo_root
        / "feature-engineering"
        / "data"
        / "offline"
        / "telemetry_features.parquet"
    )


def _default_tracking_uri() -> str:
    """Local SQLite tracking store (mlflow 3.x deprecated the bare file store)."""
    return f"sqlite:///{(_ml_root() / 'mlflow.db').as_posix()}"


@dataclass(frozen=True)
class MLConfig:
    """Configuration snapshot for model training and MLflow tracking."""

    # ── Feature source ──
    offline_features_path: Path = Path(
        os.getenv("ML_OFFLINE_FEATURES", str(_default_offline_features()))
    )

    # ── MLflow ──
    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "") or _default_tracking_uri()
    # Artifact store root. Empty → ``ml/mlartifacts`` (resolved to a file URI lazily).
    mlflow_artifact_root: str = os.getenv("MLFLOW_ARTIFACT_ROOT", "")
    experiment_predictive_maintenance: str = os.getenv(
        "ML_EXPERIMENT_PDM", "predictive_maintenance"
    )
    experiment_anomaly_detection: str = os.getenv("ML_EXPERIMENT_ANOMALY", "anomaly_detection")
    experiment_battery_health: str = os.getenv("ML_EXPERIMENT_BATTERY", "battery_health")

    # ── Model registry (Phase 7) ──
    registered_predictive_maintenance: str = os.getenv(
        "ML_REGISTERED_PDM", "iiot_predictive_maintenance"
    )
    registered_anomaly_detection: str = os.getenv(
        "ML_REGISTERED_ANOMALY", "iiot_anomaly_detection"
    )
    registered_battery_health: str = os.getenv("ML_REGISTERED_BATTERY", "iiot_battery_health")
    # Registry aliases (mlflow 3.x replaced stages with aliases).
    production_alias: str = os.getenv("ML_PRODUCTION_ALIAS", "production")
    staging_alias: str = os.getenv("ML_STAGING_ALIAS", "staging")

    # ── Batch inference (Phase 7) ──
    predictions_dir: Path = Path(
        os.getenv("ML_PREDICTIONS_DIR", str(_ml_root() / "predictions"))
    )

    # ── Time-aware split ──
    # Fraction of the timeline (by event time) reserved for the most recent test slice.
    test_fraction: float = float(os.getenv("ML_TEST_FRACTION", "0.2"))
    val_fraction: float = float(os.getenv("ML_VAL_FRACTION", "0.2"))
    time_col: str = os.getenv("ML_TIME_COL", "event_timestamp")
    entity_col: str = os.getenv("ML_ENTITY_COL", "machine_id")

    # ── Reproducibility ──
    random_state: int = int(os.getenv("ML_RANDOM_STATE", "42"))

    # ── Promotion gates (Phase 7) ──
    # A candidate is promoted to ``production_alias`` only if it clears the absolute gate
    # *and* beats the incumbent on the task's primary metric.
    pdm_min_auc: float = float(os.getenv("ML_PDM_MIN_AUC", "0.85"))
    anomaly_min_score_auc: float = float(os.getenv("ML_ANOMALY_MIN_SCORE_AUC", "0.60"))
    battery_max_rmse: float = float(os.getenv("ML_BATTERY_MAX_RMSE", "inf"))

    @property
    def ml_root(self) -> Path:
        return _ml_root()

    @property
    def artifact_location(self) -> str:
        """File URI for the experiment artifact store (created on first run)."""
        root = self.mlflow_artifact_root or str(self.ml_root / "mlartifacts")
        return Path(root).resolve().as_uri()
