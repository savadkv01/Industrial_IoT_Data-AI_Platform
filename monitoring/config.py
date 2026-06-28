"""Runtime configuration for Phase 10 monitoring.

Defaults keep drift reporting fully local: the *current* dataset is the Phase 5 offline
feature parquet and the *reference* dataset is a versioned snapshot under
``monitoring/drift/reference``. Override via environment variables (or ``.env``).
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


def _monitoring_root() -> Path:
    return Path(__file__).resolve().parent


def _repo_root() -> Path:
    return _monitoring_root().parent


def _default_offline_features() -> Path:
    return (
        _repo_root()
        / "feature-engineering"
        / "data"
        / "offline"
        / "telemetry_features.parquet"
    )


@dataclass(frozen=True)
class MonitoringConfig:
    """Configuration snapshot for drift reporting and metric export."""

    # ── Datasets ──
    # Current (live) features to inspect for drift.
    current_features_path: Path = Path(
        os.getenv("MONITORING_CURRENT_FEATURES", str(_default_offline_features()))
    )
    # Stable, versioned reference the current data is compared against. Captured once via
    # ``python -m monitoring.drift.evidently_reports snapshot``.
    reference_features_path: Path = Path(
        os.getenv(
            "MONITORING_REFERENCE_FEATURES",
            str(_monitoring_root() / "drift" / "reference" / "telemetry_features.parquet"),
        )
    )

    # ── Outputs ──
    reports_dir: Path = Path(
        os.getenv("MONITORING_REPORTS_DIR", str(_monitoring_root() / "drift" / "reports"))
    )

    # ── Identity columns to exclude from drift analysis ──
    time_col: str = os.getenv("MONITORING_TIME_COL", "event_timestamp")
    entity_col: str = os.getenv("MONITORING_ENTITY_COL", "machine_id")

    # ── Drift thresholds (mirrored as Prometheus gauges + Grafana alerts) ──
    # Per-feature drift flag fires when PSI exceeds this value.
    psi_threshold: float = float(os.getenv("MONITORING_PSI_THRESHOLD", "0.2"))
    # Dataset-level alert fires when the share of drifted features exceeds this fraction.
    dataset_drift_share: float = float(os.getenv("MONITORING_DATASET_DRIFT_SHARE", "0.5"))
    # Histogram bins used by the PSI/KS estimators.
    bins: int = int(os.getenv("MONITORING_DRIFT_BINS", "10"))

    # ── BI analytics export (Gold + predictions → Postgres for Grafana) ──
    # MinIO / S3 access for reading the Gold Delta table via DuckDB. Defaults favour a
    # host run; compose passes the in-network ``http://minio:9000`` to the Airflow worker.
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    lakehouse_bucket: str = os.getenv("S3_BUCKET_LAKEHOUSE", "lakehouse")
    gold_table: str = os.getenv("MONITORING_GOLD_TABLE", "gold/telemetry_features")
    # Batch-inference outputs written by ``ml.inference.batch`` (``<task>.parquet``).
    predictions_dir: Path = Path(
        os.getenv("MONITORING_PREDICTIONS_DIR", str(_repo_root() / "ml" / "predictions"))
    )
    # Only export the most recent N days of Gold to keep dashboards snappy (0 = all).
    bi_recent_days: int = int(os.getenv("MONITORING_BI_RECENT_DAYS", "0"))

    # Postgres analytics target (the existing platform metadata DB, ``analytics`` schema).
    analytics_pg_dsn: str = os.getenv("ANALYTICS_PG_DSN", "")
    pg_host: str = os.getenv("ANALYTICS_PG_HOST", "localhost")
    pg_port: int = int(os.getenv("ANALYTICS_PG_PORT", "5432"))
    pg_db: str = os.getenv("ANALYTICS_PG_DB", os.getenv("POSTGRES_DB", "metadata"))
    pg_user: str = os.getenv("ANALYTICS_PG_USER", os.getenv("POSTGRES_USER", "platform"))
    pg_password: str = os.getenv(
        "ANALYTICS_PG_PASSWORD", os.getenv("POSTGRES_PASSWORD", "platform")
    )
    analytics_schema: str = os.getenv("MONITORING_ANALYTICS_SCHEMA", "analytics")

    @property
    def monitoring_root(self) -> Path:
        return _monitoring_root()

    @property
    def gold_s3_url(self) -> str:
        """``s3://`` URL of the Gold Delta table for DuckDB ``delta_scan``."""
        return f"s3://{self.lakehouse_bucket}/{self.gold_table}"

    @property
    def postgres_url(self) -> str:
        """SQLAlchemy URL for the analytics Postgres target."""
        if self.analytics_pg_dsn:
            return self.analytics_pg_dsn
        return (
            f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        )


def get_config() -> MonitoringConfig:
    """Return a configuration snapshot built from the current environment."""
    return MonitoringConfig()
