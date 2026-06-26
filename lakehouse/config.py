"""Runtime configuration for the lakehouse pipeline (Phase 4).

Reads from environment variables (and ``.env`` when present).
Defaults assume the job runs *inside* the Docker network; override via env when
running from the host.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

try:  # best-effort .env loading
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


@dataclass(frozen=True)
class LakehouseConfig:
    """Configuration snapshot for the Bronze → Silver → Gold batch jobs."""

    # MinIO / S3A
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket: str = os.getenv("S3_BUCKET_LAKEHOUSE", "lakehouse")

    # Data quality gate thresholds
    dq_max_null_rate: float = float(os.getenv("DQ_MAX_NULL_RATE", "0.05"))
    dq_max_range_violation_rate: float = float(os.getenv("DQ_MAX_RANGE_VIOLATION_RATE", "0.01"))

    # Spark tuning
    spark_shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))

    @property
    def bronze_path(self) -> str:
        return f"s3a://{self.bucket}/bronze/telemetry"

    @property
    def silver_path(self) -> str:
        return f"s3a://{self.bucket}/silver/telemetry"

    @property
    def silver_quarantine_path(self) -> str:
        return f"s3a://{self.bucket}/silver/_quarantine/telemetry"

    @property
    def gold_path(self) -> str:
        return f"s3a://{self.bucket}/gold/telemetry_features"
