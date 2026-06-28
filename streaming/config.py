"""Runtime configuration for the streaming pipeline.

Reads from environment variables (and ``.env`` when present). Defaults assume the
job runs *inside* the Docker network (hostnames ``kafka`` / ``minio``); override via
env when running from the host.
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
class StreamingConfig:
    """Configuration snapshot for the Kafka -> Bronze streaming job."""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    telemetry_topic: str = os.getenv("KAFKA_TELEMETRY_TOPIC", "iot.telemetry")
    num_partitions: int = int(os.getenv("KAFKA_NUM_PARTITIONS", "6"))

    # MinIO / S3A
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket: str = os.getenv("S3_BUCKET_LAKEHOUSE", "lakehouse")

    # Streaming behaviour
    starting_offsets: str = os.getenv("STREAM_STARTING_OFFSETS", "earliest")
    max_offsets_per_trigger: int = int(os.getenv("STREAM_MAX_OFFSETS_PER_TRIGGER", "50000"))
    trigger_interval: str = os.getenv("STREAM_TRIGGER_INTERVAL", "5 seconds")
    watermark_delay: str = os.getenv("STREAM_WATERMARK_DELAY", "2 minutes")

    # Phase 8 real-time anomaly detection
    alerts_topic: str = os.getenv("KAFKA_ALERTS_TOPIC", "iot.alerts")
    alerts_trigger_interval: str = os.getenv("ALERTS_TRIGGER_INTERVAL", "10 seconds")
    alerts_checkpoint_name: str = os.getenv(
        "ALERTS_CHECKPOINT_NAME", "realtime_anomaly_alerts"
    )
    alerts_cooldown: str = os.getenv("ALERTS_COOLDOWN", "60 seconds")
    anomaly_threshold: float = float(os.getenv("ANOMALY_ALERT_THRESHOLD", "0.5"))
    model_alias: str = os.getenv("ANOMALY_MODEL_ALIAS", "production")

    @property
    def bronze_path(self) -> str:
        return f"s3a://{self.bucket}/bronze/telemetry"

    @property
    def quarantine_path(self) -> str:
        return f"s3a://{self.bucket}/bronze/_quarantine/telemetry"

    @property
    def checkpoint_path(self) -> str:
        return f"s3a://{self.bucket}/_checkpoints/bronze_telemetry"

    @property
    def alerts_path(self) -> str:
        return f"s3a://{self.bucket}/silver/alerts/anomaly_alerts"

    @property
    def alerts_checkpoint_path(self) -> str:
        return f"s3a://{self.bucket}/_checkpoints/{self.alerts_checkpoint_name}"
