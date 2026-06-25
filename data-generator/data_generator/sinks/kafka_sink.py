"""Kafka sink — produces records keyed by ``machine_id`` for partition ordering.

Uses ``confluent-kafka`` if available. Imported lazily by the sink factory so the
rest of the generator works without the dependency installed.
"""

from __future__ import annotations

from ..schema import TelemetryRecord
from .base import Sink


class KafkaSink(Sink):
    """Produce telemetry to a Kafka topic, partitioned by ``machine_id``."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "iot.telemetry",
        flush_every: int = 500,
    ) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:  # pragma: no cover - depends on optional install
            raise RuntimeError(
                "confluent-kafka is required for the Kafka sink. "
                "Install it with: pip install confluent-kafka"
            ) from exc

        self._topic = topic
        self._flush_every = flush_every
        self._pending = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 50,
                "compression.type": "snappy",
                "enable.idempotence": True,
            }
        )

    def write(self, record: TelemetryRecord) -> None:
        self._producer.produce(
            self._topic,
            key=record.machine_id.encode("utf-8"),
            value=record.to_json().encode("utf-8"),
        )
        self._pending += 1
        if self._pending >= self._flush_every:
            # poll(0) serves delivery callbacks without blocking.
            self._producer.poll(0)
            self._pending = 0

    def flush(self) -> None:
        self._producer.flush()
        self._pending = 0

    def close(self) -> None:
        self.flush()
