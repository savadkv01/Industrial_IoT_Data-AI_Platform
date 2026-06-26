"""Create the telemetry Kafka topic with an explicit partition count.

Partitioning by ``machine_id`` (the producer key) preserves per-machine event
ordering and lets the stream scale horizontally. Run this once before producing.

Usage::

    python -m streaming.producers.create_topic --partitions 6
"""

from __future__ import annotations

import argparse
import sys

from streaming.config import StreamingConfig


def create_topic(
    bootstrap_servers: str,
    topic: str,
    partitions: int,
    replication_factor: int = 1,
) -> str:
    """Create ``topic`` if it does not already exist. Returns a status string."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "confluent-kafka is required to create topics. "
            "Install it with: pip install confluent-kafka"
        ) from exc

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        return f"topic '{topic}' already exists ({len(existing[topic].partitions)} partitions)"

    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)
    futures = admin.create_topics([new_topic])
    futures[topic].result()  # raises on failure
    return f"created topic '{topic}' with {partitions} partitions"


def main(argv: list[str] | None = None) -> int:
    cfg = StreamingConfig()
    parser = argparse.ArgumentParser(description="Create the telemetry Kafka topic.")
    parser.add_argument("--bootstrap-servers", default=cfg.bootstrap_servers)
    parser.add_argument("--topic", default=cfg.telemetry_topic)
    parser.add_argument("--partitions", type=int, default=cfg.num_partitions)
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args(argv)

    status = create_topic(
        args.bootstrap_servers, args.topic, args.partitions, args.replication_factor
    )
    print(status, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
