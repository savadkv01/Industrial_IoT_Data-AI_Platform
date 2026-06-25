"""Telemetry sinks: pluggable outputs for generated records.

Each sink implements :class:`Sink`. Sinks must not block the generation loop for
long; the Kafka sink buffers and flushes asynchronously.
"""

from __future__ import annotations

from .base import Sink
from .file_sink import FileSink
from .stdout_sink import StdoutSink

__all__ = ["Sink", "FileSink", "StdoutSink", "build_sink"]


def build_sink(kind: str, **kwargs) -> Sink:
    """Factory mapping a CLI ``--sink`` choice to a concrete sink instance."""
    kind = kind.lower()
    if kind == "stdout":
        return StdoutSink()
    if kind == "file":
        return FileSink(**kwargs)
    if kind == "kafka":
        # Imported lazily so the package works without confluent-kafka installed.
        from .kafka_sink import KafkaSink

        return KafkaSink(**kwargs)
    raise ValueError(f"Unknown sink kind: {kind!r}. Expected one of: stdout, file, kafka.")
