"""Sink interface shared by all telemetry outputs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from ..schema import TelemetryRecord


class Sink(ABC):
    """Abstract telemetry sink.

    Implementations should accept records via :meth:`write`, optionally buffer, and
    release resources in :meth:`close`. Sinks support the context-manager protocol.
    """

    @abstractmethod
    def write(self, record: TelemetryRecord) -> None:
        """Emit a single telemetry record."""

    def write_many(self, records: Iterable[TelemetryRecord]) -> None:
        for record in records:
            self.write(record)

    def flush(self) -> None:  # pragma: no cover - optional override
        """Flush any buffered records. No-op by default."""

    def close(self) -> None:  # pragma: no cover - optional override
        """Release resources. No-op by default."""
        self.flush()

    def __enter__(self) -> "Sink":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
