"""File sink — appends newline-delimited JSON to a local file.

Intended for offline testing and batch backfills. Parquet/MinIO output is added in
later phases; NDJSON keeps Phase 2 dependency-free and easy to inspect.
"""

from __future__ import annotations

from pathlib import Path

from ..schema import TelemetryRecord
from .base import Sink


class FileSink(Sink):
    """Buffer records and append them as JSON lines to ``path``."""

    def __init__(self, path: str = "data/telemetry.ndjson", buffer_size: int = 1000) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer: list[str] = []
        self._buffer_size = buffer_size
        # Truncate on open so each run starts fresh.
        self._file = self._path.open("w", encoding="utf-8")

    def write(self, record: TelemetryRecord) -> None:
        self._buffer.append(record.to_json())
        if len(self._buffer) >= self._buffer_size:
            self.flush()

    def flush(self) -> None:
        if self._buffer:
            self._file.write("\n".join(self._buffer) + "\n")
            self._file.flush()
            self._buffer.clear()

    def close(self) -> None:
        self.flush()
        self._file.close()
