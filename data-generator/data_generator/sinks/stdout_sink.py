"""Stdout sink — prints newline-delimited JSON. Useful for local smoke tests."""

from __future__ import annotations

import sys

from ..schema import TelemetryRecord
from .base import Sink


class StdoutSink(Sink):
    """Write each record as a JSON line to stdout."""

    def __init__(self, stream=sys.stdout) -> None:
        self._stream = stream

    def write(self, record: TelemetryRecord) -> None:
        self._stream.write(record.to_json() + "\n")

    def flush(self) -> None:
        self._stream.flush()
