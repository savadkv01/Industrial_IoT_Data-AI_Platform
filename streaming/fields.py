"""Telemetry field specification — the single source of truth for the wire schema.

This module is intentionally free of any PySpark / pandas imports so it can be
imported by lightweight tests to assert that the Spark schema stays in parity with
the Pydantic ``TelemetryRecord`` produced by the data generator (Phase 2).

Each entry maps a field name to a Spark SQL type name (built into a real
``StructType`` in :mod:`streaming.spark.schema`).
"""

from __future__ import annotations

# Order mirrors data_generator.schema.TelemetryRecord.
TELEMETRY_FIELDS: tuple[tuple[str, str], ...] = (
    ("machine_id", "string"),
    ("ts", "timestamp"),
    ("lat", "double"),
    ("lon", "double"),
    ("speed", "double"),
    ("accel_x", "double"),
    ("accel_y", "double"),
    ("accel_z", "double"),
    ("vibration", "double"),
    ("battery_soh", "double"),
    ("motor_temp", "double"),
    ("cpu_usage", "double"),
    ("error_code", "integer"),
    ("event", "string"),
    ("failure_within_horizon", "boolean"),
)

# Ingestion metadata added by the streaming job (not part of the wire payload).
INGEST_METADATA_COLUMNS: tuple[str, ...] = (
    "_ingest_ts",
    "_topic",
    "_partition",
    "_offset",
    "event_date",
)


def field_names() -> list[str]:
    """Return the ordered telemetry field names."""
    return [name for name, _ in TELEMETRY_FIELDS]
