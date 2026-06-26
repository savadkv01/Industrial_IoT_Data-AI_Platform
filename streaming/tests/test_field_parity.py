"""Schema-parity tests: ensure the Spark ingest schema matches the producer contract.

These are intentionally lightweight (no running Spark cluster / no PySpark import)
so they run in CI and the project venv. They guard the most common streaming bug:
the consumer schema silently drifting from the producer payload, which would parse
fields as null and corrupt Bronze.
"""

from __future__ import annotations

from data_generator.schema import TelemetryRecord
from streaming.config import StreamingConfig
from streaming.fields import TELEMETRY_FIELDS, field_names


def test_spark_fields_match_producer_schema():
    """Every producer field is present in the streaming field spec, in order."""
    producer_fields = list(TelemetryRecord.model_fields.keys())
    assert field_names() == producer_fields, (
        "Streaming schema drifted from the producer TelemetryRecord. "
        "Update streaming/fields.py to match data_generator/schema.py."
    )


def test_no_duplicate_fields():
    names = field_names()
    assert len(names) == len(set(names))


def test_types_are_known():
    known = {"string", "double", "integer", "boolean", "timestamp"}
    for _, type_name in TELEMETRY_FIELDS:
        assert type_name in known, f"unknown spark type mapping: {type_name}"


def test_key_field_is_machine_id():
    """Partition/order key must be the first field for clarity and correctness."""
    assert field_names()[0] == "machine_id"


def test_bronze_paths_are_s3a():
    cfg = StreamingConfig(bucket="lakehouse")
    assert cfg.bronze_path == "s3a://lakehouse/bronze/telemetry"
    assert cfg.quarantine_path.startswith("s3a://lakehouse/")
    assert cfg.checkpoint_path.startswith("s3a://lakehouse/")


def test_config_defaults_are_sane():
    cfg = StreamingConfig()
    assert cfg.num_partitions >= 1
    assert cfg.max_offsets_per_trigger > 0
    assert "second" in cfg.trigger_interval or "minute" in cfg.trigger_interval
