"""Schema parity tests — Silver/Gold contracts vs. streaming wire spec.

These tests are pure Python (no Spark) and verify that the lakehouse layer's
expectations about column names are consistent with the TELEMETRY_FIELDS
specification owned by the streaming package.
"""

from __future__ import annotations


from lakehouse.dq import SILVER_DQ_RULES
from lakehouse.gold import WINDOW_SPECS

# Import from the sibling streaming package (available via pythonpath = [".", ".."]
# in pyproject.toml).
from streaming.fields import TELEMETRY_FIELDS, field_names


# ---------------------------------------------------------------------------
# Silver ↔ streaming parity
# ---------------------------------------------------------------------------


def test_dq_rule_columns_exist_in_telemetry_fields():
    """Every DQ rule column must be a known telemetry field."""
    telemetry_cols = set(field_names())
    dq_cols = {r.column for r in SILVER_DQ_RULES}
    unknown = dq_cols - telemetry_cols
    assert not unknown, f"DQ rules reference columns absent from TELEMETRY_FIELDS: {unknown}"


def test_identity_fields_in_telemetry_fields():
    """machine_id and ts must be in the wire schema."""
    names = field_names()
    assert "machine_id" in names
    assert "ts" in names


def test_failure_label_field_present():
    """failure_within_horizon (the ML label) must be in the wire schema."""
    assert "failure_within_horizon" in field_names()


def test_telemetry_fields_types_are_known():
    """All type strings in TELEMETRY_FIELDS must map to a recognised Spark type."""
    known_types = {"string", "double", "integer", "boolean", "timestamp"}
    for name, type_name in TELEMETRY_FIELDS:
        assert type_name in known_types, (
            f"Unknown type '{type_name}' for field '{name}'"
        )


# ---------------------------------------------------------------------------
# Gold contract
# ---------------------------------------------------------------------------


def test_window_spec_labels_are_unique():
    labels = [label for _, label in WINDOW_SPECS]
    assert len(labels) == len(set(labels)), "duplicate window labels in WINDOW_SPECS"


def test_window_spec_contains_expected_durations():
    durations = {dur for dur, _ in WINDOW_SPECS}
    assert "5 minutes" in durations
    assert "1 hour" in durations
    assert "24 hours" in durations


def test_gold_aggregated_columns_are_from_known_fields():
    """Columns aggregated in Gold must be numeric fields in the wire schema."""
    gold_source_fields = {
        "vibration", "motor_temp", "cpu_usage", "battery_soh",
        "error_code", "failure_within_horizon",
    }
    telemetry_cols = set(field_names())
    unknown = gold_source_fields - telemetry_cols
    assert not unknown, f"Gold references fields not in wire schema: {unknown}"


def test_window_duration_labels_match_expected():
    """Short labels used as partition values must be the canonical set."""
    labels = {label for _, label in WINDOW_SPECS}
    assert labels == {"5m", "1h", "24h"}
