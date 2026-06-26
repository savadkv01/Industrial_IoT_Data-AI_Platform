"""Unit tests for DQ rule definitions and DQResult (no Spark required).

These run fast without a SparkSession since all tested code is pure Python.
"""

from __future__ import annotations

import pytest

from lakehouse.dq import DQResult, SILVER_DQ_RULES


# ---------------------------------------------------------------------------
# Rule coverage
# ---------------------------------------------------------------------------


def test_identity_columns_are_non_nullable():
    non_nullable = {r.column for r in SILVER_DQ_RULES if not r.nullable}
    assert "machine_id" in non_nullable
    assert "ts" in non_nullable


def test_location_columns_are_non_nullable():
    non_nullable = {r.column for r in SILVER_DQ_RULES if not r.nullable}
    assert "lat" in non_nullable
    assert "lon" in non_nullable


def test_battery_soh_bounds():
    rule = next(r for r in SILVER_DQ_RULES if r.column == "battery_soh")
    assert rule.min_value == pytest.approx(0.0)
    assert rule.max_value == pytest.approx(1.0)


def test_cpu_usage_bounds():
    rule = next(r for r in SILVER_DQ_RULES if r.column == "cpu_usage")
    assert rule.min_value == pytest.approx(0.0)
    assert rule.max_value == pytest.approx(100.0)


def test_lat_bounds():
    rule = next(r for r in SILVER_DQ_RULES if r.column == "lat")
    assert rule.min_value == pytest.approx(-90.0)
    assert rule.max_value == pytest.approx(90.0)


def test_lon_bounds():
    rule = next(r for r in SILVER_DQ_RULES if r.column == "lon")
    assert rule.min_value == pytest.approx(-180.0)
    assert rule.max_value == pytest.approx(180.0)


def test_speed_non_negative():
    rule = next(r for r in SILVER_DQ_RULES if r.column == "speed")
    assert rule.min_value == pytest.approx(0.0)
    assert rule.max_value is None


def test_vibration_non_negative():
    rule = next(r for r in SILVER_DQ_RULES if r.column == "vibration")
    assert rule.min_value == pytest.approx(0.0)
    assert rule.max_value is None


def test_no_duplicate_columns_in_rules():
    columns = [r.column for r in SILVER_DQ_RULES]
    assert len(columns) == len(set(columns)), "duplicate columns in SILVER_DQ_RULES"


# ---------------------------------------------------------------------------
# DQResult helpers
# ---------------------------------------------------------------------------


def test_dq_result_quarantine_rate_normal():
    result = DQResult(
        total_rows=100,
        valid_rows=95,
        quarantine_rows=5,
        null_violations={},
        range_violations={},
        passed=True,
    )
    assert result.quarantine_rate == pytest.approx(0.05)


def test_dq_result_quarantine_rate_zero_total():
    result = DQResult(
        total_rows=0,
        valid_rows=0,
        quarantine_rows=0,
        null_violations={},
        range_violations={},
        passed=True,
    )
    assert result.quarantine_rate == 0.0


def test_dq_result_passed_flag():
    passing = DQResult(
        total_rows=1000,
        valid_rows=999,
        quarantine_rows=1,
        null_violations={},
        range_violations={},
        passed=True,
    )
    failing = DQResult(
        total_rows=100,
        valid_rows=80,
        quarantine_rows=20,
        null_violations={"machine_id": 0.20},
        range_violations={},
        passed=False,
    )
    assert passing.passed is True
    assert failing.passed is False


def test_dq_result_print_report_runs(capsys):
    result = DQResult(
        total_rows=50,
        valid_rows=48,
        quarantine_rows=2,
        null_violations={"ts": 0.04},
        range_violations={"battery_soh": 1},
        passed=True,
    )
    import sys

    result.print_report(file=sys.stdout)
    captured = capsys.readouterr()
    assert "DQ Report" in captured.out
    assert "ts" in captured.out
    assert "battery_soh" in captured.out
