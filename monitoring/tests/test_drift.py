"""Tests for the Phase 10 drift core and report pipeline."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from monitoring.config import MonitoringConfig
from monitoring.drift import evidently_reports
from monitoring.drift.metrics import (
    compute_drift,
    ks_statistic,
    population_stability_index,
)

RNG = np.random.default_rng(42)


def _normal(n: int, loc: float = 0.0, scale: float = 1.0) -> pd.Series:
    return pd.Series(RNG.normal(loc, scale, size=n))


def test_psi_near_zero_for_same_distribution():
    ref = _normal(5000)
    cur = _normal(5000)
    assert population_stability_index(ref, cur) < 0.1


def test_psi_large_for_shifted_distribution():
    ref = _normal(5000, loc=0.0)
    cur = _normal(5000, loc=3.0)
    assert population_stability_index(ref, cur) > 0.2


def test_psi_handles_empty_and_constant():
    assert population_stability_index(pd.Series([], dtype=float), _normal(10)) == 0.0
    constant = pd.Series([1.0] * 100)
    # Constant reference collapses to a single bin -> no measurable drift.
    assert population_stability_index(constant, constant) == 0.0


def test_ks_is_bounded_and_directional():
    same = ks_statistic(_normal(2000), _normal(2000))
    shifted = ks_statistic(_normal(2000, loc=0.0), _normal(2000, loc=4.0))
    assert 0.0 <= same <= 1.0
    assert shifted > same
    assert shifted <= 1.0


def test_compute_drift_flags_shifted_columns():
    n = 4000
    reference = pd.DataFrame(
        {"stable": _normal(n), "shifting": _normal(n, loc=0.0), "machine_id": ["m1"] * n}
    )
    current = pd.DataFrame(
        {"stable": _normal(n), "shifting": _normal(n, loc=3.0), "machine_id": ["m1"] * n}
    )
    report = compute_drift(reference, current, exclude={"machine_id"})

    by_name = {f.feature: f for f in report.features}
    assert set(by_name) == {"stable", "shifting"}
    assert by_name["shifting"].drifted is True
    assert by_name["stable"].drifted is False
    assert report.n_drifted == 1
    assert report.n_features == 2
    assert report.drift_share == pytest.approx(0.5)
    # share (0.5) does not exceed the default 0.5 threshold -> not a dataset-level drift.
    assert report.dataset_drift is False


def test_compute_drift_dataset_flag():
    n = 2000
    reference = pd.DataFrame({"a": _normal(n), "b": _normal(n)})
    current = pd.DataFrame({"a": _normal(n, loc=3.0), "b": _normal(n, loc=3.0)})
    report = compute_drift(reference, current)
    assert report.drift_share == pytest.approx(1.0)
    assert report.dataset_drift is True


def _write_parquet(frame: pd.DataFrame, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_generate_report_writes_summary(tmp_path):
    n = 1500
    reference = pd.DataFrame(
        {
            "machine_id": ["m1"] * n,
            "event_timestamp": pd.date_range("2024-01-01", periods=n, freq="min", tz="UTC"),
            "vibration": _normal(n, loc=0.0),
        }
    )
    current = reference.copy()
    current["vibration"] = _normal(n, loc=5.0)

    ref_path = tmp_path / "reference.parquet"
    cur_path = tmp_path / "current.parquet"
    _write_parquet(reference, ref_path)
    _write_parquet(current, cur_path)

    cfg = MonitoringConfig(
        current_features_path=cur_path,
        reference_features_path=ref_path,
        reports_dir=tmp_path / "reports",
    )

    report = evidently_reports.generate_report(cfg)

    assert report.dataset_drift is True
    summary = tmp_path / "reports" / evidently_reports.SUMMARY_FILENAME
    assert summary.exists()
    # Identity columns must be excluded from the drift features.
    feature_names = {f.feature for f in report.features}
    assert "machine_id" not in feature_names
    assert "event_timestamp" not in feature_names
    assert "vibration" in feature_names


def test_snapshot_reference_roundtrip(tmp_path):
    n = 200
    current = pd.DataFrame({"machine_id": ["m1"] * n, "vibration": _normal(n)})
    cur_path = tmp_path / "current.parquet"
    ref_path = tmp_path / "ref" / "reference.parquet"
    _write_parquet(current, cur_path)

    cfg = MonitoringConfig(current_features_path=cur_path, reference_features_path=ref_path)
    out = evidently_reports.snapshot_reference(cfg)

    assert out == ref_path
    assert ref_path.exists()
    pd.testing.assert_frame_equal(pd.read_parquet(ref_path), current)


def test_missing_dataset_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        evidently_reports.load_frame(tmp_path / "nope.parquet")
