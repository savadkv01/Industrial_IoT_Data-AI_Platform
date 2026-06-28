"""Data and model drift detection (Phase 10).

The :mod:`monitoring.drift.metrics` core is dependency-light (numpy only) so drift can be
computed anywhere — in unit tests, an Airflow task, or alongside Evidently. The richer
HTML report is produced by :mod:`monitoring.drift.evidently_reports` when Evidently is
installed.
"""

from __future__ import annotations

from monitoring.drift.metrics import (
    DriftReport,
    FeatureDrift,
    compute_drift,
    ks_statistic,
    population_stability_index,
)

__all__ = [
    "DriftReport",
    "FeatureDrift",
    "compute_drift",
    "ks_statistic",
    "population_stability_index",
]
