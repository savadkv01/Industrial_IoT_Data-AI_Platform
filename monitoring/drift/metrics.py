"""Pure-numpy drift metrics — the dependency-light core of Phase 10 drift detection.

Two complementary signals per numeric feature:

* **PSI** (Population Stability Index) — a binned divergence between the reference and
  current distributions. Common rule of thumb: ``<0.1`` stable, ``0.1–0.2`` moderate
  shift, ``>0.2`` significant drift.
* **KS** (Kolmogorov–Smirnov statistic) — the max gap between the two empirical CDFs in
  ``[0, 1]``; a distribution-free magnitude of shift.

The reference bin edges are reused for the current distribution so the comparison is
apples-to-apples. These functions feed both the Prometheus gauges and the Evidently
report, keeping a single source of truth for what "drift" means on this platform.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

# Floor applied to empty bins so the PSI logarithm stays finite.
_EPSILON = 1e-6


def _clean(values: "pd.Series | np.ndarray") -> np.ndarray:
    """Return a 1-D float array with NaN/inf removed."""
    arr = np.asarray(values, dtype="float64").ravel()
    return arr[np.isfinite(arr)]


def population_stability_index(
    reference: "pd.Series | np.ndarray",
    current: "pd.Series | np.ndarray",
    bins: int = 10,
) -> float:
    """Population Stability Index between a reference and current sample.

    Bin edges are quantiles of the reference so each reference bin holds a comparable
    mass. Returns ``0.0`` when either sample is empty (nothing to compare).
    """
    ref = _clean(reference)
    cur = _clean(current)
    if ref.size == 0 or cur.size == 0:
        return 0.0

    # Quantile edges on the reference; collapse to a single bin for constant features.
    quantiles = np.linspace(0.0, 1.0, bins + 1)
    edges = np.unique(np.quantile(ref, quantiles))
    if edges.size < 2:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf

    ref_counts, _ = np.histogram(ref, bins=edges)
    cur_counts, _ = np.histogram(cur, bins=edges)

    ref_pct = np.clip(ref_counts / ref.size, _EPSILON, None)
    cur_pct = np.clip(cur_counts / cur.size, _EPSILON, None)

    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def ks_statistic(
    reference: "pd.Series | np.ndarray",
    current: "pd.Series | np.ndarray",
) -> float:
    """Two-sample Kolmogorov–Smirnov statistic (max CDF gap) in ``[0, 1]``.

    A small self-contained implementation so the core has no scipy dependency.
    """
    ref = np.sort(_clean(reference))
    cur = np.sort(_clean(current))
    if ref.size == 0 or cur.size == 0:
        return 0.0

    grid = np.concatenate([ref, cur])
    cdf_ref = np.searchsorted(ref, grid, side="right") / ref.size
    cdf_cur = np.searchsorted(cur, grid, side="right") / cur.size
    return float(np.max(np.abs(cdf_ref - cdf_cur)))


@dataclass(frozen=True)
class FeatureDrift:
    """Drift verdict for a single numeric feature."""

    feature: str
    psi: float
    ks: float
    drifted: bool


@dataclass(frozen=True)
class DriftReport:
    """Dataset-level drift summary across all analysed features."""

    features: list[FeatureDrift] = field(default_factory=list)
    n_drifted: int = 0
    n_features: int = 0
    drift_share: float = 0.0
    dataset_drift: bool = False

    def to_dict(self) -> dict:
        """JSON-serialisable view, suitable for logging or an Airflow XCom."""
        return {
            "n_features": self.n_features,
            "n_drifted": self.n_drifted,
            "drift_share": self.drift_share,
            "dataset_drift": self.dataset_drift,
            "features": [
                {"feature": f.feature, "psi": f.psi, "ks": f.ks, "drifted": f.drifted}
                for f in self.features
            ],
        }


def numeric_feature_columns(
    frame: pd.DataFrame,
    exclude: "set[str] | None" = None,
) -> list[str]:
    """Return non-boolean numeric columns, excluding identifiers/timestamps."""
    skip = exclude or set()
    cols: list[str] = []
    for column in frame.columns:
        if column in skip:
            continue
        series = frame[column]
        if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
            cols.append(column)
    return cols


def compute_drift(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    *,
    columns: "list[str] | None" = None,
    exclude: "set[str] | None" = None,
    psi_threshold: float = 0.2,
    dataset_drift_share: float = 0.5,
    bins: int = 10,
) -> DriftReport:
    """Compute per-feature and dataset-level drift between two frames.

    A feature is flagged as drifted when its PSI exceeds ``psi_threshold``. The dataset is
    flagged when the share of drifted features exceeds ``dataset_drift_share``. Only
    columns present in *both* frames are analysed.
    """
    if columns is None:
        columns = numeric_feature_columns(reference, exclude=exclude)
    columns = [c for c in columns if c in reference.columns and c in current.columns]

    feature_results: list[FeatureDrift] = []
    for column in columns:
        psi = population_stability_index(reference[column], current[column], bins=bins)
        ks = ks_statistic(reference[column], current[column])
        feature_results.append(
            FeatureDrift(feature=column, psi=psi, ks=ks, drifted=psi > psi_threshold)
        )

    n_features = len(feature_results)
    n_drifted = sum(1 for f in feature_results if f.drifted)
    drift_share = (n_drifted / n_features) if n_features else 0.0
    return DriftReport(
        features=feature_results,
        n_drifted=n_drifted,
        n_features=n_features,
        drift_share=drift_share,
        dataset_drift=drift_share > dataset_drift_share,
    )
