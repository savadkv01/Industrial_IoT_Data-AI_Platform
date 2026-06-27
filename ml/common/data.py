"""Load the Phase 5 offline feature dataset into model-ready training frames."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

# The supervised predictive-maintenance label produced by the feature build.
LABEL_COLUMN = "label_failure_within_horizon"

# Columns that are identifiers / timestamps / metadata — never model inputs.
NON_FEATURE_COLUMNS: tuple[str, ...] = (
    "machine_id",
    "event_timestamp",
    "created_timestamp",
    "window_duration_anchor",
    LABEL_COLUMN,
)


def load_feature_frame(path: str | Path) -> pd.DataFrame:
    """Read the offline feature parquet and normalize the event-time column.

    The returned frame is sorted by ``(machine_id, event_timestamp)`` so downstream
    time-aware splits and lag-aware models see a consistent ordering.
    """
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(
            f"offline feature dataset not found: {source}. "
            "Run the Phase 5 build_offline_store first (or pass ML_OFFLINE_FEATURES)."
        )
    frame = pd.read_parquet(source)
    if "event_timestamp" in frame.columns:
        frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    sort_cols = [c for c in ("machine_id", "event_timestamp") if c in frame.columns]
    if sort_cols:
        frame = frame.sort_values(sort_cols).reset_index(drop=True)
    return frame


def select_feature_columns(
    frame: pd.DataFrame,
    label_col: str = LABEL_COLUMN,
    exclude: Sequence[str] = NON_FEATURE_COLUMNS,
) -> list[str]:
    """Return numeric feature columns, excluding identifiers, timestamps and the label.

    Boolean columns are excluded by default via the ``exclude`` set (the label is
    boolean); all remaining numeric dtypes are treated as model inputs.
    """
    excluded = set(exclude) | {label_col}
    feature_cols: list[str] = []
    for column in frame.columns:
        if column in excluded:
            continue
        if pd.api.types.is_numeric_dtype(frame[column]) and not pd.api.types.is_bool_dtype(
            frame[column]
        ):
            feature_cols.append(column)
    if not feature_cols:
        raise ValueError("no numeric feature columns found in the feature frame")
    return feature_cols


def load_or_synthesize(path: str | Path) -> tuple[pd.DataFrame, str]:
    """Load the offline feature parquet, or fall back to a synthetic frame.

    Returns ``(frame, source_tag)`` where ``source_tag`` is ``"offline_parquet"`` when
    the Phase 5 dataset exists, else ``"synthetic"``. The synthetic fallback lets the
    Phase 6 pipelines run end-to-end before a live lakehouse has produced features.
    """
    try:
        return load_feature_frame(path), "offline_parquet"
    except FileNotFoundError:
        from ml.common.synthetic import make_synthetic_feature_frame

        return make_synthetic_feature_frame(), "synthetic"
