"""Time-aware train/validation/test splits.

The cardinal rule for this platform: **never shuffle across time**. Models are
validated on data that is strictly *more recent* than what they trained on, so the
evaluation reflects how the model will behave when scoring future telemetry. A random
split would leak future information (e.g. a machine's later windows) into training and
inflate metrics.
"""

from __future__ import annotations

import pandas as pd


def _time_cutoff(frame: pd.DataFrame, time_col: str, tail_fraction: float) -> pd.Timestamp:
    """Return the timestamp quantile that reserves ``tail_fraction`` of the timeline."""
    if not 0.0 < tail_fraction < 1.0:
        raise ValueError(f"tail_fraction must be in (0, 1); got {tail_fraction}")
    times = pd.to_datetime(frame[time_col], utc=True)
    return times.quantile(1.0 - tail_fraction)


def time_train_test_split(
    frame: pd.DataFrame,
    time_col: str = "event_timestamp",
    test_fraction: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into (train, test) by event time — test is the most recent slice.

    Rows whose ``time_col`` is <= the cutoff go to train; later rows go to test.
    The split is deterministic and contains no overlap in time.
    """
    if time_col not in frame.columns:
        raise ValueError(f"time column '{time_col}' not in frame")
    cutoff = _time_cutoff(frame, time_col, test_fraction)
    times = pd.to_datetime(frame[time_col], utc=True)
    train = frame[times <= cutoff].reset_index(drop=True)
    test = frame[times > cutoff].reset_index(drop=True)
    return train, test


def time_train_val_test_split(
    frame: pd.DataFrame,
    time_col: str = "event_timestamp",
    val_fraction: float = 0.2,
    test_fraction: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split into (train, val, test) by event time, oldest → newest.

    ``test`` is the most recent ``test_fraction`` of the timeline; ``val`` is the
    ``val_fraction`` immediately before it; ``train`` is everything older.
    """
    train_val, test = time_train_test_split(frame, time_col, test_fraction)
    # val_fraction is expressed relative to the whole timeline; rescale to the remainder.
    remaining = 1.0 - test_fraction
    relative_val = val_fraction / remaining if remaining > 0 else 0.0
    train, val = time_train_test_split(train_val, time_col, relative_val)
    return train, val, test
