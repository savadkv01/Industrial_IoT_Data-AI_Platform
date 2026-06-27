"""Time-aware split tests — the cardinal no-leakage guarantee for Phase 6."""

from __future__ import annotations

import pandas as pd

from ml.common.splits import time_train_test_split, time_train_val_test_split
from ml.common.synthetic import make_synthetic_feature_frame


def _frame() -> pd.DataFrame:
    return make_synthetic_feature_frame(n_machines=10, n_steps=40, seed=7)


def test_train_test_split_is_time_ordered() -> None:
    frame = _frame()
    train, test = time_train_test_split(frame, test_fraction=0.25)

    assert len(train) > 0 and len(test) > 0
    # No future leakage: every train timestamp precedes or equals every test timestamp.
    assert train["event_timestamp"].max() <= test["event_timestamp"].min()


def test_train_test_split_covers_all_rows_without_overlap() -> None:
    frame = _frame()
    train, test = time_train_test_split(frame, test_fraction=0.3)
    assert len(train) + len(test) == len(frame)


def test_three_way_split_is_monotonic_in_time() -> None:
    frame = _frame()
    train, val, test = time_train_val_test_split(
        frame, val_fraction=0.2, test_fraction=0.2
    )

    assert len(train) > 0 and len(val) > 0 and len(test) > 0
    assert train["event_timestamp"].max() <= val["event_timestamp"].min()
    assert val["event_timestamp"].max() <= test["event_timestamp"].min()
    assert len(train) + len(val) + len(test) == len(frame)


def test_test_slice_is_most_recent() -> None:
    frame = _frame()
    _, test = time_train_test_split(frame, test_fraction=0.2)
    assert test["event_timestamp"].max() == frame["event_timestamp"].max()
