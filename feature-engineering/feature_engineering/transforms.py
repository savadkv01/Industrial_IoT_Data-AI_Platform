"""Pure-Python feature transforms from Gold windows to Feast-ready rows."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import timezone

import pandas as pd

WINDOW_ORDER: tuple[str, ...] = ("5m", "1h", "24h")

GOLD_REQUIRED_COLUMNS: tuple[str, ...] = (
    "machine_id",
    "window_start",
    "window_end",
    "window_duration",
    "event_date",
    "vibration_mean",
    "vibration_std",
    "vibration_max",
    "motor_temp_mean",
    "motor_temp_std",
    "motor_temp_max",
    "cpu_usage_mean",
    "cpu_usage_std",
    "battery_soh_mean",
    "battery_soh_min",
    "error_count",
    "record_count",
    "failure_label",
)

PIVOT_METRIC_COLUMNS: tuple[str, ...] = (
    "vibration_mean",
    "vibration_std",
    "vibration_max",
    "motor_temp_mean",
    "motor_temp_std",
    "motor_temp_max",
    "cpu_usage_mean",
    "cpu_usage_std",
    "battery_soh_mean",
    "battery_soh_min",
    "error_count",
    "record_count",
)

LAG_SOURCE_COLUMNS: tuple[str, ...] = (
    "vibration_mean_5m",
    "motor_temp_mean_5m",
    "battery_soh_mean_5m",
    "cpu_usage_mean_5m",
)


def _require_columns(df: pd.DataFrame, required: Sequence[str]) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz=timezone.utc)


def pivot_gold_to_wide(gold_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot Gold window rows into a single wide row per machine + event timestamp.

    ``event_timestamp`` is defined as the Gold ``window_end`` so Feast retrievals
    remain point-in-time correct when queried with ``<= event_timestamp``.
    """
    _require_columns(gold_df, GOLD_REQUIRED_COLUMNS)

    frame = gold_df.copy()
    frame["window_end"] = pd.to_datetime(frame["window_end"], utc=True)
    frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)

    label_frame = (
        frame.loc[frame["window_duration"] == "5m", ["machine_id", "window_end", "failure_label"]]
        .rename(columns={"window_end": "event_timestamp", "failure_label": "label_failure_within_horizon"})
        .drop_duplicates(subset=["machine_id", "event_timestamp"])
    )

    pivoted = (
        frame.set_index(["machine_id", "window_end", "window_duration"])[list(PIVOT_METRIC_COLUMNS)]
        .unstack("window_duration")
        .sort_index(axis=1)
    )
    pivoted.columns = [f"{metric}_{duration}" for metric, duration in pivoted.columns]
    wide = pivoted.reset_index().rename(columns={"window_end": "event_timestamp"})

    if "event_timestamp" not in wide.columns:
        raise ValueError("wide feature table is missing event_timestamp")

    wide = wide.merge(label_frame, on=["machine_id", "event_timestamp"], how="left")
    wide["window_duration_anchor"] = "5m"
    wide["created_timestamp"] = _utc_now()
    return wide.sort_values(["machine_id", "event_timestamp"]).reset_index(drop=True)


def add_lag_features(
    feature_df: pd.DataFrame,
    columns: Iterable[str] = LAG_SOURCE_COLUMNS,
    lags: Iterable[int] = (1, 3),
) -> pd.DataFrame:
    """Add lag-N features for the provided columns within each machine timeline."""
    _require_columns(feature_df, ["machine_id", "event_timestamp", *columns])

    frame = feature_df.sort_values(["machine_id", "event_timestamp"]).copy()
    grouped = frame.groupby("machine_id", sort=False)

    for column in columns:
        for lag in lags:
            frame[f"{column}_lag_{lag}"] = grouped[column].shift(lag)
    return frame


def add_rate_of_change_features(
    feature_df: pd.DataFrame,
    columns: Iterable[str] = LAG_SOURCE_COLUMNS,
) -> pd.DataFrame:
    """Add first-derivative features using the lag-1 value as the baseline."""
    frame = feature_df.copy()
    for column in columns:
        lag_column = f"{column}_lag_1"
        _require_columns(frame, [column, lag_column])
        frame[f"{column}_roc_1"] = frame[column] - frame[lag_column]
    return frame


def add_uptime_ratio(feature_df: pd.DataFrame) -> pd.DataFrame:
    """Add uptime ratio from 5m error and record counts."""
    _require_columns(feature_df, ["error_count_5m", "record_count_5m"])
    frame = feature_df.copy()
    ratio = 1.0 - (frame["error_count_5m"] / frame["record_count_5m"].replace(0, pd.NA))
    frame["uptime_ratio_5m"] = ratio.clip(lower=0.0, upper=1.0)
    return frame


def build_feature_dataset(gold_df: pd.DataFrame) -> pd.DataFrame:
    """Build the full Feast offline dataset from Gold window aggregates."""
    wide = pivot_gold_to_wide(gold_df)
    wide = add_lag_features(wide)
    wide = add_rate_of_change_features(wide)
    wide = add_uptime_ratio(wide)
    return wide.sort_values(["machine_id", "event_timestamp"]).reset_index(drop=True)


def point_in_time_join(
    entity_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    entity_timestamp_col: str = "event_timestamp",
) -> pd.DataFrame:
    """Return the latest feature row per entity with timestamp <= entity timestamp.

    This mirrors the critical correctness property expected from
    ``FeatureStore.get_historical_features``.
    """
    _require_columns(entity_df, ["machine_id", entity_timestamp_col])
    _require_columns(feature_df, ["machine_id", "event_timestamp"])

    left = entity_df.copy()
    right = feature_df.copy()
    left[entity_timestamp_col] = pd.to_datetime(left[entity_timestamp_col], utc=True)
    right["event_timestamp"] = pd.to_datetime(right["event_timestamp"], utc=True)

    left = left.sort_values(["machine_id", entity_timestamp_col]).reset_index(drop=True)
    right = right.sort_values(["machine_id", "event_timestamp"]).reset_index(drop=True)
    return pd.merge_asof(
        left,
        right,
        left_on=entity_timestamp_col,
        right_on="event_timestamp",
        by="machine_id",
        direction="backward",
        allow_exact_matches=True,
    )