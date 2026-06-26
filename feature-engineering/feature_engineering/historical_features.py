"""Point-in-time historical feature retrieval helpers."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from feature_engineering.feast_repo import get_feature_store


DEFAULT_FEATURES: tuple[str, ...] = (
    "telemetry_window_features:vibration_mean_5m",
    "telemetry_window_features:motor_temp_mean_5m",
    "telemetry_window_features:battery_soh_mean_5m",
    "telemetry_window_features:error_count_5m",
    "telemetry_temporal_features:vibration_mean_5m_lag_1",
    "telemetry_temporal_features:vibration_mean_5m_roc_1",
    "telemetry_temporal_features:uptime_ratio_5m",
)


def get_historical_features(
    entity_df: pd.DataFrame,
    features: Sequence[str] = DEFAULT_FEATURES,
) -> pd.DataFrame:
    store = get_feature_store()
    job = store.get_historical_features(
        entity_df=entity_df,
        features=list(features),
    )
    return job.to_df()