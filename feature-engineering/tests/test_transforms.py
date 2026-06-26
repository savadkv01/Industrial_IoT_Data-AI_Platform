from __future__ import annotations

import pandas as pd
import pytest

from feature_engineering.transforms import build_feature_dataset, point_in_time_join


def sample_gold_frame() -> pd.DataFrame:
    rows = [
        {
            "machine_id": "m-1",
            "window_start": "2026-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:05:00Z",
            "window_duration": duration,
            "event_date": "2026-01-01",
            "vibration_mean": {"5m": 1.0, "1h": 10.0, "24h": 100.0}[duration],
            "vibration_std": 0.1,
            "vibration_max": 1.2,
            "motor_temp_mean": {"5m": 50.0, "1h": 55.0, "24h": 60.0}[duration],
            "motor_temp_std": 0.2,
            "motor_temp_max": 51.0,
            "cpu_usage_mean": {"5m": 20.0, "1h": 25.0, "24h": 30.0}[duration],
            "cpu_usage_std": 0.3,
            "battery_soh_mean": {"5m": 0.90, "1h": 0.91, "24h": 0.92}[duration],
            "battery_soh_min": 0.88,
            "error_count": {"5m": 0, "1h": 1, "24h": 2}[duration],
            "record_count": {"5m": 5, "1h": 60, "24h": 1440}[duration],
            "failure_label": False,
        }
        for duration in ("5m", "1h", "24h")
    ]
    rows.extend(
        {
            "machine_id": "m-1",
            "window_start": "2026-01-01T00:05:00Z",
            "window_end": "2026-01-01T00:10:00Z",
            "window_duration": duration,
            "event_date": "2026-01-01",
            "vibration_mean": {"5m": 2.0, "1h": 11.0, "24h": 101.0}[duration],
            "vibration_std": 0.1,
            "vibration_max": 2.2,
            "motor_temp_mean": {"5m": 52.0, "1h": 56.0, "24h": 61.0}[duration],
            "motor_temp_std": 0.2,
            "motor_temp_max": 53.0,
            "cpu_usage_mean": {"5m": 22.0, "1h": 26.0, "24h": 31.0}[duration],
            "cpu_usage_std": 0.3,
            "battery_soh_mean": {"5m": 0.89, "1h": 0.90, "24h": 0.91}[duration],
            "battery_soh_min": 0.87,
            "error_count": {"5m": 1, "1h": 2, "24h": 3}[duration],
            "record_count": {"5m": 5, "1h": 60, "24h": 1440}[duration],
            "failure_label": True if duration == "5m" else False,
        }
        for duration in ("5m", "1h", "24h")
    )
    return pd.DataFrame(rows)


def test_build_feature_dataset_preserves_window_metrics():
    features = build_feature_dataset(sample_gold_frame())
    assert list(features["machine_id"]) == ["m-1", "m-1"]
    assert features.loc[0, "vibration_mean_5m"] == 1.0
    assert features.loc[0, "vibration_mean_1h"] == 10.0
    assert features.loc[0, "vibration_mean_24h"] == 100.0
    assert features.loc[1, "motor_temp_mean_1h"] == 56.0
    assert features.loc[1, "battery_soh_mean_24h"] == 0.91


def test_build_feature_dataset_uses_window_end_as_event_timestamp():
    features = build_feature_dataset(sample_gold_frame())
    assert str(features.loc[0, "event_timestamp"]) == "2026-01-01 00:05:00+00:00"
    assert str(features.loc[1, "event_timestamp"]) == "2026-01-01 00:10:00+00:00"


def test_lag_features_align_with_previous_5m_row():
    features = build_feature_dataset(sample_gold_frame())
    assert pd.isna(features.loc[0, "vibration_mean_5m_lag_1"])
    assert features.loc[1, "vibration_mean_5m_lag_1"] == 1.0
    assert features.loc[1, "motor_temp_mean_5m_lag_1"] == 50.0


def test_rate_of_change_uses_lag_1_baseline():
    features = build_feature_dataset(sample_gold_frame())
    assert pd.isna(features.loc[0, "vibration_mean_5m_roc_1"])
    assert features.loc[1, "vibration_mean_5m_roc_1"] == 1.0
    assert features.loc[1, "battery_soh_mean_5m_roc_1"] == pytest.approx(-0.01)


def test_uptime_ratio_uses_error_count_and_record_count():
    features = build_feature_dataset(sample_gold_frame())
    assert features.loc[0, "uptime_ratio_5m"] == 1.0
    assert features.loc[1, "uptime_ratio_5m"] == 0.8


def test_label_is_anchored_to_5m_window():
    features = build_feature_dataset(sample_gold_frame())
    assert features.loc[0, "label_failure_within_horizon"] == False
    assert features.loc[1, "label_failure_within_horizon"] == True


def test_point_in_time_join_does_not_leak_future_features():
    features = build_feature_dataset(sample_gold_frame())
    entity_df = pd.DataFrame(
        {
            "machine_id": ["m-1", "m-1"],
            "event_timestamp": [
                pd.Timestamp("2026-01-01T00:07:30Z"),
                pd.Timestamp("2026-01-01T00:12:00Z"),
            ],
        }
    )
    joined = point_in_time_join(entity_df, features)
    assert list(joined["vibration_mean_5m"]) == [1.0, 2.0]
    assert list(joined["label_failure_within_horizon"]) == [False, True]