from __future__ import annotations

import pandas as pd

from feature_engineering.transforms import build_feature_dataset, point_in_time_join
from tests.test_transforms import sample_gold_frame


def test_point_in_time_join_returns_latest_prior_feature_row():
    features = build_feature_dataset(sample_gold_frame())
    entity_df = pd.DataFrame(
        {
            "machine_id": ["m-1"],
            "event_timestamp": [pd.Timestamp("2026-01-01T00:09:00Z")],
        }
    )

    joined = point_in_time_join(entity_df, features)
    assert joined.loc[0, "vibration_mean_5m"] == 1.0
    assert joined.loc[0, "label_failure_within_horizon"] == False


def test_point_in_time_join_allows_exact_match_only_at_same_timestamp():
    features = build_feature_dataset(sample_gold_frame())
    entity_df = pd.DataFrame(
        {
            "machine_id": ["m-1"],
            "event_timestamp": [pd.Timestamp("2026-01-01T00:10:00Z")],
        }
    )

    joined = point_in_time_join(entity_df, features)
    assert joined.loc[0, "vibration_mean_5m"] == 2.0
    assert joined.loc[0, "label_failure_within_horizon"] == True