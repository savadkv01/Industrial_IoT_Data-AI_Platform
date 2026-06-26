"""Compare online and offline Feast values for a real entity row."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import pandas as pd

from feature_engineering.config import FeatureEngineeringConfig
from feature_engineering.historical_features import DEFAULT_FEATURES
from feature_engineering.feast_repo import get_feature_store


def main() -> int:
    cfg = FeatureEngineeringConfig()
    offline = pd.read_parquet(cfg.offline_file)
    offline["event_timestamp"] = pd.to_datetime(offline["event_timestamp"], utc=True)
    cutoff = datetime.now(timezone.utc)
    eligible = offline.loc[offline["event_timestamp"] <= cutoff].sort_values("event_timestamp")
    if eligible.empty:
        print("No eligible offline rows at or before current UTC time.", file=sys.stderr)
        return 1

    row = eligible.iloc[-1]

    entity_df = pd.DataFrame(
        {
            "machine_id": [row["machine_id"]],
            "event_timestamp": [row["event_timestamp"]],
        }
    )

    store = get_feature_store(cfg)
    historical = store.get_historical_features(
        entity_df=entity_df,
        features=list(DEFAULT_FEATURES),
    ).to_df()
    online = store.get_online_features(
        features=list(DEFAULT_FEATURES),
        entity_rows=[{"machine_id": row["machine_id"]}],
    ).to_dict()

    print(f"MACHINE {row['machine_id']}")
    print(f"EVENT_TS {row['event_timestamp']}")
    for feature_name in (
        "vibration_mean_5m",
        "motor_temp_mean_5m",
        "error_count_5m",
        "vibration_mean_5m_lag_1",
        "vibration_mean_5m_roc_1",
        "uptime_ratio_5m",
    ):
        hist_value = historical.loc[0, feature_name]
        online_value = online[feature_name][0]
        print(f"{feature_name} HIST={hist_value} ONLINE={online_value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())