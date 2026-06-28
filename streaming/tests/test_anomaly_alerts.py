from __future__ import annotations

import pandas as pd

from streaming.spark.anomaly_alerts import (
    score_microbatch,
    select_scoring_features,
    throttle_alerts,
)


class StubAnomalyModel:
    def score_samples(self, features: pd.DataFrame) -> pd.Series:
        return -features["vibration"].to_numpy()


class StubModelWithFeatureNames:
    feature_names_in_ = ["vibration", "rolling_mean_vibration"]

    def score_samples(self, features: pd.DataFrame) -> pd.Series:
        return -features["vibration"].to_numpy()


def _batch() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "machine_id": ["m-1", "m-1", "m-2"],
            "ts": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:20Z",
                    "2026-01-01T00:00:10Z",
                ],
                utc=True,
            ),
            "vibration": [0.95, 0.70, 0.20],
            "speed": [12.0, 11.0, 8.0],
            "motor_temp": [90.0, 88.0, 70.0],
            "battery_soh": [0.85, 0.85, 0.92],
            "cpu_usage": [0.7, 0.68, 0.4],
            "error_code": [7, 7, 0],
            "event": ["fault", "fault", "nominal"],
            "failure_within_horizon": [False, False, False],
            "_topic": ["iot.telemetry"] * 3,
            "_partition": [0, 0, 1],
            "_offset": [1, 2, 3],
        }
    )


def test_score_microbatch_filters_alerts_and_adds_model_metadata():
    alerts = score_microbatch(
        _batch(),
        model=StubAnomalyModel(),
        model_name="iiot_anomaly_detection",
        model_alias="production",
        model_version="4",
        threshold=0.8,
    )

    assert list(alerts["machine_id"]) == ["m-1"]
    assert alerts.loc[0, "anomaly_score"] == 0.95
    assert alerts.loc[0, "model_alias"] == "production"
    assert alerts.loc[0, "model_version"] == "4"


def test_select_scoring_features_excludes_ingest_metadata():
    features = select_scoring_features(StubAnomalyModel(), _batch())

    assert "vibration" in features.columns
    for leaked in ("_partition", "_offset", "error_code", "machine_id", "event"):
        assert leaked not in features.columns


def test_select_scoring_features_prefers_model_feature_names():
    features = select_scoring_features(StubModelWithFeatureNames(), _batch())

    assert list(features.columns) == ["vibration", "rolling_mean_vibration"]
    # A feature the raw stream lacks is filled with NaN for the pipeline's imputer.
    assert features["rolling_mean_vibration"].isna().all()


def test_throttle_alerts_keeps_highest_score_per_machine_and_respects_cooldown():
    alerts = score_microbatch(
        _batch(),
        model=StubAnomalyModel(),
        model_name="iiot_anomaly_detection",
        model_alias="production",
        model_version="4",
        threshold=0.5,
    )
    prior = pd.DataFrame(
        {
            "machine_id": ["m-1"],
            "event_timestamp": pd.to_datetime(["2026-01-01T00:00:30Z"], utc=True),
        }
    )

    throttled = throttle_alerts(alerts, prior, "60 seconds")

    assert throttled.empty


def test_throttle_alerts_allows_new_machine_without_prior_alert():
    alerts = score_microbatch(
        _batch(),
        model=StubAnomalyModel(),
        model_name="iiot_anomaly_detection",
        model_alias="production",
        model_version="4",
        threshold=0.15,
    )

    throttled = throttle_alerts(alerts, pd.DataFrame(columns=["machine_id", "event_timestamp"]), "60 seconds")

    assert set(throttled["machine_id"]) == {"m-1", "m-2"}
    assert throttled.groupby("machine_id").size().to_dict() == {"m-1": 1, "m-2": 1}