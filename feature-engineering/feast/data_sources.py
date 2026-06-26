from __future__ import annotations

from feast import FileSource

from feature_engineering.config import FeatureEngineeringConfig

def _offline_file_path() -> str:
    return str(FeatureEngineeringConfig().offline_file)


telemetry_feature_source = FileSource(
    name="telemetry_feature_source",
    path=_offline_file_path(),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)