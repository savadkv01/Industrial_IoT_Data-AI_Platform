from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Bool, Float32, Float64, Int64, String

from data_sources import telemetry_feature_source
from entities import machine


telemetry_window_features = FeatureView(
    name="telemetry_window_features",
    entities=[machine],
    ttl=timedelta(days=1),
    schema=[
        Field(name="window_duration_anchor", dtype=String),
        Field(name="vibration_mean_5m", dtype=Float64),
        Field(name="vibration_mean_1h", dtype=Float64),
        Field(name="vibration_mean_24h", dtype=Float64),
        Field(name="motor_temp_mean_5m", dtype=Float64),
        Field(name="motor_temp_mean_1h", dtype=Float64),
        Field(name="motor_temp_mean_24h", dtype=Float64),
        Field(name="cpu_usage_mean_5m", dtype=Float64),
        Field(name="cpu_usage_mean_1h", dtype=Float64),
        Field(name="cpu_usage_mean_24h", dtype=Float64),
        Field(name="battery_soh_mean_5m", dtype=Float64),
        Field(name="battery_soh_mean_1h", dtype=Float64),
        Field(name="battery_soh_mean_24h", dtype=Float64),
        Field(name="error_count_5m", dtype=Int64),
        Field(name="record_count_5m", dtype=Int64),
        Field(name="label_failure_within_horizon", dtype=Bool),
    ],
    source=telemetry_feature_source,
)


telemetry_temporal_features = FeatureView(
    name="telemetry_temporal_features",
    entities=[machine],
    ttl=timedelta(minutes=30),
    schema=[
        Field(name="vibration_mean_5m_lag_1", dtype=Float64),
        Field(name="vibration_mean_5m_lag_3", dtype=Float64),
        Field(name="motor_temp_mean_5m_lag_1", dtype=Float64),
        Field(name="battery_soh_mean_5m_lag_1", dtype=Float64),
        Field(name="cpu_usage_mean_5m_lag_1", dtype=Float64),
        Field(name="vibration_mean_5m_roc_1", dtype=Float64),
        Field(name="motor_temp_mean_5m_roc_1", dtype=Float64),
        Field(name="battery_soh_mean_5m_roc_1", dtype=Float64),
        Field(name="cpu_usage_mean_5m_roc_1", dtype=Float64),
        Field(name="uptime_ratio_5m", dtype=Float32),
    ],
    source=telemetry_feature_source,
)