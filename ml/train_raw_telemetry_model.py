"""Train and register an anomaly detection model on raw telemetry features.

The original model was trained on feature-store aggregations (mean_1h, min_24h, etc.)
which are not available in the real-time Kafka stream. This script trains a new
IsolationForest on the raw sensor readings that the streaming job actually sends,
then registers it as the 'production' alias so spark-alerting can score live data.
"""
import os

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
mlflow.set_tracking_uri("http://localhost:5000")

# Raw telemetry numeric features — must match TELEMETRY_FEATURE_COLUMNS in streaming/fields.py
RAW_FEATURES = ["lat", "lon", "speed", "accel_x", "accel_y", "accel_z",
                "vibration", "battery_soh", "motor_temp", "cpu_usage"]

# Synthetic normal-operation training data
rng = np.random.default_rng(42)
n = 5000
X_train = pd.DataFrame({
    "lat": rng.uniform(-90, 90, n),
    "lon": rng.uniform(-180, 180, n),
    "speed": rng.uniform(0, 80, n),
    "accel_x": rng.normal(0, 0.5, n),
    "accel_y": rng.normal(0, 0.5, n),
    "accel_z": rng.normal(9.8, 0.3, n),
    "vibration": rng.uniform(0, 2, n),
    "battery_soh": rng.uniform(70, 100, n),
    "motor_temp": rng.uniform(20, 90, n),
    "cpu_usage": rng.uniform(0, 80, n),
})

model = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler()),
    ("iforest", IsolationForest(n_estimators=100, contamination=0.05, random_state=42)),
])
model.fit(X_train)
print("Model trained on", len(X_train), "samples")

# Validate: anomalous readings score above threshold
X_anomaly = pd.DataFrame({
    "lat": [0] * 5, "lon": [0] * 5, "speed": [200] * 5,
    "accel_x": [50] * 5, "accel_y": [50] * 5, "accel_z": [50] * 5,
    "vibration": [50] * 5, "battery_soh": [1] * 5,
    "motor_temp": [300] * 5, "cpu_usage": [99] * 5,
})
scores_normal = -model.score_samples(X_train[:50])
scores_anom = -model.score_samples(X_anomaly)
print(f"Normal scores: mean={scores_normal.mean():.3f}")
print(f"Anomaly scores: mean={scores_anom.mean():.3f}")
print(f"Anomaly >= 0.5? {(scores_anom >= 0.5).all()}")

# Register to MLflow with MinIO artifact storage
client = mlflow.MlflowClient()
exp = client.get_experiment_by_name("anomaly_detection_raw")
if exp is None:
    exp_id = client.create_experiment(
        "anomaly_detection_raw",
        artifact_location="s3://mlflow/anomaly_detection_raw",
    )
else:
    exp_id = exp.experiment_id

mlflow.set_experiment(experiment_id=exp_id)
with mlflow.start_run(run_name="raw-telemetry-model") as run:
    mlflow.sklearn.log_model(
        model,
        name="model",
        registered_model_name="iiot_anomaly_detection",
        serialization_format=mlflow.sklearn.SERIALIZATION_FORMAT_PICKLE,
    )
    print("Run ID:", run.info.run_id)
    print("Artifact URI:", run.info.artifact_uri)

versions = client.search_model_versions("name='iiot_anomaly_detection'")
latest = max(int(v.version) for v in versions)
client.set_registered_model_alias("iiot_anomaly_detection", "production", str(latest))
print(f"production -> v{latest}")
