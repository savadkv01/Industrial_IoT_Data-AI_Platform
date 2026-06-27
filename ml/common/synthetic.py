"""Synthetic feature-frame generator.

Produces a frame with the **same shape** as the Phase 5 offline dataset so training
pipelines and tests can run without a live lakehouse. The failure label is correlated
with high vibration / motor temperature and low battery SoH, so baseline models learn a
signal (AUC > 0.5) rather than noise.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Numeric feature columns mirroring ``transforms.build_feature_dataset`` output.
_WINDOWS = ("5m", "1h", "24h")
_METRICS = (
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


def make_synthetic_feature_frame(
    n_machines: int = 20,
    n_steps: int = 60,
    seed: int = 42,
    failure_rate: float = 0.12,
) -> pd.DataFrame:
    """Build a deterministic synthetic feature frame for tests / smoke runs.

    "Stress" follows a per-machine AR(1) process around a machine-specific baseline,
    so it is **stationary in time** (no global trend tied to the timestamp). This keeps
    the feature→label relationship consistent across any time-aware train/test split,
    while lag features still carry signal via the autocorrelation.
    """
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=n_steps, freq="5min")
    rows: list[dict] = []

    for m in range(n_machines):
        machine_id = f"machine-{m:04d}"
        level = rng.uniform(0.1, 0.7)  # machine baseline stress
        stress_prev = level
        for t in range(n_steps):
            event_ts = timestamps[t]
            # AR(1) stress: mean-reverting to the machine baseline, stationary over time.
            stress = 0.7 * stress_prev + 0.3 * level + rng.normal(0, 0.08)
            stress = float(np.clip(stress, 0.0, 1.2))
            stress_prev = stress

            base_vib = 0.5 + 1.5 * stress + rng.normal(0, 0.1)
            base_temp = 60 + 30 * stress + rng.normal(0, 2.0)
            base_cpu = 30 + 40 * stress + rng.normal(0, 5.0)
            base_soh = 1.0 - 0.4 * stress + rng.normal(0, 0.02)

            row: dict = {
                "machine_id": machine_id,
                "event_timestamp": event_ts,
                "created_timestamp": event_ts,
                "window_duration_anchor": "5m",
            }
            for w_idx, window in enumerate(_WINDOWS):
                smooth = 1.0 + 0.1 * w_idx  # longer windows are slightly smoother/higher
                row[f"vibration_mean_{window}"] = max(0.0, base_vib * smooth)
                row[f"vibration_std_{window}"] = abs(rng.normal(0.1, 0.03))
                row[f"vibration_max_{window}"] = max(0.0, base_vib * smooth + abs(rng.normal(0.3, 0.1)))
                row[f"motor_temp_mean_{window}"] = base_temp * smooth
                row[f"motor_temp_std_{window}"] = abs(rng.normal(1.0, 0.3))
                row[f"motor_temp_max_{window}"] = base_temp * smooth + abs(rng.normal(3.0, 1.0))
                row[f"cpu_usage_mean_{window}"] = float(np.clip(base_cpu, 0, 100))
                row[f"cpu_usage_std_{window}"] = abs(rng.normal(3.0, 1.0))
                row[f"battery_soh_mean_{window}"] = float(np.clip(base_soh, 0, 1))
                row[f"battery_soh_min_{window}"] = float(np.clip(base_soh - 0.02, 0, 1))
                row[f"error_count_{window}"] = int(rng.poisson(0.5 + 2 * stress))
                row[f"record_count_{window}"] = int(60 * (w_idx + 1))

            # Temporal features (lag/roc/uptime).
            for col in (
                "vibration_mean_5m",
                "motor_temp_mean_5m",
                "battery_soh_mean_5m",
                "cpu_usage_mean_5m",
            ):
                row[f"{col}_lag_1"] = row[col] * rng.uniform(0.95, 1.0)
                if col == "vibration_mean_5m":
                    row[f"{col}_lag_3"] = row[col] * rng.uniform(0.9, 1.0)
                row[f"{col}_roc_1"] = row[col] - row[f"{col}_lag_1"]
            row["uptime_ratio_5m"] = float(
                np.clip(1.0 - row["error_count_5m"] / max(1, row["record_count_5m"]), 0, 1)
            )

            # Failure label tracks instantaneous stress (stationary in time), so the
            # feature->label signal is learnable under a time-aware split.
            prob = float(np.clip(0.04 + 0.7 * stress, 0, 1)) * (failure_rate / 0.12)
            row["label_failure_within_horizon"] = bool(rng.random() < min(prob, 1.0))
            rows.append(row)

    frame = pd.DataFrame(rows)
    return frame.sort_values(["machine_id", "event_timestamp"]).reset_index(drop=True)
