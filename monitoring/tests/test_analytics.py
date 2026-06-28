"""Tests for the Phase 10 BI analytics transforms and export writer."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from monitoring.analytics import transforms
from monitoring.analytics.export_bi import write_postgres
from monitoring.config import MonitoringConfig

RNG = np.random.default_rng(7)


def _gold_frame() -> pd.DataFrame:
    """Two machines × two window durations × a short timeline."""
    rows = []
    base = pd.Timestamp("2024-03-01", tz="UTC")
    for machine in ("m1", "m2"):
        for duration in ("5m", "1h"):
            for i in range(5):
                start = base + pd.Timedelta(hours=i)
                rows.append(
                    {
                        "machine_id": machine,
                        "window_start": start,
                        "window_end": start + pd.Timedelta(minutes=5),
                        "window_duration": duration,
                        "event_date": start.date(),
                        "vibration_mean": float(RNG.uniform(0.1, 0.5)),
                        "vibration_std": 0.05,
                        "vibration_max": float(RNG.uniform(0.5, 1.0)),
                        "motor_temp_mean": float(RNG.uniform(40, 70)),
                        "motor_temp_std": 1.0,
                        "motor_temp_max": 80.0,
                        "cpu_usage_mean": 50.0,
                        "cpu_usage_std": 5.0,
                        "battery_soh_mean": float(RNG.uniform(0.5, 0.95)),
                        "battery_soh_min": 0.5,
                        "error_count": int(RNG.integers(0, 4)),
                        "record_count": 100,
                        "failure_label": bool(machine == "m2" and i == 4),
                    }
                )
    return pd.DataFrame(rows)


def _predictions() -> dict[str, pd.DataFrame]:
    ts = pd.date_range("2024-03-01", periods=3, freq="h", tz="UTC")
    return {
        "predictive_maintenance": pd.DataFrame(
            {
                "machine_id": ["m1", "m2", "m2"],
                "event_timestamp": [ts[0], ts[0], ts[2]],
                "failure_probability": [0.1, 0.4, 0.9],
                "model": "iiot_predictive_maintenance",
                "alias": "production",
            }
        ),
        "anomaly_detection": pd.DataFrame(
            {
                "machine_id": ["m1", "m2"],
                "event_timestamp": [ts[0], ts[0]],
                "anomaly_score": [0.2, 0.7],
                "model": "iiot_anomaly_detection",
                "alias": "production",
            }
        ),
        "battery_health": pd.DataFrame(
            {
                "machine_id": ["m1", "m2"],
                "event_timestamp": [ts[0], ts[0]],
                "battery_soh_prediction": [0.85, 0.55],
                "model": "iiot_battery_health",
                "alias": "production",
            }
        ),
    }


def test_fleet_kpis():
    gold = _gold_frame()
    kpis = transforms.fleet_kpis(gold)
    assert len(kpis) == 1
    row = kpis.iloc[0]
    assert row["n_machines"] == 2
    assert row["n_windows"] == len(gold)
    assert row["failure_windows"] == 2
    assert 0.0 < row["failure_rate"] < 1.0


def test_fleet_kpis_empty():
    kpis = transforms.fleet_kpis(pd.DataFrame())
    assert kpis.iloc[0]["n_machines"] == 0
    assert kpis.iloc[0]["failure_rate"] == 0.0


def test_machine_health_uses_latest_5m_window():
    health = transforms.machine_health(_gold_frame())
    assert set(health["machine_id"]) == {"m1", "m2"}
    # One row per machine (the latest window only).
    assert len(health) == 2
    assert "battery_soh_mean" in health.columns
    assert "last_window_end" in health.columns


def test_combine_predictions_long_shape():
    long = transforms.combine_predictions(_predictions())
    assert set(long["task"].unique()) == {
        "predictive_maintenance",
        "anomaly_detection",
        "battery_health",
    }
    assert {"machine_id", "task", "score", "event_timestamp"}.issubset(long.columns)


def test_latest_predictions_pivots_and_ranks():
    long = transforms.combine_predictions(_predictions())
    latest = transforms.latest_predictions(long)
    # Wide: one row per machine with each task's score column.
    assert list(latest["machine_id"]) == ["m2", "m1"]  # m2 has the higher failure prob
    assert latest.iloc[0]["failure_probability"] == pytest.approx(0.9)  # m2 latest (ts[2])
    assert "anomaly_score" in latest.columns
    assert "battery_soh_prediction" in latest.columns


def test_build_bi_tables_keys():
    tables = transforms.build_bi_tables(_gold_frame(), _predictions())
    assert set(tables) == {
        "fleet_kpis",
        "machine_health",
        "gold_features",
        "predictions",
        "predictions_latest",
    }
    for frame in tables.values():
        assert isinstance(frame, pd.DataFrame)


def test_build_bi_tables_empty_inputs():
    tables = transforms.build_bi_tables(pd.DataFrame(), {})
    assert tables["fleet_kpis"].iloc[0]["n_machines"] == 0
    assert tables["predictions"].empty
    assert tables["predictions_latest"].empty


def test_write_postgres_roundtrip_sqlite(tmp_path):
    """The writer is dialect-generic; validate it against SQLite (no schema)."""
    db = tmp_path / "analytics.db"
    cfg = MonitoringConfig(analytics_pg_dsn=f"sqlite:///{db}")
    tables = transforms.build_bi_tables(_gold_frame(), _predictions())

    written = write_postgres(tables, cfg)

    assert written["fleet_kpis"] == 1
    assert written["predictions_latest"] == 2
    # Read one table back to confirm it persisted.
    from sqlalchemy import create_engine

    engine = create_engine(cfg.postgres_url)
    back = pd.read_sql("SELECT * FROM predictions_latest", engine)
    engine.dispose()
    assert len(back) == 2
