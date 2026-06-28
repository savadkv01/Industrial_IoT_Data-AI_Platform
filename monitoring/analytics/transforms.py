"""Pure-pandas BI transforms — Gold + ML predictions → analytics tables.

These functions are dependency-light (pandas only) so they are fully unit-testable without
MinIO, DuckDB or Postgres. :mod:`monitoring.analytics.export_bi` reads the real Gold Delta
table and prediction parquets, calls these transforms, and materialises the results into
Postgres for Grafana to chart.

Gold schema (per :mod:`lakehouse.gold.build_gold`)::

    machine_id, window_start, window_end, window_duration, event_date,
    vibration_mean/std/max, motor_temp_mean/std/max, cpu_usage_mean/std,
    battery_soh_mean/min, error_count, record_count, failure_label

Prediction parquet columns (per :mod:`ml.inference.batch`)::

    machine_id, event_timestamp, <output_column>, model, alias
"""

from __future__ import annotations

import pandas as pd

# Output column written by each model family (see ml.common.tasks).
TASK_SCORE_COLUMNS: dict[str, str] = {
    "predictive_maintenance": "failure_probability",
    "anomaly_detection": "anomaly_score",
    "battery_health": "battery_soh_prediction",
}


def filter_recent(gold: pd.DataFrame, days: int) -> pd.DataFrame:
    """Keep only rows whose ``window_end`` is within ``days`` of the latest window.

    ``days <= 0`` returns the frame unchanged. Anchoring on the data's own max timestamp
    (not wall-clock) keeps the dashboards populated even with a static demo dataset.
    """
    if days <= 0 or "window_end" not in gold.columns or gold.empty:
        return gold
    window_end = pd.to_datetime(gold["window_end"], utc=True)
    cutoff = window_end.max() - pd.Timedelta(days=days)
    return gold.loc[window_end >= cutoff].copy()


def fleet_kpis(gold: pd.DataFrame) -> pd.DataFrame:
    """Single-row fleet-level KPI summary for Grafana stat panels."""
    if gold.empty:
        row = {
            "n_machines": 0,
            "n_windows": 0,
            "failure_windows": 0,
            "failure_rate": 0.0,
            "avg_battery_soh": None,
            "avg_vibration": None,
            "total_errors": 0,
        }
        return pd.DataFrame([row])

    failures = gold["failure_label"].astype("boolean").fillna(False).astype(int)
    row = {
        "n_machines": int(gold["machine_id"].nunique()),
        "n_windows": int(len(gold)),
        "failure_windows": int(failures.sum()),
        "failure_rate": float(failures.mean()),
        "avg_battery_soh": float(gold["battery_soh_mean"].mean()),
        "avg_vibration": float(gold["vibration_mean"].mean()),
        "total_errors": int(gold["error_count"].sum()),
    }
    return pd.DataFrame([row])


def machine_health(gold: pd.DataFrame, duration: str = "5m") -> pd.DataFrame:
    """Per-machine latest operational health, taken from the finest window granularity.

    Falls back to all durations when the requested ``duration`` is not present.
    """
    if gold.empty:
        return pd.DataFrame(
            columns=[
                "machine_id",
                "last_window_end",
                "battery_soh_mean",
                "vibration_mean",
                "vibration_max",
                "motor_temp_mean",
                "error_count",
                "failure_label",
            ]
        )

    scoped = gold
    if "window_duration" in gold.columns and (gold["window_duration"] == duration).any():
        scoped = gold.loc[gold["window_duration"] == duration]

    scoped = scoped.sort_values("window_end")
    latest = scoped.groupby("machine_id", as_index=False).tail(1)
    health = latest[
        [
            "machine_id",
            "window_end",
            "battery_soh_mean",
            "vibration_mean",
            "vibration_max",
            "motor_temp_mean",
            "error_count",
            "failure_label",
        ]
    ].rename(columns={"window_end": "last_window_end"})
    return health.sort_values("machine_id").reset_index(drop=True)


def gold_export(gold: pd.DataFrame) -> pd.DataFrame:
    """Curated Gold rows for time-series BI panels (typed timestamps, sorted)."""
    if gold.empty:
        return gold
    out = gold.copy()
    for col in ("window_start", "window_end"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], utc=True)
    sort_cols = [c for c in ("machine_id", "window_end") if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def combine_predictions(predictions: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Stack per-task prediction frames into a long ``(machine_id, task, score)`` table."""
    frames: list[pd.DataFrame] = []
    for task, frame in predictions.items():
        if frame is None or frame.empty:
            continue
        score_col = TASK_SCORE_COLUMNS.get(task)
        if score_col is None or score_col not in frame.columns:
            continue
        long = pd.DataFrame(
            {
                "machine_id": frame["machine_id"],
                "task": task,
                "score": frame[score_col].astype(float),
            }
        )
        if "event_timestamp" in frame.columns:
            long["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
        else:
            long["event_timestamp"] = pd.NaT
        for meta in ("model", "alias"):
            long[meta] = frame[meta] if meta in frame.columns else None
        frames.append(long)

    if not frames:
        return pd.DataFrame(
            columns=["machine_id", "task", "score", "event_timestamp", "model", "alias"]
        )
    return pd.concat(frames, ignore_index=True)


def latest_predictions(predictions_long: pd.DataFrame) -> pd.DataFrame:
    """Per-machine latest score for each task, pivoted wide for an at-risk leaderboard.

    Columns: ``machine_id`` + one column per task score (e.g. ``failure_probability``),
    sorted by failure probability descending so the riskiest machines surface first.
    """
    score_cols = list(TASK_SCORE_COLUMNS.values())
    if predictions_long.empty:
        return pd.DataFrame(columns=["machine_id", *score_cols, "last_scored_at"])

    df = predictions_long.sort_values("event_timestamp")
    latest = df.groupby(["machine_id", "task"], as_index=False).tail(1)

    wide = latest.pivot_table(
        index="machine_id", columns="task", values="score", aggfunc="last"
    )
    # Map task keys → their human score-column names.
    wide = wide.rename(columns=TASK_SCORE_COLUMNS)
    wide = wide.reset_index()

    last_at = latest.groupby("machine_id")["event_timestamp"].max().rename("last_scored_at")
    wide = wide.merge(last_at, on="machine_id", how="left")

    for col in score_cols:
        if col not in wide.columns:
            wide[col] = pd.NA

    sort_col = TASK_SCORE_COLUMNS["predictive_maintenance"]
    wide = wide.sort_values(sort_col, ascending=False, na_position="last").reset_index(drop=True)
    return wide[["machine_id", *score_cols, "last_scored_at"]]


def build_bi_tables(
    gold: pd.DataFrame,
    predictions: dict[str, pd.DataFrame],
    *,
    recent_days: int = 0,
) -> dict[str, pd.DataFrame]:
    """Build every analytics table from a Gold frame + per-task prediction frames."""
    scoped_gold = filter_recent(gold, recent_days)
    predictions_long = combine_predictions(predictions)
    return {
        "fleet_kpis": fleet_kpis(scoped_gold),
        "machine_health": machine_health(scoped_gold),
        "gold_features": gold_export(scoped_gold),
        "predictions": predictions_long,
        "predictions_latest": latest_predictions(predictions_long),
    }
