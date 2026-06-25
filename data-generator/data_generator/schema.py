"""Telemetry schema definitions (Pydantic).

A single ``TelemetryRecord`` is what each machine emits per tick. The schema is the
contract shared with the streaming pipeline (Phase 3) and the lakehouse (Phase 4),
so keep field names stable and partition-friendly (``machine_id`` first).
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class TelemetryRecord(BaseModel):
    """One telemetry reading from a single machine at a single point in time."""

    # ── Identity & event time ──
    machine_id: str = Field(..., description="Stable machine identifier; Kafka/Delta partition key.")
    ts: datetime = Field(..., description="Event time (UTC) the reading was produced.")

    # ── Location ──
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)
    speed: float = Field(..., ge=0.0, description="Ground speed in km/h.")

    # ── IMU / acceleration (m/s^2) ──
    accel_x: float
    accel_y: float
    accel_z: float
    vibration: float = Field(..., ge=0.0, description="RMS vibration magnitude; rises with wear.")

    # ── Health signals ──
    battery_soh: float = Field(..., ge=0.0, le=1.0, description="Battery state of health [0,1].")
    motor_temp: float = Field(..., description="Motor temperature in degrees Celsius.")
    cpu_usage: float = Field(..., ge=0.0, le=100.0, description="Controller CPU usage percent.")

    # ── Diagnostics ──
    error_code: int = Field(0, description="0 = healthy; non-zero = fault category.")
    event: str = Field("ok", description="Discrete event/log label, e.g. 'ok', 'warning', 'fault'.")

    # ── Supervised label (for predictive maintenance) ──
    failure_within_horizon: bool = Field(
        False,
        description="True if this machine fails within the configured lead-time horizon.",
    )

    def to_json(self) -> str:
        """Serialize to a compact JSON string (used by the Kafka sink)."""
        return self.model_dump_json()

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(timezone.utc)
