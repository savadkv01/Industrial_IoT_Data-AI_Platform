"""Per-machine state machine producing correlated telemetry over time.

A :class:`Machine` holds evolving physical state (position, load, temperature,
battery, wear) so successive readings are temporally correlated rather than
independent random draws. Sensor noise is added on top of the latent state.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone

from .failure import DegradationState
from .schema import TelemetryRecord

# Rough geographic bounding box for the simulated fleet (central Europe).
_LAT_MIN, _LAT_MAX = 47.0, 55.0
_LON_MIN, _LON_MAX = 5.0, 15.0

_AMBIENT_TEMP_C = 20.0


class Machine:
    """Simulates one industrial machine emitting one record per :meth:`step`."""

    def __init__(
        self,
        machine_id: str,
        rng: random.Random,
        is_degrading: bool,
        lead_time_steps: int,
        start_time: datetime | None = None,
    ) -> None:
        self.machine_id = machine_id
        self._rng = rng
        self._lead_time_steps = lead_time_steps
        self._degradation = DegradationState(rng=rng, is_degrading=is_degrading)

        # Latent physical state.
        self._lat = rng.uniform(_LAT_MIN, _LAT_MAX)
        self._lon = rng.uniform(_LON_MIN, _LON_MAX)
        self._heading = rng.uniform(0.0, 2 * math.pi)
        self._speed = rng.uniform(0.0, 40.0)
        self._motor_temp = _AMBIENT_TEMP_C + rng.uniform(5.0, 15.0)
        self._battery_soh = rng.uniform(0.85, 1.0)
        self._phase = rng.uniform(0.0, 2 * math.pi)  # for periodic load
        self._t = start_time or datetime.now(timezone.utc)

    # ── helpers ──
    def _noise(self, scale: float) -> float:
        return self._rng.gauss(0.0, scale)

    def _advance_position(self, dt_seconds: float) -> None:
        # Random-walk the heading slightly, move forward at current speed.
        self._heading += self._noise(0.05)
        # Speed drifts toward a load-dependent target.
        target_speed = 20.0 + 15.0 * math.sin(self._phase)
        self._speed = max(0.0, self._speed + 0.1 * (target_speed - self._speed) + self._noise(1.0))
        # km/h -> degrees (very rough; fine for synthetic data).
        dist_deg = (self._speed / 3600.0) * dt_seconds * 0.01
        self._lat = min(_LAT_MAX, max(_LAT_MIN, self._lat + dist_deg * math.cos(self._heading)))
        self._lon = min(_LON_MAX, max(_LON_MIN, self._lon + dist_deg * math.sin(self._heading)))

    def step(self, dt_seconds: float = 1.0) -> TelemetryRecord:
        """Advance the machine by ``dt_seconds`` and emit one telemetry record."""
        self._t = self._t + timedelta(seconds=dt_seconds)
        self._phase += 0.02
        self._degradation.step()
        wear = self._degradation.wear

        self._advance_position(dt_seconds)

        # Load-driven motor temperature; wear adds a rising thermal bias.
        load = 0.5 + 0.5 * math.sin(self._phase)
        target_temp = _AMBIENT_TEMP_C + 40.0 * load + 50.0 * wear
        self._motor_temp += 0.2 * (target_temp - self._motor_temp) + self._noise(0.5)

        # Battery state of health decays slowly, faster under wear.
        self._battery_soh = max(
            0.0, self._battery_soh - (1e-6 + 5e-5 * wear) - abs(self._noise(1e-6))
        )

        # Vibration rises with wear; IMU acceleration centred on motion + vibration.
        vibration = max(0.0, 0.2 + 3.0 * wear + abs(self._noise(0.05)))
        accel_x = self._noise(0.3) + vibration * self._noise(1.0)
        accel_y = self._noise(0.3) + vibration * self._noise(1.0)
        accel_z = 9.81 + self._noise(0.3) + vibration * self._noise(1.0)

        # CPU usage tracks load with wear-related spikes.
        cpu_usage = min(
            100.0, max(0.0, 30.0 + 40.0 * load + 20.0 * wear + self._noise(3.0))
        )

        return TelemetryRecord(
            machine_id=self.machine_id,
            ts=self._t,
            lat=round(self._lat, 6),
            lon=round(self._lon, 6),
            speed=round(self._speed, 3),
            accel_x=round(accel_x, 4),
            accel_y=round(accel_y, 4),
            accel_z=round(accel_z, 4),
            vibration=round(vibration, 4),
            battery_soh=round(self._battery_soh, 6),
            motor_temp=round(self._motor_temp, 3),
            cpu_usage=round(cpu_usage, 2),
            error_code=self._degradation.error_code(),
            event=self._degradation.event_label(),
            failure_within_horizon=self._degradation.failure_within_horizon(
                self._lead_time_steps
            ),
        )
