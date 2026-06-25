"""Runtime configuration for the telemetry generator.

Values are read from environment variables (loaded from ``.env`` when present) with
sensible defaults, and can be overridden by CLI flags in :mod:`data_generator.main`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

try:  # optional dependency; generator still runs without it
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is best-effort
    pass


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class GeneratorConfig:
    """Immutable configuration snapshot for a generator run."""

    fleet_size: int = _get_int("FLEET_SIZE", 500)
    rate_hz: float = _get_float("TELEMETRY_RATE_HZ", 5.0)
    failure_injection_rate: float = _get_float("FAILURE_INJECTION_RATE", 0.02)
    seed: int = _get_int("GENERATOR_SEED", 42)

    # Kafka
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    telemetry_topic: str = os.getenv("KAFKA_TELEMETRY_TOPIC", "iot.telemetry")

    # Failure model
    failure_lead_time_steps: int = _get_int("FAILURE_LEAD_TIME_STEPS", 60)

    @property
    def tick_seconds(self) -> float:
        """Seconds between telemetry ticks per machine."""
        return 1.0 / self.rate_hz if self.rate_hz > 0 else 1.0
