"""Fleet driver — builds and ticks a population of machines.

Separated from the CLI so it can be unit-tested and reused (e.g. from a notebook).
A fixed fraction of machines (``failure_injection_rate``) are seeded as *degrading*
so labelled failures stay rare and realistic.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import datetime, timezone

from .config import GeneratorConfig
from .machine import Machine
from .schema import TelemetryRecord


class Fleet:
    """A population of :class:`Machine` instances ticked in lockstep."""

    def __init__(self, config: GeneratorConfig, start_time: datetime | None = None) -> None:
        self.config = config
        self._rng = random.Random(config.seed)
        start_time = start_time or datetime.now(timezone.utc)

        n_degrading = int(round(config.fleet_size * config.failure_injection_rate))
        degrading_ids = set(self._rng.sample(range(config.fleet_size), k=n_degrading))

        self.machines: list[Machine] = []
        for i in range(config.fleet_size):
            # Deterministic per-machine RNG stream for reproducibility.
            machine_rng = random.Random(config.seed * 1_000_003 + i)
            self.machines.append(
                Machine(
                    machine_id=f"machine-{i:05d}",
                    rng=machine_rng,
                    is_degrading=i in degrading_ids,
                    lead_time_steps=config.failure_lead_time_steps,
                    start_time=start_time,
                )
            )

    def tick(self, dt_seconds: float | None = None) -> Iterator[TelemetryRecord]:
        """Advance every machine by one step and yield their records."""
        dt = dt_seconds if dt_seconds is not None else self.config.tick_seconds
        for machine in self.machines:
            yield machine.step(dt)
