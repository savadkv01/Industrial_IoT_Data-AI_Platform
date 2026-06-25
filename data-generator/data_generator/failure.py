"""Failure and degradation modelling.

Each machine carries a hidden *wear* state in ``[0, 1]``. Wear grows slowly over
time; a subset of machines are seeded as "degrading" (faster wear) so failures stay
rare and configurable via ``FAILURE_INJECTION_RATE``. When wear crosses a threshold
the machine is considered failing, and records within ``lead_time_steps`` *before*
the crossing are labelled ``failure_within_horizon=True`` for supervised learning.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

# Fault categories surfaced via ``error_code`` / ``event``.
ERROR_NONE = 0
ERROR_OVERHEAT = 101
ERROR_VIBRATION = 102
ERROR_BATTERY = 103

FAILURE_THRESHOLD = 0.85
WARNING_THRESHOLD = 0.60


@dataclass
class DegradationState:
    """Hidden per-machine degradation state driving observable sensor drift."""

    rng: random.Random
    is_degrading: bool
    wear: float = 0.0
    # Base wear increment per tick; degrading machines wear faster.
    _base_rate: float = field(init=False)

    def __post_init__(self) -> None:
        if self.is_degrading:
            # Reaches failure over a few thousand ticks.
            self._base_rate = self.rng.uniform(2.5e-4, 6.0e-4)
        else:
            # Healthy machines barely wear within a simulation run.
            self._base_rate = self.rng.uniform(1e-6, 1e-5)

    def step(self) -> None:
        """Advance wear by one tick with mild stochastic jitter."""
        jitter = self.rng.uniform(0.8, 1.2)
        self.wear = min(1.0, self.wear + self._base_rate * jitter)

    @property
    def failing(self) -> bool:
        return self.wear >= FAILURE_THRESHOLD

    @property
    def warning(self) -> bool:
        return WARNING_THRESHOLD <= self.wear < FAILURE_THRESHOLD

    def error_code(self) -> int:
        """Map the dominant degradation signature to a fault category."""
        if not self.failing:
            return ERROR_NONE
        # Deterministic-ish category from the rng stream for variety.
        return self.rng.choice([ERROR_OVERHEAT, ERROR_VIBRATION, ERROR_BATTERY])

    def event_label(self) -> str:
        if self.failing:
            return "fault"
        if self.warning:
            return "warning"
        return "ok"

    def failure_within_horizon(self, lead_time_steps: int) -> bool:
        """True when wear is within the lead-time window before/at failure.

        Approximated by checking whether wear is close enough to the threshold that,
        at the current base rate, failure occurs within ``lead_time_steps`` ticks.
        """
        if self.failing:
            return True
        if not self.is_degrading or self._base_rate <= 0:
            return False
        remaining = (FAILURE_THRESHOLD - self.wear) / self._base_rate
        return remaining <= lead_time_steps
