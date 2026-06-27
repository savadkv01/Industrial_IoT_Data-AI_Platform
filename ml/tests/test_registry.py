"""Unit tests for the registry promotion logic (pure functions, no MLflow server)."""

from __future__ import annotations

import math

from ml.common.registry import clears_gate, is_better


def test_is_better_promotes_when_no_incumbent() -> None:
    assert is_better(0.7, None, higher_is_better=True)
    assert is_better(0.7, float("nan"), higher_is_better=True)


def test_is_better_respects_direction() -> None:
    # Higher-is-better metric (AUC).
    assert is_better(0.9, 0.8, higher_is_better=True)
    assert not is_better(0.8, 0.9, higher_is_better=True)
    assert not is_better(0.8, 0.8, higher_is_better=True)  # ties keep the incumbent
    # Lower-is-better metric (RMSE).
    assert is_better(0.1, 0.2, higher_is_better=False)
    assert not is_better(0.2, 0.1, higher_is_better=False)


def test_clears_gate_absolute_thresholds() -> None:
    assert clears_gate(0.86, 0.85, higher_is_better=True)
    assert not clears_gate(0.84, 0.85, higher_is_better=True)
    assert clears_gate(0.4, 0.5, higher_is_better=False)  # RMSE below the max passes
    assert not clears_gate(0.6, 0.5, higher_is_better=False)


def test_clears_gate_infinite_gate_always_passes() -> None:
    # An ``inf`` max-RMSE means "no absolute gate" — only relative comparison applies.
    assert clears_gate(123.0, math.inf, higher_is_better=False)
