"""Prometheus instrumentation for the serving API (Phase 9).

Exposes request volume, latency and prediction-score distribution per task so the
Phase 10 monitoring stack can scrape ``GET /metrics`` and alert on latency SLA breaches
or shifts in the prediction distribution (a cheap model-drift signal).
"""

from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

REQUEST_COUNT = Counter(
    "serving_requests_total",
    "Total prediction requests.",
    labelnames=("task", "status"),
)

REQUEST_LATENCY = Histogram(
    "serving_request_latency_seconds",
    "Prediction request latency in seconds.",
    labelnames=("task",),
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

PREDICTION_SCORE = Histogram(
    "serving_prediction_score",
    "Distribution of model output scores.",
    labelnames=("task",),
)

MODEL_VERSION = Gauge(
    "serving_model_version_info",
    "Currently served model version (label-only; value is always 1).",
    labelnames=("task", "model_name", "model_alias", "model_version"),
)


def render_metrics() -> tuple[bytes, str]:
    """Return the Prometheus exposition payload and its content type."""
    return generate_latest(), CONTENT_TYPE_LATEST
