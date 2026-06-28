"""Observability package for the Industrial IoT platform (Phase 10).

Three pillars:

* **System metrics** — Prometheus scrapes the FastAPI serving ``/metrics`` endpoint and
  any exporters (see ``monitoring/prometheus/prometheus.yml``).
* **Dashboards** — Grafana provisions platform-health and ML dashboards from
  ``monitoring/grafana``.
* **Drift** — :mod:`monitoring.drift` compares current feature/prediction data to a
  versioned reference dataset and emits drift metrics (pure-numpy core) plus an optional
  Evidently HTML report.
"""

from __future__ import annotations

__all__ = ["MonitoringConfig", "get_config"]

from monitoring.config import MonitoringConfig, get_config
