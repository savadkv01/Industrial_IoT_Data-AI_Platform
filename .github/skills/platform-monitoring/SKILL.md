---
name: platform-monitoring
description: 'Build observability for the platform (Phase 10). Use when configuring Prometheus scraping, Grafana dashboards, system metrics (CPU, Kafka lag, throughput, latency), or data/model drift detection with Evidently AI.'
argument-hint: 'e.g. "add Grafana dashboard" or "generate Evidently drift report"'
---

# Platform Monitoring

## When to use
- Implementing anything under `monitoring/`.
- Wiring metrics, dashboards, or drift detection.

## Three pillars
1. **System metrics** (Prometheus): CPU/memory, Kafka consumer lag, Spark batch duration, throughput, API latency.
2. **Data drift** (Evidently): distribution shift in incoming features vs a training reference.
3. **Model drift** (Evidently): prediction distribution shift and performance decay over time.

## Procedure
1. **Prometheus** (`monitoring/prometheus/prometheus.yml`): define scrape jobs for the FastAPI `/metrics` endpoint and any exporters.
2. Instrument services with `prometheus-client` (counters/histograms/gauges).
3. **Grafana** (`monitoring/grafana/dashboards/`): provision dashboards for platform health (ingestion rate, lag, latency) and ML metrics.
4. **Drift** (`monitoring/drift/evidently_reports.py`): compute Evidently reports comparing current Gold/feature data to a reference dataset; export metrics/HTML.
5. Schedule the drift report via Airflow (`drift_report` DAG); surface key drift metrics in Grafana.

## Alerting
- Define thresholds (Kafka lag, latency p95, drift score) and alert when breached.
- Tie model-performance alerts to the retraining DAG (close the MLOps loop).

## Guardrails
- Keep a stable, versioned reference dataset for drift comparison.
- Label metrics with `machine_id`/`model_version` sparingly to avoid cardinality blow-up.
