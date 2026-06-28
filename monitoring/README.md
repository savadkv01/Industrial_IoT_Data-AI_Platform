# Monitoring & Observability — Phase 10

System, data, and model monitoring for the Industrial IoT platform.

## Stack
- **Prometheus** — scrapes system + serving metrics (request rate, latency, errors,
  prediction-score distribution, model version).
- **Grafana** — provisioned dashboards for platform health, ML metrics/drift, and BI/AI
  analytics (Gold lakehouse KPIs + model predictions).
- **Evidently AI** — data + model drift HTML reports, backed by a dependency-light
  PSI/KS drift core that also feeds Prometheus/Grafana.

## Layout
```
monitoring/
├── __init__.py              # package: import as `monitoring`
├── config.py                # MonitoringConfig (datasets, thresholds, BI/Postgres, dirs)
├── drift/
│   ├── metrics.py           # pure-numpy PSI + KS drift core (no heavy deps)
│   └── evidently_reports.py # Evidently HTML + JSON summary + CLI
├── analytics/
│   ├── transforms.py        # pure-pandas Gold + predictions → BI tables
│   └── export_bi.py         # DuckDB read Gold + write Postgres `analytics` schema + CLI
├── prometheus/
│   ├── prometheus.yml       # scrape jobs (serving /metrics, exporters)
│   └── rules/alerts.yml     # latency/error/up alerting rules
├── grafana/
│   ├── provisioning/        # Prometheus + Postgres datasources, dashboard provider
│   └── dashboards/          # platform_health, ml_metrics, fleet_operations, ml_predictions
└── tests/
```

## Running the stack
```bash
# Operational metrics (depends on the Phase 9 serving service for app metrics).
docker compose up -d serving prometheus grafana
```
- Prometheus UI: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin by default) → *Industrial IoT* folder.

### Dashboards
| Dashboard | Datasource | Shows |
| --- | --- | --- |
| **Platform Health** | Prometheus | Serving up, request/error rate, latency p50/p95 |
| **ML Metrics & Drift** | Prometheus | Served model versions, prediction scores, drift gauges |
| **Fleet Operations (BI)** | Postgres `analytics` | Fleet KPIs, vibration/battery trends, machine-health table |
| **Predictive Maintenance (AI)** | Postgres `analytics` | At-risk leaderboard, failure-probability + anomaly trends |

## BI & AI dashboards (Gold + predictions → Postgres)
The two analytics dashboards read curated tables in the Postgres `analytics` schema,
populated from the **Gold** lakehouse table and the **ML batch predictions**:

```bash
# Prereqs running: docker compose up -d minio postgres
# (and Gold built + `python -m ml.inference.batch --task ...` predictions written)

# Host run — reads Gold from MinIO, predictions from ml/predictions, writes Postgres.
MINIO_ENDPOINT=http://localhost:9000 ANALYTICS_PG_HOST=localhost \
  python -m monitoring.analytics.export_bi          # or --dry-run for row counts

# Scheduled — the `bi_export` Airflow DAG runs this daily inside the network.
```

Tables written: `fleet_kpis`, `machine_health`, `gold_features`, `predictions`,
`predictions_latest`. Reads are best-effort — a missing Gold table or absent predictions
yields empty tables rather than a failure. Grafana auto-provisions the **Analytics**
Postgres datasource (`analytics-pg`).

## Drift reports
The drift core compares the **current** feature dataset to a **versioned reference**:

```bash
# 1. Capture a stable reference snapshot (run once on a known-good dataset).
python -m monitoring.drift.evidently_reports snapshot

# 2. Compute drift → writes monitoring/drift/reports/{drift_summary.json, drift_report.html}.
#    Exit code is non-zero when dataset-level drift is detected (CI/alert friendly).
python -m monitoring.drift.evidently_reports report
```

Key signals (per numeric feature):
- **PSI** — binned distribution divergence; *drifted* when `PSI > MONITORING_PSI_THRESHOLD` (default 0.2).
- **KS** — Kolmogorov–Smirnov max-CDF gap in `[0, 1]`.
- **Dataset drift** fires when the share of drifted features exceeds `MONITORING_DATASET_DRIFT_SHARE` (default 0.5).

The Evidently HTML report is best-effort: if Evidently is missing or its API shifts, the
JSON summary is still written.

### Configuration (env / `.env`)
| Variable | Default | Purpose |
| --- | --- | --- |
| `MONITORING_CURRENT_FEATURES` | Phase 5 offline parquet | Dataset to inspect |
| `MONITORING_REFERENCE_FEATURES` | `monitoring/drift/reference/...` | Reference snapshot |
| `MONITORING_REPORTS_DIR` | `monitoring/drift/reports` | Report output dir |
| `MONITORING_PSI_THRESHOLD` | `0.2` | Per-feature drift flag |
| `MONITORING_DATASET_DRIFT_SHARE` | `0.5` | Dataset-level drift flag |
| `PROMETHEUS_PUSHGATEWAY` | _(unset)_ | Pushgateway for the drift DAG metrics |

## Scheduling & the MLOps loop
`orchestration/dags/drift_report.py` runs the drift report daily, pushes the drift gauges
(`iiot_drift_share`, `iiot_dataset_drift`, `iiot_drift_features_drifted`) to the Prometheus
pushgateway, and triggers the `train_models` retraining DAG when dataset-level drift is
detected — closing the monitoring → retraining loop.

## Tests
```bash
cd monitoring && ../.venv/Scripts/python -m pytest -q
```

See the `platform-monitoring` agent skill for the build procedure.
