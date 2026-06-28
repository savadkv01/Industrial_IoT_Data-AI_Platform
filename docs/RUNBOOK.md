# Runbook — Step-by-Step Execution

Copy-paste commands to run each stage of the platform and a full end-to-end pass.
All commands are **Windows PowerShell** from the **repo root** unless noted otherwise.

> Phases map to [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md). Functional acceptance
> per phase is in [TEST_CASES.md](TEST_CASES.md).

---

## Conventions & gotchas

- Always activate the project virtual environment before running Python on the host:
  ```powershell
  .\.venv\Scripts\Activate.ps1
  ```
- Host tools (generator, topic admin) talk to Kafka on **`localhost:9094`**;
  in-container Spark uses **`kafka:9092`**.
- ⚠️ **Compose env leak:** when you set `KAFKA_BOOTSTRAP_SERVERS=localhost:9094` for a host
  tool, always clear it before any `docker compose` command, or it leaks into the Spark
  container and the job can't reach the broker:
  ```powershell
  Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue
  ```
- Bring up **only the services a stage needs** — the compose file is incremental.

### Service URLs

| Service | URL | Credentials | Brought up by |
|---|---|---|---|
| Kafka UI | http://localhost:8080 | — | `kafka-ui` |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin | `minio` |
| MinIO S3 API | http://localhost:9000 | minioadmin / minioadmin | `minio` |
| MLflow | http://localhost:5000 | — | `mlflow` |
| Airflow | http://localhost:8081 | admin / admin | `airflow` |
| FastAPI serving (Swagger) | http://localhost:8000/docs | — | `serving` |
| FastAPI metrics | http://localhost:8000/metrics | — | `serving` |
| Prometheus | http://localhost:9090 | — | `prometheus` |
| Grafana | http://localhost:3000 | admin / admin | `grafana` |
| Spark driver UI* | http://localhost:4040 | — | `spark-*` (see note) |

\* The Spark driver UI (port `4040`) runs **inside** each `spark-*` container but is **not
published** by default. To view it, add a port mapping to the service (e.g.
`ports: ["4040:4040"]` under `spark-streaming`) and restart it, then open
http://localhost:4040 while a job is running. Alternatively inspect progress via
`docker logs -f spark-streaming`.

---

## Stage 0 — One-time setup

```powershell
# Create & activate the virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install platform dependencies
pip install -r requirements.txt

# Install the local packages (editable) used across stages
pip install -e "data-generator"
pip install -e "streaming"
pip install -e "lakehouse[dev]"
pip install -e "feature-engineering[dev]"
pip install -e "ml"
pip install -e "serving"

# Configure environment
Copy-Item .env.example .env
```

---

## Stage 1 — Infrastructure (core services)

Bring up the storage/messaging backbone used by every later stage.

```powershell
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue

# Phase 3-4 core: Kafka, Kafka UI, MinIO + bucket bootstrap, Postgres
docker compose up -d kafka kafka-ui minio createbuckets postgres

# Confirm health
docker compose ps
```

Wait until `kafka` and `minio` report healthy (`docker compose ps`). MinIO buckets
`lakehouse` and `mlflow` are created automatically by the `createbuckets` job.

---

## Stage 2 — Data generation (Phase 2)

```powershell
.\.venv\Scripts\Activate.ps1

# Smoke test to stdout (no infra needed)
python -m data_generator.main --machines 10 --rate 5 --duration 2 --sink stdout

# Or write NDJSON to a file
python -m data_generator.main --machines 50 --rate 5 --duration 10 --sink file --path data-generator/data/telemetry.ndjson
```

Streaming telemetry into Kafka is done in Stage 3.

---

## Stage 3 — Streaming ingestion: Kafka → Bronze (Phase 3)

```powershell
# 1. Create the telemetry topic (host tool → localhost:9094)
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
.\.venv\Scripts\python.exe -m streaming.producers.create_topic
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS

# 2. Start the Spark Structured Streaming ingest job (runs continuously)
docker compose up -d --build spark-streaming
docker logs -f spark-streaming        # watch for the [ingest] banner; Ctrl-C to stop tailing

# 3. Produce telemetry from the host (in a second terminal)
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
cd data-generator
..\.venv\Scripts\python.exe -m data_generator.main --machines 50 --rate 5 --duration 30 --realtime --sink kafka
cd ..
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS

# 4. Verify Bronze (counts, duplicates, ingest metadata, quarantine)
docker compose exec spark-streaming bash /opt/app/streaming/spark/submit_verify.sh
```

Bronze lands at `s3a://lakehouse/bronze/telemetry`, partitioned by `event_date`.

---

## Stage 4 — Lakehouse: Silver & Gold (Phase 4)

One-shot batch Spark jobs (build Silver first, then Gold).

```powershell
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue

# Bronze → Silver (dedup, DQ gate, MERGE INTO)
docker compose run --rm spark-silver

# Silver → Gold (rolling 5m/1h/24h aggregates + labels)
docker compose run --rm spark-gold

# Verify the lakehouse (TC-4.1 … TC-4.9)
docker compose exec spark-streaming bash -c "python /opt/app/lakehouse/verify_lakehouse.py"
```

Gold lands at `s3a://lakehouse/gold/telemetry_features`, partitioned by
`event_date` + `window_duration`.

---

## Stage 5 — Feature engineering & Feast (Phase 5)

`build_offline_store` reads Gold directly from MinIO over S3 (DuckDB + `delta_scan`), so no
container export is needed in the common case.

```powershell
.\.venv\Scripts\Activate.ps1

# 1. Build the offline feature dataset straight from the Gold Delta table on MinIO
python -m feature_engineering.build_offline_store s3a://lakehouse/gold/telemetry_features

# 2. Apply Feast definitions + materialize the online store
python -m feature_engineering.materialize

# 3. (optional) Check online vs offline parity on a real entity
python -m feature_engineering.check_parity
```

Offline dataset: `feature-engineering/data/offline/telemetry_features.parquet`.

> Fallback when the host can't reach MinIO: export a local Gold parquet snapshot from inside
> the Spark container (the script requires an output directory argument), then build from it:
> ```powershell
> docker compose exec spark-gold bash -c "python /opt/app/lakehouse/../feature-engineering/feature_engineering/export_gold_snapshot.py /tmp/gold_snapshot"
> python -m feature_engineering.build_offline_store feature-engineering/data/raw/gold_snapshot
> ```

> If you don't yet have Gold data, the ML stage falls back to a synthetic feature frame,
> so you can still exercise Stages 6–9.

---

## Stage 6 — ML training (Phase 6)

Local training with the SQLite MLflow backend (no server required).

```powershell
.\.venv\Scripts\Activate.ps1

# Train a single baseline model
.\.venv\Scripts\python.exe -m ml.predictive_maintenance.train
.\.venv\Scripts\python.exe -m ml.anomaly_detection.train
.\.venv\Scripts\python.exe -m ml.battery_health.train
```

---

## Stage 7 — MLOps: tracking, registry, promotion, retraining (Phase 7)

```powershell
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue

# 1. Bring up the tracking server (Postgres backend + MinIO artifacts) and Airflow
docker compose up -d postgres minio createbuckets mlflow   # → http://localhost:5000
docker compose up -d airflow                               # → http://localhost:8081 (admin/admin)

# 2. Train → log → register → gate-promote (point the pipeline at the MLflow server)
.\.venv\Scripts\Activate.ps1
$env:MLFLOW_TRACKING_URI = "http://localhost:5000"
$env:MLFLOW_S3_ENDPOINT_URL = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"

.\.venv\Scripts\python.exe -m ml.pipeline                                  # all tasks
# .\.venv\Scripts\python.exe -m ml.pipeline --task predictive_maintenance  # one task
# .\.venv\Scripts\python.exe -m ml.pipeline --no-promote                   # register + stage only

# 3. Batch score the offline store with the production model
.\.venv\Scripts\python.exe -m ml.inference.batch --task predictive_maintenance

# 4. (optional) Trigger the retraining DAG from Airflow
#    Open http://localhost:8081, enable & trigger the `train_models` DAG.
```

> Registering to MLflow over S3 requires the `MLFLOW_S3_ENDPOINT_URL` + AWS creds above so
> artifacts land under `s3://mlflow/...` (reachable by the Spark alerting container in Stage 8).

---

## Stage 8 — Real-time anomaly alerting (Phase 8)

Requires a `production`-aliased anomaly model trained on **raw telemetry sensor columns**
(Stage 7) and its artifacts in MinIO.

```powershell
# 1. Create the alert topic (host tool)
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
.\.venv\Scripts\python.exe -m streaming.producers.create_topic --topic iot.alerts
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS

# 2. Start the alerting Spark job (scores micro-batches, emits to iot.alerts)
docker compose up -d --build spark-alerting
docker logs -f spark-alerting         # watch for the [alerts] banner + emitted counts

# 3. Produce telemetry so the stream has data to score (see Stage 3 step 3)
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
cd data-generator
..\.venv\Scripts\python.exe -m data_generator.main --machines 50 --rate 5 --duration 60 --realtime --sink kafka
cd ..
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS
```

Alert sinks: Kafka topic `iot.alerts` and Delta table
`s3a://lakehouse/silver/alerts/anomaly_alerts`. Tune `ANOMALY_ALERT_THRESHOLD` /
`ALERTS_COOLDOWN` in `.env` if needed.

---

## Stage 9 — Model serving (Phase 9)

```powershell
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue

# Bring up the FastAPI inference service (warm-loads production models from MLflow)
docker compose up -d --build serving
# Open http://localhost:8000/docs
```

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET  | `/docs` | Interactive Swagger UI (try requests in-browser) |
| GET  | `/health` | Liveness + which models are loaded |
| POST | `/predict/maintenance` | Failure-probability prediction |
| POST | `/predict/battery` | Battery state-of-health prediction |
| POST | `/predict/anomaly` | Anomaly score |
| POST | `/reload` | Re-pull `production`-aliased models after a new promotion |
| GET  | `/metrics` | Prometheus metrics (scraped by the `prometheus` service) |

### Using the service

```powershell
# Smoke test a prediction (same request shape for /battery and /anomaly)
$body = @{
  records = @(
    @{
      machine_id      = "machine-001"
      event_timestamp = "2026-01-01T00:00:00Z"
      features        = @{ motor_temp_mean = 90.0; vibration_mean = 3.0 }
    }
  )
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Method Post -Uri http://localhost:8000/predict/maintenance -ContentType "application/json" -Body $body
Invoke-RestMethod -Method Post -Uri http://localhost:8000/predict/battery     -ContentType "application/json" -Body $body
Invoke-RestMethod -Method Post -Uri http://localhost:8000/predict/anomaly      -ContentType "application/json" -Body $body

# Health & metrics
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/metrics

# Reload cached models after a new promotion (no restart needed)
Invoke-RestMethod -Method Post -Uri http://localhost:8000/reload
```

> Prefer a browser? Open http://localhost:8000/docs, expand an endpoint, click
> **Try it out**, paste the `records` payload, and **Execute**.

---

## Stage 10 — Monitoring & observability (Phase 10)

Operational metrics (Prometheus + Grafana) plus data-drift reports and functional
BI/AI dashboards backed by a Postgres `analytics` serving layer.

```powershell
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue

# 1. Bring up the observability stack (serving provides the app metrics to scrape)
docker compose up -d serving prometheus grafana
#    Prometheus → http://localhost:9090   (Status ▸ Targets shows the serving job)
#    Grafana    → http://localhost:3000   (admin/admin) ▸ Dashboards ▸ "Industrial IoT"
```

### Grafana dashboards

| Dashboard | Datasource | Shows |
|---|---|---|
| Platform Health | Prometheus | Serving up, request/error rate, latency p50/p95 |
| ML Metrics & Drift | Prometheus | Served model versions, prediction scores, drift gauges |
| Fleet Operations (BI) | Postgres `analytics` | Fleet KPIs, vibration/battery trends, machine-health table |
| Predictive Maintenance (AI) | Postgres `analytics` | At-risk leaderboard, failure-probability + anomaly trends |

### Data-drift reports (Evidently)

```powershell
.\.venv\Scripts\Activate.ps1

# Snapshot the current offline feature set as the drift reference (run once / on retrain)
.\.venv\Scripts\python.exe -m monitoring.drift.evidently_reports snapshot

# Generate a drift report (writes drift_summary.json + drift_report.html; exits 1 on drift)
.\.venv\Scripts\python.exe -m monitoring.drift.evidently_reports report
#    → monitoring/drift/reports/drift_report.html
```

### BI/AI analytics export (Gold + predictions → Postgres)

Populate the `analytics` schema that the two functional dashboards read from.

```powershell
# Prereqs: minio + postgres up, Gold built (Stage 4), predictions written (Stage 7 step 3)
docker compose up -d minio postgres

.\.venv\Scripts\Activate.ps1
$env:MINIO_ENDPOINT      = "http://localhost:9000"
$env:ANALYTICS_PG_HOST   = "localhost"
.\.venv\Scripts\python.exe -m monitoring.analytics.export_bi            # writes Postgres tables
# .\.venv\Scripts\python.exe -m monitoring.analytics.export_bi --dry-run  # just print row counts
Remove-Item Env:\MINIO_ENDPOINT, Env:\ANALYTICS_PG_HOST -ErrorAction SilentlyContinue
```

Tables written to schema `analytics`: `fleet_kpis`, `machine_health`, `gold_features`,
`predictions`, `predictions_latest`. Missing Gold/predictions yield empty tables rather
than failing.

> **Scheduled alternative:** the Airflow DAGs `drift_report` (daily drift check that
> triggers retraining on drift) and `bi_export` (daily analytics refresh) run these same
> steps inside the network. Enable them at http://localhost:8081.

---

## Accessing lakehouse data (Bronze / Silver / Gold)

The lakehouse is Delta tables on MinIO under `s3://lakehouse/{bronze,silver,gold}/...`.
Query them several ways:

### From the CLI — DuckDB + `delta_scan` (host)

```powershell
.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe -c @"
import duckdb
con = duckdb.connect()
con.execute(\"INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;\")
con.execute(\"SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SET s3_url_style='path';\")
con.execute(\"SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin';\")
df = con.execute(\"SELECT machine_id, window_duration, failure_label, vibration_mean \"
                 \"FROM delta_scan('s3://lakehouse/gold/telemetry_features') LIMIT 20\").df()
print(df)
"@
```

> Swap the path for `s3://lakehouse/silver/telemetry` or `s3://lakehouse/bronze/telemetry`.
> The repo also ships a smoke script: `python -m lakehouse.query.duckdb_smoke`.

### From the CLI — Spark (in-container)

```powershell
# Full lakehouse verification (row counts, schema, DQ) across all layers
docker compose exec spark-streaming bash -c "python /opt/app/lakehouse/verify_lakehouse.py"
```

### From the UI — MinIO console

Open http://localhost:9001 (minioadmin / minioadmin) → bucket **`lakehouse`** → browse
`bronze/`, `silver/`, `gold/` to inspect Delta files, partitions, and `_delta_log/`.

### From the UI — Grafana (curated Gold via Postgres)

After running the BI export (Stage 10), the **Fleet Operations (BI)** dashboard at
http://localhost:3000 visualizes Gold KPIs and per-machine health directly.

### Optional — Trino SQL engine

A `trino` service is scaffolded (commented out) in `docker-compose.yml`. Uncomment it and
`docker compose up -d trino` to expose a SQL gateway at http://localhost:8082 for ad-hoc
queries over the Delta tables.

---

## Running the unit tests

```powershell
.\.venv\Scripts\Activate.ps1
.\.venv\Scripts\python.exe -m pytest data-generator/tests -q
.\.venv\Scripts\python.exe -m pytest streaming/tests -q
.\.venv\Scripts\python.exe -m pytest lakehouse/tests -q
.\.venv\Scripts\python.exe -m pytest feature-engineering/tests -q
.\.venv\Scripts\python.exe -m pytest ml/tests -q
.\.venv\Scripts\python.exe -m pytest serving/tests -q
.\.venv\Scripts\python.exe -m pytest monitoring/tests -q
```

---

## End-to-end run (all stages in order)

```powershell
# ── 0. Setup (once) ─────────────────────────────────────────────
.\.venv\Scripts\Activate.ps1
Copy-Item .env.example .env -ErrorAction SilentlyContinue
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue

# ── 1. Infrastructure ───────────────────────────────────────────
docker compose up -d kafka kafka-ui minio createbuckets postgres mlflow
docker compose ps                      # wait for kafka + minio healthy

# ── 2/3. Topic + streaming ingest ───────────────────────────────
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
.\.venv\Scripts\python.exe -m streaming.producers.create_topic
.\.venv\Scripts\python.exe -m streaming.producers.create_topic --topic iot.alerts
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS

docker compose up -d --build spark-streaming

# Produce a burst of telemetry
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
cd data-generator
..\.venv\Scripts\python.exe -m data_generator.main --machines 50 --rate 5 --duration 60 --realtime --sink kafka
cd ..
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS

docker compose exec spark-streaming bash /opt/app/streaming/spark/submit_verify.sh

# ── 4. Lakehouse Silver + Gold ──────────────────────────────────
docker compose run --rm spark-silver
docker compose run --rm spark-gold

# ── 5. Features ─────────────────────────────────────────────────
python -m feature_engineering.build_offline_store s3a://lakehouse/gold/telemetry_features
python -m feature_engineering.materialize

# ── 6/7. Train + register + promote ─────────────────────────────
docker compose up -d airflow
$env:MLFLOW_TRACKING_URI = "http://localhost:5000"
$env:MLFLOW_S3_ENDPOINT_URL = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"
.\.venv\Scripts\python.exe -m ml.pipeline

# ── 8. Real-time alerting ───────────────────────────────────────
docker compose up -d --build spark-alerting

# ── 9. Serving ──────────────────────────────────────────────────
docker compose up -d --build serving
# http://localhost:8000/docs

# ── 10. Monitoring + BI/AI dashboards ───────────────────────────
docker compose up -d prometheus grafana                 # http://localhost:9090 / :3000
$env:MINIO_ENDPOINT = "http://localhost:9000"; $env:ANALYTICS_PG_HOST = "localhost"
.\.venv\Scripts\python.exe -m monitoring.analytics.export_bi   # populate Grafana BI tables
Remove-Item Env:\MINIO_ENDPOINT, Env:\ANALYTICS_PG_HOST -ErrorAction SilentlyContinue
# Grafana ▸ Dashboards ▸ "Industrial IoT"
```

---

## Tear down

```powershell
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS -ErrorAction SilentlyContinue

docker compose down                    # stop & remove containers (keep volumes)
docker compose down -v                 # also remove volumes (wipes MinIO + Postgres data)
```
