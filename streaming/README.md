# Streaming Pipeline — Phases 3 and 8

Real-time ingestion: synthetic telemetry → **Kafka** → **Spark Structured Streaming** →
**Bronze Delta** table on MinIO, with exactly-once semantics, schema enforcement, and a
malformed-record quarantine.

Phase 8 adds a second Spark job that consumes the same Kafka telemetry topic, loads the
registered anomaly model from MLflow, and emits deduplicated alerts to **`iot.alerts`**
plus a Delta alert table.

```
Generator (host) ──▶ Kafka topic iot.telemetry ──▶ Spark Structured Streaming ──▶ s3a://lakehouse/bronze/telemetry
   localhost:9094         6 partitions                  (checkpointed, exactly-once)        partitioned by event_date
                          keyed by machine_id
```

## Layout
```
streaming/
├── config.py                  # StreamingConfig (env-driven): brokers, topic, paths
├── fields.py                  # TELEMETRY_FIELDS — schema source of truth (no pyspark dep)
├── producers/
│   └── create_topic.py        # idempotent topic admin (6 partitions, machine_id key)
├── spark/
│   ├── schema.py              # builds StructType from fields.py
│   ├── ingest_bronze.py       # Kafka → parse/enrich → Bronze (foreachBatch + quarantine)
│   ├── submit_ingest.sh       # spark-submit wrapper (Delta + S3A packages)
│   ├── verify_bronze.py       # batch verification (counts, duplicates, metadata, quarantine)
│   └── submit_verify.sh       # spark-submit wrapper for verification
└── tests/
  ├── test_anomaly_alerts.py # pure-Python scoring + cooldown tests for Phase 8
  └── test_field_parity.py   # schema parity with the generator's TelemetryRecord
```

## Key design points
- **Partition by `machine_id`** — Kafka key = machine_id, so all events for a machine land on
  one partition (per-machine ordering). The Bronze table is partitioned by `event_date`.
- **Exactly-once** — Spark checkpoint (`s3a://lakehouse/_checkpoints/bronze_telemetry`) tracks
  committed Kafka offsets; replays/restarts do not duplicate rows.
- **Schema enforcement** — payloads are parsed against [fields.py](fields.py); rows whose
  `machine_id` is null (unparseable / malformed) are routed to a quarantine path instead of
  failing the batch.
- **Watermarking** — a 2-minute watermark on event time tolerates late-arriving telemetry.
- **Ingest metadata** — every Bronze row carries `_ingest_ts`, `_topic`, `_partition`,
  `_offset`, and a derived `event_date` for lineage and partitioning.

## Prerequisites
Bring up the Phase 3 services (Kafka, Kafka UI, MinIO, bucket init):
```powershell
docker compose up -d kafka kafka-ui minio createbuckets
```
- Kafka UI: http://localhost:8080  · MinIO console: http://localhost:9001 (minioadmin / minioadmin)
- **Host tools** (generator, topic admin) use `localhost:9094`.
- **In-container Spark** uses `kafka:9092` (set in the compose service).

## Run it

### 1. Create the topic (host, venv)
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
.\.venv\Scripts\python.exe -m streaming.producers.create_topic
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS   # clear before any docker compose command
```

### 2. Start the streaming ingest job (Spark container)
```powershell
docker compose up -d --build spark-streaming
docker logs -f spark-streaming        # watch for the [ingest] banner + batch progress
```
The job runs continuously (`awaitTermination`); the first run resolves Spark packages
(cached in the `ivy-cache` volume for subsequent runs).

### 3. Produce telemetry (host, venv)
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
cd data-generator
..\.venv\Scripts\python.exe -m data_generator.main --machines 50 --rate 5 --duration 5 --realtime --sink kafka
cd ..
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS
```

### 4. Verify Bronze (batch job)
```powershell
docker compose exec spark-streaming bash /opt/app/streaming/spark/submit_verify.sh
```
Expected output:
```
  total rows                : <N produced>
  distinct (machine_id, ts) : <N>     # equals total rows
  duplicate rows            : 0
  ingest metadata complete  : True
  quarantined (malformed)   : 0
```

> ⚠️ **Compose env gotcha:** the host shell sets `KAFKA_BOOTSTRAP_SERVERS=localhost:9094`
> for host-side tools. Always `Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS` before running any
> `docker compose` command, otherwise the value leaks into the Spark container via variable
> substitution and the job can't reach the broker.

## Phase 8 — Real-time anomaly alerting

### 1. Ensure the anomaly model is registered in MLflow
Run the Phase 7 pipeline first so the `production` alias exists for the anomaly model.

### 2. Create the alert topic (host, venv)
```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
.\.venv\Scripts\python.exe -m streaming.producers.create_topic --topic iot.alerts
Remove-Item Env:\KAFKA_BOOTSTRAP_SERVERS
```

### 3. Start the alerting job
```powershell
docker compose up -d --build spark-alerting
docker logs -f spark-alerting         # watch for the [alerts] banner + emitted counts
```

### 4. Tune threshold / cooldown when needed
The job reads these environment variables from Compose:
- `ANOMALY_ALERT_THRESHOLD` — minimum anomaly score required to emit an alert.
- `ALERTS_COOLDOWN` — per-machine suppression window to avoid alert storms.
- `ANOMALY_MODEL_ALIAS` — registry alias reloaded on every micro-batch for hot-swaps.

### 5. Alert sinks
- Kafka topic: `iot.alerts`
- Delta table: `s3a://lakehouse/silver/alerts/anomaly_alerts`

### Deployment notes
The `spark-alerting` container loads the model **from the MLflow registry over S3 (MinIO)**,
so a few things must line up or the streaming query fails on the first batch:
- **Model artifacts must live in MinIO**, not a local `file://` path. Train/register the model
  with `MLFLOW_S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` set so the
  artifacts land under `s3://mlflow/...`. A model logged to a host `file:///C:/...` path is
  unreachable from inside the container.
- **The Spark image bundles `boto3`** (see [../infra/docker/spark/Dockerfile](../infra/docker/spark/Dockerfile))
  — MLflow's S3 artifact repo imports it lazily; without it the model download raises
  `ModuleNotFoundError: No module named 'botocore'`.
- **The `spark-alerting` service sets the MinIO/S3 credentials** (`MLFLOW_S3_ENDPOINT_URL`,
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) so it can pull artifacts.
- **MLflow 3.x serves the model via the LoggedModel store** (`models:/m-<id>`);
  [../ml/inference/batch.py](../ml/inference/batch.py) resolves the version's `source` URI
  directly for this case and falls back to `runs:/<run_id>/model` for older registrations.
- The model must be trained on the **raw telemetry sensor columns** the stream actually carries
  (`lat`, `lon`, `speed`, `accel_*`, `vibration`, `battery_soh`, `motor_temp`, `cpu_usage`).
  A model trained only on offline feature-store aggregations (`*_mean_1h`, `*_min_24h`, …) will
  see all-NaN inputs in the stream and never cross the alert threshold.

## Tests
```powershell
.\.venv\Scripts\python.exe -m pytest streaming/tests -q
```
Field-parity tests assert the streaming schema stays in sync with the generator's
`TelemetryRecord`.

## Functional acceptance
See the Phase 3 cases in [../docs/TEST_CASES.md](../docs/TEST_CASES.md)
(TC-3.1 no data loss, TC-3.2 exactly-once, TC-3.7 ingest metadata, partitioning by machine_id).

See the `streaming-pipeline` agent skill for the build procedure.
