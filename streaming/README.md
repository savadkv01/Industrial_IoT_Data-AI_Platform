# Streaming Pipeline — Phase 3

Real-time ingestion: synthetic telemetry → **Kafka** → **Spark Structured Streaming** →
**Bronze Delta** table on MinIO, with exactly-once semantics, schema enforcement, and a
malformed-record quarantine.

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
