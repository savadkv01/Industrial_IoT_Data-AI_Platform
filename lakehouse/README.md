# Lakehouse — Phase 4

Medallion architecture (Bronze → Silver → Gold) on Delta Lake stored in MinIO.

## Layers
| Layer | Purpose | Contents |
|---|---|---|
| 🥉 Bronze | Raw, append-only | Exact Kafka payloads + ingestion metadata (built in Phase 3) |
| 🥈 Silver | Cleaned, conformed | Validated, deduplicated, typed telemetry |
| 🥇 Gold | Business / ML-ready | Rolling aggregations, features, labels |

## Package layout
```
lakehouse/
├── config.py             # LakehouseConfig: S3A paths + DQ thresholds (env-driven)
├── dq.py                 # Declarative DQ rules + single-pass run_dq()
├── silver/
│   ├── build_silver.py   # Bronze→Silver: dedup, DQ gate, MERGE INTO Silver
│   └── submit_silver.sh  # spark-submit wrapper (Delta + Hadoop-AWS jars)
├── gold/
│   ├── __init__.py       # WINDOW_SPECS (5m/1h/24h)
│   ├── build_gold.py     # Silver→Gold: rolling window aggregates + labels
│   └── submit_gold.sh    # spark-submit wrapper
├── query/
│   └── duckdb_smoke.py   # DuckDB delta_scan smoke test (TC-4.8)
├── verify_lakehouse.py   # Batch verifier for TC-4.1 … TC-4.9
└── tests/                # Pure-Python DQ + schema-parity unit tests
```

## Silver job (`silver/build_silver.py`)
- Reads the full Bronze Delta table and drops Kafka-internal metadata.
- **Deduplicates** to one row per (`machine_id`, `ts`), keeping the latest `_ingest_ts`.
- Applies **declarative DQ rules** (`dq.py`): non-null identity columns and range checks
  (`battery_soh ∈ [0,1]`, `cpu_usage ∈ [0,100]`, `lat/lon`, `speed ≥ 0`, `vibration ≥ 0`).
- Violations are routed to a **Silver quarantine** table (never silently dropped).
- Writes valid rows with **`MERGE INTO`** (idempotent re-runs), partitioned by `event_date`,
  with `mergeSchema=true` for additive schema evolution. Bronze is never mutated.
- The DQ gate fails the batch when null-rate / range-violation thresholds are breached.

## Gold job (`gold/build_gold.py`)
- Computes per-machine rolling statistics at three granularities (5m / 1h / 24h):
  mean/std/max for vibration & motor temp, mean/std for CPU, mean/min for battery SoH,
  error-code counts, record counts.
- Joins the predictive-maintenance label (`failure_label`) per window.
- Writes a single Gold table partitioned by `event_date` + `window_duration`
  (dynamic partition overwrite for idempotent recompute).

## Configuration (env)
| Variable | Default | Purpose |
|---|---|---|
| `MINIO_ENDPOINT` | `http://minio:9000` | S3A endpoint |
| `S3_BUCKET_LAKEHOUSE` | `lakehouse` | Lakehouse bucket |
| `DQ_MAX_NULL_RATE` | `0.05` | Null-rate gate threshold |
| `DQ_MAX_RANGE_VIOLATION_RATE` | `0.01` | Range-violation gate threshold |
| `SPARK_SHUFFLE_PARTITIONS` | `8` | Shuffle parallelism |

Table locations: `s3a://<bucket>/silver/telemetry`, `s3a://<bucket>/silver/_quarantine/telemetry`,
`s3a://<bucket>/gold/telemetry_features`.

## Running
```bash
# Bring up core services (Phase 3-4)
docker compose up -d kafka minio createbuckets

# Build Silver, then Gold (one-shot batch services)
docker compose run --rm spark-silver
docker compose run --rm spark-gold
```

## Tests
```bash
pip install -e "lakehouse[dev]"
pytest lakehouse/tests        # pure-Python DQ + schema-parity (no Spark required)
```

## Key concepts
- Partitioning: `event_date` (+ `window_duration` for Gold).
- Schema evolution via Delta `mergeSchema`.
- Declarative, unit-tested data quality (null rates, range checks).
- Quarantine tables keep bad rows for inspection rather than dropping them.

See the `lakehouse-medallion` agent skill for the build procedure.
