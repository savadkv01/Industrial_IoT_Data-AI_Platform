# Lakehouse — Phase 4

Medallion architecture (Bronze → Silver → Gold) on Delta Lake stored in MinIO.

## Layers
| Layer | Purpose | Contents |
|---|---|---|
| 🥉 Bronze | Raw, append-only | Exact Kafka payloads + ingestion metadata |
| 🥈 Silver | Cleaned, conformed | Validated, deduplicated, typed telemetry |
| 🥇 Gold | Business / ML-ready | Aggregations, features, labels |

## Planned layout
```
lakehouse/
├── bronze/   # raw ingestion jobs
├── silver/   # cleansing, dedup, schema enforcement, DQ checks
└── gold/     # aggregations & ML feature tables
```

## Key concepts
- Partitioning: `date` + `machine_id` bucketing.
- Schema evolution via Delta `mergeSchema`.
- Data quality checks (null rates, range checks, freshness).

See the `lakehouse-medallion` agent skill for the build procedure.
