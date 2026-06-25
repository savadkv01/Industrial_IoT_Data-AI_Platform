# Streaming Pipeline — Phase 3

Kafka producers and Spark Structured Streaming ingestion with exactly-once semantics.

## Planned layout
```
streaming/
├── producers/         # Kafka producer wrappers, partitioning strategy
├── spark/             # Structured Streaming jobs (Kafka -> Bronze Delta)
│   └── ingest_bronze.py
└── config/            # checkpoint locations, topic configs
```

## Key concepts
- Partition by `machine_id` for ordering guarantees.
- Checkpointing + idempotent writes for exactly-once.
- Watermarking for late-arriving telemetry.

See the `streaming-pipeline` agent skill for the build procedure.
