---
name: streaming-pipeline
description: 'Build Kafka producers and Spark Structured Streaming jobs (Phases 3 & 8). Use when working on Kafka topics/partitioning, Spark streaming ingestion into Bronze Delta, exactly-once semantics, checkpointing, watermarking, or real-time anomaly detection on the stream.'
argument-hint: 'e.g. "ingest Kafka to Bronze Delta" or "add watermark + dedup"'
---

# Streaming Pipeline

## When to use
- Implementing anything under `streaming/`.
- Wiring Kafka → Spark → Delta, or streaming model inference (Phase 8).

## Core concepts
- **Partitioning**: key Kafka messages by `machine_id` to preserve per-machine ordering; size partitions to target throughput (~10k msg/s).
- **Exactly-once**: Spark checkpoint directory + Delta idempotent writes (`txnVersion`/`foreachBatch` with deterministic batch IDs).
- **Watermarking**: handle late telemetry with `withWatermark("ts", "2 minutes")`.
- **Schema**: parse JSON with an explicit schema; reject/quarantine malformed records.

## Procedure (ingest to Bronze)
1. Read stream: `spark.readStream.format("kafka")` with `subscribe`, `startingOffsets`, `maxOffsetsPerTrigger`.
2. Deserialize value JSON using the telemetry schema; add ingestion metadata (`_ingest_ts`, `_topic`, `_partition`, `_offset`).
3. Write stream: `.writeStream.format("delta")` to `s3a://lakehouse/bronze/telemetry` with `checkpointLocation` and `trigger(processingTime=...)`.
4. Configure S3A for MinIO (endpoint, path-style access, credentials).
5. Validate exactly-once by killing/restarting the job and confirming no duplicates in Bronze.

## Procedure (real-time anomaly — Phase 8)
1. In `foreachBatch`, load the anomaly model (broadcast/cache) and score the micro-batch.
2. Filter anomalies above threshold; write to an `iot.alerts` Kafka topic and an alerts Delta table.

## Guardrails
- Never use `complete` output mode for append-only Bronze.
- Always set `maxOffsetsPerTrigger` to bound batch size and avoid lag spikes.
- Keep checkpoint locations stable per job; deleting them breaks exactly-once.
