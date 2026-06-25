---
name: lakehouse-medallion
description: 'Build the Bronze/Silver/Gold medallion lakehouse on Delta Lake + MinIO (Phase 4). Use when designing layer transformations, partitioning, schema evolution, data quality checks, deduplication, or Trino/DuckDB query access.'
argument-hint: 'e.g. "build Silver cleansing job" or "add DQ checks to Gold"'
---

# Lakehouse — Medallion Architecture

## When to use
- Implementing anything under `lakehouse/` (bronze/silver/gold).
- Defining partitioning, schema evolution, or data quality gates.

## Layer contract
| Layer | Guarantees | Transforms |
|---|---|---|
| 🥉 Bronze | Raw, append-only, immutable | None (just ingest metadata) |
| 🥈 Silver | Typed, deduped, validated | Cast types, drop dupes, enforce schema, DQ |
| 🥇 Gold | Business/ML-ready | Window aggregates, features, labels |

## Procedure
1. **Silver**: read Bronze incrementally (Delta CDF or `_ingest_ts` watermark) → cast types, dedup on (`machine_id`, `ts`), drop nulls in required cols, enforce ranges.
2. Apply **data quality checks**: null-rate thresholds, value ranges (e.g. `battery_soh ∈ [0,1]`), freshness; fail or quarantine on violation.
3. Partition Silver/Gold by `date` (and bucket by `machine_id` for large fleets).
4. **Gold**: time-window aggregations (5m/1h/24h rolling stats), error-code counts, join failure labels.
5. Enable **schema evolution** with `.option("mergeSchema", "true")` for additive changes; document breaking changes.
6. Register tables for **Trino/DuckDB**: expose Gold as queryable tables; validate with sample SQL.

## Guardrails
- Never mutate Bronze; corrections happen in Silver.
- Use `OPTIMIZE`/`VACUUM` (Delta) to manage small files and history.
- Keep DQ rules declarative and testable; log results as metrics.
