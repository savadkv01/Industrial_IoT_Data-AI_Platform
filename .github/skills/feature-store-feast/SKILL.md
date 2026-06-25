---
name: feature-store-feast
description: 'Build time-series features and the Feast feature store (Phase 5). Use when defining entities, feature views, data sources, window/lag/rolling features, online/offline materialization, or point-in-time correct training datasets.'
argument-hint: 'e.g. "add rolling temp features" or "materialize features online"'
---

# Feature Store (Feast)

## When to use
- Implementing anything under `feature-engineering/`.
- Defining features, Feast objects, or materialization jobs.

## Feature design
- **Window features**: rolling mean/std/min/max over 5m/1h/24h per sensor.
- **Lag features**: previous N readings (autocorrelation signal).
- **Rate-of-change**: first derivative of temperature, battery, vibration.
- **Counts**: error-code frequency, uptime ratio over window.

## Procedure
1. Build features from Gold tables (Spark/pandas) and store them as a Feast offline source (parquet/Delta on MinIO).
2. Define Feast objects:
   - `entities.py`: `machine` entity keyed by `machine_id`.
   - `data_sources.py`: point at Gold feature tables with an event timestamp column.
   - `features.py`: `FeatureView`s grouping related features with a TTL.
3. `feast apply` to register definitions.
4. `feast materialize` (or `materialize-incremental`) to load the online store.
5. **Point-in-time correctness**: build training sets with `get_historical_features` using an entity dataframe with label timestamps — never leak future data.
6. Serve online features to FastAPI via `get_online_features`.

## Guardrails
- Event timestamps must be the telemetry time, not ingestion time.
- Set TTLs so stale features are not served.
- Keep feature transforms deterministic and unit-tested.
