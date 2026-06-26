# Feature Engineering — Phase 5

Time-series feature engineering and a Feast feature store built on the Phase 4 Gold table.

## Feature families
- **Window features**: rolling mean/std/min/max over 5m/1h/24h (pivoted from Gold).
- **Lag features**: previous N readings per sensor (lag-1, lag-3).
- **Rate-of-change**: first derivative vs. the lag-1 baseline.
- **Aggregations**: error-code counts, uptime ratio.

## Package layout
```
feature-engineering/
├── feature_engineering/
│   ├── config.py                 # FeatureEngineeringConfig (paths, Gold source)
│   ├── transforms.py             # Gold→wide feature dataset, lag/roc/uptime, point-in-time join
│   ├── build_offline_store.py    # Build Feast offline parquet from Gold (Delta/parquet)
│   ├── export_gold_snapshot.py   # Spark helper: export Gold Delta to local parquet
│   ├── feast_repo.py             # FeatureStore loader
│   ├── materialize.py            # feast apply + materialize (online store)
│   ├── historical_features.py    # point-in-time get_historical_features helper
│   └── check_parity.py           # online vs offline parity check on a real entity
├── feast/
│   ├── feature_store.yaml        # local provider, sqlite online store, file offline store
│   ├── entities.py               # machine entity (machine_id)
│   ├── data_sources.py           # FileSource over the offline parquet (event_timestamp)
│   ├── features.py               # telemetry_window_features + telemetry_temporal_features
│   └── feature_repo.py           # FEAST_OBJECTS for explicit apply()
└── tests/                        # pure-Python transform / offline-store / PIT tests
```

## Feature views
- `telemetry_window_features` (TTL 1 day): pivoted 5m/1h/24h window stats + label.
- `telemetry_temporal_features` (TTL 30 min): lag, rate-of-change, and uptime features.

Event time is the Gold `window_end` (telemetry event time, not ingestion time), so
`get_historical_features` is point-in-time correct.

## Configuration (env)
| Variable | Default | Purpose |
|---|---|---|
| `FEATURE_GOLD_SOURCE_URI` | _(empty)_ | Override Gold source; defaults to Phase 4 Gold Delta path |
| `FEATURE_OFFLINE_DIR` | `data/offline` | Offline dataset directory |
| `FEATURE_OFFLINE_FILE` | `telemetry_features.parquet` | Offline dataset file |
| `FEATURE_FEAST_REGISTRY` | `feast/data/registry.db` | Feast registry |
| `FEATURE_FEAST_ONLINE_STORE` | `feast/data/online.db` | Feast online store |

## Running
```bash
pip install -e "feature-engineering[dev]"

# 1. Build the offline feature dataset from a Gold Delta directory or parquet snapshot
python -m feature_engineering.build_offline_store <gold-delta-or-parquet-path>

# 2. Apply Feast definitions and materialize the online store
python -m feature_engineering.materialize

# 3. (optional) Check online/offline parity on a real entity
python -m feature_engineering.check_parity
```

> When DuckDB on the host cannot reach MinIO directly, export Gold from inside the Spark
> container first (`export_gold_snapshot.py` or `mc cp`) and build from the local snapshot.

## Tests
```bash
pytest feature-engineering/tests   # transforms, offline store, point-in-time (no Spark)
```

See the `feature-store-feast` agent skill for the build procedure.
