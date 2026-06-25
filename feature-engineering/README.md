# Feature Engineering — Phase 5

Time-series feature engineering and a Feast feature store.

## Feature families
- **Window features**: rolling mean/std/min/max over 5m/1h/24h.
- **Lag features**: previous N readings per sensor.
- **Rate-of-change**: derivative of temperature, battery, vibration.
- **Aggregations**: error-code counts, uptime ratios.

## Planned layout
```
feature-engineering/
├── feast/
│   ├── feature_store.yaml
│   ├── entities.py        # machine entity
│   ├── features.py        # feature views
│   └── data_sources.py    # Gold tables as sources
└── transforms/            # Spark/pandas feature builders
```

See the `feature-store-feast` agent skill for the build procedure.
