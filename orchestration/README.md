# Orchestration — Airflow

DAGs coordinating batch processing, feature materialization, and model training.

## Planned DAGs
- `bronze_to_silver` — scheduled cleansing + DQ.
- `silver_to_gold` — aggregations & feature tables.
- `feast_materialize` — push features to the online store.
- `train_models` — retrain + register models in MLflow.
- `drift_report` — Evidently drift reports to Grafana.

## Planned layout
```
orchestration/
└── dags/
    ├── bronze_to_silver.py
    ├── silver_to_gold.py
    ├── train_models.py
    └── drift_report.py
```
