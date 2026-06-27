# Orchestration — Airflow

DAGs coordinating batch processing, feature materialization, and model training.

## Implemented
- `train_models` (`dags/train_models.py`) — **Phase 7**. Daily retrain of all three model
  families via `ml.pipeline.run_pipeline`: each task registers a new model version,
  gate-promotes it to the `production` alias only when it beats the incumbent, then batch
  scores the offline feature store with the freshly promoted model.

## Planned DAGs
- `bronze_to_silver` — scheduled cleansing + DQ.
- `silver_to_gold` — aggregations & feature tables.
- `feast_materialize` — push features to the online store.
- `drift_report` — Evidently drift reports to Grafana.

## Layout
```
orchestration/
└── dags/
    ├── train_models.py        # implemented (Phase 7)
    ├── bronze_to_silver.py    # planned
    ├── silver_to_gold.py      # planned
    └── drift_report.py        # planned
```

## Run locally
```powershell
docker compose up -d postgres minio createbuckets mlflow   # registry backend + tracking
docker compose up -d airflow                               # http://localhost:8081 (admin/admin)
```
The Airflow image bundles the platform ML deps and mounts `ml/` + `feature-engineering/`
read-only so DAG tasks import `ml.pipeline` directly. `MLFLOW_TRACKING_URI` points the
workers at the MLflow server, so a scheduled retrain writes to the shared registry.
