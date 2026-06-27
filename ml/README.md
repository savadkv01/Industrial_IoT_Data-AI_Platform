# ML & MLOps — Phases 6–7

Model training pipelines with **time-aware (leak-free) validation** and MLflow tracking.
Phase 6 delivers three baseline model families; Phase 7 adds the registry, retraining
DAG, and batch/real-time inference.

## Models (Phase 6 — implemented)
| Use case | Module | Model | Metrics |
|---|---|---|---|
| Predictive maintenance | `ml.predictive_maintenance.train` | XGBoost classifier | AUC, PR-AUC, F1 |
| Anomaly detection | `ml.anomaly_detection.train` | Isolation Forest (unsupervised) | precision@k, score AUC |
| Battery health | `ml.battery_health.train` | Gradient boosting regressor | RMSE, MAE, R² |

## MLOps (Phase 7 — implemented)
| Concern | Module | Notes |
|---|---|---|
| Train → log → register → promote | `ml.pipeline` | one code path for CLI **and** the Airflow DAG |
| Model registry + gated promotion | `ml.common.registry` | alias-based (`staging`/`production`); promote only if it clears the gate **and** beats the incumbent |
| Task specs (metric, flavor, scoring) | `ml.common.tasks` | shared by training and serving so they never drift |
| Batch inference | `ml.inference.batch` | loads a model by registry alias, scores the offline store |
| Retraining DAG | `orchestration/dags/train_models.py` | daily retrain + batch score per model family |

## Layout
```
ml/
├── config.py                 # MLConfig — feature source, MLflow URI, registry, gates
├── common/
│   ├── data.py               # load offline parquet / synthetic fallback, feature selection
│   ├── splits.py             # time-aware train/val/test splits (no shuffling)
│   ├── metrics.py            # classification / ranking / regression metrics
│   ├── synthetic.py          # stationary synthetic features for tests & smoke runs
│   ├── tracking.py           # MLflow run helper + flavor-dispatching log_model
│   ├── registry.py           # register versions + metric-gated alias promotion (Phase 7)
│   └── tasks.py              # per-task spec: metric, flavor, scoring, feature selection
├── pipeline.py               # train → log → register → promote (CLI + DAG entrypoint)
├── inference/batch.py        # batch scoring from the registry (Phase 7)
├── predictive_maintenance/train.py
├── anomaly_detection/train.py
├── battery_health/train.py
└── tests/                    # split no-leakage, metrics, training, registry, pipeline
```

## Key concepts
- **Time-aware splits** (`ml.common.splits`): the test slice is always the most recent
  window by `event_timestamp`; train precedes val precedes test. No random shuffling →
  no future leakage. Enforced by `tests/test_splits.py`.
- **Feature source**: defaults to the Phase 5 Feast offline parquet
  (`feature-engineering/data/offline/telemetry_features.parquet`). If absent, a synthetic
  generator provides a same-shaped frame so pipelines run end-to-end.
- **Leakage guards**: supervised models drop null-label rows; the battery regressor
  excludes all `battery_soh_*` columns from its inputs.
- **MLflow**: each run logs params, metrics, the time-split cutoffs, and the model
  artifact. Locally this uses a SQLite backend (`ml/mlflow.db`) with artifacts under
  `ml/mlartifacts/`. Set `MLFLOW_TRACKING_URI` to use the MLflow server (Phase 7).
- **Gated promotion** (`ml.common.registry`): a new version is always tagged `staging`;
  it is promoted to the `production` alias only when it clears the task's absolute gate
  (`ML_PDM_MIN_AUC`, `ML_ANOMALY_MIN_SCORE_AUC`, `ML_BATTERY_MAX_RMSE`) **and** beats the
  current production model on the primary metric. A noisy run can never displace a good
  incumbent.

## Run
```powershell
# from the repo root, using the project venv
.\.venv\Scripts\python.exe -m ml.predictive_maintenance.train   # Phase 6 single model

# Phase 7: train, log, register and gate-promote (all tasks, or pick one)
.\.venv\Scripts\python.exe -m ml.pipeline
.\.venv\Scripts\python.exe -m ml.pipeline --task predictive_maintenance
.\.venv\Scripts\python.exe -m ml.pipeline --no-promote          # register + stage only

# Phase 7: batch score the offline store with the production model
.\.venv\Scripts\python.exe -m ml.inference.batch --task predictive_maintenance
```
> Baseline metrics are `NaN` until enough Gold data accumulates for a meaningful time
> split (the bundled smoke sample has too few timestamps / failures). The unit tests use
> synthetic data to verify the models learn signal, the splits are leak-free, and the
> registry/promotion logic behaves.

## MLflow server + Airflow (Phase 7 services)
```powershell
docker compose up -d postgres minio createbuckets mlflow   # tracking server → http://localhost:5000
docker compose up -d airflow                               # retraining DAG → http://localhost:8081
```
The MLflow server uses a Postgres backend store (required for the registry) and MinIO
(S3) artifact store. Airflow runs `train_models` daily, reusing `ml.pipeline.run_pipeline`.

## Test
```powershell
cd ml ; ..\.venv\Scripts\python.exe -m pytest -q
```

See the `mlops-pipeline` agent skill for the full build procedure, and
[docs/DATA_MODELING.md](../docs/DATA_MODELING.md) for the AI feature-model rationale.
