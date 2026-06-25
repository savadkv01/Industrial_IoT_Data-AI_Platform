# ML & MLOps — Phases 6–7

Model training pipelines and MLOps with MLflow tracking and registry.

## Models
| Use case | Type | Metric |
|---|---|---|
| Predictive maintenance | XGBoost classifier / survival | AUC, F1 |
| Anomaly detection | Isolation Forest / Autoencoder | precision@k |
| Battery health | Regressor | RMSE, MAE |
| Fleet optimization | KMeans clustering | silhouette |

## Planned layout
```
ml/
├── predictive_maintenance/
├── anomaly_detection/
├── battery_health/
├── common/            # data loaders, metrics, validation splits
└── pipelines/         # training entrypoints logging to MLflow
```

## Key concepts
- Time-aware train/validation/test splits (no leakage).
- MLflow experiment tracking + model registry stages.
- Reproducible runs via params + artifact logging.

See the `mlops-pipeline` agent skill for the build procedure.
