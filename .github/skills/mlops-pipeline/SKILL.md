---
name: mlops-pipeline
description: 'Build ML training pipelines and MLOps with MLflow (Phases 6-7). Use when training predictive maintenance, anomaly detection, or battery health models; setting up MLflow tracking/registry; time-aware validation splits; metrics (AUC/F1/RMSE); model staging; or retraining DAGs.'
argument-hint: 'e.g. "train XGBoost maintenance model" or "register model in MLflow"'
---

# MLOps Pipeline (MLflow)

## When to use
- Implementing anything under `ml/`.
- Training, tracking, validating, or registering models.

## Models & metrics
| Use case | Model | Primary metric |
|---|---|---|
| Predictive maintenance | XGBoost classifier / survival | AUC, F1, PR-AUC |
| Anomaly detection | Isolation Forest / Autoencoder | precision@k, recall |
| Battery health | Gradient boosting regressor | RMSE, MAE |
| Fleet optimization | KMeans | silhouette |

## Procedure
1. **Load features** from Feast (point-in-time) or Gold tables into a training frame.
2. **Time-aware split**: train on earlier window, validate/test on later window — no random shuffling (prevents leakage).
3. **Train** with an MLflow run:
   - `mlflow.start_run()`, log params, metrics, and the model artifact.
   - Use `mlflow.<flavor>.log_model(...)` (sklearn/xgboost/pytorch).
4. **Evaluate**: compute the metrics above; log confusion matrix / PR curve as artifacts.
5. **Register**: `mlflow.register_model(...)`; promote stages (Staging → Production) based on thresholds (e.g. AUC ≥ 0.85).
6. **Retraining DAG** (Airflow): schedule periodic retrain, compare to current Production, auto-promote if better.

## Inference modes
- **Batch**: score Gold windows on a schedule, write predictions to Delta.
- **Real-time**: serve via FastAPI loading the Production model from the registry.

## Guardrails
- Always split by time; document the cutoff.
- Log enough params/artifacts to reproduce every run.
- Gate promotion on metrics + drift, never on a single run by chance.
