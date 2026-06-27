"""Retraining DAG — retrain, register and gate-promote all models, then batch score.

Each model family is a task group of (train+register+promote) → (batch score). The
training task uses :func:`ml.pipeline.run_pipeline`, so a scheduled retrain is identical
to a manual ``python -m ml.pipeline`` run: a new version is registered, promoted to the
``production`` alias only when it beats the incumbent, and the freshly promoted model is
then used to score the offline feature store.

Schedule: daily. Set ``MLFLOW_TRACKING_URI`` (and feature-store env) on the Airflow
workers so the DAG writes to the shared MLflow server rather than the local SQLite store.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from ml.common.tasks import TASKS

DEFAULT_ARGS = {"owner": "ml-platform", "retries": 1}


@dag(
    dag_id="train_models",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mlops", "phase7", "retrain"],
)
def train_models():
    """Retrain, register/promote, and batch-score every model family."""

    @task
    def retrain(model_task: str) -> dict:
        from ml.pipeline import run_pipeline

        result = run_pipeline(model_task, register=True, promote=True)
        return {
            "task": result.task,
            "version": result.registered_version,
            "promoted": bool(result.decision and result.decision.promoted),
            "reason": result.decision.reason if result.decision else "not registered",
        }

    @task
    def batch_score(retrain_info: dict) -> int:
        from ml.inference.batch import batch_score as run_batch_score
        from ml.inference.batch import main as write_predictions

        if not retrain_info.get("promoted"):
            # Nothing new in production — keep the previous predictions.
            return 0
        # Persist predictions for the freshly promoted production model.
        write_predictions(["--task", retrain_info["task"]])
        return len(run_batch_score(retrain_info["task"]))

    for model_task in TASKS:
        batch_score(retrain(model_task))


train_models()
