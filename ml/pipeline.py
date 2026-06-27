"""End-to-end MLOps training pipeline (Phase 7).

Runs a task's Phase 6 training function, logs the run + model to MLflow, registers the
model as a new version, and gate-promotes it to the ``production`` alias when it beats
the incumbent. This is the single code path used by both the CLI and the Airflow
retraining DAG, so a scheduled retrain and a manual run behave identically.

Run:
    python -m ml.pipeline                       # all three tasks
    python -m ml.pipeline --task predictive_maintenance
    python -m ml.pipeline --no-register         # train + log only
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import mlflow
from mlflow.tracking import MlflowClient

from ml.anomaly_detection.train import train as train_anomaly
from ml.battery_health.train import train as train_battery
from ml.common.registry import PromotionDecision, evaluate_and_promote, register_version
from ml.common.tasks import (
    ANOMALY_DETECTION,
    BATTERY_HEALTH,
    PREDICTIVE_MAINTENANCE,
    TASKS,
    TaskSpec,
    get_task,
)
from ml.common.tracking import configure_tracking, log_model, start_run
from ml.config import MLConfig
from ml.predictive_maintenance.train import train as train_pdm

# Maps a task key to its Phase 6 training entrypoint. Kept here (not in tasks.py) so the
# shared task module stays free of train-time imports usable by the serving layer.
TRAIN_FUNCS: dict[str, Callable[..., Any]] = {
    PREDICTIVE_MAINTENANCE: train_pdm,
    ANOMALY_DETECTION: train_anomaly,
    BATTERY_HEALTH: train_battery,
}


@dataclass
class PipelineResult:
    task: str
    run_id: str
    metrics: dict[str, float]
    feature_source: str
    registered_version: str | None
    decision: PromotionDecision | None


def _log_training_params(spec: TaskSpec, result: Any) -> None:
    mlflow.log_params(
        {
            "task": spec.key,
            "model": type(result.model).__name__,
            "n_features": len(result.feature_columns),
            "feature_source": result.source,
            "split": "time_aware",
            "primary_metric": spec.primary_metric,
        }
    )
    mlflow.log_params({f"cutoff_{k}": v for k, v in result.cutoffs.items()})


def run_pipeline(
    task: str,
    cfg: MLConfig | None = None,
    *,
    frame=None,
    register: bool = True,
    promote: bool = True,
) -> PipelineResult:
    """Train ``task``, log it to MLflow, and optionally register + promote the model."""
    cfg = configure_tracking(cfg)
    spec = get_task(task)
    result = TRAIN_FUNCS[task](frame=frame, cfg=cfg)

    registered_version: str | None = None
    decision: PromotionDecision | None = None

    with start_run(spec.experiment(cfg), run_name=spec.run_name, cfg=cfg) as run:
        run_id = run.info.run_id
        _log_training_params(spec, result)
        mlflow.log_metrics({k: v for k, v in result.metrics.items() if v == v})
        log_model(result.model, name="model")

        if register:
            client = MlflowClient(tracking_uri=cfg.mlflow_tracking_uri)
            name = spec.registered_model(cfg)
            registered_version = register_version(
                client,
                f"runs:/{run_id}/model",
                name,
                description=f"{spec.key} model — primary metric {spec.primary_metric}",
                tags={"task": spec.key, "feature_source": result.source},
            )
            candidate_metric = float(result.metrics.get(spec.primary_metric, float("nan")))
            decision = evaluate_and_promote(
                client,
                name,
                registered_version,
                candidate_metric=candidate_metric,
                metric_name=spec.primary_metric,
                higher_is_better=spec.higher_is_better,
                gate=spec.gate(cfg),
                staging_alias=cfg.staging_alias,
                # An empty production alias disables promotion (register + stage only).
                production_alias=cfg.production_alias if promote else "",
            )
            mlflow.set_tags(
                {
                    "registered_version": registered_version,
                    "promoted": str(decision.promoted),
                }
            )

    return PipelineResult(
        task=task,
        run_id=run_id,
        metrics=result.metrics,
        feature_source=result.source,
        registered_version=registered_version,
        decision=decision,
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train, log, register and promote models.")
    parser.add_argument(
        "--task",
        choices=sorted(TASKS),
        action="append",
        help="Task to run (repeatable). Default: all tasks.",
    )
    parser.add_argument(
        "--no-register", action="store_true", help="Train + log only; skip the registry."
    )
    parser.add_argument(
        "--no-promote",
        action="store_true",
        help="Register as a new version but never set the production alias.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    cfg = MLConfig()
    tasks = args.task or list(TASKS)
    for task in tasks:
        result = run_pipeline(
            task,
            cfg=cfg,
            register=not args.no_register,
            promote=not args.no_promote,
        )
        spec = get_task(task)
        primary = result.metrics.get(spec.primary_metric, float("nan"))
        line = (
            f"[{task}] source={result.feature_source}  "
            f"{spec.primary_metric}={primary:.4f}  "
            f"version={result.registered_version}"
        )
        if result.decision is not None:
            line += f"  -> {result.decision.reason}"
        print(line, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
