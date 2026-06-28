"""Drift report DAG (Phase 10) — close the monitoring → MLOps loop.

Daily, compare the current feature dataset against the versioned reference, persist an
Evidently report + JSON summary, push the key drift gauges to the Prometheus pushgateway
(best-effort, so Grafana's ML dashboard lights up), and trigger the ``train_models``
retraining DAG when dataset-level drift is detected.

Requires the ``monitoring`` package on the Airflow workers (mounted at
``/opt/airflow/ml_src/monitoring`` in docker-compose) and the same feature-store env as
the training DAG.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "ml-platform", "retries": 1}

# Pushgateway address (set PROMETHEUS_PUSHGATEWAY to enable metric export).
import os  # noqa: E402

PUSHGATEWAY = os.getenv("PROMETHEUS_PUSHGATEWAY", "")


def _push_drift_metrics(summary: dict) -> None:
    """Best-effort push of drift gauges to the Prometheus pushgateway."""
    if not PUSHGATEWAY:
        logger.info("PROMETHEUS_PUSHGATEWAY unset — skipping metric push.")
        return
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        Gauge("iiot_drift_share", "Share of drifted features", registry=registry).set(
            summary["drift_share"]
        )
        Gauge("iiot_dataset_drift", "Dataset-level drift flag (0/1)", registry=registry).set(
            int(summary["dataset_drift"])
        )
        Gauge(
            "iiot_drift_features_drifted", "Count of drifted features", registry=registry
        ).set(summary["n_drifted"])
        push_to_gateway(PUSHGATEWAY, job="drift_report", registry=registry)
        logger.info("pushed drift metrics to %s", PUSHGATEWAY)
    except Exception as exc:  # pragma: no cover - depends on deployment
        logger.warning("drift metric push skipped (%s)", exc)


@dag(
    dag_id="drift_report",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["monitoring", "phase10", "drift"],
)
def drift_report():
    """Compute drift, export metrics, and conditionally trigger retraining."""

    @task.short_circuit
    def compute_and_export() -> bool:
        """Generate the drift report; short-circuit (skip retrain) when stable."""
        from monitoring.drift.evidently_reports import generate_report

        report = generate_report()
        summary = report.to_dict()
        logger.info(
            "drift: %d/%d features drifted (share=%.2f, dataset_drift=%s)",
            summary["n_drifted"],
            summary["n_features"],
            summary["drift_share"],
            summary["dataset_drift"],
        )
        _push_drift_metrics(summary)
        # Only continue (trigger retraining) when the dataset has drifted.
        return bool(summary["dataset_drift"])

    trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_dag_id="train_models",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    compute_and_export() >> trigger_retrain


drift_report()
