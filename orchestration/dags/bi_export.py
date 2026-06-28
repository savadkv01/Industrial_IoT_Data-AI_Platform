"""BI export DAG (Phase 10) — Gold + ML predictions → Postgres for Grafana.

Daily, read the Gold lakehouse table and the latest batch-inference predictions, build the
curated analytics tables (fleet KPIs, machine health, at-risk leaderboard), and replace the
Postgres ``analytics`` schema that the Grafana *Fleet Operations (BI)* and
*Predictive Maintenance (AI)* dashboards read from.

Requires the ``monitoring`` package + analytics env (MinIO + Postgres) on the Airflow
workers — wired in docker-compose. Reads are best-effort: a missing Gold table or absent
predictions yields empty tables instead of a failure.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "ml-platform", "retries": 1}


@dag(
    dag_id="bi_export",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["monitoring", "phase10", "bi", "analytics"],
)
def bi_export():
    """Materialise Gold + prediction analytics tables into Postgres."""

    @task
    def export() -> dict:
        from monitoring.analytics.export_bi import export as run_export

        counts = run_export()
        logger.info("BI export wrote tables: %s", counts)
        return counts

    export()


bi_export()
