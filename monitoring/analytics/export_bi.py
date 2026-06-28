"""Export Gold + ML predictions into Postgres for Grafana BI/AI dashboards (Phase 10).

Pipeline::

    Gold Delta (MinIO)  ─┐
                         ├─►  build_bi_tables  ─►  Postgres `analytics` schema  ─►  Grafana
    ml/predictions/*  ──┘

Gold is read with DuckDB's ``delta`` + ``httpfs`` extensions (the proven
``lakehouse.query.duckdb_smoke`` pattern); predictions are read from the batch-inference
parquet outputs. Both reads degrade gracefully to empty frames so a partially-built
platform still produces (empty) tables rather than crashing.

CLI::

    python -m monitoring.analytics.export_bi            # export all tables
    python -m monitoring.analytics.export_bi --dry-run  # build + print row counts only
"""

from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from monitoring.analytics.transforms import TASK_SCORE_COLUMNS, build_bi_tables
from monitoring.config import MonitoringConfig, get_config

logger = logging.getLogger(__name__)


def read_gold(cfg: MonitoringConfig) -> pd.DataFrame:
    """Read the Gold Delta table from MinIO via DuckDB; empty frame on any failure."""
    try:
        import duckdb
    except ImportError:  # pragma: no cover - duckdb is a declared extra
        logger.warning("duckdb not installed — skipping Gold export. pip install 'duckdb>=1.0'")
        return pd.DataFrame()

    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta;")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        endpoint = cfg.minio_endpoint.replace("http://", "").replace("https://", "")
        use_ssl = "true" if cfg.minio_endpoint.startswith("https") else "false"
        # Legacy SET globals (older DuckDB / httpfs); kept for backward compatibility.
        con.execute(f"SET s3_endpoint='{endpoint}';")
        con.execute(f"SET s3_access_key_id='{cfg.minio_access_key}';")
        con.execute(f"SET s3_secret_access_key='{cfg.minio_secret_key}';")
        con.execute(f"SET s3_use_ssl={use_ssl};")
        con.execute("SET s3_url_style='path';")
        # The modern `delta` extension (delta-kernel-rs) reads credentials from the secret
        # manager, not the SET globals — without this it falls back to the EC2 metadata
        # credential chain and fails against MinIO.
        con.execute(
            "CREATE OR REPLACE SECRET minio ("
            "TYPE s3, "
            f"KEY_ID '{cfg.minio_access_key}', "
            f"SECRET '{cfg.minio_secret_key}', "
            f"ENDPOINT '{endpoint}', "
            f"USE_SSL {use_ssl}, "
            "URL_STYLE 'path');"
        )
        gold = con.execute(f"SELECT * FROM delta_scan('{cfg.gold_s3_url}')").fetch_df()
        logger.info("read %d Gold rows from %s", len(gold), cfg.gold_s3_url)
        return gold
    except Exception as exc:
        logger.warning("could not read Gold (%s) — exporting empty BI tables", exc)
        return pd.DataFrame()
    finally:
        con.close()


def read_predictions(cfg: MonitoringConfig) -> dict[str, pd.DataFrame]:
    """Load each task's batch-inference parquet (``<task>.parquet``) when present."""
    predictions: dict[str, pd.DataFrame] = {}
    for task in TASK_SCORE_COLUMNS:
        path = cfg.predictions_dir / f"{task}.parquet"
        if path.exists():
            try:
                predictions[task] = pd.read_parquet(path)
                logger.info("read %d %s predictions from %s", len(predictions[task]), task, path)
            except Exception as exc:  # pragma: no cover - corrupt file
                logger.warning("could not read %s predictions (%s)", task, exc)
        else:
            logger.info("no predictions for %s at %s (skipping)", task, path)
    return predictions


def write_postgres(tables: dict[str, pd.DataFrame], cfg: MonitoringConfig) -> dict[str, int]:
    """Replace each analytics table in Postgres; return written row counts.

    Tables are written with ``if_exists="replace"`` so the export is idempotent. The
    ``analytics`` schema is created up-front (Postgres); other dialects (e.g. SQLite in
    tests) ignore the schema.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(cfg.postgres_url)
    is_postgres = engine.dialect.name == "postgresql"
    schema = cfg.analytics_schema if is_postgres else None

    written: dict[str, int] = {}
    try:
        if is_postgres:
            with engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        for name, frame in tables.items():
            frame.to_sql(name, engine, schema=schema, if_exists="replace", index=False)
            written[name] = len(frame)
            logger.info("wrote %d rows -> %s.%s", len(frame), schema or "main", name)
    finally:
        engine.dispose()
    return written


def export(cfg: MonitoringConfig | None = None, *, dry_run: bool = False) -> dict[str, int]:
    """Read Gold + predictions, build BI tables, and (unless dry-run) write to Postgres."""
    cfg = cfg or get_config()
    gold = read_gold(cfg)
    predictions = read_predictions(cfg)
    tables = build_bi_tables(gold, predictions, recent_days=cfg.bi_recent_days)

    if dry_run:
        counts = {name: len(frame) for name, frame in tables.items()}
        logger.info("dry-run — built tables: %s", counts)
        return counts

    return write_postgres(tables, cfg)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the BI export."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Export Gold + predictions to Postgres for BI.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Build tables and print row counts; no DB write."
    )
    args = parser.parse_args(argv)

    counts = export(get_config(), dry_run=args.dry_run)
    print(f"[bi-export] tables={counts}", file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
