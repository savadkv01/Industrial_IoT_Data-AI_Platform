"""Build the Feast offline dataset from the Phase 4 Gold table."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

from feature_engineering.config import FeatureEngineeringConfig
from feature_engineering.transforms import build_feature_dataset


def load_gold_dataframe(source: str | Path) -> pd.DataFrame:
    """Load Gold data from parquet or Delta using DuckDB.

    Supports:
      * local parquet files
      * local Delta table directories
      * S3/S3A Delta table locations
    """
    source_path = Path(source)
    if source_path.exists():
        if source_path.suffix == ".parquet":
            return pd.read_parquet(source_path)
        if source_path.is_dir():
            con = duckdb.connect()
            return con.execute(f"SELECT * FROM delta_scan('{source_path.as_posix()}')").df()
        raise ValueError(f"unsupported local Gold source format: {source_path.suffix}")

    if str(source).startswith(("s3://", "s3a://")):
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        con.execute("INSTALL httpfs; LOAD httpfs;")

        from lakehouse.config import LakehouseConfig

        lakehouse_cfg = LakehouseConfig()
        endpoint = lakehouse_cfg.minio_endpoint.replace("http://", "").replace("https://", "")
        use_ssl = lakehouse_cfg.minio_endpoint.startswith("https://")
        con.execute(f"SET s3_endpoint='{endpoint}';")
        con.execute(f"SET s3_access_key_id='{lakehouse_cfg.minio_access_key}';")
        con.execute(f"SET s3_secret_access_key='{lakehouse_cfg.minio_secret_key}';")
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
        con.execute("SET s3_url_style='path';")
        normalized = str(source).replace("s3a://", "s3://")
        return con.execute(f"SELECT * FROM delta_scan('{normalized}')").df()

    raise FileNotFoundError(f"Gold source not found: {source}")


def write_offline_store(gold_df: pd.DataFrame, cfg: FeatureEngineeringConfig) -> Path:
    dataset = build_feature_dataset(gold_df)
    cfg.offline_dir.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(cfg.offline_file, index=False)
    return cfg.offline_file


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    cfg = FeatureEngineeringConfig()
    source = argv[0] if argv else cfg.gold_default_source
    if not source:
        from lakehouse.config import LakehouseConfig

        source = LakehouseConfig().gold_path.replace("s3a://", "s3://")

    gold_df = load_gold_dataframe(source)
    output = write_offline_store(gold_df, cfg)
    print(f"[phase5] built offline dataset from {source}", file=sys.stderr)
    print(f"[phase5] wrote Feast offline dataset to {output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())