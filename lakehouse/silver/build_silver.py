"""Bronze → Silver batch job (Phase 4).

Reads all Bronze Delta records, applies:
  1. Deduplication — one row per (machine_id, ts), latest _ingest_ts wins.
  2. Data quality rules — nulls, type ranges; violations → Silver quarantine.
  3. MERGE INTO Silver — idempotent insert-if-new on (machine_id, ts).

Bronze is never mutated (TC-4.1).  The MERGE makes re-runs safe (TC-4.2).
DQ gate thresholds are read from ``LakehouseConfig`` (TC-4.4).

Run via ``lakehouse/silver/submit_silver.sh`` inside the ``spark-silver`` container.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.utils import AnalysisException

from lakehouse.config import LakehouseConfig
from lakehouse.dq import SILVER_DQ_RULES, run_dq


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------


def build_spark(cfg: LakehouseConfig) -> SparkSession:
    """Create a SparkSession configured for Delta + MinIO (S3A)."""
    spark = (
        SparkSession.builder.appName("silver-telemetry-build")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", cfg.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.shuffle.partitions", str(cfg.spark_shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


def read_bronze(spark: SparkSession, cfg: LakehouseConfig) -> DataFrame:
    """Read the full Bronze Delta table."""
    return spark.read.format("delta").load(cfg.bronze_path)


def dedup_bronze(df: DataFrame) -> DataFrame:
    """Keep one row per (machine_id, ts): the one most recently ingested."""
    w = Window.partitionBy("machine_id", "ts").orderBy(F.col("_ingest_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


def write_quarantine(df: DataFrame, cfg: LakehouseConfig) -> None:
    """Append DQ-violating rows to the Silver quarantine table."""
    (
        df.withColumn("_dq_ts", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .save(cfg.silver_quarantine_path)
    )


def merge_to_silver(valid: DataFrame, cfg: LakehouseConfig, spark: SparkSession) -> None:
    """MERGE valid records into Silver — idempotent on (machine_id, ts).

    Uses Spark SQL ``MERGE INTO delta.`path``` which requires only the Delta JAR
    (no ``delta-spark`` Python package).  Falls back to a full overwrite on the
    first run when the table does not yet exist.
    """
    try:
        # Verify the table exists before attempting MERGE.
        spark.read.format("delta").load(cfg.silver_path).limit(0).count()

        valid.createOrReplaceTempView("_silver_source")
        spark.sql(f"""
            MERGE INTO delta.`{cfg.silver_path}` AS t
            USING _silver_source AS s
            ON t.machine_id = s.machine_id AND t.ts = s.ts
            WHEN NOT MATCHED THEN INSERT *
        """)
    except AnalysisException:
        # First run: Silver table does not yet exist.
        (
            valid.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date")
            .option("mergeSchema", "true")
            .save(cfg.silver_path)
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    cfg = LakehouseConfig()
    spark = build_spark(cfg)

    print(f"[silver] reading Bronze from {cfg.bronze_path}", file=sys.stderr)
    try:
        bronze = read_bronze(spark, cfg)
    except AnalysisException as exc:
        print(f"[silver] Bronze table not found: {exc}", file=sys.stderr)
        return 1

    if bronze.isEmpty():
        print("[silver] Bronze is empty — nothing to process.", file=sys.stderr)
        return 0

    # Drop Bronze-internal Kafka metadata; not needed in Silver.
    bronze = bronze.drop("_topic", "_partition", "_offset", "_kafka_ts")

    deduped = dedup_bronze(bronze)
    bronze_count = bronze.count()
    deduped_count = deduped.count()
    print(
        f"[silver] Bronze rows={bronze_count}  after dedup={deduped_count}  "
        f"dupes removed={bronze_count - deduped_count}",
        file=sys.stderr,
    )

    valid, quarantine, dq_result = run_dq(
        deduped,
        rules=SILVER_DQ_RULES,
        max_null_rate=cfg.dq_max_null_rate,
        max_range_violation_rate=cfg.dq_max_range_violation_rate,
    )
    dq_result.print_report()

    if dq_result.quarantine_rows > 0:
        write_quarantine(quarantine, cfg)
        print(
            f"[silver] {dq_result.quarantine_rows} quarantined rows written to "
            f"{cfg.silver_quarantine_path}",
            file=sys.stderr,
        )

    merge_to_silver(valid, cfg, spark)
    print(
        f"[silver] {dq_result.valid_rows} rows merged into Silver at {cfg.silver_path}",
        file=sys.stderr,
    )

    return 0 if dq_result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
