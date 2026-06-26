"""Silver → Gold batch job (Phase 4).

Reads the full Silver table and computes time-window rolling statistics for
each machine at three granularities: 5 min, 1 h, 24 h.  All windows are
stored in a single Gold table partitioned by ``event_date`` + ``window_duration``.

Gold schema per row:
  machine_id, window_start, window_end, window_duration, event_date,
  vibration_mean/std/max, motor_temp_mean/std/max,
  cpu_usage_mean/std, battery_soh_mean/min,
  error_count, record_count,
  failure_label  (True if any record in the window has failure_within_horizon=True)

Run via ``lakehouse/gold/submit_gold.sh`` inside the ``spark-gold`` container.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from lakehouse.config import LakehouseConfig
from lakehouse.gold import WINDOW_SPECS


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------


def build_spark(cfg: LakehouseConfig) -> SparkSession:
    """Create a SparkSession configured for Delta + MinIO (S3A)."""
    spark = (
        SparkSession.builder.appName("gold-telemetry-build")
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
        # Required for Delta overwrite by partition.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Window granularities — imported from lakehouse.gold (see __init__.py)
# ---------------------------------------------------------------------------


def _compute_one_window(df: DataFrame, duration: str, label: str) -> DataFrame:
    """Aggregate Silver records within a single window duration."""
    return (
        df.groupBy("machine_id", F.window("ts", duration))
        .agg(
            # Vibration — rises with wear; useful for anomaly detection.
            F.mean("vibration").alias("vibration_mean"),
            F.stddev("vibration").alias("vibration_std"),
            F.max("vibration").alias("vibration_max"),
            # Motor temperature — rises with load / degradation.
            F.mean("motor_temp").alias("motor_temp_mean"),
            F.stddev("motor_temp").alias("motor_temp_std"),
            F.max("motor_temp").alias("motor_temp_max"),
            # CPU usage.
            F.mean("cpu_usage").alias("cpu_usage_mean"),
            F.stddev("cpu_usage").alias("cpu_usage_std"),
            # Battery state of health — falls toward failure.
            F.mean("battery_soh").alias("battery_soh_mean"),
            F.min("battery_soh").alias("battery_soh_min"),
            # Error events within the window.
            F.sum(F.when(F.col("error_code") != 0, 1).otherwise(0)).alias("error_count"),
            # Predictive maintenance label: any failure flag in the window.
            F.max(F.col("failure_within_horizon").cast("int"))
            .cast("boolean")
            .alias("failure_label"),
            F.count("*").alias("record_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("window_duration", F.lit(label))
        .withColumn("event_date", F.to_date("window_start"))
        .drop("window")
    )


def build_gold(silver: DataFrame) -> DataFrame:
    """Compute Gold feature rows across all window granularities and union them."""
    frames = [_compute_one_window(silver, dur, lbl) for dur, lbl in WINDOW_SPECS]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


def write_gold(df: DataFrame, cfg: LakehouseConfig) -> None:
    """Overwrite Gold — idempotent full recompute from Silver.

    Partitioned by ``event_date`` + ``window_duration`` with dynamic overwrite
    so only touched date-partitions are replaced.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date", "window_duration")
        .option("mergeSchema", "true")
        .save(cfg.gold_path)
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    cfg = LakehouseConfig()
    spark = build_spark(cfg)

    print(f"[gold] reading Silver from {cfg.silver_path}", file=sys.stderr)
    try:
        silver = spark.read.format("delta").load(cfg.silver_path)
    except AnalysisException as exc:
        print(f"[gold] Silver table not found — run Silver job first: {exc}", file=sys.stderr)
        return 1

    silver_count = silver.count()
    if silver_count == 0:
        print("[gold] Silver is empty — nothing to aggregate.", file=sys.stderr)
        return 0
    print(f"[gold] {silver_count} Silver rows", file=sys.stderr)

    gold = build_gold(silver)
    write_gold(gold, cfg)

    gold_count = spark.read.format("delta").load(cfg.gold_path).count()
    print(
        f"[gold] {gold_count} Gold feature rows written to {cfg.gold_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
