"""Kafka -> Bronze Delta ingestion (Spark Structured Streaming).

Reads telemetry JSON from Kafka, parses it against the shared schema, splits valid
vs malformed records, and writes:
  * valid records  -> Bronze Delta table (append-only, partitioned by ``event_date``)
  * malformed rows -> a quarantine Delta table (never silently dropped)

Exactly-once is provided by Spark's checkpointing + the idempotent Delta sink. The
job is safe to kill and restart: it resumes from the committed offsets with no
duplicates or gaps.

Run via ``streaming/spark/submit_ingest.sh`` (inside the ``spark-streaming`` container)
which supplies the Kafka/Delta/Hadoop-AWS packages and S3A configuration.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.config import StreamingConfig
from streaming.spark.schema import telemetry_schema


def build_spark(cfg: StreamingConfig) -> SparkSession:
    """Create a SparkSession configured for Delta + MinIO (S3A)."""
    builder = (
        SparkSession.builder.appName("bronze-telemetry-ingest")
        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A / MinIO
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
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark: SparkSession, cfg: StreamingConfig) -> DataFrame:
    """Open the Kafka source stream."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
        .option("subscribe", cfg.telemetry_topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_enrich(raw: DataFrame) -> DataFrame:
    """Parse the JSON payload and add ingestion metadata + partition column.

    ``from_json`` yields nulls for unparseable payloads; downstream we treat a null
    ``machine_id`` as malformed and route it to quarantine.
    """
    parsed = raw.select(
        F.col("topic").alias("_topic"),
        F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
        F.col("timestamp").alias("_kafka_ts"),
        F.from_json(F.col("value").cast("string"), telemetry_schema()).alias("data"),
        F.col("value").cast("string").alias("_raw_value"),
    )
    return (
        parsed.select("_topic", "_partition", "_offset", "_kafka_ts", "_raw_value", "data.*")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("ts")))
    )


def write_bronze(df: DataFrame, cfg: StreamingConfig):
    """Write valid records to Bronze; malformed rows to quarantine.

    Both branches share a single streaming query via ``foreachBatch`` so offsets are
    committed once per micro-batch (exactly-once across both sinks).
    """

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.persist()
        try:
            valid = batch_df.filter(F.col("machine_id").isNotNull())
            invalid = batch_df.filter(F.col("machine_id").isNull())

            (
                valid.drop("_raw_value")
                .write.format("delta")
                .mode("append")
                .partitionBy("event_date")
                .option("mergeSchema", "true")
                .save(cfg.bronze_path)
            )

            if not invalid.isEmpty():
                (
                    invalid.select("_topic", "_partition", "_offset", "_kafka_ts", "_raw_value")
                    .withColumn("_ingest_ts", F.current_timestamp())
                    .write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(cfg.quarantine_path)
                )
        finally:
            batch_df.unpersist()

    return (
        df.writeStream.queryName("bronze_telemetry")
        .option("checkpointLocation", cfg.checkpoint_path)
        .trigger(processingTime=cfg.trigger_interval)
        .foreachBatch(_process_batch)
        .start()
    )


def main() -> int:
    cfg = StreamingConfig()
    spark = build_spark(cfg)
    print(
        f"[ingest] topic={cfg.telemetry_topic} -> {cfg.bronze_path} "
        f"(checkpoint={cfg.checkpoint_path})",
        file=sys.stderr,
    )
    raw = read_kafka(spark, cfg)
    enriched = parse_and_enrich(raw).withWatermark("ts", cfg.watermark_delay)
    query = write_bronze(enriched, cfg)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
