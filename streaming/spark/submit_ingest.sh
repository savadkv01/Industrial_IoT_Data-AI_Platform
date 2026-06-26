#!/usr/bin/env bash
# Submit the Bronze ingestion streaming job.
# Resolves the Kafka, Delta, and Hadoop-AWS (S3A/MinIO) packages at submit time;
# jars are cached in /opt/spark/.ivy2 (a named volume) so restarts are fast.
set -euo pipefail

export PYTHONPATH=/opt/app

SPARK_VERSION=3.5.1
DELTA_VERSION=3.2.0
HADOOP_AWS_VERSION=3.3.4
AWS_SDK_VERSION=1.12.262

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}"
PACKAGES="${PACKAGES},io.delta:delta-spark_2.12:${DELTA_VERSION}"
PACKAGES="${PACKAGES},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"
PACKAGES="${PACKAGES},com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_VERSION}"

exec /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --packages "${PACKAGES}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /opt/app/streaming/spark/ingest_bronze.py
