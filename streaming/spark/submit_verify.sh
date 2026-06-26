#!/usr/bin/env bash
# Run the batch Bronze verification job (counts, duplicate check, quarantine).
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
  /opt/app/streaming/spark/verify_bronze.py
