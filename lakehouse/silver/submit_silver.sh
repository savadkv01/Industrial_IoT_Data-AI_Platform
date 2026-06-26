#!/usr/bin/env bash
# Submit the Silver batch build job.
# Resolves the same Delta + Hadoop-AWS jars as the Bronze streaming job;
# the ivy cache is shared via the named volume for fast re-runs.
set -euo pipefail

# Both the lakehouse package (under /opt/app/lakehouse/) and the streaming
# package (under /opt/app/streaming/) must be importable.
export PYTHONPATH=/opt/app/lakehouse:/opt/app

SPARK_VERSION=3.5.1
DELTA_VERSION=3.2.0
HADOOP_AWS_VERSION=3.3.4
AWS_SDK_VERSION=1.12.262

PACKAGES="io.delta:delta-spark_2.12:${DELTA_VERSION}"
PACKAGES="${PACKAGES},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"
PACKAGES="${PACKAGES},com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_VERSION}"

exec /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --packages "${PACKAGES}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.sql.shuffle.partitions=8 \
  /opt/app/lakehouse/silver/build_silver.py
