#!/usr/bin/env bash
# Submit the Gold batch build job.
# Reuses the shared ivy cache and the same Delta + Hadoop-AWS JAR set as Silver.
set -euo pipefail

export PYTHONPATH=/opt/app/lakehouse:/opt/app

DELTA_VERSION=3.2.0
HADOOP_AWS_VERSION=3.3.4
AWS_SDK_VERSION=1.12.262

PACKAGES="io.delta:delta-spark_2.12:${DELTA_VERSION}"
PACKAGES="${PACKAGES},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION}"
PACKAGES="${PACKAGES},com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_VERSION}"

exec "${SPARK_HOME:-/usr/local}/bin/spark-submit" \
  --master "local[*]" \
  --packages "${PACKAGES}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  /opt/app/lakehouse/gold/build_gold.py
