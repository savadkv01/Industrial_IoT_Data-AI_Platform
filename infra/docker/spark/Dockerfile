# Spark image for the Industrial IoT streaming pipeline (Phases 3 & 8).
#
# Uses python:3.11-slim as the base so we get a modern Python out of the box.
# PySpark is installed via pip which bundles the full Spark 3.5.1 runtime,
# avoiding the need for a separate Spark binary install. Java (OpenJDK 17 headless)
# is the only additional system dependency.
#
# The Kafka/Delta/Hadoop-AWS JARs are still resolved at spark-submit time and
# cached in the ivy-cache named volume for fast restarts.
FROM python:3.11-slim

USER root

# Java is required by PySpark.
RUN apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    default-jdk-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Writable dirs: app code (volume-mounted) and ivy jar cache (named volume).
RUN mkdir -p /opt/app /opt/spark/.ivy2

# Install Python deps.
# PySpark brings the full Spark runtime including spark-submit.
RUN pip install --no-cache-dir \
    python-dotenv==1.0.1 \
    "pyspark==3.5.1" \
    "delta-spark==3.2.0" \
    "pandas>=2.2,<3" \
    "scikit-learn>=1.5,<2" \
    "mlflow==3.14.0" \
    "boto3>=1.34,<2"

# Locate the spark-submit binary that comes bundled with PySpark and symlink it.
RUN SPARK_SUBMIT=$(python3 -c 'import pyspark,os; print(os.path.join(os.path.dirname(pyspark.__file__),"bin","spark-submit"))') && ln -sf "$SPARK_SUBMIT" /usr/local/bin/spark-submit

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/app \
    HOME=/root
WORKDIR /opt/app
