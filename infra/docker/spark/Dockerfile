# Spark image for the Industrial IoT streaming pipeline (Phase 3).
# Bundles PySpark (3.5.1) + Python deps; Kafka/Delta/Hadoop-AWS jars are resolved
# at submit time and cached in the mounted ivy volume for fast restarts.
FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

# Writable dirs for the app code (mounted) and the ivy jar cache (named volume).
RUN mkdir -p /opt/app /opt/spark/.ivy2 \
    && chown -R spark:spark /opt/app /opt/spark/.ivy2

# Python runtime deps used by the job (Spark/PySpark already present).
RUN python3 -m pip install --no-cache-dir python-dotenv==1.0.1

ENV PYTHONPATH=/opt/app \
    HOME=/opt/spark
WORKDIR /opt/app

USER spark
