# Airflow image for the ML retraining DAG (Phase 7).
# Bundles the platform's ML deps + the `ml` package so DAG tasks can call
# ml.pipeline.run_pipeline directly. The `ml` source is mounted read-only by compose.
FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH=/opt/airflow/ml_src
