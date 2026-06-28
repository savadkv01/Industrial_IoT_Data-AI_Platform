# MLflow tracking server (Phase 7).
# Postgres-backed registry + MinIO (S3) artifact store. The DB backend is required for
# the Model Registry; the file store cannot version/alias models.
FROM python:3.11-slim

RUN pip install --no-cache-dir \
    mlflow==3.14.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.162

EXPOSE 5000

# Backend + artifact roots are passed via environment from docker-compose.
CMD ["sh", "-c", "mlflow server \
    --host 0.0.0.0 --port 5000 \
    --backend-store-uri ${MLFLOW_BACKEND_STORE_URI} \
    --artifacts-destination ${MLFLOW_ARTIFACTS_DESTINATION} \
    --serve-artifacts \
    --allowed-hosts '*'"]
