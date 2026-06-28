# Infrastructure

Dockerfiles and service configuration for platform components that need custom images
(MLflow, Airflow, Spark, serving).

```
infra/
└── docker/
    ├── mlflow/
    ├── airflow/
    └── spark/
```

## Notable configuration
- **mlflow** — the server runs with `--allowed-hosts '*'` so containers reaching it by the
  Compose service name (`Host: mlflow`) are not rejected by MLflow 3.x's DNS-rebinding guard.
  Artifacts are stored in MinIO (`--artifacts-destination s3://mlflow/`).
- **spark** — built on `python:3.11-slim` with PySpark, Delta, scikit-learn, MLflow, and
  `boto3` so streaming jobs (Phase 8) can load registered models directly from MinIO/S3.
