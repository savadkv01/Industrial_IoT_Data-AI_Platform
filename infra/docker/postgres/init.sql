-- Creates the per-service databases used by MLflow and Airflow (Phase 7).
-- Runs only on a fresh postgres-data volume (docker-entrypoint-initdb.d convention).
SELECT 'CREATE DATABASE mlflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
