# FastAPI model inference service (Phase 9).
# Build context is the repo root so the image can install the shared `ml` package
# alongside `serving`:  docker compose build serving
FROM python:3.11-slim

WORKDIR /opt/app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/app

# Shared ML task specs + registry loaders, then the serving app itself.
COPY ml /opt/app/ml
COPY serving /opt/app/serving

RUN pip install --no-cache-dir ./ml ./serving

EXPOSE 8000

# `app` and `ml` are pip-installed above, so import as top-level packages.
# Single worker keeps each model loaded once in-process; scale out with replicas.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
