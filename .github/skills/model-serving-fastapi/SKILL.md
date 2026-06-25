---
name: model-serving-fastapi
description: 'Build the FastAPI model inference service (Phase 9). Use when creating prediction endpoints, loading models from the MLflow registry, request/response schemas, model caching for low latency, batch vs real-time inference, or Prometheus instrumentation of the API.'
argument-hint: 'e.g. "add /predict/maintenance endpoint" or "cache MLflow model"'
---

# Model Serving (FastAPI)

## When to use
- Implementing anything under `serving/`.
- Building inference endpoints or model loading logic.

## Procedure
1. **App skeleton** (`app/main.py`): create FastAPI app, include routers, add startup hook to warm-load models.
2. **Model loading** (`app/models.py`): load the Production model from MLflow registry once at startup; cache in memory; expose a `reload()` for new versions.
3. **Schemas** (`app/schemas.py`): Pydantic request models (machine feature window) and response models (probability/score + model version).
4. **Endpoints**:
   - `GET /health` — readiness (model loaded?).
   - `POST /predict/maintenance` — failure probability.
   - `POST /predict/battery` — degradation estimate.
   - `POST /predict/anomaly` — anomaly score.
5. **Online features**: optionally fetch real-time features from Feast before scoring.
6. **Metrics** (`app/metrics.py`): Prometheus counters/histograms for request count, latency, prediction distribution; expose `GET /metrics`.
7. **Containerize**: `Dockerfile` running `uvicorn app.main:app`.

## Latency tips
- Load/parse the model once; never per request.
- Use vectorized/batch scoring; avoid Python loops over rows.
- Keep payloads small; validate with Pydantic at the boundary.

## Guardrails
- Validate every input at the boundary; reject malformed payloads with 422.
- Return the serving model version in every response for traceability.
- Never load secrets into code — read from environment.
