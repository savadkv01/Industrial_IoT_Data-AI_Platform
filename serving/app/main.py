"""FastAPI inference service (Phase 9).

Loads the production models from the MLflow registry once at startup and serves
low-latency predictions for the three model families. Every response carries the model
name/alias/version for traceability, and ``GET /metrics`` exposes Prometheus metrics for
the Phase 10 monitoring stack.

Run locally:  ``uvicorn app.main:app --reload``
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response

from ml.common.tasks import (
    ANOMALY_DETECTION,
    BATTERY_HEALTH,
    PREDICTIVE_MAINTENANCE,
    TASKS,
    get_task,
)

from app import metrics
from app.config import ServingConfig
from app.models import ModelCache, ModelNotReady, score_records
from app.schemas import (
    HealthResponse,
    ModelStatus,
    Prediction,
    PredictRequest,
    PredictResponse,
)

# Friendly endpoint segment -> internal task key.
ROUTE_TASKS = {
    "maintenance": PREDICTIVE_MAINTENANCE,
    "battery": BATTERY_HEALTH,
    "anomaly": ANOMALY_DETECTION,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = ServingConfig()
    cache = ModelCache(serving_cfg=cfg)
    app.state.cache = cache
    app.state.config = cfg
    if cfg.warm_start:
        cache.warm()
    yield
    cache.reload()


app = FastAPI(
    title="Industrial IoT — Model Serving",
    description="Real-time predictive-maintenance, battery-health and anomaly scoring.",
    version="0.1.0",
    lifespan=lifespan,
)


def _cache(request: Request) -> ModelCache:
    return request.app.state.cache


def _predict(request: Request, task: str, payload: PredictRequest) -> PredictResponse:
    """Shared scoring path for all three predict endpoints."""
    cache = _cache(request)
    fill = request.app.state.config.missing_feature_fill
    spec = get_task(task)
    start = time.perf_counter()
    try:
        loaded, scores = score_records(cache, task, payload.records, fill=fill)
    except ModelNotReady as exc:
        metrics.REQUEST_COUNT.labels(task=task, status="unavailable").inc()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # malformed feature space, bad dtypes, etc.
        metrics.REQUEST_COUNT.labels(task=task, status="error").inc()
        raise HTTPException(status_code=422, detail=f"scoring failed: {exc}") from exc

    metrics.REQUEST_LATENCY.labels(task=task).observe(time.perf_counter() - start)
    metrics.REQUEST_COUNT.labels(task=task, status="ok").inc()
    metrics.MODEL_VERSION.labels(
        task=task,
        model_name=loaded.name,
        model_alias=loaded.alias,
        model_version=loaded.version,
    ).set(1)
    for value in scores:
        metrics.PREDICTION_SCORE.labels(task=task).observe(value)

    predictions = [
        Prediction(
            machine_id=record.machine_id,
            event_timestamp=record.event_timestamp,
            score=score,
        )
        for record, score in zip(payload.records, scores)
    ]
    return PredictResponse(
        task=task,
        model_name=loaded.name,
        model_alias=loaded.alias,
        model_version=loaded.version,
        output=spec.output_column,
        predictions=predictions,
    )


@app.get("/health", response_model=HealthResponse)
def health(request: Request) -> HealthResponse:
    """Readiness probe: reports per-task model load state without forcing a reload."""
    cache = _cache(request)
    statuses: list[ModelStatus] = []
    any_loaded = False
    for task in TASKS:
        try:
            loaded = cache.get(task)
            any_loaded = True
            statuses.append(
                ModelStatus(
                    task=task,
                    loaded=True,
                    model_name=loaded.name,
                    model_alias=loaded.alias,
                    model_version=loaded.version,
                )
            )
        except ModelNotReady as exc:
            spec = get_task(task)
            statuses.append(
                ModelStatus(
                    task=task,
                    loaded=False,
                    model_name=spec.registered_model(cache.ml_cfg),
                    model_alias=cache.alias,
                    error=str(exc),
                )
            )
    return HealthResponse(status="ok" if any_loaded else "unavailable", models=statuses)


@app.post("/predict/maintenance", response_model=PredictResponse)
def predict_maintenance(request: Request, payload: PredictRequest) -> PredictResponse:
    """Failure probability within the prediction horizon."""
    return _predict(request, ROUTE_TASKS["maintenance"], payload)


@app.post("/predict/battery", response_model=PredictResponse)
def predict_battery(request: Request, payload: PredictRequest) -> PredictResponse:
    """Battery state-of-health degradation estimate."""
    return _predict(request, ROUTE_TASKS["battery"], payload)


@app.post("/predict/anomaly", response_model=PredictResponse)
def predict_anomaly(request: Request, payload: PredictRequest) -> PredictResponse:
    """Anomaly score (higher = more anomalous)."""
    return _predict(request, ROUTE_TASKS["anomaly"], payload)


@app.post("/reload")
def reload_models(request: Request) -> dict[str, str | None]:
    """Drop cached models and re-resolve aliases (e.g. after a promotion)."""
    cache = _cache(request)
    cache.reload()
    return cache.warm()


@app.get("/metrics")
def prometheus_metrics() -> Response:
    payload, content_type = metrics.render_metrics()
    return Response(content=payload, media_type=content_type)
