# Model Serving — Phase 9

FastAPI inference service loading models from the MLflow registry.

## Endpoints (planned)
- `GET  /health` — liveness/readiness.
- `POST /predict/maintenance` — failure probability for a machine window.
- `POST /predict/battery` — battery degradation estimate.
- `POST /predict/anomaly` — anomaly score.
- `GET  /metrics` — Prometheus metrics.

## Planned layout
```
serving/
├── app/
│   ├── main.py        # FastAPI app
│   ├── models.py      # MLflow model loaders + caching
│   ├── schemas.py     # request/response Pydantic models
│   └── metrics.py     # Prometheus instrumentation
└── Dockerfile
```

## Key concepts
- Model caching & warm start for low latency.
- Batch + real-time inference paths.
- Request/response validation and structured logging.

See the `model-serving-fastapi` agent skill for the build procedure.
