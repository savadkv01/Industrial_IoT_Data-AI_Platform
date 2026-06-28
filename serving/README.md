# Model Serving — Phase 9

FastAPI inference service that loads the **production** models from the MLflow
registry once at startup and serves low-latency predictions for the three model
families. Online and batch (`ml.inference.batch`) paths share the same registered
models and `ml.common.tasks` specs, so scores stay consistent across both.

## Endpoints
- `GET  /health` — readiness; reports per-task model load state + served version.
- `POST /predict/maintenance` — failure probability (`failure_probability`).
- `POST /predict/battery` — battery state-of-health estimate (`battery_soh_prediction`).
- `POST /predict/anomaly` — anomaly score, higher = more anomalous (`anomaly_score`).
- `POST /reload` — drop cached models and re-resolve aliases (e.g. after a promotion).
- `GET  /metrics` — Prometheus metrics (request count, latency, score distribution).
- `GET  /docs` — interactive OpenAPI docs.

## Request / response

```jsonc
// POST /predict/maintenance
{
  "records": [
    {
      "machine_id": "machine-001",
      "event_timestamp": "2026-01-01T00:00:00Z",
      "features": { "motor_temp_mean": 90.0, "vibration_mean": 3.0 }
    }
  ]
}
```

```jsonc
{
  "task": "predictive_maintenance",
  "model_name": "iiot_predictive_maintenance",
  "model_alias": "production",
  "model_version": "7",
  "output": "failure_probability",
  "predictions": [
    { "machine_id": "machine-001", "event_timestamp": "2026-01-01T00:00:00Z", "score": 0.83 }
  ]
}
```

`features` is an open mapping of engineered feature name → value. The service aligns
it to each model's trained feature space (`feature_names_in_`): unknown extras are
dropped and missing features are filled with `SERVING_MISSING_FEATURE_FILL` (default
`0.0`). Every response echoes the model name/alias/version for traceability.

## Layout
```
serving/
├── app/
│   ├── main.py      # FastAPI app, endpoints, lifespan warm-start
│   ├── models.py    # MLflow registry loader + in-memory ModelCache + scoring
│   ├── schemas.py   # request/response Pydantic models
│   ├── metrics.py   # Prometheus instrumentation
│   └── config.py    # serving-specific settings
├── tests/
├── Dockerfile
└── pyproject.toml
```

## Configuration
| Env var | Default | Meaning |
| --- | --- | --- |
| `MLFLOW_TRACKING_URI` | local sqlite | MLflow tracking + registry URI. |
| `SERVING_MODEL_ALIAS` | `production` | Registry alias to serve. |
| `SERVING_WARM_START` | `true` | Load all models at startup. |
| `SERVING_MISSING_FEATURE_FILL` | `0.0` | Value for features absent from a request. |

## Run

Local (models resolved from the configured MLflow registry):
```pwsh
cd serving
..\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Docker (build context is the repo root so the image bundles the shared `ml` package):
```pwsh
docker compose up -d --build serving
# http://localhost:8000/docs
```

## Tests
```pwsh
cd serving
..\.venv\Scripts\python.exe -m pytest -q
```

See the `model-serving-fastapi` agent skill for the build procedure.
