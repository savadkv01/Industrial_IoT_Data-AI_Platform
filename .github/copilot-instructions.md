# Copilot Instructions — Industrial IoT Data & AI Platform

This repository is an on-premise, Docker-based Data + AI Platform for predictive
maintenance of an industrial machine fleet. See [docs/IMPLEMENTATION_PLAN.md](../docs/IMPLEMENTATION_PLAN.md)
for the phased roadmap and [docs/architecture/architecture.md](../docs/architecture/architecture.md)
for requirements and SLAs.

## How to work in this repo
- Build **one phase at a time**; bring up only the Docker services a phase needs.
- Each technical domain has a dedicated **agent skill** under `.github/skills/` — load the
  matching skill before implementing a phase:
  - `iot-telemetry-generator` (Phase 2) · `streaming-pipeline` (Phases 3, 8) ·
    `lakehouse-medallion` (Phase 4) · `feature-store-feast` (Phase 5) ·
    `mlops-pipeline` (Phases 6-7) · `model-serving-fastapi` (Phase 9) ·
    `platform-monitoring` (Phase 10).

## Conventions
- Python 3.11; one package per domain folder; promote notebook logic into packages.
- Config via `.env` (copy from `.env.example`); never commit secrets.
- Partition Kafka and Delta by `machine_id`; use event time (not ingestion time) for features.
- Lint with `ruff` and test with `pytest` before committing.
- Instrument long-running services with Prometheus metrics.
