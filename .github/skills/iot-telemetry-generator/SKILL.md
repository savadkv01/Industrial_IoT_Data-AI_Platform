---
name: iot-telemetry-generator
description: 'Build or extend the synthetic IoT telemetry generator (Phase 2). Use when creating machine simulators, sensor signals (GPS, IMU, battery, motor temp, CPU, error codes), noise injection, failure/degradation simulation, or Kafka/file sinks for the data-generator package.'
argument-hint: 'e.g. "add battery degradation curve" or "stream 1000 machines to Kafka"'
---

# IoT Telemetry Generator

## When to use
- Implementing or modifying anything under `data-generator/`.
- Adding new sensor signals, failure modes, or output sinks.

## Design principles
- **Per-machine state machine**: each machine holds evolving state (location, wear, battery SoH) so signals are temporally correlated, not random per tick.
- **Physics-ish models**: motor temperature rises with load and ambient; battery SoH decays monotonically with cycles; vibration increases as bearings wear.
- **Labelled failures**: degradation curves cross a threshold → emit `error_code` and a `failure` label N steps ahead (for supervised predictive maintenance).
- **Controlled noise**: add Gaussian sensor noise + occasional dropouts/outliers.

## Procedure
1. Define the telemetry schema in `data_generator/schema.py` (Pydantic): `machine_id`, `ts`, `lat`, `lon`, `speed`, `accel_x/y/z`, `battery_soh`, `motor_temp`, `cpu_usage`, `error_code`, `event`.
2. Implement `Machine` in `machine.py`: `step(dt)` advances internal state and returns one telemetry record.
3. Implement degradation/failure logic in `failure.py`: per-machine hazard rate, gradual drift, threshold crossing → failure label with a configurable lead time.
4. Implement sinks in `sinks/`: `kafka_sink.py` (key = `machine_id` for partition ordering) and `file_sink.py` (parquet to MinIO).
5. Build `main.py` CLI: `--machines`, `--rate`, `--duration`, `--sink {kafka,file}`; run an async/event loop ticking all machines.
6. Validate: confirm message rate ≈ `machines × rate`, failures are rare (~2%), and signals look realistic in a quick notebook plot.

## Guardrails
- Keep failure rate low and configurable via `FAILURE_INJECTION_RATE`.
- Make randomness reproducible with a seed for tests.
- Never block the event loop on slow sinks — batch/async produce.
