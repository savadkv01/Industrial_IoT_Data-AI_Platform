# Data Generator — Phase 2

Synthetic telemetry generator simulating a fleet of 500–2000 industrial machines.
Each machine is a stateful simulator so signals are temporally correlated, with a
hidden wear process that drives realistic degradation and **labelled failures** for
supervised predictive maintenance.

## Features
- Correlated sensor signals: GPS, speed, IMU acceleration, vibration, battery SoH,
  motor temperature, CPU usage, error codes, events.
- Configurable, rare failure injection (`FAILURE_INJECTION_RATE`) with a forward-looking
  `failure_within_horizon` label.
- Reproducible runs via a fixed seed.
- Pluggable sinks: `stdout`, `file` (NDJSON), `kafka` (keyed by `machine_id`).

## Layout
```
data-generator/
├── data_generator/
│   ├── schema.py        # Pydantic TelemetryRecord (the shared contract)
│   ├── config.py        # env/.env-driven GeneratorConfig
│   ├── failure.py       # wear & degradation model + fault labels
│   ├── machine.py       # per-machine state machine (step -> record)
│   ├── fleet.py         # builds & ticks the whole fleet
│   ├── main.py          # CLI entrypoint
│   └── sinks/           # stdout / file / kafka outputs
├── tests/               # pytest unit tests
└── pyproject.toml
```

## Usage
```bash
cd data-generator

# Smoke test to stdout
python -m data_generator.main --machines 10 --rate 5 --duration 2 --sink stdout

# Write NDJSON to a file
python -m data_generator.main --machines 50 --rate 5 --duration 10 --sink file --path data/telemetry.ndjson

# Stream to Kafka (requires the kafka service + confluent-kafka installed)
python -m data_generator.main --machines 500 --rate 5 --sink kafka

# Emit at wall-clock rate instead of as-fast-as-possible
python -m data_generator.main --machines 500 --rate 5 --sink kafka --realtime
```

### CLI flags
| Flag | Default | Description |
|---|---|---|
| `--machines` | `FLEET_SIZE` (500) | Number of machines |
| `--rate` | `TELEMETRY_RATE_HZ` (5) | Readings per machine per second |
| `--duration` | 0 (∞) | Run length in seconds; 0 runs until Ctrl-C |
| `--sink` | `stdout` | `stdout` \| `file` \| `kafka` |
| `--path` | `data/telemetry.ndjson` | Output path for the file sink |
| `--seed` | `GENERATOR_SEED` (42) | Random seed |
| `--realtime` | off | Throttle to wall-clock rate |

## Tests
```bash
cd data-generator
python -m pytest -q
```

See the `iot-telemetry-generator` agent skill for design rationale and extension steps.
