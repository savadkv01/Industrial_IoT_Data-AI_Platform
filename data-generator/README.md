# Data Generator — Phase 2

Synthetic telemetry generator simulating a fleet of 500–2000 industrial machines.

## Responsibilities
- Emit realistic time-series telemetry: GPS, speed, IMU acceleration, battery health, motor temperature, CPU usage, error codes, event logs.
- Inject controlled noise and correlated sensor drift.
- Simulate degradation patterns that lead to failures (labels for predictive maintenance).
- Stream records to Kafka (`iot.telemetry`) or write batch files to MinIO.

## Planned layout
```
data_generator/
├── main.py            # CLI entrypoint (--machines, --rate, --duration)
├── machine.py         # Per-machine state machine & physics-ish model
├── failure.py         # Failure injection & degradation curves
├── schema.py          # Pydantic telemetry schema
└── sinks/
    ├── kafka_sink.py
    └── file_sink.py
```

See the `iot-telemetry-generator` agent skill for the build procedure.
