# Monitoring & Observability — Phase 10

System, data, and model monitoring.

## Stack
- **Prometheus** — scrape system + serving metrics (CPU, Kafka lag, latency, throughput).
- **Grafana** — dashboards for platform health and ML metrics.
- **Evidently AI** — data drift and model drift reports.

## Planned layout
```
monitoring/
├── prometheus/
│   └── prometheus.yml
├── grafana/
│   └── dashboards/
└── drift/
    └── evidently_reports.py
```

See the `platform-monitoring` agent skill for the build procedure.
