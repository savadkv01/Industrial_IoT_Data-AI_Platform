# Functional & Business Test Cases

This document defines **behavioural / acceptance test cases** for each phase of the
Industrial IoT Data & AI Platform. These describe *what the system must do* from a
business and data-correctness standpoint — **not** unit/syntax checks. Use them as
acceptance criteria, QA scripts, and demo checklists.

**Legend** — Priority: 🔴 Critical · 🟠 High · 🟡 Medium
Status: ⬜ Not run · ✅ Pass · ❌ Fail · ⏭️ Blocked

---

## Phase 2 — Synthetic Data Generation

| ID | Scenario | Given | When | Then (expected business outcome) | Prio | Status |
|----|----------|-------|------|----------------------------------|:----:|:------:|
| TC-2.1 | Fleet size honoured | A fleet of N machines is requested | The generator runs one tick | Exactly N distinct `machine_id`s emit one record each | 🔴 | ⬜ |
| TC-2.2 | Throughput target | 500 machines at 5 Hz | Generator runs in real-time mode | Sustained rate ≈ 2,500 msg/s (±10%) | 🟠 | ⬜ |
| TC-2.3 | Failures are rare | `FAILURE_INJECTION_RATE = 0.02` | A full run completes | ~2% of machines (not records) exhibit a fault trajectory | 🔴 | ⬜ |
| TC-2.4 | Failure label leads the fault | A degrading machine | It approaches the failure threshold | `failure_within_horizon = true` appears *before* the fault, within the lead-time window | 🔴 | ⬜ |
| TC-2.5 | Healthy machines stay healthy | A non-degrading machine | It runs for a long horizon | `event = "ok"`, `error_code = 0`, no false failure labels | 🟠 | ⬜ |
| TC-2.6 | Signals are physically plausible | Any record | Inspected | `battery_soh ∈ [0,1]`, `cpu_usage ∈ [0,100]`, `motor_temp` rises with load/wear, GPS within fleet region | 🟠 | ⬜ |
| TC-2.7 | Temporal correlation | Consecutive records for one machine | Compared | Values evolve smoothly (no teleporting GPS, no random temp jumps) | 🟡 | ⬜ |
| TC-2.8 | Reproducibility | Same seed + start time | Two runs executed | Byte-identical telemetry produced | 🟠 | ⬜ |
| TC-2.9 | Degradation correlates signals | A failing machine | Failure approaches | Motor temp ↑, vibration ↑, battery SoH ↓ together (not independently) | 🟡 | ⬜ |
| TC-2.10 | Sink delivery — Kafka | Kafka sink selected | Records produced | Messages land on `iot.telemetry`, keyed by `machine_id` | 🔴 | ⬜ |

---

## Phase 3 — Streaming Pipeline (Kafka → Spark → Bronze)

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-3.1 | No data loss | X messages produced to Kafka | Streaming job consumes them | Bronze row count == X (no loss) | 🔴 | ✅ |
| TC-3.2 | Exactly-once on restart | Job mid-stream | Job is killed and restarted from checkpoint | No duplicate rows in Bronze; no gaps | 🔴 | ✅ |
| TC-3.3 | Ordering per machine | Records for one machine | Read back from Bronze | Event-time order preserved within each `machine_id` partition | 🟠 | ⬜ |
| TC-3.4 | Late data handled | Out-of-order/late events | Watermark window applies | Late events accepted within watermark; dropped beyond it as designed | 🟠 | ⬜ |
| TC-3.5 | Ingestion latency SLA | Steady stream | Measured end-to-end | Event → Bronze < 10 s p95 | 🟠 | ⬜ |
| TC-3.6 | Malformed payload isolation | A corrupt/invalid message | Consumed | Bad record quarantined/logged, pipeline keeps running | 🔴 | ⬜ |
| TC-3.7 | Ingestion metadata present | Any Bronze row | Inspected | `_ingest_ts`, `_topic`, `_partition`, `_offset` populated | 🟡 | ✅ |
| TC-3.8 | Backpressure stability | Producer faster than consumer | Lag builds | `maxOffsetsPerTrigger` bounds batches; job stays stable, lag recovers | 🟠 | ⬜ |

---

## Phase 4 — Lakehouse (Bronze → Silver → Gold)

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-4.1 | Bronze is immutable | Bronze data | Silver job runs | Bronze is never mutated; corrections happen in Silver | 🔴 | ✅ |
| TC-4.2 | Deduplication | Duplicate (machine_id, ts) rows in Bronze | Silver processes | Silver has exactly one row per (machine_id, ts) | 🔴 | ✅ |
| TC-4.3 | Type & range enforcement | Raw strings/out-of-range values | Silver validates | Types cast correctly; out-of-range values quarantined or clipped per rule | 🟠 | ✅ |
| TC-4.4 | Data quality gate | A batch failing DQ thresholds (e.g. >5% nulls) | Silver runs | Batch flagged/failed; alert raised; clean data still flows | 🔴 | ⬜ |
| TC-4.5 | Gold aggregation correctness | Known input window | Gold builds rolling stats | Aggregates (5m/1h/24h mean/std) match hand-computed values | 🟠 | ✅ |
| TC-4.6 | Partitioning | Gold/Silver tables | Queried by date + machine | Partition pruning occurs; query scans only relevant partitions | 🟡 | ⬜ |
| TC-4.7 | Schema evolution | A new additive sensor column | Ingested | Pipeline absorbs it via mergeSchema without breaking | 🟠 | ⬜ |
| TC-4.8 | Query access | Gold tables | Queried via Trino & DuckDB | Same row counts/aggregates returned by both engines | 🟡 | ⬜ |
| TC-4.9 | Freshness SLA | Continuous ingestion | Gold checked | Gold updated within 15 min of event time | 🟠 | ⬜ |

---

## Phase 5 — Feature Engineering (Feast)

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-5.1 | Point-in-time correctness | A training entity at time T | `get_historical_features` | Only features with event time ≤ T are returned (no leakage) | 🔴 | ✅ |
| TC-5.2 | Window feature accuracy | Known sensor series | Rolling features computed | Rolling mean/std/min/max match manual calculation | 🟠 | ✅ |
| TC-5.3 | Lag feature alignment | A sensor stream | Lag-N feature requested | Value equals the reading N steps earlier for that machine | 🟠 | ✅ |
| TC-5.4 | Online/offline parity | Same entity + timestamp | Online vs offline fetch | Feature values match between online and offline stores | 🔴 | ✅ |
| TC-5.5 | TTL / staleness | A stale feature beyond TTL | Online lookup | Stale feature is not served | 🟡 | ⬜ |
| TC-5.6 | Event time, not ingestion time | Late-arriving data | Feature built | Feature keyed on telemetry event time, not arrival time | 🟠 | ⬜ |
| TC-5.7 | Materialization completeness | Feature views applied | `materialize` runs | All entities have online features post-materialization | 🟡 | ✅ |

---

## Phase 6 — ML Model Building

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-6.1 | No time leakage | Train/val/test split | Split is built | Splits are time-ordered; no future data in training | 🔴 | ⬜ |
| TC-6.2 | Predictive maintenance quality | Trained classifier | Evaluated on holdout | AUC ≥ 0.85, F1 reported; beats a naive baseline | 🔴 | ⬜ |
| TC-6.3 | Anomaly detection recall | Injected known anomalies | Model scores them | Majority of injected anomalies flagged (recall target met) | 🟠 | ⬜ |
| TC-6.4 | Battery regression error | Battery model | Evaluated | RMSE/MAE within agreed bound; residuals unbiased | 🟠 | ⬜ |
| TC-6.5 | Class imbalance handled | Rare-failure dataset | Training | Imbalance addressed (weighting/sampling); PR-AUC reported | 🟠 | ⬜ |
| TC-6.6 | Reproducible training | Fixed seed + data version | Re-trained | Metrics reproduce within tolerance | 🟡 | ⬜ |

---

## Phase 7 — ML Pipeline (MLOps)

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-7.1 | Experiment tracking | A training run | Executed | Params, metrics, and artifacts logged to MLflow | 🔴 | ⬜ |
| TC-7.2 | Model registry staging | A validated model | Registered | Versioned; promotable Staging → Production | 🔴 | ⬜ |
| TC-7.3 | Promotion gate | A new model worse than Production | Promotion attempted | Promotion blocked unless metric threshold met | 🟠 | ⬜ |
| TC-7.4 | Retraining DAG | Scheduled retrain | DAG runs | New model trained, compared, conditionally promoted | 🟠 | ⬜ |
| TC-7.5 | Rollback | A bad Production model | Rollback triggered | Previous version restored cleanly | 🔴 | ⬜ |
| TC-7.6 | Lineage/reproducibility | A registered model | Inspected | Data version + params recover the exact run | 🟡 | ⬜ |

---

## Phase 8 — Real-time AI

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-8.1 | Streaming anomaly detection | Live anomalous telemetry | Flows through Kafka→Spark→model | Anomaly flagged in near real-time | 🔴 | ⬜ |
| TC-8.2 | Alert emission | A detected anomaly | Scored above threshold | Alert written to `iot.alerts` topic + alert table | 🔴 | ⬜ |
| TC-8.3 | Detection latency | Anomaly occurs | Measured | Detected within target window (e.g. < 60 s) | 🟠 | ⬜ |
| TC-8.4 | No alert storms | Sustained anomaly | Ongoing | Alerts deduplicated/throttled, not one per record | 🟠 | ⬜ |
| TC-8.5 | Model hot-swap | New model version | Promoted | Streaming job picks up new model without data loss | 🟡 | ⬜ |

---

## Phase 9 — Model Serving (FastAPI)

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-9.1 | Prediction latency SLA | A `/predict` request | Served | p95 latency < 100 ms | 🔴 | ⬜ |
| TC-9.2 | Correct prediction contract | A valid feature payload | Posted | Response includes score/probability + model version | 🔴 | ⬜ |
| TC-9.3 | Input validation | A malformed payload | Posted | 422 returned with a clear error; no crash | 🟠 | ⬜ |
| TC-9.4 | Health/readiness | Service starting | `/health` polled | Reports not-ready until model loaded, then ready | 🟠 | ⬜ |
| TC-9.5 | Model version traceability | Any prediction | Returned | Serving model version is identifiable for audit | 🟡 | ⬜ |
| TC-9.6 | Online feature integration | A request needing live features | Served | Features fetched from Feast and used in scoring | 🟡 | ⬜ |
| TC-9.7 | Concurrency | Many simultaneous requests | Load applied | No errors; latency degrades gracefully | 🟠 | ⬜ |

---

## Phase 10 — Monitoring & Drift

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-10.1 | System metrics visible | Running platform | Grafana opened | CPU, Kafka lag, throughput, latency dashboards populated | 🟠 | ⬜ |
| TC-10.2 | Data drift detected | Shifted input distribution | Evidently report runs | Drift flagged with a quantified score | 🔴 | ⬜ |
| TC-10.3 | Model drift / decay | Degrading predictions | Monitored | Performance decay detected and alerted | 🔴 | ⬜ |
| TC-10.4 | Alert thresholds | Kafka lag/latency breach | Threshold crossed | Alert fires | 🟠 | ⬜ |
| TC-10.5 | Closed-loop retrain | Drift alert | Raised | Triggers/recommends retraining DAG | 🟡 | ⬜ |
| TC-10.6 | Reference dataset stability | Drift comparison | Run repeatedly | Uses a stable, versioned reference baseline | 🟡 | ⬜ |

---

## Phase 12 — Production Incident Readiness (resilience scenarios)

| ID | Scenario | Given | When | Then | Prio | Status |
|----|----------|-------|------|------|:----:|:------:|
| TC-12.1 | Kafka lag explosion | Producer spike | Lag grows | System recovers without data loss; lag drains | 🟠 | ⬜ |
| TC-12.2 | Spark memory pressure | Large skewed batch | Processed | Job completes or fails safely with clear diagnostics | 🟠 | ⬜ |
| TC-12.3 | Bad model in production | A regression in predictions | Detected | Drift/perf alert fires; rollback path works | 🔴 | ⬜ |
| TC-12.4 | Data skew | One machine dominates volume | Processed | No single partition stalls the pipeline | 🟡 | ⬜ |
| TC-12.5 | Breaking schema change | Incompatible schema arrives | Ingested | Pipeline rejects safely; alert raised; no silent corruption | 🔴 | ⬜ |

---

## How to use this document
- Treat each row as an **acceptance criterion** for its phase; a phase is "done" when
  its 🔴 Critical cases pass.
- Record evidence (query result, screenshot, metric) per case when executed.
- Keep unit/syntax tests separate (in `tests/`); this file tracks **behaviour**.
