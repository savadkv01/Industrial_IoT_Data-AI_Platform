"""Unit tests for the telemetry generator."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

from data_generator.config import GeneratorConfig
from data_generator.fleet import Fleet
from data_generator.machine import Machine
from data_generator.schema import TelemetryRecord
from data_generator.sinks import FileSink, StdoutSink, build_sink

import random


def _config(**overrides) -> GeneratorConfig:
    base = GeneratorConfig(fleet_size=20, rate_hz=5.0, failure_injection_rate=0.2, seed=7)
    return dataclasses.replace(base, **overrides)


def test_machine_step_returns_valid_record():
    machine = Machine("machine-00001", random.Random(1), is_degrading=False, lead_time_steps=60)
    record = machine.step(1.0)
    assert isinstance(record, TelemetryRecord)
    assert 0.0 <= record.battery_soh <= 1.0
    assert 0.0 <= record.cpu_usage <= 100.0
    assert record.machine_id == "machine-00001"


def test_fleet_tick_count_matches_fleet_size():
    fleet = Fleet(_config(fleet_size=15))
    records = list(fleet.tick())
    assert len(records) == 15
    assert len({r.machine_id for r in records}) == 15


def test_reproducible_with_same_seed():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    a = list(Fleet(_config(), start_time=start).tick())
    b = list(Fleet(_config(), start_time=start).tick())
    assert [r.model_dump() for r in a] == [r.model_dump() for r in b]


def test_degrading_machine_eventually_fails():
    machine = Machine("m", random.Random(3), is_degrading=True, lead_time_steps=60)
    failed = False
    for _ in range(5000):
        rec = machine.step(1.0)
        if rec.event == "fault":
            failed = True
            assert rec.error_code != 0
            assert rec.failure_within_horizon is True
            break
    assert failed, "degrading machine should reach a fault within the horizon"


def test_healthy_machine_stays_ok():
    machine = Machine("m", random.Random(4), is_degrading=False, lead_time_steps=60)
    for _ in range(1000):
        rec = machine.step(1.0)
    assert rec.event == "ok"
    assert rec.error_code == 0
    assert rec.failure_within_horizon is False


def test_failure_rate_is_low():
    fleet = Fleet(_config(fleet_size=100, failure_injection_rate=0.02))
    degrading = sum(1 for m in fleet.machines if m._degradation.is_degrading)
    assert degrading == 2


def test_file_sink_writes_lines(tmp_path):
    path = tmp_path / "out.ndjson"
    fleet = Fleet(_config(fleet_size=5))
    with FileSink(path=str(path)) as sink:
        for record in fleet.tick():
            sink.write(record)
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 5


def test_build_sink_factory(tmp_path):
    assert isinstance(build_sink("stdout"), StdoutSink)
    assert isinstance(build_sink("file", path=str(tmp_path / "x.ndjson")), FileSink)


def test_record_json_roundtrip():
    machine = Machine("m", random.Random(9), is_degrading=False, lead_time_steps=60)
    record = machine.step(1.0)
    restored = TelemetryRecord.model_validate_json(record.to_json())
    assert restored.machine_id == record.machine_id
    assert restored.motor_temp == record.motor_temp
