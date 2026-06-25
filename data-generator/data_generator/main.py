"""CLI entrypoint for the telemetry generator.

Examples
--------
Stream 500 machines at 5 Hz to Kafka::

    python -m data_generator.main --machines 500 --rate 5 --sink kafka

Write 10 seconds of data to a file for inspection::

    python -m data_generator.main --machines 50 --rate 5 --duration 10 --sink file
"""

from __future__ import annotations

import argparse
import dataclasses
import signal
import sys
import time

from .config import GeneratorConfig
from .fleet import Fleet
from .sinks import build_sink


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="data_generator",
        description="Synthetic IoT telemetry generator (Phase 2).",
    )
    parser.add_argument("--machines", type=int, help="Number of machines in the fleet.")
    parser.add_argument("--rate", type=float, help="Telemetry rate per machine in Hz.")
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Run duration in seconds (0 = run until interrupted).",
    )
    parser.add_argument(
        "--sink",
        choices=["stdout", "file", "kafka"],
        default="stdout",
        help="Where to emit telemetry.",
    )
    parser.add_argument("--path", default="data/telemetry.ndjson", help="Output path for the file sink.")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility.")
    parser.add_argument(
        "--realtime",
        action="store_true",
        help="Sleep between ticks to emit at wall-clock rate (default: as fast as possible).",
    )
    return parser.parse_args(argv)


def _build_config(args: argparse.Namespace) -> GeneratorConfig:
    base = GeneratorConfig()
    overrides = {}
    if args.machines is not None:
        overrides["fleet_size"] = args.machines
    if args.rate is not None:
        overrides["rate_hz"] = args.rate
    if args.seed is not None:
        overrides["seed"] = args.seed
    return dataclasses.replace(base, **overrides) if overrides else base


def run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = _build_config(args)

    sink_kwargs: dict = {}
    if args.sink == "file":
        sink_kwargs["path"] = args.path
    elif args.sink == "kafka":
        sink_kwargs["bootstrap_servers"] = config.bootstrap_servers
        sink_kwargs["topic"] = config.telemetry_topic

    fleet = Fleet(config)
    sink = build_sink(args.sink, **sink_kwargs)

    stop = {"flag": False}

    def _handle_signal(signum, frame):  # noqa: ARG001
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print(
        f"[generator] fleet={config.fleet_size} rate={config.rate_hz}Hz "
        f"sink={args.sink} duration={args.duration or '∞'}s seed={config.seed}",
        file=sys.stderr,
    )

    start = time.monotonic()
    total = 0
    ticks = 0
    try:
        with sink:
            while not stop["flag"]:
                tick_start = time.monotonic()
                for record in fleet.tick():
                    sink.write(record)
                    total += 1
                ticks += 1

                elapsed = time.monotonic() - start
                if args.duration and elapsed >= args.duration:
                    break

                if args.realtime:
                    sleep_for = config.tick_seconds - (time.monotonic() - tick_start)
                    if sleep_for > 0:
                        time.sleep(sleep_for)
    finally:
        elapsed = max(time.monotonic() - start, 1e-9)
        print(
            f"[generator] emitted {total} records over {ticks} ticks "
            f"in {elapsed:.2f}s (~{total / elapsed:.0f} msg/s)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
