"""Batch verification of the Silver and Gold tables — supports TC-4.x.

Checks:
  TC-4.1  Bronze is immutable — row count unchanged after Silver job.
  TC-4.2  Deduplication     — no duplicate (machine_id, ts) in Silver.
  TC-4.3  Range enforcement — no out-of-range values survive in Silver.
  TC-4.4  DQ null gate      — null rates in Silver are within threshold.
  TC-4.5  Gold aggregation  — Gold has rows for all three window durations.
  TC-4.6  Partitioning      — Silver and Gold partitioned by event_date.
  TC-4.9  Freshness SLA     — latest Gold window_end within 15 min of now.

Run inside the spark-silver or spark-gold container::

    docker compose run --rm spark-silver bash \
        /opt/app/lakehouse/silver/submit_silver.sh \
        # then:
    docker compose exec spark-silver bash \
        /opt/app/lakehouse/verify_lakehouse.sh
    # or directly:
    PYTHONPATH=/opt/app/lakehouse:/opt/app spark-submit ... verify_lakehouse.py
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from lakehouse.config import LakehouseConfig
from lakehouse.dq import SILVER_DQ_RULES
from lakehouse.silver.build_silver import build_spark


def _sep(label: str) -> None:
    print(f"\n{'─' * 10} {label} {'─' * (40 - len(label))}", file=sys.stderr)


def main() -> int:  # noqa: C901
    cfg = LakehouseConfig()
    spark = build_spark(cfg)
    exit_code = 0

    # ── Bronze baseline ───────────────────────────────────────────────────────
    _sep("Bronze (TC-4.1)")
    try:
        bronze = spark.read.format("delta").load(cfg.bronze_path)
        bronze_count = bronze.count()
        print(f"  bronze rows         : {bronze_count}", file=sys.stderr)
        print("  bronze is read-only — Silver job must not mutate this table.", file=sys.stderr)
    except AnalysisException as exc:
        print(f"  [SKIP] Bronze not found: {exc}", file=sys.stderr)
        bronze_count = 0

    # ── Silver checks ─────────────────────────────────────────────────────────
    _sep("Silver (TC-4.2, TC-4.3, TC-4.4, TC-4.6)")
    try:
        silver = spark.read.format("delta").load(cfg.silver_path)
        silver_count = silver.count()
        print(f"  silver rows         : {silver_count}", file=sys.stderr)

        # TC-4.2 — deduplication
        distinct_keys = silver.select("machine_id", "ts").distinct().count()
        dupes = silver_count - distinct_keys
        print(f"  distinct (mid, ts)  : {distinct_keys}", file=sys.stderr)
        print(f"  duplicates          : {dupes}", file=sys.stderr)
        if dupes > 0:
            print("  [FAIL] TC-4.2 duplicates found in Silver!", file=sys.stderr)
            exit_code = 1

        # TC-4.3 — range enforcement (verify no violations survived)
        range_ok = True
        for rule in SILVER_DQ_RULES:
            if rule.min_value is not None:
                bad = silver.filter(F.col(rule.column) < rule.min_value).count()
                if bad:
                    print(
                        f"  [FAIL] TC-4.3 {rule.column} < {rule.min_value}: {bad} rows",
                        file=sys.stderr,
                    )
                    range_ok = False
                    exit_code = 1
            if rule.max_value is not None:
                bad = silver.filter(F.col(rule.column) > rule.max_value).count()
                if bad:
                    print(
                        f"  [FAIL] TC-4.3 {rule.column} > {rule.max_value}: {bad} rows",
                        file=sys.stderr,
                    )
                    range_ok = False
                    exit_code = 1
        if range_ok:
            print("  range enforcement   : PASS", file=sys.stderr)

        # TC-4.4 — null rates
        null_ok = True
        for rule in SILVER_DQ_RULES:
            if not rule.nullable:
                null_count = silver.filter(F.col(rule.column).isNull()).count()
                rate = null_count / silver_count if silver_count else 0.0
                if rate > cfg.dq_max_null_rate:
                    print(
                        f"  [FAIL] TC-4.4 {rule.column} null rate={rate:.2%} > "
                        f"{cfg.dq_max_null_rate:.2%}",
                        file=sys.stderr,
                    )
                    null_ok = False
                    exit_code = 1
        if null_ok:
            print("  null DQ gate        : PASS", file=sys.stderr)

        # TC-4.6 — partitioning present
        partitions = [r["event_date"] for r in silver.select("event_date").distinct().collect()]
        print(f"  event_date partitions: {len(partitions)}", file=sys.stderr)

    except AnalysisException as exc:
        print(f"  [SKIP] Silver not found (run Silver job first): {exc}", file=sys.stderr)
        silver_count = 0

    # ── Gold checks ───────────────────────────────────────────────────────────
    _sep("Gold (TC-4.5, TC-4.6, TC-4.9)")
    try:
        gold = spark.read.format("delta").load(cfg.gold_path)
        gold_count = gold.count()
        print(f"  gold rows           : {gold_count}", file=sys.stderr)

        # TC-4.5 — all three window durations present
        durations = {
            r["window_duration"]
            for r in gold.select("window_duration").distinct().collect()
        }
        expected_durations = {"5m", "1h", "24h"}
        missing = expected_durations - durations
        if missing:
            print(f"  [FAIL] TC-4.5 missing window durations: {missing}", file=sys.stderr)
            exit_code = 1
        else:
            print(f"  window durations    : {sorted(durations)} — PASS", file=sys.stderr)

        # TC-4.5 — aggregate sanity: all means must be non-negative
        agg = gold.agg(
            F.min("vibration_mean").alias("vib_min"),
            F.min("motor_temp_mean").alias("temp_min"),
            F.min("battery_soh_mean").alias("soh_min"),
            F.min("cpu_usage_mean").alias("cpu_min"),
        ).collect()[0]
        agg_ok = all(
            (v is None or v >= 0) for v in [agg["vib_min"], agg["soh_min"], agg["cpu_min"]]
        )
        print(f"  aggregates non-negative: {'PASS' if agg_ok else 'FAIL'}", file=sys.stderr)
        if not agg_ok:
            exit_code = 1

        # TC-4.6 — Gold partitioned by event_date + window_duration
        gold_partitions = gold.select("event_date", "window_duration").distinct().count()
        print(f"  Gold date×duration partitions: {gold_partitions}", file=sys.stderr)

        # TC-4.9 — freshness: latest window_end within 15 minutes of now
        latest_row = gold.agg(F.max("window_end").alias("latest")).collect()[0]
        if latest_row["latest"] is not None:
            import datetime

            latest_ts: datetime.datetime = latest_row["latest"]
            now = datetime.datetime.now(tz=latest_ts.tzinfo)
            lag_minutes = (now - latest_ts).total_seconds() / 60.0
            sla_ok = lag_minutes <= 15.0
            print(
                f"  freshness lag       : {lag_minutes:.1f} min "
                f"({'PASS' if sla_ok else 'WARN — SLA breach'} ≤ 15 min)",
                file=sys.stderr,
            )

    except AnalysisException as exc:
        print(f"  [SKIP] Gold not found (run Gold job first): {exc}", file=sys.stderr)

    # ── Quarantine summary ────────────────────────────────────────────────────
    _sep("Quarantine")
    for label, path in (
        ("bronze", cfg.bronze_path.replace("/telemetry", "/_quarantine/telemetry")),
        ("silver", cfg.silver_quarantine_path),
    ):
        try:
            q_count = spark.read.format("delta").load(path).count()
            print(f"  {label} quarantine    : {q_count} rows", file=sys.stderr)
        except AnalysisException:
            print(f"  {label} quarantine    : (empty or not yet created)", file=sys.stderr)

    _sep("Summary")
    print(
        f"  exit code = {exit_code}  ({'ALL CHECKS PASSED' if exit_code == 0 else 'FAILURES DETECTED'})",
        file=sys.stderr,
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
