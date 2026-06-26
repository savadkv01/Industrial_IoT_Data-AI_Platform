"""Batch verification of the Bronze table — supports the Phase 3 functional tests.

Reads the Bronze (and quarantine) Delta tables and prints:
  * total Bronze row count                       (TC-3.1 no data loss)
  * distinct (machine_id, ts) count + duplicates (TC-3.2 exactly-once)
  * quarantine row count                         (TC-3.6 malformed isolation)
  * ingestion-metadata completeness              (TC-3.7)

Run inside the spark container::

    docker compose exec spark-streaming bash /opt/app/streaming/spark/submit_verify.sh
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

from streaming.config import StreamingConfig
from streaming.spark.ingest_bronze import build_spark


def main() -> int:
    cfg = StreamingConfig()
    spark = build_spark(cfg)

    try:
        bronze = spark.read.format("delta").load(cfg.bronze_path)
    except Exception as exc:  # noqa: BLE001
        print(f"[verify] could not read Bronze at {cfg.bronze_path}: {exc}", file=sys.stderr)
        return 1

    total = bronze.count()
    distinct_keys = bronze.select("machine_id", "ts").distinct().count()
    duplicates = total - distinct_keys

    meta_complete = (
        bronze.filter(
            F.col("_ingest_ts").isNotNull()
            & F.col("_topic").isNotNull()
            & F.col("_offset").isNotNull()
        ).count()
        == total
    )

    try:
        quarantine = spark.read.format("delta").load(cfg.quarantine_path).count()
    except Exception:  # noqa: BLE001
        quarantine = 0

    print("──────── Bronze verification ────────", file=sys.stderr)
    print(f"  total rows                : {total}", file=sys.stderr)
    print(f"  distinct (machine_id, ts) : {distinct_keys}", file=sys.stderr)
    print(f"  duplicate rows            : {duplicates}", file=sys.stderr)
    print(f"  ingest metadata complete  : {meta_complete}", file=sys.stderr)
    print(f"  quarantined (malformed)   : {quarantine}", file=sys.stderr)
    print("─────────────────────────────────────", file=sys.stderr)

    # Exactly-once expectation: no duplicate business keys.
    return 0 if duplicates == 0 and meta_complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
