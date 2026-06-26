"""Export the Phase 4 Gold Delta table to a local parquet snapshot.

This is an execution helper for host-side Phase 5 workflows when direct
DuckDB/Delta access to MinIO is not available from the host environment.
"""

from __future__ import annotations

import sys
from pathlib import Path

from lakehouse.config import LakehouseConfig
from lakehouse.gold.build_gold import build_spark


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    if not argv:
        print(
            "usage: python export_gold_snapshot.py <output-parquet-dir>",
            file=sys.stderr,
        )
        return 2

    output_path = Path(argv[0])
    cfg = LakehouseConfig()
    spark = build_spark(cfg)

    print(f"[phase5] exporting Gold from {cfg.gold_path}", file=sys.stderr)
    gold = spark.read.format("delta").load(cfg.gold_path)
    row_count = gold.count()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        gold.write.mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"[phase5] exported {row_count} Gold rows to {output_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())