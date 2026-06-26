"""DuckDB smoke test for the Gold Delta table (TC-4.8).

Uses DuckDB's native ``delta`` extension (delta-rs under the hood, DuckDB >= 0.10)
to scan the Gold table directly from MinIO and validate row counts and aggregate
sanity.

Run from the host with MinIO accessible::

    python -m lakehouse.query.duckdb_smoke

Environment overrides (same as LakehouseConfig):
    MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, S3_BUCKET_LAKEHOUSE
"""

from __future__ import annotations

import sys

from lakehouse.config import LakehouseConfig


def main() -> int:
    cfg = LakehouseConfig()

    try:
        import duckdb
    except ImportError:
        print(
            "[duckdb-smoke] duckdb not installed. "
            "Run: pip install 'duckdb>=0.10'",
            file=sys.stderr,
        )
        return 2

    con = duckdb.connect()

    # Install delta + httpfs extensions (cached after first run).
    con.execute("INSTALL delta; LOAD delta;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Configure S3-compatible MinIO access.
    endpoint = cfg.minio_endpoint.replace("http://", "").replace("https://", "")
    use_ssl = cfg.minio_endpoint.startswith("https://")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{cfg.minio_access_key}';")
    con.execute(f"SET s3_secret_access_key='{cfg.minio_secret_key}';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
    con.execute("SET s3_url_style='path';")

    gold_url = f"s3://{cfg.bucket}/gold/telemetry_features"

    print(f"[duckdb-smoke] scanning Gold at {gold_url}", file=sys.stderr)

    try:
        row_count = con.execute(
            f"SELECT COUNT(*) FROM delta_scan('{gold_url}')"
        ).fetchone()[0]
    except Exception as exc:
        print(f"[duckdb-smoke] could not read Gold: {exc}", file=sys.stderr)
        return 1

    print(f"[duckdb-smoke] Gold row count: {row_count}", file=sys.stderr)

    if row_count == 0:
        print("[duckdb-smoke] Gold is empty — run Gold job first.", file=sys.stderr)
        return 1

    # Per-duration summary: TC-4.8 check.
    summary = con.execute(f"""
        SELECT
            window_duration,
            COUNT(*)                             AS windows,
            ROUND(AVG(vibration_mean), 4)        AS avg_vibration_mean,
            ROUND(AVG(battery_soh_mean), 4)      AS avg_battery_soh,
            SUM(error_count)                     AS total_errors,
            SUM(CAST(failure_label AS INTEGER))  AS failure_windows
        FROM delta_scan('{gold_url}')
        GROUP BY window_duration
        ORDER BY window_duration
    """).fetchall()

    print(
        f"\n{'duration':<10} {'windows':>8} {'avg_vib':>10} "
        f"{'avg_soh':>10} {'errors':>8} {'fail_windows':>13}",
        file=sys.stderr,
    )
    for row in summary:
        print(
            f"{row[0]:<10} {row[1]:>8} {row[2]:>10} "
            f"{row[3]:>10} {row[4]:>8} {row[5]:>13}",
            file=sys.stderr,
        )

    expected_durations = {"5m", "1h", "24h"}
    found_durations = {row[0] for row in summary}
    missing = expected_durations - found_durations
    if missing:
        print(f"\n[FAIL] missing window durations in Gold: {missing}", file=sys.stderr)
        return 1

    print("\n[duckdb-smoke] TC-4.8 PASS — all window durations present.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
