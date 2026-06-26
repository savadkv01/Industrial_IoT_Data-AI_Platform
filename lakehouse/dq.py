"""Declarative data quality rules for the Silver layer (Phase 4).

Rule definitions are pure Python (no PySpark import at module level) so unit
tests run without Spark.  The :func:`run_dq` function imports PySpark lazily at
call time and computes all metrics in a single aggregation pass.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Rule definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DQRule:
    """Field-level validation rule."""

    column: str
    nullable: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None


# Derived from TelemetryRecord definitions in data_generator.schema.
# Keep in sync with streaming.fields.TELEMETRY_FIELDS.
SILVER_DQ_RULES: tuple[DQRule, ...] = (
    DQRule("machine_id", nullable=False),
    DQRule("ts", nullable=False),
    DQRule("lat", nullable=False, min_value=-90.0, max_value=90.0),
    DQRule("lon", nullable=False, min_value=-180.0, max_value=180.0),
    DQRule("speed", nullable=True, min_value=0.0),
    DQRule("vibration", nullable=True, min_value=0.0),
    DQRule("battery_soh", nullable=True, min_value=0.0, max_value=1.0),
    DQRule("cpu_usage", nullable=True, min_value=0.0, max_value=100.0),
    DQRule("motor_temp", nullable=True),
    DQRule("error_code", nullable=True),
)


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass
class DQResult:
    """Outcome of a DQ check pass."""

    total_rows: int
    valid_rows: int
    quarantine_rows: int
    null_violations: dict[str, float]   # column → observed null rate
    range_violations: dict[str, int]    # column → violation row count
    passed: bool

    @property
    def quarantine_rate(self) -> float:
        return self.quarantine_rows / self.total_rows if self.total_rows else 0.0

    def print_report(self, file=sys.stderr) -> None:
        print("──────── DQ Report ────────", file=file)
        print(f"  total rows      : {self.total_rows}", file=file)
        print(f"  valid rows      : {self.valid_rows}", file=file)
        print(
            f"  quarantine rows : {self.quarantine_rows} ({self.quarantine_rate:.2%})",
            file=file,
        )
        for col, rate in self.null_violations.items():
            print(f"  null violation  : {col} = {rate:.2%}", file=file)
        for col, count in self.range_violations.items():
            print(f"  range violation : {col} = {count} rows", file=file)
        print(f"  passed          : {self.passed}", file=file)
        print("───────────────────────────", file=file)


# ---------------------------------------------------------------------------
# Spark execution helpers (PySpark imported lazily)
# ---------------------------------------------------------------------------


def violation_expr(rules: tuple[DQRule, ...] = SILVER_DQ_RULES):
    """Return a Spark Column expression that is True for rows violating ANY rule.

    Imported lazily so the module is safe to import in pure-Python test contexts.
    """
    from pyspark.sql import functions as F

    conditions = []
    for rule in rules:
        if not rule.nullable:
            conditions.append(F.col(rule.column).isNull())
        if rule.min_value is not None:
            conditions.append(
                F.col(rule.column).isNotNull() & (F.col(rule.column) < rule.min_value)
            )
        if rule.max_value is not None:
            conditions.append(
                F.col(rule.column).isNotNull() & (F.col(rule.column) > rule.max_value)
            )
    if not conditions:
        return F.lit(False)
    result = conditions[0]
    for c in conditions[1:]:
        result = result | c
    return result


def run_dq(
    df,  # pyspark.sql.DataFrame
    rules: tuple[DQRule, ...] = SILVER_DQ_RULES,
    max_null_rate: float = 0.05,
    max_range_violation_rate: float = 0.01,
) -> tuple:  # (valid_df, quarantine_df, DQResult)
    """Split a DataFrame into (valid, quarantine) and return a DQResult.

    Computes all column metrics in a single aggregation action, then persists the
    source DataFrame so that the valid/quarantine filter passes scan the cache only.
    """
    from pyspark.sql import functions as F

    df = df.persist()

    # Single aggregation pass: total count + per-rule null/range counts.
    agg_exprs = [F.count("*").alias("_total")]
    for rule in rules:
        if not rule.nullable:
            agg_exprs.append(
                F.sum(F.col(rule.column).isNull().cast("int")).alias(f"_null_{rule.column}")
            )
        if rule.min_value is not None:
            agg_exprs.append(
                F.sum(
                    (
                        F.col(rule.column).isNotNull()
                        & (F.col(rule.column) < rule.min_value)
                    ).cast("int")
                ).alias(f"_min_{rule.column}")
            )
        if rule.max_value is not None:
            agg_exprs.append(
                F.sum(
                    (
                        F.col(rule.column).isNotNull()
                        & (F.col(rule.column) > rule.max_value)
                    ).cast("int")
                ).alias(f"_max_{rule.column}")
            )

    stats_row = df.agg(*agg_exprs).collect()[0]
    total: int = stats_row["_total"] or 0

    null_violations: dict[str, float] = {}
    range_violations: dict[str, int] = {}

    for rule in rules:
        if not rule.nullable:
            null_count = stats_row[f"_null_{rule.column}"] or 0
            if null_count > 0:
                null_violations[rule.column] = null_count / total if total else 0.0
        if rule.min_value is not None:
            cnt = stats_row[f"_min_{rule.column}"] or 0
            if cnt > 0:
                range_violations[rule.column] = range_violations.get(rule.column, 0) + cnt
        if rule.max_value is not None:
            cnt = stats_row[f"_max_{rule.column}"] or 0
            if cnt > 0:
                range_violations[rule.column] = range_violations.get(rule.column, 0) + cnt

    is_violation = violation_expr(rules)
    valid_df = df.filter(~is_violation)
    quarantine_df = df.filter(is_violation)

    valid_count = valid_df.count()

    passed = True
    for rate in null_violations.values():
        if rate > max_null_rate:
            passed = False
    if total > 0:
        for col_count in range_violations.values():
            if col_count / total > max_range_violation_rate:
                passed = False

    result = DQResult(
        total_rows=total,
        valid_rows=valid_count,
        quarantine_rows=total - valid_count,
        null_violations=null_violations,
        range_violations=range_violations,
        passed=passed,
    )

    df.unpersist()
    return valid_df, quarantine_df, result
