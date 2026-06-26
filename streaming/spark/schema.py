"""Spark schema for telemetry, built from the shared field specification.

Building the ``StructType`` from :data:`streaming.fields.TELEMETRY_FIELDS` guarantees
the Spark parse schema stays in lock-step with the generator's Pydantic contract.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.fields import TELEMETRY_FIELDS

_TYPE_MAP = {
    "string": StringType(),
    "double": DoubleType(),
    "integer": IntegerType(),
    "boolean": BooleanType(),
    "timestamp": TimestampType(),
}


def telemetry_schema() -> StructType:
    """Return the Spark ``StructType`` mirroring ``TelemetryRecord``."""
    return StructType(
        [StructField(name, _TYPE_MAP[type_name], True) for name, type_name in TELEMETRY_FIELDS]
    )
