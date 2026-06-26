"""Spark Structured Streaming pipeline (Phase 3).

Ingests telemetry from Kafka into the Bronze Delta layer on MinIO with
exactly-once semantics, checkpointing, and watermarking.
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
