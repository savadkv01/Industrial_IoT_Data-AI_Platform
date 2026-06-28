"""Pydantic request/response schemas for the serving API (Phase 9).

Validation happens at the boundary: malformed payloads are rejected with HTTP 422
before any model is touched. Every response echoes the serving model name, alias and
version so a prediction can always be traced back to the exact artifact that produced it.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class FeatureRecord(BaseModel):
    """One machine feature window to score.

    ``features`` is an open mapping of engineered feature name -> value. The service
    aligns it to the model's trained feature space at scoring time, so callers only need
    to send the features they have; the rest are filled with the configured default.
    """

    machine_id: str = Field(..., min_length=1, description="Machine identifier.")
    event_timestamp: datetime | None = Field(
        default=None, description="Event time of the feature window (UTC)."
    )
    features: dict[str, float] = Field(
        ..., min_length=1, description="Engineered feature name -> numeric value."
    )


class PredictRequest(BaseModel):
    """Batch of one or more feature windows to score in a single call."""

    records: list[FeatureRecord] = Field(..., min_length=1, max_length=1000)


class Prediction(BaseModel):
    """Single scored record."""

    machine_id: str
    event_timestamp: datetime | None
    score: float


class PredictResponse(BaseModel):
    """Scored batch plus the identity of the model that produced it."""

    task: str
    model_name: str
    model_alias: str
    model_version: str
    output: str = Field(..., description="Name of the quantity the score represents.")
    predictions: list[Prediction]


class ModelStatus(BaseModel):
    task: str
    loaded: bool
    model_name: str
    model_alias: str
    model_version: str | None = None
    error: str | None = None


class HealthResponse(BaseModel):
    status: str
    models: list[ModelStatus]
