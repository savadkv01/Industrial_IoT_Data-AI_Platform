"""MLflow Model Registry helpers — register versions and gate-promote by metric (Phase 7).

The platform uses **aliases** rather than the deprecated stage API (mlflow 3.x). A newly
trained model is always registered as a new version and tagged with the ``staging`` alias.
It is promoted to the ``production`` alias only when it (a) clears an absolute quality gate
and (b) beats the current production model on the task's primary metric. This keeps a noisy
single run from displacing a good incumbent.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from mlflow.tracking import MlflowClient


@dataclass(frozen=True)
class PromotionDecision:
    """Outcome of evaluating a freshly registered candidate against production."""

    registered_model: str
    version: str
    candidate_metric: float
    production_metric: float | None
    promoted: bool
    staged: bool
    reason: str


def _is_finite(value: float | None) -> bool:
    return value is not None and isinstance(value, float) and math.isfinite(value)


def register_version(
    client: MlflowClient,
    model_uri: str,
    name: str,
    *,
    description: str | None = None,
    tags: dict[str, str] | None = None,
) -> str:
    """Register ``model_uri`` (e.g. ``runs:/<id>/model``) under ``name``; return version."""
    try:
        client.get_registered_model(name)
    except Exception:
        client.create_registered_model(name, description=description)

    run_id = None
    source = model_uri
    if model_uri.startswith("runs:/"):
        # runs:/<run_id>/<artifact_path>
        _, _, remainder = model_uri.partition("runs:/")
        run_id = remainder.split("/", 1)[0]

    version = client.create_model_version(
        name=name,
        source=source,
        run_id=run_id,
        tags=tags,
    )
    # MLflow returns the version as an int; normalize to str for stable comparisons.
    return str(version.version)


def _alias_metric(
    client: MlflowClient, name: str, alias: str, metric: str
) -> tuple[str | None, float | None]:
    """Return ``(version, metric_value)`` for the model behind ``alias``, if any."""
    try:
        mv = client.get_model_version_by_alias(name, alias)
    except Exception:
        return None, None
    if mv.run_id is None:
        return mv.version, None
    try:
        run = client.get_run(mv.run_id)
    except Exception:
        return mv.version, None
    value = run.data.metrics.get(metric)
    return mv.version, (float(value) if value is not None else None)


def is_better(candidate: float, incumbent: float | None, higher_is_better: bool) -> bool:
    """True when ``candidate`` should replace ``incumbent`` for the given direction."""
    if not _is_finite(incumbent):
        return True
    if higher_is_better:
        return candidate > incumbent  # type: ignore[operator]
    return candidate < incumbent  # type: ignore[operator]


def clears_gate(value: float, gate: float, higher_is_better: bool) -> bool:
    """True when ``value`` satisfies the absolute quality gate."""
    if not math.isfinite(gate):
        # No absolute gate configured (e.g. ``inf`` max-RMSE) → only relative comparison.
        return True
    return value >= gate if higher_is_better else value <= gate


def evaluate_and_promote(
    client: MlflowClient,
    name: str,
    version: str,
    *,
    candidate_metric: float,
    metric_name: str,
    higher_is_better: bool,
    gate: float,
    staging_alias: str = "staging",
    production_alias: str = "production",
) -> PromotionDecision:
    """Tag ``version`` as staging and promote to production when it wins on the metric."""
    staged = False
    if staging_alias:
        client.set_registered_model_alias(name, staging_alias, version)
        staged = True

    if not production_alias:
        return PromotionDecision(
            name, version, candidate_metric, None, False, staged,
            reason="promotion disabled — registered and staged only",
        )

    if not _is_finite(candidate_metric):
        return PromotionDecision(
            name, version, candidate_metric, None, False, staged,
            reason=f"candidate {metric_name} is not finite — not promoted",
        )

    if not clears_gate(candidate_metric, gate, higher_is_better):
        return PromotionDecision(
            name, version, candidate_metric, None, False, staged,
            reason=f"{metric_name}={candidate_metric:.4f} fails gate {gate}",
        )

    _, prod_metric = _alias_metric(client, name, production_alias, metric_name)
    if is_better(candidate_metric, prod_metric, higher_is_better):
        client.set_registered_model_alias(name, production_alias, version)
        reason = (
            f"promoted: {metric_name}={candidate_metric:.4f} beats "
            f"production={'none' if prod_metric is None else f'{prod_metric:.4f}'}"
        )
        return PromotionDecision(
            name, version, candidate_metric, prod_metric, True, staged, reason
        )

    return PromotionDecision(
        name, version, candidate_metric, prod_metric, False, staged,
        reason=(
            f"kept incumbent: {metric_name}={candidate_metric:.4f} "
            f"does not beat production={prod_metric:.4f}"
        ),
    )
