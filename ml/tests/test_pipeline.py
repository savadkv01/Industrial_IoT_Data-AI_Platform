"""End-to-end MLOps pipeline + batch inference tests against an isolated MLflow store.

Each test uses a temporary SQLite tracking/registry DB and a temp artifact root so it
never touches the repo's ``ml/mlflow.db``. Promotion gates are relaxed so the synthetic
models (AUC ~0.7) clear them; the real ``ML_PDM_MIN_AUC=0.85`` gate is unit-tested
separately in ``test_registry.py``.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from mlflow.tracking import MlflowClient

from ml.common.synthetic import make_synthetic_feature_frame
from ml.common.tasks import TASKS
from ml.config import MLConfig
from ml.inference.batch import batch_score
from ml.pipeline import run_pipeline


@pytest.fixture
def cfg(tmp_path) -> MLConfig:
    db = tmp_path / "mlflow.db"
    return replace(
        MLConfig(),
        mlflow_tracking_uri=f"sqlite:///{db.as_posix()}",
        mlflow_artifact_root=str(tmp_path / "artifacts"),
        predictions_dir=tmp_path / "predictions",
        pdm_min_auc=0.5,
        # The synthetic Isolation Forest is uncorrelated with the failure proxy
        # (score_auc ~0.48); use a permissive gate so the promotion *mechanics* run.
        anomaly_min_score_auc=0.0,
    )


@pytest.fixture
def frame():
    return make_synthetic_feature_frame(n_machines=40, n_steps=60, seed=11)


@pytest.mark.parametrize("task", sorted(TASKS))
def test_pipeline_registers_and_promotes(cfg: MLConfig, frame, task: str) -> None:
    result = run_pipeline(task, cfg=cfg, frame=frame, register=True, promote=True)

    assert result.registered_version == "1"
    assert result.decision is not None
    assert result.decision.staged is True
    assert result.decision.promoted is True  # no incumbent → first version wins

    client = MlflowClient(tracking_uri=cfg.mlflow_tracking_uri)
    spec = TASKS[task]
    prod = client.get_model_version_by_alias(spec.registered_model(cfg), cfg.production_alias)
    assert str(prod.version) == "1"


def test_pipeline_keeps_incumbent_when_not_better(cfg: MLConfig, frame) -> None:
    task = "predictive_maintenance"
    first = run_pipeline(task, cfg=cfg, frame=frame)
    assert first.decision.promoted is True

    # Identical data + seed → identical metric, which does not strictly beat the incumbent.
    second = run_pipeline(task, cfg=cfg, frame=frame)
    assert second.registered_version == "2"
    assert second.decision.promoted is False
    assert "incumbent" in second.decision.reason

    client = MlflowClient(tracking_uri=cfg.mlflow_tracking_uri)
    spec = TASKS[task]
    prod = client.get_model_version_by_alias(spec.registered_model(cfg), cfg.production_alias)
    assert str(prod.version) == "1"  # production still points at the first model


def test_no_promote_registers_and_stages_only(cfg: MLConfig, frame) -> None:
    task = "anomaly_detection"
    result = run_pipeline(task, cfg=cfg, frame=frame, register=True, promote=False)
    assert result.decision.promoted is False
    assert result.decision.staged is True

    client = MlflowClient(tracking_uri=cfg.mlflow_tracking_uri)
    spec = TASKS[task]
    name = spec.registered_model(cfg)
    assert str(client.get_model_version_by_alias(name, cfg.staging_alias).version) == "1"
    with pytest.raises(Exception):
        client.get_model_version_by_alias(name, cfg.production_alias)


@pytest.mark.parametrize("task", sorted(TASKS))
def test_batch_score_loads_production_model(cfg: MLConfig, frame, task: str) -> None:
    run_pipeline(task, cfg=cfg, frame=frame, register=True, promote=True)
    predictions = batch_score(task, frame, cfg=cfg)

    spec = TASKS[task]
    assert spec.output_column in predictions.columns
    assert len(predictions) == len(frame)
    assert predictions[spec.output_column].notna().all()
    assert (predictions["alias"] == cfg.production_alias).all()
