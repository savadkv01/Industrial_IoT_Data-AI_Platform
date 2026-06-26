"""Helpers for loading the local Feast repo used in Phase 5."""

from __future__ import annotations

from feast import FeatureStore

from feature_engineering.config import FeatureEngineeringConfig


def get_feature_store(cfg: FeatureEngineeringConfig | None = None) -> FeatureStore:
    cfg = cfg or FeatureEngineeringConfig()
    return FeatureStore(repo_path=str(cfg.feast_dir))