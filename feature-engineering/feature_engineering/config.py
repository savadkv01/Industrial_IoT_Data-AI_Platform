"""Runtime configuration for Phase 5 feature engineering and Feast."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:  # best-effort .env loading
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover
    pass


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class FeatureEngineeringConfig:
    """Configuration snapshot for feature build + Feast local repo."""

    repo_root: Path = Path(os.getenv("FEATURE_REPO_ROOT", _default_repo_root()))
    offline_dir_name: str = os.getenv("FEATURE_OFFLINE_DIR", "data/offline")
    offline_file_name: str = os.getenv("FEATURE_OFFLINE_FILE", "telemetry_features.parquet")
    registry_path_name: str = os.getenv("FEATURE_FEAST_REGISTRY", "feast/data/registry.db")
    online_store_path_name: str = os.getenv("FEATURE_FEAST_ONLINE_STORE", "feast/data/online.db")
    offline_store_s3_uri: str = os.getenv(
        "FEATURE_OFFLINE_S3_URI",
        "",
    )
    gold_source_uri: str = os.getenv("FEATURE_GOLD_SOURCE_URI", "")

    @property
    def offline_dir(self) -> Path:
        return self.repo_root / self.offline_dir_name

    @property
    def offline_file(self) -> Path:
        return self.offline_dir / self.offline_file_name

    @property
    def feast_dir(self) -> Path:
        return self.repo_root / "feast"

    @property
    def registry_path(self) -> Path:
        return self.repo_root / self.registry_path_name

    @property
    def online_store_path(self) -> Path:
        return self.repo_root / self.online_store_path_name

    @property
    def gold_default_source(self) -> str:
        if self.gold_source_uri:
            return self.gold_source_uri
        return ""
