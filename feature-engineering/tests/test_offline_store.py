from __future__ import annotations

from pathlib import Path

import pandas as pd

from feature_engineering.build_offline_store import write_offline_store
from feature_engineering.config import FeatureEngineeringConfig
from feature_engineering.transforms import build_feature_dataset
from tests.test_transforms import sample_gold_frame


def test_write_offline_store_creates_parquet(tmp_path: Path):
    cfg = FeatureEngineeringConfig(
        repo_root=tmp_path,
        offline_dir_name="offline",
        offline_file_name="features.parquet",
    )
    output = write_offline_store(sample_gold_frame(), cfg)
    assert output.exists()

    written = pd.read_parquet(output)
    expected = build_feature_dataset(sample_gold_frame())
    assert list(written.columns) == list(expected.columns)
    assert len(written) == len(expected)