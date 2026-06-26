"""Apply Feast definitions and materialize the local online store."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from feature_engineering.config import FeatureEngineeringConfig
from feature_engineering.feast_repo import get_feature_store


def main() -> int:
    cfg = FeatureEngineeringConfig()
    sys.path.insert(0, str(cfg.feast_dir))
    from feature_repo import FEAST_OBJECTS

    store = get_feature_store()
    store.apply(FEAST_OBJECTS)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    store.materialize(start_date=start, end_date=end)

    print(
        f"[phase5] Feast apply + materialize complete for {start.isoformat()} -> {end.isoformat()}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())