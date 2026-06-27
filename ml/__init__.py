"""ML model building & MLOps package (Phases 6-7).

Subpackages:
  * ``ml.predictive_maintenance`` — failure-within-horizon classifier (XGBoost).
  * ``ml.anomaly_detection``      — unsupervised anomaly scoring (Isolation Forest).
  * ``ml.battery_health``         — battery state-of-health regressor.
  * ``ml.common``                 — shared data loaders, time splits, metrics, MLflow tracking.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
