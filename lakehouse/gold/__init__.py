from __future__ import annotations

"""Gold sub-package.

WINDOW_SPECS is defined here so tests can import it without pulling in PySpark.
"""

# (spark_window_duration, short_label) — used by build_gold.py and tests.
WINDOW_SPECS: tuple[tuple[str, str], ...] = (
    ("5 minutes", "5m"),
    ("1 hour", "1h"),
    ("24 hours", "24h"),
)
