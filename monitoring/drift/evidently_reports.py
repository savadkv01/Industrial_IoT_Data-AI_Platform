"""Evidently drift reports + CLI (Phase 10).

Compares the current feature/prediction dataset against a versioned reference and writes:

* ``drift_summary.json`` — the pure-numpy :class:`~monitoring.drift.metrics.DriftReport`
  (always written; no Evidently required).
* ``drift_report.html`` — a rich Evidently report (best-effort; only when Evidently is
  installed). The loader is version-tolerant across Evidently's legacy and modern APIs.

CLI::

    python -m monitoring.drift.evidently_reports snapshot   # capture the reference
    python -m monitoring.drift.evidently_reports report     # compute + write reports
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from monitoring.config import MonitoringConfig, get_config
from monitoring.drift.metrics import DriftReport, compute_drift

logger = logging.getLogger(__name__)

SUMMARY_FILENAME = "drift_summary.json"
HTML_FILENAME = "drift_report.html"


def load_frame(path: str | Path) -> pd.DataFrame:
    """Read a parquet feature dataset; raise a clear error when it is missing."""
    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(
            f"dataset not found: {source}. Build the Phase 5 offline store, or capture a "
            "reference with `python -m monitoring.drift.evidently_reports snapshot`."
        )
    return pd.read_parquet(source)


def snapshot_reference(cfg: MonitoringConfig | None = None) -> Path:
    """Freeze the current feature dataset as the versioned drift reference."""
    cfg = cfg or get_config()
    current = load_frame(cfg.current_features_path)
    cfg.reference_features_path.parent.mkdir(parents=True, exist_ok=True)
    current.to_parquet(cfg.reference_features_path, index=False)
    logger.info(
        "captured drift reference (%d rows) -> %s",
        len(current),
        cfg.reference_features_path,
    )
    return cfg.reference_features_path


def _drop_identity_columns(frame: pd.DataFrame, cfg: MonitoringConfig) -> pd.DataFrame:
    """Drop identifier/timestamp columns that must not be treated as drift features."""
    drop = [c for c in (cfg.entity_col, cfg.time_col, "created_timestamp") if c in frame.columns]
    return frame.drop(columns=drop) if drop else frame


def build_evidently_html(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    output_path: Path,
) -> bool:
    """Render an Evidently data-drift HTML report; return ``True`` on success.

    Best-effort and version-tolerant: tries the legacy ``evidently.report`` API, then the
    modern top-level API. Any failure (Evidently absent or API mismatch) is logged and the
    function returns ``False`` so callers still get the JSON summary.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ── Legacy API (evidently 0.4.x / 0.5.x) ──
    try:
        from evidently.metric_preset import DataDriftPreset  # type: ignore
        from evidently.report import Report  # type: ignore

        report = Report(metrics=[DataDriftPreset()])
        report.run(reference_data=reference, current_data=current)
        report.save_html(str(output_path))
        return True
    except Exception as exc:  # pragma: no cover - depends on installed version
        logger.debug("legacy Evidently API unavailable: %s", exc)

    # ── Modern API (evidently >= 0.6) ──
    try:
        from evidently import Report  # type: ignore
        from evidently.presets import DataDriftPreset  # type: ignore

        report = Report([DataDriftPreset()])
        result = report.run(current_data=current, reference_data=reference)
        # ``result`` exposes save_html in modern versions; fall back to the report object.
        saver = getattr(result, "save_html", None) or getattr(report, "save_html", None)
        if saver is None:
            raise RuntimeError("no save_html on Evidently result")
        saver(str(output_path))
        return True
    except Exception as exc:  # pragma: no cover - depends on installed version
        logger.warning("Evidently HTML report skipped (%s)", exc)
        return False


def generate_report(cfg: MonitoringConfig | None = None) -> DriftReport:
    """Compute drift, persist the JSON summary + optional HTML, and return the report."""
    cfg = cfg or get_config()
    reference = _drop_identity_columns(load_frame(cfg.reference_features_path), cfg)
    current = _drop_identity_columns(load_frame(cfg.current_features_path), cfg)

    report = compute_drift(
        reference,
        current,
        psi_threshold=cfg.psi_threshold,
        dataset_drift_share=cfg.dataset_drift_share,
        bins=cfg.bins,
    )

    cfg.reports_dir.mkdir(parents=True, exist_ok=True)
    summary_path = cfg.reports_dir / SUMMARY_FILENAME
    summary_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    logger.info(
        "drift summary -> %s (%d/%d features drifted, dataset_drift=%s)",
        summary_path,
        report.n_drifted,
        report.n_features,
        report.dataset_drift,
    )

    build_evidently_html(reference, current, cfg.reports_dir / HTML_FILENAME)
    return report


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for capturing references and generating drift reports."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Phase 10 drift reporting.")
    parser.add_argument(
        "command",
        choices=("report", "snapshot"),
        help="'snapshot' captures the reference dataset; 'report' computes drift.",
    )
    args = parser.parse_args(argv)
    cfg = get_config()

    if args.command == "snapshot":
        snapshot_reference(cfg)
        return 0

    report = generate_report(cfg)
    # Non-zero exit when the dataset has drifted, so schedulers/CI can alert on it.
    return 1 if report.dataset_drift else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
