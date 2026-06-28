"""Real-time anomaly alerting (Phase 8).

Consumes telemetry from Kafka, scores each micro-batch with the production anomaly
model from MLflow, throttles sustained alerts per machine, then writes the surviving
alerts to both Kafka and Delta.

Run via ``streaming/spark/submit_alerts.sh`` inside the Spark container.
"""

from __future__ import annotations

import re
import sys
from typing import Any
from typing import TYPE_CHECKING

import pandas as pd
from ml.common.tasks import ANOMALY_DETECTION, get_task
from ml.common.tracking import configure_tracking
from ml.config import MLConfig
from ml.inference.batch import load_model
from mlflow.tracking import MlflowClient

from streaming.config import StreamingConfig
from streaming.fields import TELEMETRY_FIELDS

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Continuous sensor signals the anomaly model can score directly. Derived from the
# telemetry schema so it stays in parity; ingest metadata (``_partition``/``_offset``)
# and categorical codes are never fed to the model.
_NUMERIC_FIELD_TYPES = {"double", "integer"}
_NON_FEATURE_TELEMETRY = {"error_code"}
TELEMETRY_FEATURE_COLUMNS: tuple[str, ...] = tuple(
    name
    for name, type_name in TELEMETRY_FIELDS
    if type_name in _NUMERIC_FIELD_TYPES and name not in _NON_FEATURE_TELEMETRY
)

ALERT_COLUMNS = [
    "machine_id",
    "event_timestamp",
    "anomaly_score",
    "alert_threshold",
    "model_name",
    "model_alias",
    "model_version",
    "event",
    "error_code",
    "_topic",
    "_partition",
    "_offset",
]

_DURATION_PATTERN = re.compile(
    r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>second|seconds|minute|minutes)\s*$",
    re.IGNORECASE,
)


def parse_duration_seconds(duration: str) -> float:
    """Parse the repo's human-readable duration strings into seconds."""
    match = _DURATION_PATTERN.match(duration)
    if not match:
        raise ValueError(
            f"unsupported duration '{duration}'; expected '<n> seconds' or '<n> minutes'"
        )
    value = float(match.group("value"))
    unit = match.group("unit").lower()
    multiplier = 60.0 if unit.startswith("minute") else 1.0
    return value * multiplier


class AnomalyModelCache:
    """Load the aliased anomaly model once and reload only on a version change.

    The alias->version lookup is a cheap metadata call, so each micro-batch checks
    whether ``model_alias`` now points at a new version (a model hot-swap) and reloads
    the heavy artifact only then — keeping per-batch latency low (TC-8.5).
    """

    def __init__(self, cfg: StreamingConfig, ml_cfg: MLConfig | None = None) -> None:
        self._cfg = cfg
        self._ml_cfg = configure_tracking(ml_cfg)
        self._spec = get_task(ANOMALY_DETECTION)
        self._name = self._spec.registered_model(self._ml_cfg)
        self._client = MlflowClient(
            tracking_uri=self._ml_cfg.mlflow_tracking_uri,
            registry_uri=self._ml_cfg.mlflow_tracking_uri,
        )
        self._version: str | None = None
        self._model: Any | None = None

    def current(self) -> tuple[Any, str, str]:
        """Return ``(model, registered_name, version)``, reloading on version change."""
        version = str(
            self._client.get_model_version_by_alias(self._name, self._cfg.model_alias).version
        )
        if version != self._version or self._model is None:
            self._model = load_model(
                ANOMALY_DETECTION, alias=self._cfg.model_alias, cfg=self._ml_cfg
            )
            self._version = version
        return self._model, self._name, self._version


def select_scoring_features(model: Any, frame: pd.DataFrame) -> pd.DataFrame:
    """Align a telemetry frame to the model's expected feature columns.

    Prefers the model's own ``feature_names_in_`` (set when it was fit on a DataFrame)
    so streaming inputs match the trained feature space exactly; columns absent from the
    raw stream are filled with NaN for the pipeline's imputer. Falls back to the
    telemetry sensor allowlist, which deliberately excludes ingest metadata and
    id/timestamp columns so they can never leak in as model inputs.
    """
    names = getattr(model, "feature_names_in_", None)
    if names is not None:
        columns = list(names)
    else:
        columns = [c for c in TELEMETRY_FEATURE_COLUMNS if c in frame.columns]
    return frame.reindex(columns=columns)


def _anomaly_scores(model: Any, features: pd.DataFrame) -> pd.Series:
    """Higher = more anomalous (negate the Isolation Forest log-likelihood)."""
    return pd.Series(-model.score_samples(features), index=features.index)


def score_microbatch(
    frame: pd.DataFrame,
    *,
    model: Any,
    model_name: str,
    model_alias: str,
    model_version: str,
    threshold: float,
) -> pd.DataFrame:
    """Score a telemetry micro-batch and return rows above the alert threshold."""
    if frame.empty:
        return pd.DataFrame(columns=ALERT_COLUMNS)

    scored = frame.copy()
    scored["event_timestamp"] = pd.to_datetime(scored["ts"], utc=True)
    features = select_scoring_features(model, scored)
    scored["anomaly_score"] = _anomaly_scores(model, features).to_numpy()
    alerts = scored.loc[scored["anomaly_score"] >= threshold].copy()
    if alerts.empty:
        return pd.DataFrame(columns=ALERT_COLUMNS)

    alerts["alert_threshold"] = threshold
    alerts["model_name"] = model_name
    alerts["model_alias"] = model_alias
    alerts["model_version"] = model_version
    return alerts[ALERT_COLUMNS].reset_index(drop=True)


def throttle_alerts(alerts: pd.DataFrame, prior_alerts: pd.DataFrame, cooldown: str) -> pd.DataFrame:
    """Keep at most one alert per machine inside the cooldown window."""
    if alerts.empty:
        return alerts.copy()

    current = alerts.copy()
    current["event_timestamp"] = pd.to_datetime(current["event_timestamp"], utc=True)
    current = current.sort_values(
        ["machine_id", "anomaly_score", "event_timestamp"],
        ascending=[True, False, True],
    ).drop_duplicates(subset=["machine_id"], keep="first")

    if prior_alerts.empty:
        return current.reset_index(drop=True)

    lookback = prior_alerts.copy()
    lookback["event_timestamp"] = pd.to_datetime(lookback["event_timestamp"], utc=True)
    latest = (
        lookback.sort_values("event_timestamp")
        .groupby("machine_id", as_index=False)
        .tail(1)
        .rename(columns={"event_timestamp": "last_alert_timestamp"})
    )[["machine_id", "last_alert_timestamp"]]

    throttled = current.merge(latest, on="machine_id", how="left")
    cooldown_seconds = parse_duration_seconds(cooldown)
    elapsed_seconds = (
        throttled["event_timestamp"] - throttled["last_alert_timestamp"]
    ).dt.total_seconds()
    keep_mask = throttled["last_alert_timestamp"].isna() | (elapsed_seconds >= cooldown_seconds)
    return throttled.loc[keep_mask, ALERT_COLUMNS].reset_index(drop=True)


def _path_exists(spark: SparkSession, path: str) -> bool:
    uri = spark._jvm.java.net.URI(path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))


def load_recent_alerts(spark: SparkSession, cfg: StreamingConfig, max_event_ts: pd.Timestamp) -> pd.DataFrame:
    """Load recent alerts from Delta to enforce per-machine cooldown across batches."""
    from pyspark.sql import functions as F

    if not _path_exists(spark, cfg.alerts_path):
        return pd.DataFrame(columns=["machine_id", "event_timestamp"])

    cooldown = pd.Timedelta(seconds=parse_duration_seconds(cfg.alerts_cooldown))
    lower_bound = (max_event_ts - cooldown).to_pydatetime()
    return (
        spark.read.format("delta")
        .load(cfg.alerts_path)
        .filter(F.col("event_timestamp") >= F.lit(lower_bound))
        .select("machine_id", "event_timestamp")
        .toPandas()
    )


def write_alerts(df: DataFrame, cfg: StreamingConfig):
    """Score telemetry and emit deduplicated alerts to Delta + Kafka."""
    from pyspark.sql import functions as F

    model_cache = AnomalyModelCache(cfg)

    def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        valid = batch_df.filter(F.col("machine_id").isNotNull())
        if valid.isEmpty():
            return

        microbatch = valid.toPandas()
        model, model_name, model_version = model_cache.current()
        alerts_pdf = score_microbatch(
            microbatch,
            model=model,
            model_name=model_name,
            model_alias=cfg.model_alias,
            model_version=model_version,
            threshold=cfg.anomaly_threshold,
        )
        if alerts_pdf.empty:
            return

        prior_alerts = load_recent_alerts(spark, cfg, alerts_pdf["event_timestamp"].max())
        alerts_pdf = throttle_alerts(alerts_pdf, prior_alerts, cfg.alerts_cooldown)
        if alerts_pdf.empty:
            return

        alerts_spark = spark.createDataFrame(alerts_pdf).withColumn(
            "alert_emitted_ts", F.current_timestamp()
        )
        (
            alerts_spark.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(cfg.alerts_path)
        )

        kafka_rows = alerts_spark.select(
            F.col("machine_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "machine_id",
                    "event_timestamp",
                    "anomaly_score",
                    "alert_threshold",
                    "model_name",
                    "model_alias",
                    "model_version",
                    "event",
                    "error_code",
                    "_topic",
                    "_partition",
                    "_offset",
                    "alert_emitted_ts",
                )
            ).alias("value"),
        )
        (
            kafka_rows.write.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
            .option("topic", cfg.alerts_topic)
            .save()
        )

        print(
            f"[alerts] batch={batch_id} emitted={len(alerts_pdf)} topic={cfg.alerts_topic}",
            file=sys.stderr,
        )

    return (
        df.writeStream.queryName("realtime_anomaly_alerts")
        .option("checkpointLocation", cfg.alerts_checkpoint_path)
        .trigger(processingTime=cfg.alerts_trigger_interval)
        .foreachBatch(_process_batch)
        .start()
    )


def main() -> int:
    from streaming.spark.ingest_bronze import build_spark, parse_and_enrich, read_kafka

    cfg = StreamingConfig()
    spark = build_spark(cfg)
    print(
        f"[alerts] topic={cfg.telemetry_topic} -> {cfg.alerts_topic} "
        f"(threshold={cfg.anomaly_threshold}, checkpoint={cfg.alerts_checkpoint_path})",
        file=sys.stderr,
    )
    raw = read_kafka(spark, cfg)
    enriched = parse_and_enrich(raw).withWatermark("ts", cfg.watermark_delay)
    query = write_alerts(enriched, cfg)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())