"""Gold (streaming): per-minute vehicle metrics and operational alerts.

- ``vehicle_minute_metrics`` is aggregated in update mode and upserted with
  MERGE, so a dashboard sees each minute fill in within one trigger, and a late
  reading (inside the watermark) corrects its minute instead of being lost.
- ``vehicle_alerts`` is an append-only stream of threshold breaches.

    python pipelines/gold_stream.py --lakehouse-root ./data/lakehouse
"""

from _common import merge_into, run_queries, wait_for_delta  # must come first: puts src/ on sys.path
from telemetry.config import apply_trigger, parse_args
from telemetry.spark import get_spark
from telemetry.transforms import detect_alerts, minute_metrics

METRICS_WATERMARK = "10 minutes"
METRIC_KEYS = ["window_date", "window_start", "vehicle_id"]


def main(argv=None) -> None:
    cfg = parse_args("Build streaming Gold tables from Silver.", argv)
    spark = get_spark("telemetry-gold")
    wait_for_delta(spark, cfg.paths.silver)

    silver = spark.readStream.format("delta").load(cfg.paths.silver)

    def upsert_metrics(batch_df, _batch_id: int) -> None:
        if not batch_df.isEmpty():
            merge_into(batch_df, cfg.paths.gold_minute, METRIC_KEYS, partition_by="window_date")

    metrics_writer = (
        minute_metrics(silver.withWatermark("event_ts", METRICS_WATERMARK))
        .writeStream.queryName("gold_minute_metrics")
        .outputMode("update")
        .option("checkpointLocation", cfg.paths.checkpoint("gold_minute_metrics"))
        .foreachBatch(upsert_metrics)
    )
    alerts_writer = (
        detect_alerts(silver)
        .writeStream.queryName("gold_alerts")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.paths.checkpoint("gold_alerts"))
        .partitionBy("event_date")
    )

    queries = [
        apply_trigger(metrics_writer, cfg.trigger).start(),
        apply_trigger(alerts_writer, cfg.trigger).start(cfg.paths.gold_alerts),
    ]
    run_queries(spark, queries, cfg.await_termination)


if __name__ == "__main__":
    main()
