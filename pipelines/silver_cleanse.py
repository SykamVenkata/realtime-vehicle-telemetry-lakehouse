"""Silver: parse, validate, deduplicate and enrich telemetry.

Valid events go to the Silver table; events that break a data quality rule go
to a quarantine table with the rule names and the raw payload, so nothing is
silently lost and bad records can be fixed and replayed.

    python pipelines/silver_cleanse.py --lakehouse-root ./data/lakehouse
"""

from _common import run_queries, wait_for_delta  # must come first: puts src/ on sys.path
from telemetry.config import apply_trigger, parse_args
from telemetry.spark import get_spark
from telemetry.transforms import (
    add_quality_flags,
    deduplicate,
    parse_events,
    quarantined_events,
    valid_events,
)


def main(argv=None) -> None:
    cfg = parse_args("Cleanse Bronze telemetry into Silver.", argv)
    spark = get_spark("telemetry-silver")
    wait_for_delta(spark, cfg.paths.bronze)

    bronze = spark.readStream.format("delta").load(cfg.paths.bronze)
    flagged = add_quality_flags(parse_events(bronze))

    silver_writer = (
        deduplicate(valid_events(flagged), streaming=True)
        .writeStream.queryName("silver_events")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.paths.checkpoint("silver_events"))
        .partitionBy("event_date")
    )
    quarantine_writer = (
        quarantined_events(flagged)
        .writeStream.queryName("silver_quarantine")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.paths.checkpoint("silver_quarantine"))
        .partitionBy("quarantine_date")
    )

    queries = [
        apply_trigger(silver_writer, cfg.trigger).start(cfg.paths.silver),
        apply_trigger(quarantine_writer, cfg.trigger).start(cfg.paths.quarantine),
    ]
    run_queries(spark, queries, cfg.await_termination)


if __name__ == "__main__":
    main()
