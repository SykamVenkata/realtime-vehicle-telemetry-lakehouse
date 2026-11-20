"""Bronze: land raw telemetry from Kafka / Event Hubs into Delta, unchanged.

python pipelines/bronze_ingest.py --lakehouse-root ./data/lakehouse
"""

from _common import run_queries  # must come first: puts src/ on sys.path
from telemetry.config import apply_trigger, kafka_source_options, parse_args
from telemetry.spark import get_spark
from telemetry.transforms import bronze_from_kafka


def main(argv=None) -> None:
    cfg = parse_args("Ingest raw telemetry into the Bronze Delta table.", argv)
    spark = get_spark("telemetry-bronze", with_kafka=True)

    raw = spark.readStream.format("kafka").options(**kafka_source_options(cfg, spark)).load()

    writer = (
        bronze_from_kafka(raw)
        .writeStream.queryName("bronze_ingest")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.paths.checkpoint("bronze_ingest"))
        .partitionBy("ingest_date")
    )
    query = apply_trigger(writer, cfg.trigger).start(cfg.paths.bronze)
    run_queries(spark, [query], cfg.await_termination)


if __name__ == "__main__":
    main()
