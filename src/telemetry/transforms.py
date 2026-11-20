"""DataFrame transformations for each Medallion layer.

Every function here takes and returns a DataFrame and works the same on a
batch or a streaming DataFrame. That keeps the business logic unit-testable
with small batch inputs, while the jobs in ``pipelines/`` only wire sources,
sinks, watermarks and triggers around it.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .quality import RULES
from .schema import ALERT_THRESHOLDS, FIELD_NAMES, spark_schema

# --- Bronze ------------------------------------------------------------------


def bronze_from_kafka(kafka_df: DataFrame) -> DataFrame:
    """Keep the raw payload exactly as received, plus where and when it came from."""
    return (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingested_at"))
    )


# --- Silver ------------------------------------------------------------------


def parse_events(bronze_df: DataFrame) -> DataFrame:
    """Parse the JSON payload against the event contract.

    Unparseable payloads become rows of nulls (PERMISSIVE mode), so they fail
    the quality rules and land in quarantine with their raw payload kept.
    """
    parsed = F.from_json(F.col("payload"), spark_schema())
    return bronze_df.select(
        parsed.alias("e"), "payload", "kafka_key", "partition", "offset", "kafka_ts", "ingested_at"
    ).select("e.*", "payload", "kafka_key", "partition", "offset", "kafka_ts", "ingested_at")


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Add ``dq_failed_rules``: the names of every rule the row breaks."""
    checks = [F.when(~F.coalesce(F.expr(r.sql), F.lit(False)), F.lit(r.name)) for r in RULES]
    return df.withColumn("dq_failed_rules", F.array_compact(F.array(*checks)))


def valid_events(flagged: DataFrame) -> DataFrame:
    """Rows that passed every rule, enriched for analytics."""
    return (
        flagged.filter(F.size("dq_failed_rules") == 0)
        .select(*FIELD_NAMES, "kafka_ts", "ingested_at")
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("energy_pct", F.coalesce("battery_soc_pct", "fuel_level_pct"))
        .withColumn(
            "ingest_delay_s",
            F.col("ingested_at").cast("long") - F.col("event_ts").cast("long"),
        )
    )


def quarantined_events(flagged: DataFrame) -> DataFrame:
    """Rows that broke at least one rule, with the raw payload for replay."""
    return (
        flagged.filter(F.size("dq_failed_rules") > 0)
        .select(
            "payload",
            "dq_failed_rules",
            "vehicle_id",
            "event_id",
            "kafka_key",
            "partition",
            "offset",
            "kafka_ts",
            "ingested_at",
        )
        .withColumn("quarantine_date", F.to_date("ingested_at"))
    )


def deduplicate(df: DataFrame, streaming: bool, watermark: str = "20 minutes") -> DataFrame:
    """Drop repeated events (device retries) by ``event_id``.

    In a stream, state is kept only for the watermark window, so memory stays
    bounded however long the job runs.
    """
    if streaming:
        return df.withWatermark("event_ts", watermark).dropDuplicatesWithinWatermark(["event_id"])
    return df.dropDuplicates(["event_id"])


# --- Gold --------------------------------------------------------------------


def minute_metrics(silver_df: DataFrame) -> DataFrame:
    """Per-vehicle, per-minute health and driving metrics."""
    return (
        silver_df.groupBy(F.window("event_ts", "1 minute").alias("w"), "vehicle_id", "powertrain")
        .agg(
            F.count("*").alias("events"),
            F.round(F.avg("speed_kph"), 1).alias("avg_speed_kph"),
            F.max("speed_kph").alias("max_speed_kph"),
            F.round(F.avg("engine_temp_c"), 1).alias("avg_engine_temp_c"),
            F.max("engine_temp_c").alias("max_engine_temp_c"),
            F.min("energy_pct").alias("min_energy_pct"),
            F.sum(F.col("harsh_brake").cast("int")).alias("harsh_brakes"),
            F.round(F.max("odometer_km") - F.min("odometer_km"), 3).alias("km_driven"),
            F.max_by("lat", "event_ts").alias("last_lat"),
            F.max_by("lon", "event_ts").alias("last_lon"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.to_date(F.col("w.start")).alias("window_date"),
            "*",
        )
        .drop("w")
        .withColumn("updated_at", F.current_timestamp())
    )


def detect_alerts(silver_df: DataFrame) -> DataFrame:
    """One row per operational alert raised by a reading."""
    t = ALERT_THRESHOLDS
    alert_types = F.array_compact(
        F.array(
            F.when(F.col("engine_temp_c") > t["engine_temp_c"], F.lit("high_engine_temp")),
            F.when(
                (F.col("powertrain") == "EV") & (F.col("battery_soc_pct") < t["battery_soc_pct_low"]),
                F.lit("low_battery"),
            ),
            F.when(
                (F.col("powertrain") == "ICE") & (F.col("fuel_level_pct") < t["fuel_level_pct_low"]),
                F.lit("low_fuel"),
            ),
            F.when(F.coalesce(F.col("harsh_brake"), F.lit(False)), F.lit("harsh_brake")),
        )
    )
    return (
        silver_df.withColumn("alert_type", F.explode(alert_types))
        .select(
            "event_id",
            "vehicle_id",
            "powertrain",
            "event_ts",
            "event_date",
            "alert_type",
            "speed_kph",
            "engine_temp_c",
            "energy_pct",
            "lat",
            "lon",
        )
        .withColumn("raised_at", F.current_timestamp())
    )


def daily_fleet_summary(silver_df: DataFrame) -> DataFrame:
    """Fleet totals per day and powertrain (batch job, merged idempotently)."""
    per_vehicle = silver_df.groupBy("event_date", "powertrain", "vehicle_id").agg(
        F.count("*").alias("events"),
        (F.max("odometer_km") - F.min("odometer_km")).alias("km_driven"),
        F.avg("speed_kph").alias("avg_speed_kph"),
        F.sum(F.col("harsh_brake").cast("int")).alias("harsh_brakes"),
        F.max(F.coalesce(F.size("dtc_codes"), F.lit(0)) > 0).alias("had_dtc"),
        F.max(F.col("engine_temp_c") > ALERT_THRESHOLDS["engine_temp_c"]).alias("overheated"),
    )
    return (
        per_vehicle.groupBy("event_date", "powertrain")
        .agg(
            F.countDistinct("vehicle_id").alias("active_vehicles"),
            F.sum("events").alias("events"),
            F.round(F.sum("km_driven"), 1).alias("km_driven"),
            F.round(F.avg("avg_speed_kph"), 1).alias("avg_speed_kph"),
            F.sum("harsh_brakes").alias("harsh_brakes"),
            F.sum(F.col("had_dtc").cast("int")).alias("vehicles_with_dtc"),
            F.sum(F.col("overheated").cast("int")).alias("vehicles_overheated"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
