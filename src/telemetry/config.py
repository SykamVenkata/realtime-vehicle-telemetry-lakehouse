"""Runtime configuration shared by the streaming jobs.

Everything that differs between a laptop and Azure lives here: where the
Lakehouse is, where events come from, and how often micro-batches run.
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class Paths:
    root: str

    @property
    def bronze(self) -> str:
        return f"{self.root}/bronze/vehicle_telemetry_raw"

    @property
    def silver(self) -> str:
        return f"{self.root}/silver/vehicle_telemetry"

    @property
    def quarantine(self) -> str:
        return f"{self.root}/silver/vehicle_telemetry_quarantine"

    @property
    def gold_minute(self) -> str:
        return f"{self.root}/gold/vehicle_minute_metrics"

    @property
    def gold_alerts(self) -> str:
        return f"{self.root}/gold/vehicle_alerts"

    @property
    def gold_daily(self) -> str:
        return f"{self.root}/gold/fleet_daily_summary"

    def checkpoint(self, name: str) -> str:
        return f"{self.root}/_checkpoints/{name}"


@dataclass(frozen=True)
class JobConfig:
    paths: Paths
    trigger: str  # "10 seconds", "1 minute", ... or "available-now"
    topic: str
    max_offsets_per_trigger: int
    await_termination: bool


def parse_args(description: str, argv: list[str] | None = None) -> JobConfig:
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "--lakehouse-root",
        default=os.getenv("LAKEHOUSE_ROOT", "./data/lakehouse"),
        help="local path, abfss:// URI or Unity Catalog volume path",
    )
    p.add_argument(
        "--trigger",
        default=os.getenv("STREAM_TRIGGER", "10 seconds"),
        help='processing-time interval, or "available-now" to drain and stop',
    )
    p.add_argument("--topic", default=os.getenv("TELEMETRY_TOPIC", "vehicle-telemetry"))
    p.add_argument(
        "--max-offsets-per-trigger", type=int, default=int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000"))
    )
    p.add_argument("--no-await", action="store_true", help="start the query and return")
    a = p.parse_args(argv)
    return JobConfig(
        paths=Paths(a.lakehouse_root.rstrip("/")),
        trigger=a.trigger,
        topic=a.topic,
        max_offsets_per_trigger=a.max_offsets_per_trigger,
        await_termination=not a.no_await,
    )


def apply_trigger(writer, trigger: str):
    """Set a Structured Streaming trigger from a config string."""
    if trigger == "available-now":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=trigger)


def eventhubs_connection_string(spark=None) -> str | None:
    """Event Hubs connection string, from a Databricks secret or the environment."""
    scope, key = os.getenv("EVENTHUBS_SECRET_SCOPE"), os.getenv("EVENTHUBS_SECRET_KEY")
    if scope and key and spark is not None and on_databricks():
        from pyspark.dbutils import DBUtils  # available on Databricks runtimes only

        return DBUtils(spark).secrets.get(scope=scope, key=key)
    return os.getenv("EVENTHUBS_CONNECTION_STRING")


def kafka_source_options(cfg: JobConfig, spark=None) -> dict[str, str]:
    """Options for ``spark.readStream.format("kafka")``: local Kafka or Event Hubs."""
    opts = {
        "subscribe": cfg.topic,
        "startingOffsets": "earliest",
        "maxOffsetsPerTrigger": str(cfg.max_offsets_per_trigger),
        "failOnDataLoss": "false",
    }
    conn = eventhubs_connection_string(spark)
    if not conn:
        opts["kafka.bootstrap.servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        return opts

    match = re.search(r"Endpoint=sb://([^/;]+)", conn)
    if not match:
        raise ValueError("Event Hubs connection string has no Endpoint=sb://... part")
    # Databricks ships a shaded Kafka client, so the login module class differs.
    login_module = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        if on_databricks()
        else "org.apache.kafka.common.security.plain.PlainLoginModule"
    )
    opts.update(
        {
            "kafka.bootstrap.servers": f"{match.group(1)}:9093",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": f'{login_module} required username="$ConnectionString" password="{conn}";',
            "kafka.request.timeout.ms": "60000",
            "kafka.session.timeout.ms": "30000",
        }
    )
    return opts
