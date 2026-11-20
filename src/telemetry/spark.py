"""Spark session factory.

On Databricks the runtime already provides a session with Delta Lake and the
Kafka connector. Locally we build one with the open-source Delta package and,
for jobs that read Kafka, the Spark Kafka connector.
"""

from __future__ import annotations

import os

from .config import on_databricks


def get_spark(app_name: str, with_kafka: bool = False):
    from pyspark.sql import SparkSession

    if on_databricks():
        return SparkSession.builder.getOrCreate()

    import pyspark
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    packages = [f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"] if with_kafka else []
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
