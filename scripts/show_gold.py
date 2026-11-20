"""Print a quick look at every layer of the local Lakehouse.

python scripts/show_gold.py --lakehouse-root ./data/lakehouse
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipelines"))

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from _common import SRC  # noqa: E402,F401 - puts src/ on sys.path
from telemetry.config import parse_args  # noqa: E402
from telemetry.spark import get_spark  # noqa: E402


def main() -> None:
    cfg = parse_args("Show row counts and samples from each layer.")
    spark = get_spark("telemetry-show")
    p = cfg.paths
    layers = [
        ("bronze", p.bronze),
        ("silver", p.silver),
        ("quarantine", p.quarantine),
        ("gold_minute_metrics", p.gold_minute),
        ("gold_alerts", p.gold_alerts),
        ("gold_daily_summary", p.gold_daily),
    ]
    for name, path in layers:
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"{name:<22} (not created yet)")
            continue
        print(f"{name:<22} {spark.read.format('delta').load(path).count():>10,} rows")

    if DeltaTable.isDeltaTable(spark, p.quarantine):
        print("\nQuarantined records by rule:")
        (
            spark.read.format("delta")
            .load(p.quarantine)
            .select(F.explode("dq_failed_rules").alias("rule"))
            .groupBy("rule")
            .count()
            .orderBy(F.desc("count"))
            .show(truncate=False)
        )
    if DeltaTable.isDeltaTable(spark, p.gold_minute):
        print("Latest per-minute metrics:")
        (
            spark.read.format("delta")
            .load(p.gold_minute)
            .orderBy(F.desc("window_start"))
            .limit(10)
            .select(
                "window_start",
                "vehicle_id",
                "powertrain",
                "events",
                "avg_speed_kph",
                "max_engine_temp_c",
                "min_energy_pct",
                "km_driven",
            )
            .show(truncate=False)
        )
    if DeltaTable.isDeltaTable(spark, p.gold_alerts):
        print("Alerts by type:")
        spark.read.format("delta").load(p.gold_alerts).groupBy("alert_type").count().show()
    if DeltaTable.isDeltaTable(spark, p.silver):
        print("End-to-end ingest delay (seconds, event time to Silver):")
        (
            spark.read.format("delta")
            .load(p.silver)
            .select(F.percentile_approx("ingest_delay_s", [0.5, 0.95, 0.99]).alias("p50_p95_p99"))
            .show(truncate=False)
        )


if __name__ == "__main__":
    main()
