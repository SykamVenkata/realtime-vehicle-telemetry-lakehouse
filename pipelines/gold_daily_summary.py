"""Gold (batch): daily fleet summary, merged idempotently.

Recomputes the last few days from Silver and MERGEs them into the summary
table, so late-arriving data is picked up and reruns never double count.

    python pipelines/gold_daily_summary.py --lakehouse-root ./data/lakehouse
"""

import os

from pyspark.sql import functions as F

from _common import merge_into  # must come first: puts src/ on sys.path
from telemetry.config import parse_args
from telemetry.spark import get_spark
from telemetry.transforms import daily_fleet_summary


def main(argv=None) -> None:
    cfg = parse_args("Build the daily fleet summary.", argv)
    lookback_days = int(os.getenv("DAILY_LOOKBACK_DAYS", "2"))
    spark = get_spark("telemetry-daily-summary")

    silver = (
        spark.read.format("delta")
        .load(cfg.paths.silver)
        .filter(F.col("event_date") >= F.date_sub(F.current_date(), lookback_days))
    )
    summary = daily_fleet_summary(silver)
    merge_into(summary, cfg.paths.gold_daily, ["event_date", "powertrain"], partition_by="event_date")
    print(f"[daily] merged {summary.count()} rows into {cfg.paths.gold_daily}", flush=True)


if __name__ == "__main__":
    main()
