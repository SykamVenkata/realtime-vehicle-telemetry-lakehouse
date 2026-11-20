"""Table maintenance: compact small files, Z-Order, and vacuum old versions.

Streaming writes many small files; compaction and Z-Ordering by vehicle keep
point lookups and dashboard queries fast. On Databricks you can replace this
job with predictive optimization and liquid clustering.

    python pipelines/maintenance.py --lakehouse-root ./data/lakehouse
"""

import os

from delta.tables import DeltaTable

from _common import SRC  # noqa: F401 - puts src/ on sys.path
from telemetry.config import parse_args
from telemetry.spark import get_spark


def main(argv=None) -> None:
    cfg = parse_args("Optimize and vacuum the Lakehouse tables.", argv)
    retention_hours = int(os.getenv("VACUUM_RETENTION_HOURS", "168"))
    spark = get_spark("telemetry-maintenance")
    p = cfg.paths

    # (path, Z-Order column or None for plain compaction)
    tables = [
        (p.bronze, None),
        (p.silver, "vehicle_id"),
        (p.quarantine, None),
        (p.gold_minute, "vehicle_id"),
        (p.gold_alerts, "vehicle_id"),
        (p.gold_daily, None),
    ]
    for path, zorder in tables:
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"[maintenance] skip {path}: not created yet", flush=True)
            continue
        table = DeltaTable.forPath(spark, path)
        optimizer = table.optimize()
        if zorder:
            optimizer.executeZOrderBy(zorder)
        else:
            optimizer.executeCompaction()
        table.vacuum(retention_hours)
        print(f"[maintenance] optimized {path}" + (f" (Z-Order by {zorder})" if zorder else ""), flush=True)


if __name__ == "__main__":
    main()
