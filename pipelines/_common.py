"""Helpers shared by the pipeline entry points."""

from __future__ import annotations

import sys
import time
from pathlib import Path

# Make the ``telemetry`` package importable when a job runs as a plain script
# (locally, in Docker, or as a Databricks spark_python_task).
SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def wait_for_delta(spark, path: str, timeout_s: int = 900, poll_s: int = 10) -> None:
    """Block until an upstream job has created the Delta table at ``path``.

    The streaming jobs start together; downstream layers wait here instead of
    failing because their source table doesn't exist yet.
    """
    from delta.tables import DeltaTable

    deadline = time.monotonic() + timeout_s
    while not DeltaTable.isDeltaTable(spark, path):
        if time.monotonic() > deadline:
            raise TimeoutError(f"No Delta table appeared at {path} within {timeout_s}s")
        print(f"[wait] waiting for upstream table {path} ...", flush=True)
        time.sleep(poll_s)


def merge_into(batch_df, path: str, keys: list[str], partition_by: str) -> None:
    """Idempotent upsert of ``batch_df`` into the Delta table at ``path``.

    Creates the table on first write. Replaying the same batch leaves the table
    unchanged, which is what makes restarts and backfills safe.
    """
    from delta.tables import DeltaTable

    spark = batch_df.sparkSession
    if not DeltaTable.isDeltaTable(spark, path):
        batch_df.write.format("delta").partitionBy(partition_by).mode("append").save(path)
        return
    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(batch_df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_queries(spark, queries: list, await_termination: bool) -> None:
    names = ", ".join(q.name or q.id for q in queries)
    print(f"[stream] started: {names}", flush=True)
    if await_termination:
        spark.streams.awaitAnyTermination()
