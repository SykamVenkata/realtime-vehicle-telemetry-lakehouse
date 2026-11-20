"""Spark tests for the Medallion transformations (run in CI; skipped without PySpark)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from telemetry.generator import Fleet, InjectionRates
from telemetry.quality import failed_rules

pytestmark = pytest.mark.spark


def bronze_df(spark, events, extra_payloads=()):
    from pyspark.sql import functions as F

    rows = [(e.get("vehicle_id"), json.dumps(e), i) for i, e in enumerate(events)]
    rows += [(None, p, len(rows) + j) for j, p in enumerate(extra_payloads)]
    return (
        spark.createDataFrame(rows, "kafka_key string, payload string, offset long")
        .withColumn("partition", F.lit(0))
        .withColumn("kafka_ts", F.current_timestamp())
        .withColumn("ingested_at", F.current_timestamp())
    )


def test_spark_rules_match_python_rules_for_every_event(spark):
    """The Silver stream and the unit tests must agree on what's valid."""
    from telemetry.transforms import add_quality_flags, parse_events

    now = datetime.now(timezone.utc)
    fleet = Fleet(400, seed=11, rates=InjectionRates(duplicate=0, late=0.05, bad=0.2))
    events = list(fleet.tick(now))
    flagged = add_quality_flags(parse_events(bronze_df(spark, events)))
    spark_result = {r["offset"]: sorted(r["dq_failed_rules"]) for r in flagged.collect()}
    for i, e in enumerate(events):
        assert spark_result[i] == sorted(failed_rules(e, now)), e


def test_unparseable_payload_goes_to_quarantine_with_payload_kept(spark):
    from telemetry.transforms import add_quality_flags, parse_events, quarantined_events, valid_events

    events = list(Fleet(5, seed=12, rates=InjectionRates(0, 0, 0)).tick())
    flagged = add_quality_flags(parse_events(bronze_df(spark, events, ["{this is not json"])))
    assert valid_events(flagged).count() == 5
    q = quarantined_events(flagged).collect()
    assert len(q) == 1
    assert q[0]["payload"] == "{this is not json"
    assert "event_id_present" in q[0]["dq_failed_rules"]


def test_silver_enrichment_and_dedup(spark):
    from telemetry.transforms import add_quality_flags, deduplicate, parse_events, valid_events

    events = list(Fleet(10, seed=13, rates=InjectionRates(0, 0, 0)).tick())
    events += [dict(events[0]), dict(events[1])]  # device retries
    silver = deduplicate(
        valid_events(add_quality_flags(parse_events(bronze_df(spark, events)))), streaming=False
    )
    rows = silver.collect()
    assert len(rows) == 10
    for r in rows:
        assert r["event_date"] is not None
        assert r["energy_pct"] == (r["battery_soc_pct"] if r["powertrain"] == "EV" else r["fuel_level_pct"])
        assert r["ingest_delay_s"] is not None


def silver_from(spark, events):
    from telemetry.transforms import add_quality_flags, deduplicate, parse_events, valid_events

    return deduplicate(
        valid_events(add_quality_flags(parse_events(bronze_df(spark, events)))), streaming=False
    )


def test_minute_metrics_aggregate_per_vehicle_per_minute(spark):
    from telemetry.transforms import minute_metrics

    start = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
    fleet = Fleet(4, seed=14, rates=InjectionRates(0, 0, 0))
    events = [e for s in range(120) for e in fleet.tick(start + timedelta(seconds=s))]
    metrics = minute_metrics(silver_from(spark, events)).collect()
    assert len(metrics) == 4 * 2  # 4 vehicles x 2 minutes
    assert all(m["events"] == 60 for m in metrics)
    assert all(m["km_driven"] >= 0 for m in metrics)
    assert all(m["window_end"] - m["window_start"] == timedelta(minutes=1) for m in metrics)


def test_alerts_fire_on_thresholds(spark):
    from telemetry.transforms import detect_alerts

    now = datetime.now(timezone.utc)
    base = list(Fleet(3, seed=15, rates=InjectionRates(0, 0, 0)).tick(now))
    hot = dict(base[0], engine_temp_c=130.0, harsh_brake=True)
    alerts = detect_alerts(silver_from(spark, [hot, base[1], base[2]])).collect()
    types = sorted(a["alert_type"] for a in alerts if a["event_id"] == hot["event_id"])
    assert types == ["harsh_brake", "high_engine_temp"]


def test_daily_summary_merge_is_idempotent(spark, tmp_path):
    from _common import merge_into
    from telemetry.transforms import daily_fleet_summary

    now = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
    fleet = Fleet(20, seed=16, rates=InjectionRates(0, 0, 0))
    events = [e for s in range(30) for e in fleet.tick(now - timedelta(seconds=30 - s))]
    summary = daily_fleet_summary(silver_from(spark, events))
    path = str(tmp_path / "fleet_daily_summary")

    def snapshot():
        rows = spark.read.format("delta").load(path).drop("updated_at").collect()
        return sorted((r.asDict() for r in rows), key=lambda d: (d["event_date"], d["powertrain"]))

    merge_into(summary, path, ["event_date", "powertrain"], partition_by="event_date")
    first = snapshot()
    merge_into(summary, path, ["event_date", "powertrain"], partition_by="event_date")
    second = snapshot()

    assert first == second
    assert sum(r["active_vehicles"] for r in first) == 20
    assert sum(r["events"] for r in first) == 20 * 30
