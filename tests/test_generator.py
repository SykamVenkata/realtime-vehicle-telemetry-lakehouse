from datetime import datetime, timedelta, timezone

from telemetry.generator import Fleet, InjectionRates, main
from telemetry.quality import failed_rules
from telemetry.schema import FIELD_NAMES

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)


def test_events_follow_the_contract():
    fleet = Fleet(50, seed=1, rates=InjectionRates(0, 0, 0))
    events = list(fleet.tick(NOW))
    assert len(events) == 50
    for e in events:
        assert tuple(e) == FIELD_NAMES
        assert failed_rules(e, NOW) == []


def test_ev_and_ice_report_the_right_energy_field():
    events = list(Fleet(200, seed=2, rates=InjectionRates(0, 0, 0)).tick(NOW))
    evs = [e for e in events if e["powertrain"] == "EV"]
    ices = [e for e in events if e["powertrain"] == "ICE"]
    assert evs and ices
    assert all(e["battery_soc_pct"] is not None and e["fuel_level_pct"] is None for e in evs)
    assert all(e["fuel_level_pct"] is not None and e["battery_soc_pct"] is None for e in ices)


def test_same_seed_gives_the_same_stream():
    a = list(Fleet(20, seed=7).tick(NOW))
    b = list(Fleet(20, seed=7).tick(NOW))
    assert a == b


def test_injected_duplicates_reuse_event_ids():
    fleet = Fleet(100, seed=3, rates=InjectionRates(duplicate=0.5, late=0, bad=0))
    events = [e for _ in range(3) for e in fleet.tick(NOW)]
    ids = [e["event_id"] for e in events]
    assert len(ids) > len(set(ids))
    assert fleet.stats["duplicates"] == len(ids) - len(set(ids))


def test_late_events_are_5_to_15_minutes_old():
    fleet = Fleet(200, seed=4, rates=InjectionRates(duplicate=0, late=1.0, bad=0))
    for e in fleet.tick(NOW):
        ts = datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00"))
        assert timedelta(minutes=5) <= NOW - ts <= timedelta(minutes=15)


def test_bad_records_break_at_least_one_rule():
    fleet = Fleet(300, seed=5, rates=InjectionRates(duplicate=0, late=0, bad=1.0))
    events = list(fleet.tick(NOW))
    assert all(failed_rules(e) for e in events)


def test_vehicles_move_and_odometers_only_go_up():
    fleet = Fleet(30, seed=6, rates=InjectionRates(0, 0, 0))
    first = {e["vehicle_id"]: e for e in fleet.tick(NOW)}
    for i in range(1, 30):
        last = {e["vehicle_id"]: e for e in fleet.tick(NOW + timedelta(seconds=i))}
    assert all(last[v]["odometer_km"] >= first[v]["odometer_km"] for v in first)
    assert any(last[v]["odometer_km"] > first[v]["odometer_km"] for v in first)


def test_cli_writes_jsonl(tmp_path):
    out = tmp_path / "events.jsonl"
    assert (
        main(
            [
                "--sink",
                "file",
                "--path",
                str(out),
                "--vehicles",
                "10",
                "--interval",
                "0.01",
                "--duration",
                "0.01",
            ]
        )
        == 0
    )
    lines = out.read_text().splitlines()
    assert len(lines) >= 10
