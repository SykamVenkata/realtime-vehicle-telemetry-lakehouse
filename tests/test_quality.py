from datetime import datetime, timedelta, timezone

import pytest

from telemetry.quality import RULE_NAMES, RULES, failed_rules, is_valid

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)


def good_event(**overrides):
    event = {
        "event_id": "8f14e45f-ceea-4e7c-9b2f-1a2b3c4d5e6f",
        "vehicle_id": "VH000001",
        "event_ts": "2026-11-20T11:59:58.000Z",
        "powertrain": "EV",
        "lat": 32.85,
        "lon": -97.0,
        "speed_kph": 62.5,
        "engine_temp_c": 91.0,
        "battery_soc_pct": 71.2,
        "fuel_level_pct": None,
        "odometer_km": 45210.5,
        "harsh_brake": False,
        "dtc_codes": [],
        "firmware_version": "4.3.0",
        "schema_version": 1,
    }
    event.update(overrides)
    return event


def test_a_clean_event_passes_every_rule():
    assert failed_rules(good_event(), NOW) == []
    assert is_valid(good_event(), NOW)


@pytest.mark.parametrize(
    ("overrides", "rule"),
    [
        ({"event_id": None}, "event_id_present"),
        ({"event_id": ""}, "event_id_present"),
        ({"vehicle_id": None}, "vehicle_id_present"),
        ({"event_ts": None}, "event_ts_valid"),
        ({"event_ts": "not-a-time"}, "event_ts_valid"),
        ({"event_ts": (NOW + timedelta(hours=2)).isoformat()}, "event_ts_not_future"),
        ({"powertrain": "HYBRID"}, "powertrain_valid"),
        ({"fuel_level_pct": 40.0}, "powertrain_consistent"),
        ({"lat": 91.0}, "lat_in_range"),
        ({"lon": -181.0}, "lon_in_range"),
        ({"speed_kph": 999.0}, "speed_in_range"),
        ({"speed_kph": -1.0}, "speed_in_range"),
        ({"engine_temp_c": -273.0}, "engine_temp_in_range"),
        ({"battery_soc_pct": 101.0}, "battery_in_range"),
        ({"schema_version": 2}, "schema_version_supported"),
    ],
)
def test_each_defect_is_caught_by_the_right_rule(overrides, rule):
    assert rule in failed_rules(good_event(**overrides), NOW)


def test_small_clock_skew_is_tolerated():
    slightly_ahead = (NOW + timedelta(minutes=2)).isoformat()
    assert "event_ts_not_future" not in failed_rules(good_event(event_ts=slightly_ahead), NOW)


def test_missing_optional_readings_are_allowed():
    assert failed_rules(good_event(lat=None, lon=None, speed_kph=None), NOW) == []


def test_ice_vehicle_must_not_report_battery():
    ice = good_event(powertrain="ICE", battery_soc_pct=55.0, fuel_level_pct=60.0)
    assert failed_rules(ice, NOW) == ["powertrain_consistent"]


def test_every_rule_has_a_unique_name_and_sql():
    assert len(set(RULE_NAMES)) == len(RULES)
    assert all(r.sql.strip() and r.description for r in RULES)
