"""Data quality rules.

Each rule is defined once, with two equivalent forms:

- ``sql``: a Spark SQL boolean expression, applied in the Silver stream
- ``check``: a Python predicate, used by the unit tests and the generator

Keeping both side by side means a rule can't drift between the code that
enforces it and the code that tests it. A record that fails any rule goes to
the quarantine table with the names of the rules it broke, instead of being
dropped.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .schema import LIMITS, SCHEMA_VERSION

Event = dict[str, Any]

# Readings stamped more than this far in the future point to a bad device clock.
MAX_CLOCK_SKEW = timedelta(minutes=5)


@dataclass(frozen=True)
class Rule:
    name: str
    sql: str
    check: Callable[[Event, datetime], bool]
    description: str


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        try:
            ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _present(field: str) -> Callable[[Event, datetime], bool]:
    return lambda e, _now: e.get(field) not in (None, "")


def _in_range(field: str) -> Callable[[Event, datetime], bool]:
    lo, hi = LIMITS[field]

    def check(e: Event, _now: datetime) -> bool:
        v = e.get(field)
        if v is None:
            return True
        try:
            return lo <= float(v) <= hi
        except (TypeError, ValueError):
            return False

    return check


def _range_sql(field: str) -> str:
    lo, hi = LIMITS[field]
    return f"{field} IS NULL OR {field} BETWEEN {lo} AND {hi}"


def _not_future(e: Event, now: datetime) -> bool:
    ts = _parse_ts(e.get("event_ts"))
    return ts is not None and ts <= now + MAX_CLOCK_SKEW


def _powertrain_consistent(e: Event, _now: datetime) -> bool:
    pt = e.get("powertrain")
    if pt == "EV":
        return e.get("fuel_level_pct") is None
    if pt == "ICE":
        return e.get("battery_soc_pct") is None
    return True  # an unknown powertrain is caught by powertrain_valid


RULES: tuple[Rule, ...] = (
    Rule(
        "event_id_present",
        "event_id IS NOT NULL AND length(event_id) > 0",
        _present("event_id"),
        "Every event needs an ID for deduplication",
    ),
    Rule(
        "vehicle_id_present",
        "vehicle_id IS NOT NULL AND length(vehicle_id) > 0",
        _present("vehicle_id"),
        "Every event belongs to a vehicle",
    ),
    Rule(
        "event_ts_valid",
        "event_ts IS NOT NULL",
        lambda e, _now: _parse_ts(e.get("event_ts")) is not None,
        "Event time must be present and parseable",
    ),
    Rule(
        "event_ts_not_future",
        f"event_ts <= current_timestamp() + INTERVAL {int(MAX_CLOCK_SKEW.total_seconds() // 60)} MINUTES",
        _not_future,
        "Event time can't be in the future (bad device clock)",
    ),
    Rule(
        "powertrain_valid",
        "powertrain IN ('EV', 'ICE')",
        lambda e, _now: e.get("powertrain") in ("EV", "ICE"),
        "Powertrain must be EV or ICE",
    ),
    Rule(
        "powertrain_consistent",
        "NOT (coalesce(powertrain = 'EV', false) AND fuel_level_pct IS NOT NULL) "
        "AND NOT (coalesce(powertrain = 'ICE', false) AND battery_soc_pct IS NOT NULL)",
        _powertrain_consistent,
        "EVs don't report fuel; ICE vehicles don't report battery charge",
    ),
    Rule("lat_in_range", _range_sql("lat"), _in_range("lat"), "Latitude within -90..90"),
    Rule("lon_in_range", _range_sql("lon"), _in_range("lon"), "Longitude within -180..180"),
    Rule("speed_in_range", _range_sql("speed_kph"), _in_range("speed_kph"), "Speed within 0..260 km/h"),
    Rule(
        "engine_temp_in_range",
        _range_sql("engine_temp_c"),
        _in_range("engine_temp_c"),
        "Temperature within -40..150 °C",
    ),
    Rule(
        "battery_in_range",
        _range_sql("battery_soc_pct"),
        _in_range("battery_soc_pct"),
        "Battery charge within 0..100%",
    ),
    Rule(
        "fuel_in_range",
        _range_sql("fuel_level_pct"),
        _in_range("fuel_level_pct"),
        "Fuel level within 0..100%",
    ),
    Rule(
        "schema_version_supported",
        f"schema_version = {SCHEMA_VERSION}",
        lambda e, _now: e.get("schema_version") == SCHEMA_VERSION,
        "Only the current contract version is accepted",
    ),
)

RULE_NAMES: tuple[str, ...] = tuple(r.name for r in RULES)


def failed_rules(event: Event, now: datetime | None = None) -> list[str]:
    """Return the names of the rules this event breaks (empty list = valid)."""
    now = now or datetime.now(timezone.utc)
    return [r.name for r in RULES if not r.check(event, now)]


def is_valid(event: Event, now: datetime | None = None) -> bool:
    return not failed_rules(event, now)
