"""Synthetic connected-vehicle fleet.

Simulates a mixed EV and ICE fleet driving around a few US metro areas and
emits one telemetry reading per vehicle per tick. To exercise the pipeline the
way real devices do, it also injects, at configurable rates:

- duplicates   the same event sent twice (retries over a flaky cellular link)
- late events  readings buffered offline and delivered minutes later
- bad records  out-of-range values, missing IDs, bad clocks, wrong contract version

Run ``python -m telemetry.generator --help`` for the command line.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .schema import SCHEMA_VERSION

# (name, lat, lon) — vehicles start near one of these and drift from there.
METROS = (
    ("Dallas-Fort Worth", 32.85, -97.00),
    ("Austin", 30.27, -97.74),
    ("Detroit", 42.33, -83.05),
    ("Phoenix", 33.45, -112.07),
    ("Atlanta", 33.75, -84.39),
)

DTC_POOL = ("P0128", "P0300", "P0420", "P0171", "P0A80", "U0100", "B1342")
FIRMWARE = ("4.2.1", "4.2.3", "4.3.0")


@dataclass
class InjectionRates:
    duplicate: float = 0.01
    late: float = 0.02
    bad: float = 0.005


@dataclass
class Vehicle:
    vehicle_id: str
    powertrain: str
    lat: float
    lon: float
    heading: float
    speed_kph: float
    odometer_km: float
    engine_temp_c: float
    energy_pct: float  # battery SoC for EVs, fuel level for ICE
    firmware_version: str
    dtc_codes: list[str] = field(default_factory=list)

    def step(self, rng: random.Random, seconds: float) -> None:
        # Speed follows a gentle random walk, with occasional stops.
        if rng.random() < 0.02:
            self.speed_kph = 0.0
        else:
            self.speed_kph = min(140.0, max(0.0, self.speed_kph + rng.gauss(0, 6)))
        self.heading = (self.heading + rng.gauss(0, 10)) % 360
        km = self.speed_kph * seconds / 3600
        self.odometer_km += km
        self.lat += km / 111.0 * math.cos(math.radians(self.heading))
        self.lon += km / (111.0 * math.cos(math.radians(self.lat))) * math.sin(math.radians(self.heading))
        target_temp = 88.0 + self.speed_kph * 0.12
        self.engine_temp_c += (target_temp - self.engine_temp_c) * 0.1 + rng.gauss(0, 0.8)
        drain = km * (0.25 if self.powertrain == "EV" else 0.12)
        self.energy_pct = max(0.0, self.energy_pct - drain)
        if self.energy_pct < 5 and rng.random() < 0.05:
            self.energy_pct = rng.uniform(80, 100)  # recharged or refueled
        if rng.random() < 0.0005:
            self.dtc_codes = sorted(set(self.dtc_codes) | {rng.choice(DTC_POOL)})
        elif self.dtc_codes and rng.random() < 0.001:
            self.dtc_codes = []

    def reading(self, ts: datetime, rng: random.Random) -> dict:
        is_ev = self.powertrain == "EV"
        return {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "vehicle_id": self.vehicle_id,
            "event_ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "powertrain": self.powertrain,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed_kph": round(self.speed_kph, 1),
            "engine_temp_c": round(self.engine_temp_c, 1),
            "battery_soc_pct": round(self.energy_pct, 1) if is_ev else None,
            "fuel_level_pct": None if is_ev else round(self.energy_pct, 1),
            "odometer_km": round(self.odometer_km, 2),
            "harsh_brake": rng.random() < 0.004 and self.speed_kph > 30,
            "dtc_codes": list(self.dtc_codes),
            "firmware_version": self.firmware_version,
            "schema_version": SCHEMA_VERSION,
        }


def _corrupt(event: dict, rng: random.Random) -> dict:
    """Return a copy of ``event`` with one realistic defect."""
    bad = dict(event)
    kind = rng.choice(("speed", "vehicle_id", "future_ts", "powertrain_mix", "schema", "temp"))
    if kind == "speed":
        bad["speed_kph"] = 999.0
    elif kind == "vehicle_id":
        bad["vehicle_id"] = None
    elif kind == "future_ts":
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        bad["event_ts"] = future.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    elif kind == "powertrain_mix":
        bad["fuel_level_pct"] = 42.0 if bad["powertrain"] == "EV" else None
        bad["battery_soc_pct"] = None if bad["powertrain"] == "EV" else 42.0
    elif kind == "schema":
        bad["schema_version"] = SCHEMA_VERSION + 1
    else:
        bad["engine_temp_c"] = -273.0
    return bad


class Fleet:
    def __init__(
        self, size: int, seed: int | None = 42, rates: InjectionRates | None = None, ev_share: float = 0.4
    ):
        self.rng = random.Random(seed)
        self.rates = rates or InjectionRates()
        self.stats: Counter[str] = Counter()
        self._recent: list[dict] = []
        self.vehicles = [self._new_vehicle(i, ev_share) for i in range(size)]

    def _new_vehicle(self, i: int, ev_share: float) -> Vehicle:
        rng = self.rng
        _, lat, lon = rng.choice(METROS)
        powertrain = "EV" if rng.random() < ev_share else "ICE"
        return Vehicle(
            vehicle_id=f"VH{i:06d}",
            powertrain=powertrain,
            lat=lat + rng.uniform(-0.3, 0.3),
            lon=lon + rng.uniform(-0.3, 0.3),
            heading=rng.uniform(0, 360),
            speed_kph=rng.uniform(0, 90),
            odometer_km=rng.uniform(1_000, 120_000),
            engine_temp_c=rng.uniform(70, 95),
            energy_pct=rng.uniform(20, 100),
            firmware_version=rng.choice(FIRMWARE),
        )

    def tick(self, now: datetime | None = None, seconds: float = 1.0) -> Iterator[dict]:
        """Advance every vehicle by ``seconds`` and yield the events for this tick."""
        now = now or datetime.now(timezone.utc)
        rng, rates = self.rng, self.rates
        for v in self.vehicles:
            v.step(rng, seconds)
            ts = now
            if rng.random() < rates.late:
                ts = now - timedelta(seconds=rng.uniform(5 * 60, 15 * 60))
                self.stats["late"] += 1
            event = v.reading(ts, rng)
            if rng.random() < rates.bad:
                event = _corrupt(event, rng)
                self.stats["bad"] += 1
            self.stats["events"] += 1
            yield event
            self._recent.append(event)
            if rng.random() < rates.duplicate and self._recent:
                self.stats["duplicates"] += 1
                self.stats["events"] += 1
                yield dict(rng.choice(self._recent))
        self._recent = self._recent[-500:]


def _stdout_sink():
    def send(event: dict) -> None:
        sys.stdout.write(json.dumps(event) + "\n")

    return send, sys.stdout.flush


def _file_sink(path: str):
    fh = open(path, "a", encoding="utf-8")  # noqa: SIM115 - closed by the returned closer

    def send(event: dict) -> None:
        fh.write(json.dumps(event) + "\n")

    return send, fh.close


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Stream synthetic vehicle telemetry.")
    p.add_argument("--vehicles", type=int, default=200, help="fleet size (one event per vehicle per tick)")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between ticks")
    p.add_argument("--duration", type=float, default=0, help="seconds to run; 0 = forever")
    p.add_argument("--sink", choices=("stdout", "file", "kafka"), default="stdout")
    p.add_argument("--path", default="events.jsonl", help="output file for --sink file")
    p.add_argument("--topic", default="vehicle-telemetry", help="Kafka / Event Hubs topic")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--late-rate", type=float, default=InjectionRates.late)
    p.add_argument("--duplicate-rate", type=float, default=InjectionRates.duplicate)
    p.add_argument("--bad-rate", type=float, default=InjectionRates.bad)
    args = p.parse_args(argv)

    fleet = Fleet(
        args.vehicles,
        seed=args.seed,
        rates=InjectionRates(args.duplicate_rate, args.late_rate, args.bad_rate),
    )

    if args.sink == "kafka":
        from .producer import kafka_sink

        send, close = kafka_sink(args.topic)
    elif args.sink == "file":
        send, close = _file_sink(args.path)
    else:
        send, close = _stdout_sink()

    started = time.monotonic()
    try:
        while True:
            tick_start = time.monotonic()
            for event in fleet.tick(seconds=args.interval):
                send(event)
            elapsed = time.monotonic() - started
            if args.duration and elapsed >= args.duration:
                break
            if args.sink != "stdout" and elapsed >= 1 and int(elapsed) % 10 == 0:
                rate = fleet.stats["events"] / max(elapsed, 1e-6)
                print(f"[generator] {dict(fleet.stats)} ~{rate:,.0f} events/s", file=sys.stderr)
            time.sleep(max(0.0, args.interval - (time.monotonic() - tick_start)))
    except KeyboardInterrupt:
        pass
    finally:
        close()
    print(f"[generator] done: {dict(fleet.stats)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
