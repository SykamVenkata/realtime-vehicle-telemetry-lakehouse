"""Event contract for vehicle telemetry.

The contract is declared once as plain Python so that the generator, the data
quality rules and the Spark jobs all agree on field names and types. The Spark
``StructType`` is built from the same declaration, lazily, so this module can
be imported without PySpark installed.
"""

from __future__ import annotations

from dataclasses import dataclass

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class Field:
    name: str
    type: str  # one of: string, timestamp, double, int, long, boolean, array<string>
    nullable: bool = True
    description: str = ""


EVENT_FIELDS: tuple[Field, ...] = (
    Field("event_id", "string", False, "UUID assigned by the vehicle; used for deduplication"),
    Field("vehicle_id", "string", False, "Synthetic vehicle identifier"),
    Field("event_ts", "timestamp", False, "When the reading was taken on the vehicle (event time)"),
    Field("powertrain", "string", False, "EV or ICE"),
    Field("lat", "double", True, "Latitude in degrees"),
    Field("lon", "double", True, "Longitude in degrees"),
    Field("speed_kph", "double", True, "Vehicle speed"),
    Field("engine_temp_c", "double", True, "Engine or motor temperature"),
    Field("battery_soc_pct", "double", True, "Battery state of charge (EV only)"),
    Field("fuel_level_pct", "double", True, "Fuel level (ICE only)"),
    Field("odometer_km", "double", True, "Odometer reading"),
    Field("harsh_brake", "boolean", True, "Harsh braking detected in this interval"),
    Field("dtc_codes", "array<string>", True, "Active diagnostic trouble codes"),
    Field("firmware_version", "string", True, "Telematics unit firmware"),
    Field("schema_version", "int", False, "Version of this contract"),
)

FIELD_NAMES: tuple[str, ...] = tuple(f.name for f in EVENT_FIELDS)

# Physical limits used by the data quality rules.
LIMITS = {
    "lat": (-90.0, 90.0),
    "lon": (-180.0, 180.0),
    "speed_kph": (0.0, 260.0),
    "engine_temp_c": (-40.0, 150.0),
    "battery_soc_pct": (0.0, 100.0),
    "fuel_level_pct": (0.0, 100.0),
}

# Readings above these values raise an operational alert in the Gold layer.
ALERT_THRESHOLDS = {
    "engine_temp_c": 115.0,
    "battery_soc_pct_low": 10.0,
    "fuel_level_pct_low": 8.0,
}


def spark_schema():
    """Return the event contract as a PySpark ``StructType``."""
    from pyspark.sql import types as T

    mapping = {
        "string": T.StringType(),
        "timestamp": T.TimestampType(),
        "double": T.DoubleType(),
        "int": T.IntegerType(),
        "long": T.LongType(),
        "boolean": T.BooleanType(),
        "array<string>": T.ArrayType(T.StringType()),
    }
    # Every field is nullable at parse time: a missing required field must
    # reach the quality rules and be quarantined, not silently dropped.
    return T.StructType([T.StructField(f.name, mapping[f.type], True) for f in EVENT_FIELDS])
