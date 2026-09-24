"""One generic normalizer shared by every IngestionSource. The actual wire
format is JSON everywhere already -- tailscale-ros-telemetry's /spi_data
publishes a std_msgs/String containing a JSON snapshot (confirmed via
subscriber.py's json.loads and rosbag_db3_to_csv.py's CDR decoder, which
only understands std_msgs/msg/String), and a CSV row is just a flat JSON
object with string values. So Ros2Source's hand-rolled field lookups and
CsvFileSource's separate alias-tuple logic were the same problem solved
twice. This module solves it once: flatten whatever nested dict arrives
into dotted keys, normalize key casing/punctuation, and alias-match against
canonical fields. A rosbag reader or a future Zenoh payload only needs to
hand this a dict -- extending to a new field or format means editing the
alias tables below, not writing a new adapter's worth of field lookups.
"""

from __future__ import annotations

import re
from typing import Any

from evil.models import GpsReading, JoulemeterReading, PlannerReading, RawSample

_TS_ALIASES = ("globalts", "timestamp", "time")
_LAT_ALIASES = ("gpslat", "latitude", "lat")
_LON_ALIASES = ("gpslong", "gpslon", "longitude", "lon", "long")
_SPEED_ALIASES = ("speed", "speedmps", "gpsspeed", "velocity", "filteredspeed")
_HEADING_ALIASES = ("gpsheading", "heading")
_VOLTAGE_ALIASES = ("powervoltage", "busvoltage", "voltage")
_CURRENT_ALIASES = ("powercurrent", "current")
_PLANNED_PATH_ALIASES = ("plannedpath", "driverplannedpath", "plannerplannedpath")
_TARGET_SPEED_ALIASES = ("targetspeed", "drivertargetspeed", "plannertargetspeed")


def flatten(payload: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Recursively flattens nested dicts to dotted keys, e.g.
    {"power": {"voltage": 48.2}} -> {"power.voltage": 48.2}. Non-dict values
    (including lists) are kept as-is; nothing in this telemetry format nests
    data inside lists today."""
    result: dict[str, Any] = {}
    for key, value in payload.items():
        full_key = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            result.update(flatten(value, full_key))
        else:
            result[full_key] = value
    return result


def normalize_key(name: str) -> str:
    """Lowercase, strip all punctuation, so "gps.lat", "GPS_LAT", and
    "gps lat" all collide on the same alias -- matches how
    RaceEngineerDashboard's own CSV replay already documents its column
    convention (frontend/README.md's "CSV expectations" section)."""
    return re.sub(r"[^a-z0-9]", "", name.strip().lower())


def _first_present(row: dict[str, Any], aliases: tuple[str, ...]) -> Any | None:
    for alias in aliases:
        value = row.get(alias)
        if value not in (None, ""):
            return value
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_raw_sample(
    run_id: str,
    payload: dict[str, Any],
    *,
    ts: float | None = None,
    default_ts: float = 0.0,
) -> RawSample:
    """Normalize one payload (nested JSON dict, or an already-flat CSV row)
    into a RawSample. Pass `ts` when the caller already knows the correct
    timestamp and it shouldn't come from the payload (e.g. Ros2Source uses
    wall-clock receipt time, not anything inside the message). Otherwise the
    timestamp is read from the payload's own ts alias, falling back to
    `default_ts` (e.g. CsvFileSource's row-index fallback) if absent.
    """
    row = {normalize_key(k): v for k, v in flatten(payload).items()}

    resolved_ts = ts
    if resolved_ts is None:
        ts_raw = _first_present(row, _TS_ALIASES)
        resolved_ts = _to_float(ts_raw) if ts_raw is not None else default_ts

    lat = _to_float(_first_present(row, _LAT_ALIASES))
    lon = _to_float(_first_present(row, _LON_ALIASES))
    gps = None
    if lat is not None and lon is not None:
        gps = GpsReading(
            ts=resolved_ts,
            lat=lat,
            lon=lon,
            speed=_to_float(_first_present(row, _SPEED_ALIASES)),
            heading=_to_float(_first_present(row, _HEADING_ALIASES)),
        )

    voltage = _to_float(_first_present(row, _VOLTAGE_ALIASES))
    current = _to_float(_first_present(row, _CURRENT_ALIASES))
    joulemeter = None
    if voltage is not None and current is not None:
        joulemeter = JoulemeterReading(ts=resolved_ts, voltage=voltage, current=current)

    planned_path = _first_present(row, _PLANNED_PATH_ALIASES)
    target_speed = _to_float(_first_present(row, _TARGET_SPEED_ALIASES))
    planner = None
    if planned_path is not None or target_speed is not None:
        planner = PlannerReading(ts=resolved_ts, planned_path=planned_path, target_speed=target_speed)

    return RawSample(run_id=run_id, ts=resolved_ts, joulemeter=joulemeter, planner=planner, gps=gps)
