"""evil.telemetry.v1: the strict wire format of uc26's DAQ telemetry.

The source of truth is `read_snapshot_dict()` in cornellev/uc26_sensor_reader,
republished as JSON in a std_msgs/String by tailscale-ros-telemetry (which adds
`_t_publish_ns`). See recording-catalog-design.md section 4.1 and
telemetry-v1-v2-shapes.md for how this was verified against real data.

Strict means: exact key set at every level (nothing missing, extra or renamed;
case-sensitive), `seq`/`global_ts`/`_t_publish_ns` and every `ts` are integers,
every other leaf is a number where NaN is a valid number (real data is full of
it: no GPS fix, unplugged sensors). Booleans, strings, null and nested
surprises are rejected. Any change to this shape is a NEW schema id, never an
edit here. A message that does not match exactly is non-conforming.
"""

from __future__ import annotations

import json
import math
from typing import Any

from evil.models import (
    GpsReading,
    JoulemeterReading,
    MotorReading,
    RawSample,
    RpmReading,
    SteeringReading,
)

SCHEMA_ID = "evil.telemetry.v1"
PARSER_VERSION = "telemetry.v1/1"
CAR = "uc26"

_INT = "int"
_NUM = "num"

SPEC: dict[str, Any] = {
    "seq": _INT,
    "global_ts": _INT,
    "_t_publish_ns": _INT,
    "power": {"ts": _INT, "current": _NUM, "voltage": _NUM},
    "steering": {"ts": _INT, "brake_pressure": _NUM, "turn_angle": _NUM},
    "rpm_front": {"ts": _INT, "rpm_left": _NUM, "rpm_right": _NUM},
    "rpm_back": {"ts": _INT, "rpm_left": _NUM, "rpm_right": _NUM},
    "gps": {"ts": _INT, "lat": _NUM, "long": _NUM, "heading": _NUM, "speed": _NUM},
    "motor": {"ts": _INT, "rpm": _NUM, "throttle": _NUM},
    "filtered": {"speed": _NUM},
}

MAX_REASONS = 5


def flat_keys() -> set[str]:
    """The dotted leaf keys, e.g. {'seq', 'power.ts', ...} (a CSV header minus 'timestamp')."""
    keys: set[str] = set()
    for name, spec in SPEC.items():
        if isinstance(spec, dict):
            keys.update(f"{name}.{leaf}" for leaf in spec)
        else:
            keys.add(name)
    return keys


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check(path: str, value: Any, kind: str, reasons: list[str]) -> None:
    ok = _is_int(value) if kind == _INT else _is_num(value)
    if not ok:
        reasons.append(f"{path}: expected {'integer' if kind == _INT else 'number'}, got {type(value).__name__}")


def validate(payload: Any) -> list[str]:
    """Reasons the payload does not conform (empty list = conforms exactly)."""
    reasons: list[str] = []
    if not isinstance(payload, dict):
        return [f"payload is {type(payload).__name__}, expected an object"]

    for key in sorted(set(payload) - set(SPEC)):
        reasons.append(f"unexpected key {key!r}")
    for key in sorted(set(SPEC) - set(payload)):
        reasons.append(f"missing key {key!r}")

    for name, spec in SPEC.items():
        if name not in payload:
            continue
        value = payload[name]
        if isinstance(spec, dict):
            if not isinstance(value, dict):
                reasons.append(f"{name}: expected an object, got {type(value).__name__}")
                continue
            for key in sorted(set(value) - set(spec)):
                reasons.append(f"unexpected key '{name}.{key}'")
            for key in sorted(set(spec) - set(value)):
                reasons.append(f"missing key '{name}.{key}'")
            for leaf, kind in spec.items():
                if leaf in value:
                    _check(f"{name}.{leaf}", value[leaf], kind, reasons)
        else:
            _check(name, value, spec, reasons)
    return reasons[:MAX_REASONS]


def conforms(payload: Any) -> bool:
    return not validate(payload)


def repeat_key(payload: dict) -> str:
    """Identity of a snapshot for repeat detection: the whole body except the
    publisher's `_t_publish_ns` (NaN-safe: json emits the same NaN token)."""
    return json.dumps({k: v for k, v in payload.items() if k != "_t_publish_ns"}, sort_keys=True)


def _f(value: Any) -> float | None:
    """A number as a float; NaN and infinities become None (stored as NULL)."""
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def to_sample(run_id: str, payload: dict, ts: float) -> RawSample:
    """Map a conforming payload to a RawSample. `ts` is EVIL's row time: the
    container's record time in seconds. A group whose data fields are all NaN
    becomes no row (a NULL pointer on the snapshot) rather than an empty row."""
    p = payload
    power, steer, front, back, gps, motor = (p[k] for k in ("power", "steering", "rpm_front", "rpm_back", "gps", "motor"))

    def keep(*values: float | None) -> bool:
        return any(v is not None for v in values)

    voltage, current = _f(power["voltage"]), _f(power["current"])
    brake, angle = _f(steer["brake_pressure"]), _f(steer["turn_angle"])
    fl, fr = _f(front["rpm_left"]), _f(front["rpm_right"])
    bl, br = _f(back["rpm_left"]), _f(back["rpm_right"])
    lat, lon, heading, speed = _f(gps["lat"]), _f(gps["long"]), _f(gps["heading"]), _f(gps["speed"])
    m_rpm, throttle = _f(motor["rpm"]), _f(motor["throttle"])

    return RawSample(
        run_id=run_id,
        ts=ts,
        joulemeter=JoulemeterReading(ts, voltage, current, power["ts"]) if keep(voltage, current) else None,
        steering=SteeringReading(ts, brake, angle, steer["ts"]) if keep(brake, angle) else None,
        rpm_front=RpmReading(ts, fl, fr, front["ts"]) if keep(fl, fr) else None,
        rpm_back=RpmReading(ts, bl, br, back["ts"]) if keep(bl, br) else None,
        gps=GpsReading(ts, lat, lon, speed, heading, gps["ts"]) if keep(lat, lon, heading, speed) else None,
        motor=MotorReading(ts, m_rpm, throttle, motor["ts"]) if keep(m_rpm, throttle) else None,
        device_seq=p["seq"],
        device_global_ts_us=p["global_ts"],
        publish_ns=p["_t_publish_ns"],
        filtered_speed=_f(p["filtered"]["speed"]),
    )
