"""Shared machinery for strict telemetry schemas (v1, v2, the Apr 10 transitional shape).

A schema is an exact key spec plus a mapping to RawSample. Strict means: exact key set at
every level (nothing missing, extra or renamed; case-sensitive), `seq`/`global_ts`/
`_t_publish_ns`/`errcount` and every `ts` are integers, every other leaf is a number where
NaN is valid. Booleans, strings, null and nested surprises are rejected. Any change to a
shape is a NEW schema id, never an edit to an existing one.

Shapes are verified against real data in telemetry-v1-v2-shapes.md.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

from evil.models import (
    GpsReading,
    JoulemeterReading,
    MotorReading,
    RawSample,
    RpmReading,
    SteeringReading,
)

INT = "int"
NUM = "num"
MAX_REASONS = 5
CAR = "uc26"


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check(path: str, value: Any, kind: str, reasons: list[str]) -> None:
    ok = _is_int(value) if kind == INT else _is_num(value)
    if not ok:
        reasons.append(f"{path}: expected {'integer' if kind == INT else 'number'}, got {type(value).__name__}")


def _f(value: Any) -> float | None:
    """A number as a float; NaN and infinities become None (stored as NULL)."""
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


@dataclass(frozen=True)
class StrictSchema:
    schema_id: str
    parser_version: str
    spec: dict[str, Any]

    def flat_keys(self) -> set[str]:
        """The dotted leaf keys, e.g. {'seq', 'power.ts', ...} (a CSV header minus 'timestamp')."""
        keys: set[str] = set()
        for name, spec in self.spec.items():
            if isinstance(spec, dict):
                keys.update(f"{name}.{leaf}" for leaf in spec)
            else:
                keys.add(name)
        return keys

    def validate(self, payload: Any) -> list[str]:
        """Reasons the payload does not conform (empty list = conforms exactly)."""
        reasons: list[str] = []
        if not isinstance(payload, dict):
            return [f"payload is {type(payload).__name__}, expected an object"]
        spec_all = self.spec
        for key in sorted(set(payload) - set(spec_all)):
            reasons.append(f"unexpected key {key!r}")
        for key in sorted(set(spec_all) - set(payload)):
            reasons.append(f"missing key {key!r}")
        for name, spec in spec_all.items():
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

    def conforms(self, payload: Any) -> bool:
        return not self.validate(payload)

    @staticmethod
    def repeat_key(payload: dict) -> str:
        """Identity of a snapshot for repeat detection: the whole body except the
        publisher's `_t_publish_ns` (NaN-safe: json emits the same NaN token)."""
        return json.dumps({k: v for k, v in payload.items() if k != "_t_publish_ns"}, sort_keys=True)

    @staticmethod
    def to_sample(run_id: str, payload: dict, ts: float) -> RawSample:
        """Map a conforming payload to a RawSample. `ts` is EVIL's row time (record /
        receive time in seconds). A group whose data fields are all NaN becomes no row
        (a NULL pointer on the snapshot) rather than an empty row. Fields a schema lacks
        (`errcount` in v1, `throttle` in v2, `duty_cycle` in v1) are stored as NULL."""
        p = payload
        power, steer, front, back, gps, motor = (
            p[k] for k in ("power", "steering", "rpm_front", "rpm_back", "gps", "motor"))

        def keep(*values: float | None) -> bool:
            return any(v is not None for v in values)

        voltage, current = _f(power["voltage"]), _f(power["current"])
        brake, angle = _f(steer["brake_pressure"]), _f(steer["turn_angle"])
        fl, fr = _f(front["rpm_left"]), _f(front["rpm_right"])
        bl, br = _f(back["rpm_left"]), _f(back["rpm_right"])
        lat, lon, heading, speed = _f(gps["lat"]), _f(gps["long"]), _f(gps["heading"]), _f(gps["speed"])
        m_rpm, throttle, duty = _f(motor["rpm"]), _f(motor.get("throttle")), _f(motor.get("duty_cycle"))

        return RawSample(
            run_id=run_id,
            ts=ts,
            joulemeter=JoulemeterReading(ts, voltage, current, power["ts"]) if keep(voltage, current) else None,
            steering=SteeringReading(ts, brake, angle, steer["ts"]) if keep(brake, angle) else None,
            rpm_front=RpmReading(ts, fl, fr, front["ts"]) if keep(fl, fr) else None,
            rpm_back=RpmReading(ts, bl, br, back["ts"]) if keep(bl, br) else None,
            gps=GpsReading(ts, lat, lon, speed, heading, gps["ts"]) if keep(lat, lon, heading, speed) else None,
            motor=MotorReading(ts, m_rpm, throttle, motor["ts"], duty) if keep(m_rpm, throttle, duty) else None,
            device_seq=p["seq"],
            device_global_ts_us=p["global_ts"],
            publish_ns=p["_t_publish_ns"],
            filtered_speed=_f(p["filtered"]["speed"]),
            errcount=p.get("errcount"),
        )


# The groups every known shape shares; schemas differ only in `errcount` and the motor group.
def base_spec(motor_control: str, errcount: bool) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "seq": INT,
        "global_ts": INT,
        "_t_publish_ns": INT,
        "power": {"ts": INT, "current": NUM, "voltage": NUM},
        "steering": {"ts": INT, "brake_pressure": NUM, "turn_angle": NUM},
        "rpm_front": {"ts": INT, "rpm_left": NUM, "rpm_right": NUM},
        "rpm_back": {"ts": INT, "rpm_left": NUM, "rpm_right": NUM},
        "gps": {"ts": INT, "lat": NUM, "long": NUM, "heading": NUM, "speed": NUM},
        "motor": {"ts": INT, "rpm": NUM, motor_control: NUM},
        "filtered": {"speed": NUM},
    }
    if errcount:
        spec["errcount"] = INT
    return spec
