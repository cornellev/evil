"""Transport-independent representation of one ingested tick.

Every IngestionSource, regardless of whether it's reading ROS2 topics today
or Zenoh later, must normalize to this shape before it reaches ingest.py.
Storage and everything downstream never sees the wire format.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JoulemeterReading:
    ts: float
    voltage: float | None
    current: float | None
    device_ts_us: int | None = None


@dataclass(frozen=True)
class PlannerReading:
    ts: float
    planned_path: str | None
    target_speed: float | None


@dataclass(frozen=True)
class GpsReading:
    ts: float
    lat: float | None
    lon: float | None
    speed: float | None = None
    heading: float | None = None
    device_ts_us: int | None = None


@dataclass(frozen=True)
class SteeringReading:
    ts: float
    brake_pressure: float | None
    turn_angle: float | None
    device_ts_us: int | None = None


@dataclass(frozen=True)
class RpmReading:
    """One wheel pair (rpm_front or rpm_back)."""

    ts: float
    rpm_left: float | None
    rpm_right: float | None
    device_ts_us: int | None = None


@dataclass(frozen=True)
class MotorReading:
    ts: float
    rpm: float | None
    throttle: float | None
    device_ts_us: int | None = None


@dataclass(frozen=True)
class RawSample:
    run_id: str
    ts: float
    joulemeter: JoulemeterReading | None = None
    planner: PlannerReading | None = None
    gps: GpsReading | None = None
    steering: SteeringReading | None = None
    rpm_front: RpmReading | None = None
    rpm_back: RpmReading | None = None
    motor: MotorReading | None = None
    # Snapshot-level scalars (strict telemetry payloads only)
    device_seq: int | None = None
    device_global_ts_us: int | None = None
    publish_ns: int | None = None
    filtered_speed: float | None = None
