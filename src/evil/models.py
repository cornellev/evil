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
    voltage: float
    current: float


@dataclass(frozen=True)
class PlannerReading:
    ts: float
    planned_path: str | None
    target_speed: float | None


@dataclass(frozen=True)
class GpsReading:
    ts: float
    lat: float
    lon: float
    speed: float | None = None
    heading: float | None = None


@dataclass(frozen=True)
class RawSample:
    run_id: str
    ts: float
    joulemeter: JoulemeterReading | None = None
    planner: PlannerReading | None = None
    gps: GpsReading | None = None
