"""Energy, distance and efficiency, computed the way the Race Engineer Dashboard (RED) does so the
two never disagree (RED: frontend/src/utils/telemetry.ts):

- power (kW) = max(0, current x voltage / 1000): a negative reading counts as zero, never as regen;
- energy (kWh) = trapezoid of that power over the DAQ's own clock (`global_ts`, microseconds):
  sum of (P_prev + P_cur) / 2 x dt;
- distance (m) = sum of straight-line distances between consecutive valid GPS fixes, each measured in the
  local tangent plane of the earlier fix on the WGS84 ellipsoid (ECEF); a fix is valid if finite and not 0/0;
- efficiency (mi/kWh) = distance in miles / energy in kWh (the run's "average efficiency" is this ratio,
  not a mean of instantaneous values).

Additions RED does not need because it works on a live stream: a gap guard (readings further apart than
MAX_GAP_S, or a GPS jump longer than MAX_JUMP_M, contribute nothing instead of a straight-line guess), and
interpolation at segment boundaries so that the segments of a lap add up to the lap.

Time axis: the DAQ clock (`main_snapshot.device_global_ts_us`) when the snapshot has it, else the row time
`global_ts` (recordings that carry no device clock).
"""

from __future__ import annotations

import math
import sqlite3
from typing import Any, Iterable

METERS_PER_MILE = 1609.344
MAX_GAP_S = 5.0          # power readings further apart than this are not bridged
MAX_JUMP_M = 200.0       # a GPS step longer than this is a glitch/dropout, not distance driven
MAX_PLAUSIBLE_MPS = 40.0  # a GPS step faster than this is a glitch (the car does ~10 m/s; 40 = 89 mph)
_A = 6378137.0
_F = 1 / 298.257223563
_E2 = _F * (2 - _F)


# ---- RED's formulas ---------------------------------------------------------

def power_kw(current: float | None, voltage: float | None) -> float | None:
    """max(0, I x V / 1000), None if either reading is missing."""
    if current is None or voltage is None:
        return None
    return max(0.0, current * voltage / 1000.0)


def trapezoid_kwh(p0_kw: float, p1_kw: float, t0_s: float, t1_s: float) -> float:
    dt_h = max(0.0, t1_s - t0_s) / 3600.0
    return (p0_kw + p1_kw) / 2.0 * dt_h if dt_h > 0 else 0.0


def _ecef(lat: float, lon: float) -> tuple[float, float, float]:
    la, lo = math.radians(lat), math.radians(lon)
    n = _A / math.sqrt(1 - _E2 * math.sin(la) ** 2)
    return (n * math.cos(la) * math.cos(lo), n * math.cos(la) * math.sin(lo), n * (1 - _E2) * math.sin(la))


def tangent_distance_m(lat0: float, lon0: float, lat1: float, lon1: float) -> float:
    """RED's calculateLocalTangentDistanceMeters: horizontal distance in the tangent plane at (lat0, lon0)."""
    ox, oy, oz = _ecef(lat0, lon0)
    tx, ty, tz = _ecef(lat1, lon1)
    dx, dy, dz = tx - ox, ty - oy, tz - oz
    la, lo = math.radians(lat0), math.radians(lon0)
    east = -math.sin(lo) * dx + math.cos(lo) * dy
    north = -math.sin(la) * math.cos(lo) * dx - math.sin(la) * math.sin(lo) * dy + math.cos(la) * dz
    return math.hypot(east, north)


def valid_fix(lat: float | None, lon: float | None) -> bool:
    return lat is not None and lon is not None and math.isfinite(lat) and math.isfinite(lon) and lat != 0 and lon != 0


def efficiency_mi_per_kwh(distance_m: float | None, energy_wh: float | None) -> float | None:
    """None when there is no energy to divide by (RED shows 0 in that case)."""
    if distance_m is None or energy_wh is None or energy_wh <= 0:
        return None
    return (distance_m / METERS_PER_MILE) / (energy_wh / 1000.0)


def instant_efficiency_mi_per_kwh(speed_mps: float | None, power_kw_: float | None) -> float | None:
    """RED's calculateEfficiency: speed (mph) / power (kW) = mi/kWh; None at zero power."""
    if speed_mps is None or power_kw_ is None or power_kw_ <= 0:
        return None
    return speed_mps * 2.23694 / power_kw_


# ---- reading a range of snapshots --------------------------------------------

_SNAPSHOTS = """
    SELECT ms.seq, ms.global_ts, ms.device_global_ts_us AS dev_us, ms.filtered_speed,
           j.voltage, j.current, g.lat, g.lon, g.device_ts_us AS gps_us, g.speed AS gps_speed
    FROM main_snapshot ms
    LEFT JOIN joulemeter j ON j.id = ms.joulemeter_id
    LEFT JOIN gps g ON g.id = ms.gps_id
    WHERE ms.run_id = ? AND ms.seq >= ? AND ms.seq <= ?
    ORDER BY ms.seq"""
_SNAPSHOTS_AFTER = _SNAPSHOTS.replace("ms.seq >= ? AND ms.seq <= ?", "ms.seq > ?")


def snapshot_time(row: sqlite3.Row) -> float:
    """Seconds on the DAQ clock if the snapshot carries it, else the row time."""
    return row["dev_us"] / 1e6 if row["dev_us"] is not None else row["global_ts"]


def fix_time(row: sqlite3.Row) -> float:
    """Time of the GPS reading itself (the DAQ clock), else the snapshot time."""
    return row["gps_us"] / 1e6 if row["gps_us"] is not None else snapshot_time(row)


def energy_kwh(samples: Iterable[tuple[float, float]], t0: float | None = None, t1: float | None = None) -> float:
    """Trapezoid energy over (time_s, power_kw) samples in time order. With t0/t1 the integral is cut
    exactly at those times, interpolating power linearly inside the sample interval that contains them.
    Samples further apart than MAX_GAP_S are not bridged."""
    total = 0.0
    prev: tuple[float, float] | None = None
    for cur in samples:
        if prev is not None:
            total += _energy_step(prev, cur, t0, t1)
        prev = cur
    return total


def _energy_step(prev: tuple[float, float], cur: tuple[float, float], t0: float | None = None,
                 t1: float | None = None) -> float:
    """kWh between two consecutive (time_s, power_kw) samples (energy_kwh's one step)."""
    (a_t, a_p), (t, p) = prev, cur
    if not (t > a_t and t - a_t <= MAX_GAP_S):
        return 0.0
    lo = a_t if t0 is None else max(a_t, t0)
    hi = t if t1 is None else min(t, t1)
    if hi <= lo:
        return 0.0

    def at(x: float) -> float:
        return a_p + (p - a_p) * (x - a_t) / (t - a_t)
    return trapezoid_kwh(at(lo), at(hi), lo, hi)


def distinct_fixes(rows: Iterable[sqlite3.Row]) -> list[tuple[float, float, float]]:
    """(time_s, lat, lon) of each distinct valid GPS reading, in order. The DAQ delivers GPS at ~1 Hz and the
    snapshots repeat it ~50 times, so consecutive snapshots with the same reading count once."""
    out: list[tuple[float, float, float]] = []
    key = None
    for r in rows:
        if not valid_fix(r["lat"], r["lon"]):
            continue
        k = (r["gps_us"], r["lat"], r["lon"]) if r["gps_us"] is not None else (r["lat"], r["lon"])
        if k == key:
            continue
        key = k
        out.append((fix_time(r), r["lat"], r["lon"]))
    return out


def path_distance_m(fixes: list[tuple[float, float, float]]) -> float:
    """RED's distance: sum of tangent-plane steps between consecutive fixes, minus guarded glitches."""
    total = 0.0
    for a, b in zip(fixes, fixes[1:]):
        total += _distance_step(a, b)
    return total


def _distance_step(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    """Metres between two consecutive distinct fixes (path_distance_m's one step)."""
    (t0, la0, lo0), (t1, la1, lo1) = a, b
    if t1 - t0 > MAX_GAP_S:
        return 0.0
    d = tangent_distance_m(la0, lo0, la1, lo1)
    dt = t1 - t0
    # a multipath glitch is a jump there and back: both steps imply an impossible speed
    if d <= MAX_JUMP_M and (d <= 1.0 or (dt > 0 and d / dt <= MAX_PLAUSIBLE_MPS)):
        return d
    return 0.0


def range_metrics(conn: sqlite3.Connection, run_id: str, start_seq: int, end_seq: int) -> dict[str, Any]:
    """Energy, distance and efficiency over snapshots start_seq..end_seq (both inclusive)."""
    rows = conn.execute(_SNAPSHOTS, (run_id, start_seq, end_seq)).fetchall()
    return summarize_rows(rows)


def summarize_rows(rows: Iterable[sqlite3.Row]) -> dict[str, Any]:
    return RunTotals().add(rows).result()


class RunTotals:
    """summarize_rows fed in pieces: rows (in seq order) can arrive across many add() calls and the
    result is the same, bit for bit, as one summarize_rows over all of them. The live compiler keeps
    one per open run so a summary refresh reads only the rows since the last one (a full rescan
    every refresh grew with the run: memory and CPU, O(n^2) over a session)."""

    def __init__(self) -> None:
        self.last_seq: int | None = None
        self._first_t: float | None = None
        self._last_t: float | None = None
        self._rows = 0
        self._energy_kwh = 0.0
        self._power_samples = 0
        self._prev_power: tuple[float, float] | None = None
        self._fix_key: tuple | None = None
        self._prev_fix: tuple[float, float, float] | None = None
        self._fixes = 0
        self._distance_m = 0.0

    def add(self, rows: Iterable[sqlite3.Row]) -> "RunTotals":
        for r in rows:
            t = snapshot_time(r)
            if self._rows == 0:
                self._first_t = t
            self._last_t = t
            self._rows += 1
            self.last_seq = r["seq"]
            p = power_kw(r["current"], r["voltage"])
            if p is not None:
                cur = (t, p)
                if self._prev_power is not None:
                    self._energy_kwh += _energy_step(self._prev_power, cur)
                self._prev_power = cur
                self._power_samples += 1
            if valid_fix(r["lat"], r["lon"]):
                k = (r["gps_us"], r["lat"], r["lon"]) if r["gps_us"] is not None else (r["lat"], r["lon"])
                if k != self._fix_key:
                    self._fix_key = k
                    fix = (fix_time(r), r["lat"], r["lon"])
                    if self._prev_fix is not None:
                        self._distance_m += _distance_step(self._prev_fix, fix)
                    self._prev_fix = fix
                    self._fixes += 1
        return self

    def result(self) -> dict[str, Any]:
        energy_wh = self._energy_kwh * 1000.0 if self._power_samples >= 2 else None
        distance = self._distance_m if self._fixes >= 2 else None
        duration = (self._last_t - self._first_t) if self._rows >= 2 else None
        return {
            "energy_wh": energy_wh,
            "distance_m": distance,
            "duration_s": duration,
            "efficiency_mi_per_kwh": efficiency_mi_per_kwh(distance, energy_wh),
            "avg_speed": (distance / duration) if distance is not None and duration and duration > 0 else None,
        }


def turn_count(conn: sqlite3.Connection, run_id: str, start_seq: int, end_seq: int) -> int:
    return conn.execute(
        """SELECT COUNT(*) AS n FROM turns
           WHERE run_id = ? AND start_seq > ? AND start_seq <= ?""",
        (run_id, start_seq, end_seq),
    ).fetchone()["n"]


def store_run_summary(conn: sqlite3.Connection, run_id: str, totals: RunTotals | None = None) -> dict[str, Any]:
    """Whole-run totals (RED's 'average efficiency' = run distance / run energy), kept in run_summary so
    list_runs does not rescan the run. With `totals` (kept by the caller across calls) only rows after
    totals.last_seq are read; rows are streamed, never all held at once."""
    totals = totals if totals is not None else RunTotals()
    after = totals.last_seq if totals.last_seq is not None else -1
    totals.add(conn.execute(_SNAPSHOTS_AFTER, (run_id, after)))
    if totals.last_seq is None:
        return {}
    m = totals.result()
    with conn:
        conn.execute(
            """INSERT INTO run_summary (run_id, distance_m, energy_wh, efficiency_mi_per_kwh, duration_s, avg_speed)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(run_id) DO UPDATE SET distance_m = excluded.distance_m, energy_wh = excluded.energy_wh,
                   efficiency_mi_per_kwh = excluded.efficiency_mi_per_kwh, duration_s = excluded.duration_s,
                   avg_speed = excluded.avg_speed""",
            (run_id, m["distance_m"], m["energy_wh"], m["efficiency_mi_per_kwh"], m["duration_s"], m["avg_speed"]),
        )
    return m
