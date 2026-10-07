"""Splits a run into turn and straight instances by watching the track's gates in lap order.

Replaces the circle-geofence `turns` classifier and the gap-between-turns `straights` classifier
(see track.py for why). Each GPS reading is compared with the previous distinct one; when that step crosses
the next gate (or the one after it, to survive a missed reading) a boundary is recorded at the crossing time,
interpolated between the two readings on the DAQ clock. A segment instance runs from one boundary to the next,
so instances tile the run exactly.

Per instance it stores time, distance, energy, efficiency (RED's formulas, metrics.py) and entry/exit speed.
Energy and distance are cut at the interpolated boundaries, so the segments of a lap add up to the lap.

State (which segment the car is in and where it entered) lives in `segments_open_state`, written in the same
transaction as the instances, so the runner's incremental ticks and crash replay behave like the other classifiers.
A GPS dropout (a step longer than 5 s or 120 m) drops the open segment: the next gate crossing re-syncs.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from evil import track
from evil.classifiers import metrics
from evil.classifiers.base import ClassifierSpec

DROPOUT_S = 5.0
DROPOUT_M = 120.0
SKIP_TOLERANCE = 1           # accept the gate after the next one if a reading was lost
SEEK_WINDOW = 3000           # snapshots to look back through when locating a boundary row


@dataclass
class Boundary:
    t: float                 # crossing time, DAQ clock (s)
    seq: int                 # snapshot where the reading after the crossing first appeared
    f: float                 # fraction along the chord at which the gate was crossed
    a: tuple[float, float, float]   # (time, lat, lon) of the reading before
    b: tuple[float, float, float]   # ... and after


class SegmentsClassifier:
    spec = ClassifierSpec(name="segments", version=1, depends_on=["main_snapshot"], lookback_margin_s=2.0)

    def run(self, conn: sqlite3.Connection, run_id: str, since_seq: int, until_seq: int) -> None:
        segments = track.load_segments(conn)
        if len(segments) < 2:
            return
        state = _load_state(conn, run_id)
        rows = conn.execute(metrics._SNAPSHOTS, (run_id, since_seq + 1, until_seq)).fetchall()
        n = len(segments)
        last_key = state["last_key"]
        last_fix = state["last_fix"]            # (t, lat, lon) or None
        current = state["current"]              # (ordinal, Boundary) or None

        for row in rows:
            if not metrics.valid_fix(row["lat"], row["lon"]):
                continue
            key = (row["gps_us"], row["lat"], row["lon"]) if row["gps_us"] is not None else (row["lat"], row["lon"])
            if key == last_key:
                continue
            last_key = key
            fix = (metrics.fix_time(row), row["lat"], row["lon"])
            prev, last_fix = last_fix, fix
            if prev is None:
                continue
            if fix[0] - prev[0] > DROPOUT_S or metrics.tangent_distance_m(prev[1], prev[2], fix[1], fix[2]) > DROPOUT_M:
                current = None                  # dropout: forget the open segment, re-sync on the next crossing
                continue

            candidates = range(n) if current is None else [(current[0] + 1 + j) % n for j in range(SKIP_TOLERANCE + 1)]
            for ordinal in candidates:
                f = track.crossing_fraction((prev[1], prev[2]), (fix[1], fix[2]), segments[ordinal].gate)
                if f is None:
                    continue
                boundary = Boundary(prev[0] + f * (fix[0] - prev[0]), row["seq"], f, prev, fix)
                if current is not None and ordinal == (current[0] + 1) % n:
                    _store(conn, run_id, segments[current[0]], current[1], boundary)
                # a skipped gate leaves the in-between segment unrecorded rather than guessed
                current = (ordinal, boundary)
                break

        _save_state(conn, run_id, last_key, last_fix, current)


# ---- instance metrics -----------------------------------------------------------

def _first_seq_at_or_after(conn: sqlite3.Connection, run_id: str, t: float, around_seq: int) -> int:
    """Smallest snapshot seq whose time is >= t, searching back from the snapshot where the crossing was detected."""
    rows = conn.execute(
        """SELECT ms.seq, ms.global_ts, ms.device_global_ts_us AS dev_us FROM main_snapshot ms
           WHERE ms.run_id = ? AND ms.seq <= ? AND ms.seq > ? ORDER BY ms.seq DESC""",
        (run_id, around_seq, around_seq - SEEK_WINDOW),
    ).fetchall()
    best = around_seq
    for r in rows:
        if metrics.snapshot_time(r) >= t:
            best = r["seq"]
        else:
            break
    return best


def _store(conn: sqlite3.Connection, run_id: str, seg: track.Segment, enter: Boundary, exit_: Boundary) -> None:
    start_seq = _first_seq_at_or_after(conn, run_id, enter.t, enter.seq)
    end_seq = _first_seq_at_or_after(conn, run_id, exit_.t, exit_.seq)
    if end_seq < start_seq:
        return
    rows = conn.execute(metrics._SNAPSHOTS, (run_id, max(start_seq - 1, 0), end_seq)).fetchall()
    samples = []
    for r in rows:
        p = metrics.power_kw(r["current"], r["voltage"])
        if p is not None:
            samples.append((metrics.snapshot_time(r), p))
    energy_wh = metrics.energy_kwh(samples, enter.t, exit_.t) * 1000.0 if len(samples) >= 2 else None

    distance = _segment_distance(conn, run_id, enter, exit_)
    duration = exit_.t - enter.t
    by_seq = {r["seq"]: r for r in rows}
    first, last = by_seq.get(start_seq), by_seq.get(end_seq)
    entry_speed = _speed(first)
    exit_speed = _speed(last)
    efficiency = metrics.efficiency_mi_per_kwh(distance, energy_wh)
    avg_speed = (distance / duration) if distance is not None and duration > 0 else None
    start_ts = first["global_ts"] if first is not None else enter.t
    end_ts = last["global_ts"] if last is not None else exit_.t

    if seg.kind == "turn":
        conn.execute(
            """INSERT OR REPLACE INTO turns
                   (run_id, turn_def_id, start_seq, end_seq, start_ts, end_ts, entry_speed, exit_speed,
                    duration_s, distance_m, energy_wh, efficiency_mi_per_kwh, avg_speed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, seg.segment_id, start_seq, end_seq, start_ts, end_ts, entry_speed, exit_speed,
             duration, distance, energy_wh, efficiency, avg_speed),
        )
    else:
        conn.execute(
            """INSERT OR REPLACE INTO straights
                   (run_id, segment_id, start_seq, end_seq, start_ts, end_ts, entry_speed, exit_speed,
                    avg_speed, energy_wh, duration_s, distance_m, efficiency_mi_per_kwh)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, seg.segment_id, start_seq, end_seq, start_ts, end_ts, entry_speed, exit_speed,
             avg_speed, energy_wh, duration, distance, efficiency),
        )


def _speed(row: sqlite3.Row | None) -> float | None:
    if row is None:
        return None
    return row["filtered_speed"] if row["filtered_speed"] is not None else row["gps_speed"]


def _segment_distance(conn: sqlite3.Connection, run_id: str, enter: Boundary, exit_: Boundary) -> float | None:
    """RED's distance, cut at the two crossings: the part of the entry chord after the gate, every full chord
    between, and the part of the exit chord before the gate."""
    def chord(p: tuple[float, float, float], q: tuple[float, float, float]) -> float:
        d = metrics.tangent_distance_m(p[1], p[2], q[1], q[2])
        return d if d <= metrics.MAX_JUMP_M else 0.0

    total = (1.0 - enter.f) * chord(enter.a, enter.b)
    if exit_.seq > enter.seq:
        rows = conn.execute(metrics._SNAPSHOTS, (run_id, enter.seq, exit_.seq - 1)).fetchall()
        fixes = metrics.distinct_fixes(rows)
        # the first distinct fix here is enter.b (it first appears at enter.seq); chain through to exit_.a
        total += metrics.path_distance_m(fixes)
        last = fixes[-1] if fixes else enter.b
        # distinct_fixes is built from the rows only; join the exit chord from the last fix before the gate
        total += exit_.f * chord(last, exit_.b)
    else:
        total += exit_.f * chord(enter.b, exit_.b)
    return total


# ---- state -----------------------------------------------------------------------------

def _load_state(conn: sqlite3.Connection, run_id: str) -> dict:
    r = conn.execute("SELECT * FROM segments_open_state WHERE run_id = ?", (run_id,)).fetchone()
    if r is None:
        return {"last_key": None, "last_fix": None, "current": None}
    last_key = None
    if r["last_key_lat"] is not None:
        last_key = ((r["last_key_gps_us"], r["last_key_lat"], r["last_key_lon"]) if r["last_key_gps_us"] is not None
                    else (r["last_key_lat"], r["last_key_lon"]))
    last_fix = (r["last_fix_t"], r["last_fix_lat"], r["last_fix_lon"]) if r["last_fix_t"] is not None else None
    current = None
    if r["ordinal"] is not None:
        current = (r["ordinal"], Boundary(r["enter_t"], r["enter_seq"], r["enter_f"],
                                          (r["enter_a_t"], r["enter_a_lat"], r["enter_a_lon"]),
                                          (r["enter_b_t"], r["enter_b_lat"], r["enter_b_lon"])))
    return {"last_key": last_key, "last_fix": last_fix, "current": current}


def _save_state(conn, run_id, last_key, last_fix, current) -> None:
    if last_key is None:
        key = (None, None, None)
    elif len(last_key) == 3:
        key = last_key
    else:
        key = (None, last_key[0], last_key[1])
    ordinal, b = (current[0], current[1]) if current else (None, None)
    conn.execute(
        """INSERT OR REPLACE INTO segments_open_state
               (run_id, last_key_gps_us, last_key_lat, last_key_lon, last_fix_t, last_fix_lat, last_fix_lon,
                ordinal, enter_t, enter_seq, enter_f,
                enter_a_t, enter_a_lat, enter_a_lon, enter_b_t, enter_b_lat, enter_b_lon)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (run_id, *key, *(last_fix or (None, None, None)), ordinal,
         b.t if b else None, b.seq if b else None, b.f if b else None,
         *(b.a if b else (None, None, None)), *(b.b if b else (None, None, None))),
    )
