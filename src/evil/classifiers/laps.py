"""Laps: depends on the `segments` classifier's output, since a lap counts the
turns driven in it and needs those rows committed first. The runner's topological
ordering (classifiers/registry.py) guarantees that within one tick(), segments runs
and commits before laps does.

Boundary detection is a circle around the start/finish line: crossing INTO it is the
event of interest (not dwelling inside it): a crossing both closes the currently-open
lap and starts the next one.

GPS glitches: a fix that implies an impossible speed from the last accepted fix (over
MAX_PLAUSIBLE_MPS; the car does ~10 m/s) is ignored, so a one-second 200 m multipath jump
that happens to land in the circle cannot close a lap (found by mock-daq's gps-jumps
profile, 11-04). If the GPS really has moved (REANCHOR_AFTER consecutive rejected fixes
that agree with each other), the new position is accepted. The last accepted fix is kept
in laps_open_state so this holds across live ticks. Per-lap energy, distance and efficiency use RED's formulas
(metrics.py).
"""

from __future__ import annotations

import sqlite3

from evil.classifiers import metrics
from evil.classifiers.base import Classifier, ClassifierSpec
from evil.geo import haversine_m

MAX_PLAUSIBLE_MPS = 40.0
REANCHOR_AFTER = 3


def _inside(lat: float, lon: float, line: sqlite3.Row) -> bool:
    return haversine_m(lat, lon, line["center_lat"], line["center_lon"]) <= line["radius_m"]


def _close_lap(
    conn: sqlite3.Connection,
    run_id: str,
    lap_number: int,
    start_seq: int,
    start_ts: float,
    end_seq: int,
    end_ts: float,
) -> None:
    m = metrics.range_metrics(conn, run_id, start_seq, end_seq)
    conn.execute(
        """INSERT INTO laps
               (run_id, lap_number, start_seq, end_seq, start_ts, end_ts,
                turn_count, energy_wh, avg_speed, duration_s, distance_m, efficiency_mi_per_kwh)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            lap_number,
            start_seq,
            end_seq,
            start_ts,
            end_ts,
            metrics.turn_count(conn, run_id, start_seq, end_seq),
            m["energy_wh"],
            m["avg_speed"],
            m["duration_s"],
            m["distance_m"],
            m["efficiency_mi_per_kwh"],
        ),
    )


class LapsClassifier:
    spec = ClassifierSpec(
        name="laps",
        version=1,
        depends_on=["main_snapshot", "segments"],
        lookback_margin_s=2.0,
    )

    def run(self, conn: sqlite3.Connection, run_id: str, since_seq: int, until_seq: int) -> None:
        lines = conn.execute("SELECT * FROM start_finish_line").fetchall()
        if not lines:
            return

        rows = conn.execute(
            """SELECT ms.seq, ms.global_ts, ms.device_global_ts_us AS dev_us, g.lat, g.lon, g.device_ts_us AS gps_us
               FROM main_snapshot ms JOIN gps g ON g.id = ms.gps_id
               WHERE ms.run_id = ? AND ms.seq > ? AND ms.seq <= ?
                 AND g.lat IS NOT NULL AND g.lon IS NOT NULL   -- ticks with no GPS fix are skipped
               ORDER BY ms.seq""",
            (run_id, since_seq, until_seq),
        ).fetchall()

        for row in rows:
            for line in lines:
                state = conn.execute(
                    "SELECT * FROM laps_open_state WHERE run_id = ? AND line_id = ?",
                    (run_id, line["line_id"]),
                ).fetchone()
                if state is not None and not _plausible(conn, run_id, line["line_id"], state, row):
                    continue
                inside = _inside(row["lat"], row["lon"], line)

                if state is None:
                    conn.execute(
                        """INSERT INTO laps_open_state
                               (run_id, line_id, lap_number, start_seq, start_ts, currently_inside,
                                last_fix_t, last_fix_lat, last_fix_lon)
                           VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?)""",
                        (run_id, line["line_id"], row["seq"], row["global_ts"], int(inside),
                         metrics.fix_time(row), row["lat"], row["lon"]),
                    )
                    continue

                if inside and not state["currently_inside"]:
                    _close_lap(
                        conn,
                        run_id,
                        state["lap_number"],
                        state["start_seq"],
                        state["start_ts"],
                        row["seq"],
                        row["global_ts"],
                    )
                    conn.execute(
                        """UPDATE laps_open_state
                           SET lap_number = ?, start_seq = ?, start_ts = ?, currently_inside = 1
                           WHERE run_id = ? AND line_id = ?""",
                        (state["lap_number"] + 1, row["seq"], row["global_ts"], run_id, line["line_id"]),
                    )
                else:
                    conn.execute(
                        """UPDATE laps_open_state SET currently_inside = ?
                           WHERE run_id = ? AND line_id = ?""",
                        (int(inside), run_id, line["line_id"]),
                    )


def _plausible(conn: sqlite3.Connection, run_id: str, line_id: int, state: sqlite3.Row, row: sqlite3.Row) -> bool:
    """Accept this fix (and remember it) unless it implies an impossible speed from the last
    accepted one. Repeated snapshots of the same fix (dt 0, distance 0) are accepted."""
    t, lat, lon = metrics.fix_time(row), row["lat"], row["lon"]
    if state["last_fix_t"] is None:
        ok = True
    else:
        dt = t - state["last_fix_t"]
        d = haversine_m(state["last_fix_lat"], state["last_fix_lon"], lat, lon)
        ok = d <= 1.0 or (dt > 0 and d / dt <= MAX_PLAUSIBLE_MPS)
    if ok:
        conn.execute("UPDATE laps_open_state SET last_fix_t = ?, last_fix_lat = ?, last_fix_lon = ?, "
                     "reject_streak = 0 WHERE run_id = ? AND line_id = ?", (t, lat, lon, run_id, line_id))
        return True
    # A run of DISTINCT rejected fixes that agree with each other: the GPS really is somewhere
    # else now. The ~50 snapshots repeating one fix are one fix, not a streak.
    cand_t, cand_lat, cand_lon = state["reject_t"], state["reject_lat"], state["reject_lon"]
    if cand_t is not None and t == cand_t and haversine_m(cand_lat, cand_lon, lat, lon) <= 1.0:
        return False
    agrees = cand_t is not None and t > cand_t and \
        haversine_m(cand_lat, cand_lon, lat, lon) / (t - cand_t) <= MAX_PLAUSIBLE_MPS
    streak = (state["reject_streak"] or 0) + 1 if agrees else 1
    if streak >= REANCHOR_AFTER:
        conn.execute("UPDATE laps_open_state SET last_fix_t = ?, last_fix_lat = ?, last_fix_lon = ?, "
                     "reject_streak = 0, reject_t = NULL WHERE run_id = ? AND line_id = ?",
                     (t, lat, lon, run_id, line_id))
        return True
    conn.execute("UPDATE laps_open_state SET reject_streak = ?, reject_t = ?, reject_lat = ?, reject_lon = ? "
                 "WHERE run_id = ? AND line_id = ?", (streak, t, lat, lon, run_id, line_id))
    return False
