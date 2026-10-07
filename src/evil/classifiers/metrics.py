"""Shared metrics over a (start_seq, end_seq] raw-row range, used by any
classifier that closes a segment (laps.py, straights.py) -- factored out
once a second consumer needed the identical energy/avg-speed computation
rather than duplicated per classifier.
"""

from __future__ import annotations

import sqlite3


def energy_wh(conn: sqlite3.Connection, run_id: str, start_seq: int, end_seq: int) -> float | None:
    """Trapezoidal integration of voltage*current over time. None if fewer
    than 2 joulemeter rows fall in range -- not enough to integrate."""
    power_rows = conn.execute(
        """SELECT ms.global_ts AS ts, j.voltage, j.current
           FROM main_snapshot ms JOIN joulemeter j ON j.id = ms.joulemeter_id
           WHERE ms.run_id = ? AND ms.seq > ? AND ms.seq <= ?
             AND j.voltage IS NOT NULL AND j.current IS NOT NULL
           ORDER BY ms.seq""",
        (run_id, start_seq, end_seq),
    ).fetchall()
    if len(power_rows) < 2:
        return None

    energy_j = 0.0
    for prev, curr in zip(power_rows, power_rows[1:]):
        p1 = prev["voltage"] * prev["current"]
        p2 = curr["voltage"] * curr["current"]
        dt = curr["ts"] - prev["ts"]
        energy_j += (p1 + p2) / 2.0 * dt
    return energy_j / 3600.0


def avg_speed(conn: sqlite3.Connection, run_id: str, start_seq: int, end_seq: int) -> float | None:
    speed_rows = conn.execute(
        """SELECT g.speed FROM main_snapshot ms JOIN gps g ON g.id = ms.gps_id
           WHERE ms.run_id = ? AND ms.seq > ? AND ms.seq <= ? AND g.speed IS NOT NULL""",
        (run_id, start_seq, end_seq),
    ).fetchall()
    if not speed_rows:
        return None
    return sum(r["speed"] for r in speed_rows) / len(speed_rows)


def turn_count(conn: sqlite3.Connection, run_id: str, start_seq: int, end_seq: int) -> int:
    return conn.execute(
        """SELECT COUNT(*) AS n FROM turns
           WHERE run_id = ? AND start_seq > ? AND start_seq <= ?""",
        (run_id, start_seq, end_seq),
    ).fetchone()["n"]
