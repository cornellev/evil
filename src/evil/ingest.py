"""Writes a normalized RawSample into the raw tables in one transaction."""

from __future__ import annotations

import sqlite3

from evil.models import RawSample


def ingest_sample(conn: sqlite3.Connection, sample: RawSample) -> int:
    """Insert one tick's readings and the main_snapshot row that ties them
    together. Returns the assigned seq. Raw rows are immutable once written,
    so this is the only function in the package that writes to raw tables."""
    with conn:
        joulemeter_id = None
        if sample.joulemeter is not None:
            cur = conn.execute(
                "INSERT INTO joulemeter (ts, voltage, current) VALUES (?, ?, ?)",
                (sample.joulemeter.ts, sample.joulemeter.voltage, sample.joulemeter.current),
            )
            joulemeter_id = cur.lastrowid

        planner_id = None
        if sample.planner is not None:
            cur = conn.execute(
                "INSERT INTO local_planner (ts, planned_path, target_speed) VALUES (?, ?, ?)",
                (sample.planner.ts, sample.planner.planned_path, sample.planner.target_speed),
            )
            planner_id = cur.lastrowid

        gps_id = None
        if sample.gps is not None:
            cur = conn.execute(
                "INSERT INTO gps (ts, lat, lon, speed, heading) VALUES (?, ?, ?, ?, ?)",
                (sample.gps.ts, sample.gps.lat, sample.gps.lon, sample.gps.speed, sample.gps.heading),
            )
            gps_id = cur.lastrowid

        cur = conn.execute(
            """INSERT INTO main_snapshot (run_id, global_ts, joulemeter_id, planner_id, gps_id)
               VALUES (?, ?, ?, ?, ?)""",
            (sample.run_id, sample.ts, joulemeter_id, planner_id, gps_id),
        )
        return cur.lastrowid
