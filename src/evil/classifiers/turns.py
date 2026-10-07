"""Example classifier proving the registry/runner pattern end to end: segments
GPS track into turn instances using a circular geofence per track_geometry
row. Deliberately simple (circle, not polygon) -- swap the geometry test in
_inside() if a real track map needs sharper boundaries; nothing else in the
runner or cursor machinery needs to change to do that.
"""

from __future__ import annotations

import sqlite3

from evil.classifiers.base import Classifier, ClassifierSpec
from evil.geo import haversine_m


def _inside(lat: float, lon: float, turn_def: sqlite3.Row) -> bool:
    return haversine_m(lat, lon, turn_def["center_lat"], turn_def["center_lon"]) <= turn_def["radius_m"]


class TurnsClassifier:
    spec = ClassifierSpec(
        name="turns",
        version=1,
        depends_on=["main_snapshot"],
        lookback_margin_s=2.0,
    )

    def run(self, conn: sqlite3.Connection, run_id: str, since_seq: int, until_seq: int) -> None:
        turn_defs = conn.execute("SELECT * FROM track_geometry").fetchall()
        if not turn_defs:
            return

        rows = conn.execute(
            """SELECT ms.seq, ms.global_ts, g.lat, g.lon, g.speed
               FROM main_snapshot ms JOIN gps g ON g.id = ms.gps_id
               WHERE ms.run_id = ? AND ms.seq > ? AND ms.seq <= ?
                 AND g.lat IS NOT NULL AND g.lon IS NOT NULL   -- ticks with no GPS fix are skipped
               ORDER BY ms.seq""",
            (run_id, since_seq, until_seq),
        ).fetchall()

        for row in rows:
            for turn_def in turn_defs:
                inside = _inside(row["lat"], row["lon"], turn_def)
                open_state = conn.execute(
                    "SELECT * FROM turns_open_state WHERE run_id = ? AND turn_def_id = ?",
                    (run_id, turn_def["turn_def_id"]),
                ).fetchone()

                if inside and open_state is None:
                    conn.execute(
                        """INSERT INTO turns_open_state
                               (run_id, turn_def_id, start_seq, start_ts, entry_speed)
                           VALUES (?, ?, ?, ?, ?)""",
                        (run_id, turn_def["turn_def_id"], row["seq"], row["global_ts"], row["speed"]),
                    )
                elif not inside and open_state is not None:
                    conn.execute(
                        """INSERT INTO turns
                               (run_id, turn_def_id, start_seq, end_seq, start_ts, end_ts,
                                entry_speed, exit_speed)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            run_id,
                            turn_def["turn_def_id"],
                            open_state["start_seq"],
                            row["seq"],
                            open_state["start_ts"],
                            row["global_ts"],
                            open_state["entry_speed"],
                            row["speed"],
                        ),
                    )
                    conn.execute(
                        "DELETE FROM turns_open_state WHERE run_id = ? AND turn_def_id = ?",
                        (run_id, turn_def["turn_def_id"]),
                    )
