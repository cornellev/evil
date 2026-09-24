"""Second real classifier, after turns.py -- and the first one that actually
depends on another classifier's output rather than a test double: laps
counts turns crossed and needs turns' rows to already be committed. The
runner's topological ordering (classifiers/registry.py) guarantees that
within one tick(), turns runs and commits before laps does, so this join is
always safe, never a race against a partially-written tick.

Boundary detection mirrors turns.py's circle-geofence state machine, but
crossing INTO the line is the event of interest (not dwelling inside it):
a crossing both closes the currently-open lap and starts the next one.
"""

from __future__ import annotations

import sqlite3

from evil.classifiers import metrics
from evil.classifiers.base import Classifier, ClassifierSpec
from evil.geo import haversine_m


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
    conn.execute(
        """INSERT INTO laps
               (run_id, lap_number, start_seq, end_seq, start_ts, end_ts,
                turn_count, energy_wh, avg_speed)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            lap_number,
            start_seq,
            end_seq,
            start_ts,
            end_ts,
            metrics.turn_count(conn, run_id, start_seq, end_seq),
            metrics.energy_wh(conn, run_id, start_seq, end_seq),
            metrics.avg_speed(conn, run_id, start_seq, end_seq),
        ),
    )


class LapsClassifier:
    spec = ClassifierSpec(
        name="laps",
        version=1,
        depends_on=["main_snapshot", "turns"],
        lookback_margin_s=2.0,
    )

    def run(self, conn: sqlite3.Connection, run_id: str, since_seq: int, until_seq: int) -> None:
        lines = conn.execute("SELECT * FROM start_finish_line").fetchall()
        if not lines:
            return

        rows = conn.execute(
            """SELECT ms.seq, ms.global_ts, g.lat, g.lon
               FROM main_snapshot ms JOIN gps g ON g.id = ms.gps_id
               WHERE ms.run_id = ? AND ms.seq > ? AND ms.seq <= ?
               ORDER BY ms.seq""",
            (run_id, since_seq, until_seq),
        ).fetchall()

        for row in rows:
            for line in lines:
                inside = _inside(row["lat"], row["lon"], line)
                state = conn.execute(
                    "SELECT * FROM laps_open_state WHERE run_id = ? AND line_id = ?",
                    (run_id, line["line_id"]),
                ).fetchone()

                if state is None:
                    conn.execute(
                        """INSERT INTO laps_open_state
                               (run_id, line_id, lap_number, start_seq, start_ts, currently_inside)
                           VALUES (?, ?, 1, ?, ?, ?)""",
                        (run_id, line["line_id"], row["seq"], row["global_ts"], int(inside)),
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
