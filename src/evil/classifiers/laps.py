"""Laps: depends on the `segments` classifier's output, since a lap counts the
turns driven in it and needs those rows committed first. The runner's topological
ordering (classifiers/registry.py) guarantees that within one tick(), segments runs
and commits before laps does.

Boundary detection is a circle around the start/finish line: crossing INTO it is the
event of interest (not dwelling inside it): a crossing both closes the currently-open
lap and starts the next one. Per-lap energy, distance and efficiency use RED's formulas
(metrics.py).
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
            """SELECT ms.seq, ms.global_ts, g.lat, g.lon
               FROM main_snapshot ms JOIN gps g ON g.id = ms.gps_id
               WHERE ms.run_id = ? AND ms.seq > ? AND ms.seq <= ?
                 AND g.lat IS NOT NULL AND g.lon IS NOT NULL   -- ticks with no GPS fix are skipped
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
