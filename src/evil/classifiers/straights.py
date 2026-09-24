"""straights: the complement of turns. Depends on turns for the same reason
laps does (see laps.py) -- the runner's topological ordering guarantees
turns commits before straights runs.

Unlike turns/laps, a straight has no "open" state to persist across ticks:
it's only ever finalized retroactively, once BOTH boundary turns already
exist. So the incremental unit here is "a new turn appeared" (found via the
normal since_seq/until_seq window on turns' own start_seq), not "a raw GPS
row arrived" -- for each newly-visible turn, look up whatever turn
immediately precedes it (regardless of how long ago that one was created)
and, if one exists, that gap is a new straight.
"""

from __future__ import annotations

import sqlite3

from evil.classifiers import metrics
from evil.classifiers.base import Classifier, ClassifierSpec


def _ts_at_seq(conn: sqlite3.Connection, run_id: str, seq: int) -> float:
    row = conn.execute(
        "SELECT global_ts FROM main_snapshot WHERE run_id = ? AND seq = ?", (run_id, seq)
    ).fetchone()
    return row["global_ts"]


class StraightsClassifier:
    spec = ClassifierSpec(
        name="straights",
        version=1,
        depends_on=["main_snapshot", "turns"],
        lookback_margin_s=2.0,
    )

    def run(self, conn: sqlite3.Connection, run_id: str, since_seq: int, until_seq: int) -> None:
        new_turns = conn.execute(
            """SELECT * FROM turns WHERE run_id = ? AND start_seq > ? AND start_seq <= ?
               ORDER BY start_seq""",
            (run_id, since_seq, until_seq),
        ).fetchall()

        for next_turn in new_turns:
            prev_turn = conn.execute(
                """SELECT * FROM turns WHERE run_id = ? AND start_seq < ?
                   ORDER BY start_seq DESC LIMIT 1""",
                (run_id, next_turn["start_seq"]),
            ).fetchone()
            if prev_turn is None:
                continue  # first-ever turn in the run: nothing precedes it yet

            start_seq, end_seq = prev_turn["end_seq"], next_turn["start_seq"]
            conn.execute(
                """INSERT INTO straights
                       (run_id, start_seq, end_seq, start_ts, end_ts,
                        entry_speed, exit_speed, avg_speed, energy_wh)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    run_id,
                    start_seq,
                    end_seq,
                    _ts_at_seq(conn, run_id, start_seq),
                    _ts_at_seq(conn, run_id, end_seq),
                    prev_turn["exit_speed"],
                    next_turn["entry_speed"],
                    metrics.avg_speed(conn, run_id, start_seq, end_seq),
                    metrics.energy_wh(conn, run_id, start_seq, end_seq),
                ),
            )
