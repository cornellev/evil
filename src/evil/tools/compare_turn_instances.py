"""compare_turn_instances: answers the broader "what could I have done
better" question for a given turn, by comparing every instance of it in a
run rather than just the most recent one. Deliberately named "instances" not
"laps" -- a compare_laps tool grouping by lap_id (evil/src/evil/classifiers/laps.py
now exists) is a natural sibling function once there's a concrete need for
it, not built here.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.turn_name_matching import resolve_turn_name


def compare_turn_instances(
    conn: sqlite3.Connection, run_id: str, turn_name: str, limit: int = 10
) -> dict[str, Any]:
    resolved_name = resolve_turn_name(conn, turn_name)
    if resolved_name is None:
        return {"instances": [], "best_exit_speed_instance": None}

    rows = conn.execute(
        """SELECT t.*, g.turn_name FROM turns t
           JOIN track_geometry g ON g.turn_def_id = t.turn_def_id
           WHERE t.run_id = ? AND g.turn_name = ?
           ORDER BY t.start_seq ASC
           LIMIT ?""",
        (run_id, resolved_name, limit),
    ).fetchall()

    instances = [
        {
            "turn_id": r["turn_id"],
            "start_ts": r["start_ts"],
            "duration_s": r["end_ts"] - r["start_ts"],
            "entry_speed": r["entry_speed"],
            "exit_speed": r["exit_speed"],
        }
        for r in rows
    ]

    if not instances:
        return {"instances": [], "best_exit_speed_instance": None}

    with_exit_speed = [i for i in instances if i["exit_speed"] is not None]
    best = max(with_exit_speed, key=lambda i: i["exit_speed"]) if with_exit_speed else None

    return {"instances": instances, "best_exit_speed_instance": best}
