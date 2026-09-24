"""get_turn: answers "how was I in turn X" -- the concrete case from the
original design discussion."""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.turn_name_matching import resolve_turn_name


def get_turn(conn: sqlite3.Connection, run_id: str, turn_name: str, occurrence: str = "latest") -> dict[str, Any]:
    resolved_name = resolve_turn_name(conn, turn_name)
    if resolved_name is None:
        return {"found": False}

    order = "DESC" if occurrence == "latest" else "ASC"
    row = conn.execute(
        f"""SELECT t.*, g.turn_name FROM turns t
            JOIN track_geometry g ON g.turn_def_id = t.turn_def_id
            WHERE t.run_id = ? AND g.turn_name = ?
            ORDER BY t.start_seq {order}
            LIMIT 1""",
        (run_id, resolved_name),
    ).fetchone()

    if row is None:
        return {"found": False}

    entry, exit_ = row["entry_speed"], row["exit_speed"]
    return {
        "found": True,
        "turn_id": row["turn_id"],
        "turn_name": row["turn_name"],
        "start_ts": row["start_ts"],
        "end_ts": row["end_ts"],
        "duration_s": row["end_ts"] - row["start_ts"],
        "entry_speed": entry,
        "exit_speed": exit_,
        "speed_delta": (exit_ - entry) if entry is not None and exit_ is not None else None,
    }
