"""list_turns: paginated turns for a run, for evil-ui's browsing table.
Distinct from get_turn (which requires already knowing a turn name) --
this is "show me everything," the shape a UI table needs.
"""

from __future__ import annotations

import sqlite3
from typing import Any


def list_turns(conn: sqlite3.Connection, run_id: str, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM turns WHERE run_id = ?", (run_id,)
    ).fetchone()["n"]

    rows = conn.execute(
        """SELECT t.*, g.turn_name FROM turns t
           JOIN track_geometry g ON g.turn_def_id = t.turn_def_id
           WHERE t.run_id = ?
           ORDER BY t.start_seq
           LIMIT ? OFFSET ?""",
        (run_id, limit, offset),
    ).fetchall()

    return {"total": total, "turns": [dict(row) for row in rows]}
