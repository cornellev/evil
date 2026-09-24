"""list_laps: paginated laps for a run, for evil-ui's browsing table."""

from __future__ import annotations

import sqlite3
from typing import Any


def list_laps(conn: sqlite3.Connection, run_id: str, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM laps WHERE run_id = ?", (run_id,)
    ).fetchone()["n"]

    rows = conn.execute(
        """SELECT * FROM laps
           WHERE run_id = ?
           ORDER BY lap_number
           LIMIT ? OFFSET ?""",
        (run_id, limit, offset),
    ).fetchall()

    return {"total": total, "laps": [dict(row) for row in rows]}
