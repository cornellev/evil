"""list_runs: the browsing-shaped counterpart to get_turn's narrow lookup.
Needed for evil-ui's data browser -- a UI needs to enumerate what runs exist
before a user can pick one, which none of the LLM-shaped tools support."""

from __future__ import annotations

import sqlite3
from typing import Any


def list_runs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT run_id, COUNT(*) AS sample_count, MIN(global_ts) AS start_ts, MAX(global_ts) AS end_ts
           FROM main_snapshot
           GROUP BY run_id
           ORDER BY start_ts DESC"""
    ).fetchall()
    return [dict(row) for row in rows]
