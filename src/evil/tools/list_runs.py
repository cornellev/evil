"""list_runs: the browsing-shaped counterpart to get_turn's narrow lookup.
Needed for evil-ui's data browser -- a UI needs to enumerate what runs exist
before a user can pick one, which none of the LLM-shaped tools support.

Each run also carries its whole-run totals once classified (run_summary): distance, energy and
efficiency in mi/kWh (distance / energy, the Race Engineer Dashboard's "average efficiency")."""

from __future__ import annotations

import sqlite3
from typing import Any


def list_runs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT m.run_id, COUNT(*) AS sample_count, MIN(m.global_ts) AS start_ts, MAX(m.global_ts) AS end_ts,
                  s.distance_m, s.energy_wh, s.efficiency_mi_per_kwh
           FROM main_snapshot m LEFT JOIN run_summary s ON s.run_id = m.run_id
           GROUP BY m.run_id
           ORDER BY start_ts DESC"""
    ).fetchall()
    return [dict(row) for row in rows]
