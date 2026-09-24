"""compare_laps: named T4 in inference-agent/4.md's original diagram, never
built until laps existed to compare. Also covers the diagram's vaguer T3
(get_efficiency_delta(a, b)) -- one tool, not two overlapping ones for an
ambiguously-specified second concept, since laps' energy_wh/avg_speed are
exactly the efficiency metrics that comparison would need anyway.
"""

from __future__ import annotations

import sqlite3
from typing import Any


def _lap_row(conn: sqlite3.Connection, run_id: str, lap_number: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM laps WHERE run_id = ? AND lap_number = ?", (run_id, lap_number)
    ).fetchone()


def _delta(a: float | None, b: float | None) -> float | None:
    return (b - a) if a is not None and b is not None else None


def compare_laps(conn: sqlite3.Connection, run_id: str, lap_a: int, lap_b: int) -> dict[str, Any]:
    row_a = _lap_row(conn, run_id, lap_a)
    row_b = _lap_row(conn, run_id, lap_b)

    if row_a is None or row_b is None:
        return {"found": False}

    def _summarize(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "lap_number": row["lap_number"],
            "duration_s": row["end_ts"] - row["start_ts"],
            "turn_count": row["turn_count"],
            "energy_wh": row["energy_wh"],
            "avg_speed": row["avg_speed"],
        }

    summary_a, summary_b = _summarize(row_a), _summarize(row_b)

    return {
        "found": True,
        "lap_a": summary_a,
        "lap_b": summary_b,
        "delta": {
            "duration_s": summary_b["duration_s"] - summary_a["duration_s"],
            "energy_wh": _delta(summary_a["energy_wh"], summary_b["energy_wh"]),
            "avg_speed": _delta(summary_a["avg_speed"], summary_b["avg_speed"]),
        },
    }
