"""The write and read halves of nas_index: the bridge from a derived row's
time range to the raw autonomy file (bag/video/lidar) it came from on NAS --
the SEGMENTS -.-> NASIDX link in inference-agent/4.md's diagram, schema-only
until now.

register_nas_file is deliberately NOT an MCP tool: MCP is EVIL's read
interface (every tool in mcp_server.py runs on a read-only connection), and
registering a file is a write -- same reasoning that keeps bulk ingestion
(scripts/ingest_recording.py) as a CLI, not a tool call. find_nas_files is
the read half and is exposed over MCP, in tools/registry.py.

There's no real NAS/bag service to integrate against here (same limitation
as Ros2Source/rclpy) -- this is the write path a human or a future watcher
on tailscale-ros-telemetry's bag service would call once a recording
finishes, made code-ready and tested, not connected to real hardware.
"""

from __future__ import annotations

import sqlite3
from typing import Any


def register_nas_file(
    conn: sqlite3.Connection, run_id: str, path: str, kind: str, start_ts: float, end_ts: float
) -> int:
    cursor = conn.execute(
        "INSERT INTO nas_index (run_id, path, kind, start_ts, end_ts) VALUES (?, ?, ?, ?, ?)",
        (run_id, path, kind, start_ts, end_ts),
    )
    conn.commit()
    return cursor.lastrowid


def find_nas_files(
    conn: sqlite3.Connection,
    run_id: str,
    start_ts: float | None = None,
    end_ts: float | None = None,
) -> list[dict[str, Any]]:
    """All files for a run, or (if start_ts/end_ts given) only those whose
    recorded range overlaps the query range -- e.g. "what recording covers
    this turn" using the turn's own start_ts/end_ts."""
    if start_ts is None or end_ts is None:
        rows = conn.execute(
            "SELECT * FROM nas_index WHERE run_id = ? ORDER BY start_ts", (run_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT * FROM nas_index
               WHERE run_id = ? AND start_ts <= ? AND end_ts >= ?
               ORDER BY start_ts""",
            (run_id, end_ts, start_ts),
        ).fetchall()
    return [dict(row) for row in rows]
