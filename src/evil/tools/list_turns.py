"""list_turns: paginated turns for a run, for evil-ui's browsing table. Distinct from get_turn (which requires
already knowing a turn name) -- this is "show me everything," the shape a UI table needs."""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.segment_tools import list_segment_instances


def list_turns(conn: sqlite3.Connection, run_id: str, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    return list_segment_instances(conn, "turn", run_id, limit, offset)
