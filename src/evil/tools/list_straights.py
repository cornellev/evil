"""list_straights: paginated straights for a run, for evil-ui's browsing table. Straights are named segments
like turns, with duration, distance, energy and efficiency."""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.segment_tools import list_segment_instances


def list_straights(conn: sqlite3.Connection, run_id: str, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    return list_segment_instances(conn, "straight", run_id, limit, offset)
