"""compare_turn_instances: answers the broader "what could I have done better" question for a given
turn, by comparing every instance of it in a run rather than just the most recent one. Reports speed, time,
energy and efficiency (mi/kWh) per instance and the best instance by exit speed and by efficiency."""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.segment_tools import compare_instances


def compare_turn_instances(conn: sqlite3.Connection, run_id: str, turn_name: str, limit: int = 10) -> dict[str, Any]:
    return compare_instances(conn, "turn", run_id, turn_name, limit)
