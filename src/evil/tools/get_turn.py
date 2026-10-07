"""get_turn: answers "how was I in turn X" -- the concrete case from the original design discussion.
Also returns the turn's duration, distance, energy and efficiency (mi/kWh)."""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.segment_tools import get_segment


def get_turn(conn: sqlite3.Connection, run_id: str, turn_name: str, occurrence: str = "latest") -> dict[str, Any]:
    return get_segment(conn, "turn", run_id, turn_name, occurrence)
