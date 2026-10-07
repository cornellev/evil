"""get_straight: the straight counterpart of get_turn ("how was I down the back straight"). Straights are
named, quantified segments like turns: duration, distance, energy and efficiency (mi/kWh)."""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.segment_tools import get_segment


def get_straight(conn: sqlite3.Connection, run_id: str, straight_name: str, occurrence: str = "latest") -> dict[str, Any]:
    return get_segment(conn, "straight", run_id, straight_name, occurrence)
