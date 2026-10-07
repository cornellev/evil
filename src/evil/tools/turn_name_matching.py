"""Resolves a loosely-worded turn or straight name to the exact stored segment name.

Added after real testing against an actual model (gemma4:e4b) showed that asked "how was turn 3" it
called get_turn with turn_name="3", not the stored "Turn 3". Matching is by official turn NUMBER, not by the
digits of the name, because a segment can cover several official turns (T1 and T2 are one continuous S-bend,
stored as "Turn 1-2" with aliases "1,2") and "Turn 1-2" must never be confused with "Turn 12".
"""

from __future__ import annotations

import re
import sqlite3


def _numbers(value: str) -> tuple[str, ...]:
    return tuple(re.findall(r"\d+", value))


def resolve_segment_name(conn: sqlite3.Connection, text: str, kind: str) -> str | None:
    """Exact name (any case) first. Then, for a turn, the segment whose aliases include the number in `text`
    ("3" -> "Turn 3"; "2" -> "Turn 1-2"). For a straight, the segment whose name carries the same numbers
    ("6 7", "6-7", "straight 6-7" -> "Straight 6-7")."""
    text = (text or "").strip()
    if not text:
        return None
    row = conn.execute(
        "SELECT name FROM track_segments WHERE kind = ? AND lower(name) = lower(?)", (kind, text)
    ).fetchone()
    if row is not None:
        return row["name"]

    wanted = _numbers(text)
    if not wanted:
        return None
    rows = conn.execute("SELECT name, aliases FROM track_segments WHERE kind = ? ORDER BY ordinal", (kind,)).fetchall()
    if kind == "turn":
        if len(wanted) != 1:
            return None
        for r in rows:
            if wanted[0] in [a for a in (r["aliases"] or "").split(",") if a]:
                return r["name"]
        return None
    for r in rows:
        if _numbers(r["name"]) == wanted:
            return r["name"]
    return None


def resolve_turn_name(conn: sqlite3.Connection, turn_name: str) -> str | None:
    return resolve_segment_name(conn, turn_name, "turn")


def resolve_straight_name(conn: sqlite3.Connection, name: str) -> str | None:
    return resolve_segment_name(conn, name, "straight")
