"""Shared by get_turn and compare_turn_instances: resolves a possibly-loose
turn name to the exact stored track_geometry.turn_name.

Added after real e2e testing against an actual model (gemma4:e4b, not
FakeLLMClient) surfaced this directly: asked "how was turn 3", the model
called get_turn with turn_name="3", not the stored "Turn 3" -- confirmed by
inspecting the actual tool_call arguments, not assumed. A fake-LLM test
would never have caught this, since scripted tool calls always used the
exact stored string. See inference-agent/4.md's plan doc, Phase 5.
"""

from __future__ import annotations

import re
import sqlite3


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value)


def resolve_turn_name(conn: sqlite3.Connection, turn_name: str) -> str | None:
    """Exact match first; falls back to comparing digits only, so "3" also
    matches "Turn 3" without confusing "1" with "Turn 10"/"Turn 11" (their
    digit strings, "1" vs "10"/"11", stay distinct). Returns None if nothing
    matches either way.
    """
    exact = conn.execute(
        "SELECT turn_name FROM track_geometry WHERE turn_name = ?", (turn_name,)
    ).fetchone()
    if exact is not None:
        return exact["turn_name"]

    query_digits = _digits(turn_name)
    if not query_digits:
        return None

    for row in conn.execute("SELECT turn_name FROM track_geometry"):
        if _digits(row["turn_name"]) == query_digits:
            return row["turn_name"]
    return None
