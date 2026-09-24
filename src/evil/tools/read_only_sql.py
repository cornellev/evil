"""The fallback escape hatch for questions the fixed tools don't cover.
Constrained on purpose, per inference-agent/4.md finding 8: this runs against
a mode=ro connection (can't write no matter what), rejects anything but a
single SELECT (a leading WITH is allowed, for read-only CTEs), and aborts
via SQLite's progress handler if a query reads an
unreasonable number of VM steps, so one bad query from the harness can't
hang or degrade a shared edge device.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

_SELECT_ONLY = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_DISALLOWED = re.compile(r"\b(insert|update|delete|drop|alter|attach|pragma|vacuum)\b", re.IGNORECASE)


def read_only_sql(
    conn: sqlite3.Connection, query: str, row_limit: int = 200, max_steps: int = 1_000_000
) -> list[dict[str, Any]]:
    statement = query.strip().rstrip(";")
    if ";" in statement:
        raise ValueError("read_only_sql allows exactly one statement")
    if not _SELECT_ONLY.match(statement):
        raise ValueError("read_only_sql only allows SELECT statements")
    if _DISALLOWED.search(statement):
        raise ValueError("read_only_sql rejected a disallowed keyword")

    steps = 0

    def _step_guard() -> int:
        nonlocal steps
        steps += 1
        return 1 if steps > max_steps else 0

    conn.set_progress_handler(_step_guard, 1000)
    try:
        cursor = conn.execute(statement)
        rows = cursor.fetchmany(row_limit)
    except sqlite3.OperationalError as exc:
        if "interrupted" in str(exc).lower():
            raise ValueError("read_only_sql query exceeded its step budget") from exc
        raise
    finally:
        conn.set_progress_handler(None, 0)

    return [dict(row) for row in rows]
