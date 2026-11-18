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


# Bookkeeping tables a question never needs (classifier cursors, open-segment state, heartbeats).
_INTERNAL = re.compile(r"^(sqlite_|classifier_cursor$|.*_open_state$|live_heartbeat$|schema_)")
SCHEMA_NOTES = (
    "Raw tables: main_snapshot is the time series (global_ts, filtered_speed m/s, *_id columns joining motor, gps, "
    "joulemeter, steering, rpm_front / rpm_back by id); turns use turn_def_id and straights segment_id, both naming "
    "track_segments.segment_id. Uploaded runs are in the main database, live runs in `live` (prefix live.)."
)


def schema_summary(conn: sqlite3.Connection) -> str:
    """The tables and columns a SELECT can use, read from the database itself (seen 11-18: the model guessed
    `telemetry` and `main_snapshot.speed`, neither exists, and gave up)."""
    lines = []
    schemas = [r[1] for r in conn.execute("PRAGMA database_list")]
    tables_by_schema: dict[str, list[str]] = {}
    for schema in schemas:
        if schema == "temp":
            continue
        names = [r[0] for r in conn.execute(
            f"SELECT name FROM {schema}.sqlite_master WHERE type = 'table' ORDER BY name") if not _INTERNAL.match(r[0])]
        tables_by_schema[schema] = names
        if schema != "main" and names == tables_by_schema.get("main"):
            lines.append(f"{schema}.*: the same tables as main.")
            continue
        for name in names:
            cols = [r[1] for r in conn.execute(f"PRAGMA {schema}.table_info({name})")]
            prefix = "" if schema == "main" else f"{schema}."
            lines.append(f"{prefix}{name}({', '.join(cols)})")
    return "\n".join(lines)


def explain_error(conn: sqlite3.Connection, exc: Exception) -> str:
    """The reason a query failed, for the model to fix it: SQLite's own message, plus the real names when it
    used a table or column that doesn't exist."""
    msg = str(exc)
    if "no such table" in msg or "no such column" in msg:
        try:
            msg += "\n" + SAMPLES_NOTES + "\nTables and columns:\n" + schema_summary(conn)
        except sqlite3.Error:
            pass
    return msg


# One ready-made view of the time series (user 11-18: "it should be able to oneshot the query"). The raw tables keep
# the payload's names (main_snapshot.global_ts beside sensor tables' ts; turns.turn_def_id beside
# straights.segment_id), and the model guessed the common name and ran out of steps (seen 11-18: "what segment of
# the lap" spent 5 steps on `ts` and `segment_id`). Here the guess is right: ts, speed, lap, segment.
TIMEZONE = "America/New_York"          # the team's local time (Ithaca, and IMS in Indianapolis)
SAMPLES_NOTES = (
    "Use the view `samples` (live runs: `live_samples`), one row per sample, already joined: run_id, run_date "
    "('2026-04-09', local), seq, ts (epoch s), time (local 'YYYY-MM-DD HH:MM:SS'), t (s since the run started), "
    "speed (m/s), speed_mph, throttle (v1 recordings), duty_cycle (v2), rpm, voltage (V), current (A), power (W), "
    "lat, lon, brake, steering, lap (lap number, NULL outside laps), segment (e.g. 'Turn 14', 'Straight 14-1', "
    "NULL between segments). Examples: when and where was the top speed: SELECT run_id, time, lap, segment, "
    "speed_mph FROM samples ORDER BY speed DESC LIMIT 1. Average power on lap 2 of the Apr 9 run: SELECT AVG(power) "
    "FROM samples WHERE run_date = '2026-04-09' AND lap = 2. Separate throttle presses: SELECT COUNT(*) FROM "
    "(SELECT throttle > 0 AS pressed, LAG(throttle > 0) OVER (ORDER BY seq) AS before FROM samples WHERE run_id = "
    "'<run>' AND throttle IS NOT NULL) WHERE pressed AND (before IS NULL OR NOT before) (leave out NULL readings: "
    "they split a press in two). Filter by run_id or run_date: a scan of every run takes seconds. Speeds: give mph. "
    "The raw tables below are for anything the view lacks."
)


def _local(fmt: str):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    zone = ZoneInfo(TIMEZONE)

    def convert(ts):
        return None if ts is None else datetime.fromtimestamp(ts, zone).strftime(fmt)
    return convert


def add_samples(conn: sqlite3.Connection) -> None:
    """The `samples` view (and `live_samples` when `live` is attached) on this connection only: TEMP views, so the
    read-only database is never touched."""
    conn.create_function("local_time", 1, _local("%Y-%m-%d %H:%M:%S"), deterministic=True)
    conn.create_function("local_date", 1, _local("%Y-%m-%d"), deterministic=True)
    schemas = [r[1] for r in conn.execute("PRAGMA database_list")]
    for schema, view in (("main", "samples"), ("live", "live_samples")):
        if schema not in schemas:
            continue
        s = schema
        conn.execute(f"""CREATE TEMP VIEW IF NOT EXISTS {view}_segments AS
            SELECT t.run_id, t.start_seq, t.end_seq, d.name FROM {s}.turns t
                JOIN {s}.track_segments d ON d.segment_id = t.turn_def_id
            UNION ALL
            SELECT x.run_id, x.start_seq, x.end_seq, d.name FROM {s}.straights x
                JOIN {s}.track_segments d ON d.segment_id = x.segment_id""")
        # A run's start from its own first sample: older uploads have no row in `runs` (seen 11-18, Apr 9).
        conn.execute(f"""CREATE TEMP VIEW IF NOT EXISTS {view}_starts AS
            SELECT run_id, MIN(global_ts) AS started FROM {s}.main_snapshot GROUP BY run_id""")
        conn.execute(f"""CREATE TEMP VIEW IF NOT EXISTS {view} AS
            SELECT m.run_id, local_date(r.started) AS run_date, m.seq, m.global_ts AS ts,
                   local_time(m.global_ts) AS time, m.global_ts - r.started AS t,
                   m.filtered_speed AS speed, m.filtered_speed * 2.23694 AS speed_mph,
                   mo.throttle, mo.duty_cycle, mo.rpm, j.voltage, j.current, j.voltage * j.current AS power,
                   g.lat, g.lon, st.brake_pressure AS brake, st.turn_angle AS steering,
                   (SELECT l.lap_number FROM {s}.laps l
                     WHERE l.run_id = m.run_id AND m.seq >= l.start_seq AND m.seq < l.end_seq) AS lap,
                   (SELECT g2.name FROM {view}_segments g2
                     WHERE g2.run_id = m.run_id AND m.seq >= g2.start_seq AND m.seq < g2.end_seq LIMIT 1) AS segment
            FROM {s}.main_snapshot m
            JOIN {view}_starts r ON r.run_id = m.run_id
            LEFT JOIN {s}.motor mo ON mo.id = m.motor_id
            LEFT JOIN {s}.joulemeter j ON j.id = m.joulemeter_id
            LEFT JOIN {s}.gps g ON g.id = m.gps_id
            LEFT JOIN {s}.steering st ON st.id = m.steering_id""")
