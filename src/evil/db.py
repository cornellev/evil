"""SQLite connection + schema management. No ORM: the schema/*.sql files are
the source of truth, and every query in this package is plain SQL."""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema"


def connect(path: str) -> sqlite3.Connection:
    """Open a read/write connection tuned for a single-writer edge device."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def connect_readonly(path: str) -> sqlite3.Connection:
    """Open a read-only connection. Used by tern-llm's tool layer so a bad
    query from the harness can never write to the live ingest database."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# Schema version (PRAGMA user_version). 2 = gate-based track segments: turns/straights/laps gained
# duration/distance/energy/efficiency and reference data moved from circles (track_geometry) to
# track_segments. Derived tables are rebuildable (scripts/reclassify), so an older database has them
# dropped and recreated rather than altered.
SCHEMA_VERSION = 2
_DERIVED_TABLES = ("turns", "turns_open_state", "straights", "laps", "laps_open_state", "classifier_cursor")


def apply_schema(conn: sqlite3.Connection, schema_dir: Path | None = None) -> None:
    """Apply every schema/*.sql file in filename order. Idempotent: every
    statement uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.
    A database from before SCHEMA_VERSION 2 first has its derived tables (and the old circle
    geometry) dropped; raw snapshots are untouched and `scripts/reclassify` rebuilds the rest."""
    directory = schema_dir or SCHEMA_DIR
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version < SCHEMA_VERSION:
        has_old = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name IN ('turns', 'track_geometry') LIMIT 1"
        ).fetchone()
        if has_old:
            for table in _DERIVED_TABLES + ("track_geometry",):
                conn.execute(f"DROP TABLE IF EXISTS {table}")
    for sql_file in sorted(directory.glob("*.sql")):
        conn.executescript(sql_file.read_text())
    conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
    conn.commit()


_RUN_TABLES_IN_DELETE_ORDER = (
    # derived first, then the snapshot rows that point at raw rows, then raw rows
    "turns", "segments_open_state", "laps", "laps_open_state", "straights", "classifier_cursor", "run_summary",
    "main_snapshot",
    "joulemeter", "steering", "rpm_front", "rpm_back", "gps", "motor", "local_planner",
)


def delete_run(conn: sqlite3.Connection, run_id: str) -> None:
    """Remove everything stored for one run (raw, snapshots, derived, cursors),
    so a re-parse is idempotent. The caller owns the transaction."""
    for table in _RUN_TABLES_IN_DELETE_ORDER:
        conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))
