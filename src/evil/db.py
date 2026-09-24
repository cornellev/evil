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


def apply_schema(conn: sqlite3.Connection, schema_dir: Path | None = None) -> None:
    """Apply every schema/*.sql file in filename order. Idempotent: every
    statement uses CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS."""
    directory = schema_dir or SCHEMA_DIR
    for sql_file in sorted(directory.glob("*.sql")):
        conn.executescript(sql_file.read_text())
    conn.commit()
