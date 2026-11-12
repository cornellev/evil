"""knowledge.db: the filtered copy of the `cev` space, one SQLite file on the NUC's local disk.

One writer (the indexer), readers open it read-only (the server), WAL so a reindex never blocks a search.
Tables: pages, chunks, chunks_fts (FTS5 over title / heading / text, porter stemming), restricted (ids of pages
with their own view restriction, so children are skipped on incremental runs too), meta, index_runs.
Confluence attachments are rows of `pages` too (kind attachment / attachment_link_only, `parent` = their page).
embeddings holds one vector per section for the current embedding model (embeddings.py); it goes with its section.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from evil.knowledge.chunker import Chunk

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS pages (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    space TEXT NOT NULL,
    breadcrumb TEXT NOT NULL,        -- JSON list of ancestor titles, root first
    updated TEXT NOT NULL,
    version INTEGER NOT NULL,
    labels TEXT NOT NULL,            -- JSON list
    kind TEXT NOT NULL,              -- page | work_log | meeting_notes | credentials_link_only | attachment[_link_only]
    parent TEXT,                     -- attachments: the page they are attached to (deleted with it)
    chunker_version TEXT NOT NULL,
    indexed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    id INTEGER PRIMARY KEY,
    page_id TEXT NOT NULL REFERENCES pages(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    heading TEXT NOT NULL,
    text TEXT NOT NULL,
    drive_links TEXT NOT NULL,       -- JSON list of [url, link text]
    UNIQUE (page_id, ordinal)
);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(title, heading, text, tokenize = 'porter unicode61');
CREATE TABLE IF NOT EXISTS embeddings (
    chunk_id INTEGER PRIMARY KEY REFERENCES chunks(id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    dims INTEGER NOT NULL,
    vector BLOB NOT NULL             -- float32, L2-normalized
);
CREATE TABLE IF NOT EXISTS restricted (id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS index_runs (
    id INTEGER PRIMARY KEY,
    started TEXT NOT NULL,
    finished TEXT,
    mode TEXT NOT NULL,              -- full | incremental
    fetched INTEGER DEFAULT 0,
    indexed INTEGER DEFAULT 0,
    unchanged INTEGER DEFAULT 0,
    removed INTEGER DEFAULT 0,
    skipped INTEGER DEFAULT 0,
    scrubbed INTEGER DEFAULT 0,      -- credential lines/rows/tables/sections/keys removed (counts only)
    requests INTEGER DEFAULT 0,
    error TEXT
);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    if "parent" not in {r[1] for r in conn.execute("PRAGMA table_info(pages)")}:      # index from before phase 6
        conn.execute("ALTER TABLE pages ADD COLUMN parent TEXT")
        conn.commit()
    return conn


def connect_readonly(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                 (key, value))


def page_state(conn: sqlite3.Connection, page_id: str) -> tuple[int, str] | None:
    row = conn.execute("SELECT version, chunker_version FROM pages WHERE id = ?", (page_id,)).fetchone()
    return (row[0], row[1]) if row else None


def page_ids(conn: sqlite3.Connection) -> set[str]:
    """Pages only (not attachments)."""
    return {r[0] for r in conn.execute("SELECT id FROM pages WHERE parent IS NULL")}


def _delete_rows(conn: sqlite3.Connection, page_id: str) -> bool:
    ids = [r[0] for r in conn.execute("SELECT id FROM chunks WHERE page_id = ?", (page_id,))]
    conn.executemany("DELETE FROM chunks_fts WHERE rowid = ?", [(i,) for i in ids])
    cur = conn.execute("DELETE FROM pages WHERE id = ?", (page_id,))
    return cur.rowcount > 0


def delete_page(conn: sqlite3.Connection, page_id: str) -> bool:
    """Removes a page (or an attachment) and the attachments of that page."""
    for (child,) in conn.execute("SELECT id FROM pages WHERE parent = ?", (page_id,)).fetchall():
        _delete_rows(conn, child)
    return _delete_rows(conn, page_id)


def attachment_ids(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT id FROM pages WHERE parent IS NOT NULL")}


def upsert_page(conn: sqlite3.Connection, *, page_id: str, title: str, url: str, space: str, breadcrumb: list[str],
                updated: str, version: int, labels: list[str], kind: str, chunker_version: str, indexed_at: str,
                chunks: Iterable[Chunk], parent: str | None = None) -> int:
    _delete_rows(conn, page_id)           # its attachments stay: they have their own versions
    conn.execute(
        "INSERT INTO pages (id, title, url, space, breadcrumb, updated, version, labels, kind, chunker_version,"
        " indexed_at, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (page_id, title, url, space, json.dumps(breadcrumb), updated, version, json.dumps(labels), kind,
         chunker_version, indexed_at, parent),
    )
    n = 0
    for c in chunks:
        cur = conn.execute(
            "INSERT INTO chunks (page_id, ordinal, heading, text, drive_links) VALUES (?, ?, ?, ?, ?)",
            (page_id, c.ordinal, c.heading, c.text, json.dumps(c.drive_links)),
        )
        conn.execute("INSERT INTO chunks_fts (rowid, title, heading, text) VALUES (?, ?, ?, ?)",
                     (cur.lastrowid, title, c.heading, c.text))
        n += 1
    return n


def restricted_ids(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT id FROM restricted")}


def replace_restricted(conn: sqlite3.Connection, ids: set[str]) -> None:
    conn.execute("DELETE FROM restricted")
    conn.executemany("INSERT INTO restricted (id) VALUES (?)", [(i,) for i in sorted(ids)])


def stats(conn: sqlite3.Connection) -> dict:
    pages = conn.execute("SELECT count(*) FROM pages WHERE parent IS NULL").fetchone()[0]
    try:
        attachments = conn.execute("SELECT count(*) FROM pages WHERE parent IS NOT NULL").fetchone()[0]
    except sqlite3.OperationalError:      # read-only server on an index from before phase 6
        attachments = 0
    chunks = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]
    try:
        embedded = conn.execute("SELECT model, count(*) FROM embeddings GROUP BY model").fetchall()
    except sqlite3.OperationalError:      # read-only server on an index from before phase 3
        embedded = []
    last = conn.execute("SELECT * FROM index_runs ORDER BY id DESC LIMIT 1").fetchone()
    last_ok = conn.execute("SELECT finished, mode FROM index_runs WHERE error IS NULL AND finished IS NOT NULL"
                           " ORDER BY id DESC LIMIT 1").fetchone()
    return {
        "pages": pages,
        "attachments": attachments,
        "chunks": chunks,
        "embedded": {m: n for m, n in embedded},
        "last_run": dict(last) if last else None,
        "last_success": last_ok["finished"] if last_ok else None,
    }
