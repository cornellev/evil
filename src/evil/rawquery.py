"""Read-only SQL over a stored recording or over the catalog, for power users and
real agents (never Tern: see tern-llm's tool allowlist). The query runs in a
sandboxed child process (evil.rawquery_child); this module resolves WHICH file
from a catalog id (callers never supply paths), enforces the wall-clock timeout
and turns child failures into errors.

Honest limit: rosbag2 payloads are binary CDR blobs, so plain SQL sees topics,
counts, time ranges and structure. The one concession is `cdr_string(data)`,
which decodes a std_msgs/String blob to text so JSON-in-String bags (the
telemetry format, including shapes EVIL does not tabulate) can be queried with
json_extract(). Strings that come back are DATA FROM FILES: untrusted, possibly
adversarial (prompt injection), and size-capped.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from evil import catalog, parser

CATALOG_TABLES = ("recordings", "recording_files", "recording_streams", "named_locations", "jobs", "cache_entries")


class RawQueryError(ValueError):
    """The query could not be run or was rejected (message is safe to show an agent)."""


@dataclass(frozen=True)
class QueryLimits:
    max_rows: int = 200
    max_cell_chars: int = 2000
    max_result_bytes: int = 1_000_000
    max_seconds: float = 20.0
    memory_bytes: int = 1 << 30


def _child_env() -> dict[str, str]:
    import evil
    src = str(Path(evil.__file__).resolve().parents[1])
    existing = os.environ.get("PYTHONPATH")
    return {**os.environ, "PYTHONPATH": src + (os.pathsep + existing if existing else "")}


def run_sql(kind: str, path: Path, sql: str, *, tables: tuple[str, ...] | None = None,
            limits: QueryLimits = QueryLimits()) -> dict:
    """Run one SELECT in the sandbox. kind: 'sqlite-immutable' | 'sqlite' | 'csv'."""
    if not isinstance(sql, str) or not sql.strip():
        raise RawQueryError("sql is required")
    request = {"kind": kind, "path": str(path), "sql": sql, "tables": list(tables) if tables else None,
               "max_rows": limits.max_rows, "max_cell_chars": limits.max_cell_chars,
               "max_result_bytes": limits.max_result_bytes, "max_seconds": limits.max_seconds,
               "memory_bytes": limits.memory_bytes}
    try:
        done = subprocess.run([sys.executable, "-m", "evil.rawquery_child"], input=json.dumps(request),
                              capture_output=True, text=True, timeout=limits.max_seconds + 10, env=_child_env())
    except subprocess.TimeoutExpired as exc:
        raise RawQueryError(f"query exceeded its {limits.max_seconds:g}s time budget") from exc
    try:
        result = json.loads(done.stdout.strip().splitlines()[-1])
    except (IndexError, ValueError):
        raise RawQueryError("query was stopped by the sandbox (memory or CPU limit)") from None
    if not result.get("ok"):
        message = str(result.get("error", "query failed")).replace(str(path), "<file>")
        raise RawQueryError(message)
    result.pop("ok")
    return result


def resolve_recording_target(cat: sqlite3.Connection | None, root: catalog.DataRoot, recording_id: str,
                             file: str | None = None) -> tuple[str, Path, str]:
    """(kind, absolute path, label) for a recording; the path never leaves the server."""
    if cat is None:
        raise RawQueryError("no recording catalog exists yet")
    rec = catalog.get_recording(cat, recording_id)
    if rec is None:
        raise RawQueryError(f"recording {recording_id!r} not found")
    container = rec["container"]
    wanted = "db3" if container == "rosbag2-sqlite3" else "csv" if container == "csv" else None
    if wanted is None:
        raise RawQueryError(f"no SQL view for container {container!r} (it is stored; use describe_recording)")
    candidates = [f for f in rec["files"] if f["role"] == wanted]
    candidates.sort(key=lambda f: parser._natural_key(Path(f["rel_path"])))
    if file:
        candidates = [f for f in candidates if file in (f["original_name"], f["rel_path"], Path(f["rel_path"]).name)]
        if not candidates:
            raise RawQueryError(f"no file {file!r} in that recording; files: "
                                + ", ".join(f["original_name"] for f in rec["files"]))
    if not candidates:
        raise RawQueryError("that recording has no readable data file")
    chosen = candidates[0]
    path = (root.raw / chosen["rel_path"]).resolve()
    if not path.is_relative_to(root.raw.resolve()) or not path.is_file():
        raise RawQueryError("the stored file is missing or outside the raw store")
    return ("csv" if wanted == "csv" else "sqlite-immutable"), path, chosen["original_name"]


def query_recording_sql(cat: sqlite3.Connection | None, root: catalog.DataRoot, recording_id: str, sql: str,
                        file: str | None = None, limit: int = 200,
                        limits: QueryLimits | None = None) -> dict:
    kind, path, label = resolve_recording_target(cat, root, recording_id, file)
    base = limits or QueryLimits()
    lim = QueryLimits(**{**base.__dict__, "max_rows": max(1, min(int(limit), 1000))})
    try:
        result = run_sql(kind, path, sql, limits=lim, tables=None if kind != "csv" else ("csv",))
    except RawQueryError as exc:
        if "no such table" in str(exc) or "no such column" in str(exc):     # seen 11-18: `csv` guessed on a bag
            tables = "one table, csv" if kind == "csv" else "topics(id, name, type) and messages(id, topic_id, timestamp, data)"
            raise RawQueryError(f"{exc}. This file has {tables}: call recording_schema first for its columns, "
                                "topics and JSON paths.") from exc
        raise
    result["file"] = label
    result["table_hint"] = "table `csv`" if kind == "csv" else "rosbag2 tables: topics, messages (data is CDR; use cdr_string(data))"
    return result


def catalog_sql(root: catalog.DataRoot, sql: str, limit: int = 200, limits: QueryLimits | None = None) -> dict:
    """The catalog is a live WAL database, which a sandboxed (no-file-writes) reader cannot open
    safely (it must touch the -shm file). So each query runs against a point-in-time SNAPSHOT taken
    with SQLite's backup API (the catalog is small), opened immutable in the child."""
    import tempfile

    src = catalog.connect_catalog_readonly(root.catalog_db)
    if src is None:
        raise RawQueryError("no recording catalog exists yet")
    base = limits or QueryLimits()
    lim = QueryLimits(**{**base.__dict__, "max_rows": max(1, min(int(limit), 1000))})
    with tempfile.TemporaryDirectory(prefix="evil-catalog-snap-") as tmp:
        snapshot = Path(tmp) / "catalog.db"
        dst = sqlite3.connect(snapshot)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        return run_sql("sqlite-immutable", snapshot, sql, tables=CATALOG_TABLES, limits=lim)


SCHEMA_TOPICS = 20            # JSON topics sampled per bag (one decoded message each)
SCHEMA_PATHS = 40             # json_extract paths listed per topic


def _json_paths(value: Any, prefix: str = "$") -> list[tuple[str, Any]]:
    """Leaf paths of a decoded JSON message, as json_extract wants them ('$.gps.lat', '$.cells[0]')."""
    if isinstance(value, dict):
        out: list[tuple[str, Any]] = []
        for k, v in value.items():
            out += _json_paths(v, f"{prefix}.{k}" if str(k).isidentifier() else f'{prefix}."{k}"')
        return out
    if isinstance(value, list):
        return _json_paths(value[0], f"{prefix}[0]") if value else [(prefix, [])]
    if isinstance(value, float) and value != value:          # NaN (a sensor with no reading) isn't JSON
        value = None
    return [(prefix, value)]


def recording_schema(cat: sqlite3.Connection | None, root: catalog.DataRoot, recording_id: str,
                     file: str | None = None, limits: QueryLimits | None = None) -> dict:
    """How to query one stored recording, so the first query_recording_sql is right (user 11-18: the parsed tables
    got their schema; raw files need theirs): a CSV's columns, 3 sample rows and row count; a bag's topics (type,
    message count, from the catalog) and, for JSON-in-String topics, one decoded message as json_extract paths with
    example values; plus a ready query. Every read runs in the sandbox, like query_recording_sql."""
    kind, _, label = resolve_recording_target(cat, root, recording_id, file)

    def sql(query: str, limit: int = 3) -> dict:
        return query_recording_sql(cat, root, recording_id, query, file=file, limit=limit, limits=limits)
    if kind == "csv":
        sample = sql("SELECT * FROM csv LIMIT 3")
        count = sql("SELECT COUNT(*) AS n FROM csv")["rows"][0][0]
        cols = sample["columns"]
        return {"recording_id": recording_id, "file": label, "table": "csv", "columns": cols,
                "sample_rows": sample["rows"], "row_count": count,
                "example": f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols[:3])} FROM csv LIMIT 20',
                "note": 'Column names with dots or spaces need double quotes ("filtered.speed").'}
    streams = {r[0]: {"type": r[1], "messages": r[2]} for r in cat.execute(
        "SELECT stream_name, type_or_encoding, msg_count FROM recording_streams WHERE recording_id = ? "
        "AND kind = 'ros_topic' ORDER BY stream_name", (recording_id,))}
    topics = sql("SELECT id, name, type FROM topics ORDER BY name", limit=200)["rows"]
    out_topics, sampled = [], 0
    for topic_id, name, type_ in topics:
        entry = {"topic": name, "topic_id": topic_id, "type": type_,
                 "messages": streams.get(name, {}).get("messages")}
        if type_.endswith("/String") and sampled < SCHEMA_TOPICS:
            sampled += 1
            text = sql(f"SELECT cdr_string(data) FROM messages WHERE topic_id = {int(topic_id)} LIMIT 1",
                       limit=1)["rows"]
            try:
                paths = _json_paths(json.loads(text[0][0])) if text and text[0][0] else []
            except (ValueError, TypeError):
                paths = []
            if paths:
                entry["json_paths"] = {p: v for p, v in paths[:SCHEMA_PATHS]}
                first = paths[0][0]
                entry["example"] = (f"SELECT timestamp, json_extract(cdr_string(data), '{first}') FROM messages "
                                    f"WHERE topic_id = {int(topic_id)} ORDER BY timestamp LIMIT 20")
            elif text:
                sample = str(text[0][0])
                entry["sample_text"] = sample[:200]
                if "," in sample and not sample.lstrip().startswith(("{", "[")):
                    # Comma-separated text (older bags, seen 11-18): how to pull out a field, here the 2nd.
                    entry["fields"] = sample.count(",") + 1
                    entry["example"] = (
                        f"WITH t AS (SELECT timestamp, cdr_string(data) || ',' AS s FROM messages WHERE topic_id = "
                        f"{int(topic_id)}), f1 AS (SELECT timestamp, substr(s, instr(s, ',') + 1) AS rest FROM t) "
                        "SELECT timestamp, CAST(substr(rest, 1, instr(rest, ',') - 1) AS REAL) AS field2 FROM f1 "
                        "ORDER BY timestamp LIMIT 20")
        out_topics.append(entry)
    return {"recording_id": recording_id, "file": label, "tables": "topics(id, name, type), messages(id, topic_id, "
            "timestamp, data)", "topics": out_topics,
            "note": "messages.timestamp is nanoseconds since the epoch (timestamp / 1e9 for seconds); data is CDR: "
                    "cdr_string(data) decodes String messages, json_extract reads JSON in them. Filter by topic_id."}
