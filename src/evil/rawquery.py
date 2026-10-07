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
    result = run_sql(kind, path, sql, limits=lim, tables=None if kind != "csv" else ("csv",))
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
