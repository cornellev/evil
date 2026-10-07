"""The sandboxed half of the raw query tools: runs ONE read-only SELECT in its
own process and prints a JSON result. Never imported by the servers; started by
evil.rawquery.run_sql() as `python -m evil.rawquery_child` with the request on
stdin. Everything that makes this safe to point at an untrusted file lives here:

- opened read-only (and immutable for stored files), `trusted_schema` off;
- an authorizer that allows only SELECT/CTE and a short list of safe functions,
  and denies everything else (ATTACH, PRAGMA, writes, load_extension, ...);
- SQLite size limits (blob/string length, SQL length, expression depth, attached
  databases = 0), a progress handler that interrupts at the time budget;
- process limits (address space, CPU time, no file writes) so a corrupt or huge
  file can only ever kill this child;
- result caps: rows, characters per cell, total bytes. Blobs are never returned
  raw, only their length and a short hex prefix.
"""

from __future__ import annotations

import csv
import json
import math
import re
import resource
import signal
import sqlite3
import struct
import sys
import time

SAFE_FUNCTIONS = frozenset(
    """abs avg coalesce count date datetime glob group_concat hex ifnull iif instr json json_array
    json_array_length json_extract json_group_array json_group_object json_insert json_object
    json_patch json_quote json_remove json_replace json_set json_type json_valid json_each json_tree
    julianday length like lower ltrim max min nullif printf format quote random replace round rtrim
    sign strftime substr substring sum time total trim typeof unicode unixepoch upper
    row_number rank dense_rank ntile lag lead first_value last_value nth_value percent_rank cume_dist
    cdr_string""".split()
)
INTERNAL_TABLES = {"sqlite_master", "sqlite_schema", "sqlite_temp_master", "sqlite_temp_schema"}

SQLITE_PRAGMA = 19
MAX_SQL_CHARS = 10_000
CSV_MAX_ROWS = 3_000_000
CSV_MAX_BYTES = 300 * 1024 * 1024


def _fail(message: str) -> None:
    print(json.dumps({"ok": False, "error": message}))
    sys.exit(0)


def _limit_process(req: dict) -> None:
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    for which, value in (
        (resource.RLIMIT_AS, req["memory_bytes"]),
        (resource.RLIMIT_CPU, int(req["max_seconds"]) + 5),
        (resource.RLIMIT_FSIZE, 0),
    ):
        try:
            resource.setrlimit(which, (value, value))
        except (ValueError, OSError):
            pass  # a platform without that limit: the parent's wall timeout still applies


def _cdr_string(blob) -> str | None:
    """std_msgs/msg/String CDR payload -> text (None if it is not one). Lets SQL reach
    into JSON-in-String bags: json_extract(cdr_string(data), '$.gps.lat')."""
    if not isinstance(blob, (bytes, bytearray)) or len(blob) < 9:
        return None
    try:
        endian = "<" if (blob[1] & 1) else ">"
        (n,) = struct.unpack_from(f"{endian}I", blob, 4)
        if 8 + n > len(blob):
            return None
        raw = bytes(blob[8:8 + n])
        return (raw[:-1] if raw.endswith(b"\x00") else raw).decode("utf-8", errors="replace")
    except (struct.error, IndexError):
        return None


def _make_authorizer(allowed_tables: set[str] | None):
    allow_select, allow_read, allow_function, allow_recursive = (
        sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE)

    def authorizer(action, arg1, arg2, _db, _source):
        if action in (allow_select, allow_recursive):
            return sqlite3.SQLITE_OK
        if action == allow_read:
            table = (arg1 or "").lower()
            if table in INTERNAL_TABLES or table.startswith("json_"):
                return sqlite3.SQLITE_OK
            if allowed_tables is None or table in allowed_tables:
                return sqlite3.SQLITE_OK
            return sqlite3.SQLITE_DENY
        if action == allow_function:
            return sqlite3.SQLITE_OK if (arg2 or "").lower() in SAFE_FUNCTIONS else sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_DENY  # ATTACH, PRAGMA, INSERT, UPDATE, DELETE, DDL, transactions, ...

    return authorizer


def _csv_cell(text: str):
    text = text.strip()
    if text == "":
        return None
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    try:
        value = float(text)
    except ValueError:
        return text
    return None if math.isnan(value) else value


def _open_csv(path: str) -> sqlite3.Connection:
    import os
    if os.path.getsize(path) > CSV_MAX_BYTES:
        _fail(f"CSV is larger than {CSV_MAX_BYTES // 2**20} MB; query it with a narrower tool")
    conn = sqlite3.connect(":memory:")
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            _fail("CSV has no header row")
        names = [h if h else f"col{i}" for i, h in enumerate(header)]
        cols = ", ".join('"' + n.replace('"', '""') + '"' for n in names)
        conn.execute(f"CREATE TABLE csv ({cols})")
        marks = ",".join("?" * len(names))
        batch, count = [], 0
        for row in reader:
            row = (row + [""] * len(names))[: len(names)]
            batch.append([_csv_cell(c) for c in row])
            count += 1
            if count > CSV_MAX_ROWS:
                _fail(f"CSV has more than {CSV_MAX_ROWS} rows")
            if len(batch) >= 5000:
                conn.executemany(f"INSERT INTO csv VALUES ({marks})", batch)
                batch = []
        if batch:
            conn.executemany(f"INSERT INTO csv VALUES ({marks})", batch)
    conn.commit()
    return conn


def _open(req: dict) -> tuple[sqlite3.Connection, set[str] | None]:
    kind, path = req["kind"], req["path"]
    try:
        if kind == "csv":
            return _open_csv(path), {"csv"}
        if kind == "sqlite-immutable":
            uri = f"file:{path}?mode=ro&immutable=1"
        elif kind == "sqlite":
            uri = f"file:{path}?mode=ro"
        else:
            _fail(f"unknown target kind {kind!r}")
        conn = sqlite3.connect(uri, uri=True, timeout=5)
    except (sqlite3.Error, OSError) as exc:
        _fail(f"cannot open the file as a database: {exc}")
    tables = {t.lower() for t in req["tables"]} if req.get("tables") else None
    return conn, tables


def _cell(value, max_chars: int):
    if value is None or isinstance(value, (int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (bytes, bytearray)):
        return f"<blob {len(value)} bytes: {bytes(value[:16]).hex()}{'...' if len(value) > 16 else ''}>"
    text = str(value)
    if len(text) > max_chars:
        return text[:max_chars] + f"...[{len(text) - max_chars} more chars]"
    return text


def main() -> None:
    req = json.loads(sys.stdin.read())
    sql = req["sql"]
    if len(sql) > MAX_SQL_CHARS:
        _fail(f"query is longer than {MAX_SQL_CHARS} characters")
    if not re.match(r"\s*(select|with)\b", sql, re.IGNORECASE):
        _fail("only a single SELECT statement is allowed")
    _limit_process(req)
    conn, tables = _open(req)
    started = time.monotonic()
    try:
        conn.execute("PRAGMA trusted_schema = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.create_function("cdr_string", 1, _cdr_string, deterministic=True)
        for limit, value in (
            (sqlite3.SQLITE_LIMIT_LENGTH, 4 * 1024 * 1024),
            (sqlite3.SQLITE_LIMIT_SQL_LENGTH, MAX_SQL_CHARS),
            (sqlite3.SQLITE_LIMIT_EXPR_DEPTH, 50),
            (sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, 10),
            (sqlite3.SQLITE_LIMIT_FUNCTION_ARG, 16),
            (sqlite3.SQLITE_LIMIT_ATTACHED, 0),
            (sqlite3.SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 200),
        ):
            conn.setlimit(limit, value)
        conn.set_authorizer(_make_authorizer(tables))
        deadline = started + req["max_seconds"]
        conn.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 10_000)

        cursor = conn.execute(sql)  # a second statement raises ProgrammingError
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows, size, truncated = [], 0, False
        for raw in cursor:
            if len(rows) >= req["max_rows"]:
                truncated = True
                break
            row = [_cell(v, req["max_cell_chars"]) for v in raw]
            size += len(json.dumps(row))
            if size > req["max_result_bytes"]:
                truncated = True
                break
            rows.append(row)
        print(json.dumps({"ok": True, "columns": columns, "rows": rows, "truncated": truncated,
                          "elapsed_ms": int((time.monotonic() - started) * 1000)}))
    except sqlite3.OperationalError as exc:
        message = str(exc)
        if "interrupted" in message.lower():
            message = f"query exceeded its {req['max_seconds']:g}s time budget"
        elif "not authorized" in message.lower() or "prohibited" in message.lower():
            message = "that statement or function is not allowed (read-only SELECT queries only)"
        _fail(message)
    except sqlite3.ProgrammingError as exc:
        _fail("only a single SELECT statement is allowed" if "one statement" in str(exc) else str(exc))
    except sqlite3.DatabaseError as exc:
        _fail(f"the file is damaged or not a readable database: {exc}")
    except MemoryError:
        _fail("query exceeded the memory limit")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
