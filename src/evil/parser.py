"""Parsing a stored recording into the parsed database (container reader x
schema mapper, recording-catalog-design.md section 3.1).

Container readers: rosbag2-sqlite3 (`.db3`, possibly split across several
files) and `csv` (flattened payload keys plus a `timestamp` column in ns, as
written by shleeco26-analysis's csv_converter). Schema mapper:
evil.telemetry.v1 (schemas/telemetry_v1.py).

A recording matches a schema by CONTENT, never by topic name: a std_msgs/String
stream matches when every message in a sample (first 50 plus 50 spread through
the file, or all if fewer) conforms exactly. Once matched, every message is
parsed; a non-conforming message is rejected and counted, never coerced.
No match is not a failure: the recording is `skipped` (stored, cataloged,
scanned, simply not tabulated).

Consecutive repeat snapshots (same seq/ts/body, only `_t_publish_ns` differs:
the 50 Hz publisher re-reading an unchanged DAQ snapshot) are dropped and
counted in `rows_duplicate`. Re-parse is idempotent (the run is deleted first,
inside the same transaction as the inserts).
"""

from __future__ import annotations

import csv
import json
import math
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

from evil import catalog, db
from evil.classifiers.runner import tick
from evil.geo import haversine_m
from evil.ingest import insert_sample
from evil.ingestion.rosbag_file_source import decode_std_msgs_string
from evil.registered_classifiers import ALL_CLASSIFIERS
from evil.schemas import telemetry_v1 as v1

STRING_TYPE = "std_msgs/msg/String"
SAMPLE_HEAD = 50
SAMPLE_SPREAD = 50
PROGRESS_EVERY = 5000
MAX_REJECT_SAMPLES = 5

ProgressFn = Callable[[float], None]


class ContainerUnreadable(Exception):
    """The file cannot be read as its container type (e.g. malformed SQLite)."""


@dataclass
class Match:
    schema_id: str
    stream: str
    sampled: int
    conforming: int


@dataclass
class ParseResult:
    status: str                      # 'parsed' | 'skipped' | 'failed'
    run_id: str | None = None
    schema_id: str | None = None
    matched_stream: str | None = None
    rows_ingested: int = 0
    rows_duplicate: int = 0
    rows_rejected: int = 0
    reject_samples: list[dict] = field(default_factory=list)
    stats: dict = field(default_factory=dict)
    message: str | None = None


# ---- containers ----------------------------------------------------------

def _natural_key(path: Path) -> list:
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", path.name)]


def db3_paths(files: list[Path]) -> list[Path]:
    return sorted((f for f in files if f.suffix.lower() == ".db3"), key=_natural_key)


def _open_db3(path: Path) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.execute("SELECT COUNT(*) FROM topics").fetchone()
        conn.execute("SELECT COUNT(*) FROM messages").fetchone()
        return conn
    except sqlite3.DatabaseError as exc:
        raise ContainerUnreadable(f"{path.name}: {exc}") from exc


def db3_streams(path: Path) -> list[tuple[int, str, str, int, float | None, float | None]]:
    """(topic_id, name, type, msg_count, first_ts_s, last_ts_s) for one .db3."""
    conn = _open_db3(path)
    try:
        rows = conn.execute(
            """SELECT t.id, t.name, t.type, COUNT(m.id), MIN(m.timestamp), MAX(m.timestamp)
               FROM topics t LEFT JOIN messages m ON m.topic_id = t.id
               GROUP BY t.id ORDER BY t.name"""
        ).fetchall()
    except sqlite3.DatabaseError as exc:
        raise ContainerUnreadable(f"{path.name}: {exc}") from exc
    finally:
        conn.close()
    return [(i, n, t, c, lo / 1e9 if lo is not None else None, hi / 1e9 if hi is not None else None)
            for i, n, t, c, lo, hi in rows]


def _db3_messages(path: Path, stream: str) -> Iterator[tuple[float, bytes]]:
    """Messages of one stream in order, streamed (a bag can be 400 MB)."""
    conn = _open_db3(path)
    try:
        cursor = conn.execute(
            """SELECT m.timestamp, m.data FROM messages m JOIN topics t ON t.id = m.topic_id
               WHERE t.name = ? ORDER BY m.id""", (stream,))
        for timestamp_ns, data in cursor:
            yield timestamp_ns / 1e9, bytes(data)
    except sqlite3.DatabaseError as exc:
        raise ContainerUnreadable(f"{path.name}: {exc}") from exc
    finally:
        conn.close()


def _db3_sample(path: Path, stream: str) -> list[tuple[float, bytes]]:
    """First SAMPLE_HEAD plus SAMPLE_SPREAD spread through the stream (all if fewer)."""
    conn = _open_db3(path)
    try:
        row = conn.execute(
            """SELECT MIN(m.id), MAX(m.id), COUNT(*) FROM messages m JOIN topics t ON t.id = m.topic_id
               WHERE t.name = ?""", (stream,)).fetchone()
        lo, hi, count = row
        if not count:
            return []
        if count <= SAMPLE_HEAD + SAMPLE_SPREAD:
            return [(ts / 1e9, bytes(d)) for ts, d in conn.execute(
                """SELECT m.timestamp, m.data FROM messages m JOIN topics t ON t.id = m.topic_id
                   WHERE t.name = ? ORDER BY m.id""", (stream,))]
        picked: dict[int, tuple[float, bytes]] = {}
        for ts, d, i in conn.execute(
            """SELECT m.timestamp, m.data, m.id FROM messages m JOIN topics t ON t.id = m.topic_id
               WHERE t.name = ? ORDER BY m.id LIMIT ?""", (stream, SAMPLE_HEAD)):
            picked[i] = (ts / 1e9, bytes(d))
        for k in range(SAMPLE_SPREAD):
            target = lo + (hi - lo) * (k + 1) // (SAMPLE_SPREAD + 1)
            r = conn.execute(
                """SELECT m.timestamp, m.data, m.id FROM messages m JOIN topics t ON t.id = m.topic_id
                   WHERE t.name = ? AND m.id >= ? ORDER BY m.id LIMIT 1""", (stream, target)).fetchone()
            if r:
                picked[r[2]] = (r[0] / 1e9, bytes(r[1]))
        return list(picked.values())
    except sqlite3.DatabaseError as exc:
        raise ContainerUnreadable(f"{path.name}: {exc}") from exc
    finally:
        conn.close()


def _decode_string_payload(data: bytes) -> tuple[dict | None, str | None]:
    """CDR std_msgs/String -> JSON object. (payload, reject_reason)."""
    try:
        text = decode_std_msgs_string(data)
    except ValueError as exc:
        return None, f"undecodable CDR string: {exc}"
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, f"not JSON: {exc.msg}"
    return payload, None


_INT_RE = re.compile(r"^-?\d+$")


def _csv_value(text: str) -> int | float | None:
    text = text.strip()
    if text == "":
        return None
    if _INT_RE.match(text):
        return int(text)
    try:
        return float(text)  # also accepts 'nan' / 'inf'
    except ValueError:
        return None


def _csv_row_to_payload(row: dict[str, str]) -> tuple[dict | None, float | None, str | None]:
    """Un-flatten a CSV row back into the nested payload. (payload, ts_s, reject_reason)."""
    ts_raw = _csv_value(row.get("timestamp") or "")
    if not isinstance(ts_raw, int):
        return None, None, "timestamp is not an integer (ns)"
    payload: dict = {}
    for key, text in row.items():
        if key == "timestamp":
            continue
        value = _csv_value(text if isinstance(text, str) else "")
        if value is None:
            return None, ts_raw / 1e9, f"{key}: not a number ({text!r})"
        head, _, leaf = key.partition(".")
        if leaf:
            payload.setdefault(head, {})[leaf] = value
        else:
            payload[head] = value
    return payload, ts_raw / 1e9, None


def _csv_header(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return next(csv.reader(f), [])


def _csv_rows(path: Path) -> Iterator[dict[str, str]]:
    with path.open(newline="") as f:
        yield from csv.DictReader(f)


# ---- matching ------------------------------------------------------------

def _sample_conformance(items: list[tuple[dict | None, str | None]]) -> tuple[int, int]:
    ok = sum(1 for payload, reason in items if reason is None and v1.conforms(payload))
    return ok, len(items)


def match_db3(paths: list[Path]) -> tuple[Match | None, dict]:
    """Content match over every String stream of the first readable .db3.
    Returns (match or None, per-stream coverage for the catalog)."""
    coverage: dict[str, dict] = {}
    best: Match | None = None
    for path in paths[:1]:
        for _tid, name, type_, count, _lo, _hi in db3_streams(path):
            if type_ != STRING_TYPE or not count:
                continue
            decoded = [_decode_string_payload(d) for _ts, d in _db3_sample(path, name)]
            ok, total = _sample_conformance(decoded)
            coverage[name] = {v1.SCHEMA_ID: ok / total if total else 0.0, "sampled": total}
            if total and ok == total and best is None:
                best = Match(v1.SCHEMA_ID, name, total, ok)
    return best, coverage


def match_csv(path: Path) -> tuple[Match | None, dict]:
    header = _csv_header(path)
    expected = v1.flat_keys() | {"timestamp"}
    if set(header) != expected or len(header) != len(set(header)):
        extra, missing = sorted(set(header) - expected)[:5], sorted(expected - set(header))[:5]
        return None, {path.name: {v1.SCHEMA_ID: 0.0, "header_extra": extra, "header_missing": missing}}
    rows = list(_csv_rows(path))  # CSV exports are tens of MB
    if not rows:
        return None, {path.name: {v1.SCHEMA_ID: 0.0, "sampled": 0}}
    if len(rows) <= SAMPLE_HEAD + SAMPLE_SPREAD:
        picks = rows
    else:
        step = (len(rows) - SAMPLE_HEAD) / SAMPLE_SPREAD
        picks = rows[:SAMPLE_HEAD] + [rows[SAMPLE_HEAD + int(i * step)] for i in range(SAMPLE_SPREAD)]
    decoded = [(p, r) for p, _ts, r in (_csv_row_to_payload(row) for row in picks)]
    ok, total = _sample_conformance(decoded)
    coverage = {path.name: {v1.SCHEMA_ID: ok / total, "sampled": total}}
    return (Match(v1.SCHEMA_ID, path.name, total, ok) if ok == total else None), coverage


# ---- run ids and locations -----------------------------------------------

def _local_date(epoch: float) -> str:
    try:
        from zoneinfo import ZoneInfo
        when = datetime.fromtimestamp(epoch, tz=ZoneInfo("America/New_York"))
    except Exception:  # tzdata missing: fall back to UTC rather than fail a parse
        when = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return f"{when:%Y-%m-%d}"


def assign_run_id(conn: sqlite3.Connection, rec: dict) -> str:
    """`<local date>-<6 hex>` from the recorded date (upload date if unknown).
    Assigned once, after the scan, and kept across re-parses."""
    if rec.get("run_id"):
        return rec["run_id"]
    when = rec.get("recorded_start") or rec["uploaded_at"]
    base = f"{_local_date(when)}-{rec['recording_id'][-6:]}"
    run_id, n = base, 1
    while conn.execute("SELECT 1 FROM recordings WHERE run_id = ?", (run_id,)).fetchone():
        n += 1
        run_id = f"{base}-{n}"
    return run_id


def match_location(cat: sqlite3.Connection, lat: float, lon: float) -> int | None:
    best, best_d = None, math.inf
    for loc in cat.execute("SELECT * FROM named_locations"):
        d = haversine_m(lat, lon, loc["center_lat"], loc["center_lon"])
        if d <= loc["radius_m"] and d < best_d:
            best, best_d = loc["location_id"], d
    return best


def _gps_summary(parsed: sqlite3.Connection, run_id: str) -> dict | None:
    row = parsed.execute(
        """SELECT MIN(lat) a, MAX(lat) b, MIN(lon) c, MAX(lon) d, COUNT(*) n FROM gps
           WHERE run_id = ? AND lat IS NOT NULL AND lon IS NOT NULL AND NOT (lat = 0 AND lon = 0)""",
        (run_id,)).fetchone()
    if not row or not row["n"]:
        return None
    return {"min_lat": row["a"], "max_lat": row["b"], "min_lon": row["c"], "max_lon": row["d"], "fixes": row["n"]}


# ---- the parse -----------------------------------------------------------

def _recording_files(cat: sqlite3.Connection, root: catalog.DataRoot, recording_id: str) -> list[Path]:
    return [root.raw / r["rel_path"] for r in cat.execute(
        "SELECT rel_path FROM recording_files WHERE recording_id = ? ORDER BY rel_path", (recording_id,))]


def _messages(container: str, files: list[Path], stream: str) -> Iterator[tuple[float, dict | None, str | None]]:
    """(record_ts_s, payload, reject_reason) for every message of the matched stream."""
    if container == "csv":
        csv_file = next(f for f in files if f.name == stream or f.suffix.lower() == ".csv")
        for row in _csv_rows(csv_file):
            payload, ts, reason = _csv_row_to_payload(row)
            yield (ts if ts is not None else 0.0), payload, reason
    else:
        for path in db3_paths(files):
            for ts, data in _db3_messages(path, stream):
                payload, reason = _decode_string_payload(data)
                yield ts, payload, reason


def parse_recording(
    root: catalog.DataRoot,
    recording_id: str,
    progress: ProgressFn | None = None,
    parsed_db_path: Path | None = None,
) -> ParseResult:
    """Parse one stored recording. Updates the catalog row (status, counts, run_id,
    schema, location) and the parsed DB. Raises only on unexpected errors, after
    marking the recording `failed`."""
    cat = catalog.connect_catalog(root)
    try:
        rec = catalog.get_recording(cat, recording_id)
        if rec is None:
            raise KeyError(recording_id)
        if rec["recorded_start"] is None and rec["time_trust"] != "overridden":
            # The run_id's date comes from the data, so make sure the scan has run
            # (normally it already did: scan is the first, fast-lane job).
            from evil import scan
            scan.scan_recording(root, recording_id)
            rec = catalog.get_recording(cat, recording_id)
        files = _recording_files(cat, root, recording_id)
        result = _parse(cat, root, rec, files, progress, parsed_db_path)
    except ContainerUnreadable as exc:
        result = ParseResult("skipped", message=f"container unreadable: {exc}")
    except Exception as exc:
        _finish(cat, recording_id, ParseResult("failed", message=f"{type(exc).__name__}: {exc}"))
        cat.close()
        raise
    _finish(cat, recording_id, result)
    cat.close()
    return result


def _finish(cat: sqlite3.Connection, recording_id: str, r: ParseResult) -> None:
    with cat:
        cat.execute(
            """UPDATE recordings SET parse_status = ?, parser_version = ?, schema_id = ?, matched_stream = ?,
                   rows_ingested = ?, rows_duplicate = ?, rows_rejected = ?, reject_samples_json = ?,
                   parse_stats_json = ?, parse_error = ?
               WHERE recording_id = ?""",
            (r.status, v1.PARSER_VERSION if r.status == "parsed" else None, r.schema_id, r.matched_stream,
             r.rows_ingested if r.status == "parsed" else None, r.rows_duplicate if r.status == "parsed" else None,
             r.rows_rejected if r.status == "parsed" else None,
             json.dumps(r.reject_samples) if r.reject_samples else None,
             json.dumps(r.stats) if r.stats else None, r.message, recording_id),
        )


def _parse(cat, root, rec, files, progress, parsed_db_path) -> ParseResult:
    container = rec["container"]
    if container == "unknown" and any(f.suffix.lower() == ".db3" for f in files):
        raise ContainerUnreadable("not a SQLite database")
    if container not in ("rosbag2-sqlite3", "csv"):
        return ParseResult("skipped", message=f"no reader for container {container!r}")

    if container == "rosbag2-sqlite3":
        match, coverage = match_db3(db3_paths(files))
    else:
        csv_file = next((f for f in files if f.suffix.lower() == ".csv"), None)
        match, coverage = match_csv(csv_file) if csv_file else (None, {})
    with cat:
        cat.execute("UPDATE recordings SET schema_match_json = ? WHERE recording_id = ?",
                    (json.dumps(coverage), rec["recording_id"]))
    if match is None:
        return ParseResult("skipped", message="no stream matches a known schema (evil.telemetry.v1)")

    car = rec["car"] or v1.CAR
    target = parsed_db_path or catalog.parsed_db_path(root, car)
    target.parent.mkdir(parents=True, exist_ok=True)
    run_id = assign_run_id(cat, rec)
    with cat:  # claim the run id before the long parse so a concurrent assign cannot reuse it
        cat.execute("UPDATE recordings SET run_id = ?, car = COALESCE(car, ?) WHERE recording_id = ?",
                    (run_id, car, rec["recording_id"]))

    total = sum((s[3] for p in db3_paths(files) for s in db3_streams(p) if s[1] == match.stream), 0) \
        if container == "rosbag2-sqlite3" else 0
    parsed = db.connect(str(target))
    db.apply_schema(parsed)
    stored = dup = rejected = conflicts = seen = 0
    rejects: list[dict] = []
    first_ts = last_ts = None
    prev_key: str | None = None
    prev_seq: int | None = None
    try:
        parsed.execute("BEGIN")
        db.delete_run(parsed, run_id)
        for ts, payload, reason in _messages(container, files, match.stream):
            seen += 1
            if progress and seen % PROGRESS_EVERY == 0:
                progress(min(0.95, seen / total) if total else 0.5)
            if reason is None:
                bad = v1.validate(payload)
                reason = "; ".join(bad) if bad else None
            if reason is not None:
                rejected += 1
                if len(rejects) < MAX_REJECT_SAMPLES:
                    rejects.append({"index": seen - 1, "ts": ts, "reason": reason})
                continue
            key = v1.repeat_key(payload)
            if key == prev_key:
                dup += 1
                continue
            if prev_seq is not None and payload["seq"] == prev_seq:
                conflicts += 1  # same DAQ seq, different body: kept, but worth knowing
            prev_key, prev_seq = key, payload["seq"]
            insert_sample(parsed, v1.to_sample(run_id, payload, ts))
            stored += 1
            first_ts = ts if first_ts is None else first_ts
            last_ts = ts
        if stored == 0:
            parsed.rollback()
            return ParseResult("failed", run_id, v1.SCHEMA_ID, match.stream, 0, dup, rejected, rejects,
                               message="matched a schema but no message was valid")
        parsed.commit()
        if progress:
            progress(0.97)
        tick(parsed, run_id, ALL_CLASSIFIERS, now_ts=last_ts + 3600.0)
        gps = _gps_summary(parsed, run_id)
    except BaseException:
        parsed.rollback()
        raise
    finally:
        parsed.close()

    _record_context(cat, rec, target, first_ts, last_ts, gps)
    stats = {"messages_seen": seen, "seq_conflicts": conflicts}
    return ParseResult("parsed", run_id, v1.SCHEMA_ID, match.stream, stored, dup, rejected, rejects, stats)


def _record_context(cat, rec, target: Path, first_ts, last_ts, gps: dict | None) -> None:
    """After a parse: the data's own time range, GPS box and (advisory) location label."""
    sets, args = ["target_db = ?"], [target.name]
    if rec["recorded_start"] is None:
        sets += ["recorded_start = ?", "recorded_end = ?", "time_source = ?"]
        args += [first_ts, last_ts, "ros-record-time" if rec["container"] != "csv" else "csv-timestamp"]
    if gps:
        sets += ["gps_min_lat = ?", "gps_max_lat = ?", "gps_min_lon = ?", "gps_max_lon = ?"]
        args += [gps["min_lat"], gps["max_lat"], gps["min_lon"], gps["max_lon"]]
        if rec["location_method"] != "manual":
            loc = match_location(cat, (gps["min_lat"] + gps["max_lat"]) / 2, (gps["min_lon"] + gps["max_lon"]) / 2)
            if loc is not None:
                sets += ["location_id = ?", "location_method = 'gps-match'"]
                args.append(loc)
    with cat:
        cat.execute(f"UPDATE recordings SET {', '.join(sets)} WHERE recording_id = ?", [*args, rec["recording_id"]])
