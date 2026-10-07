"""Read tools over the recording catalog (catalog.db): what recordings exist,
what is in one, and which stored files cover a moment in a run. These replace
the old nas_index/find_nas_files pair. Paths returned are catalog-relative
(`storage_backend` + `rel_path`); callers never supply paths.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from evil import catalog

_LIST_COLUMNS = (
    "recording_id", "run_id", "label", "source", "container", "schema_id", "car", "category", "event",
    "recorded_start", "recorded_end", "time_trust", "uploaded_at", "original_name", "total_bytes",
    "parse_status", "rows_ingested", "rows_duplicate", "rows_rejected", "parse_error", "location_id",
)


def list_recordings(
    conn: sqlite3.Connection | None,
    since: float | None = None,
    until: float | None = None,
    category: str | None = None,
    car: str | None = None,
    parse_status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Recordings, newest first, with enough detail to tell them apart. Several
    candidates are returned on purpose: the caller should list them and ask
    which one is meant rather than silently picking."""
    if conn is None:
        return []
    rows = catalog.list_recordings(
        conn, since=since, until=until, category=category, car=car, parse_status=parse_status,
        limit=limit, offset=offset,
    )
    names = {r["location_id"]: r["name"] for r in conn.execute("SELECT location_id, name FROM named_locations")}
    out = []
    for row in rows:
        item = {k: row[k] for k in _LIST_COLUMNS}
        item["location"] = names.get(row["location_id"])
        del item["location_id"]
        out.append(item)
    return out


def describe_recording(conn: sqlite3.Connection | None, recording_id: str) -> dict[str, Any]:
    if conn is None:
        return {"error": "no recording catalog exists yet"}
    rec = catalog.get_recording(conn, recording_id)
    if rec is None:
        return {"error": f"recording {recording_id!r} not found"}
    rec["streams"] = [
        dict(r) for r in conn.execute(
            "SELECT stream_name, kind, type_or_encoding, msg_count, first_ts, last_ts "
            "FROM recording_streams WHERE recording_id = ? ORDER BY stream_name", (recording_id,))
    ]
    loc = conn.execute("SELECT name FROM named_locations WHERE location_id = ?", (rec["location_id"],)).fetchone()
    rec["location"] = loc["name"] if loc else None
    rec.pop("metadata_json", None)  # verbose; reachable through the catalog tool if ever needed
    return rec


def find_nas_files(
    conn: sqlite3.Connection | None,
    run_id: str,
    start_ts: float | None = None,
    end_ts: float | None = None,
) -> list[dict[str, Any]]:
    """Stored/registered files covering a run, or only a moment in it. A run's
    time range is its own recording's recorded range; with start_ts/end_ts
    (e.g. a turn's start and end) only recordings overlapping that range are
    returned. Includes other sources (autonomy video/lidar/bags) recorded at
    the same time, which is the point: "what footage covers this turn"."""
    if conn is None:
        return []
    own = conn.execute("SELECT * FROM recordings WHERE run_id = ?", (run_id,)).fetchone()
    lo = start_ts if start_ts is not None else (own["recorded_start"] if own else None)
    hi = end_ts if end_ts is not None else (own["recorded_end"] if own else None)

    if lo is not None and hi is not None:
        recs = conn.execute(
            """SELECT * FROM recordings
               WHERE recorded_start IS NOT NULL AND recorded_start <= ? AND recorded_end >= ?
                  OR run_id = ?
               ORDER BY recorded_start""",
            (hi, lo, run_id),
        ).fetchall()
    else:
        recs = [own] if own else []

    out: list[dict[str, Any]] = []
    for rec in recs:
        for f in conn.execute(
            "SELECT * FROM recording_files WHERE recording_id = ? ORDER BY rel_path", (rec["recording_id"],)
        ):
            out.append({
                "recording_id": rec["recording_id"], "run_id": rec["run_id"], "source": rec["source"],
                "container": rec["container"], "role": f["role"], "original_name": f["original_name"],
                "storage_backend": f["storage_backend"], "rel_path": f["rel_path"],
                "size_bytes": f["size_bytes"], "recorded_start": rec["recorded_start"],
                "recorded_end": rec["recorded_end"],
            })
    return out
