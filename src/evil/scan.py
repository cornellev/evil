"""Level-1 scan: the cheap pass that fills the catalog right after upload, with
no ROS and no decoding of message payloads. Reads straight from a rosbag2 .db3
(topics, types, message counts, first/last timestamps), or a CSV's timestamp
column, plus metadata.yaml if present. Sets recorded_start/end, time_source
and time_trust. The raw file is rarely reopened afterwards because everything
worth knowing about it now lives in the catalog.

Unreadable containers are a normal case, not an error: the recording is marked
`skipped` with a parse_error saying why, and nothing else is attempted.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from evil import catalog, parser

PLAUSIBLE_MIN = 1_577_836_800.0   # 2020-01-01: earlier than this means an unsynced device clock
FUTURE_SLACK_SEC = 86_400.0


def _read_metadata(files: list[Path]) -> dict | None:
    meta = next((f for f in files if f.name.lower() == "metadata.yaml"), None)
    if meta is None:
        return None
    text = meta.read_text(errors="replace")
    try:
        import yaml
        data = yaml.safe_load(text)
        return data if isinstance(data, dict) else {"raw": text}
    except Exception:
        return {"raw": text}


def _csv_time_range(path: Path) -> tuple[float | None, float | None, int]:
    lo = hi = None
    count = 0
    for row in parser._csv_rows(path):
        ts = parser._csv_value(row.get("timestamp") or "")
        count += 1
        if isinstance(ts, int):
            lo = ts if lo is None else min(lo, ts)
            hi = ts if hi is None else max(hi, ts)
    return (lo / 1e9 if lo is not None else None, hi / 1e9 if hi is not None else None, count)


def _trust(start: float | None, end: float | None, now: float) -> str:
    if start is None or end is None:
        return "suspect"
    if start < PLAUSIBLE_MIN or end > now + FUTURE_SLACK_SEC:
        return "suspect"
    return "ok"


def scan_recording(root: catalog.DataRoot, recording_id: str) -> dict:
    """Scan one stored recording and update its catalog row. Idempotent."""
    cat = catalog.connect_catalog(root)
    try:
        rec = catalog.get_recording(cat, recording_id)
        if rec is None:
            raise KeyError(recording_id)
        files = [root.raw / f["rel_path"] for f in rec["files"]]
        container = rec["container"]
        streams: list[tuple] = []
        start = end = None
        time_source = None
        error = None

        try:
            if container == "rosbag2-sqlite3" or any(f.suffix.lower() == ".db3" for f in files):
                merged: dict[str, list] = {}
                for path in parser.db3_paths(files):
                    for _tid, name, type_, count, lo, hi in parser.db3_streams(path):
                        cur = merged.setdefault(name, [type_, 0, None, None])
                        cur[1] += count
                        if lo is not None:
                            cur[2] = lo if cur[2] is None else min(cur[2], lo)
                        if hi is not None:
                            cur[3] = hi if cur[3] is None else max(cur[3], hi)
                streams = [(name, "ros_topic", t, c, lo, hi) for name, (t, c, lo, hi) in sorted(merged.items())]
                los = [s[4] for s in streams if s[4] is not None]
                his = [s[5] for s in streams if s[5] is not None]
                start, end = (min(los), max(his)) if los else (None, None)
                time_source = "ros-record-time"
            elif container == "csv":
                csv_file = next((f for f in files if f.suffix.lower() == ".csv"), None)
                if csv_file is not None:
                    start, end, count = _csv_time_range(csv_file)
                    streams = [(csv_file.name, "csv", None, count, start, end)]
                    time_source = "csv-timestamp" if start is not None else None
        except parser.ContainerUnreadable as exc:
            error = f"container unreadable: {exc}"

        metadata = _read_metadata(files)
        now = time.time()
        with cat:
            cat.execute("DELETE FROM recording_streams WHERE recording_id = ?", (recording_id,))
            cat.executemany(
                "INSERT INTO recording_streams (recording_id, stream_name, kind, type_or_encoding, msg_count, first_ts, last_ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(recording_id, *s) for s in streams],
            )
            if error:
                cat.execute(
                    "UPDATE recordings SET parse_status = 'skipped', parse_error = ?, metadata_json = ? WHERE recording_id = ?",
                    (error, json.dumps(metadata) if metadata else None, recording_id))
            else:
                sets = ["metadata_json = ?"]
                args: list = [json.dumps(metadata) if metadata else None]
                if time_source and rec["time_trust"] != "overridden":
                    sets += ["recorded_start = ?", "recorded_end = ?", "time_source = ?", "time_trust = ?"]
                    args += [start, end, time_source, _trust(start, end, now)]
                cat.execute(f"UPDATE recordings SET {', '.join(sets)} WHERE recording_id = ?", [*args, recording_id])
        return {"streams": len(streams), "recorded_start": start, "recorded_end": end, "error": error}
    finally:
        cat.close()
