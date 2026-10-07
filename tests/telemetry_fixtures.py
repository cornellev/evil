"""Builders for strict evil.telemetry.v1 payloads, rosbag2 .db3 files and CSVs."""

from __future__ import annotations

import copy
import csv
import json
import sqlite3
import struct
from pathlib import Path

NAN = float("nan")


def payload(seq: int = 2, *, lat: float | None = 42.0, lon: float = -76.0, speed: float = 5.0,
            voltage: float = 48.0, current: float = 3.0, publish_ns: int | None = None,
            **overrides) -> dict:
    """One strict v1 payload. lat=None makes the GPS group NaN (no fix)."""
    p = {
        "seq": seq,
        "global_ts": 1_000_000 + seq * 1000,
        "_t_publish_ns": publish_ns if publish_ns is not None else 1_700_000_000_000_000_000 + seq,
        "power": {"ts": 1_000_100 + seq * 1000, "current": current, "voltage": voltage},
        "steering": {"ts": 1_000_200 + seq * 1000, "brake_pressure": 0.1, "turn_angle": 0.2},
        "rpm_front": {"ts": 0, "rpm_left": NAN, "rpm_right": NAN},
        "rpm_back": {"ts": 1_000_300 + seq * 1000, "rpm_left": 200.0, "rpm_right": 201.0},
        "gps": {"ts": 1_000_400 + seq * 1000,
                "lat": NAN if lat is None else lat, "long": NAN if lat is None else lon,
                "heading": NAN if lat is None else 90.0, "speed": NAN if lat is None else speed},
        "motor": {"ts": 1_000_500 + seq * 1000, "rpm": 0.0, "throttle": 0.0},
        "filtered": {"speed": speed},
    }
    p.update(copy.deepcopy(overrides))
    return p


def v2_payload(seq: int = 2, **kw) -> dict:
    """The later shape: errcount added, motor.throttle renamed duty_cycle."""
    p = payload(seq, **kw)
    p["errcount"] = 35
    p["motor"] = {"ts": p["motor"]["ts"], "rpm": 0.0, "duty_cycle": 0.0}
    return p


def encode_cdr_string(text: str) -> bytes:
    body = text.encode() + b"\x00"
    return b"\x00\x01\x00\x00" + struct.pack("<I", len(body)) + body


def write_db3(path: Path, streams: dict[str, list[tuple[float, object]]], types: dict[str, str] | None = None) -> Path:
    """streams: {topic: [(record_ts_seconds, payload_dict | raw_bytes)]}."""
    types = types or {}
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT, type TEXT)")
    conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, topic_id INTEGER, timestamp INTEGER, data BLOB)")
    rows = []
    for tid, (topic, msgs) in enumerate(streams.items(), start=1):
        conn.execute("INSERT INTO topics VALUES (?, ?, ?)", (tid, topic, types.get(topic, "std_msgs/msg/String")))
        for ts, item in msgs:
            data = item if isinstance(item, bytes) else encode_cdr_string(json.dumps(item))
            rows.append((ts, tid, data))
    for ts, tid, data in sorted(rows, key=lambda r: r[0]):
        conn.execute("INSERT INTO messages (topic_id, timestamp, data) VALUES (?, ?, ?)", (tid, int(ts * 1e9), data))
    conn.commit()
    conn.close()
    return path


def flatten(p: dict) -> dict:
    out = {}
    for k, v in p.items():
        if isinstance(v, dict):
            out.update({f"{k}.{kk}": vv for kk, vv in v.items()})
        else:
            out[k] = v
    return out


def write_csv(path: Path, items: list[tuple[float, dict]], extra_header: list[str] | None = None) -> Path:
    header = list(flatten(items[0][1])) + ["timestamp"] + (extra_header or [])
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for ts, p in items:
            flat = flatten(p)
            w.writerow([flat[k] for k in flatten(items[0][1])] + [int(ts * 1e9)] + [""] * len(extra_header or []))
    return path
