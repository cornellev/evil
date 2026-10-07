"""Track geometry as an ordered ring of segments separated by gates.

A GATE is a short line across the track. Segment i runs from its entry gate to the next
segment's entry gate (the last wraps to the first), so every point of a lap belongs to exactly
one segment: segments cannot overlap and cannot leave gaps. A segment is a `turn` or a
`straight`, both first-class and quantified the same way (time, distance, energy, efficiency).

Why gates and not circles: circles overlap, leave stretches of track inside no circle at all
(those gaps are what used to become bogus "straights"), and a point-in-circle test has to be
debounced. A gate is crossed exactly once per pass; the classifier only watches the next gate in
lap order (plus one more, to survive a missed fix), so it costs one segment-intersection test per
GPS fix, and it stays in step when the GPS drops out.

The packaged definition (data/ims_2026_segments.json) was derived from real competition GPS; see
its `notes`. Gates are drawn far wider than the road so GPS error cannot make a car miss one.
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

DEFAULT_TRACK_FILE = Path(__file__).resolve().parent / "data" / "ims_2026_segments.json"
_EARTH_RADIUS_M = 6371000.0


@dataclass(frozen=True)
class Segment:
    segment_id: int
    ordinal: int
    kind: str                       # 'turn' | 'straight'
    name: str
    aliases: tuple[str, ...]        # official turn numbers this segment covers, e.g. ('1', '2')
    length_m: float
    gate: tuple[float, float, float, float]   # entry gate: lat1, lon1, lat2, lon2


def _project(lat: float, lon: float, lat0: float) -> tuple[float, float]:
    """Local east/north metres around lat0 (flat-earth: gates and fixes are within a few hundred metres)."""
    return (
        math.radians(lon) * math.cos(math.radians(lat0)) * _EARTH_RADIUS_M,
        math.radians(lat) * _EARTH_RADIUS_M,
    )


def crossing_fraction(
    p: tuple[float, float], q: tuple[float, float], gate: tuple[float, float, float, float]
) -> float | None:
    """Where the straight line from fix p to fix q (each lat, lon) crosses the gate: a fraction in
    [0, 1] along p->q, or None if it does not cross. The fraction is used to interpolate the crossing
    time between the two fixes, so boundaries are exact to well under one GPS sample."""
    lat0 = gate[0]
    x1, y1 = _project(p[0], p[1], lat0)
    x2, y2 = _project(q[0], q[1], lat0)
    x3, y3 = _project(gate[0], gate[1], lat0)
    x4, y4 = _project(gate[2], gate[3], lat0)
    d = (x2 - x1) * (y4 - y3) - (y2 - y1) * (x4 - x3)
    if abs(d) < 1e-9:
        return None  # parallel: the path runs along the gate, never across it
    t = ((x3 - x1) * (y4 - y3) - (y3 - y1) * (x4 - x3)) / d
    u = ((x3 - x1) * (y2 - y1) - (y3 - y1) * (x2 - x1)) / d
    return t if 0.0 <= t <= 1.0 and 0.0 <= u <= 1.0 else None


def load_track_file(path: str | Path = DEFAULT_TRACK_FILE) -> dict:
    return json.loads(Path(path).read_text())


def seed_track(conn: sqlite3.Connection, track: dict, replace: bool = False) -> int:
    """Write a track definition's segments (and its start/finish circle) into the reference tables.
    Seeding twice with the same definition is a no-op; a different one needs replace=True, after which
    derived rows (turns, straights, laps) must be rebuilt with scripts/reclassify. Returns segment count."""
    segments = track["segments"]
    existing = conn.execute("SELECT COUNT(*) AS n FROM track_segments").fetchone()["n"]
    if existing:
        current = [(s.name, s.kind, s.gate) for s in load_segments(conn)]
        wanted = [(s["name"], s["kind"], tuple(s["entry_gate"])) for s in segments]
        if current == wanted:
            return existing
        if not replace:
            raise ValueError("track_segments already holds a different track; pass replace=True and reclassify")
    with conn:
        if existing:
            conn.execute("DELETE FROM track_segments")
        for i, s in enumerate(segments):
            g = s["entry_gate"]
            conn.execute(
                """INSERT INTO track_segments
                       (segment_id, ordinal, kind, name, aliases, length_m,
                        gate_lat1, gate_lon1, gate_lat2, gate_lon2)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (i + 1, i, s["kind"], s["name"], ",".join(s.get("aliases", [])), s["length_m"], *g),
            )
        sf = track.get("start_finish")
        if sf and conn.execute("SELECT COUNT(*) AS n FROM start_finish_line").fetchone()["n"] == 0:
            conn.execute(
                "INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (?, ?, ?)",
                (sf["lat"], sf["lon"], sf["radius_m"]),
            )
    return len(segments)


def load_segments(conn: sqlite3.Connection) -> list[Segment]:
    rows = conn.execute("SELECT * FROM track_segments ORDER BY ordinal").fetchall()
    return [
        Segment(
            r["segment_id"], r["ordinal"], r["kind"], r["name"],
            tuple(a for a in (r["aliases"] or "").split(",") if a), r["length_m"],
            (r["gate_lat1"], r["gate_lon1"], r["gate_lat2"], r["gate_lon2"]),
        )
        for r in rows
    ]
