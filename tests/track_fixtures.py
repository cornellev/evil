"""Small hand-made tracks for the segments classifier tests, in local metres so the expected answers are obvious."""

from __future__ import annotations

import math

from evil import track

LAT0, LON0 = 42.0, -76.0
M_LAT = 111_320.0
M_LON = 111_320.0 * math.cos(math.radians(LAT0))


def ll(x: float, y: float) -> tuple[float, float]:
    """Metres east/north of the origin -> lat, lon."""
    return LAT0 + y / M_LAT, LON0 + x / M_LON


def gate_across(x: float, y: float, heading: str, half: float = 40.0) -> list[float]:
    """A gate through (x, y) perpendicular to travel along 'x' (east-west driving) or 'y' (north-south)."""
    if heading == "x":
        a, b = ll(x, y - half), ll(x, y + half)
    else:
        a, b = ll(x - half, y), ll(x + half, y)
    return [a[0], a[1], b[0], b[1]]


# A 300 m x 150 m rectangle driven anticlockwise from (100, 0). Six segments, kinds mixed.
RING_W, RING_H = 300.0, 150.0
RING_BOUNDS = [  # (name, kind, entry point, travel axis there)
    ("Straight 1", "straight", (100, 0), "x"),
    ("Turn A", "turn", (250, 0), "x"),
    ("Straight 2", "straight", (300, 50), "y"),
    ("Turn B", "turn", (300, 100), "y"),
    ("Straight 3", "straight", (250, 150), "x"),
    ("Turn C", "turn", (50, 150), "x"),
]


def ring_perimeter() -> list[tuple[float, float]]:
    return [(0, 0), (RING_W, 0), (RING_W, RING_H), (0, RING_H), (0, 0)]


def ring_point(s: float) -> tuple[float, float]:
    """Point at arc length s (m) along the rectangle from (0, 0), anticlockwise, wrapping."""
    per = 2 * (RING_W + RING_H)
    s %= per
    for (x0, y0), (x1, y1) in zip(ring_perimeter(), ring_perimeter()[1:]):
        seg = math.hypot(x1 - x0, y1 - y0)
        if s <= seg:
            f = s / seg
            return x0 + f * (x1 - x0), y0 + f * (y1 - y0)
        s -= seg
    return ring_point(0)


def ring_arc(x: float, y: float) -> float:
    """Arc length of a point that lies on the rectangle."""
    if y == 0:
        return x
    if x == RING_W:
        return RING_W + y
    if y == RING_H:
        return RING_W + RING_H + (RING_W - x)
    return 2 * RING_W + RING_H + (RING_H - y)


def ring_definition() -> dict:
    segs = []
    arcs = [ring_arc(*p) for _, _, p, _ in RING_BOUNDS]
    per = 2 * (RING_W + RING_H)
    for i, (name, kind, p, axis) in enumerate(RING_BOUNDS):
        length = (arcs[(i + 1) % len(arcs)] - arcs[i]) % per
        segs.append({"name": name, "kind": kind, "aliases": [name.split()[-1]] if kind == "turn" else [],
                     "length_m": length, "entry_gate": gate_across(*p, axis)})
    return {"segments": segs}


def seed_ring(conn) -> list[dict]:
    track.seed_track(conn, ring_definition())
    return ring_definition()["segments"]


# A straight line track driven west to east at latitude 42.0: a turn between two gates, then a straight.
def seed_line(conn, turn_from_x: float = 100.0, turn_to_x: float = 200.0) -> None:
    track.seed_track(conn, {"segments": [
        {"name": "Turn 1", "kind": "turn", "aliases": ["1"], "length_m": turn_to_x - turn_from_x,
         "entry_gate": gate_across(turn_from_x, 0, "x")},
        {"name": "Straight 1", "kind": "straight", "aliases": [], "length_m": 500,
         "entry_gate": gate_across(turn_to_x, 0, "x")},
    ]})
