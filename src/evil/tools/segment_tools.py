"""Turn and straight lookups. Turns and straights are the same kind of thing (a segment of the track between
two gates, see track.py) and carry the same measures, so one implementation serves get_turn / get_straight and
the compare and list tools. Every instance reports duration, distance, energy and efficiency in mi/kWh (the
Race Engineer Dashboard's definition; classifiers/metrics.py)."""

from __future__ import annotations

import sqlite3
from typing import Any

from evil.tools.turn_name_matching import resolve_segment_name

_TABLES = {"turn": ("turns", "turn_def_id", "turn_id"), "straight": ("straights", "segment_id", "straight_id")}

MEASURES = ("entry_speed", "exit_speed", "avg_speed", "distance_m", "energy_wh", "efficiency_mi_per_kwh")


def instance_dict(row: sqlite3.Row, name: str, id_key: str) -> dict[str, Any]:
    entry, exit_ = row["entry_speed"], row["exit_speed"]
    duration = row["duration_s"] if row["duration_s"] is not None else row["end_ts"] - row["start_ts"]
    return {
        id_key: row[id_key],
        "name": name,
        "start_ts": row["start_ts"],
        "end_ts": row["end_ts"],
        "duration_s": duration,
        "entry_speed": entry,
        "exit_speed": exit_,
        "speed_delta": (exit_ - entry) if entry is not None and exit_ is not None else None,
        "avg_speed": row["avg_speed"],
        "distance_m": row["distance_m"],
        "energy_wh": row["energy_wh"],
        "efficiency_mi_per_kwh": row["efficiency_mi_per_kwh"],
    }


def get_segment(conn: sqlite3.Connection, kind: str, run_id: str, name: str, occurrence: str = "latest") -> dict[str, Any]:
    resolved = resolve_segment_name(conn, name, kind)
    if resolved is None:
        return {"found": False}
    table, def_col, id_col = _TABLES[kind]
    order = "DESC" if occurrence == "latest" else "ASC"
    row = conn.execute(
        f"""SELECT t.*, s.name AS seg_name FROM {table} t JOIN track_segments s ON s.segment_id = t.{def_col}
            WHERE t.run_id = ? AND s.name = ? ORDER BY t.start_seq {order} LIMIT 1""",
        (run_id, resolved),
    ).fetchone()
    if row is None:
        return {"found": False}
    out = {"found": True, **instance_dict(row, row["seg_name"], id_col)}
    out[f"{kind}_name"] = row["seg_name"]
    return out


def compare_instances(conn: sqlite3.Connection, kind: str, run_id: str, name: str, limit: int = 10) -> dict[str, Any]:
    resolved = resolve_segment_name(conn, name, kind)
    empty = {"instances": [], "best_exit_speed_instance": None, "best_efficiency_instance": None}
    if resolved is None:
        return empty
    table, def_col, id_col = _TABLES[kind]
    rows = conn.execute(
        f"""SELECT t.*, s.name AS seg_name FROM {table} t JOIN track_segments s ON s.segment_id = t.{def_col}
            WHERE t.run_id = ? AND s.name = ? ORDER BY t.start_seq ASC LIMIT ?""",
        (run_id, resolved, limit),
    ).fetchall()
    instances = [instance_dict(r, r["seg_name"], id_col) for r in rows]
    if not instances:
        return empty
    with_exit = [i for i in instances if i["exit_speed"] is not None]
    with_eff = [i for i in instances if i["efficiency_mi_per_kwh"] is not None]
    return {
        "instances": instances,
        "best_exit_speed_instance": max(with_exit, key=lambda i: i["exit_speed"]) if with_exit else None,
        "best_efficiency_instance": max(with_eff, key=lambda i: i["efficiency_mi_per_kwh"]) if with_eff else None,
    }


def list_segment_instances(conn: sqlite3.Connection, kind: str, run_id: str, limit: int = 50, offset: int = 0) -> dict[str, Any]:
    table, def_col, id_col = _TABLES[kind]
    total = conn.execute(f"SELECT COUNT(*) AS n FROM {table} WHERE run_id = ?", (run_id,)).fetchone()["n"]
    rows = conn.execute(
        f"""SELECT t.*, s.name AS seg_name FROM {table} t JOIN track_segments s ON s.segment_id = t.{def_col}
            WHERE t.run_id = ? ORDER BY t.start_seq LIMIT ? OFFSET ?""",
        (run_id, limit, offset),
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d[f"{kind}_name"] = r["seg_name"]
        d["name"] = r["seg_name"]
        d.pop("seg_name", None)
        if d.get("duration_s") is None:
            d["duration_s"] = r["end_ts"] - r["start_ts"]
        out.append(d)
    return {"total": total, f"{kind}s": out}


def list_track_segments(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """The track's segments in lap order, so a caller can see which turns and straights exist."""
    return [
        {
            "ordinal": r["ordinal"], "name": r["name"], "kind": r["kind"], "length_m": r["length_m"],
            "official_turns": [a for a in (r["aliases"] or "").split(",") if a],
        }
        for r in conn.execute("SELECT * FROM track_segments ORDER BY ordinal")
    ]
