"""The `runs` table (schema/007_runs.sql) and the link between a live run and the full
upload of the same drive (live-next-steps-2026-11.md section C).

Linking is by the car's own clock: a live run's car_start/car_end come from the
publisher's `_t_publish_ns`, an upload's from the bag's record time, and both are the
car's clock (a few ms apart), whatever EVIL's clock says. Two runs are the same drive
when their overlap covers more than LINK_MIN_OVERLAP of the shorter one.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

LINK_MIN_OVERLAP = 0.5


def upsert_run(conn: sqlite3.Connection, run_id: str, *, tier: str, source: str, schema_id: str | None,
               status: str, started_at: float | None, ended_at: float | None, car_start: float | None = None,
               car_end: float | None = None, recording_id: str | None = None, **live) -> None:
    """Insert or update one run (the caller owns the transaction). On update, the
    facts only the live compiler knows (session mode, bag id, end reason) are kept
    unless given again."""
    cols = {"tier": tier, "source": source, "schema_id": schema_id, "status": status, "started_at": started_at,
            "ended_at": ended_at, "car_start": car_start, "car_end": car_end, "recording_id": recording_id,
            "updated_at": time.time(), **live}
    names = ", ".join(cols)
    marks = ", ".join(f":{k}" for k in cols)
    updates = ", ".join(f"{k} = COALESCE(excluded.{k}, {k})" if k in ("car_start", "car_end", "recording_id")
                        else f"{k} = excluded.{k}" for k in cols if k not in ("tier", "source"))
    conn.execute(
        f"INSERT INTO runs (run_id, {names}) VALUES (:run_id, {marks}) ON CONFLICT(run_id) DO UPDATE SET {updates}",
        {"run_id": run_id, **cols})


def overlap_fraction(a: tuple[float, float], b: tuple[float, float]) -> float:
    lo, hi = max(a[0], b[0]), min(a[1], b[1])
    shorter = min(a[1] - a[0], b[1] - b[0])
    if hi <= lo or shorter <= 0:
        return 0.0
    return (hi - lo) / shorter


def best_match(conn: sqlite3.Connection, tier: str, car_start: float, car_end: float) -> sqlite3.Row | None:
    """The run of `tier` in this DB overlapping [car_start, car_end] the most, if above the threshold."""
    best, best_f = None, LINK_MIN_OVERLAP
    for row in conn.execute(
            "SELECT * FROM runs WHERE tier = ? AND car_start IS NOT NULL AND car_end IS NOT NULL "
            "AND car_start < ? AND car_end > ?", (tier, car_end, car_start)):
        f = overlap_fraction((row["car_start"], row["car_end"]), (car_start, car_end))
        if f > best_f:
            best, best_f = row, f
    return best


def link_upload_and_live(main: sqlite3.Connection, live_db: Path, upload_run_id: str,
                         cat: sqlite3.Connection | None = None) -> str | None:
    """After an upload is parsed: link it to the live run of the same drive, if one exists.
    Returns the linked live run id. Writes both DBs and, when given, the catalog."""
    up = main.execute("SELECT * FROM runs WHERE run_id = ?", (upload_run_id,)).fetchone()
    if up is None or up["car_start"] is None or not live_db.exists():
        return None
    from evil import db
    live = db.connect(str(live_db))
    try:
        db.apply_schema(live)
        match = best_match(live, "live", up["car_start"], up["car_end"])
        if match is None:
            return None
        with live:
            live.execute("UPDATE runs SET linked_run_id = ?, updated_at = ? WHERE run_id = ?",
                         (upload_run_id, time.time(), match["run_id"]))
        with main:
            main.execute("UPDATE runs SET linked_run_id = ?, updated_at = ? WHERE run_id = ?",
                         (match["run_id"], time.time(), upload_run_id))
        if cat is not None and up["recording_id"] and match["recording_id"]:
            from evil import catalog
            catalog.link_recordings(cat, up["recording_id"], match["recording_id"])
        return match["run_id"]
    finally:
        live.close()


def link_live_to_upload(live: sqlite3.Connection, main_db: Path, live_run_id: str,
                        cat: sqlite3.Connection | None = None) -> str | None:
    """When a live run closes after its upload already exists (rare): the same link, from the live side."""
    run = live.execute("SELECT * FROM runs WHERE run_id = ?", (live_run_id,)).fetchone()
    if run is None or run["car_start"] is None or not main_db.exists():
        return None
    from evil import db
    main = db.connect(str(main_db))
    try:
        db.apply_schema(main)
        match = best_match(main, "upload", run["car_start"], run["car_end"])
        if match is None:
            return None
        with main:
            main.execute("UPDATE runs SET linked_run_id = ?, updated_at = ? WHERE run_id = ?",
                         (live_run_id, time.time(), match["run_id"]))
        with live:
            live.execute("UPDATE runs SET linked_run_id = ?, updated_at = ? WHERE run_id = ?",
                         (match["run_id"], time.time(), live_run_id))
        if cat is not None and run["recording_id"] and match["recording_id"]:
            from evil import catalog
            catalog.link_recordings(cat, match["recording_id"], run["recording_id"])
        return match["run_id"]
    finally:
        main.close()
