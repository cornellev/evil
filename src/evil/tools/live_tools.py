"""Tools over the live tier (live-next-steps-2026-11.md section D).

The live compiler writes its own database (evil_<car>_live.db); uploads go to the main one.
These functions read both, so a caller never has to know which file a run is in:

  - list_runs_all: every run from both DBs, most recent first, with tier ('live' | 'upload'),
    status ('recording' | 'closed') and the linked run (the live capture and the full upload
    of the same drive). A live run whose upload is parsed is hidden by default: the upload is
    the complete version.
  - get_live_status: is the car's feed alive, is a run recording, and the latest numbers.
  - get_live_window: the last N seconds of the recording (or latest live) run, downsampled.

Live metrics are approximate until the full upload lands (the live feed can miss messages;
the onboard bag does not).
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from evil import track

FEED_STALE_S = 5.0             # no message for this long = feed not alive
MAX_WINDOW_S = 600.0
MAX_WINDOW_POINTS = 300


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)).fetchone() is not None


def _runs_of(conn: sqlite3.Connection, default_tier: str) -> list[dict[str, Any]]:
    has_runs = _has_table(conn, "runs")
    meta = ("r.tier, r.status, r.linked_run_id, r.end_reason, r.schema_id, r.session_mode"
            if has_runs else "NULL AS tier, NULL AS status, NULL AS linked_run_id, NULL AS end_reason, "
                             "NULL AS schema_id, NULL AS session_mode")
    join = "LEFT JOIN runs r ON r.run_id = m.run_id" if has_runs else ""
    rows = conn.execute(
        f"""SELECT m.run_id, COUNT(*) AS sample_count, MIN(m.global_ts) AS start_ts, MAX(m.global_ts) AS end_ts,
                   MAX(m.global_ts) - MIN(m.global_ts) AS duration_s,
                   (SELECT COUNT(*) FROM laps l WHERE l.run_id = m.run_id) AS lap_count,
                   s.distance_m, s.energy_wh, s.efficiency_mi_per_kwh, {meta}
            FROM main_snapshot m LEFT JOIN run_summary s ON s.run_id = m.run_id {join}
            GROUP BY m.run_id"""
    ).fetchall()
    out = []
    for row in rows:
        d = dict(row)
        d["tier"] = d["tier"] or default_tier
        d["status"] = d["status"] or "closed"
        if d["session_mode"] is None:
            d.pop("session_mode")
        out.append(d)
    return out


def _norm(name: str | None) -> str:
    """'B Lot', 'b lot', 'blot', 'B-Lot' all compare equal."""
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def catalog_run_info(cat: sqlite3.Connection | None) -> dict[str, dict[str, Any]]:
    """run_id -> {location, category} from the recording catalog (uploads and live captures are both
    catalog rows, so both tiers get them). Empty without a catalog."""
    if cat is None:
        return {}
    names = {r["location_id"]: r["name"] for r in cat.execute("SELECT location_id, name FROM named_locations")}
    info: dict[str, dict[str, Any]] = {}
    for row in cat.execute("SELECT run_id, location_id, category FROM recordings WHERE run_id IS NOT NULL "
                           "ORDER BY uploaded_at"):
        cur = info.setdefault(row["run_id"], {"location": None, "category": None})
        cur["location"] = cur["location"] or names.get(row["location_id"])
        cur["category"] = cur["category"] or row["category"]
    return info


def list_runs_all(main: sqlite3.Connection, live: sqlite3.Connection | None,
                  include_live_twins: bool = False, catalog: sqlite3.Connection | None = None,
                  location: str | None = None, category: str | None = None,
                  since: float | None = None, until: float | None = None,
                  run_id: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    """Filters (all optional, off by default): location by name (case/space-insensitive, e.g. 'B Lot'),
    category (competition/testing/b_lot/...), since/until on the run's start time (epoch s), run_id
    (just that run, live twin or not). limit: at most this many, after sorting (None = all)."""
    runs = _runs_of(main, "upload")
    if live is not None:
        runs += [r for r in _runs_of(live, "live")
                 if include_live_twins or run_id is not None or not r["linked_run_id"]]
    if run_id is not None:
        runs = [r for r in runs if r["run_id"] == run_id]
    info = catalog_run_info(catalog)
    for r in runs:
        r.update(info.get(r["run_id"], {"location": None, "category": None}))
    if location:
        runs = [r for r in runs if _norm(r["location"]) == _norm(location)]
    if category:
        runs = [r for r in runs if r["category"] == category]
    if since is not None:
        runs = [r for r in runs if (r["start_ts"] or 0) >= since]
    if until is not None:
        runs = [r for r in runs if (r["start_ts"] or 0) < until]
    runs.sort(key=lambda r: (r["status"] != "recording", -(r["start_ts"] or 0)))
    return runs if limit is None else runs[:max(1, limit)]


def run_tier(live: sqlite3.Connection | None, run_id: str) -> bool:
    """True if run_id is a live run (lives in the live DB)."""
    if live is None:
        return False
    if _has_table(live, "runs") and live.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone():
        return True
    return live.execute("SELECT 1 FROM main_snapshot WHERE run_id = ? LIMIT 1", (run_id,)).fetchone() is not None


def _heartbeat(live: sqlite3.Connection) -> tuple[float | None, dict]:
    if not _has_table(live, "live_heartbeat"):
        return None, {}
    row = live.execute("SELECT updated_at, status_json FROM live_heartbeat WHERE id = 1").fetchone()
    return (row["updated_at"], json.loads(row["status_json"])) if row else (None, {})


def _current_run(live: sqlite3.Connection) -> dict | None:
    row = live.execute("SELECT * FROM runs WHERE tier = 'live' AND status = 'recording' "
                       "ORDER BY started_at DESC LIMIT 1").fetchone() if _has_table(live, "runs") else None
    return dict(row) if row else None


def _latest_sample(live: sqlite3.Connection, run_id: str) -> dict | None:
    row = live.execute(
        """SELECT ms.global_ts AS ts, ms.filtered_speed, g.speed AS gps_speed, g.lat, g.lon,
                  j.voltage, j.current, m.rpm AS motor_rpm, s.turn_angle
           FROM main_snapshot ms LEFT JOIN gps g ON g.id = ms.gps_id LEFT JOIN joulemeter j ON j.id = ms.joulemeter_id
           LEFT JOIN motor m ON m.id = ms.motor_id LEFT JOIN steering s ON s.id = ms.steering_id
           WHERE ms.run_id = ? ORDER BY ms.seq DESC LIMIT 1""", (run_id,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["speed"] = d.pop("filtered_speed") if d.get("filtered_speed") is not None else d.get("gps_speed")
    if d.get("voltage") is not None and d.get("current") is not None:
        d["power_w"] = d["voltage"] * d["current"]
    if d.get("lat") is None:   # no fix in the very latest snapshot: last known position
        fix = live.execute(
            """SELECT g.lat, g.lon FROM main_snapshot ms JOIN gps g ON g.id = ms.gps_id
               WHERE ms.run_id = ? AND g.lat IS NOT NULL ORDER BY ms.seq DESC LIMIT 1""", (run_id,)).fetchone()
        if fix:
            d["lat"], d["lon"], d["position_is_last_known"] = fix["lat"], fix["lon"], True
    return d


def _current_segment(live: sqlite3.Connection, run_id: str) -> str | None:
    row = live.execute("SELECT ordinal FROM segments_open_state WHERE run_id = ?", (run_id,)).fetchone()
    if row is None or row["ordinal"] is None:
        return None
    segments = track.load_segments(live)
    return segments[row["ordinal"]].name if 0 <= row["ordinal"] < len(segments) else None


def get_live_status(live: sqlite3.Connection | None, now: float | None = None) -> dict[str, Any]:
    """Whether the car's live feed is alive, whether a run is being recorded right now, and the
    latest numbers of that run (speed m/s, voltage V, current A, power W, position, current lap
    and segment, laps completed, distance m / energy Wh / efficiency mi/kWh so far)."""
    now = time.time() if now is None else now
    if live is None:
        return {"live_tier": "not set up (no live database)", "recording": False, "feed_alive": False}
    updated, hb = _heartbeat(live)
    last_msg = hb.get("last_message_at")
    out: dict[str, Any] = {
        "recording": False,
        "feed_alive": last_msg is not None and now - last_msg <= FEED_STALE_S,
        "last_message_age_s": round(now - last_msg, 1) if last_msg is not None else None,
        "msgs_per_s": hb.get("msgs_per_s"),
        "session_mode": hb.get("session_mode"),
        "bag_service": hb.get("bag"),
        "heartbeat_age_s": round(now - updated, 1) if updated is not None else None,
        "note": "Live numbers are approximate until the full upload of this drive is parsed.",
    }
    if updated is None or now - updated > 30:
        out["compiler"] = "not running (no heartbeat in 30 s)"
    run = _current_run(live)
    if run is None:
        last = live.execute("SELECT run_id, ended_at, end_reason FROM runs WHERE tier = 'live' "
                            "ORDER BY started_at DESC LIMIT 1").fetchone() if _has_table(live, "runs") else None
        if last:
            out["last_live_run"] = dict(last)
        return out
    rid = run["run_id"]
    out.update(recording=True, run_id=rid, schema_id=run["schema_id"], started_at=run["started_at"])
    out["latest"] = _latest_sample(live, rid)
    out["laps_completed"] = live.execute("SELECT COUNT(*) FROM laps WHERE run_id = ?", (rid,)).fetchone()[0]
    lap = live.execute("SELECT lap_number, start_ts FROM laps_open_state WHERE run_id = ?", (rid,)).fetchone()
    if lap:
        latest_ts = (out["latest"] or {}).get("ts")
        out["current_lap"] = {"lap_number": lap["lap_number"],
                              "elapsed_s": round(latest_ts - lap["start_ts"], 1) if latest_ts else None}
    out["current_segment"] = _current_segment(live, rid)
    summary = live.execute("SELECT distance_m, energy_wh, efficiency_mi_per_kwh FROM run_summary WHERE run_id = ?",
                           (rid,)).fetchone()
    if summary:
        out["so_far"] = dict(summary)
    counters = next((r for r in hb.get("runs", []) if r.get("run_id") == rid), None)
    if counters:
        out["counters"] = {k: counters[k] for k in ("stored", "duplicate", "rejected") if k in counters}
        out["counters"]["estimated_lost"] = (hb.get("loss") or {}).get("missing")
        out["counters"]["overflow"] = (hb.get("source") or {}).get("overflow", 0)
    return out


def get_live_window(live: sqlite3.Connection | None, seconds: float = 30.0, run_id: str | None = None,
                    max_points: int = MAX_WINDOW_POINTS) -> dict[str, Any]:
    """The last `seconds` (max 600) of a live run (default: the one recording now, else the
    latest live run): time, speed m/s, voltage V, current A, power W, lat/lon, steering angle,
    motor rpm, evenly downsampled to at most max_points (max 300) rows."""
    if live is None:
        return {"error": "no live database"}
    seconds = max(1.0, min(float(seconds), MAX_WINDOW_S))
    max_points = max(1, min(int(max_points), MAX_WINDOW_POINTS))
    if run_id is None:
        run = _current_run(live)
        if run is None and _has_table(live, "runs"):
            row = live.execute("SELECT run_id FROM runs WHERE tier = 'live' ORDER BY started_at DESC LIMIT 1").fetchone()
            run = dict(row) if row else None
        if run is None:
            return {"error": "no live run yet"}
        run_id = run["run_id"]
    end = live.execute("SELECT MAX(global_ts) FROM main_snapshot WHERE run_id = ?", (run_id,)).fetchone()[0]
    if end is None:
        return {"run_id": run_id, "seconds": seconds, "rows": []}
    rows = live.execute(
        """SELECT ms.global_ts AS ts, COALESCE(ms.filtered_speed, g.speed) AS speed, j.voltage, j.current,
                  j.voltage * j.current AS power_w, g.lat, g.lon, s.turn_angle, m.rpm AS motor_rpm
           FROM main_snapshot ms LEFT JOIN gps g ON g.id = ms.gps_id LEFT JOIN joulemeter j ON j.id = ms.joulemeter_id
           LEFT JOIN motor m ON m.id = ms.motor_id LEFT JOIN steering s ON s.id = ms.steering_id
           WHERE ms.run_id = ? AND ms.global_ts >= ? ORDER BY ms.seq""", (run_id, end - seconds)).fetchall()
    total = len(rows)
    if total > max_points:
        step = total / max_points
        rows = [rows[int(i * step)] for i in range(max_points - 1)] + [rows[-1]]
    return {"run_id": run_id, "seconds": seconds, "rows_in_window": total, "downsampled": total > max_points,
            "rows": [dict(r) for r in rows]}
