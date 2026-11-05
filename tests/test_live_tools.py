"""MCP over both tiers (live-next-steps-2026-11.md section D): list_runs merges the main and
live DBs, run-scoped tools find live runs, get_live_status / get_live_window read the
recording run, read_only_sql sees `live`. The live DB is produced by a real LiveCompiler."""

from __future__ import annotations

import asyncio
import json

import pytest

from evil import catalog, db, parser
from evil.ingestion.base import decode_json
from evil.ingestion.replay_source import ReplaySource
from evil.mcp_server import create_server
from evil.registered_classifiers import ALL_CLASSIFIERS
from evil.scripts.run_live_compiler import LiveCompiler
from evil.bag_service import BagState
from telemetry_fixtures import payload, write_db3
from track_fixtures import RING_H, RING_W, ll, ring_point, seed_ring

T0 = 1_775_000_000.0
HZ = 50
RUNNING = BagState(True, True, "bag-1")
STOPPED = BagState(True, False, None)


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def _drive_msgs(seconds, t0=T0, speed=8.0):
    out, fix = [], None
    for k in range(int(seconds * HZ)):
        t = t0 + k / HZ
        if k % HZ == 0:
            x, y = ring_point(k * speed / HZ)
            fix = (*ll(x, y), 1_000_000_000 + int((t - t0) * 1e6))
        p = payload(seq=2 + 2 * k, lat=fix[0], lon=fix[1], speed=speed, voltage=50.0, current=8.0,
                    publish_ns=int(t * 1e9))
        p["global_ts"] = 1_000_000_000 + int((t - t0) * 1e6)
        p["gps"]["ts"] = fix[2]                 # as on the car: the fix's own time, repeated until the next fix
        out.append(decode_json(json.dumps(p), t))
    return out


def _seed(conn):
    seed_ring(conn)
    lat, lon = ll(150.0, 0.0)
    conn.execute("INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (?, ?, ?)", (lat, lon, 10.0))
    conn.commit()


class FakeBag:
    async def poll(self):
        return STOPPED


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """Main DB with one uploaded run (linked to a live twin) + live DB with a closed twin and
    a run recording right now (2.5 laps in)."""
    tmp_path = tmp_path_factory.mktemp("live_world")
    root = catalog.DataRoot(tmp_path / "data")
    root.ensure()
    main_path = catalog.ensure_parsed_db(root, "uc26")
    m = db.connect(str(main_path))
    _seed(m)
    m.close()
    live_path = catalog.live_db_path(root, "uc26")
    live = db.connect(str(live_path))
    db.apply_schema(live)
    _seed(live)

    clock = Clock(T0)
    c = LiveCompiler(live, None, ReplaySource([]), ALL_CLASSIFIERS, bag=FakeBag(), now=clock,
                     data_root=root, main_db=main_path)
    first = _drive_msgs(60)
    second = _drive_msgs(300, t0=T0 + 3600)

    async def go():
        c.apply_bag_state(RUNNING)
        for env in first:
            c.on_envelope(env)
        c.apply_bag_state(STOPPED)                         # run A closed
        clock.t = T0 + 3600
        c.apply_bag_state(BagState(True, True, "bag-2"))
        for env in second:
            clock.t = env.ts
            c.on_envelope(env)
            if int((env.ts - T0) * HZ) % 250 == 0:
                c._flush()
                c._tick_once()
        c._flush()
        c._tick_once()
        clock.t = second[-1].ts + 1.0
        c.heartbeat()

    asyncio.run(go())
    live.close()

    # the full upload of run A arrives later and links to it
    msgs = [(e.ts + 0.003, json.loads(e.raw)) for e in first]
    bag = write_db3(tmp_path / "a.db3", {"/spi_data": msgs})
    cat = catalog.connect_catalog(root)
    st = catalog.Staging(root, 10**10)
    with open(bag, "rb") as f:
        st.add("a.db3", f)
    rid = catalog.commit(cat, root, st, catalog.UploadFields()).recording_id
    cat.close()
    up = parser.parse_recording(root, rid)
    server = create_server(db_path=str(main_path), catalog_path=str(root.catalog_db), live_db_path=str(live_path))
    return {"server": server, "upload_run": up.run_id, "now": second[-1].ts + 1.0, "live_path": live_path}


def _call(server, name, args=None):
    result = asyncio.run(server.call_tool(name, args or {}))
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, dict) and set(structured) == {"result"}:
        return structured["result"]
    return json.loads(result.content[0].text)


def test_list_runs_merges_tiers_puts_recording_first_and_hides_linked_twins(world):
    runs = _call(world["server"], "list_runs")
    assert [r["tier"] for r in runs] == ["live", "upload"]
    rec, up = runs
    assert rec["status"] == "recording" and rec["run_id"].startswith("live-uc26-") and rec["lap_count"] >= 1
    assert up["run_id"] == world["upload_run"] and up["linked_run_id"].startswith("live-uc26-")

    every = _call(world["server"], "list_runs", {"include_live_twins": True})
    assert len(every) == 3
    twin = next(r for r in every if r["run_id"] == up["linked_run_id"])
    assert (twin["tier"], twin["status"], twin["linked_run_id"]) == ("live", "closed", world["upload_run"])


def test_run_scoped_tools_find_live_runs(world):
    runs = _call(world["server"], "list_runs")
    live_run = runs[0]["run_id"]
    laps = _call(world["server"], "list_laps", {"run_id": live_run})
    assert laps["total"] >= 1
    turn = _call(world["server"], "get_turn", {"run_id": live_run, "turn_name": "Turn A"})
    assert turn["found"] is True
    up_laps = _call(world["server"], "list_laps", {"run_id": world["upload_run"]})
    assert "total" in up_laps


def test_get_live_status_reports_the_recording_run(world, monkeypatch):
    monkeypatch.setattr("time.time", lambda: world["now"])
    s = _call(world["server"], "get_live_status")
    assert s["recording"] is True and s["run_id"].startswith("live-uc26-") and s["feed_alive"] is True
    assert s["latest"]["speed"] == pytest.approx(8.0) and s["latest"]["power_w"] == pytest.approx(400.0)
    assert s["laps_completed"] >= 1 and s["current_lap"]["lap_number"] >= 2
    assert s["current_segment"] in {"Straight 1", "Turn A", "Straight 2", "Turn B", "Straight 3", "Turn C"}
    assert s["so_far"]["distance_m"] > 1000 and s["counters"]["stored"] == 300 * HZ
    assert s["session_mode"] == "bag" and "approximate" in s["note"]


def test_get_live_status_when_the_feed_is_quiet(world, monkeypatch):
    monkeypatch.setattr("time.time", lambda: world["now"] + 120)
    s = _call(world["server"], "get_live_status")
    assert s["feed_alive"] is False and s["last_message_age_s"] == pytest.approx(121.0, abs=0.1)
    assert s["compiler"].startswith("not running")


def test_get_live_window_is_capped_and_downsampled(world):
    w = _call(world["server"], "get_live_window", {"seconds": 60, "max_points": 50})
    assert w["rows_in_window"] == pytest.approx(60 * HZ, abs=2) and len(w["rows"]) == 50 and w["downsampled"]
    ts = [r["ts"] for r in w["rows"]]
    assert ts == sorted(ts) and ts[-1] - ts[0] == pytest.approx(60, abs=0.1)
    big = _call(world["server"], "get_live_window", {"seconds": 99999, "max_points": 99999})
    assert big["seconds"] == 600 and len(big["rows"]) == 300


def test_read_only_sql_sees_the_live_db_as_live(world):
    out = _call(world["server"], "read_only_sql",
                {"query": "SELECT COUNT(*) AS n FROM live.runs WHERE status = 'recording'"})
    assert out[0]["n"] == 1


def test_without_a_live_db_everything_still_works(tmp_path):
    root = catalog.DataRoot(tmp_path / "data")
    root.ensure()
    main_path = catalog.ensure_parsed_db(root, "uc26")
    server = create_server(db_path=str(main_path), catalog_path=str(root.catalog_db))
    assert _call(server, "get_live_status")["recording"] is False
    runs = asyncio.run(server.call_tool("list_runs", {}))
    assert runs is not None
