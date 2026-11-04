"""Live sessions (live-next-steps-2026-11.md section C): RED's record button via the bag
service, gap fallback, DAQ reboot, restart resume, export as a live capture, and linking
to the full upload. Driven step by step (apply_bag_state / on_envelope / check_gap) with a
fake clock, so every boundary is deterministic."""

from __future__ import annotations

import asyncio
import gzip
import http.server
import json
import threading

import pytest

from evil import catalog, db, parser
from evil.bag_service import BagServiceClient, BagState, parse_status
from evil.ingestion.base import decode_json
from evil.ingestion.replay_source import ReplaySource
from evil.registered_classifiers import ALL_CLASSIFIERS
from evil.scripts.run_live_compiler import LiveCompiler
from telemetry_fixtures import payload, write_db3

T0 = 1_775_000_000.0
RUNNING_A = BagState(True, True, "bag-aaa")
RUNNING_B = BagState(True, True, "bag-bbb")
STOPPED = BagState(True, False, None)
DOWN = BagState(False, detail="connection refused")


class Clock:
    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t


def _msg(i, t=None, seq=None, gts=None):
    t = T0 + i * 0.02 if t is None else t
    p = payload(seq=2 + 2 * i if seq is None else seq, publish_ns=int(t * 1e9))
    if gts is not None:
        p["global_ts"] = gts
    return decode_json(json.dumps(p), t)


class FakeBag:
    async def poll(self):
        return STOPPED


def _compiler(conn, clock, bag=True, **kw):
    return LiveCompiler(conn, None, ReplaySource([]), ALL_CLASSIFIERS, lock_after=3, batch_s=0.01,
                        bag=FakeBag() if bag else None, now=clock, **kw)


def _drive(fn):
    """Run fn(loop) inside an event loop (the compiler's batch timer needs one)."""
    async def go():
        return fn()
    return asyncio.run(go())


def _runs(conn):
    return [dict(r) for r in conn.execute("SELECT * FROM runs ORDER BY rowid")]


def _rows(conn, run_id):
    return conn.execute("SELECT COUNT(*) FROM main_snapshot WHERE run_id = ?", (run_id,)).fetchone()[0]


# ---- bag mode ------------------------------------------------------------

def test_a_live_run_is_exactly_the_bag_recording(conn):
    clock = Clock()
    c = _compiler(conn, clock)

    def steps():
        c.apply_bag_state(STOPPED)
        for i in range(5):
            c.on_envelope(_msg(i))                 # car talking, nobody pressed record: not stored
        assert c.run_id is None
        c.apply_bag_state(RUNNING_A)               # RED's record button
        run = c.run_id
        for i in range(5, 15):
            c.on_envelope(_msg(i))
        c.apply_bag_state(RUNNING_A)               # still the same recording
        c.apply_bag_state(STOPPED)
        for i in range(15, 20):
            c.on_envelope(_msg(i))
        return run

    run = _drive(steps)
    assert run.startswith("live-uc26-") and c.run_id is None
    (r,) = _runs(conn)
    assert (r["run_id"], r["status"], r["end_reason"], r["session_mode"], r["bag_id"]) == \
        (run, "closed", "bag_stopped", "bag", "bag-aaa")
    assert r["schema_id"] == "evil.telemetry.v1" and _rows(conn, run) == 10
    assert r["car_start"] == pytest.approx(T0 + 5 * 0.02) and r["car_end"] == pytest.approx(T0 + 14 * 0.02)
    assert json.loads(r["stats_json"])["stored"] == 10
    assert conn.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 10


def test_a_bag_restarted_between_polls_is_two_runs(conn):
    clock = Clock()
    c = _compiler(conn, clock)

    def steps():
        c.apply_bag_state(RUNNING_A)
        for i in range(5):
            c.on_envelope(_msg(i))
        clock.t += 1                             # names are per second
        c.apply_bag_state(RUNNING_B)
        for i in range(5, 10):
            c.on_envelope(_msg(i))
        c.apply_bag_state(STOPPED)

    _drive(steps)
    a, b = _runs(conn)
    assert (a["bag_id"], a["end_reason"], b["bag_id"], b["end_reason"]) == ("bag-aaa", "bag_stopped", "bag-bbb",
                                                                           "bag_stopped")
    assert _rows(conn, a["run_id"]) == 5 and _rows(conn, b["run_id"]) == 5


def test_an_unreachable_bag_service_never_drops_a_recording(conn):
    clock = Clock()
    c = _compiler(conn, clock, gap_s=60)

    def steps():
        c.apply_bag_state(RUNNING_A)
        c.on_envelope(_msg(0))
        for _ in range(5):
            c.apply_bag_state(DOWN)              # car's Tailscale API gone
        assert c.counters()["bag"]["reachable"] is False and c.run_id is not None
        for i in range(1, 10):
            c.on_envelope(_msg(i))
        clock.t += 30
        c.check_gap()                            # 30 s quiet: still recording
        assert c.run_id is not None
        c.apply_bag_state(STOPPED)               # back, and the bag was stopped meanwhile
        return c.run_id

    assert _drive(steps) is None
    (r,) = _runs(conn)
    assert r["end_reason"] == "bag_stopped" and _rows(conn, r["run_id"]) == 10


def test_with_the_bag_service_down_sessions_open_on_data_and_close_on_silence(conn):
    clock = Clock()
    c = _compiler(conn, clock, gap_s=60)

    def steps():
        for _ in range(3):
            c.apply_bag_state(DOWN)
        clock.t = T0
        for i in range(10):
            c.on_envelope(_msg(i))
        clock.t = T0 + 30
        c.check_gap()
        assert c.run_id is not None
        clock.t = T0 + 61
        c.check_gap()

    _drive(steps)
    (r,) = _runs(conn)
    assert (r["session_mode"], r["end_reason"], r["bag_id"]) == ("gap", "gap", None)


def test_a_gap_session_is_adopted_when_the_bag_service_comes_back_running(conn):
    clock = Clock()
    c = _compiler(conn, clock)

    def steps():
        for _ in range(3):
            c.apply_bag_state(DOWN)
        for i in range(5):
            c.on_envelope(_msg(i))
        run = c.run_id
        c.apply_bag_state(RUNNING_A)
        assert c.run_id == run                   # the same recording, now known by its bag
        c.apply_bag_state(STOPPED)
        return run

    run = _drive(steps)
    (r,) = _runs(conn)
    assert (r["run_id"], r["session_mode"], r["bag_id"], r["end_reason"]) == (run, "bag", "bag-aaa", "bag_stopped")


def test_without_a_bag_service_it_is_gap_mode(conn):
    clock = Clock()
    c = _compiler(conn, clock, bag=False, gap_s=60)

    def steps():
        for i in range(5):
            c.on_envelope(_msg(i))
        clock.t += 61
        c.check_gap()
        for i in range(5, 10):
            c.on_envelope(_msg(i, t=T0 + 100 + i * 0.02))

    _drive(steps)
    a, b = _runs(conn)
    assert a["end_reason"] == "gap" and b["status"] == "recording"


# ---- splits --------------------------------------------------------------

def test_a_daq_reboot_starts_a_new_run_in_the_same_recording(conn):
    clock = Clock()
    c = _compiler(conn, clock)

    def steps():
        c.apply_bag_state(RUNNING_A)
        for i in range(10):
            c.on_envelope(_msg(i, seq=50_000 + 2 * i, gts=900_000_000 + i * 20_000))
        clock.t += 1
        for i in range(10, 20):                  # DAQ came back: seq and global_ts restart near zero
            c.on_envelope(_msg(i, seq=2 * (i - 10), gts=5_000_000 + (i - 10) * 20_000))
        c.apply_bag_state(STOPPED)

    _drive(steps)
    a, b = _runs(conn)
    assert (a["end_reason"], b["end_reason"]) == ("daq_reboot", "bag_stopped")
    assert a["bag_id"] == b["bag_id"] == "bag-aaa"
    assert _rows(conn, a["run_id"]) == 10 and _rows(conn, b["run_id"]) == 10


def test_a_schema_change_moves_the_held_messages_to_the_new_run(conn, tmp_path):
    from telemetry_fixtures import v2_payload
    clock = Clock()
    c = _compiler(conn, clock, switch_after=3)

    def steps():
        c.apply_bag_state(RUNNING_A)
        for i in range(6):
            c.on_envelope(_msg(i))
        clock.t += 1
        for i in range(6, 12):
            t = T0 + i * 0.02
            c.on_envelope(decode_json(json.dumps(v2_payload(seq=2 + 2 * i, publish_ns=int(t * 1e9))), t))
        c.apply_bag_state(STOPPED)

    _drive(steps)
    a, b = _runs(conn)
    assert (a["schema_id"], a["end_reason"], b["schema_id"]) == ("evil.telemetry.v1", "schema_change",
                                                                 "evil.telemetry.v2")
    raw = dict(conn.execute("SELECT run_id, COUNT(*) FROM live_raw GROUP BY run_id").fetchall())
    assert raw == {a["run_id"]: 6, b["run_id"]: 6}           # no data root: raw kept, split exactly
    assert _rows(conn, a["run_id"]) == 6 and _rows(conn, b["run_id"]) == 6


# ---- restart -------------------------------------------------------------

def test_a_restart_mid_recording_resumes_the_same_run_without_a_duplicate(tmp_path):
    path = str(tmp_path / "live.db")
    clock = Clock()
    conn = db.connect(path)
    c1 = _compiler(conn, clock)

    def first():
        c1.apply_bag_state(RUNNING_A)
        for i in range(10):
            c1.on_envelope(_msg(i))
        c1._flush()                              # SIGTERM path: committed, run left 'recording'
        return c1.run_id

    run = _drive(first)
    conn.close()

    conn = db.connect(path)
    c2 = _compiler(conn, clock)

    def second():
        c2.start(RUNNING_A)                      # same bag still recording
        assert c2.run_id == run
        again = payload(seq=2 + 2 * 9, publish_ns=int((T0 + 9 * 0.02 + 0.005) * 1e9))   # repeat of the last one
        c2.on_envelope(decode_json(json.dumps(again), T0 + 1))
        for i in range(10, 15):
            c2.on_envelope(_msg(i))
        c2.apply_bag_state(STOPPED)

    _drive(second)
    (r,) = _runs(conn)
    assert (r["run_id"], r["end_reason"]) == (run, "bag_stopped")
    assert _rows(conn, run) == 15                                  # the repeat was dropped
    seqs = [s for (s,) in conn.execute("SELECT device_seq FROM main_snapshot ORDER BY seq")]
    assert seqs == sorted(set(seqs))


def test_a_restart_after_the_recording_ended_closes_the_run_as_interrupted(tmp_path):
    path = str(tmp_path / "live.db")
    clock = Clock()
    conn = db.connect(path)
    c1 = _compiler(conn, clock)

    def first():
        c1.apply_bag_state(RUNNING_A)
        for i in range(10):
            c1.on_envelope(_msg(i))
        c1._flush()

    _drive(first)
    conn.close()
    conn = db.connect(path)
    c2 = _compiler(conn, clock)

    def second():
        c2.start(RUNNING_B)                      # a different recording is running now

    _drive(second)
    a, b = _runs(conn)
    assert (a["end_reason"], a["status"], b["bag_id"], b["status"]) == ("interrupted", "closed", "bag-bbb",
                                                                        "recording")
    assert conn.execute("SELECT COUNT(*) FROM laps WHERE run_id = ?", (a["run_id"],)).fetchone()[0] == 0


# ---- export, reparse, linking -------------------------------------------

@pytest.fixture
def root(tmp_path):
    r = catalog.DataRoot(tmp_path / "data")
    r.ensure()
    return r


def _live_conn(root):
    path = catalog.live_db_path(root, "uc26")
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(str(path))
    db.apply_schema(conn)
    return conn


def test_a_closed_run_is_exported_as_a_live_capture_and_reparses_to_the_same_rows(root):
    conn = _live_conn(root)
    clock = Clock()
    c = _compiler(conn, clock, data_root=root, main_db=catalog.parsed_db_path(root, "uc26"))
    msgs = [_msg(i) for i in range(40)]
    msgs.insert(20, decode_json("{broken", T0 + 0.39))

    def steps():
        c.apply_bag_state(RUNNING_A)
        for m in msgs:
            c.on_envelope(m)
        c.apply_bag_state(STOPPED)

    _drive(steps)
    (r,) = _runs(conn)
    cat = catalog.connect_catalog(root)
    rec = catalog.get_recording(cat, r["recording_id"])
    assert (rec["kind"], rec["container"], rec["run_id"], rec["parse_status"]) == \
        ("live_capture", "live-capture", r["run_id"], "parsed")
    assert (rec["rows_ingested"], rec["rows_rejected"], rec["schema_id"]) == (40, 1, "evil.telemetry.v1")
    assert rec["target_db"] == "evil_uc26_live.db"
    assert conn.execute("SELECT COUNT(*) FROM live_raw").fetchone()[0] == 0          # moved to the raw store
    capture = root.raw / rec["files"][0]["rel_path"]
    with gzip.open(capture, "rt") as f:
        lines = f.read().splitlines()
    assert len(lines) == 41 and lines[20].endswith("\t{broken")

    before = conn.execute("SELECT device_seq, global_ts, filtered_speed FROM main_snapshot ORDER BY seq").fetchall()
    conn.close()
    res = parser.parse_recording(root, rec["recording_id"])                           # a reparse of the capture
    assert res.status == "parsed" and res.run_id == r["run_id"] and res.rows_rejected == 1
    conn = _live_conn(root)
    after = conn.execute("SELECT device_seq, global_ts, filtered_speed FROM main_snapshot ORDER BY seq").fetchall()
    assert [tuple(x) for x in after] == [tuple(x) for x in before]
    r2 = dict(conn.execute("SELECT * FROM runs").fetchone())
    assert (r2["tier"], r2["end_reason"], r2["bag_id"]) == ("live", "bag_stopped", "bag-aaa")   # live facts kept


def _upload_bag(root, tmp_path, n, t0):
    msgs = [(t0 + i * 0.02 + 0.003, payload(seq=2 + 2 * i, publish_ns=int((t0 + i * 0.02) * 1e9))) for i in range(n)]
    bag = write_db3(tmp_path / "bag.db3", {"/spi_data": msgs})
    cat = catalog.connect_catalog(root)
    staging = catalog.Staging(root, 10**10)
    with open(bag, "rb") as f:
        staging.add("bag.db3", f)
    rid = catalog.commit(cat, root, staging, catalog.UploadFields()).recording_id
    cat.close()
    return rid


def test_an_upload_parsed_after_the_live_run_links_both_ways(root, tmp_path):
    conn = _live_conn(root)
    c = _compiler(conn, Clock(), data_root=root, main_db=catalog.parsed_db_path(root, "uc26"))

    def steps():
        c.apply_bag_state(RUNNING_A)
        for i in range(10, 90):                  # live joined a little late
            c.on_envelope(_msg(i))
        c.apply_bag_state(STOPPED)

    _drive(steps)
    live_run = _runs(conn)[0]
    upload_rid = _upload_bag(root, tmp_path, 100, T0)
    res = parser.parse_recording(root, upload_rid)

    main = db.connect(str(catalog.parsed_db_path(root, "uc26")))
    up = dict(main.execute("SELECT * FROM runs WHERE run_id = ?", (res.run_id,)).fetchone())
    assert (up["tier"], up["linked_run_id"]) == ("upload", live_run["run_id"])
    assert conn.execute("SELECT linked_run_id FROM runs").fetchone()[0] == res.run_id
    cat = catalog.connect_catalog(root)
    assert catalog.get_recording(cat, upload_rid)["linked_recording_id"] == live_run["recording_id"]
    assert catalog.get_recording(cat, live_run["recording_id"])["linked_recording_id"] == upload_rid


def test_a_live_run_closing_after_its_upload_links_too(root, tmp_path):
    upload_rid = _upload_bag(root, tmp_path, 100, T0)
    res = parser.parse_recording(root, upload_rid)
    conn = _live_conn(root)
    c = _compiler(conn, Clock(), data_root=root, main_db=catalog.parsed_db_path(root, "uc26"))

    def steps():
        c.apply_bag_state(RUNNING_A)
        for i in range(0, 100):
            c.on_envelope(_msg(i))
        c.apply_bag_state(STOPPED)

    _drive(steps)
    assert conn.execute("SELECT linked_run_id FROM runs").fetchone()[0] == res.run_id


def test_a_different_drive_is_not_linked(root, tmp_path):
    conn = _live_conn(root)
    c = _compiler(conn, Clock(), data_root=root, main_db=catalog.parsed_db_path(root, "uc26"))

    def steps():
        c.apply_bag_state(RUNNING_A)
        for i in range(50):
            c.on_envelope(_msg(i))
        c.apply_bag_state(STOPPED)

    _drive(steps)
    parser.parse_recording(root, _upload_bag(root, tmp_path, 100, T0 + 3600))
    assert conn.execute("SELECT linked_run_id FROM runs").fetchone()[0] is None


def test_reference_data_is_copied_from_the_main_db(root):
    from evil.scripts.run_live_compiler import _copy_reference_data
    from track_fixtures import seed_ring
    main = db.connect(str(catalog.ensure_parsed_db(root, "uc26")))
    seed_ring(main)
    main.execute("INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (42, -76, 10)")
    main.commit()
    live = _live_conn(root)
    _copy_reference_data(catalog.parsed_db_path(root, "uc26"), live)
    _copy_reference_data(catalog.parsed_db_path(root, "uc26"), live)                   # idempotent
    assert live.execute("SELECT COUNT(*) FROM track_segments").fetchone()[0] == 6
    assert live.execute("SELECT COUNT(*) FROM start_finish_line").fetchone()[0] == 1


# ---- the bag service client ---------------------------------------------

def test_parse_status_matches_the_bag_service_api():
    running = json.dumps({"id": "abc123", "name": "rosbag", "status": "running"}).encode()
    exited = json.dumps({"id": "abc123", "name": "rosbag", "status": "exited"}).encode()
    assert parse_status(200, running) == BagState(True, True, "abc123", "running")
    assert parse_status(200, exited) == BagState(True, False, None, "exited")
    assert parse_status(404, b'{"detail": "container not found: rosbag"}').running is False
    assert parse_status(404, b"").reachable is True
    assert parse_status(500, b'{"detail": "docker error"}').reachable is False


def test_the_client_talks_http():
    state = {"code": 200, "body": {"id": "c1", "name": "rosbag", "status": "running"}}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == "/bag/status"
            body = json.dumps(state["body"]).encode()
            self.send_response(state["code"])
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        client = BagServiceClient(f"http://127.0.0.1:{server.server_port}/")
        assert asyncio.run(client.poll()) == BagState(True, True, "c1", "running")
        state.update(code=404, body={"detail": "container not found: rosbag"})
        assert asyncio.run(client.poll()).running is False
    finally:
        server.shutdown()
    dead = BagServiceClient(f"http://127.0.0.1:{server.server_port}", timeout_s=0.5)
    assert asyncio.run(dead.poll()).reachable is False
