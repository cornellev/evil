import io
import json
import sys
import time

import pytest

from evil import catalog, db, worker
from telemetry_fixtures import payload, write_db3

T0 = 1_775_000_000.0


@pytest.fixture
def root(tmp_path):
    r = catalog.DataRoot(tmp_path / "data")
    r.ensure()
    return r


@pytest.fixture
def cat(root):
    c = catalog.connect_catalog(root)
    yield c
    c.close()


def _upload(cat, root, files):
    staging = catalog.Staging(root, 10**10)
    for name, data in files.items():
        staging.add(name, io.BytesIO(data) if isinstance(data, bytes) else open(data, "rb"))
    return catalog.commit(cat, root, staging, catalog.UploadFields()).recording_id


def _bag(tmp_path, n=5, name="bag.db3", seq0=2):
    return write_db3(tmp_path / name, {"/spi_data": [(T0 + i * 0.02, payload(seq=seq0 + 2 * i)) for i in range(n)]})


def _jobs(cat, rid=None):
    q = "SELECT kind, lane, status, attempts, error FROM jobs" + (" WHERE recording_id = ?" if rid else "") + " ORDER BY job_id"
    return [tuple(r) for r in cat.execute(q, (rid,) if rid else ())]


def test_upload_queues_a_fast_scan_and_a_deep_parse(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    assert _jobs(cat, rid) == [("scan", "fast", "pending", 0, None), ("parse", "deep", "pending", 0, None)]


def test_drain_scans_then_parses_and_reports_done(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})

    ran = worker.Worker(root).drain()

    assert ran == 2
    rec = catalog.get_recording(cat, rid)
    assert rec["parse_status"] == "parsed" and rec["rows_ingested"] == 5 and rec["time_source"] == "ros-record-time"
    assert [j[2] for j in _jobs(cat, rid)] == ["done", "done"]
    assert cat.execute("SELECT progress FROM jobs WHERE kind = 'parse'").fetchone()[0] == 1


def test_a_parse_waits_for_its_recordings_scan(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    assert worker.claim_next(cat, "deep") is None                    # scan still pending
    fast = worker.claim_next(cat, "fast")
    assert fast.kind == "scan"
    assert worker.claim_next(cat, "deep") is None                    # scan running
    worker._complete(cat, fast, None)
    parse = worker.claim_next(cat, "deep")
    assert parse.kind == "parse" and parse.recording_id == rid
    assert catalog.get_recording(cat, rid)["parse_status"] == "running"


def test_jobs_are_fifo_within_a_lane_and_lanes_are_independent(tmp_path, root, cat):
    a = _upload(cat, root, {"a.db3": _bag(tmp_path, name="a.db3", seq0=2)})
    b = _upload(cat, root, {"b.db3": _bag(tmp_path, name="b.db3", seq0=100)})
    first, second = worker.claim_next(cat, "fast"), worker.claim_next(cat, "fast")
    assert (first.recording_id, second.recording_id) == (a, b)
    assert worker.claim_next(cat, "fast") is None
    # a slow parse in the deep lane does not stop another fast scan from being claimed
    worker._complete(cat, first, None)
    deep = worker.claim_next(cat, "deep")
    assert deep.recording_id == a
    c = _upload(cat, root, {"c.db3": _bag(tmp_path, name="c.db3", seq0=200)})
    assert worker.claim_next(cat, "fast").recording_id == c


def test_a_corrupt_file_fails_nothing_else(tmp_path, root, cat):
    bad = _upload(cat, root, {"broken.db3": b"SQLite format 3\x00" + b"\x00" * 100})
    good = _upload(cat, root, {"b.db3": _bag(tmp_path)})

    worker.Worker(root).drain()

    assert catalog.get_recording(cat, bad)["parse_status"] == "skipped"
    assert catalog.get_recording(cat, good)["parse_status"] == "parsed"
    assert all(j[2] == "done" for j in _jobs(cat))


def test_a_crashing_parse_process_is_retried_then_marked_failed(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    crash = lambda r, rec: [sys.executable, "-c", "import sys; sys.stderr.write('boom: segfault-ish'); sys.exit(3)"]
    w = worker.Worker(root, parse_command=crash)

    w.drain()

    jobs = _jobs(cat, rid)
    assert jobs[0][2] == "done"
    assert jobs[1][2] == "failed" and jobs[1][3] == 2 and "boom" in jobs[1][4]
    rec = catalog.get_recording(cat, rid)
    assert rec["parse_status"] == "failed" and "boom" in rec["parse_error"]
    # the whole worker is fine: a later good upload still parses
    ok = _upload(cat, root, {"c.db3": _bag(tmp_path, name="c.db3", seq0=50)})
    worker.Worker(root).drain()
    assert catalog.get_recording(cat, ok)["parse_status"] == "parsed"


def test_a_first_failure_goes_back_to_pending_for_a_retry(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    w = worker.Worker(root, parse_command=lambda r, rec: [sys.executable, "-c", "import sys; sys.exit(1)"])
    w.run_once("fast")
    assert w.run_once("deep") is True

    assert _jobs(cat, rid)[1][2:4] == ("pending", 1)
    assert catalog.get_recording(cat, rid)["parse_status"] == "pending"
    w.parse_command = worker.default_parse_command  # the retry succeeds
    assert w.run_once("deep") is True
    assert catalog.get_recording(cat, rid)["parse_status"] == "parsed"


def test_a_parse_over_its_timeout_is_killed_and_failed(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    sleeper = lambda r, rec: [sys.executable, "-c", "import time; time.sleep(60)"]
    w = worker.Worker(root, parse_timeout=0.5, heartbeat_sec=0.2, parse_command=sleeper)
    w.run_once("fast")

    t = time.monotonic()
    w.run_once("deep")

    assert time.monotonic() - t < 10
    assert "timed out" in _jobs(cat, rid)[1][4]
    assert catalog.get_recording(cat, rid)["parse_status"] == "pending"   # one attempt left


def test_heartbeat_and_progress_are_written_while_a_parse_runs(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    script = "import time\nprint('PROGRESS 0.5', flush=True)\ntime.sleep(1.2)"
    w = worker.Worker(root, heartbeat_sec=0.3, parse_command=lambda r, rec: [sys.executable, "-c", script])
    w.run_once("fast")
    seen = []

    import threading
    t = threading.Thread(target=lambda: w.run_once("deep"))
    t.start()
    c2 = catalog.connect_catalog(root)
    for _ in range(30):
        row = c2.execute("SELECT status, progress, heartbeat_at, started_at FROM jobs WHERE kind = 'parse'").fetchone()
        if row["status"] == "running" and row["progress"] and row["progress"] > 0:
            seen.append(tuple(row))
            break
        time.sleep(0.1)
    t.join()
    c2.close()
    assert seen and seen[0][1] == 0.5 and seen[0][2] >= seen[0][3]


def test_a_killed_worker_resumes_its_queue(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    # a worker claimed both jobs and then died: running, heartbeat long ago
    long_ago = time.time() - 3600
    with cat:
        cat.execute("UPDATE jobs SET status = 'running', attempts = 1, heartbeat_at = ?, started_at = ?", (long_ago, long_ago))
        cat.execute("UPDATE recordings SET parse_status = 'running' WHERE recording_id = ?", (rid,))

    w = worker.Worker(root)
    assert worker.reset_stale(cat) == 2
    w.drain()

    assert catalog.get_recording(cat, rid)["parse_status"] == "parsed"
    assert [j[2] for j in _jobs(cat, rid)] == ["done", "done"]


def test_reset_stale_leaves_fresh_jobs_alone_and_fails_exhausted_ones(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    now = time.time()
    with cat:
        cat.execute("UPDATE jobs SET status = 'running', attempts = 1, heartbeat_at = ? WHERE kind = 'scan'", (now,))
        cat.execute("UPDATE jobs SET status = 'running', attempts = 2, heartbeat_at = ? WHERE kind = 'parse'", (now - 3600,))
    assert worker.reset_stale(cat) == 1
    jobs = {j[0]: j for j in _jobs(cat, rid)}
    assert jobs["scan"][2] == "running" and jobs["parse"][2] == "failed"
    assert catalog.get_recording(cat, rid)["parse_status"] == "failed"


def test_reparse_requests_are_idempotent_and_rerun_the_parse(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    worker.Worker(root).drain()
    first_run = catalog.get_recording(cat, rid)["run_id"]

    j1 = catalog.request_reparse(cat, rid)
    j2 = catalog.request_reparse(cat, rid)
    assert j1 == j2 and catalog.get_recording(cat, rid)["parse_status"] == "pending"
    assert catalog.request_reparse(cat, "missing") is None
    worker.Worker(root).drain()

    rec = catalog.get_recording(cat, rid)
    assert rec["parse_status"] == "parsed" and rec["run_id"] == first_run and rec["rows_ingested"] == 5
    p = db.connect(str(catalog.parsed_db_path(root, "uc26")))
    assert p.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 5


def test_adding_a_location_relabels_existing_recordings(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    worker.Worker(root).drain()
    assert catalog.get_recording(cat, rid)["location_id"] is None

    lid = catalog.add_location(cat, "B-lot", 42.0, -76.0, 300)

    rec = catalog.get_recording(cat, rid)
    assert rec["location_id"] == lid and rec["location_method"] == "gps-match"
    with pytest.raises(catalog.CatalogError):
        catalog.add_location(cat, "", 1, 1, 1)
    with pytest.raises(catalog.CatalogError):
        catalog.add_location(cat, "x", 99, 1, 1)
    assert catalog.add_location(cat, "B-lot", 42.0, -76.0, 1) == lid       # upsert by name; now out of range
    assert catalog.get_recording(cat, rid)["location_id"] == lid           # (box centre is exactly on it)


def test_system_status_reports_queue_and_resources(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    w = worker.Worker(root)
    w.run_once("fast")

    status = catalog.system_status(cat, root)

    assert status["jobs"]["counts"] == {"pending": 1, "running": 0, "done": 1, "failed": 0}
    assert [q["status"] for q in status["jobs"]["queue"]] == ["pending", "done"]
    assert status["jobs"]["queue"][0]["kind"] == "parse" and status["jobs"]["queue"][0]["size_bytes"] > 0
    assert status["jobs"]["oldest_pending_age_sec"] >= 0
    assert status["system"]["disk"]["free_bytes"] > 0 and status["system"]["cpu_count"] >= 1
    json.dumps(status)  # serializable


def test_starting_the_worker_requeues_jobs_a_dead_instance_left_running(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path)})
    with cat:  # claimed a second ago by an instance that was then killed: heartbeat is fresh
        cat.execute("UPDATE jobs SET status = 'running', attempts = 1, heartbeat_at = ? WHERE kind = 'scan'", (time.time(),))

    w = worker.Worker(root)
    threads = w.start()
    try:
        for _ in range(100):
            if catalog.get_recording(cat, rid)["parse_status"] == "parsed":
                break
            time.sleep(0.2)
    finally:
        w.stop.set()
        for t in threads:
            t.join(timeout=10)

    assert catalog.get_recording(cat, rid)["parse_status"] == "parsed"
