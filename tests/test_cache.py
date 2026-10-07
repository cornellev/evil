import io
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from fastapi.testclient import TestClient

from evil import catalog, worker
from evil.upload_server import app
from telemetry_fixtures import payload, write_csv, write_db3

T0 = 1_775_000_000.0


class FakeCat:
    """Stands in for CAT's pyworker: records every call, can be told to fail or stall."""

    def __init__(self):
        self.builds, self.deletes, self.progress_calls = [], [], 0
        self.fail_build = None            # (status, detail)
        self.fail_delete = False
        self.build_delay = 0.0
        self.result = {"messages": 42, "bytes": 1000, "topics": [
            {"name": "/spi_data", "type": "std_msgs/msg/String", "count": 42, "decoded": True},
            {"name": "/odd", "type": "weird_pkgs/msg/Thing", "count": 3, "decoded": False}],
            "opaque_topics": ["/odd"]}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, body):
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                outer.builds.append(body)
                time.sleep(outer.build_delay)
                if outer.fail_build:
                    return self._send(*outer.fail_build)
                self._send(200, {"recording_id": body["recording_id"], **outer.result})

            def do_GET(self):
                outer.progress_calls += 1
                self._send(200, {"fraction": 0.5, "messages": 21})

            def do_DELETE(self):
                outer.deletes.append(self.path)
                if outer.fail_delete:
                    return self._send(500, {"detail": "db down"})
                self._send(200, {"deleted_messages": 42})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


@pytest.fixture
def fake():
    f = FakeCat()
    yield f
    f.close()


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


def _store(cat, root, files, **fields):
    staging = catalog.Staging(root, 10**10)
    for name, data in files.items():
        staging.add(name, io.BytesIO(data) if isinstance(data, bytes) else open(data, "rb"))
    return catalog.commit(cat, root, staging, catalog.UploadFields(**fields)).recording_id


@pytest.fixture
def bag(tmp_path, root, cat):
    path = write_db3(tmp_path / "bag.db3", {"/spi_data": [(T0 + i, payload(seq=2 + 2 * i)) for i in range(5)]})
    return _store(cat, root, {"bag/bag.db3": path, "bag/metadata.yaml": b"x: 1"}, label="my bag", category="testing")


def _worker(root, fake, **kw):
    return worker.Worker(root, cat_url=fake.url, heartbeat_sec=0.1, **kw)


# ---- states and requests -----------------------------------------------------

def test_a_recording_starts_absent_and_queues_one_build_on_request(cat, root, bag):
    assert catalog.cache_state(cat, bag)["state"] == "absent"

    first = catalog.request_cache(cat, bag)
    second = catalog.request_cache(cat, bag)

    assert first["state"] == "queued" and second["state"] == "queued"
    jobs = cat.execute("SELECT kind, lane, status FROM jobs WHERE kind = 'cache_build'").fetchall()
    assert [tuple(j) for j in jobs] == [("cache_build", "cache", "pending")]       # not queued twice


def test_unknown_unreadable_and_non_cacheable_recordings_are_refused(cat, root, tmp_path):
    with pytest.raises(KeyError):
        catalog.request_cache(cat, "nope")
    mp4 = _store(cat, root, {"clip.mp4": b"\x00\x00\x00\x18ftyp"})
    with pytest.raises(catalog.CatalogError, match="cannot be opened in CAT"):
        catalog.request_cache(cat, mp4)
    broken = _store(cat, root, {"broken.db3": b"SQLite format 3\x00" + b"\x00" * 200})
    with cat:
        cat.execute("UPDATE recordings SET parse_error = 'container unreadable: x' WHERE recording_id = ?", (broken,))
    with pytest.raises(catalog.CatalogError):
        catalog.request_cache(cat, broken)
    assert catalog.cache_state(cat, broken)["cacheable"] is False


def test_the_worker_builds_through_cats_pyworker_and_the_entry_becomes_ready(cat, root, bag, fake):
    catalog.request_cache(cat, bag)

    assert _worker(root, fake).run_once("cache") is True

    st = catalog.cache_state(cat, bag)
    assert st["state"] == "ready" and st["size_bytes"] == 1000 and st["messages"] == 42
    assert [t["name"] for t in st["detail"]["topics"]] == ["/spi_data", "/odd"] and st["detail"]["opaque_topics"] == ["/odd"]
    (call,) = fake.builds
    assert call["recording_id"] == bag and call["kind"] == "bag" and call["name"] == "my bag"
    assert sorted(f.rsplit("/", 1)[-1] for f in call["files"]) == ["bag.db3", "metadata.yaml"]
    assert all(not f.startswith("/") and ".." not in f for f in call["files"])       # relative paths only
    assert cat.execute("SELECT status FROM jobs WHERE kind = 'cache_build'").fetchone()[0] == "done"
    assert catalog.request_cache(cat, bag)["state"] == "ready"                       # a second open builds nothing
    assert len(fake.builds) == 1 and cat.execute("SELECT COUNT(*) FROM jobs WHERE kind = 'cache_build'").fetchone()[0] == 1


def test_a_csv_recording_is_cached_as_a_csv(tmp_path, cat, root, fake):
    path = write_csv(tmp_path / "x.csv", [(T0 + i, payload(seq=2 + 2 * i)) for i in range(3)])
    rid = _store(cat, root, {"0409.csv": path})
    catalog.request_cache(cat, rid)
    _worker(root, fake).run_once("cache")
    assert fake.builds[0]["kind"] == "csv" and fake.builds[0]["files"][0].endswith("0409.csv")


def test_a_failed_build_is_retried_then_reported_with_its_error(cat, root, bag, fake):
    fake.fail_build = (500, {"detail": "no module named weird_pkgs"})
    catalog.request_cache(cat, bag)
    w = _worker(root, fake)

    w.run_once("cache")
    mid = catalog.cache_state(cat, bag)
    w.run_once("cache")
    end = catalog.cache_state(cat, bag)

    assert mid["state"] == "building" and "retrying" in mid["error"]
    assert end["state"] == "failed" and "weird_pkgs" in end["error"]
    assert catalog.cache_state(cat, bag)["state"] != "ready"
    fake.fail_build = None                                                          # opening again just retries
    assert catalog.request_cache(cat, bag)["state"] == "queued"
    w.run_once("cache")
    assert catalog.cache_state(cat, bag)["state"] == "ready"


def test_an_unreachable_pyworker_fails_the_job_clearly(cat, root, bag):
    catalog.request_cache(cat, bag)
    import socket
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    dead_port = sock.getsockname()[1]
    sock.close()                                    # nothing listens here any more: connection refused
    w = worker.Worker(root, cat_url=f"http://127.0.0.1:{dead_port}", heartbeat_sec=0.1)
    w.run_once("cache")
    w.run_once("cache")
    assert "unreachable or timed out" in catalog.cache_state(cat, bag)["error"]
    unset = worker.Worker(root, cat_url="", heartbeat_sec=0.1)
    catalog.request_cache(cat, bag)
    unset.run_once("cache")
    assert "EVIL_CAT_PYWORKER_URL" in cat.execute("SELECT error FROM jobs WHERE kind='cache_build' ORDER BY job_id DESC").fetchone()[0]


def test_progress_is_mirrored_from_the_pyworker_while_it_builds(cat, root, bag, fake):
    fake.build_delay = 0.6
    catalog.request_cache(cat, bag)
    w = _worker(root, fake)
    t = threading.Thread(target=lambda: w.run_once("cache"))
    t.start()
    seen = None
    for _ in range(40):
        st = catalog.cache_state(catalog.connect_catalog(root), bag)
        if st["state"] == "building" and st["progress"]:
            seen = st
            break
        time.sleep(0.05)
    t.join()
    assert seen and seen["progress"] == 0.5 and fake.progress_calls >= 1


def test_the_cache_lane_never_blocks_parsing(tmp_path, cat, root, bag, fake):
    fake.build_delay = 1.0
    catalog.request_cache(cat, bag)
    w = _worker(root, fake)
    t = threading.Thread(target=lambda: w.run_once("cache"))
    t.start()
    time.sleep(0.2)
    start = time.monotonic()
    assert w.run_once("fast") is True and w.run_once("deep") is True                  # scan + parse while a build is in flight
    assert time.monotonic() - start < 0.9
    t.join()
    assert catalog.get_recording(cat, bag)["parse_status"] == "parsed"


# ---- eviction -------------------------------------------------------------------

def _entry(cat, rid, size, accessed):
    catalog.record_cache_built(cat, rid, size, 1, None, now=accessed)
    catalog.touch_cache(cat, rid, now=accessed)


def _three(cat, root):
    ids = [_store(cat, root, {f"{i}.csv": f"a\n{i}\n".encode()}, label=f"r{i}") for i in range(3)]
    return ids


def test_idle_entries_are_evicted_after_a_week_only(cat, root):
    a, b, c = _three(cat, root)
    now = 10 * 86400.0
    _entry(cat, a, 10, now - 8 * 86400)      # idle 8 days
    _entry(cat, b, 10, now - 6 * 86400)      # idle 6 days
    _entry(cat, c, 10, now - 60)
    assert catalog.plan_eviction(cat, now=now, max_age=7 * 86400, max_bytes=10**9) == [a]


def test_over_the_size_cap_the_least_recently_used_go_first(cat, root):
    a, b, c = _three(cat, root)
    now = 10 * 86400.0
    _entry(cat, a, 100, now - 3000)
    _entry(cat, b, 100, now - 2000)
    _entry(cat, c, 100, now - 1000)
    assert catalog.plan_eviction(cat, now=now, max_age=10**9, max_bytes=250) == [a]
    assert catalog.plan_eviction(cat, now=now, max_age=10**9, max_bytes=120) == [a, b]
    assert catalog.plan_eviction(cat, now=now, max_age=10**9, max_bytes=10**9) == []
    assert catalog.plan_eviction(cat, now=now, max_age=10**9, max_bytes=0) == [a, b, c]


def test_touching_an_entry_keeps_it_alive(cat, root):
    a, b, _ = _three(cat, root)
    now = 20 * 86400.0
    _entry(cat, a, 10, now - 9 * 86400)
    _entry(cat, b, 10, now - 9 * 86400)
    assert catalog.touch_cache(cat, a, now=now) is True
    assert catalog.touch_cache(cat, "nope") is False
    assert catalog.plan_eviction(cat, now=now, max_age=7 * 86400, max_bytes=10**9) == [b]


def test_eviction_deletes_in_cat_first_and_forgets_only_on_success(cat, root, fake):
    a, b, _ = _three(cat, root)
    _entry(cat, a, 10, 1.0)
    _entry(cat, b, 10, 2.0)
    w = _worker(root, fake)

    fake.fail_delete = True                                       # CAT's database is down
    assert w.evict_cache(now=10 * 86400.0) == []
    assert catalog.cache_state(cat, a)["state"] == "ready"        # kept, retried next sweep

    fake.fail_delete = False
    assert w.evict_cache(now=10 * 86400.0) == [a, b]
    assert catalog.cache_state(cat, a)["state"] == "absent" and catalog.cache_state(cat, b)["state"] == "absent"
    assert [p.split("?")[0] for p in fake.deletes[-2:]] == [f"/cache/{a}", f"/cache/{b}"]
    assert "kind=csv" in fake.deletes[-1]


def test_an_evicted_recording_is_rebuilt_on_demand(cat, root, bag, fake):
    catalog.request_cache(cat, bag)
    w = _worker(root, fake)
    w.run_once("cache")
    w.evict_cache(now=time.time() + 30 * 86400)
    assert catalog.cache_state(cat, bag)["state"] == "absent"
    assert catalog.request_cache(cat, bag)["state"] == "queued"
    w.run_once("cache")
    assert catalog.cache_state(cat, bag)["state"] == "ready" and len(fake.builds) == 2


# ---- CAT's listing and the HTTP API ----------------------------------------------

def test_cat_lists_bags_and_csvs_with_names_and_cache_state(tmp_path, cat, root, bag, fake):
    csvp = write_csv(tmp_path / "x.csv", [(T0 + i, payload(seq=2 + 2 * i)) for i in range(3)])
    csv_id = _store(cat, root, {"drive.csv": csvp})
    _store(cat, root, {"clip.mp4": b"\x00\x00\x00\x18ftyp"})
    catalog.request_cache(cat, bag)
    _worker(root, fake).run_once("cache")

    rows = catalog.cat_recordings(cat)
    by_id = {r["recording_id"]: r for r in rows}

    assert set(by_id) == {bag, csv_id}                                  # the mp4 is not listed
    assert by_id[bag]["name"] == "my bag" and by_id[bag]["kind"] == "bag" and by_id[bag]["cache_state"] == "ready"
    assert by_id[csv_id]["name"] == "drive.csv" and by_id[csv_id]["cache_state"] == "absent"
    assert [r["recording_id"] for r in catalog.cat_recordings(cat, kind="csv")] == [csv_id]


def test_http_endpoints(tmp_path, monkeypatch, fake):
    monkeypatch.setenv("EVIL_DATA_ROOT", str(tmp_path / "data"))
    client = TestClient(app)
    path = write_db3(tmp_path / "b.db3", {"/spi_data": [(T0 + i, payload(seq=2 + 2 * i)) for i in range(3)]})
    rid = client.post("/recordings", files=[("files", ("b.db3", open(path, "rb"), "application/octet-stream"))],
                      data={"label": "http bag"}).json()["recording_id"]

    listed = client.get("/cat/recordings").json()
    assert [(r["recording_id"], r["cache_state"], r["name"]) for r in listed] == [(rid, "absent", "http bag")]
    assert client.get("/cat/recordings", params={"kind": "nope"}).status_code == 400

    queued = client.post(f"/recordings/{rid}/cache")
    assert queued.status_code == 202 and queued.json()["state"] == "queued"
    assert client.get(f"/recordings/{rid}/cache").json()["state"] == "queued"
    assert client.post("/recordings/nope/cache").status_code == 404
    assert client.get("/recordings/nope/cache").status_code == 404
    mp4 = client.post("/recordings", files=[("files", ("c.mp4", b"\x00\x00\x00\x18ftyp", "video/mp4"))]).json()["recording_id"]
    assert client.post(f"/recordings/{mp4}/cache").status_code == 409

    root = catalog.DataRoot(tmp_path / "data")
    w = worker.Worker(root, cat_url=fake.url, heartbeat_sec=0.1)
    w.run_once("cache")
    assert client.get(f"/recordings/{rid}/cache").json()["state"] == "ready"
    assert client.post(f"/recordings/{rid}/cache/touch").json()["touched"] is True
    assert client.post("/recordings/nope/cache/touch").json()["touched"] is False
    overview = client.get("/cache").json()
    assert overview["summary"]["entries"] == 1 and overview["entries"][0]["recording_id"] == rid
    status = client.get("/system/status").json()
    assert status["cache"]["entries"] == 1 and status["cache"]["bytes"] == 1000
    assert any(q["kind"] == "cache_build" and q["status"] == "done" for q in status["jobs"]["queue"])
