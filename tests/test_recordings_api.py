import json

import pytest
from fastapi.testclient import TestClient

from evil import catalog
from evil.upload_server import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("EVIL_DATA_ROOT", str(tmp_path / "data"))
    return TestClient(app)


def _root(tmp_path):
    return catalog.DataRoot(tmp_path / "data")


def test_upload_returns_201_only_after_files_are_stored(client, tmp_path):
    resp = client.post(
        "/recordings",
        data={"category": "testing", "car": "uc26", "label": "garage run"},
        files=[("files", ("run.csv", b"a,b\n1,2\n", "text/csv"))],
    )

    assert resp.status_code == 201
    body = resp.json()
    assert body["deduplicated"] is False and body["parse_status"] == "pending" and body["files"] == 1
    rec = client.get(f"/recordings/{body['recording_id']}").json()
    assert rec["label"] == "garage run" and rec["container"] == "csv"
    assert (_root(tmp_path).raw / rec["files"][0]["rel_path"]).read_bytes() == b"a,b\n1,2\n"


def test_folder_upload_with_several_files(client, tmp_path):
    resp = client.post(
        "/recordings",
        files=[
            ("files", ("bag_1/bag_1_0.db3", b"SQLite format 3\x00abc", "application/octet-stream")),
            ("files", ("bag_1/metadata.yaml", b"x: 1", "text/yaml")),
        ],
    )
    assert resp.status_code == 201 and resp.json()["files"] == 2
    rec = client.get(f"/recordings/{resp.json()['recording_id']}").json()
    assert rec["container"] == "rosbag2-sqlite3"
    assert sorted(f["role"] for f in rec["files"]) == ["db3", "metadata"]


def test_identical_upload_returns_200_with_the_existing_recording(client):
    files = [("files", ("a.csv", b"same", "text/csv"))]
    first = client.post("/recordings", files=files)
    second = client.post("/recordings", files=[("files", ("other_name.csv", b"same", "text/csv"))])

    assert first.status_code == 201 and second.status_code == 200
    assert second.json()["recording_id"] == first.json()["recording_id"]
    assert second.json()["deduplicated"] is True
    assert len(client.get("/recordings").json()) == 1


def test_malformed_and_unknown_files_are_still_stored(client):
    resp = client.post("/recordings", files=[("files", ("broken.db3", b"garbage not sqlite", "application/octet-stream"))])
    assert resp.status_code == 201
    rec = client.get(f"/recordings/{resp.json()['recording_id']}").json()
    assert rec["container"] == "unknown" and rec["parse_status"] == "pending"


def test_bad_requests_are_400_and_store_nothing(client, tmp_path):
    assert client.post("/recordings", data={"label": "x"}).status_code == 400  # no files
    bad_cat = client.post("/recordings", data={"category": "party"}, files=[("files", ("a.csv", b"x", "text/csv"))])
    assert bad_cat.status_code == 400
    bad_src = client.post("/recordings", data={"source": "robot"}, files=[("files", ("a.csv", b"x", "text/csv"))])
    assert bad_src.status_code == 400
    assert client.get("/recordings").json() == []
    root = _root(tmp_path)
    assert list(root.incoming.iterdir()) == [root.spool]


def test_oversized_upload_is_413_and_leaves_no_row_or_files(client, tmp_path, monkeypatch):
    monkeypatch.setattr("evil.upload_server.MAX_RECORDING_BYTES", 10)
    resp = client.post("/recordings", files=[("files", ("big.csv", b"x" * 100, "text/csv"))])

    assert resp.status_code == 413
    assert client.get("/recordings").json() == []
    root = _root(tmp_path)
    assert list(root.incoming.iterdir()) == [root.spool]
    assert list(root.raw.rglob("*.csv")) == []


def test_multi_file_upload_over_the_total_cap_is_413(client, tmp_path, monkeypatch):
    monkeypatch.setattr("evil.upload_server.MAX_RECORDING_BYTES", 150)
    resp = client.post("/recordings", files=[
        ("files", ("a.bin", b"x" * 50, "application/octet-stream")),
        ("files", ("b.bin", b"y" * 120, "application/octet-stream")),
    ])
    assert resp.status_code == 413
    root = _root(tmp_path)
    assert list(root.incoming.iterdir()) == [root.spool]
    assert client.get("/recordings").json() == []


def test_not_enough_disk_is_507(client, monkeypatch):
    monkeypatch.setattr("evil.upload_server.MIN_FREE_BYTES", 10**18)
    resp = client.post("/recordings", files=[("files", ("a.csv", b"x", "text/csv"))])
    assert resp.status_code == 507


def test_list_filters_and_patch(client):
    a = client.post("/recordings", data={"category": "testing"}, files=[("files", ("a.csv", b"a", "text/csv"))]).json()
    client.post("/recordings", data={"category": "competition"}, files=[("files", ("b.csv", b"b", "text/csv"))])

    assert len(client.get("/recordings").json()) == 2
    assert len(client.get("/recordings", params={"category": "testing"}).json()) == 1
    assert client.get("/recordings", params={"since": "2999-01-01"}).json() == []
    assert client.get("/recordings", params={"since": "garbage"}).status_code == 400

    patched = client.patch(f"/recordings/{a['recording_id']}", json={"label": "renamed", "event": "IMS 2026"})
    assert patched.status_code == 200 and patched.json()["label"] == "renamed"
    assert client.patch(f"/recordings/{a['recording_id']}", json={"category": "nope"}).status_code == 400
    assert client.patch("/recordings/missing", json={"label": "x"}).status_code == 404
    assert client.get("/recordings/missing").status_code == 404


def test_startup_maintenance_adopts_orphans_and_backs_up(tmp_path, monkeypatch):
    monkeypatch.setenv("EVIL_DATA_ROOT", str(tmp_path / "data"))
    with TestClient(app) as c:  # runs lifespan -> maintenance
        c.post("/recordings", files=[("files", ("a.csv", b"a", "text/csv"))])
    root = _root(tmp_path)
    conn = catalog.connect_catalog(root)
    with conn:
        conn.execute("DELETE FROM recording_files")
        conn.execute("DELETE FROM recordings")
    conn.close()

    with TestClient(app) as c:
        import time
        for _ in range(50):
            if c.get("/recordings").json():
                break
            time.sleep(0.1)
        assert len(c.get("/recordings").json()) == 1
    assert list(root.backup.glob("catalog-*.db"))


def test_failure_midway_through_staging_leaves_nothing(tmp_path, monkeypatch):
    """Calls the blocking store step directly so the cap trips on the second
    file *after* the first was already written to staging."""
    import io
    from types import SimpleNamespace

    from evil import upload_server

    monkeypatch.setenv("EVIL_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setattr(upload_server, "MAX_RECORDING_BYTES", 100)
    root = _root(tmp_path)
    root.ensure()
    parts = [
        SimpleNamespace(filename="a.bin", file=io.BytesIO(b"x" * 60)),
        SimpleNamespace(filename="b.bin", file=io.BytesIO(b"y" * 60)),
    ]

    with pytest.raises(catalog.UploadTooLarge):
        upload_server._store_recording(root, parts, catalog.UploadFields())

    assert list(root.incoming.iterdir()) == [root.spool]
    assert list(root.raw.rglob("*.bin")) == []
    conn = catalog.connect_catalog(root)
    assert conn.execute("SELECT COUNT(*) FROM recordings").fetchone()[0] == 0
    conn.close()


def test_killed_connection_mid_upload_stores_nothing(tmp_path, monkeypatch):
    """Real server, real socket: send half of a large multipart body, then drop
    the connection (browser killed). No catalog row, no recording folder, and
    the request spool is cleaned up."""
    import socket
    import threading
    import time

    import uvicorn

    monkeypatch.setenv("EVIL_DATA_ROOT", str(tmp_path / "data"))
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started and server.servers and server.servers[0].sockets:
            break
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]

    boundary = "XBOUNDARY"
    head = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"big.db3\"\r\n"
            f"Content-Type: application/octet-stream\r\n\r\n").encode()
    payload = b"z" * 5_000_000
    tail = f"\r\n--{boundary}--\r\n".encode()
    total = len(head) + len(payload) + len(tail)
    request = (f"POST /recordings HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary={boundary}\r\n"
               f"Content-Length: {total}\r\n\r\n").encode() + head + payload[: len(payload) // 2]
    try:
        sock = socket.create_connection(("127.0.0.1", port))
        sock.sendall(request)
        time.sleep(0.5)
        sock.close()  # browser killed mid-upload
        time.sleep(1.0)

        root = _root(tmp_path)
        conn = catalog.connect_catalog(root)
        assert conn.execute("SELECT COUNT(*) FROM recordings").fetchone()[0] == 0
        conn.close()
        assert list(root.raw.rglob("big.db3")) == []
        assert list(root.incoming.iterdir()) == [root.spool]
        assert list(root.spool.iterdir()) == []
    finally:
        server.should_exit = True
        thread.join(timeout=5)
