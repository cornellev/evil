import json
import struct

from fastapi.testclient import TestClient

from evil import db
from evil.upload_server import app


def _encode_std_msgs_string(text: str) -> bytes:
    body = text.encode("utf-8") + b"\x00"
    return b"\x00\x01\x00\x00" + struct.pack("<I", len(body)) + body


def test_healthz():
    resp = TestClient(app).get("/healthz")
    assert resp.status_code == 200


def test_upload_csv_ingests_and_returns_counts(tmp_path, monkeypatch):
    db_path = str(tmp_path / "evil.db")
    monkeypatch.setattr("evil.upload_server.DEFAULT_DB_PATH", db_path)

    csv_bytes = b"global_ts,gps.lat,gps.long,speed\n0.0,42.0,-76.01,20.0\n1.0,42.0,-76.0,8.0\n"
    resp = TestClient(app).post(
        "/upload",
        data={"run_id": "run-1"},
        files={"file": ("recording.csv", csv_bytes, "text/csv")},
    )

    assert resp.status_code == 200
    assert resp.json()["rows_ingested"] == 2

    conn = db.connect_readonly(db_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM main_snapshot WHERE run_id = 'run-1'"
        ).fetchone()["n"]
    finally:
        conn.close()
    assert n == 2


def test_upload_rosbag_ingests_and_returns_counts(tmp_path, monkeypatch):
    import sqlite3

    db_path = str(tmp_path / "evil.db")
    monkeypatch.setattr("evil.upload_server.DEFAULT_DB_PATH", db_path)

    bag_path = tmp_path / "recording.db3"
    conn = sqlite3.connect(str(bag_path))
    conn.execute("CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT, type TEXT)")
    conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, topic_id INTEGER, timestamp INTEGER, data BLOB)"
    )
    conn.execute("INSERT INTO topics (id, name, type) VALUES (1, 'spi_data', 'std_msgs/msg/String')")
    conn.execute(
        "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, 1000000000, ?)",
        (_encode_std_msgs_string(json.dumps({"gps": {"lat": 42.0, "long": -76.0}})),),
    )
    conn.commit()
    conn.close()

    resp = TestClient(app).post(
        "/upload",
        data={"run_id": "run-bag"},
        files={"file": ("recording.db3", bag_path.read_bytes(), "application/octet-stream")},
    )

    assert resp.status_code == 200
    assert resp.json()["rows_ingested"] == 1


def test_upload_rejects_unsupported_extension():
    resp = TestClient(app).post(
        "/upload",
        data={"run_id": "run-1"},
        files={"file": ("recording.parquet", b"nope", "application/octet-stream")},
    )
    assert resp.status_code == 400


def test_upload_rejects_oversized_file(monkeypatch):
    monkeypatch.setattr("evil.upload_server.MAX_UPLOAD_BYTES", 10)
    resp = TestClient(app).post(
        "/upload",
        data={"run_id": "run-1"},
        files={"file": ("recording.csv", b"global_ts,gps.lat,gps.long\n0,1,2\n", "text/csv")},
    )
    assert resp.status_code == 413
