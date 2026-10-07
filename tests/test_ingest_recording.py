import json
import sqlite3
import struct

import pytest

from evil import db
from evil.scripts.ingest_recording import ingest_recording, main


def _encode_std_msgs_string(text: str) -> bytes:
    body = text.encode("utf-8") + b"\x00"
    return b"\x00\x01\x00\x00" + struct.pack("<I", len(body)) + body


def _write_rosbag(path: str, payloads: list[dict]) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT, type TEXT)")
    conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, topic_id INTEGER, timestamp INTEGER, data BLOB)"
    )
    conn.execute("INSERT INTO topics (id, name, type) VALUES (1, 'spi_data', 'std_msgs/msg/String')")
    for i, payload in enumerate(payloads):
        conn.execute(
            "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, ?, ?)",
            ((i + 1) * 1_000_000_000, _encode_std_msgs_string(json.dumps(payload))),
        )
    conn.commit()
    conn.close()


def _seed_track_geometry(db_path: str) -> None:
    """A turn 100 m long (x = -50..50 m east of lat 42, lon -76), then a straight."""
    from track_fixtures import seed_line

    conn = db.connect(db_path)
    db.apply_schema(conn)
    seed_line(conn, turn_from_x=-50.0, turn_to_x=50.0)
    conn.close()


def test_ingest_recording_produces_rows_and_a_classified_turn(tmp_path):
    db_path = str(tmp_path / "evil.db")
    _seed_track_geometry(db_path)

    csv_path = tmp_path / "recording.csv"
    csv_path.write_text(
        "global_ts,gps.lat,gps.long,speed\n"
        "0.0,42.0,-76.0012087,20.0\n"
        "1.0,42.0,-76.0003022,8.0\n"
        "2.0,42.0,-76.0000,9.0\n"
        "3.0,42.0,-75.9990935,7.0\n"
    )

    result = ingest_recording(str(csv_path), "hist-run-1", db_path)

    assert result["rows_ingested"] == 4

    conn = db.connect_readonly(db_path)
    try:
        snapshot_count = conn.execute(
            "SELECT COUNT(*) AS n FROM main_snapshot WHERE run_id = 'hist-run-1'"
        ).fetchone()["n"]
        assert snapshot_count == 4

        turn = conn.execute("SELECT * FROM turns WHERE run_id = 'hist-run-1'").fetchone()
        assert turn is not None
        assert turn["entry_speed"] == 8.0
        assert turn["exit_speed"] == 7.0
    finally:
        conn.close()


def test_ingest_recording_rejects_empty_csv(tmp_path):
    db_path = str(tmp_path / "evil.db")
    _seed_track_geometry(db_path)
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("global_ts,gps.lat,gps.long\n")

    with pytest.raises(ValueError, match="no rows ingested"):
        ingest_recording(str(csv_path), "hist-run-2", db_path)


def test_ingest_recording_auto_detects_rosbag_format(tmp_path):
    db_path = str(tmp_path / "evil.db")
    _seed_track_geometry(db_path)
    bag_path = str(tmp_path / "recording.db3")
    _write_rosbag(
        bag_path,
        [
            {"gps": {"lat": 42.0, "long": -76.01}, "power": {"voltage": 48.0, "current": 3.0}},
            {"gps": {"lat": 42.0, "long": -76.0003}},
        ],
    )

    result = ingest_recording(bag_path, "hist-run-bag", db_path)

    assert result["rows_ingested"] == 2
    conn = db.connect_readonly(db_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM main_snapshot WHERE run_id = 'hist-run-bag'"
        ).fetchone()["n"]
        assert n == 2
    finally:
        conn.close()


def test_ingest_recording_rejects_unsupported_extensions(tmp_path):
    db_path = str(tmp_path / "evil.db")
    _seed_track_geometry(db_path)
    bad_path = tmp_path / "recording.parquet"
    bad_path.write_text("nope")

    with pytest.raises(ValueError, match="unsupported"):
        ingest_recording(str(bad_path), "hist-run-x", db_path)


def test_main_cli_entry_point_parses_args_and_runs(tmp_path, capsys):
    db_path = str(tmp_path / "evil.db")
    _seed_track_geometry(db_path)
    csv_path = tmp_path / "recording.csv"
    csv_path.write_text("global_ts,gps.lat,gps.long,speed\n0.0,42.0,-76.01,20.0\n")

    exit_code = main([str(csv_path), "hist-run-3", "--db", db_path])

    assert exit_code == 0
    assert "rows_ingested" in capsys.readouterr().out


def test_ingest_recording_zero_rows_from_bag_names_topics_found(tmp_path):
    db_path = str(tmp_path / "evil.db")
    _seed_track_geometry(db_path)
    db3_path = tmp_path / "wrong_topic.db3"
    conn = sqlite3.connect(db3_path)
    conn.execute("CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT, type TEXT)")
    conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, topic_id INTEGER, timestamp INTEGER, data BLOB)")
    conn.execute("INSERT INTO topics VALUES (1, '/camera/image', 'sensor_msgs/msg/Image')")
    conn.commit()
    conn.close()

    with pytest.raises(ValueError, match=r"/camera/image .*sensor_msgs/msg/Image"):
        ingest_recording(str(db3_path), "hist-run-3", db_path)
