"""RosbagFileSource is tested against a minimal synthetic .db3 (a plain
sqlite file with the messages/topics shape rosbag2 uses), CDR-encoding the
test fixture data by hand -- the inverse of decode_std_msgs_string, which
evil itself never needs to do, only test fixtures do.
"""

import asyncio
import json
import sqlite3
import struct

from evil.ingestion.rosbag_file_source import RosbagFileSource


def _encode_std_msgs_string(text: str) -> bytes:
    body = text.encode("utf-8") + b"\x00"
    header = b"\x00\x01\x00\x00"  # CDR_LE representation identifier + options
    length = struct.pack("<I", len(body))
    return header + length + body


def _make_db3(path, topic_type="std_msgs/msg/String", topic_name="spi_data"):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT, type TEXT)")
    conn.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, topic_id INTEGER, timestamp INTEGER, data BLOB)"
    )
    conn.execute("INSERT INTO topics (id, name, type) VALUES (1, ?, ?)", (topic_name, topic_type))
    return conn


def _collect(source: RosbagFileSource) -> list:
    async def _drain():
        return [sample async for sample in source.samples()]

    return asyncio.run(_drain())


def test_decodes_json_messages_from_a_synthetic_bag(tmp_path):
    db3_path = str(tmp_path / "recording.db3")
    conn = _make_db3(db3_path)
    payloads = [
        {"power": {"voltage": 48.0, "current": 3.0}, "gps": {"lat": 42.0, "long": -76.0}},
        {"power": {"voltage": 47.5, "current": 3.2}, "gps": {"lat": 42.1, "long": -76.1}},
    ]
    for i, payload in enumerate(payloads):
        timestamp_ns = (i + 1) * 1_000_000_000  # 1s, 2s
        conn.execute(
            "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, ?, ?)",
            (timestamp_ns, _encode_std_msgs_string(json.dumps(payload))),
        )
    conn.commit()
    conn.close()

    samples = _collect(RosbagFileSource(run_id="run-1", path=db3_path))

    assert len(samples) == 2
    assert samples[0].ts == 1.0
    assert samples[0].joulemeter.voltage == 48.0
    assert samples[0].gps.lat == 42.0
    assert samples[1].ts == 2.0


def test_ignores_messages_on_other_topics(tmp_path):
    db3_path = str(tmp_path / "recording.db3")
    conn = _make_db3(db3_path)
    conn.execute(
        "INSERT INTO topics (id, name, type) VALUES (2, 'other_topic', 'std_msgs/msg/String')"
    )
    conn.execute(
        "INSERT INTO messages (topic_id, timestamp, data) VALUES (2, 1000000000, ?)",
        (_encode_std_msgs_string(json.dumps({"gps": {"lat": 1.0, "long": 2.0}})),),
    )
    conn.commit()
    conn.close()

    samples = _collect(RosbagFileSource(run_id="run-1", path=db3_path))

    assert samples == []


def test_ignores_non_string_message_types(tmp_path):
    db3_path = str(tmp_path / "recording.db3")
    conn = _make_db3(db3_path, topic_type="sensor_msgs/msg/Imu")
    conn.execute(
        "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, 1000000000, ?)",
        (b"\x00\x01\x00\x00garbage",),
    )
    conn.commit()
    conn.close()

    samples = _collect(RosbagFileSource(run_id="run-1", path=db3_path))

    assert samples == []


def test_skips_undecodable_or_non_json_payloads_without_crashing(tmp_path):
    db3_path = str(tmp_path / "recording.db3")
    conn = _make_db3(db3_path)
    conn.execute(
        "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, 1000000000, ?)",
        (_encode_std_msgs_string("not json"),),
    )
    conn.execute(
        "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, 2000000000, ?)",
        (_encode_std_msgs_string(json.dumps({"power": {"voltage": 1.0, "current": 1.0}})),),
    )
    conn.commit()
    conn.close()

    samples = _collect(RosbagFileSource(run_id="run-1", path=db3_path))

    assert len(samples) == 1
    assert samples[0].ts == 2.0


def test_matches_topic_stored_with_leading_slash(tmp_path):
    db3_path = str(tmp_path / "recording.db3")
    conn = _make_db3(db3_path, topic_name="/spi_data")  # real bags store the slash
    conn.execute(
        "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, 1000000000, ?)",
        (_encode_std_msgs_string(json.dumps({"gps": {"lat": 1.0, "long": 2.0}})),),
    )
    conn.commit()
    conn.close()

    samples = _collect(RosbagFileSource(run_id="run-1", path=db3_path))

    assert len(samples) == 1
    assert samples[0].gps.lat == 1.0
