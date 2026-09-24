"""IngestionSource reading a recorded rosbag2 `.db3` file's std_msgs/String
messages into RawSamples, for bulk historical ingestion. The CDR decode
logic (is_little_endian_cdr / decode_std_msgs_string) is ported from
rosbag_db3_to_csv.py at the repo root -- the one existing, proven piece of
code for reading this format, not reinvented here. Once decoded, the JSON
string routes through the same normalize.to_raw_sample() every other
source uses.
"""

from __future__ import annotations

import json
import sqlite3
import struct
from pathlib import Path
from typing import AsyncIterator

from evil.ingestion.normalize import to_raw_sample
from evil.models import RawSample


def is_little_endian_cdr(payload: bytes) -> bool:
    if len(payload) < 4:
        raise ValueError("Payload too short to contain a CDR header")
    return (payload[1] & 0x01) == 0x01


def decode_std_msgs_string(payload: bytes) -> str:
    if len(payload) < 8:
        raise ValueError("Payload too short to decode std_msgs/msg/String")

    endian_prefix = "<" if is_little_endian_cdr(payload) else ">"
    string_length = struct.unpack_from(f"{endian_prefix}I", payload, 4)[0]
    string_start = 8
    string_end = string_start + string_length

    if string_end > len(payload):
        raise ValueError("String length extends past payload size")

    string_bytes = payload[string_start:string_end]
    if string_bytes.endswith(b"\x00"):
        string_bytes = string_bytes[:-1]
    return string_bytes.decode("utf-8", errors="replace")


class RosbagFileSource:
    def __init__(self, run_id: str, path: str | Path, topic: str = "spi_data"):
        self.run_id = run_id
        self.path = Path(path)
        self.topic = topic

    async def samples(self) -> AsyncIterator[RawSample]:
        conn = sqlite3.connect(str(self.path))
        try:
            rows = conn.execute(
                """SELECT m.timestamp, t.type, m.data
                   FROM messages AS m
                   JOIN topics AS t ON t.id = m.topic_id
                   WHERE t.name = ?
                   ORDER BY m.id""",
                (self.topic,),
            ).fetchall()
        finally:
            conn.close()

        for timestamp_ns, topic_type, data in rows:
            if topic_type != "std_msgs/msg/String":
                continue
            try:
                decoded = decode_std_msgs_string(bytes(data))
                payload = json.loads(decoded)
            except (ValueError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue

            ts = timestamp_ns / 1e9
            yield to_raw_sample(self.run_id, payload, ts=ts)
