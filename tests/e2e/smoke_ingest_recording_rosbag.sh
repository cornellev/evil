#!/usr/bin/env bash
# Mirrors smoke_ingest_recording.sh but feeds a synthetic .db3 rosbag through
# the same CLI entry point, proving the format auto-detection and the CDR
# decode path work end to end as a real subprocess, not just in-process.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_PYTHON="$REPO_ROOT/.venv/bin/python"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

export PYTHONPATH="$REPO_ROOT/src"

DB_PATH="$WORKDIR/evil.db"
BAG_PATH="$WORKDIR/recording.db3"

echo "[smoke] seeding track_geometry"
"$VENV_PYTHON" -c "
from evil import db
conn = db.connect('$DB_PATH')
db.apply_schema(conn)
conn.execute(
    \"INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('T1', 42.0, -76.0, 50)\"
)
conn.commit()
conn.close()
"

echo "[smoke] writing a synthetic rosbag .db3 fixture"
"$VENV_PYTHON" -c "
import json, sqlite3, struct

def encode(text):
    body = text.encode('utf-8') + b'\x00'
    return b'\x00\x01\x00\x00' + struct.pack('<I', len(body)) + body

conn = sqlite3.connect('$BAG_PATH')
conn.execute('CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT, type TEXT)')
conn.execute('CREATE TABLE messages (id INTEGER PRIMARY KEY, topic_id INTEGER, timestamp INTEGER, data BLOB)')
conn.execute(\"INSERT INTO topics (id, name, type) VALUES (1, 'spi_data', 'std_msgs/msg/String')\")

payloads = [
    {'gps': {'lat': 42.0, 'long': -76.01}, 'power': {'voltage': 20.0, 'current': 1.0}},
    {'gps': {'lat': 42.0, 'long': -76.0003}, 'power': {'voltage': 8.0, 'current': 1.0}},
    {'gps': {'lat': 42.0, 'long': -76.0000}, 'power': {'voltage': 9.0, 'current': 1.0}},
    {'gps': {'lat': 42.0, 'long': -75.999}, 'power': {'voltage': 7.0, 'current': 1.0}},
]
for i, payload in enumerate(payloads):
    conn.execute(
        'INSERT INTO messages (topic_id, timestamp, data) VALUES (1, ?, ?)',
        ((i + 1) * 1_000_000_000, encode(json.dumps(payload))),
    )
conn.commit()
conn.close()
"

echo "[smoke] running the packaged CLI entry point as a real subprocess"
"$VENV_PYTHON" -m evil.scripts.ingest_recording "$BAG_PATH" "hist-bag-run" --db "$DB_PATH"

echo "[smoke] verifying the resulting database independently"
"$VENV_PYTHON" -c "
import sqlite3
conn = sqlite3.connect('$DB_PATH')
conn.row_factory = sqlite3.Row

n_snapshot = conn.execute(
    \"SELECT COUNT(*) AS n FROM main_snapshot WHERE run_id = 'hist-bag-run'\"
).fetchone()['n']
assert n_snapshot == 4, f'expected 4 main_snapshot rows, got {n_snapshot}'

turn = conn.execute(\"SELECT * FROM turns WHERE run_id = 'hist-bag-run'\").fetchone()
assert turn is not None, 'expected a classified turn row from the rosbag-derived data, found none'

print('OK: CLI auto-detected .db3, decoded CDR/JSON messages, ingested 4 rows, classified 1 turn')
"

echo "[smoke] PASS"
