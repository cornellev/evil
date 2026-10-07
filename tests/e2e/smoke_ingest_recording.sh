#!/usr/bin/env bash
# End-to-end smoke test: runs the packaged `python -m evil.scripts.ingest_recording`
# entry point as a real subprocess against a fixture CSV, then verifies the
# resulting database independently (raw stdlib sqlite3, not evil's own db.py)
# so this genuinely proves the shipped CLI works end-to-end, not just that
# the underlying Python function does when called in-process.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_PYTHON="$REPO_ROOT/.venv/bin/python"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

export PYTHONPATH="$REPO_ROOT/src"

DB_PATH="$WORKDIR/evil.db"
CSV_PATH="$WORKDIR/recording.csv"

echo "[smoke] seeding the track (reference data, loaded separately from ingestion): a 100 m turn, then a straight"
"$VENV_PYTHON" -c "
from evil import db, track
conn = db.connect('$DB_PATH')
db.apply_schema(conn)
track.seed_track(conn, {'segments': [
    {'name': 'T1', 'kind': 'turn', 'aliases': ['1'], 'length_m': 100.0, 'entry_gate': [41.999, -76.0006043, 42.001, -76.0006043]},
    {'name': 'Straight 1', 'kind': 'straight', 'aliases': [], 'length_m': 500.0, 'entry_gate': [41.999, -75.9993957, 42.001, -75.9993957]},
]})
conn.close()
"

echo "[smoke] writing fixture recording CSV"
cat > "$CSV_PATH" <<'CSV'
global_ts,gps.lat,gps.long,speed
0.0,42.0,-76.0012087,20.0
1.0,42.0,-76.0003022,8.0
2.0,42.0,-76.0000,9.0
3.0,42.0,-75.9990935,7.0
CSV

echo "[smoke] running the packaged CLI entry point as a real subprocess"
"$VENV_PYTHON" -m evil.scripts.ingest_recording "$CSV_PATH" "hist-run-1" --db "$DB_PATH"

echo "[smoke] verifying the resulting database independently"
"$VENV_PYTHON" -c "
import sqlite3
conn = sqlite3.connect('$DB_PATH')
conn.row_factory = sqlite3.Row

n_snapshot = conn.execute(
    \"SELECT COUNT(*) AS n FROM main_snapshot WHERE run_id = 'hist-run-1'\"
).fetchone()['n']
assert n_snapshot == 4, f'expected 4 main_snapshot rows, got {n_snapshot}'

turn = conn.execute(\"SELECT * FROM turns WHERE run_id = 'hist-run-1'\").fetchone()
assert turn is not None, 'expected a classified turn row, found none'
assert turn['entry_speed'] == 8.0, f\"unexpected entry_speed: {turn['entry_speed']}\"
assert turn['exit_speed'] == 7.0, f\"unexpected exit_speed: {turn['exit_speed']}\"

print('OK: CLI ingested 4 rows and produced 1 classified turn, verified independently')
"

echo "[smoke] PASS"
