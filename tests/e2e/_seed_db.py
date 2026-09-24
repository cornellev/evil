"""Create and seed a throwaway EVIL database for the e2e smoke scripts.
Prints its path on stdout so the calling shell script can capture it."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from evil import db  # noqa: E402
from evil.ingest import ingest_sample  # noqa: E402
from evil.models import RawSample  # noqa: E402

path = sys.argv[1]
conn = db.connect(path)
db.apply_schema(conn)
# a couple of main_snapshot rows so list_runs has a real run to report
ingest_sample(conn, RawSample(run_id="run-1", ts=0.0))
ingest_sample(conn, RawSample(run_id="run-1", ts=42.0))
conn.execute(
    "INSERT INTO track_geometry (turn_def_id, turn_name, center_lat, center_lon, radius_m) "
    "VALUES (1, 'Turn 3', 42.0, -76.0, 50)"
)
conn.executemany(
    """INSERT INTO turns
           (run_id, turn_def_id, start_seq, end_seq, start_ts, end_ts, entry_speed, exit_speed)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
    [
        ("run-1", 1, 10, 14, 10.0, 12.0, 8.0, 6.0),
        ("run-1", 1, 40, 44, 40.0, 42.0, 9.0, 9.5),
    ],
)
conn.executemany(
    """INSERT INTO laps
           (run_id, lap_number, start_seq, end_seq, start_ts, end_ts, turn_count, energy_wh, avg_speed)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    [
        ("run-1", 1, 1, 20, 0.0, 20.0, 1, 5.0, 8.0),
        ("run-1", 2, 20, 44, 20.0, 42.0, 1, 4.5, 9.0),
    ],
)
conn.execute(
    "INSERT INTO nas_index (run_id, path, kind, start_ts, end_ts) VALUES ('run-1', '/nas/run-1.bag', 'rosbag', 0.0, 42.0)"
)
conn.commit()
conn.close()
print(path)
