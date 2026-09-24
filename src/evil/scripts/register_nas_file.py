"""CLI for registering one raw autonomy file (bag/video/lidar) on NAS
against a run's time range, so a derived row can later be joined back to
the exact recording it came from (nas_index.find_nas_files).

A human runs this after a recording finishes today; a future watcher on
tailscale-ros-telemetry's bag service (which already exposes /bag/start,
/bag/stop, /bag/status -- see RaceEngineerDashboard/backend/main.py's
proxy) would call register_nas_file() the same way once one exists.

Usage:
    python -m evil.scripts.register_nas_file <run-id> <path> <kind> <start-ts> <end-ts> [--db PATH]
"""

from __future__ import annotations

import argparse
import sys

from evil import db
from evil.tools.nas_index import register_nas_file


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    parser.add_argument("path", help="where the file lives on NAS")
    parser.add_argument("kind", help="e.g. rosbag, video, lidar")
    parser.add_argument("start_ts", type=float)
    parser.add_argument("end_ts", type=float)
    parser.add_argument("--db", default="evil.db", dest="db_path")
    args = parser.parse_args(argv)

    conn = db.connect(args.db_path)
    db.apply_schema(conn)
    file_id = register_nas_file(conn, args.run_id, args.path, args.kind, args.start_ts, args.end_ts)
    conn.close()

    print({"file_id": file_id})
    return 0


if __name__ == "__main__":
    sys.exit(main())
