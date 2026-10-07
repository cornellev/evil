"""CLI for loading static reference data (track_segments, start_finish_line)
-- the "no dedicated CLI for this yet" gap: previously a hand-written Python
snippet in the README, now a real command so a Docker-only deployment
doesn't need a Python shell open to get started.

Usage:
    python -m evil.scripts.seed_reference_data --db PATH track [FILE.json] [--replace]
        load the track's turns and straights (default: the packaged IMS 2026 definition)
    python -m evil.scripts.seed_reference_data --db PATH start-finish <lat> <lon> <radius_m>
"""

from __future__ import annotations

import argparse
import sys

from evil import db, track


def add_track(db_path: str, track_file: str | None = None, replace: bool = False) -> int:
    conn = db.connect(db_path)
    db.apply_schema(conn)
    count = track.seed_track(conn, track.load_track_file(track_file) if track_file else track.load_track_file(), replace)
    conn.close()
    return count


def add_start_finish(db_path: str, lat: float, lon: float, radius_m: float) -> int:
    conn = db.connect(db_path)
    db.apply_schema(conn)
    cursor = conn.execute(
        "INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (?, ?, ?)",
        (lat, lon, radius_m),
    )
    conn.commit()
    conn.close()
    return cursor.lastrowid


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="evil.db", dest="db_path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    track_parser = subparsers.add_parser("track", help="load the track's turns and straights from a track definition")
    track_parser.add_argument("file", nargs="?", default=None, help="track JSON (default: packaged IMS 2026)")
    track_parser.add_argument("--replace", action="store_true", help="replace a different track already loaded")

    sf_parser = subparsers.add_parser("start-finish", help="add the start_finish_line")
    sf_parser.add_argument("lat", type=float)
    sf_parser.add_argument("lon", type=float)
    sf_parser.add_argument("radius_m", type=float)

    args = parser.parse_args(argv)

    if args.command == "track":
        row_id = add_track(args.db_path, args.file, args.replace)
    else:
        row_id = add_start_finish(args.db_path, args.lat, args.lon, args.radius_m)

    print({"segments": row_id} if args.command == "track" else {"id": row_id})
    return 0


if __name__ == "__main__":
    sys.exit(main())
