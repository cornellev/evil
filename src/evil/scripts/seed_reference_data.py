"""CLI for loading static reference data (track_geometry, start_finish_line)
-- the "no dedicated CLI for this yet" gap: previously a hand-written Python
snippet in the README, now a real command so a Docker-only deployment
doesn't need a Python shell open to get started.

Usage:
    python -m evil.scripts.seed_reference_data turn <name> <lat> <lon> <radius_m> [--db PATH]
    python -m evil.scripts.seed_reference_data start-finish <lat> <lon> <radius_m> [--db PATH]
"""

from __future__ import annotations

import argparse
import sys

from evil import db


def add_turn(db_path: str, turn_name: str, lat: float, lon: float, radius_m: float) -> int:
    conn = db.connect(db_path)
    db.apply_schema(conn)
    cursor = conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES (?, ?, ?, ?)",
        (turn_name, lat, lon, radius_m),
    )
    conn.commit()
    conn.close()
    return cursor.lastrowid


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

    turn_parser = subparsers.add_parser("turn", help="add one track_geometry turn")
    turn_parser.add_argument("turn_name")
    turn_parser.add_argument("lat", type=float)
    turn_parser.add_argument("lon", type=float)
    turn_parser.add_argument("radius_m", type=float)

    sf_parser = subparsers.add_parser("start-finish", help="add the start_finish_line")
    sf_parser.add_argument("lat", type=float)
    sf_parser.add_argument("lon", type=float)
    sf_parser.add_argument("radius_m", type=float)

    args = parser.parse_args(argv)

    if args.command == "turn":
        row_id = add_turn(args.db_path, args.turn_name, args.lat, args.lon, args.radius_m)
    else:
        row_id = add_start_finish(args.db_path, args.lat, args.lon, args.radius_m)

    print({"id": row_id})
    return 0


if __name__ == "__main__":
    sys.exit(main())
