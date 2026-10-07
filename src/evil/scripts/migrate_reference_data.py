"""Copy static reference data (track segments, start/finish) from an older EVIL database into the new parsed
database. Needed once when moving to the catalog layout: the parsed DB (parsed/uc26/evil_uc26.db) is a
fresh file and the classifiers do nothing until it has a track.

A source that already has `track_segments` is copied as is. A source from before gate-based segments (circle
`track_geometry` only) cannot be converted, so the packaged IMS 2026 track is loaded instead and its start/finish
circle is copied. Idempotent: a table that already has rows in the destination is left alone.

Usage:
    python -m evil.scripts.migrate_reference_data --from /data/evil.db --to /data/parsed/uc26/evil_uc26.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from evil import db, track

SEGMENT_COLUMNS = ("segment_id", "ordinal", "kind", "name", "aliases", "length_m",
                   "gate_lat1", "gate_lon1", "gate_lat2", "gate_lon2")
SF_COLUMNS = ("line_id", "center_lat", "center_lon", "radius_m")


def _rows(src: sqlite3.Connection, table: str, columns: tuple[str, ...]) -> list[tuple]:
    try:
        return [tuple(r) for r in src.execute(f"SELECT {', '.join(columns)} FROM {table}").fetchall()]
    except sqlite3.OperationalError:
        return []


def migrate(src_path: str, dst_path: str) -> dict[str, object]:
    src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
    dst = db.connect(dst_path)
    db.apply_schema(dst)
    copied: dict[str, object] = {}
    try:
        # the team's own start/finish circle goes first, so seeding the packaged track never overrides it
        if dst.execute("SELECT COUNT(*) FROM start_finish_line").fetchone()[0]:
            copied["start_finish_line"] = 0
        else:
            rows = _rows(src, "start_finish_line", SF_COLUMNS)
            dst.executemany(f"INSERT INTO start_finish_line ({', '.join(SF_COLUMNS)}) VALUES (?, ?, ?, ?)", rows)
            dst.commit()
            copied["start_finish_line"] = len(rows)
        if dst.execute("SELECT COUNT(*) FROM track_segments").fetchone()[0]:
            copied["track_segments"] = 0
        else:
            segments = _rows(src, "track_segments", SEGMENT_COLUMNS)
            if segments:
                dst.executemany(
                    f"INSERT INTO track_segments ({', '.join(SEGMENT_COLUMNS)}) VALUES ({', '.join('?' * len(SEGMENT_COLUMNS))})",
                    segments,
                )
                dst.commit()
                copied["track_segments"] = len(segments)
            else:
                copied["track_segments"] = track.seed_track(dst, track.load_track_file())
                copied["track"] = "packaged IMS 2026 (source had no segments)"
    finally:
        src.close()
        dst.close()
    return copied


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", required=True, dest="src")
    ap.add_argument("--to", required=True, dest="dst")
    args = ap.parse_args(argv)
    print(migrate(args.src, args.dst))
    return 0


if __name__ == "__main__":
    sys.exit(main())
