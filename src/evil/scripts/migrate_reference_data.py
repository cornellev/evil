"""Copy static reference data (track_geometry, start_finish_line) from an older
EVIL database into the new parsed database. Needed once when moving to the
catalog layout: the parsed DB (parsed/uc26/evil_uc26.db) is a fresh file, and
the classifiers do nothing until it has track geometry.

Idempotent: a table that already has rows in the destination is left alone.

Usage:
    python -m evil.scripts.migrate_reference_data --from /data/evil.db --to /data/parsed/uc26/evil_uc26.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from evil import db

TABLES = {
    "track_geometry": ("turn_def_id", "turn_name", "center_lat", "center_lon", "radius_m"),
    "start_finish_line": ("line_id", "center_lat", "center_lon", "radius_m"),
}


def migrate(src_path: str, dst_path: str) -> dict[str, int]:
    src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row
    Path(dst_path).parent.mkdir(parents=True, exist_ok=True)
    dst = db.connect(dst_path)
    db.apply_schema(dst)
    copied: dict[str, int] = {}
    try:
        for table, columns in TABLES.items():
            if dst.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]:
                copied[table] = 0
                continue
            try:
                rows = src.execute(f"SELECT {', '.join(columns)} FROM {table}").fetchall()
            except sqlite3.OperationalError:
                rows = []
            marks = ", ".join("?" * len(columns))
            dst.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})", [tuple(r) for r in rows])
            copied[table] = len(rows)
        dst.commit()
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
