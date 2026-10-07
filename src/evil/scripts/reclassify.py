"""Rebuild every derived table (turns, straights, laps, run totals) from the raw snapshots already in a parsed
database, without the original recordings. Use it after changing the track definition or a classifier, or after
upgrading a database from before gate-based segments (apply_schema drops the old derived tables).

Usage:
    python -m evil.scripts.reclassify --db /data/parsed/uc26/evil_uc26.db [--run RUN_ID]
"""

from __future__ import annotations

import argparse
import sys

from evil import db
from evil.classifiers import metrics
from evil.classifiers.runner import tick
from evil.registered_classifiers import ALL_CLASSIFIERS

_DERIVED = ("turns", "straights", "laps", "laps_open_state", "segments_open_state", "classifier_cursor", "run_summary")


def reclassify(db_path: str, only_run: str | None = None) -> dict[str, dict]:
    conn = db.connect(db_path)
    db.apply_schema(conn)
    runs = [r["run_id"] for r in conn.execute("SELECT DISTINCT run_id FROM main_snapshot ORDER BY run_id")]
    if only_run:
        runs = [r for r in runs if r == only_run]
    out: dict[str, dict] = {}
    for run_id in runs:
        with conn:
            for table in _DERIVED:
                conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))
        last_ts = conn.execute("SELECT MAX(global_ts) FROM main_snapshot WHERE run_id = ?", (run_id,)).fetchone()[0]
        tick(conn, run_id, ALL_CLASSIFIERS, now_ts=last_ts + 3600.0)
        metrics.store_run_summary(conn, run_id)
        out[run_id] = {
            "turns": conn.execute("SELECT COUNT(*) FROM turns WHERE run_id = ?", (run_id,)).fetchone()[0],
            "straights": conn.execute("SELECT COUNT(*) FROM straights WHERE run_id = ?", (run_id,)).fetchone()[0],
            "laps": conn.execute("SELECT COUNT(*) FROM laps WHERE run_id = ?", (run_id,)).fetchone()[0],
        }
    conn.close()
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--run", default=None)
    args = ap.parse_args(argv)
    for run_id, counts in reclassify(args.db, args.run).items():
        print(run_id, counts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
