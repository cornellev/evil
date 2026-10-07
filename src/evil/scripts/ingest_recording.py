"""CLI for bulk-loading a previously recorded session into EVIL as historical
data. Reuses the same IngestionSource -> ingest_sample() path live ingestion
uses; the only difference is where samples come from and that the classifier
backfill runs once at the end instead of ticking incrementally, since a
finished historical run has no live edge still arriving. No special
"backfill mode" is needed: pass now_ts far enough past the run's last
sample and the existing incremental tick() classifies the whole thing in
one pass. See the plan doc and inference-agent/4.md section 4.

Input format is auto-detected by extension (.csv or .db3) so callers, and a
future UI's "upload" button, don't need to know which IngestionSource to
pick -- see this module's role in the plan as the one place bulk ingestion
happens, regardless of which trigger (CLI today, a UI button later) kicks
it off.

Usage:
    python -m evil.scripts.ingest_recording <recording-file> <run-id> [--db PATH]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from evil import db
from evil.classifiers.runner import tick
from evil.ingest import ingest_sample
from evil.ingestion.base import IngestionSource
from evil.ingestion.csv_file_source import CsvFileSource
from evil.ingestion.rosbag_file_source import RosbagFileSource
from evil.registered_classifiers import ALL_CLASSIFIERS

_SOURCES_BY_SUFFIX = {
    ".csv": CsvFileSource,
    ".db3": RosbagFileSource,
}


def _build_source(recording_path: str, run_id: str) -> IngestionSource:
    suffix = Path(recording_path).suffix.lower()
    try:
        source_cls = _SOURCES_BY_SUFFIX[suffix]
    except KeyError:
        supported = ", ".join(sorted(_SOURCES_BY_SUFFIX))
        raise ValueError(f"unsupported recording format {suffix!r}; supported: {supported}") from None
    return source_cls(run_id=run_id, path=recording_path)


def ingest_recording(recording_path: str, run_id: str, db_path: str) -> dict[str, object]:
    conn = db.connect(db_path)
    db.apply_schema(conn)

    source = _build_source(recording_path, run_id)
    max_ts = 0.0
    count = 0

    async def _drain() -> None:
        nonlocal max_ts, count
        async for sample in source.samples():
            ingest_sample(conn, sample)
            max_ts = max(max_ts, sample.ts)
            count += 1

    asyncio.run(_drain())

    if count == 0:
        conn.close()
        describe = getattr(source, "describe_no_rows", None)
        raise ValueError(describe() if describe else f"no rows ingested from {Path(recording_path).name}")

    # No live edge left to wait behind: classify the whole run in one pass.
    backfill_now_ts = max_ts + 3600.0
    advanced = tick(conn, run_id, ALL_CLASSIFIERS, now_ts=backfill_now_ts)
    conn.close()

    return {"rows_ingested": count, "classifiers_advanced_to_seq": advanced}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording_path", help="path to a recorded telemetry export (.csv or .db3)")
    parser.add_argument("run_id", help="run_id to assign this historical session")
    parser.add_argument("--db", default="evil.db", dest="db_path")
    args = parser.parse_args(argv)

    result = ingest_recording(args.recording_path, args.run_id, args.db_path)
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
