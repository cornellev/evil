"""Subprocess entry point for one parse job. The worker runs each parse in its
own process with a timeout, so a corrupt or huge file can kill one child, never
the worker. Progress is reported on stdout as `PROGRESS <0..1>` lines and the
outcome as a final `RESULT <json>` line; the catalog row is updated by the
parse itself.

    python -m evil.parse_job <recording_id> [--root DIR]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from evil import catalog, parser


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("recording_id")
    ap.add_argument("--root", default=None, help="data root (default: from the environment)")
    args = ap.parse_args(argv)
    root = catalog.DataRoot(Path(args.root)) if args.root else catalog.data_root_from_env()

    def progress(fraction: float) -> None:
        print(f"PROGRESS {fraction:.4f}", flush=True)

    result = parser.parse_recording(root, args.recording_id, progress=progress)
    print("RESULT " + json.dumps({
        "status": result.status, "run_id": result.run_id, "rows_ingested": result.rows_ingested,
        "rows_duplicate": result.rows_duplicate, "rows_rejected": result.rows_rejected,
        "message": result.message,
    }), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
