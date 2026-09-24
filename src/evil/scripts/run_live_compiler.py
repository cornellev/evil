"""Live compiler service: consumes samples from an IngestionSource, ingests
each one, and runs tick() across the registered classifiers -- woken by an
ingestion-commit signal with a bounded max-interval fallback sleep instead
of blind fixed-interval polling. This is the event-driven design from
inference-agent/4.md section 4, not built until now (previously tick() was
only a function you called, not a running loop).

Transport-agnostic by construction: works with ReplaySource (tests, offline
replay) or Ros2Source (production) identically. This sandbox can't exercise
the Ros2Source half -- no rclpy here -- so that side is documented, not
tested; run this same script against a real Ros2Source on the DAQ device.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import sys
import time

from evil import db
from evil.classifiers.base import Classifier
from evil.classifiers.runner import tick
from evil.ingest import ingest_sample
from evil.ingestion.base import IngestionSource
from evil.registered_classifiers import ALL_CLASSIFIERS

DEFAULT_MAX_INTERVAL_S = 5.0


class LiveCompiler:
    def __init__(
        self,
        conn,
        run_id: str,
        source: IngestionSource,
        classifiers: list[Classifier],
        max_interval_s: float = DEFAULT_MAX_INTERVAL_S,
    ):
        self.conn = conn
        self.run_id = run_id
        self.source = source
        self.classifiers = classifiers
        self.max_interval_s = max_interval_s
        self.tick_count = 0
        self._wake = asyncio.Event()

    async def _consume(self) -> None:
        async for sample in self.source.samples():
            ingest_sample(self.conn, sample)
            self._wake.set()

    async def _compile_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.max_interval_s)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            self._tick_once()

    def _tick_once(self) -> None:
        tick(self.conn, self.run_id, self.classifiers, now_ts=time.time())
        self.tick_count += 1

    async def run(self) -> None:
        """Runs until the source is exhausted (a finite ReplaySource) or
        forever (a live source like Ros2Source). Always ticks once more
        after the source ends, to catch anything ingested right before
        shutdown that the compile loop hadn't gotten to yet."""
        compile_task = asyncio.create_task(self._compile_loop())
        try:
            await self._consume()
        finally:
            compile_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await compile_task
            self._tick_once()


def main(argv: list[str] | None = None) -> int:
    """Production entry point: a live Ros2Source against ALL_CLASSIFIERS.
    Requires rclpy (a real ROS2 environment) -- not runnable, and not
    covered by this package's tests, on a machine without one; the service
    loop itself is tested against ReplaySource in test_run_live_compiler.py.
    """
    from evil.ingestion.ros2_source import Ros2Source

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id", help="run_id to assign this live session")
    parser.add_argument("--db", default=os.getenv("EVIL_DB_PATH", "evil.db"), dest="db_path")
    parser.add_argument("--topic", default="spi_data")
    parser.add_argument("--max-interval", type=float, default=DEFAULT_MAX_INTERVAL_S, dest="max_interval_s")
    args = parser.parse_args(argv)

    conn = db.connect(args.db_path)
    db.apply_schema(conn)
    source = Ros2Source(run_id=args.run_id, topic=args.topic)
    compiler = LiveCompiler(conn, args.run_id, source, ALL_CLASSIFIERS, max_interval_s=args.max_interval_s)

    try:
        asyncio.run(compiler.run())
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
