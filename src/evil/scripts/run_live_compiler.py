"""Live compiler service: consumes envelopes from an IngestionSource, runs them through
the same pipeline uploads use (schema detection, validation, repeat drop, mapping:
ingestion/pipeline.py), ingests each stored sample, and runs tick() across the registered
classifiers, woken by ingestion with a bounded max-interval fallback.

Schema: detected from the first LOCK_AFTER messages (v1 / v2 / v1-dutycycle; "neither" is
rejected and counted, never stored). A sustained switch to another schema mid-stream (DAQ
reflashed) closes the run and opens `<run_id>-2`, `-3`, ... (sessions proper: section C of
live-next-steps-2026-11.md).

Time: rows are stamped by LiveClock (publisher time on EVIL's clock), and classifiers tick
with the latest sample time as "now", so a replay at 10x classifies exactly like 1x.

Transport-agnostic: works with ReplaySource (tests) or Ros2Source (production) identically.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys

from evil import db
from evil.classifiers.base import Classifier
from evil.classifiers.runner import tick
from evil.ingest import ingest_sample
from evil.ingestion.base import IngestionSource
from evil.ingestion.pipeline import LOCK_AFTER, SWITCH_AFTER, Fed, LiveClock, LiveDetector
from evil.registered_classifiers import ALL_CLASSIFIERS
from evil.schemas._strict import StrictSchema

log = logging.getLogger("evil.live")

DEFAULT_MAX_INTERVAL_S = 5.0


class LiveCompiler:
    def __init__(
        self,
        conn,
        run_id: str,
        source: IngestionSource,
        classifiers: list[Classifier],
        max_interval_s: float = DEFAULT_MAX_INTERVAL_S,
        lock_after: int = LOCK_AFTER,
        switch_after: int = SWITCH_AFTER,
    ):
        self.conn = conn
        self.base_run_id = run_id
        self.source = source
        self.classifiers = classifiers
        self.max_interval_s = max_interval_s
        self.tick_count = 0
        self.runs: list[tuple[str, str]] = []          # (run_id, schema_id) in order
        self.clock = LiveClock()
        self.detector = LiveDetector(self._new_run_id, lock_after=lock_after, switch_after=switch_after)
        self._latest_ts: dict[str, float] = {}
        self._wake = asyncio.Event()

    @property
    def run_id(self) -> str | None:
        """The run currently being written (None until a schema has locked)."""
        return self.detector.stream.run_id if self.detector.stream else None

    def _new_run_id(self, schema: StrictSchema) -> str:
        n = len(self.runs) + 1
        run_id = self.base_run_id if n == 1 else f"{self.base_run_id}-{n}"
        self.runs.append((run_id, schema.schema_id))
        log.info("run %s: schema %s", run_id, schema.schema_id)
        return run_id

    def _handle(self, fed: Fed) -> None:
        if fed.switched_to is not None:
            closed = self.detector.closed_streams[-1]
            log.warning("schema change %s -> %s: closing %s", closed.schema.schema_id,
                        fed.switched_to.schema_id, closed.run_id)
            self._tick_run(closed.run_id, final=True)
        for sample in fed.samples:
            ingest_sample(self.conn, sample)
            self._latest_ts[sample.run_id] = sample.ts
        if fed.samples:
            self._wake.set()

    async def _consume(self) -> None:
        async for env in self.source.envelopes():
            self._handle(self.detector.feed(self.clock.stamp(env)))
        self._handle(self.detector.finish())

    async def _compile_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.max_interval_s)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            self._tick_once()

    def _tick_run(self, run_id: str, final: bool = False) -> None:
        latest = self._latest_ts.get(run_id)
        if latest is None:
            return
        # "now" is the data's own time: replays at any speed close segments as at 1x. A final
        # tick (source ended / run closed) looks far enough ahead to close everything.
        tick(self.conn, run_id, self.classifiers, now_ts=latest + (3600.0 if final else 0.0))

    def _tick_once(self, final: bool = False) -> None:
        if self.run_id is not None:
            self._tick_run(self.run_id, final=final)
        self.tick_count += 1

    def counters(self) -> dict:
        """Totals for status/logging: per run plus messages that matched no schema."""
        streams = self.detector.closed_streams + ([self.detector.stream] if self.detector.stream else [])
        return {
            "runs": [{"run_id": st.run_id, "schema_id": st.schema.schema_id, "seen": st.counters.seen,
                      "stored": st.counters.stored, "duplicate": st.counters.duplicate,
                      "rejected": st.counters.rejected, "seq_conflicts": st.counters.seq_conflicts,
                      "reject_samples": st.counters.reject_samples} for st in streams],
            "unknown": {"rejected": self.detector.unknown.rejected,
                        "reject_samples": self.detector.unknown.reject_samples},
            "clock_jumps": self.clock.clock_jumps,
        }

    async def run(self) -> None:
        """Runs until the source is exhausted (a finite ReplaySource) or forever (a live
        source like Ros2Source). Always ticks once more after the source ends, looking far
        enough ahead to close whatever is still open."""
        compile_task = asyncio.create_task(self._compile_loop())
        try:
            await self._consume()
        finally:
            compile_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await compile_task
            self._tick_once(final=True)


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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    source = Ros2Source(topic=args.topic)
    compiler = LiveCompiler(conn, args.run_id, source, ALL_CLASSIFIERS, max_interval_s=args.max_interval_s)

    try:
        asyncio.run(compiler.run())
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
