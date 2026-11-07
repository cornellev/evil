"""Live compiler service: consumes envelopes from an IngestionSource, runs them through
the same pipeline uploads use (schema detection, validation, repeat drop, mapping:
ingestion/pipeline.py), ingests each stored sample, and runs tick() across the registered
classifiers, woken by ingestion with a bounded max-interval fallback.

Sessions (live-next-steps-2026-11.md section C). A run is written only while a session is
open; messages outside one still update the status (and loss meter) but are not stored.
  - bag mode (BAG_SERVICE_URL set and reachable): a session is open exactly while the car's
    bag service reports the rosbag container running, i.e. RED's record button. One live
    run = one onboard bag.
  - gap mode (no bag service, or it has been unreachable for UNREACHABLE_AFTER polls): a
    session opens on the first message and closes after GAP_S seconds of silence. A
    recording in progress is never dropped because the bag service went away.
  - inside a session a new run starts on a schema change (DAQ reflashed: LiveDetector) or a
    DAQ reboot (`global_ts` and `seq` going backwards). end_reason records why.
  - restart: a run still marked 'recording' is resumed if its bag is still the running one
    (bag mode) or its last message is younger than GAP_S (gap mode); otherwise it is closed
    as 'interrupted'. SIGTERM therefore commits and ticks but leaves the run open.
  - close: final tick, run_summary, `runs` row closed with counters; with a data root the
    received messages (live_raw) are exported as a live capture recording in the catalog
    and linked to the full upload of the same drive if that is already parsed.

Schema: detected from the first LOCK_AFTER messages of a session (v1 / v2 / v1-dutycycle;
"neither" is rejected and counted, never stored).

Time: rows are stamped by LiveClock (publisher time on EVIL's clock), and classifiers tick
with the latest sample time as "now", so a replay at 10x classifies exactly like 1x.

Writes: rows are inserted into an open transaction and committed in batches (BATCH_ROWS
rows or BATCH_S seconds after the first uncommitted row, whichever comes first), then the
classifiers are woken. Loss: LossMeter estimates messages lost upstream from publish-stamp
gaps; the source's own counters (ThreadBridge overflow) say what EVIL dropped itself. Both
are in counters() and logged every STATUS_EVERY_S.

Transport-agnostic: works with ReplaySource (tests) or Ros2Source (production) identically.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import gzip
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from evil import catalog, db, runs, schemas
from evil.bag_service import BagServiceClient, BagState
from evil.classifiers import metrics as run_metrics
from evil.classifiers.base import Classifier
from evil.classifiers.runner import tick
from evil.ingest import insert_sample
from evil.ingestion.base import IngestionSource
from evil.ingestion.pipeline import LOCK_AFTER, SWITCH_AFTER, Envelope, Fed, LiveClock, LiveDetector, LossMeter
from evil.registered_classifiers import ALL_CLASSIFIERS
from evil.schemas._strict import StrictSchema

log = logging.getLogger("evil.live")

DEFAULT_MAX_INTERVAL_S = 5.0
BATCH_ROWS = 250           # writes (rows + raw messages) per commit at most; at 50 Hz BATCH_S commits first
BATCH_S = 0.5              # every write is committed within this (the freshness bound)
STATUS_EVERY_S = 10.0
SILENCE_WARN_S = 5.0       # warn when a recording gets no data for this long
GAP_S = 60.0               # gap mode: silence that ends a session
POLL_S = 1.0               # bag service poll interval
UNREACHABLE_AFTER = 3      # consecutive failed polls before falling back to gap mode
SUMMARY_EVERY_S = 10.0     # run_summary refresh while recording (data time)
HEARTBEAT_EVERY_S = 2.0    # live_heartbeat row rewrite
REBOOT_BACK_US = 1_000_000  # DAQ global_ts going back more than this (with seq going back) = reboot
REBOOT_HOLDOFF_S = 30.0     # after a reboot split, no further split for this long: counters flipping
                            # back and forth means two publishers on the topic, not a reboot storm


class Session:
    """One open recording window. Holds the current run and the per-session detection state."""

    def __init__(self, mode: str, bag_id: str | None, detector: LiveDetector):
        self.mode = mode                 # 'bag' | 'gap'
        self.bag_id = bag_id
        self.detector = detector
        self.clock = LiveClock()
        self.run_id: str | None = None   # assigned when the session opens
        self.last_recv: float | None = None
        self.opened_at: float | None = None   # compiler clock when the session opened
        self.silent_warned = False       # "no data while recording" logged for the current silence
        self.last_daq: tuple[int, int] | None = None   # (seq, global_ts) of the last conforming message


class LiveCompiler:
    def __init__(
        self,
        conn,
        run_id: str | None,
        source: IngestionSource,
        classifiers: list[Classifier],
        max_interval_s: float = DEFAULT_MAX_INTERVAL_S,
        lock_after: int = LOCK_AFTER,
        switch_after: int = SWITCH_AFTER,
        batch_rows: int = BATCH_ROWS,
        batch_s: float = BATCH_S,
        status_every_s: float = STATUS_EVERY_S,
        *,
        car: str = "uc26",
        bag: BagServiceClient | None = None,
        gap_s: float = GAP_S,
        poll_s: float = POLL_S,
        data_root: catalog.DataRoot | None = None,
        main_db: Path | None = None,
        now=time.time,
    ):
        """run_id: a fixed name for the first run (tests); None = `live-<car>-<UTC start>`."""
        self.conn = conn
        self.base_run_id = run_id
        self.source = source
        self.classifiers = classifiers
        self.max_interval_s = max_interval_s
        self.lock_after = lock_after
        self.switch_after = switch_after
        self.batch_rows = batch_rows
        self.batch_s = batch_s
        self.status_every_s = status_every_s
        self.car = car
        self.bag = bag
        self.gap_s = gap_s
        self.poll_s = poll_s
        self.data_root = data_root
        self.main_db = main_db
        self.now = now
        self.tick_count = 0
        self.commits = 0
        self.runs: list[tuple[str, str]] = []          # (run_id, schema_id) of runs that locked a schema
        self.loss = LossMeter()
        self.session: Session | None = None
        self.bag_state: BagState | None = None
        self.bag_failures = 0
        self.unknown_rejected = 0
        self.unknown_samples: list[dict] = []
        self._streams: dict[str, object] = {}           # run_id -> SchemaStream (for counters)
        self._run_meta: dict[str, dict] = {}            # run_id -> car_start/car_end/started_at
        self._n_runs = 0
        self._uncommitted = 0
        self._flush_handle: asyncio.TimerHandle | None = None
        self._latest_ts: dict[str, float] = {}
        self._last_summary = 0.0
        self._totals: dict[str, run_metrics.RunTotals] = {}   # incremental run_summary per open run
        self._last_reboot_split: float | None = None
        self.counter_jumps_ignored = 0
        self._last_heartbeat = 0.0
        self.last_message_at: float | None = None
        self.received = 0
        self._rate_mark: tuple[float, int] = (time.monotonic(), 0)
        self.msgs_per_s = 0.0
        self._wake = asyncio.Event()
        db.apply_schema(conn)
        # WAL + synchronous=NORMAL: commits stop fsyncing the WAL every time (measured 2.4 ms per
        # commit, most of the cost at high rates). Still crash-safe (no corruption); a power cut
        # can lose at most the last batch, which the onboard bag has anyway.
        conn.execute("PRAGMA synchronous = NORMAL")

    # ---- state -----------------------------------------------------------

    @property
    def run_id(self) -> str | None:
        """The run currently being written (None when not recording)."""
        return self.session.run_id if self.session else None

    @property
    def bag_mode(self) -> bool:
        """True while session boundaries follow the bag service."""
        return self.bag is not None and self.bag_failures < UNREACHABLE_AFTER and self.bag_state is not None

    def _new_name(self) -> str:
        self._n_runs += 1
        if self.base_run_id:
            name = self.base_run_id if self._n_runs == 1 else f"{self.base_run_id}-{self._n_runs}"
        else:
            name = f"live-{self.car}-{datetime.fromtimestamp(self.now(), tz=timezone.utc):%Y%m%d-%H%M%S}"
        base, n = name, 1
        while self.conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (name,)).fetchone():
            n += 1
            name = f"{base}-{n}"
        return name

    # ---- sessions and runs ----------------------------------------------

    def _open_session(self, mode: str, bag_id: str | None, reason: str) -> Session:
        session = Session(mode, bag_id, None)
        session.opened_at = self.now()
        session.detector = LiveDetector(lambda schema: self._on_lock(session, schema),
                                        lock_after=self.lock_after, switch_after=self.switch_after)
        self.session = session
        self._open_run(session)
        log.info("session open (%s, %s%s): run %s", mode, reason, f", bag {bag_id[:12]}" if bag_id else "",
                 session.run_id)
        self.heartbeat()
        return session

    def _open_run(self, session: Session) -> None:
        session.run_id = self._new_name()
        self._run_meta[session.run_id] = {}
        with self.conn:
            runs.upsert_run(self.conn, session.run_id, tier="live", source="ros2", schema_id=None,
                            status="recording", started_at=None, ended_at=None,
                            session_mode=session.mode, bag_id=session.bag_id)

    def _on_lock(self, session: Session, schema: StrictSchema) -> str:
        """LiveDetector callback: the current run gets its schema, or (a switch) the run
        closes and a new one in the same session takes the held messages."""
        if session.detector.stream is not None:            # a switch: the old stream is still current here
            old = session.run_id
            log.warning("schema change %s -> %s: closing %s", session.detector.stream.schema.schema_id,
                        schema.schema_id, old)
            self._open_run(session)
            # the held messages that triggered the switch belong to the new run (raw first, then close+export)
            self.conn.execute(
                "UPDATE live_raw SET run_id = ? WHERE id IN "
                "(SELECT id FROM live_raw WHERE run_id = ? ORDER BY id DESC LIMIT ?)",
                (session.run_id, old, self.switch_after))
            self._close_run(old, "schema_change")
        self.runs.append((session.run_id, schema.schema_id))
        self.conn.execute("UPDATE runs SET schema_id = ?, updated_at = ? WHERE run_id = ?",
                          (schema.schema_id, time.time(), session.run_id))
        log.info("run %s: schema %s", session.run_id, schema.schema_id)
        return session.run_id

    def _close_session(self, reason: str) -> None:
        session, self.session = self.session, None
        if session is None:
            return
        self._handle(session.detector.finish())
        self.unknown_rejected += session.detector.unknown.rejected
        self.unknown_samples = (self.unknown_samples + session.detector.unknown.reject_samples)[:5]
        self._close_run(session.run_id, reason)
        log.info("session closed (%s): run %s", reason, session.run_id)
        self.heartbeat()

    def _close_run(self, run_id: str, reason: str) -> None:
        self._flush()
        self._tick_run(run_id, final=True)
        has_rows = self._latest_ts.get(run_id) is not None or self.conn.execute(
            "SELECT 1 FROM main_snapshot WHERE run_id = ? LIMIT 1", (run_id,)).fetchone()
        if has_rows:
            run_metrics.store_run_summary(self.conn, run_id, self._totals.pop(run_id, None))
        stats = self._run_stats(run_id)
        meta = self._run_meta.get(run_id, {})
        with self.conn:
            self.conn.execute(
                """UPDATE runs SET status = 'closed', end_reason = ?, ended_at = COALESCE(?, ended_at),
                       started_at = COALESCE(?, started_at), car_start = COALESCE(?, car_start),
                       car_end = COALESCE(?, car_end), stats_json = ?, updated_at = ? WHERE run_id = ?""",
                (reason, self._latest_ts.get(run_id), meta.get("started_at"), meta.get("car_start"),
                 meta.get("car_end"), json.dumps(stats), time.time(), run_id))
        try:
            self._export(run_id, stats)
        except Exception:
            # The run is closed and its raw messages stay in live_raw: export_pending() retries.
            # An export problem must never stop the compiler from following the record button.
            log.exception("exporting run %s as a live capture failed; will retry", run_id)

    EXPORT_RETRY_S = 60.0

    def export_pending(self) -> int:
        """Retry exporting closed live runs that still have raw messages (a failed export, or
        a crash between closing and exporting). Returns how many were exported."""
        if self.data_root is None:
            return 0
        done = 0
        for row in self.conn.execute(
                "SELECT r.run_id, r.stats_json FROM runs r WHERE r.tier = 'live' AND r.status = 'closed' "
                "AND r.recording_id IS NULL AND EXISTS (SELECT 1 FROM live_raw l WHERE l.run_id = r.run_id)"
                ).fetchall():
            try:
                self._export(row["run_id"], json.loads(row["stats_json"] or "{}") or self._run_stats(row["run_id"]))
                done += 1
            except Exception:
                log.exception("retrying the export of %s failed", row["run_id"])
        return done

    def _run_stats(self, run_id: str) -> dict:
        st = self._streams.get(run_id)
        c = st.counters if st is not None else None
        return {"seen": c.seen if c else 0, "stored": c.stored if c else 0, "duplicate": c.duplicate if c else 0,
                "rejected": c.rejected if c else 0, "seq_conflicts": c.seq_conflicts if c else 0,
                "reject_samples": c.reject_samples if c else [], "loss": self.loss.stats(),
                "source": self.source.stats() if hasattr(self.source, "stats") else {}}

    def _export(self, run_id: str, stats: dict) -> None:
        """Write the run's received messages to the raw store as a live capture, catalog it,
        link it to its upload if parsed already, and drop live_raw. Needs a data root."""
        if self.data_root is None:
            return
        run = self.conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        n = self.conn.execute("SELECT COUNT(*) FROM live_raw WHERE run_id = ?", (run_id,)).fetchone()[0]
        if not n:
            return
        tmp = self.data_root.incoming / f"{run_id}{catalog.LIVE_CAPTURE_SUFFIX}"
        self.data_root.ensure()
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            for recv, text in self.conn.execute("SELECT recv_ts, text FROM live_raw WHERE run_id = ? ORDER BY id",
                                                (run_id,)):
                f.write(f"{recv:.6f}\t{text}\n")
        cat = catalog.connect_catalog(self.data_root)
        try:
            recording_id = catalog.register_live_capture(
                cat, self.data_root, run_id=run_id, car=self.car, capture=tmp, schema_id=run["schema_id"],
                recorded_start=run["car_start"], recorded_end=run["car_end"], rows_ingested=stats["stored"],
                rows_duplicate=stats["duplicate"], rows_rejected=stats["rejected"],
                stats={k: v for k, v in stats.items() if k != "reject_samples"},
                reject_samples=stats["reject_samples"])
            self._record_location(cat, recording_id, run_id)
            with self.conn:
                self.conn.execute("UPDATE runs SET recording_id = ? WHERE run_id = ?", (recording_id, run_id))
                self.conn.execute("DELETE FROM live_raw WHERE run_id = ?", (run_id,))
            if self.main_db is not None:
                linked = runs.link_live_to_upload(self.conn, self.main_db, run_id, cat)
                if linked:
                    log.info("run %s linked to upload %s", run_id, linked)
        finally:
            cat.close()
            tmp.unlink(missing_ok=True)
        log.info("run %s exported as live capture %s (%d messages)", run_id, recording_id, n)

    def _record_location(self, cat, recording_id: str, run_id: str) -> None:
        from evil import parser
        gps = parser._gps_summary(self.conn, run_id)
        if not gps:
            return
        loc = parser.match_location(cat, (gps["min_lat"] + gps["max_lat"]) / 2, (gps["min_lon"] + gps["max_lon"]) / 2)
        with cat:
            cat.execute("UPDATE recordings SET gps_min_lat = ?, gps_max_lat = ?, gps_min_lon = ?, gps_max_lon = ?, "
                        "location_id = COALESCE(?, location_id), location_method = CASE WHEN ? IS NULL "
                        "THEN location_method ELSE 'gps-match' END WHERE recording_id = ?",
                        (gps["min_lat"], gps["max_lat"], gps["min_lon"], gps["max_lon"], loc, loc, recording_id))
        catalog.apply_auto_category(cat)

    # ---- restart ---------------------------------------------------------

    def _resume_or_close_stale(self) -> None:
        """A run left 'recording' by a previous process: resume it if it is still the same
        recording, else close it as interrupted."""
        for row in self.conn.execute("SELECT * FROM runs WHERE tier = 'live' AND status = 'recording'").fetchall():
            last = self.conn.execute(
                "SELECT global_ts, device_seq, device_global_ts_us, publish_ns FROM main_snapshot "
                "WHERE run_id = ? ORDER BY seq DESC LIMIT 1", (row["run_id"],)).fetchone()
            last_recv = self.conn.execute("SELECT MAX(recv_ts) FROM live_raw WHERE run_id = ?",
                                          (row["run_id"],)).fetchone()[0]
            if self.bag_mode:
                same = (self.bag_state.running and row["session_mode"] == "bag"
                        and row["bag_id"] == self.bag_state.bag_id)
            else:
                same = last_recv is not None and self.now() - last_recv < self.gap_s
            if self.session is None and same:
                self._resume(row, last, last_recv)
            else:
                if last is not None:
                    self._latest_ts[row["run_id"]] = last["global_ts"]
                self._close_run(row["run_id"], "interrupted")
                log.info("run %s was left recording by a previous process: closed (interrupted)", row["run_id"])

    def _resume(self, row, last, last_recv) -> None:
        session = Session(row["session_mode"] or "gap", row["bag_id"], None)
        session.opened_at = self.now()
        session.detector = LiveDetector(lambda schema: self._on_lock(session, schema),
                                        lock_after=self.lock_after, switch_after=self.switch_after)
        session.run_id = row["run_id"]
        session.last_recv = last_recv
        self._n_runs += 1
        meta = self._run_meta.setdefault(row["run_id"], {})
        if row["schema_id"]:
            stream = session.detector.lock_to(schemas.BY_ID[row["schema_id"]], row["run_id"])
            self._streams[row["run_id"]] = stream
            self.runs.append((row["run_id"], row["schema_id"]))
        if last is not None:
            self._latest_ts[row["run_id"]] = last["global_ts"]
            if session.detector.stream is not None:
                session.detector.stream.resume_after(last["device_seq"], last["device_global_ts_us"])
            session.last_daq = (last["device_seq"], last["device_global_ts_us"])
            meta["car_end"] = last["publish_ns"] / 1e9 if last["publish_ns"] else None
        self.session = session
        log.info("resumed run %s (%s mode)", row["run_id"], session.mode)

    # ---- bag service -----------------------------------------------------

    def start(self, first_poll: BagState | None) -> None:
        """Startup order matters: learn the bag state, THEN resume or close a run left
        'recording' by a previous process, THEN follow the bag (which may open a new run)."""
        if first_poll is not None:
            self.apply_bag_state(first_poll, act=False)
        self._resume_or_close_stale()
        if first_poll is not None:
            self.apply_bag_state(first_poll)

    def apply_bag_state(self, state: BagState, act: bool = True) -> None:
        """One poll result: open/close sessions to follow the recording."""
        if not state.reachable:
            self.bag_failures += 1
            if self.bag_failures == UNREACHABLE_AFTER:
                log.warning("bag service unreachable (%s): gap-based sessions until it is back", state.detail)
            return
        if self.bag_failures >= UNREACHABLE_AFTER:
            log.info("bag service reachable again")
        self.bag_failures = 0
        self.bag_state = state
        if not act:
            return
        s = self.session
        if state.running:
            if s is None:
                self._open_session("bag", state.bag_id, "bag_start")
            elif s.bag_id is None:                  # opened in gap fallback: the bag is this recording
                s.mode, s.bag_id = "bag", state.bag_id
                with self.conn:
                    self.conn.execute("UPDATE runs SET session_mode = 'bag', bag_id = ? WHERE run_id = ?",
                                      (state.bag_id, s.run_id))
            elif s.bag_id != state.bag_id:          # stopped and restarted between two polls
                self._close_session("bag_stopped")
                self._open_session("bag", state.bag_id, "bag_start")
        elif s is not None:
            self._close_session("bag_stopped")

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self.poll_s)
            try:
                self.apply_bag_state(await self.bag.poll())
            except Exception:
                # Never let one bad close/open stop the compiler from following the record button.
                log.exception("handling the bag service state failed")

    async def _watch_loop(self) -> None:
        """Gap mode: close a session after gap_s of silence; retry pending exports."""
        last_retry = time.monotonic()
        while True:
            await asyncio.sleep(min(1.0, self.gap_s / 4))
            try:
                self.check_silence()
                self.check_gap()
                if time.monotonic() - last_retry >= self.EXPORT_RETRY_S:
                    last_retry = time.monotonic()
                    self.export_pending()
            except Exception:
                log.exception("session watch failed")

    def check_silence(self) -> None:
        """While a recording is open, warn once when the car has been silent for SILENCE_WARN_S
        (car off the tailnet, DAQ down, discovery not matched), and note when data is back."""
        s = self.session
        if s is None:
            return
        since = s.last_recv if s.last_recv is not None else s.opened_at
        if since is None:
            return
        quiet = self.now() - since
        if quiet > SILENCE_WARN_S and not s.silent_warned:
            s.silent_warned = True
            log.warning("no data for %.0f s while recording run %s (car off the tailnet? DAQ or discovery "
                        "server down?)", quiet, s.run_id)

    def check_gap(self) -> None:
        s = self.session
        if s is not None and not self.bag_mode and s.last_recv is not None and self.now() - s.last_recv > self.gap_s:
            self._close_session("gap")

    # ---- messages --------------------------------------------------------

    def _is_reboot(self, s: Session, env: Envelope) -> bool:
        p = env.payload
        if not isinstance(p, dict):
            return False
        seq, gts = p.get("seq"), p.get("global_ts")
        if not isinstance(seq, int) or not isinstance(gts, int):
            return False
        prev, s.last_daq = s.last_daq, (seq, gts)
        return prev is not None and seq < prev[0] and gts < prev[1] - REBOOT_BACK_US

    def _reboot_held_off(self) -> bool:
        if self._last_reboot_split is None or time.monotonic() - self._last_reboot_split > REBOOT_HOLDOFF_S:
            return False
        self.counter_jumps_ignored += 1
        if self.counter_jumps_ignored in (1, 100) or self.counter_jumps_ignored % 1000 == 0:
            log.warning("DAQ counters went back again %d times within %.0f s of a reboot split: two "
                        "publishers on the topic? not splitting again", self.counter_jumps_ignored,
                        REBOOT_HOLDOFF_S)
        return True

    def on_envelope(self, env: Envelope) -> None:
        self.loss.observe(env)
        self.last_message_at = env.ts
        self.received += 1
        s = self.session
        if s is None:
            if self.bag_mode:
                return                              # not recording: RED's button decides
            s = self._open_session("gap", None, "first_message")
        if self._is_reboot(s, env) and not self._reboot_held_off():
            self._last_reboot_split = time.monotonic()
            log.warning("DAQ reboot detected (seq/global_ts went back) in run %s", s.run_id)
            mode, bag_id, last_daq = s.mode, s.bag_id, s.last_daq
            self._close_session("daq_reboot")
            s = self._open_session(mode, bag_id, "daq_reboot")
            s.last_daq = last_daq
        if s.silent_warned:
            s.silent_warned = False
            log.info("data back in run %s", s.run_id)
        s.last_recv = env.ts
        text = env.raw if env.raw is not None else json.dumps(env.payload)
        self.conn.execute("INSERT INTO live_raw (run_id, recv_ts, text) VALUES (?, ?, ?)", (s.run_id, env.ts, text))
        self._uncommitted += 1
        fed = s.detector.feed(s.clock.stamp(env))
        if fed.locked is not None or fed.switched_to is not None:
            self._streams[s.detector.stream.run_id] = s.detector.stream
        self._handle(fed)

    def _handle(self, fed: Fed) -> None:
        for sample in fed.samples:
            insert_sample(self.conn, sample)
            self._latest_ts[sample.run_id] = sample.ts
            meta = self._run_meta.setdefault(sample.run_id, {})
            meta.setdefault("started_at", sample.ts)
            if sample.publish_ns:
                meta.setdefault("car_start", sample.publish_ns / 1e9)
                meta["car_end"] = sample.publish_ns / 1e9
            self._uncommitted += 1
        if not self._uncommitted:
            return
        if self._uncommitted >= self.batch_rows:
            self._flush()
        elif self._flush_handle is None:
            self._flush_handle = asyncio.get_running_loop().call_later(self.batch_s, self._flush)

    def _flush(self) -> None:
        """Commit the open batch and wake the classifiers."""
        if self._flush_handle is not None:
            self._flush_handle.cancel()
            self._flush_handle = None
        if self._uncommitted:
            self.conn.commit()
            self.commits += 1
            self._uncommitted = 0
            self._wake.set()

    async def _consume(self) -> None:
        async for env in self.source.envelopes():
            self.on_envelope(env)
        self._flush()

    # ---- classifiers and status -----------------------------------------

    async def _compile_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.max_interval_s)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            try:
                self._flush()            # tick() commits on this connection: close the batch first
                self._tick_once()
                if time.monotonic() - self._last_heartbeat >= HEARTBEAT_EVERY_S:
                    self.heartbeat()
            except Exception:
                log.exception("classifier tick / heartbeat failed")

    def heartbeat(self) -> None:
        """Rewrite the live_heartbeat row (readers: get_live_status, evil-ui)."""
        mono = time.monotonic()
        t0, n0 = self._rate_mark
        if mono - t0 >= 1.0:
            self.msgs_per_s = (self.received - n0) / (mono - t0)
            self._rate_mark = (mono, self.received)
        c = self.counters()
        status = {**c, "run_id": self.run_id, "last_message_at": self.last_message_at,
                  "msgs_per_s": round(self.msgs_per_s, 2), "received": self.received, "car": self.car,
                  "gap_s": self.gap_s}
        with self.conn:
            self.conn.execute("INSERT INTO live_heartbeat (id, updated_at, status_json) VALUES (1, ?, ?) "
                              "ON CONFLICT(id) DO UPDATE SET updated_at = excluded.updated_at, "
                              "status_json = excluded.status_json", (self.now(), json.dumps(status, default=str)))
        self._last_heartbeat = mono

    def _tick_run(self, run_id: str, final: bool = False) -> None:
        latest = self._latest_ts.get(run_id)
        if latest is None:
            return
        # "now" is the data's own time: replays at any speed close segments as at 1x. A final
        # tick (run closed) looks far enough ahead to close everything.
        tick(self.conn, run_id, self.classifiers, now_ts=latest + (3600.0 if final else 0.0))

    def _tick_once(self, final: bool = False) -> None:
        if self.run_id is not None:
            self._tick_run(self.run_id, final=final)
            latest = self._latest_ts.get(self.run_id)
            # every SUMMARY_EVERY_S of DATA time, like the classifiers: a 10x replay stays current
            if latest is not None and (latest - self._last_summary >= SUMMARY_EVERY_S or latest < self._last_summary):
                run_metrics.store_run_summary(self.conn, self.run_id,
                                              self._totals.setdefault(self.run_id, run_metrics.RunTotals()))
                self.conn.commit()
                self._last_summary = latest
        self.tick_count += 1

    def counters(self) -> dict:
        """Totals for status/logging: per run plus messages that matched no schema."""
        unknown = [s.detector.unknown for s in ([self.session] if self.session else [])]
        return {
            "runs": [{"run_id": rid, "schema_id": st.schema.schema_id, "seen": st.counters.seen,
                      "stored": st.counters.stored, "duplicate": st.counters.duplicate,
                      "rejected": st.counters.rejected, "seq_conflicts": st.counters.seq_conflicts,
                      "reject_samples": st.counters.reject_samples} for rid, st in self._streams.items()],
            "unknown": {"rejected": self.unknown_rejected + sum(u.rejected for u in unknown),
                        "reject_samples": (self.unknown_samples + [r for u in unknown for r in u.reject_samples])[:5]},
            "recording": self.session is not None,
            "session_mode": self.session.mode if self.session else None,
            "bag": {"configured": self.bag is not None, "reachable": self.bag_failures < UNREACHABLE_AFTER,
                    "running": self.bag_state.running if self.bag_state else None},
            "clock_jumps": self.session.clock.clock_jumps if self.session else 0,
            "counter_jumps_ignored": self.counter_jumps_ignored,
            "loss": self.loss.stats(),
            "source": self.source.stats() if hasattr(self.source, "stats") else {},
            "commits": self.commits,
        }

    async def _status_loop(self) -> None:
        while True:
            await asyncio.sleep(self.status_every_s)
            log.info("status %s", self.status_line())

    def status_line(self) -> str:
        c = self.counters()
        cur = next((r for r in c["runs"] if r["run_id"] == self.run_id), {}) if self.run_id else {}
        return (f"recording={c['recording']} mode={c['session_mode']} run={self.run_id} "
                f"schema={cur.get('schema_id')} stored={cur.get('stored', 0)} "
                f"dup={cur.get('duplicate', 0)} rejected={cur.get('rejected', 0)} unknown={c['unknown']['rejected']} "
                f"lost~{c['loss']['missing']} outages={c['loss']['outages']} received={c['source'].get('received', '-')} "
                f"overflow={c['source'].get('overflow', 0)}")

    # ---- lifecycle -------------------------------------------------------

    async def run(self) -> None:
        """Runs until the source is exhausted (a finite ReplaySource: the open session is
        closed as 'source_ended') or forever (Ros2Source). Cancellation (SIGTERM) commits
        and ticks but leaves the run 'recording', to be resumed or closed on restart."""
        self.start(await self.bag.poll() if self.bag is not None else None)
        tasks = [asyncio.create_task(self._compile_loop()), asyncio.create_task(self._status_loop()),
                 asyncio.create_task(self._watch_loop())]
        if self.bag is not None:
            tasks.append(asyncio.create_task(self._poll_loop()))

        def _died(task: asyncio.Task) -> None:
            if not task.cancelled() and task.exception() is not None:
                log.critical("background task %s died: %r", task.get_coro().__name__, task.exception(),
                             exc_info=task.exception())
        for task in tasks:
            task.add_done_callback(_died)
        self.export_pending()
        ended = False
        try:
            await self._consume()
            ended = True
        finally:
            for task in tasks:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            if ended:
                self._close_session("source_ended")
            else:
                self._flush()
                self._tick_once()
            self.heartbeat()
            log.info("stopped %s", self.status_line())


def main(argv: list[str] | None = None) -> int:
    """Production entry point: a live Ros2Source against ALL_CLASSIFIERS, writing the car's
    live DB under EVIL_DATA_ROOT. Requires rclpy (the ROS2 image)."""
    from evil.ingestion.ros2_source import Ros2Source

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=None, help="fixed name for the first run (default live-<car>-<UTC start>)")
    parser.add_argument("--car", default=os.getenv("EVIL_CAR", "uc26"))
    parser.add_argument("--db", default=os.getenv("EVIL_LIVE_DB_PATH"), dest="db_path",
                        help="live DB (default <data root>/parsed/<car>/evil_<car>_live.db)")
    parser.add_argument("--topic", default=os.getenv("EVIL_LIVE_TOPIC", "spi_data"))
    parser.add_argument("--bag-service", default=os.getenv("BAG_SERVICE_URL", ""),
                        help="the car's bag service, e.g. http://<car tailscale ip>:8080 (empty = gap mode)")
    parser.add_argument("--gap", type=float, default=float(os.getenv("EVIL_LIVE_GAP_S", GAP_S)), dest="gap_s")
    parser.add_argument("--max-interval", type=float, default=DEFAULT_MAX_INTERVAL_S, dest="max_interval_s")
    parser.add_argument("--qos-depth", type=int, default=int(os.getenv("EVIL_LIVE_QOS_DEPTH", "100")))
    parser.add_argument("--reliability", choices=("reliable", "best_effort"),
                        default=os.getenv("EVIL_LIVE_RELIABILITY", "reliable"))
    parser.add_argument("--queue-max", type=int, default=int(os.getenv("EVIL_LIVE_QUEUE_MAX", "5000")))
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    root = catalog.data_root_from_env()
    db_path = Path(args.db_path) if args.db_path else catalog.live_db_path(root, args.car)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(str(db_path))
    db.apply_schema(conn)
    main_db = catalog.parsed_db_path(root, args.car)
    _copy_reference_data(main_db, conn)
    source = Ros2Source(topic=args.topic, qos_depth=args.qos_depth, reliability=args.reliability,
                        queue_max=args.queue_max)
    bag = BagServiceClient(args.bag_service) if args.bag_service else None
    compiler = LiveCompiler(conn, args.run_id, source, ALL_CLASSIFIERS, max_interval_s=args.max_interval_s,
                            car=args.car, bag=bag, gap_s=args.gap_s, data_root=root, main_db=main_db)
    log.info("live DB %s, topic /%s, %s", db_path, args.topic,
             f"bag service {args.bag_service}" if bag else f"gap mode ({args.gap_s:.0f} s)")

    async def _serve() -> None:
        # SIGTERM (docker stop) and SIGINT cancel the run: the batch is committed and ticked,
        # the run stays 'recording' and is resumed (or closed as interrupted) on restart.
        import signal
        task = asyncio.current_task()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, task.cancel)
        with contextlib.suppress(asyncio.CancelledError):
            await compiler.run()

    try:
        asyncio.run(_serve())
    finally:
        conn.close()
    return 0


REFERENCE_TABLES = ("track_segments", "start_finish_line")


def _copy_reference_data(main_db: Path, live: sqlite3.Connection) -> None:
    """The live DB classifies against the same track as the main DB: copy its reference
    data (track segments, start/finish line) when the live DB has none yet."""
    if not main_db.exists():
        return
    have = live.execute("SELECT COUNT(*) FROM track_segments").fetchone()[0]
    if have:
        return
    live.commit()
    live.execute("ATTACH DATABASE ? AS main_ref", (str(main_db),))
    try:
        with live:
            for table in REFERENCE_TABLES:
                live.execute(f"INSERT INTO {table} SELECT * FROM main_ref.{table}")
    finally:
        live.execute("DETACH DATABASE main_ref")
    log.info("copied track reference data from %s", main_db)


if __name__ == "__main__":
    sys.exit(main())
