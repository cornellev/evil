"""The job worker: runs scans and parses off the persistent `jobs` table in
catalog.db, so uploads return as soon as files are stored.

- Two lanes, FIFO within each: `fast` (cheap scans, never blocked behind a
  multi-GB parse) and `deep` (parses). One job at a time per lane. Parsed DBs
  are written only by the deep lane (single writer).
- Durable: queue state lives in the catalog, so a restart loses nothing. At
  startup, jobs left `running` with an old heartbeat are put back to `pending`
  (or `failed` once they are out of attempts).
- Isolated: each parse runs in a subprocess with a timeout (evil.parse_job).
- A parse waits for the same recording's scan to finish (the run date comes
  from the scan).
- Idempotent: re-running a parse deletes that run's rows first.

Run with `python -m evil.worker` (docker-compose's `worker` service).
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from evil import catalog, scan

log = logging.getLogger("evil.worker")

LANE_KINDS = {"fast": ("scan",), "deep": ("parse",)}
HEARTBEAT_SEC = 5.0
STALE_AFTER_SEC = 60.0
POLL_SEC = 1.0
PARSE_TIMEOUT_SEC = float(os.getenv("EVIL_PARSE_TIMEOUT_SEC", str(2 * 3600)))


@dataclass
class Job:
    job_id: int
    recording_id: str
    kind: str
    lane: str
    attempts: int
    max_attempts: int


def claim_next(cat, lane: str) -> Job | None:
    """Atomically take the oldest eligible pending job in a lane."""
    kinds = LANE_KINDS[lane]
    marks = ",".join("?" * len(kinds))
    cat.execute("BEGIN IMMEDIATE")
    try:
        row = cat.execute(
            f"""SELECT * FROM jobs j
                WHERE j.lane = ? AND j.status = 'pending' AND j.kind IN ({marks})
                  AND j.attempts < j.max_attempts
                  AND NOT (j.kind = 'parse' AND EXISTS (
                        SELECT 1 FROM jobs s WHERE s.recording_id = j.recording_id AND s.kind = 'scan'
                          AND s.status IN ('pending', 'running')))
                ORDER BY j.created_at, j.job_id LIMIT 1""",
            (lane, *kinds),
        ).fetchone()
        if row is None:
            cat.rollback()
            return None
        now = time.time()
        cat.execute(
            "UPDATE jobs SET status = 'running', attempts = attempts + 1, started_at = ?, heartbeat_at = ?, "
            "progress = 0, error = NULL WHERE job_id = ?", (now, now, row["job_id"]))
        if row["kind"] == "parse":
            cat.execute("UPDATE recordings SET parse_status = 'running' WHERE recording_id = ?", (row["recording_id"],))
        cat.commit()
    except BaseException:
        cat.rollback()
        raise
    return Job(row["job_id"], row["recording_id"], row["kind"], row["lane"], row["attempts"] + 1, row["max_attempts"])


def _heartbeat(cat, job_id: int, progress: float | None) -> None:
    with cat:
        cat.execute("UPDATE jobs SET heartbeat_at = ?, progress = COALESCE(?, progress) WHERE job_id = ?",
                    (time.time(), progress, job_id))


def _complete(cat, job: Job, error: str | None) -> None:
    """Mark a job done, or retry/fail it. A failed parse leaves the recording
    `failed` with the error once attempts are exhausted; before that it goes back
    to `pending` so the retry is visible as a normal queued job."""
    with cat:
        if error is None:
            cat.execute("UPDATE jobs SET status = 'done', progress = 1, finished_at = ?, error = NULL WHERE job_id = ?",
                        (time.time(), job.job_id))
            return
        final = job.attempts >= job.max_attempts
        cat.execute("UPDATE jobs SET status = ?, finished_at = ?, error = ? WHERE job_id = ?",
                    ("failed" if final else "pending", time.time() if final else None, error, job.job_id))
        if job.kind == "parse":
            if final:
                cat.execute("UPDATE recordings SET parse_status = 'failed', parse_error = ? WHERE recording_id = ?",
                            (error, job.recording_id))
            else:
                cat.execute("UPDATE recordings SET parse_status = 'pending' WHERE recording_id = ?", (job.recording_id,))


def reset_stale(cat, stale_after: float = STALE_AFTER_SEC, now: float | None = None) -> int:
    """Jobs left `running` by a worker that died: back to pending, or failed if out of attempts."""
    now = time.time() if now is None else now
    cutoff = now - stale_after
    with cat:
        rows = cat.execute("SELECT * FROM jobs WHERE status = 'running' AND COALESCE(heartbeat_at, 0) < ?", (cutoff,)).fetchall()
        for r in rows:
            final = r["attempts"] >= r["max_attempts"]
            cat.execute("UPDATE jobs SET status = ?, error = ?, finished_at = ? WHERE job_id = ?",
                        ("failed" if final else "pending", "worker stopped while this job was running",
                         now if final else None, r["job_id"]))
            if r["kind"] == "parse":
                cat.execute("UPDATE recordings SET parse_status = ?, parse_error = ? WHERE recording_id = ?",
                            ("failed" if final else "pending",
                             "worker stopped while parsing" if final else None, r["recording_id"]))
    return len(rows)


def _child_env() -> dict[str, str]:
    """The parse child must import this same `evil` package however the worker was started."""
    import evil
    src = str(Path(evil.__file__).resolve().parents[1])
    existing = os.environ.get("PYTHONPATH")
    return {**os.environ, "PYTHONPATH": src + (os.pathsep + existing if existing else "")}


def default_parse_command(root: catalog.DataRoot, recording_id: str) -> list[str]:
    return [sys.executable, "-m", "evil.parse_job", recording_id, "--root", str(root.path)]


class Worker:
    def __init__(self, root: catalog.DataRoot, *, parse_timeout: float = PARSE_TIMEOUT_SEC,
                 heartbeat_sec: float = HEARTBEAT_SEC,
                 parse_command: Callable[[catalog.DataRoot, str], Sequence[str]] = default_parse_command):
        self.root = root
        self.parse_timeout = parse_timeout
        self.heartbeat_sec = heartbeat_sec
        self.parse_command = parse_command
        self.stop = threading.Event()

    # -- one job ----------------------------------------------------------
    def _run_scan(self, job: Job) -> str | None:
        # Scans run in this process; a heartbeat thread keeps a long scan of a
        # big bag from looking like a dead worker.
        stop = threading.Event()

        def beat() -> None:
            cat = catalog.connect_catalog(self.root)
            try:
                while not stop.wait(self.heartbeat_sec):
                    _heartbeat(cat, job.job_id, None)
            finally:
                cat.close()

        beater = threading.Thread(target=beat, daemon=True)
        beater.start()
        try:
            scan.scan_recording(self.root, job.recording_id)
            return None
        except Exception as exc:
            log.exception("scan failed for %s", job.recording_id)
            return f"{type(exc).__name__}: {exc}"
        finally:
            stop.set()
            beater.join(timeout=2)

    def _run_parse(self, cat, job: Job) -> str | None:
        """Parse in a subprocess with a timeout; keep the job's heartbeat/progress fresh."""
        err_log = self.root.spool / f"parse-{job.job_id}.log"
        progress = {"value": 0.0}
        with open(err_log, "w") as err:
            proc = subprocess.Popen(list(self.parse_command(self.root, job.recording_id)),
                                    stdout=subprocess.PIPE, stderr=err, text=True, env=_child_env())

            def read_stdout() -> None:
                for line in proc.stdout:
                    if line.startswith("PROGRESS "):
                        try:
                            progress["value"] = float(line.split()[1])
                        except ValueError:
                            pass

            reader = threading.Thread(target=read_stdout, daemon=True)
            reader.start()
            deadline = time.monotonic() + self.parse_timeout
            while True:
                try:
                    proc.wait(timeout=self.heartbeat_sec)
                    break
                except subprocess.TimeoutExpired:
                    if time.monotonic() > deadline:
                        proc.kill()
                        proc.wait()
                        reader.join(timeout=2)
                        err_log.unlink(missing_ok=True)
                        return f"timed out after {self.parse_timeout:.0f}s"
                    _heartbeat(cat, job.job_id, progress["value"])
            reader.join(timeout=2)
        tail = err_log.read_text()[-800:].strip()
        err_log.unlink(missing_ok=True)
        if proc.returncode != 0:
            last = tail.splitlines()[-1] if tail else f"exit code {proc.returncode}"
            return f"parse process failed ({proc.returncode}): {last}"
        return None

    def run_job(self, cat, job: Job) -> None:
        error = self._run_scan(job) if job.kind == "scan" else self._run_parse(cat, job)
        _complete(cat, job, error)

    # -- loops ------------------------------------------------------------
    def run_once(self, lane: str) -> bool:
        """Run at most one job in a lane. True if one ran."""
        cat = catalog.connect_catalog(self.root)
        try:
            job = claim_next(cat, lane)
            if job is None:
                return False
            self.run_job(cat, job)
            return True
        finally:
            cat.close()

    def drain(self, max_jobs: int = 10_000) -> int:
        """Run every runnable job (both lanes, fast first) until none are left. For tests and scripts."""
        n = 0
        while n < max_jobs and (self.run_once("fast") or self.run_once("deep")):
            n += 1
        return n

    def _lane_loop(self, lane: str) -> None:
        while not self.stop.is_set():
            try:
                ran = self.run_once(lane)
            except Exception:
                log.exception("worker %s lane error", lane)
                ran = False
            if not ran:
                self.stop.wait(POLL_SEC)

    def start(self) -> list[threading.Thread]:
        catalog.ensure_parsed_db(self.root)
        cat = catalog.connect_catalog(self.root)
        try:
            # Exactly one worker process runs, so anything still `running` at startup was
            # left by a previous instance: requeue it now instead of waiting out the heartbeat.
            reset = reset_stale(cat, stale_after=0.0)
        finally:
            cat.close()
        if reset:
            log.info("put %d stale jobs back in the queue", reset)
        threads = [threading.Thread(target=self._lane_loop, args=(lane,), name=f"lane-{lane}", daemon=True)
                   for lane in LANE_KINDS]
        for t in threads:
            t.start()
        return threads


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    root = catalog.data_root_from_env()
    root.ensure()
    worker = Worker(root)
    signal.signal(signal.SIGTERM, lambda *_: worker.stop.set())
    signal.signal(signal.SIGINT, lambda *_: worker.stop.set())
    threads = worker.start()
    log.info("worker started (data root %s)", root.path)
    while not worker.stop.is_set():
        worker.stop.wait(60)
        cat = catalog.connect_catalog(root)
        try:
            reset_stale(cat)  # a lane that hung without heartbeating
        finally:
            cat.close()
    for t in threads:
        t.join(timeout=10)
    return 0


if __name__ == "__main__":
    sys.exit(main())
