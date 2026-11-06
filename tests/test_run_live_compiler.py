"""Proves the live compiler classifies incrementally as messages trickle in
(via a delayed ReplaySource), not only once at shutdown -- the actual point
of the event-driven wake design over blind fixed-interval polling -- and that
live messages go through the same strict pipeline as uploads.
"""

import asyncio

from evil.classifiers.segments import SegmentsClassifier
from evil.ingestion.pipeline import Envelope
from evil.ingestion.replay_source import ReplaySource
from evil.scripts.run_live_compiler import LiveCompiler
from telemetry_fixtures import payload
from track_fixtures import ll, seed_line

CENTER_LAT = 42.0


def _seed_turn(conn):
    seed_line(conn, turn_from_x=-50.0, turn_to_x=50.0)     # a turn 100 m long, then a straight


def _lon(x_m):
    return ll(x_m, 0.0)[1]


T0 = 1_775_000_000.0


def _gps_sample(ts, lon, speed, seq=None):
    """A strict v1 message at data time T0 + ts (publish stamp = receive stamp)."""
    seq = seq if seq is not None else 2 + int(ts * 10)
    p = payload(seq=seq, lat=CENTER_LAT, lon=lon, speed=speed, publish_ns=int((T0 + ts) * 1e9))
    return Envelope(T0 + ts, p)


def test_compiler_ticks_incrementally_while_samples_are_still_arriving(conn):
    """Messages are 3 s apart in data time and arrive 0.15 s apart in wall time.
    Sleeps 0.5s (3 arrived: outside, entry, inside), then checks mid-run state:
    the turn should already be open (classification happened DURING the run,
    once the entry is older than the 2 s lookback in DATA time) but not yet
    closed (nothing closes it until the exit message). tick_count > 1 in that
    run proves it was wake-driven, not the 10s fallback interval."""
    _seed_turn(conn)
    samples = [
        _gps_sample(0.0, _lon(-100.0), 20.0),  # outside
        _gps_sample(3.0, _lon(-25.0), 8.0),  # entry (crosses the turn's entry gate)
        _gps_sample(6.0, _lon(0.0), 9.0),  # inside
        _gps_sample(9.0, _lon(75.0), 7.0),  # exit (crosses the exit gate)
    ]
    source = ReplaySource(samples, delay_s=0.15)
    compiler = LiveCompiler(conn, "run-1", source, [SegmentsClassifier()], max_interval_s=10.0, lock_after=1,
                            batch_s=0.02)

    async def scenario():
        task = asyncio.create_task(compiler.run())

        await asyncio.sleep(0.5)
        mid_run_open = conn.execute(
            "SELECT COUNT(*) AS n FROM segments_open_state WHERE run_id = 'run-1' AND ordinal IS NOT NULL"
        ).fetchone()["n"]
        mid_run_closed = conn.execute(
            "SELECT COUNT(*) AS n FROM turns WHERE run_id = 'run-1'"
        ).fetchone()["n"]

        await asyncio.wait_for(task, timeout=5.0)
        return mid_run_open, mid_run_closed

    mid_run_open, mid_run_closed = asyncio.run(scenario())

    assert mid_run_open == 1
    assert mid_run_closed == 0
    assert compiler.tick_count > 1

    final_turn = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert final_turn is not None
    assert final_turn["entry_speed"] == 8.0
    assert final_turn["exit_speed"] == 7.0
    assert final_turn["duration_s"] > 0


def test_final_tick_after_source_ends_catches_trailing_data(conn):
    """No delay: both samples land before the compile loop's first wake
    wait even starts, so only the shutdown tick can classify them."""
    _seed_turn(conn)
    samples = [
        _gps_sample(0.0, _lon(-100.0), 20.0),  # outside
        _gps_sample(1.0, _lon(-25.0), 8.0),  # entry
        _gps_sample(2.0, _lon(75.0), 7.0),  # exit -- arrives right as the source ends
    ]
    source = ReplaySource(samples, delay_s=0.0)
    compiler = LiveCompiler(conn, "run-1", source, [SegmentsClassifier()], max_interval_s=10.0, lock_after=1,
                            batch_s=0.02)

    asyncio.run(asyncio.wait_for(compiler.run(), timeout=5.0))

    turn = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert turn is not None
    assert turn["exit_speed"] == 7.0


def test_a_schema_change_mid_stream_closes_the_run_and_starts_another(conn):
    from telemetry_fixtures import v2_payload
    envs = [Envelope(T0 + i * 0.02, payload(seq=2 + 2 * i, publish_ns=int((T0 + i * 0.02) * 1e9))) for i in range(30)]
    envs += [Envelope(T0 + 1 + i * 0.02, v2_payload(seq=2 + 2 * i, publish_ns=int((T0 + 1 + i * 0.02) * 1e9)))
             for i in range(30)]
    compiler = LiveCompiler(conn, "live-1", ReplaySource(envs), [SegmentsClassifier()], max_interval_s=10.0)
    asyncio.run(asyncio.wait_for(compiler.run(), timeout=5.0))

    assert compiler.runs == [("live-1", "evil.telemetry.v1"), ("live-1-2", "evil.telemetry.v2")]
    counts = dict(conn.execute("SELECT run_id, COUNT(*) FROM main_snapshot GROUP BY run_id").fetchall())
    assert counts == {"live-1": 30, "live-1-2": 30}
    assert conn.execute("SELECT COUNT(errcount) FROM main_snapshot WHERE run_id = 'live-1-2'").fetchone()[0] == 30


def test_a_stream_matching_no_schema_stores_nothing_and_counts_it(conn):
    envs = [Envelope(T0 + i, {"gps": {"lat": 42.0, "lon": -76.0}, "speed": 5.0}) for i in range(50)]
    compiler = LiveCompiler(conn, "live-1", ReplaySource(envs), [SegmentsClassifier()], max_interval_s=10.0)
    asyncio.run(asyncio.wait_for(compiler.run(), timeout=5.0))

    assert compiler.runs == [] and compiler.run_id is None
    assert conn.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 0
    assert compiler.counters()["unknown"]["rejected"] == 50


def test_writes_are_committed_in_batches_and_loss_is_reported(conn):
    envs = [Envelope(T0 + i * 0.02, payload(seq=2 + 2 * i, publish_ns=int((T0 + i * 0.02) * 1e9)))
            for i in range(130) if not 60 <= i < 63]                    # 3 messages lost upstream
    compiler = LiveCompiler(conn, "live-1", ReplaySource(envs), [SegmentsClassifier()], max_interval_s=10.0,
                            batch_rows=25)
    asyncio.run(asyncio.wait_for(compiler.run(), timeout=5.0))
    assert conn.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 127
    assert 9 <= compiler.commits <= 12          # (127 rows + 127 raw messages) / 25 per batch, + the tail
    loss = compiler.counters()["loss"]
    assert (loss["holes"], loss["missing"]) == (1, 3)
    assert "lost~3" in compiler.status_line()


def test_a_slow_trickle_is_committed_within_the_batch_window(tmp_path):
    """Fewer than batch_rows messages: the batch timer commits them, so another connection
    sees them while the source is still open (no sleeps to line up: the source waits for the
    test before it ends)."""
    import sqlite3
    import time as _time

    from evil import db
    path = str(tmp_path / "live.db")
    conn = db.connect(path)
    db.apply_schema(conn)
    envs = [Envelope(T0 + i * 0.02, payload(seq=2 + 2 * i, publish_ns=int((T0 + i * 0.02) * 1e9))) for i in range(3)]
    release = asyncio.Event()

    class HeldSource:
        async def envelopes(self):
            for env in envs:
                yield env
            await release.wait()                    # stays open until the test has looked

    compiler = LiveCompiler(conn, "live-1", HeldSource(), [SegmentsClassifier()], max_interval_s=10.0,
                            lock_after=1, batch_rows=25, batch_s=0.05)

    async def scenario():
        task = asyncio.create_task(compiler.run())
        other = sqlite3.connect(path)
        deadline = _time.monotonic() + 5.0
        seen = 0
        while _time.monotonic() < deadline:
            await asyncio.sleep(0.02)
            seen = other.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0]
            if seen == 3:
                break
        other.close()
        release.set()
        await task
        return seen

    assert asyncio.run(scenario()) == 3
