"""Proves the live compiler classifies incrementally as samples trickle in
(via a delayed ReplaySource), not only once at shutdown -- the actual point
of the event-driven wake design over blind fixed-interval polling.
"""

import asyncio

from evil.classifiers.segments import SegmentsClassifier
from evil.ingestion.replay_source import ReplaySource
from evil.models import GpsReading, RawSample
from evil.scripts.run_live_compiler import LiveCompiler
from track_fixtures import ll, seed_line

CENTER_LAT = 42.0


def _seed_turn(conn):
    seed_line(conn, turn_from_x=-50.0, turn_to_x=50.0)     # a turn 100 m long, then a straight


def _lon(x_m):
    return ll(x_m, 0.0)[1]


def _gps_sample(ts, lon, speed):
    return RawSample(run_id="run-1", ts=ts, gps=GpsReading(ts=ts, lat=CENTER_LAT, lon=lon, speed=speed))


def test_compiler_ticks_incrementally_while_samples_are_still_arriving(conn):
    """Sleeps 0.35s, well after ~2 samples (entry included) have arrived but
    before the 4th (exit) sample or the 10s fallback interval, then checks
    mid-run state: the turn should already be open (proving classification
    happened DURING the run, not only at shutdown) but not yet closed (since
    nothing closes it until the exit sample). tick_count > 1 in that ~0.6s
    run proves it was wake-driven, not the 10s fallback interval."""
    _seed_turn(conn)
    samples = [
        _gps_sample(0.0, _lon(-100.0), 20.0),  # outside
        _gps_sample(1.0, _lon(-25.0), 8.0),  # entry (crosses the turn's entry gate)
        _gps_sample(2.0, _lon(0.0), 9.0),  # inside
        _gps_sample(3.0, _lon(75.0), 7.0),  # exit (crosses the exit gate)
    ]
    source = ReplaySource(samples, delay_s=0.15)
    compiler = LiveCompiler(conn, "run-1", source, [SegmentsClassifier()], max_interval_s=10.0)

    async def scenario():
        task = asyncio.create_task(compiler.run())

        await asyncio.sleep(0.35)
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
    compiler = LiveCompiler(conn, "run-1", source, [SegmentsClassifier()], max_interval_s=10.0)

    asyncio.run(asyncio.wait_for(compiler.run(), timeout=5.0))

    turn = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert turn is not None
    assert turn["exit_speed"] == 7.0
