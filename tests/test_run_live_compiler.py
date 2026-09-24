"""Proves the live compiler classifies incrementally as samples trickle in
(via a delayed ReplaySource), not only once at shutdown -- the actual point
of the event-driven wake design over blind fixed-interval polling.
"""

import asyncio

from evil.classifiers.turns import TurnsClassifier
from evil.ingestion.replay_source import ReplaySource
from evil.models import GpsReading, RawSample
from evil.scripts.run_live_compiler import LiveCompiler

CENTER_LAT, CENTER_LON, RADIUS_M = 42.0, -76.0, 50.0


def _seed_turn(conn):
    conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('T1', ?, ?, ?)",
        (CENTER_LAT, CENTER_LON, RADIUS_M),
    )
    conn.commit()


def _gps_sample(ts, lon, speed):
    return RawSample(run_id="run-1", ts=ts, gps=GpsReading(ts=ts, lat=CENTER_LAT, lon=lon, speed=speed))


def test_compiler_ticks_incrementally_while_samples_are_still_arriving(conn):
    _seed_turn(conn)
    samples = [
        _gps_sample(0.0, -76.01, 20.0),  # outside
        _gps_sample(1.0, -76.0003, 8.0),  # entry
        _gps_sample(2.0, -76.0000, 9.0),  # inside
        _gps_sample(3.0, -75.999, 7.0),  # exit
    ]
    source = ReplaySource(samples, delay_s=0.15)
    compiler = LiveCompiler(conn, "run-1", source, [TurnsClassifier()], max_interval_s=10.0)

    async def scenario():
        task = asyncio.create_task(compiler.run())

        # after ~2 samples (entry included) have arrived, well before the
        # 4th (exit) sample or the 10s fallback interval
        await asyncio.sleep(0.35)
        mid_run_open = conn.execute(
            "SELECT COUNT(*) AS n FROM turns_open_state WHERE run_id = 'run-1'"
        ).fetchone()["n"]
        mid_run_closed = conn.execute(
            "SELECT COUNT(*) AS n FROM turns WHERE run_id = 'run-1'"
        ).fetchone()["n"]

        await asyncio.wait_for(task, timeout=5.0)
        return mid_run_open, mid_run_closed

    mid_run_open, mid_run_closed = asyncio.run(scenario())

    # proves classification happened DURING the run: the turn had opened by
    # the midpoint even though nothing closes it until the exit sample
    assert mid_run_open == 1
    assert mid_run_closed == 0
    # proves it was wake-driven, not the 10s fallback: multiple ticks fit
    # inside this ~0.6s run
    assert compiler.tick_count > 1

    final_turn = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert final_turn is not None
    assert final_turn["entry_speed"] == 8.0
    assert final_turn["exit_speed"] == 7.0


def test_final_tick_after_source_ends_catches_trailing_data(conn):
    _seed_turn(conn)
    samples = [
        _gps_sample(0.0, -76.0003, 8.0),  # entry
        _gps_sample(1.0, -75.999, 7.0),  # exit -- arrives right as the source ends
    ]
    # no delay: both samples land before the compile loop's first wake wait
    # even starts, so only the shutdown tick can classify them
    source = ReplaySource(samples, delay_s=0.0)
    compiler = LiveCompiler(conn, "run-1", source, [TurnsClassifier()], max_interval_s=10.0)

    asyncio.run(asyncio.wait_for(compiler.run(), timeout=5.0))

    turn = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert turn is not None
    assert turn["exit_speed"] == 7.0
