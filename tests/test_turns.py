from evil.classifiers.runner import tick
from evil.classifiers.turns import TurnsClassifier
from evil.ingest import ingest_sample
from evil.models import GpsReading, RawSample

CENTER_LAT = 42.0
CENTER_LON = -76.0
RADIUS_M = 50.0


def _seed_turn(conn):
    conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES (?, ?, ?, ?)",
        ("T1", CENTER_LAT, CENTER_LON, RADIUS_M),
    )
    conn.commit()


def _gps_sample(run_id, ts, lon, speed):
    return RawSample(run_id=run_id, ts=ts, gps=GpsReading(ts=ts, lat=CENTER_LAT, lon=lon, speed=speed))


def test_turn_closes_in_one_tick_once_safely_past_exit(conn):
    _seed_turn(conn)
    # outside (~750m) -> inside (~25m, entry) -> inside (center) -> outside (~83m, exit)
    ingest_sample(conn, _gps_sample("run-1", 0.0, -76.01, speed=20.0))
    ingest_sample(conn, _gps_sample("run-1", 1.0, -76.0003, speed=8.0))
    ingest_sample(conn, _gps_sample("run-1", 2.0, -76.0000, speed=9.0))
    ingest_sample(conn, _gps_sample("run-1", 3.0, -75.999, speed=7.0))

    tick(conn, "run-1", [TurnsClassifier()], now_ts=6.0)  # margin 2s, cutoff ts<=4.0: all 4 rows safe

    row = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert row is not None
    assert row["start_seq"] == 2
    assert row["end_seq"] == 4
    assert row["entry_speed"] == 8.0
    assert row["exit_speed"] == 7.0
    assert conn.execute("SELECT COUNT(*) AS n FROM turns_open_state").fetchone()["n"] == 0


def test_turn_stays_open_until_exit_is_safely_visible(conn):
    """now_ts=3.0 with a 2.0s lookback_margin_s gives cutoff ts<=1.0, so only
    the entry sample (seq 1,2) is safe to process yet -- the exit sample at
    ts=2.0 isn't, so the turn must stay open, not close early."""
    _seed_turn(conn)
    ingest_sample(conn, _gps_sample("run-1", 0.0, -76.01, speed=20.0))
    ingest_sample(conn, _gps_sample("run-1", 1.0, -76.0003, speed=8.0))
    ingest_sample(conn, _gps_sample("run-1", 2.0, -76.0000, speed=9.0))

    tick(conn, "run-1", [TurnsClassifier()], now_ts=3.0)

    assert conn.execute("SELECT COUNT(*) AS n FROM turns").fetchone()["n"] == 0
    open_state = conn.execute("SELECT * FROM turns_open_state WHERE run_id = 'run-1'").fetchone()
    assert open_state is not None
    assert open_state["start_seq"] == 2

    # now the exit point becomes safely visible
    ingest_sample(conn, _gps_sample("run-1", 3.0, -75.999, speed=7.0))
    tick(conn, "run-1", [TurnsClassifier()], now_ts=6.0)

    row = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert row is not None
    assert row["exit_speed"] == 7.0
    assert conn.execute("SELECT COUNT(*) AS n FROM turns_open_state").fetchone()["n"] == 0
