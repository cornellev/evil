import pytest

from evil.classifiers.runner import tick
from evil.classifiers.straights import StraightsClassifier
from evil.classifiers.turns import TurnsClassifier
from evil.ingest import ingest_sample
from evil.models import GpsReading, RawSample

TURN_A = (42.0, -76.0, 30.0)
TURN_B = (42.0, -75.98, 30.0)


def _seed_two_turns(conn):
    conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('A', ?, ?, ?)",
        TURN_A,
    )
    conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('B', ?, ?, ?)",
        TURN_B,
    )
    conn.commit()


def _gps(ts, lon, speed):
    return RawSample(run_id="run-1", ts=ts, gps=GpsReading(ts=ts, lat=42.0, lon=lon, speed=speed))


def test_straight_created_between_two_consecutive_turns(conn):
    _seed_two_turns(conn)
    points = [
        _gps(0.0, -76.001, 20.0),  # outside both
        _gps(1.0, -76.000, 8.0),  # turn A entry
        _gps(2.0, -75.9995, 9.0),  # turn A exit
        _gps(3.0, -75.981, 15.0),  # mid-straight
        _gps(4.0, -75.980, 10.0),  # turn B entry
        _gps(5.0, -75.9795, 11.0),  # turn B exit
    ]
    for p in points:
        ingest_sample(conn, p)

    tick(conn, "run-1", [TurnsClassifier(), StraightsClassifier()], now_ts=20.0)

    assert conn.execute("SELECT COUNT(*) AS n FROM turns WHERE run_id = 'run-1'").fetchone()["n"] == 2

    straight = conn.execute("SELECT * FROM straights WHERE run_id = 'run-1'").fetchone()
    assert straight is not None
    assert straight["start_seq"] == 3
    assert straight["end_seq"] == 5
    assert straight["entry_speed"] == 9.0  # turn A's exit speed
    assert straight["exit_speed"] == 10.0  # turn B's entry speed
    assert straight["avg_speed"] == pytest.approx((15.0 + 10.0) / 2)


def test_first_turn_in_a_run_produces_no_straight(conn):
    _seed_two_turns(conn)
    points = [_gps(0.0, -76.001, 20.0), _gps(1.0, -76.000, 8.0), _gps(2.0, -75.9995, 9.0)]
    for p in points:
        ingest_sample(conn, p)

    tick(conn, "run-1", [TurnsClassifier(), StraightsClassifier()], now_ts=20.0)

    assert conn.execute("SELECT COUNT(*) AS n FROM turns WHERE run_id = 'run-1'").fetchone()["n"] == 1
    assert conn.execute("SELECT COUNT(*) AS n FROM straights WHERE run_id = 'run-1'").fetchone()["n"] == 0


def test_straight_finalizes_only_once_the_second_turn_is_safely_visible(conn):
    """The incremental proof: no straight exists after the first tick (only
    one turn visible yet), one appears after the second tick (once the
    second turn closes), without ever re-deriving the first turn's data."""
    _seed_two_turns(conn)
    for p in [_gps(0.0, -76.001, 20.0), _gps(1.0, -76.000, 8.0), _gps(2.0, -75.9995, 9.0)]:
        ingest_sample(conn, p)
    tick(conn, "run-1", [TurnsClassifier(), StraightsClassifier()], now_ts=5.0)

    assert conn.execute("SELECT COUNT(*) AS n FROM straights").fetchone()["n"] == 0

    for p in [_gps(3.0, -75.981, 15.0), _gps(4.0, -75.980, 10.0), _gps(5.0, -75.9795, 11.0)]:
        ingest_sample(conn, p)
    tick(conn, "run-1", [TurnsClassifier(), StraightsClassifier()], now_ts=20.0)

    straight = conn.execute("SELECT * FROM straights WHERE run_id = 'run-1'").fetchone()
    assert straight is not None
    assert straight["start_seq"] == 3
    assert straight["end_seq"] == 5
