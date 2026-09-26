import pytest

from evil.classifiers.laps import LapsClassifier
from evil.classifiers.runner import tick
from evil.classifiers.turns import TurnsClassifier
from evil.ingest import ingest_sample
from evil.models import GpsReading, JoulemeterReading, RawSample

LINE_LAT, LINE_LON, LINE_RADIUS = 42.0, -76.0, 30.0


def _seed_start_finish(conn):
    conn.execute(
        "INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (?, ?, ?)",
        (LINE_LAT, LINE_LON, LINE_RADIUS),
    )
    conn.commit()


def _gps_sample(run_id, ts, lon, speed=None, joulemeter=None):
    return RawSample(
        run_id=run_id,
        ts=ts,
        gps=GpsReading(ts=ts, lat=LINE_LAT, lon=lon, speed=speed),
        joulemeter=joulemeter,
    )


def test_lap_stays_open_until_crossing_is_safely_visible(conn):
    """laps depends_on "turns", so every tick() call must register both --
    there's no track_geometry seeded here, so TurnsClassifier is a no-op."""
    _seed_start_finish(conn)
    ingest_sample(conn, _gps_sample("run-1", 0.0, -76.01))  # outside, initializes lap 1

    tick(conn, "run-1", [TurnsClassifier(), LapsClassifier()], now_ts=2.5)

    assert conn.execute("SELECT COUNT(*) AS n FROM laps").fetchone()["n"] == 0
    state = conn.execute("SELECT * FROM laps_open_state WHERE run_id = 'run-1'").fetchone()
    assert state["lap_number"] == 1
    assert state["currently_inside"] == 0


def test_lap_closes_once_the_crossing_is_safely_visible(conn):
    _seed_start_finish(conn)
    ingest_sample(conn, _gps_sample("run-1", 0.0, -76.01))
    tick(conn, "run-1", [TurnsClassifier(), LapsClassifier()], now_ts=2.5)

    ingest_sample(conn, _gps_sample("run-1", 1.0, -76.0))  # crossing into the line
    tick(conn, "run-1", [TurnsClassifier(), LapsClassifier()], now_ts=4.0)  # cutoff ts<=2.0: now safe

    lap = conn.execute("SELECT * FROM laps WHERE run_id = 'run-1'").fetchone()
    assert lap is not None
    assert lap["lap_number"] == 1
    assert lap["start_seq"] == 1
    assert lap["end_seq"] == 2

    state = conn.execute("SELECT * FROM laps_open_state WHERE run_id = 'run-1'").fetchone()
    assert state["lap_number"] == 2  # next lap opened at the same crossing


def test_lap_counts_a_turn_committed_in_the_same_tick(conn):
    """The real DAG-ordering proof: turns and laps both process a batch of
    rows in one tick() call. If the runner didn't guarantee turns commits
    before laps runs, this join would see an empty turns table. Order passed
    to tick() below is deliberately reversed -- the runner must sequence by
    the DAG (turns before laps), not by list order."""
    _seed_start_finish(conn)
    conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('T1', 42.0, -75.995, 30)"
    )
    conn.commit()

    jm = lambda ts: JoulemeterReading(ts=ts, voltage=48.0, current=2.0)
    ingest_sample(conn, _gps_sample("run-1", 0.0, -76.010, speed=20.0))  # outside everything; opens lap 1
    ingest_sample(conn, _gps_sample("run-1", 1.0, -76.000, speed=15.0, joulemeter=jm(1.0)))  # crossing: closes lap1, opens lap2
    ingest_sample(conn, _gps_sample("run-1", 2.0, -75.995, speed=8.0, joulemeter=jm(2.0)))  # turn entry
    ingest_sample(conn, _gps_sample("run-1", 3.0, -75.990, speed=9.0, joulemeter=jm(3.0)))  # turn exit
    ingest_sample(conn, _gps_sample("run-1", 4.0, -76.000, speed=12.0, joulemeter=jm(4.0)))  # crossing: closes lap2

    tick(conn, "run-1", [LapsClassifier(), TurnsClassifier()], now_ts=10.0)

    turn = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert turn is not None

    lap2 = conn.execute(
        "SELECT * FROM laps WHERE run_id = 'run-1' AND lap_number = 2"
    ).fetchone()
    assert lap2 is not None
    assert lap2["turn_count"] == 1
    assert lap2["avg_speed"] == pytest.approx((8.0 + 9.0 + 12.0) / 3)
    assert lap2["energy_wh"] == pytest.approx((96.0 * 1.0 + 96.0 * 1.0) / 3600.0)

    lap1 = conn.execute(
        "SELECT * FROM laps WHERE run_id = 'run-1' AND lap_number = 1"
    ).fetchone()
    assert lap1["turn_count"] == 0
    assert lap1["energy_wh"] is None  # fewer than 2 joulemeter rows in range


def test_no_start_finish_line_means_no_laps_classified(conn):
    ingest_sample(conn, _gps_sample("run-1", 0.0, -76.0))

    tick(conn, "run-1", [TurnsClassifier(), LapsClassifier()], now_ts=10.0)

    assert conn.execute("SELECT COUNT(*) AS n FROM laps").fetchone()["n"] == 0
