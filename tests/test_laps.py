import pytest

from evil.classifiers.laps import LapsClassifier
from evil.classifiers.runner import tick
from evil.classifiers.segments import SegmentsClassifier
from evil.ingest import ingest_sample
from evil.models import GpsReading, JoulemeterReading, RawSample
from track_fixtures import ll, seed_line

LINE_LAT, LINE_LON, LINE_RADIUS = 42.0, -76.0, 30.0
BOTH = lambda: [SegmentsClassifier(), LapsClassifier()]


def _seed_start_finish(conn):
    conn.execute(
        "INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (?, ?, ?)",
        (LINE_LAT, LINE_LON, LINE_RADIUS),
    )
    conn.commit()


def _at(x_m):
    """Longitude x metres east of the start/finish line (lat 42)."""
    return ll(x_m, 0.0)[1]


def _gps_sample(run_id, ts, x_m, speed=None, joulemeter=None):
    return RawSample(
        run_id=run_id,
        ts=ts,
        gps=GpsReading(ts=ts, lat=LINE_LAT, lon=_at(x_m), speed=speed),
        joulemeter=joulemeter,
    )


def test_lap_stays_open_until_crossing_is_safely_visible(conn):
    """laps depends_on "segments", so every tick() call must register both."""
    _seed_start_finish(conn)
    ingest_sample(conn, _gps_sample("run-1", 0.0, -100.0))  # outside, initializes lap 1

    tick(conn, "run-1", BOTH(), now_ts=2.5)

    assert conn.execute("SELECT COUNT(*) AS n FROM laps").fetchone()["n"] == 0
    state = conn.execute("SELECT * FROM laps_open_state WHERE run_id = 'run-1'").fetchone()
    assert state["lap_number"] == 1
    assert state["currently_inside"] == 0


def test_lap_closes_once_the_crossing_is_safely_visible(conn):
    _seed_start_finish(conn)
    ingest_sample(conn, _gps_sample("run-1", 0.0, -100.0))
    tick(conn, "run-1", BOTH(), now_ts=2.5)

    ingest_sample(conn, _gps_sample("run-1", 1.0, 0.0))  # crossing into the line
    tick(conn, "run-1", BOTH(), now_ts=4.0)  # cutoff ts<=2.0: now safe

    lap = conn.execute("SELECT * FROM laps WHERE run_id = 'run-1'").fetchone()
    assert lap is not None
    assert lap["lap_number"] == 1
    assert lap["start_seq"] == 1
    assert lap["end_seq"] == 2

    state = conn.execute("SELECT * FROM laps_open_state WHERE run_id = 'run-1'").fetchone()
    assert state["lap_number"] == 2  # next lap opened at the same crossing


def test_lap_counts_a_turn_committed_in_the_same_tick_and_measures_efficiency(conn):
    """The DAG-ordering proof: segments and laps both process a batch in one tick(). The order passed to
    tick() is deliberately reversed -- the runner must sequence by the DAG (segments before laps). Energy,
    distance and efficiency use RED's formulas."""
    _seed_start_finish(conn)
    seed_line(conn, turn_from_x=95.0, turn_to_x=135.0)       # a turn 95..135 m east of the line

    jm = lambda ts: JoulemeterReading(ts=ts, voltage=48.0, current=2.0)   # 96 W
    ingest_sample(conn, _gps_sample("run-1", 0.0, -100.0, speed=20.0))                   # outside; opens lap 1
    ingest_sample(conn, _gps_sample("run-1", 1.0, 0.0, speed=15.0, joulemeter=jm(1.0)))   # crossing: closes lap 1, opens lap 2
    ingest_sample(conn, _gps_sample("run-1", 2.0, 60.0, speed=8.0, joulemeter=jm(2.0)))
    ingest_sample(conn, _gps_sample("run-1", 3.0, 110.0, speed=9.0, joulemeter=jm(3.0)))  # crosses the turn's entry gate
    ingest_sample(conn, _gps_sample("run-1", 4.0, 170.0, speed=12.0, joulemeter=jm(4.0)))  # crosses its exit gate
    ingest_sample(conn, _gps_sample("run-1", 5.0, 100.0, speed=12.0, joulemeter=jm(5.0)))
    ingest_sample(conn, _gps_sample("run-1", 6.0, 40.0, speed=12.0, joulemeter=jm(6.0)))
    ingest_sample(conn, _gps_sample("run-1", 7.0, 0.0, speed=12.0, joulemeter=jm(7.0)))   # crossing: closes lap 2

    tick(conn, "run-1", [LapsClassifier(), SegmentsClassifier()], now_ts=20.0)

    turn = conn.execute("SELECT * FROM turns WHERE run_id = 'run-1'").fetchone()
    assert turn is not None

    lap2 = conn.execute("SELECT * FROM laps WHERE run_id = 'run-1' AND lap_number = 2").fetchone()
    assert lap2 is not None
    assert lap2["turn_count"] == 1
    assert lap2["duration_s"] == pytest.approx(6.0)
    assert lap2["energy_wh"] == pytest.approx(0.096 * 6.0 / 3600 * 1000)         # 96 W for 6 s, trapezoid
    assert lap2["distance_m"] == pytest.approx(60 + 50 + 60 + 70 + 60 + 40, rel=0.01)   # fix to fix, 0 -> 170 -> 0
    assert lap2["avg_speed"] == pytest.approx(lap2["distance_m"] / 6.0)
    assert lap2["efficiency_mi_per_kwh"] == pytest.approx(
        lap2["distance_m"] / 1609.344 / (lap2["energy_wh"] / 1000))

    lap1 = conn.execute("SELECT * FROM laps WHERE run_id = 'run-1' AND lap_number = 1").fetchone()
    assert lap1["turn_count"] == 0
    assert lap1["energy_wh"] is None  # fewer than 2 joulemeter rows in range
    assert lap1["efficiency_mi_per_kwh"] is None


def test_no_start_finish_line_means_no_laps_classified(conn):
    ingest_sample(conn, _gps_sample("run-1", 0.0, 0.0))

    tick(conn, "run-1", BOTH(), now_ts=10.0)

    assert conn.execute("SELECT COUNT(*) AS n FROM laps").fetchone()["n"] == 0
