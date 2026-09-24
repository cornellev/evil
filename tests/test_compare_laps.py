import pytest

from evil.tools.compare_laps import compare_laps


def _seed_two_laps(conn):
    conn.execute(
        """INSERT INTO laps (run_id, lap_number, start_seq, end_seq, start_ts, end_ts,
                              turn_count, energy_wh, avg_speed)
           VALUES ('run-1', 1, 1, 10, 0.0, 10.0, 2, 5.0, 8.0)"""
    )
    conn.execute(
        """INSERT INTO laps (run_id, lap_number, start_seq, end_seq, start_ts, end_ts,
                              turn_count, energy_wh, avg_speed)
           VALUES ('run-1', 2, 10, 22, 10.0, 22.0, 3, 4.5, 9.0)"""
    )
    conn.commit()


def test_compare_laps_computes_deltas(conn):
    _seed_two_laps(conn)

    result = compare_laps(conn, run_id="run-1", lap_a=1, lap_b=2)

    assert result["found"] is True
    assert result["lap_a"]["lap_number"] == 1
    assert result["lap_b"]["lap_number"] == 2
    assert result["lap_a"]["duration_s"] == 10.0
    assert result["lap_b"]["duration_s"] == 12.0
    assert result["delta"]["duration_s"] == pytest.approx(2.0)
    assert result["delta"]["energy_wh"] == pytest.approx(-0.5)  # lap 2 used less energy
    assert result["delta"]["avg_speed"] == pytest.approx(1.0)


def test_compare_laps_missing_lap_reports_not_found(conn):
    _seed_two_laps(conn)

    result = compare_laps(conn, run_id="run-1", lap_a=1, lap_b=99)

    assert result == {"found": False}


def test_compare_laps_handles_null_metrics(conn):
    conn.execute(
        """INSERT INTO laps (run_id, lap_number, start_seq, end_seq, start_ts, end_ts, turn_count)
           VALUES ('run-1', 1, 1, 10, 0.0, 10.0, 0)"""
    )
    conn.execute(
        """INSERT INTO laps (run_id, lap_number, start_seq, end_seq, start_ts, end_ts, turn_count)
           VALUES ('run-1', 2, 10, 20, 10.0, 20.0, 0)"""
    )
    conn.commit()

    result = compare_laps(conn, run_id="run-1", lap_a=1, lap_b=2)

    assert result["found"] is True
    assert result["delta"]["energy_wh"] is None
    assert result["delta"]["avg_speed"] is None
