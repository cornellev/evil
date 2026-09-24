import sqlite3

import pytest

from evil.tools.compare_turn_instances import compare_turn_instances
from evil.tools.get_turn import get_turn
from evil.tools.read_only_sql import read_only_sql


def test_get_turn_defaults_to_most_recent_occurrence(readonly_conn):
    result = get_turn(readonly_conn, run_id="run-1", turn_name="Turn 3")

    assert result["found"] is True
    assert result["start_ts"] == 40.0
    assert result["speed_delta"] == pytest.approx(0.5)


def test_get_turn_first_occurrence(readonly_conn):
    result = get_turn(readonly_conn, run_id="run-1", turn_name="Turn 3", occurrence="first")

    assert result["start_ts"] == 10.0
    assert result["speed_delta"] == pytest.approx(-2.0)


def test_get_turn_not_found_is_reported_not_raised(readonly_conn):
    assert get_turn(readonly_conn, run_id="run-1", turn_name="Turn 99") == {"found": False}


def test_get_turn_accepts_a_bare_number_like_a_real_model_sends(readonly_conn):
    # gemma4:e4b, asked "how was turn 3", called get_turn(turn_name="3")
    result = get_turn(readonly_conn, run_id="run-1", turn_name="3")

    assert result["found"] is True
    assert result["turn_name"] == "Turn 3"


def test_compare_turn_instances_finds_best_exit_speed(readonly_conn):
    result = compare_turn_instances(readonly_conn, run_id="run-1", turn_name="Turn 3")

    assert len(result["instances"]) == 2
    assert result["best_exit_speed_instance"]["start_ts"] == 40.0


def test_compare_turn_instances_handles_no_data(readonly_conn):
    result = compare_turn_instances(readonly_conn, run_id="run-1", turn_name="Turn 99")

    assert result == {"instances": [], "best_exit_speed_instance": None}


def test_read_only_sql_returns_rows(readonly_conn):
    rows = read_only_sql(readonly_conn, "SELECT turn_name FROM track_geometry")

    assert rows == [{"turn_name": "Turn 3"}]


def test_read_only_sql_respects_row_limit(readonly_conn):
    rows = read_only_sql(readonly_conn, "SELECT * FROM turns ORDER BY start_seq", row_limit=1)

    assert len(rows) == 1


def test_read_only_sql_allows_with_ctes(readonly_conn):
    rows = read_only_sql(
        readonly_conn,
        "WITH t AS (SELECT * FROM turns) SELECT COUNT(*) AS n FROM t",
    )

    assert rows == [{"n": 2}]


def test_read_only_sql_rejects_non_select(readonly_conn):
    with pytest.raises(ValueError, match="SELECT"):
        read_only_sql(readonly_conn, "DELETE FROM turns")


def test_read_only_sql_rejects_multiple_statements(readonly_conn):
    with pytest.raises(ValueError, match="one statement"):
        read_only_sql(readonly_conn, "SELECT 1; DROP TABLE turns;")


def test_read_only_sql_step_budget_guard_aborts_runaway_queries(readonly_conn):
    huge_query = (
        "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 500000) "
        "SELECT COUNT(*) FROM cnt"
    )

    with pytest.raises(ValueError, match="step budget"):
        read_only_sql(readonly_conn, huge_query, max_steps=1)


def test_read_only_sql_connection_cannot_write_even_if_the_check_were_bypassed(readonly_conn):
    # defense in depth: the connection itself is opened mode=ro, independent
    # of the keyword filter above
    with pytest.raises(sqlite3.OperationalError):
        readonly_conn.execute("DELETE FROM turns")
