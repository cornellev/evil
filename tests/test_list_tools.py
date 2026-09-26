"""Unit tests for the browsing-shaped tools (list_runs/list_turns/list_laps/
list_straights) -- distinct from get_turn/compare_turn_instances, which
require already knowing what you're looking for.
"""

from evil.tools.list_laps import list_laps
from evil.tools.list_runs import list_runs
from evil.tools.list_straights import list_straights
from evil.tools.list_turns import list_turns


def test_list_runs_returns_every_run_with_counts_and_range(evil_db_path):
    """list_runs reads main_snapshot, which the shared readonly_conn fixture
    never populates (it seeds turns/track_geometry directly) -- needs its
    own setup through the real ingest path."""
    from evil import db
    from evil.ingest import ingest_sample
    from evil.models import RawSample

    conn = db.connect(evil_db_path)
    ingest_sample(conn, RawSample(run_id="run-1", ts=0.0))
    ingest_sample(conn, RawSample(run_id="run-1", ts=5.0))
    conn.close()

    readonly = db.connect_readonly(evil_db_path)
    try:
        runs = list_runs(readonly)
    finally:
        readonly.close()

    assert len(runs) == 1
    assert runs[0]["run_id"] == "run-1"
    assert runs[0]["sample_count"] == 2
    assert runs[0]["start_ts"] == 0.0
    assert runs[0]["end_ts"] == 5.0


def test_list_runs_empty_db_returns_empty_list(conn):
    assert list_runs(conn) == []


def test_list_turns_paginates(readonly_conn):
    first_page = list_turns(readonly_conn, run_id="run-1", limit=1, offset=0)
    assert first_page["total"] == 2
    assert len(first_page["turns"]) == 1

    second_page = list_turns(readonly_conn, run_id="run-1", limit=1, offset=1)
    assert len(second_page["turns"]) == 1
    assert first_page["turns"][0]["turn_id"] != second_page["turns"][0]["turn_id"]


def test_list_turns_unknown_run_returns_empty(readonly_conn):
    result = list_turns(readonly_conn, run_id="no-such-run")
    assert result == {"total": 0, "turns": []}


def test_list_laps_and_list_straights_shape(evil_db_path):
    from evil import db

    conn = db.connect(evil_db_path)
    conn.execute(
        "INSERT INTO laps (run_id, lap_number, start_seq, end_seq, start_ts, end_ts, turn_count) "
        "VALUES ('run-1', 1, 1, 10, 0.0, 10.0, 2)"
    )
    conn.execute(
        "INSERT INTO straights (run_id, start_seq, end_seq, start_ts, end_ts) "
        "VALUES ('run-1', 10, 20, 10.0, 20.0)"
    )
    conn.commit()
    conn.close()

    readonly = db.connect_readonly(evil_db_path)
    try:
        laps_result = list_laps(readonly, run_id="run-1")
        assert laps_result["total"] == 1
        assert laps_result["laps"][0]["lap_number"] == 1

        straights_result = list_straights(readonly, run_id="run-1")
        assert straights_result["total"] == 1
        assert straights_result["straights"][0]["start_seq"] == 10
    finally:
        readonly.close()
