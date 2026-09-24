import pytest

from evil.classifiers.base import ClassifierSpec
from evil.classifiers.runner import get_cursor, tick
from evil.ingest import ingest_sample
from evil.models import RawSample


class RecordingClassifier:
    def __init__(self, name, depends_on=("main_snapshot",), lookback_margin_s=0.0, version=1):
        self.spec = ClassifierSpec(
            name=name, version=version, depends_on=list(depends_on), lookback_margin_s=lookback_margin_s
        )
        self.calls: list[tuple[int, int]] = []

    def run(self, conn, run_id, since_seq, until_seq):
        self.calls.append((since_seq, until_seq))


class RaisingClassifier:
    spec = ClassifierSpec(name="boom", version=1, depends_on=["main_snapshot"], lookback_margin_s=0.0)

    def run(self, conn, run_id, since_seq, until_seq):
        conn.execute(
            "INSERT INTO turns_open_state (run_id, turn_def_id, start_seq, start_ts) VALUES ('run-1', 1, 1, 1.0)"
        )
        raise RuntimeError("boom")


def _ingest_n(conn, run_id, n, start_ts=0.0, step=1.0):
    for i in range(n):
        ingest_sample(conn, RawSample(run_id=run_id, ts=start_ts + i * step))


def test_tick_only_processes_rows_past_lookback_margin(conn):
    _ingest_n(conn, "run-1", 5, start_ts=0.0, step=1.0)  # ts = 0,1,2,3,4 -> seq 1..5
    classifier = RecordingClassifier("c", lookback_margin_s=2.0)

    advanced = tick(conn, "run-1", [classifier], now_ts=4.0)

    # safe cutoff = now(4.0) - margin(2.0) = 2.0 -> rows with ts <= 2.0 -> seq 1..3
    assert classifier.calls == [(0, 3)]
    assert advanced == {"c": 3}
    assert get_cursor(conn, "run-1", "c", 1) == 3


def test_tick_is_idempotent_when_no_new_safe_data(conn):
    _ingest_n(conn, "run-1", 5, start_ts=0.0, step=1.0)
    classifier = RecordingClassifier("c", lookback_margin_s=2.0)

    tick(conn, "run-1", [classifier], now_ts=4.0)
    second = tick(conn, "run-1", [classifier], now_ts=4.0)

    assert classifier.calls == [(0, 3)]  # not called again
    assert second == {}


def test_tick_advances_as_time_passes(conn):
    _ingest_n(conn, "run-1", 5, start_ts=0.0, step=1.0)
    classifier = RecordingClassifier("c", lookback_margin_s=2.0)

    tick(conn, "run-1", [classifier], now_ts=4.0)  # -> up to seq 3 (ts <= 2.0)
    tick(conn, "run-1", [classifier], now_ts=6.0)  # -> up to seq 5 (ts <= 4.0)

    assert classifier.calls == [(0, 3), (3, 5)]


def test_dependent_classifier_waits_for_upstream_cursor(conn):
    _ingest_n(conn, "run-1", 5, start_ts=0.0, step=1.0)
    upstream = RecordingClassifier("turns", lookback_margin_s=2.0)
    downstream = RecordingClassifier("laps", depends_on=["turns"], lookback_margin_s=0.0)

    # order passed in deliberately reversed -- the runner must still sequence
    # by dependency, not by list order
    tick(conn, "run-1", [downstream, upstream], now_ts=4.0)

    assert upstream.calls == [(0, 3)]
    # downstream is bounded by upstream's cursor (3), not by raw data availability (5)
    assert downstream.calls == [(0, 3)]


def test_missing_dependency_raises_a_clear_error_not_a_bare_keyerror(conn):
    _ingest_n(conn, "run-1", 5, start_ts=0.0, step=1.0)
    downstream = RecordingClassifier("laps", depends_on=["turns"], lookback_margin_s=0.0)

    with pytest.raises(ValueError, match="depends_on 'turns'"):
        tick(conn, "run-1", [downstream], now_ts=4.0)


def test_failed_classifier_run_does_not_advance_cursor(conn):
    _ingest_n(conn, "run-1", 5, start_ts=0.0, step=1.0)
    conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('T1', 0, 0, 10)"
    )
    conn.commit()
    classifier = RaisingClassifier()

    with pytest.raises(RuntimeError):
        tick(conn, "run-1", [classifier], now_ts=4.0)

    assert get_cursor(conn, "run-1", "boom", 1) == 0
    # the write inside the failed transaction must be rolled back, not just the cursor
    assert conn.execute("SELECT COUNT(*) AS n FROM turns_open_state").fetchone()["n"] == 0
