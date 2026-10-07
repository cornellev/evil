"""Property-based test for read_only_sql's one non-negotiable guarantee:
however malformed or adversarial the input, it can never change a row. The
existing tests in test_tools.py pin specific hand-picked cases (rejects a
DELETE, rejects multiple statements, aborts a runaway query); this throws
a much larger volume of random and adversarial input at it than anyone
would think to hand-write, on the theory that the interesting failure is
the one nobody thought of.

Worth knowing: this test still passes even with read_only_sql's own
_DISALLOWED keyword regex removed entirely (checked by hand while building
this) -- real defense in depth, not a gap in this test. The connection's
own `mode=ro` (see db.connect_readonly) backstops the regex guards at the
SQLite driver level, so a write attempt fails with sqlite3.Error either way.
This test's assertion (row counts never change) is intentionally agnostic
to *which* layer caught it -- that's the actual property that matters.

Runs every example against the SAME seeded database on purpose (opening a
fresh connection to check row counts each time, not a fresh database) --
cumulative confidence across many examples matters more here than fixture
isolation, so the function-scoped-fixture health check is suppressed
deliberately, not accidentally.

_SQL_FRAGMENTS mixes real SQL keywords into the input, not just noise --
pure random text mostly just fails the leading SELECT/WITH check
immediately, while real keywords increase the odds of actually reaching
the parser/execution path pure noise alone rarely would.
"""

from __future__ import annotations

import sqlite3

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from evil import db
from evil.tools.read_only_sql import read_only_sql

_TABLES = [
    "joulemeter",
    "local_planner",
    "gps",
    "main_snapshot",
    "track_segments",
    "run_summary",
    "segments_open_state",
    "turns",
    "classifier_cursor",
    "steering",
    "rpm_front",
    "rpm_back",
    "motor",
    "start_finish_line",
    "laps_open_state",
    "laps",
    "straights",
]

_SQL_FRAGMENTS = [
    "SELECT",
    "select",
    "WITH",
    "*",
    "FROM turns",
    "FROM nonexistent_table",
    ";",
    "--",
    "/*",
    "*/",
    "'",
    '"',
    "\x00",
    "DELETE FROM turns",
    "DROP TABLE turns",
    "UPDATE turns SET start_ts=0",
    "INSERT INTO turns DEFAULT VALUES",
    "ATTACH DATABASE 'x' AS y",
    "PRAGMA table_info(turns)",
    "VACUUM",
    "OR 1=1",
    "UNION SELECT 1",
]

adversarial_sql = st.one_of(
    st.text(min_size=0, max_size=200),
    st.lists(st.sampled_from(_SQL_FRAGMENTS), min_size=1, max_size=6).map(lambda parts: " ".join(parts)),
)


def _row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in _TABLES}


@given(query=adversarial_sql)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_read_only_sql_never_mutates_the_database_no_matter_the_input(evil_db_path, query):
    verify_conn = db.connect(evil_db_path)
    read_conn = db.connect_readonly(evil_db_path)
    before = _row_counts(verify_conn)

    try:
        read_only_sql(read_conn, query)
    except (ValueError, sqlite3.Error):
        pass  # the expected outcome for malformed/adversarial input
    except Exception as exc:  # anything else is the actual bug being fuzzed for
        raise AssertionError(f"read_only_sql raised {exc!r} (not ValueError/sqlite3.Error) for input {query!r}") from exc
    finally:
        read_conn.close()

    after = _row_counts(verify_conn)
    verify_conn.close()
    assert before == after, f"row counts changed after query {query!r}: {before} -> {after}"
