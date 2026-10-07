"""Throws random and adversarial SQL at the raw query sandbox and asserts the
one property that matters: nothing ever changes the target file, and every
failure is a clean RawQueryError (never a crash, a hang, or a stray exception).
Fewer examples than read_only_sql's fuzz test because each query is a real
subprocess."""

import hashlib
import sqlite3

from hypothesis import HealthCheck, given, settings, strategies as st

from evil import rawquery

_FRAGMENTS = [
    "SELECT", "select", "WITH", "*", "FROM topics", "FROM messages", "FROM sqlite_master", "WHERE", "1=1", "AND", "OR",
    "ORDER BY", "LIMIT 3", "GROUP BY", "UNION", "UNION ALL", "JOIN", ";", "--", "/*", "*/", "'", '"', "(", ")", ",",
    "DELETE", "DROP TABLE topics", "INSERT INTO topics VALUES (1,2,3)", "UPDATE messages SET data=NULL", "ATTACH DATABASE ':memory:' AS x",
    "PRAGMA writable_schema=1", "VACUUM", "load_extension('x')", "readfile('/etc/passwd')", "writefile('/tmp/x','y')", "zeroblob(100)",
    "randomblob(8)", "json_extract(cdr_string(data), '$.a')", "cdr_string(data)", "hex(data)", "length(data)", "count(*)",
    "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r WHERE x<5) SELECT * FROM r", "pragma_table_info('topics')",
    "REPLACE", "CREATE TABLE z(a)", "EXPLAIN", "BEGIN", "COMMIT", "\x00", "\n", "😀", "name", "id", "topic_id",
]


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.lists(st.one_of(st.sampled_from(_FRAGMENTS), st.text(max_size=12)), min_size=0, max_size=12))
def test_no_input_ever_changes_the_file_or_escapes_as_a_stray_exception(tmp_path_factory, parts):
    path = _FIXTURE.get("path")
    if path is None:
        path = tmp_path_factory.mktemp("fuzz") / "bag.db3"
        c = sqlite3.connect(path)
        c.execute("CREATE TABLE topics (id INTEGER PRIMARY KEY, name TEXT, type TEXT)")
        c.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, topic_id INTEGER, timestamp INTEGER, data BLOB)")
        c.execute("INSERT INTO topics VALUES (1, '/spi_data', 'std_msgs/msg/String')")
        c.executemany("INSERT INTO messages (topic_id, timestamp, data) VALUES (1, ?, ?)", [(i, b"\x00\x01\x00\x00\x03\x00\x00\x00ab\x00") for i in range(50)])
        c.commit()
        c.close()
        _FIXTURE["path"], _FIXTURE["digest"] = path, _digest(path)
    sql = " ".join(parts)
    try:
        out = rawquery.run_sql("sqlite-immutable", path, sql, limits=rawquery.QueryLimits(max_seconds=2.0, max_rows=20))
        assert isinstance(out["rows"], list)
    except rawquery.RawQueryError:
        pass
    assert _digest(path) == _FIXTURE["digest"]
    assert not list(path.parent.glob("*-journal")) and not list(path.parent.glob("*-wal"))


_FIXTURE: dict = {}
