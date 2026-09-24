import pytest

from evil import db


@pytest.fixture
def conn():
    connection = db.connect(":memory:")
    db.apply_schema(connection)
    yield connection
    connection.close()


@pytest.fixture
def evil_db_path(tmp_path) -> str:
    """A real, file-backed EVIL database (":memory:" databases are private
    per-connection, so connect_readonly() can't see one), schema applied and
    seeded with two 'Turn 3' instances for run-1. Used by anything that needs
    a genuinely separate read-only connection, e.g. the tool tests."""
    path = str(tmp_path / "evil.db")
    seed_conn = db.connect(path)
    db.apply_schema(seed_conn)
    seed_conn.execute(
        """INSERT INTO track_geometry (turn_def_id, turn_name, center_lat, center_lon, radius_m)
           VALUES (1, 'Turn 3', 42.0, -76.0, 50)"""
    )
    seed_conn.executemany(
        """INSERT INTO turns
               (run_id, turn_def_id, start_seq, end_seq, start_ts, end_ts, entry_speed, exit_speed)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("run-1", 1, 10, 14, 10.0, 12.0, 8.0, 6.0),
            ("run-1", 1, 40, 44, 40.0, 42.0, 9.0, 9.5),
        ],
    )
    seed_conn.commit()
    seed_conn.close()
    return path


@pytest.fixture
def readonly_conn(evil_db_path):
    connection = db.connect_readonly(evil_db_path)
    yield connection
    connection.close()
