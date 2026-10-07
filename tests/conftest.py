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
    seeded with two 'Turn 3' instances for run-1 (a turn and a straight segment defined). Used by anything that needs
    a genuinely separate read-only connection, e.g. the tool tests."""
    path = str(tmp_path / "evil.db")
    seed_conn = db.connect(path)
    db.apply_schema(seed_conn)
    seed_conn.executemany(
        """INSERT INTO track_segments (segment_id, ordinal, kind, name, aliases, length_m,
                                      gate_lat1, gate_lon1, gate_lat2, gate_lon2)
           VALUES (?, ?, ?, ?, ?, ?, 42.0, -76.0, 42.001, -76.0)""",
        [(1, 0, "turn", "Turn 3", "3", 100.0), (2, 1, "straight", "Straight 6-7", "", 600.0)],
    )
    seed_conn.executemany(
        """INSERT INTO turns
               (run_id, turn_def_id, start_seq, end_seq, start_ts, end_ts, entry_speed, exit_speed,
                duration_s, distance_m, energy_wh, efficiency_mi_per_kwh, avg_speed)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ("run-1", 1, 10, 14, 10.0, 12.0, 8.0, 6.0, 2.0, 14.0, 0.5, 17.4, 7.0),
            ("run-1", 1, 40, 44, 40.0, 42.0, 9.0, 9.5, 2.0, 18.5, 0.4, 28.7, 9.25),
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
