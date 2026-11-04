def test_schema_creates_expected_tables(conn):
    tables = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    expected = {
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
    }
    assert expected.issubset(tables)


def test_nas_index_is_gone_and_raw_tables_carry_run_id(conn):
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "nas_index" not in tables
    for table in ("joulemeter", "steering", "rpm_front", "rpm_back", "gps", "motor", "local_planner"):
        columns = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        assert "run_id" in columns, table
    snapshot_cols = {r["name"] for r in conn.execute("PRAGMA table_info(main_snapshot)")}
    assert {"device_seq", "device_global_ts_us", "publish_ns", "filtered_speed", "gps_id", "motor_id"} <= snapshot_cols


def test_a_database_from_before_gate_segments_is_upgraded_without_losing_raw_data(tmp_path):
    """An older database (circle geometry, no duration/distance/energy columns, user_version 0) has its derived
    tables and the old geometry dropped and recreated; the raw snapshots are untouched."""
    import sqlite3

    from evil import db
    from evil.ingest import ingest_sample
    from evil.models import GpsReading, RawSample

    path = str(tmp_path / "old.db")
    old = sqlite3.connect(path)
    old.executescript("""
        CREATE TABLE track_geometry (turn_def_id INTEGER PRIMARY KEY, turn_name TEXT, center_lat REAL, center_lon REAL, radius_m REAL);
        CREATE TABLE turns (turn_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, turn_def_id INTEGER REFERENCES track_geometry(turn_def_id),
            start_seq INTEGER, end_seq INTEGER, start_ts REAL, end_ts REAL, entry_speed REAL, exit_speed REAL);
        CREATE TABLE turns_open_state (run_id TEXT, turn_def_id INTEGER, start_seq INTEGER, start_ts REAL);
        INSERT INTO track_geometry VALUES (1, 'Turn 3', 42.0, -76.0, 50);
        INSERT INTO turns (run_id, turn_def_id, start_seq, end_seq, start_ts, end_ts) VALUES ('r', 1, 1, 2, 0, 1);
    """)
    old.commit()
    old.close()

    # raw tables written by an older schema version are compatible; add one snapshot after the upgrade
    conn = db.connect(path)
    db.apply_schema(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
    names = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "track_geometry" not in names and "turns_open_state" not in names and "track_segments" in names
    assert conn.execute("SELECT COUNT(*) FROM turns").fetchone()[0] == 0          # derived data is rebuilt, not migrated
    assert "efficiency_mi_per_kwh" in {r["name"] for r in conn.execute("PRAGMA table_info(turns)")}
    ingest_sample(conn, RawSample("r", 1.0, gps=GpsReading(1.0, 42.0, -76.0)))
    db.apply_schema(conn)                                                          # re-applying keeps everything
    assert conn.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 1
    conn.close()


def test_apply_schema_is_idempotent(conn):
    # re-applying must not raise, since deployment may re-run migrations
    from evil import db

    db.apply_schema(conn)
    db.apply_schema(conn)


def test_a_version_2_database_gains_the_v2_columns_and_keeps_its_derived_data(tmp_path):
    """The NUC's database is at user_version 2: upgrading must ADD motor.duty_cycle and
    main_snapshot.errcount in place and must NOT drop turns/laps (only pre-2 databases are rebuilt)."""

    from evil import db
    from evil.ingest import ingest_sample
    from evil.models import MotorReading, RawSample

    path = str(tmp_path / "v2.db")
    conn = db.connect(path)
    db.apply_schema(conn)
    conn.execute("ALTER TABLE motor DROP COLUMN duty_cycle")
    conn.execute("ALTER TABLE main_snapshot DROP COLUMN errcount")
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("INSERT INTO turns (run_id, turn_def_id, start_seq, end_seq, start_ts, end_ts) VALUES ('r', 1, 1, 2, 0, 1)")
    conn.execute("PRAGMA user_version = 2")
    conn.commit()
    conn.close()

    conn = db.connect(path)
    db.apply_schema(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
    assert conn.execute("SELECT COUNT(*) FROM turns").fetchone()[0] == 1
    assert "duty_cycle" in {r["name"] for r in conn.execute("PRAGMA table_info(motor)")}
    ingest_sample(conn, RawSample("r", 1.0, motor=MotorReading(1.0, 0.0, None, 5, 0.3), errcount=7))
    assert conn.execute("SELECT errcount FROM main_snapshot").fetchone()[0] == 7
    assert conn.execute("SELECT duty_cycle FROM motor").fetchone()[0] == 0.3
    conn.close()
