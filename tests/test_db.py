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
        "track_geometry",
        "turns",
        "turns_open_state",
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


def test_apply_schema_is_idempotent(conn):
    # re-applying must not raise, since deployment may re-run migrations
    from evil import db

    db.apply_schema(conn)
    db.apply_schema(conn)
