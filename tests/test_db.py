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
        "nas_index",
        "start_finish_line",
        "laps_open_state",
        "laps",
        "straights",
    }
    assert expected.issubset(tables)


def test_apply_schema_is_idempotent(conn):
    # re-applying must not raise, since deployment may re-run migrations
    from evil import db

    db.apply_schema(conn)
    db.apply_schema(conn)
