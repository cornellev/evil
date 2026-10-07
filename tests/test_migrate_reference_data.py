from evil import catalog, db, track
from evil.scripts.migrate_reference_data import migrate


def test_a_source_with_segments_is_copied_as_is(tmp_path):
    old = tmp_path / "old.db"
    c = db.connect(str(old))
    db.apply_schema(c)
    track.seed_track(c, track.load_track_file())
    c.close()
    new = tmp_path / "parsed" / "uc26" / "evil_uc26.db"

    assert migrate(str(old), str(new)) == {"track_segments": 17, "start_finish_line": 1}
    assert migrate(str(old), str(new)) == {"track_segments": 0, "start_finish_line": 0}      # idempotent

    n = db.connect(str(new))
    assert [s.name for s in track.load_segments(n)][:2] == ["Turn 1-2", "Turn 3"]
    assert n.execute("SELECT radius_m FROM start_finish_line").fetchone()[0] == 25


def test_a_circle_era_database_gets_the_packaged_track_and_keeps_its_start_finish(tmp_path):
    import sqlite3
    old = tmp_path / "evil.db"
    c = sqlite3.connect(old)
    c.executescript("""
        CREATE TABLE track_geometry (turn_def_id INTEGER PRIMARY KEY, turn_name TEXT, center_lat REAL, center_lon REAL, radius_m REAL);
        INSERT INTO track_geometry VALUES (1, 'Turn 1', 39.79, -86.23, 40);
        CREATE TABLE start_finish_line (line_id INTEGER PRIMARY KEY, center_lat REAL, center_lon REAL, radius_m REAL);
        INSERT INTO start_finish_line VALUES (1, 39.7986, -86.2388, 20);
    """)
    c.commit()
    c.close()
    new = tmp_path / "new.db"

    out = migrate(str(old), str(new))

    assert out["track_segments"] == 17 and "packaged" in out["track"] and out["start_finish_line"] == 1
    n = db.connect(str(new))
    assert n.execute("SELECT radius_m FROM start_finish_line").fetchone()[0] == 20       # kept the team's own circle


def test_a_source_without_any_reference_tables_still_creates_the_destination(tmp_path):
    import sqlite3
    old = tmp_path / "empty.db"
    sqlite3.connect(old).close()
    out = migrate(str(old), str(tmp_path / "new.db"))
    assert out["track_segments"] == 17 and out["start_finish_line"] == 0      # the packaged track brings its own circle


def test_ensure_parsed_db_creates_a_readable_empty_database(tmp_path):
    root = catalog.DataRoot(tmp_path / "data")
    path = catalog.ensure_parsed_db(root)
    assert path == root.path / "parsed" / "uc26" / "evil_uc26.db" and path.exists()
    ro = db.connect_readonly(str(path))
    assert ro.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 0
