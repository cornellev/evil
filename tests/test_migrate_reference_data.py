from evil import catalog, db
from evil.scripts.migrate_reference_data import migrate


def test_copies_reference_rows_once(tmp_path):
    old = tmp_path / "evil.db"
    c = db.connect(str(old))
    db.apply_schema(c)
    c.execute("INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('Turn 3', 39.79, -86.23, 40)")
    c.execute("INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (39.795, -86.234, 15)")
    c.commit()
    c.close()
    new = tmp_path / "parsed" / "uc26" / "evil_uc26.db"

    assert migrate(str(old), str(new)) == {"track_geometry": 1, "start_finish_line": 1}
    assert migrate(str(old), str(new)) == {"track_geometry": 0, "start_finish_line": 0}   # idempotent

    n = db.connect(str(new))
    assert n.execute("SELECT turn_name FROM track_geometry").fetchone()[0] == "Turn 3"
    assert n.execute("SELECT radius_m FROM start_finish_line").fetchone()[0] == 15


def test_a_source_without_the_tables_still_creates_the_destination(tmp_path):
    import sqlite3
    old = tmp_path / "empty.db"
    sqlite3.connect(old).close()
    assert migrate(str(old), str(tmp_path / "new.db")) == {"track_geometry": 0, "start_finish_line": 0}


def test_ensure_parsed_db_creates_a_readable_empty_database(tmp_path):
    root = catalog.DataRoot(tmp_path / "data")
    path = catalog.ensure_parsed_db(root)
    assert path == root.path / "parsed" / "uc26" / "evil_uc26.db" and path.exists()
    ro = db.connect_readonly(str(path))
    assert ro.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 0
