from evil import db
from evil.scripts.seed_reference_data import main


def test_cli_adds_a_turn(tmp_path, capsys):
    db_path = str(tmp_path / "evil.db")

    exit_code = main(["--db", db_path, "turn", "Turn 1", "42.0", "-76.0", "30"])

    assert exit_code == 0
    assert "id" in capsys.readouterr().out

    conn = db.connect_readonly(db_path)
    try:
        row = conn.execute("SELECT * FROM track_geometry WHERE turn_name = 'Turn 1'").fetchone()
    finally:
        conn.close()
    assert row["center_lat"] == 42.0
    assert row["center_lon"] == -76.0
    assert row["radius_m"] == 30.0


def test_cli_adds_a_start_finish_line(tmp_path, capsys):
    db_path = str(tmp_path / "evil.db")

    exit_code = main(["--db", db_path, "start-finish", "42.0", "-76.0", "30"])

    assert exit_code == 0
    conn = db.connect_readonly(db_path)
    try:
        row = conn.execute("SELECT * FROM start_finish_line").fetchone()
    finally:
        conn.close()
    assert row["center_lat"] == 42.0


def test_cli_can_add_multiple_turns(tmp_path):
    db_path = str(tmp_path / "evil.db")

    main(["--db", db_path, "turn", "Turn 1", "42.0", "-76.0", "30"])
    main(["--db", db_path, "turn", "Turn 2", "42.1", "-76.1", "25"])

    conn = db.connect_readonly(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM track_geometry").fetchone()["n"]
    finally:
        conn.close()
    assert count == 2
