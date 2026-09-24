from evil import db
from evil.scripts.register_nas_file import main
from evil.tools.nas_index import find_nas_files


def test_cli_registers_a_file(tmp_path, capsys):
    db_path = str(tmp_path / "evil.db")

    exit_code = main(["run-1", "/nas/run1.bag", "rosbag", "0.0", "100.0", "--db", db_path])

    assert exit_code == 0
    assert "file_id" in capsys.readouterr().out

    conn = db.connect_readonly(db_path)
    try:
        files = find_nas_files(conn, "run-1")
    finally:
        conn.close()
    assert len(files) == 1
    assert files[0]["path"] == "/nas/run1.bag"
    assert files[0]["kind"] == "rosbag"
