import json

from evil import db, track
from evil.scripts.seed_reference_data import main


def test_cli_loads_the_packaged_track(tmp_path, capsys):
    db_path = str(tmp_path / "evil.db")

    exit_code = main(["--db", db_path, "track"])

    assert exit_code == 0
    assert "segments" in capsys.readouterr().out
    conn = db.connect_readonly(db_path)
    try:
        segs = track.load_segments(conn)
        sf = conn.execute("SELECT * FROM start_finish_line").fetchone()
    finally:
        conn.close()
    assert len(segs) == 17
    assert sum(s.kind == "turn" for s in segs) == 13 and sum(s.kind == "straight" for s in segs) == 4
    assert [s.ordinal for s in segs] == list(range(17))
    assert sf["radius_m"] == 25


def test_packaged_track_is_consistent_with_itself():
    t = track.load_track_file()
    assert abs(sum(s["length_m"] for s in t["segments"]) - t["lap_length_m"]) <= 2        # tiles the whole lap
    names = [s["name"] for s in t["segments"]]
    assert len(set(names)) == len(names)
    assert {a for s in t["segments"] if s["kind"] == "turn" for a in s["aliases"]} == {str(i) for i in range(1, 15)}
    merged = next(s for s in t["segments"] if s["name"] == "Turn 1-2")
    assert merged["aliases"] == ["1", "2"]                       # official T1 and T2: one segment on the outer route
    assert [s["name"] for s in t["segments"] if s["kind"] == "straight"] == [
        "Straight 6-7", "Straight 10-11", "Straight 11-12", "Straight 14-1"]
    for s in t["segments"]:                                      # every gate is a real line, well wider than the road
        la1, lo1, la2, lo2 = s["entry_gate"]
        from evil.classifiers.metrics import tangent_distance_m
        assert 70 <= tangent_distance_m(la1, lo1, la2, lo2) <= 90


def test_seeding_twice_is_a_no_op_and_a_different_track_needs_replace(tmp_path):
    db_path = str(tmp_path / "evil.db")
    assert main(["--db", db_path, "track"]) == 0
    assert main(["--db", db_path, "track"]) == 0                  # same definition: nothing changes
    other = tmp_path / "other.json"
    t = track.load_track_file()
    t["segments"] = t["segments"][:4]
    other.write_text(json.dumps(t))
    import pytest
    with pytest.raises(ValueError, match="replace"):
        main(["--db", db_path, "track", str(other)])
    assert main(["--db", db_path, "track", str(other), "--replace"]) == 0
    conn = db.connect_readonly(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM track_segments").fetchone()[0] == 4
    finally:
        conn.close()


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
