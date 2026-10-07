import io
import json
import sqlite3
import time

import pytest

from evil import catalog, db, parser, scan
from telemetry_fixtures import NAN, payload, v2_payload, write_csv, write_db3

T0 = 1_775_000_000.0  # an April 2026 epoch


@pytest.fixture
def root(tmp_path):
    r = catalog.DataRoot(tmp_path / "data")
    r.ensure()
    return r


@pytest.fixture
def cat(root):
    c = catalog.connect_catalog(root)
    yield c
    c.close()


def _upload(cat, root, files, **fields):
    """Store files (name -> path or bytes) through the real commit path."""
    staging = catalog.Staging(root, 10**10)
    for name, data in files.items():
        stream = open(data, "rb") if not isinstance(data, bytes) else io.BytesIO(data)
        staging.add(name, stream)
        stream.close()
    return catalog.commit(cat, root, staging, catalog.UploadFields(**fields)).recording_id


def _bag(tmp_path, msgs, topic="/spi_data", name="bag.db3"):
    return write_db3(tmp_path / name, {topic: msgs})


def _seed_turn(root, car="uc26"):
    path = catalog.parsed_db_path(root, car)
    path.parent.mkdir(parents=True, exist_ok=True)
    c = db.connect(str(path))
    db.apply_schema(c)
    c.execute("INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) VALUES ('Turn 3', 42.0, -76.0, 50)")
    c.commit()
    c.close()
    return path


def _parsed(root, car="uc26"):
    c = db.connect(str(catalog.parsed_db_path(root, car)))
    return c


def _msgs(n, **kw):
    return [(T0 + i * 0.02, payload(seq=2 + 2 * i, **kw)) for i in range(n)]


# ---- happy path ----------------------------------------------------------

def test_parses_a_bag_into_every_group_table(tmp_path, root, cat):
    rid = _upload(cat, root, {"bag.db3": _bag(tmp_path, _msgs(10))}, category="testing")

    r = parser.parse_recording(root, rid)

    assert r.status == "parsed" and r.rows_ingested == 10 and r.rows_duplicate == 0 and r.rows_rejected == 0
    rec = catalog.get_recording(cat, rid)
    assert rec["parse_status"] == "parsed" and rec["schema_id"] == "evil.telemetry.v1"
    assert rec["matched_stream"] == "/spi_data" and rec["target_db"] == "evil_uc26.db"
    assert rec["rows_ingested"] == 10 and rec["run_id"] == r.run_id and rec["car"] == "uc26"
    assert r.run_id.startswith("2026-03-") or r.run_id.startswith("2026-04-")
    p = _parsed(root)
    assert p.execute("SELECT COUNT(*) FROM main_snapshot WHERE run_id = ?", (r.run_id,)).fetchone()[0] == 10
    for table, n in (("joulemeter", 10), ("steering", 10), ("rpm_back", 10), ("gps", 10), ("motor", 10), ("rpm_front", 0)):
        assert p.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = ?", (r.run_id,)).fetchone()[0] == n, table
    first = p.execute("SELECT * FROM main_snapshot ORDER BY seq LIMIT 1").fetchone()
    assert first["device_seq"] == 2 and first["global_ts"] == pytest.approx(T0)
    assert first["rpm_front_id"] is None and first["gps_id"] is not None and first["filtered_speed"] == 5.0
    g = p.execute("SELECT * FROM gps WHERE id = ?", (first["gps_id"],)).fetchone()
    assert g["device_ts_us"] == 1_000_400 + 2 * 1000 and g["lat"] == 42.0 and g["lon"] == -76.0
    p.close()
    assert rec["recorded_start"] == pytest.approx(T0)


def test_matching_is_by_content_not_topic_name(tmp_path, root, cat):
    for i, topic in enumerate(["/spi_data", "spi_data", "/some/other/name"]):
        rid = _upload(cat, root, {f"b{i}.db3": _bag(tmp_path, _msgs(3, speed=float(i)), topic=topic, name=f"b{i}.db3")})
        r = parser.parse_recording(root, rid)
        assert r.status == "parsed" and r.matched_stream == topic


def test_only_the_matching_stream_is_parsed(tmp_path, root, cat):
    path = write_db3(tmp_path / "multi.db3", {
        "/spi_data": _msgs(5),
        "/chatter": [(T0, {"hello": "world"})],
        "/camera": [(T0, b"\x00\x01")],
    }, types={"/camera": "sensor_msgs/msg/Image"})
    rid = _upload(cat, root, {"multi.db3": path})

    r = parser.parse_recording(root, rid)

    assert r.status == "parsed" and r.rows_ingested == 5 and r.matched_stream == "/spi_data"
    coverage = json.loads(catalog.get_recording(cat, rid)["schema_match_json"])
    assert coverage["/chatter"]["evil.telemetry.v1"] == 0.0 and coverage["/spi_data"]["evil.telemetry.v1"] == 1.0


def test_split_bag_files_are_read_in_order(tmp_path, root, cat):
    a = write_db3(tmp_path / "bag_2.db3", {"/spi_data": [(T0 + 1.0 + i * 0.02, payload(seq=100 + 2 * i)) for i in range(3)]})
    b = write_db3(tmp_path / "bag_10.db3", {"/spi_data": [(T0 + 2.0 + i * 0.02, payload(seq=200 + 2 * i)) for i in range(3)]})
    c = write_db3(tmp_path / "bag_0.db3", {"/spi_data": [(T0 + i * 0.02, payload(seq=2 + 2 * i)) for i in range(3)]})
    rid = _upload(cat, root, {"bag/bag_2.db3": a, "bag/bag_10.db3": b, "bag/bag_0.db3": c})

    r = parser.parse_recording(root, rid)

    assert r.status == "parsed" and r.rows_ingested == 9
    p = _parsed(root)
    seqs = [row[0] for row in p.execute("SELECT device_seq FROM main_snapshot ORDER BY seq")]
    assert seqs == [2, 4, 6, 100, 102, 104, 200, 202, 204]  # natural order: 0, 2, 10


# ---- repeats and rejects -------------------------------------------------

def test_consecutive_repeats_are_dropped_and_counted(tmp_path, root, cat):
    base = payload(seq=2)
    msgs = [(T0 + i * 0.02, {**base, "_t_publish_ns": 1000 + i}) for i in range(4)]       # 1 new + 3 repeats
    msgs += [(T0 + 0.1, payload(seq=4)), (T0 + 0.12, {**payload(seq=4), "_t_publish_ns": 5})]  # 1 new + 1 repeat
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)})

    r = parser.parse_recording(root, rid)

    assert (r.rows_ingested, r.rows_duplicate) == (2, 4)
    rec = catalog.get_recording(cat, rid)
    assert rec["rows_ingested"] == 2 and rec["rows_duplicate"] == 4


def test_a_non_consecutive_repeat_is_kept(tmp_path, root, cat):
    a, b = payload(seq=2), payload(seq=4)
    msgs = [(T0, a), (T0 + 0.02, b), (T0 + 0.04, {**a, "_t_publish_ns": 9})]
    r = parser.parse_recording(root, _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)}))
    assert (r.rows_ingested, r.rows_duplicate) == (3, 0)


def test_unchanged_sensor_values_with_a_new_seq_are_not_repeats(tmp_path, root, cat):
    msgs = [(T0 + i * 0.02, payload(seq=2 + 2 * i)) for i in range(5)]  # identical readings, new seq/ts each time
    r = parser.parse_recording(root, _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)}))
    assert (r.rows_ingested, r.rows_duplicate) == (5, 0)


def test_same_seq_but_different_body_is_kept_and_flagged(tmp_path, root, cat):
    msgs = [(T0, payload(seq=2)), (T0 + 0.02, payload(seq=2, speed=9.0))]
    r = parser.parse_recording(root, _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)}))
    assert (r.rows_ingested, r.rows_duplicate) == (2, 0)
    assert json.loads(catalog.get_recording(cat, r.run_id and _rid(cat, r.run_id))["parse_stats_json"])["seq_conflicts"] == 1


def _rid(cat, run_id):
    return cat.execute("SELECT recording_id FROM recordings WHERE run_id = ?", (run_id,)).fetchone()[0]


def test_bad_messages_inside_a_matched_stream_are_rejected_not_coerced(tmp_path, root, cat, monkeypatch):
    # Matching samples only the first/spread messages, so bad ones elsewhere are
    # rejected-and-counted, not a reason to refuse the whole recording.
    monkeypatch.setattr(parser, "SAMPLE_HEAD", 2)
    monkeypatch.setattr(parser, "SAMPLE_SPREAD", 0)
    bad_type = payload(seq=6)
    bad_type["gps"]["lat"] = "42.0"                       # string where a number is required
    extra = {**payload(seq=8), "errcount": 3}             # extra key (the later shape)
    msgs = [(T0, payload(seq=2)), (T0 + 0.02, payload(seq=4)), (T0 + 0.04, bad_type),
            (T0 + 0.06, extra), (T0 + 0.08, b"\x00\x01garbage"), (T0 + 0.1, payload(seq=12))]
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)})

    r = parser.parse_recording(root, rid)

    assert r.status == "parsed" and r.rows_ingested == 3 and r.rows_rejected == 3
    rec = catalog.get_recording(cat, rid)
    assert rec["rows_rejected"] == 3
    reasons = " | ".join(s["reason"] for s in json.loads(rec["reject_samples_json"]))
    assert "gps.lat" in reasons and "errcount" in reasons and "CDR" in reasons
    assert _parsed(root).execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 3


def test_matching_samples_head_plus_spread_and_misses_nothing_it_samples(tmp_path, root):
    msgs = _msgs(400)
    path = _bag(tmp_path, msgs, name="probe.db3")
    sampled_ts = {round(ts, 3) for ts, _ in parser._db3_sample(path, "/spi_data")}
    assert 90 <= len(sampled_ts) <= 100 and round(T0, 3) in sampled_ts          # head included
    assert max(sampled_ts) > T0 + 0.95 * (msgs[-1][0] - T0)                      # spread reaches near the end
    # a bad message at a SAMPLED position stops the match; at an unsampled one it does not
    sampled_idx = [i for i, (ts, _) in enumerate(msgs) if round(ts, 3) in sampled_ts]
    unsampled_idx = [i for i in range(len(msgs)) if i not in set(sampled_idx)]
    for idx, expect in ((sampled_idx[60], "skipped"), (unsampled_idx[50], "parsed")):
        broken = list(msgs)
        broken[idx] = (broken[idx][0], {"junk": 1})
        cat = catalog.connect_catalog(root)
        rid = _upload(cat, root, {f"b{idx}.db3": _bag(tmp_path, broken, name=f"b{idx}.db3")})
        assert parser.parse_recording(root, rid).status == expect
        cat.close()


# ---- no match / unreadable -----------------------------------------------

def test_a_bag_with_no_matching_stream_is_skipped_not_failed(tmp_path, root, cat):
    path = write_db3(tmp_path / "other.db3", {"/camera": [(T0, b"\x00\x01")], "/chatter": [(T0, {"hi": 1})]},
                     types={"/camera": "sensor_msgs/msg/Image"})
    rid = _upload(cat, root, {"other.db3": path})

    r = parser.parse_recording(root, rid)

    rec = catalog.get_recording(cat, rid)
    assert r.status == "skipped" and rec["parse_status"] == "skipped" and rec["run_id"] is None
    assert "no stream matches" in rec["parse_error"] and rec["schema_id"] is None
    assert not catalog.parsed_db_path(root, "uc26").exists()


def test_the_later_v2_shape_is_stored_but_skipped(tmp_path, root, cat):
    msgs = [(T0 + i * 0.02, v2_payload(seq=2 + 2 * i)) for i in range(5)]
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)})
    assert parser.parse_recording(root, rid).status == "skipped"
    assert catalog.get_recording(cat, rid)["files"]  # still stored


def test_a_half_conforming_stream_does_not_match(tmp_path, root, cat):
    msgs = [(T0 + i * 0.02, payload(seq=2 + 2 * i) if i % 2 else {"junk": i}) for i in range(10)]
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)})
    assert parser.parse_recording(root, rid).status == "skipped"


def test_malformed_sqlite_is_skipped_with_a_reason(tmp_path, root, cat):
    rid = _upload(cat, root, {"broken.db3": b"SQLite format 3\x00" + b"\x00" * 200})
    assert catalog.get_recording(cat, rid)["container"] == "rosbag2-sqlite3"

    r = parser.parse_recording(root, rid)

    rec = catalog.get_recording(cat, rid)
    assert r.status == "skipped" and rec["parse_status"] == "skipped"
    assert rec["parse_error"].startswith("container unreadable")


def test_a_db3_that_is_not_sqlite_at_all_is_skipped(tmp_path, root, cat):
    rid = _upload(cat, root, {"junk.db3": b"not a database"})
    assert parser.parse_recording(root, rid).status == "skipped"
    assert "unreadable" in catalog.get_recording(cat, rid)["parse_error"]


def test_unknown_containers_are_skipped(root, cat):
    rid = _upload(cat, root, {"clip.mp4": b"\x00\x00\x00\x18ftypmp42"})
    r = parser.parse_recording(root, rid)
    assert r.status == "skipped" and "no reader" in r.message


# ---- csv -----------------------------------------------------------------

def test_csv_with_the_v1_header_parses_including_nan(tmp_path, root, cat):
    items = [(T0 + i * 0.02, payload(seq=2 + 2 * i, lat=None if i == 1 else 42.0)) for i in range(4)]
    rid = _upload(cat, root, {"0409.csv": write_csv(tmp_path / "x.csv", items)})
    assert catalog.get_recording(cat, rid)["container"] == "csv"

    r = parser.parse_recording(root, rid)

    assert r.status == "parsed" and r.rows_ingested == 4 and r.rows_rejected == 0
    p = _parsed(root)
    assert p.execute("SELECT COUNT(*) FROM main_snapshot WHERE gps_id IS NULL").fetchone()[0] == 1
    assert p.execute("SELECT global_ts FROM main_snapshot ORDER BY seq").fetchone()[0] == pytest.approx(T0)


def test_csv_with_a_different_header_is_skipped(tmp_path, root, cat):
    items = [(T0 + i * 0.02, v2_payload(seq=2 + 2 * i)) for i in range(3)]
    rid = _upload(cat, root, {"0411.csv": write_csv(tmp_path / "x.csv", items)})
    r = parser.parse_recording(root, rid)
    assert r.status == "skipped"
    cov = json.loads(catalog.get_recording(cat, rid)["schema_match_json"])
    assert "errcount" in json.dumps(cov)


def test_loose_dashboard_csv_is_skipped(root, cat):
    rid = _upload(cat, root, {"dash.csv": b"global_ts,gps.lat,gps.long,speed\n0.0,42.0,-76.0,5.0\n"})
    assert parser.parse_recording(root, rid).status == "skipped"


# ---- idempotence, ids, classifiers, locations ----------------------------

def test_reparse_is_idempotent_and_keeps_the_run_id(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, _msgs(8))})
    first = parser.parse_recording(root, rid)
    second = parser.parse_recording(root, rid)

    assert second.run_id == first.run_id and second.rows_ingested == 8
    p = _parsed(root)
    assert p.execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 8
    for table in ("joulemeter", "steering", "rpm_back", "gps", "motor"):
        assert p.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 8, table


def test_run_ids_are_unique_per_recording(tmp_path, root, cat):
    ids = set()
    for i in range(3):
        rid = _upload(cat, root, {f"b{i}.db3": _bag(tmp_path, _msgs(2, speed=float(i)), name=f"b{i}.db3")})
        ids.add(parser.parse_recording(root, rid).run_id)
    assert len(ids) == 3


def test_classifiers_run_and_skip_ticks_without_a_gps_fix(tmp_path, root, cat):
    _seed_turn(root)
    # inside the turn circle, then no-fix ticks, then outside: the no-fix ticks must not end the turn
    msgs = []
    for i, lat in enumerate([42.0, 42.0, None, None, 42.0, 42.01, 42.01]):
        msgs.append((T0 + i, payload(seq=2 + 2 * i, lat=lat)))
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, msgs)})

    r = parser.parse_recording(root, rid)

    p = _parsed(root)
    turns = p.execute("SELECT start_ts, end_ts FROM turns WHERE run_id = ?", (r.run_id,)).fetchall()
    assert len(turns) == 1
    assert turns[0]["start_ts"] == pytest.approx(T0) and turns[0]["end_ts"] == pytest.approx(T0 + 5)


def test_location_label_and_gps_box_come_from_the_parsed_gps(tmp_path, root, cat):
    with cat:
        cat.execute("INSERT INTO named_locations (name, center_lat, center_lon, radius_m) VALUES ('Far away', 10.0, 10.0, 100)")
        cat.execute("INSERT INTO named_locations (name, center_lat, center_lon, radius_m) VALUES ('B-lot', 42.0, -76.0, 500)")
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, _msgs(5))})

    parser.parse_recording(root, rid)

    rec = catalog.get_recording(cat, rid)
    assert (rec["gps_min_lat"], rec["gps_max_lat"]) == (42.0, 42.0)
    assert rec["location_method"] == "gps-match"
    assert cat.execute("SELECT name FROM named_locations WHERE location_id = ?", (rec["location_id"],)).fetchone()[0] == "B-lot"


def test_a_manual_location_is_not_overwritten_by_a_reparse(tmp_path, root, cat):
    with cat:
        cat.execute("INSERT INTO named_locations (name, center_lat, center_lon, radius_m) VALUES ('B-lot', 42.0, -76.0, 500)")
        cat.execute("INSERT INTO named_locations (name, center_lat, center_lon, radius_m) VALUES ('Manual', 1.0, 1.0, 5)")
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, _msgs(5))})
    with cat:
        cat.execute("UPDATE recordings SET location_id = 2, location_method = 'manual' WHERE recording_id = ?", (rid,))

    parser.parse_recording(root, rid)

    assert catalog.get_recording(cat, rid)["location_id"] == 2


def test_no_fix_means_no_gps_box(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, _msgs(5, lat=None))})
    parser.parse_recording(root, rid)
    rec = catalog.get_recording(cat, rid)
    assert rec["parse_status"] == "parsed" and rec["gps_min_lat"] is None and rec["location_id"] is None


def test_parse_failure_rolls_back_and_marks_failed(tmp_path, root, cat, monkeypatch):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, _msgs(20))})
    calls = {"n": 0}
    real = parser.insert_sample

    def boom(conn, sample):
        calls["n"] += 1
        if calls["n"] == 10:
            raise RuntimeError("disk exploded")
        return real(conn, sample)

    monkeypatch.setattr(parser, "insert_sample", boom)
    with pytest.raises(RuntimeError):
        parser.parse_recording(root, rid)

    rec = catalog.get_recording(cat, rid)
    assert rec["parse_status"] == "failed" and "disk exploded" in rec["parse_error"]
    assert _parsed(root).execute("SELECT COUNT(*) FROM main_snapshot").fetchone()[0] == 0  # nothing half-written

    monkeypatch.setattr(parser, "insert_sample", real)
    assert parser.parse_recording(root, rid).status == "parsed"   # a retry just works


# ---- scan ----------------------------------------------------------------

def test_scan_fills_streams_times_and_metadata(tmp_path, root, cat):
    path = write_db3(tmp_path / "bag.db3", {"/spi_data": _msgs(5), "/chatter": [(T0 + 10, {"a": 1})]})
    rid = _upload(cat, root, {"bag/bag.db3": path, "bag/metadata.yaml": b"rosbag2_bagfile_information:\n  version: 5\n"})

    out = scan.scan_recording(root, rid)

    rec = catalog.get_recording(cat, rid)
    assert out["streams"] == 2 and rec["time_source"] == "ros-record-time" and rec["time_trust"] == "ok"
    assert rec["recorded_start"] == pytest.approx(T0) and rec["recorded_end"] == pytest.approx(T0 + 10)
    streams = {r["stream_name"]: r for r in cat.execute("SELECT * FROM recording_streams WHERE recording_id = ?", (rid,))}
    assert streams["/spi_data"]["msg_count"] == 5 and streams["/spi_data"]["type_or_encoding"] == "std_msgs/msg/String"
    assert json.loads(rec["metadata_json"])["rosbag2_bagfile_information"]["version"] == 5
    assert rec["parse_status"] == "pending"  # a scan alone does not parse
    scan.scan_recording(root, rid)  # idempotent
    assert cat.execute("SELECT COUNT(*) FROM recording_streams WHERE recording_id = ?", (rid,)).fetchone()[0] == 2


def test_scan_flags_an_unsynced_clock_as_suspect(tmp_path, root, cat):
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, [(5.0 + i, payload(seq=2 + 2 * i)) for i in range(3)])})
    scan.scan_recording(root, rid)
    rec = catalog.get_recording(cat, rid)
    assert rec["time_trust"] == "suspect" and rec["recorded_start"] == 5.0


def test_scan_marks_a_malformed_bag_skipped_without_failing(root, cat):
    rid = _upload(cat, root, {"broken.db3": b"SQLite format 3\x00" + b"\x00" * 200})
    out = scan.scan_recording(root, rid)
    rec = catalog.get_recording(cat, rid)
    assert out["error"] and rec["parse_status"] == "skipped" and rec["parse_error"].startswith("container unreadable")


def test_scan_reads_a_csv_time_range(tmp_path, root, cat):
    items = [(T0 + i, payload(seq=2 + 2 * i)) for i in range(4)]
    rid = _upload(cat, root, {"a.csv": write_csv(tmp_path / "x.csv", items)})
    scan.scan_recording(root, rid)
    rec = catalog.get_recording(cat, rid)
    assert rec["time_source"] == "csv-timestamp" and rec["recorded_end"] == pytest.approx(T0 + 3)


def test_run_date_uses_the_recorded_date_in_new_york(tmp_path, root, cat):
    # 2026-04-10 02:00 UTC is still the evening of April 9 in New York
    t = 1_775_786_400.0
    rid = _upload(cat, root, {"b.db3": _bag(tmp_path, [(t + i * 0.02, payload(seq=2 + 2 * i)) for i in range(3)])})
    scan.scan_recording(root, rid)
    assert parser.parse_recording(root, rid).run_id.startswith("2026-04-09-")
