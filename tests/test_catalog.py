import io
import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import pytest

from evil import catalog


@pytest.fixture
def root(tmp_path):
    r = catalog.DataRoot(tmp_path / "data")
    r.ensure()
    return r


@pytest.fixture
def conn(root):
    c = catalog.connect_catalog(root)
    yield c
    c.close()


def _store(conn, root, files, **fields):
    staging = catalog.Staging(root, 10**9)
    for name, data in files:
        staging.add(name, io.BytesIO(data))
    return catalog.commit(conn, root, staging, catalog.UploadFields(**fields))


def test_sanitize_relpath_blocks_traversal_and_keeps_folders():
    assert catalog.sanitize_relpath("bag_1/metadata.yaml") == "bag_1/metadata.yaml"
    assert catalog.sanitize_relpath("../../etc/passwd") == "etc/passwd"
    assert catalog.sanitize_relpath("/abs/path/x.db3") == "abs/path/x.db3"
    assert catalog.sanitize_relpath("a\\b\\c.csv") == "a/b/c.csv"
    assert catalog.sanitize_relpath("we ird;$name.csv") == "we ird__name.csv"
    assert ".." not in catalog.sanitize_relpath("a/../../b")
    with pytest.raises(catalog.CatalogError):
        catalog.sanitize_relpath("../..")


def test_commit_stores_raw_file_manifest_and_rows(conn, root):
    result = _store(conn, root, [("run.csv", b"a,b\n1,2\n")], car="uc26", category="testing", label="garage")

    assert not result.deduplicated
    rec = catalog.get_recording(conn, result.recording_id)
    assert rec["parse_status"] == "pending"
    assert rec["container"] == "csv"
    assert rec["category"] == "testing" and rec["car"] == "uc26" and rec["label"] == "garage"
    assert rec["total_bytes"] == 8
    (f,) = rec["files"]
    stored = root.raw / f["rel_path"]
    assert stored.read_bytes() == b"a,b\n1,2\n"
    manifest = json.loads((stored.parent / "manifest.json").read_text())
    assert manifest["recording_id"] == result.recording_id
    assert manifest["fields"]["label"] == "garage"
    assert "/uc26/" in stored.as_posix()
    assert list(root.incoming.iterdir()) == [root.spool]  # nothing left in staging


def test_identical_upload_is_deduplicated_even_if_renamed(conn, root):
    first = _store(conn, root, [("a.csv", b"same bytes")])
    second = _store(conn, root, [("renamed.csv", b"same bytes")])

    assert second.deduplicated and second.recording_id == first.recording_id
    assert conn.execute("SELECT COUNT(*) FROM recordings").fetchone()[0] == 1
    assert len(list(root.raw.rglob("same*"))) == 0  # names are sanitized originals, no second copy
    assert len([p for p in root.raw.rglob("*.csv")]) == 1
    assert list(root.incoming.iterdir()) == [root.spool]


def test_two_parts_with_the_same_name_do_not_overwrite_each_other(conn, root):
    result = _store(conn, root, [("metadata.yaml", b"one"), ("metadata.yaml", b"two")])
    rec = catalog.get_recording(conn, result.recording_id)
    contents = sorted((root.raw / f["rel_path"]).read_bytes() for f in rec["files"])
    assert contents == [b"one", b"two"]


def test_container_detection_is_structural(conn, root):
    good = _store(conn, root, [("bag.db3", b"SQLite format 3\x00" + b"x" * 100)])
    bad = _store(conn, root, [("broken.db3", b"not sqlite at all")])
    other = _store(conn, root, [("clip.mp4", b"\x00\x01")])
    kinds = {r: catalog.get_recording(conn, r)["container"] for r in (good.recording_id, bad.recording_id, other.recording_id)}
    assert kinds[good.recording_id] == "rosbag2-sqlite3"
    assert kinds[bad.recording_id] == "unknown"  # still stored and cataloged
    assert kinds[other.recording_id] == "unknown"
    assert catalog.get_recording(conn, bad.recording_id)["parse_status"] == "pending"


def test_folder_upload_keeps_relative_paths_and_roles(conn, root):
    result = _store(conn, root, [
        ("bag_20260409/bag_0.db3", b"SQLite format 3\x00 data"),
        ("bag_20260409/metadata.yaml", b"rosbag2_bagfile_information: {}"),
    ])
    rec = catalog.get_recording(conn, result.recording_id)
    assert {f["role"] for f in rec["files"]} == {"db3", "metadata"}
    assert all("/bag_20260409/" in f["rel_path"] for f in rec["files"])
    assert rec["container"] == "rosbag2-sqlite3"


def test_upload_over_the_cap_raises_and_discard_leaves_nothing(root):
    staging = catalog.Staging(root, 10)
    with pytest.raises(catalog.UploadTooLarge):
        staging.add("big.csv", io.BytesIO(b"x" * 11))
    staging.discard()
    assert list(root.incoming.iterdir()) == [root.spool]


def test_invalid_category_and_empty_upload_rejected(conn, root):
    with pytest.raises(catalog.CatalogError):
        catalog.fields_from_form(category="party")
    staging = catalog.Staging(root, 10**9)
    with pytest.raises(catalog.CatalogError):
        catalog.commit(conn, root, staging, catalog.UploadFields())
    assert list(root.incoming.iterdir()) == [root.spool]


def test_sweep_removes_only_stale_staging(root):
    stale = root.incoming / "old_upload"
    fresh = root.incoming / "new_upload"
    for d in (stale, fresh):
        d.mkdir()
        (d / "f.bin").write_bytes(b"x")
    old = time.time() - 48 * 3600
    for p in (stale, stale / "f.bin"):
        os.utime(p, (old, old))
    spooled = root.spool / "tmpold"
    spooled.write_bytes(b"x")
    os.utime(spooled, (old, old))

    removed = catalog.sweep_incoming(root)

    assert removed == 2
    assert not stale.exists() and fresh.exists() and root.spool.exists() and not spooled.exists()


def test_reconcile_adopts_orphan_folder_after_crash_before_insert(conn, root):
    result = _store(conn, root, [("run.csv", b"a,b\n1,2\n")], label="keep me")
    with conn:  # simulate a crash between rename and insert
        conn.execute("DELETE FROM recording_files")
        conn.execute("DELETE FROM recordings")

    adopted = catalog.reconcile(conn, root)

    assert adopted == [result.recording_id]
    rec = catalog.get_recording(conn, result.recording_id)
    assert rec["label"] == "keep me" and len(rec["files"]) == 1
    assert catalog.reconcile(conn, root) == []  # idempotent


def test_lost_catalog_is_rebuilt_from_manifests_with_human_fields(root):
    conn = catalog.connect_catalog(root)
    a = _store(conn, root, [("a.csv", b"aaa")], category="competition", event="IMS 2026", notes="n1", car="uc26")
    b = _store(conn, root, [("b.csv", b"bbb")], label="second")
    conn.close()
    root.catalog_db.unlink()
    for ext in ("-wal", "-shm"):
        (root.path / f"catalog.db{ext}").unlink(missing_ok=True)

    fresh = catalog.connect_catalog(root)
    adopted = catalog.reconcile(fresh, root)

    assert sorted(adopted) == sorted([a.recording_id, b.recording_id])
    ra = catalog.get_recording(fresh, a.recording_id)
    assert (ra["category"], ra["event"], ra["notes"], ra["car"]) == ("competition", "IMS 2026", "n1", "uc26")
    assert catalog.get_recording(fresh, b.recording_id)["label"] == "second"
    fresh.close()


def test_reconcile_skips_folder_whose_files_do_not_match_manifest(conn, root):
    result = _store(conn, root, [("run.csv", b"a,b\n1,2\n")])
    rec = catalog.get_recording(conn, result.recording_id)
    with conn:
        conn.execute("DELETE FROM recording_files")
        conn.execute("DELETE FROM recordings")
    (root.raw / rec["files"][0]["rel_path"]).write_bytes(b"truncated")

    assert catalog.reconcile(conn, root) == []


def test_update_recording_edits_fields_and_rewrites_manifest(conn, root):
    rid = _store(conn, root, [("run.csv", b"x")]).recording_id

    updated = catalog.update_recording(conn, root, rid, {"label": " Lap test ", "category": "testing", "ignored": "x"})

    assert updated["label"] == "Lap test" and updated["category"] == "testing"
    directory = catalog.raw_dir_of(conn, root, rid)
    manifest = json.loads((directory / "manifest.json").read_text())
    assert manifest["fields"]["label"] == "Lap test"
    assert manifest["files"][0]["rel_path"] == "run.csv"
    with pytest.raises(catalog.CatalogError):
        catalog.update_recording(conn, root, rid, {"category": "nope"})
    with pytest.raises(catalog.CatalogError):
        catalog.update_recording(conn, root, rid, {"ignored": "x"})
    with pytest.raises(KeyError):
        catalog.update_recording(conn, root, "missing", {"label": "x"})


def test_backup_is_once_per_day_and_prunes(conn, root):
    _store(conn, root, [("run.csv", b"x")])
    day = lambda d: datetime(2026, 10, d, 3, 0, tzinfo=timezone.utc)

    first = catalog.backup_catalog(conn, root, keep=3, today=day(1))
    again = catalog.backup_catalog(conn, root, keep=3, today=day(1))
    for d in (2, 3, 4, 5):
        catalog.backup_catalog(conn, root, keep=3, today=day(d))

    assert first is not None and again is None
    names = sorted(p.name for p in root.backup.glob("catalog-*.db"))
    assert names == ["catalog-20261003.db", "catalog-20261004.db", "catalog-20261005.db"]
    copy = sqlite3.connect(root.backup / names[-1])
    assert copy.execute("SELECT COUNT(*) FROM recordings").fetchone()[0] == 1
    copy.close()


def test_list_recordings_filters_and_orders_newest_first(conn, root):
    a = _store(conn, root, [("a.csv", b"a")], category="testing", car="uc26").recording_id
    b = _store(conn, root, [("b.csv", b"b")], category="competition", car="uc26").recording_id
    with conn:
        conn.execute("UPDATE recordings SET uploaded_at = 1000 WHERE recording_id = ?", (a,))
        conn.execute("UPDATE recordings SET uploaded_at = 2000 WHERE recording_id = ?", (b,))

    assert [r["recording_id"] for r in catalog.list_recordings(conn)] == [b, a]
    assert [r["recording_id"] for r in catalog.list_recordings(conn, category="testing")] == [a]
    assert [r["recording_id"] for r in catalog.list_recordings(conn, since=1500)] == [b]
    assert [r["recording_id"] for r in catalog.list_recordings(conn, until=1500)] == [a]
    assert catalog.list_recordings(conn, parse_status="parsed") == []
