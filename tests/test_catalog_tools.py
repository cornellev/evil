import asyncio
import io

import pytest

from evil import catalog
from evil.mcp_server import create_server
from evil.tools import catalog_tools
from evil.tools.registry import build_registry


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


def _rec(cat, root, name, data, start, end, run_id=None, source="telemetry", **fields):
    staging = catalog.Staging(root, 10**9)
    staging.add(name, io.BytesIO(data))
    rid = catalog.commit(cat, root, staging, catalog.UploadFields(source=source, **fields)).recording_id
    with cat:
        cat.execute("UPDATE recordings SET recorded_start = ?, recorded_end = ?, run_id = ? WHERE recording_id = ?",
                    (start, end, run_id, rid))
    return rid


@pytest.fixture
def populated(cat, root):
    run = _rec(cat, root, "run.csv", b"a", 1000.0, 1100.0, run_id="2026-04-09-aaaaaa", category="testing", car="uc26", label="run")
    video = _rec(cat, root, "front.mp4", b"video", 1050.0, 1090.0, source="autonomy", category="testing", car="uc26")
    later = _rec(cat, root, "later.csv", b"b", 5000.0, 5100.0, run_id="2026-04-10-bbbbbb", category="competition", car="uc26")
    return run, video, later


def test_find_nas_files_returns_footage_overlapping_the_run(cat, populated):
    run, video, later = populated

    files = catalog_tools.find_nas_files(cat, "2026-04-09-aaaaaa")

    assert {f["recording_id"] for f in files} == {run, video}
    assert {f["role"] for f in files} == {"csv", "video"}
    assert all(f["storage_backend"] == "nuc-local" and f["rel_path"] and f["size_bytes"] for f in files)


def test_find_nas_files_narrows_to_a_time_range(cat, populated):
    run, video, _ = populated
    # a turn at t=1010..1020: the video starts later (1050), so only the run's own file covers it
    assert {f["recording_id"] for f in catalog_tools.find_nas_files(cat, "2026-04-09-aaaaaa", 1010.0, 1020.0)} == {run}
    assert {f["recording_id"] for f in catalog_tools.find_nas_files(cat, "2026-04-09-aaaaaa", 1060.0, 1070.0)} == {run, video}


def test_find_nas_files_for_an_unknown_run_is_empty(cat, populated):
    assert catalog_tools.find_nas_files(cat, "nope") == []
    assert catalog_tools.find_nas_files(None, "x") == []


def test_list_recordings_gives_enough_detail_to_tell_candidates_apart(cat, populated):
    rows = catalog_tools.list_recordings(cat)
    assert len(rows) == 3
    assert rows[0]["category"] == "competition" and rows[0]["run_id"] == "2026-04-10-bbbbbb"   # newest first
    assert {"recorded_start", "car", "container", "parse_status", "label", "location"} <= set(rows[0])
    assert [r["category"] for r in catalog_tools.list_recordings(cat, category="testing")] == ["testing", "testing"]
    assert len(catalog_tools.list_recordings(cat, since=4000.0)) == 1
    assert catalog_tools.list_recordings(None) == []


def test_describe_recording(cat, root, populated):
    run, _, _ = populated
    with cat:
        cat.execute("INSERT INTO recording_streams VALUES (?, '/spi_data', 'ros_topic', 'std_msgs/msg/String', 10, 1000.0, 1100.0)", (run,))
        cat.execute("INSERT INTO named_locations (name, center_lat, center_lon, radius_m) VALUES ('B-lot', 1, 1, 10)")
        cat.execute("UPDATE recordings SET location_id = 1 WHERE recording_id = ?", (run,))

    d = catalog_tools.describe_recording(cat, run)

    assert d["location"] == "B-lot" and d["streams"][0]["msg_count"] == 10 and d["files"][0]["role"] == "csv"
    assert "metadata_json" not in d
    assert "not found" in catalog_tools.describe_recording(cat, "zzz")["error"]
    assert "no recording catalog" in catalog_tools.describe_recording(None, "x")["error"]


def test_registry_exposes_catalog_tools_only_when_given_a_catalog(cat, populated, readonly_conn):
    without = {s["function"]["name"] for s in build_registry(readonly_conn).schemas()}
    with_cat = build_registry(readonly_conn, cat)
    assert "find_nas_files" not in without and "list_recordings" not in without
    assert {"find_nas_files", "list_recordings", "describe_recording"} <= {s["function"]["name"] for s in with_cat.schemas()}
    assert len(with_cat.invoke("list_recordings", {"category": "competition"})) == 1


def test_mcp_server_serves_catalog_tools_from_a_readonly_catalog(root, cat, populated, evil_db_path):
    run, _, _ = populated
    cat.commit()
    server = create_server(db_path=evil_db_path, catalog_path=str(root.catalog_db))

    listed = asyncio.run(server.call_tool("list_recordings", {"category": "testing"})).structured_content["result"]
    found = asyncio.run(server.call_tool("find_nas_files", {"run_id": "2026-04-09-aaaaaa"})).structured_content["result"]
    described = asyncio.run(server.call_tool("describe_recording", {"recording_id": run}))

    assert run in {r["recording_id"] for r in listed} and len(listed) == 2
    assert {f["original_name"] for f in found} == {"run.csv", "front.mp4"}
    assert run in described.content[0].text


def test_mcp_catalog_tools_answer_empty_before_any_upload(tmp_path, evil_db_path):
    server = create_server(db_path=evil_db_path, catalog_path=str(tmp_path / "no_catalog.db"))

    listed = asyncio.run(server.call_tool("list_recordings", {}))
    nas = asyncio.run(server.call_tool("find_nas_files", {"run_id": "r"}))

    assert not listed.is_error and listed.structured_content["result"] == []
    assert not nas.is_error and nas.structured_content["result"] == []
    assert not (tmp_path / "no_catalog.db").exists()      # the MCP server never creates the catalog
