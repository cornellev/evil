"""In-process tests against MCPServer's own call_tool()/list_tools() API --
fast, no network needed. The real wire protocol (a client actually talking
HTTP to a running server) is exercised separately by
tests/e2e/smoke_mcp_server.sh, since that's the part these tests can't see:
call_tool() bypasses the transport layer entirely.
"""

from __future__ import annotations

import asyncio

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from evil.mcp_server import create_server


def test_registers_expected_tools_with_schemas(evil_db_path):
    server = create_server(db_path=evil_db_path)

    tools = asyncio.run(server.list_tools())

    names = {t.name for t in tools}
    assert names == {
        "get_turn",
        "get_straight",
        "list_track_segments",
        "compare_turn_instances",
        "compare_laps",
        "list_runs",
        "list_turns",
        "list_laps",
        "list_straights",
        "find_nas_files",
        "list_recordings",
        "describe_recording",
        "read_only_sql",
        "get_live_status",
        "get_live_window",
    }

    get_turn_tool = next(t for t in tools if t.name == "get_turn")
    assert set(get_turn_tool.input_schema["required"]) == {"run_id", "turn_name"}


def test_get_turn_tool_returns_seeded_data(evil_db_path):
    server = create_server(db_path=evil_db_path)

    result = asyncio.run(server.call_tool("get_turn", {"run_id": "run-1", "turn_name": "Turn 3"}))

    payload = result.content[0].text.lower()
    assert '"found": true' in payload
    assert "40.0" in result.content[0].text  # most recent occurrence's start_ts


def test_read_only_sql_tool_still_enforces_select_only_guard(evil_db_path):
    """The guard's refusal comes back as a tool error with its reason (a real client over HTTP gets it as an MCP
    error result, see the e2e smoke script)."""
    server = create_server(db_path=evil_db_path)

    with pytest.raises(ToolError, match="only allows SELECT"):
        asyncio.run(server.call_tool("read_only_sql", {"query": "DELETE FROM turns"}))


def test_read_only_sql_shows_the_schema_and_says_why_a_query_failed(evil_db_path):
    # seen 11-18: the model guessed `telemetry` and `main_snapshot.speed`, got a masked error, and gave up
    server = create_server(db_path=evil_db_path)
    tool = next(t for t in asyncio.run(server.list_tools()) if t.name == "read_only_sql")
    assert "main_snapshot(seq, run_id, global_ts" in tool.description and "filtered_speed" in tool.description
    assert "motor(id, run_id, ts, device_ts_us, rpm, throttle, duty_cycle)" in tool.description
    assert "classifier_cursor" not in tool.description                          # bookkeeping left out
    with pytest.raises(ToolError, match=r"(?s)no such column: speed.*main_snapshot\(seq"):
        asyncio.run(server.call_tool("read_only_sql", {"query": "SELECT MAX(speed) FROM main_snapshot"}))
    with pytest.raises(ToolError, match="no such table: telemetry"):
        asyncio.run(server.call_tool("read_only_sql", {"query": "SELECT 1 FROM telemetry"}))


def test_samples_answers_when_and_where_in_one_query(evil_db_path):
    # user 11-18: "it should be able to oneshot the query"; seen: "what segment of the lap" ran out of steps
    import json
    import sqlite3

    conn = sqlite3.connect(evil_db_path)
    conn.execute("INSERT INTO laps (run_id, lap_number, start_seq, end_seq, start_ts, end_ts, turn_count) "
                 "VALUES ('run-1', 1, 10, 50, 1775913490.0, 1775913530.0, 2)")
    conn.executemany("INSERT INTO motor (id, run_id, ts, throttle) VALUES (?, 'run-1', ?, ?)",
                     [(1, 1775913490.0, 0.5), (2, 1775913491.0, 0.0), (3, 1775913492.0, 0.4)])
    conn.executemany("INSERT INTO main_snapshot (seq, run_id, global_ts, filtered_speed, motor_id) "
                     "VALUES (?, 'run-1', ?, ?, ?)",
                     [(5, 1775913485.0, 3.0, None), (11, 1775913502.0, 13.85, 1), (20, 1775913503.0, 9.0, 2),
                      (41, 1775913504.0, 8.0, 3)])
    conn.commit()
    conn.close()
    server = create_server(db_path=evil_db_path)
    tool = next(t for t in asyncio.run(server.list_tools()) if t.name == "read_only_sql")
    assert tool.description.index("samples") < tool.description.index("main_snapshot(")    # the easy path first

    def sql(query):
        result = asyncio.run(server.call_tool("read_only_sql", {"query": query}))
        return json.loads(result.content[0].text) if not isinstance(result, list) else result

    top = sql("SELECT run_id, run_date, time, t, lap, segment, ROUND(speed_mph, 1) AS mph FROM samples "
              "ORDER BY speed DESC LIMIT 1")
    row = top[0] if isinstance(top, list) else top
    assert row["time"] == "2026-04-11 09:18:22" and row["run_date"] == "2026-04-11" and row["t"] == 17.0
    assert row["lap"] == 1 and row["segment"] == "Turn 3" and row["mph"] == 31.0
    outside = sql("SELECT lap, segment FROM samples WHERE seq = 5")
    outside = outside[0] if isinstance(outside, list) else outside
    assert outside["lap"] is None and outside["segment"] is None                    # before the lap, no segment


def test_each_call_opens_its_own_connection_not_a_shared_one(evil_db_path):
    """Regression guard for the concurrency design: every tool call must
    open a fresh connection, never share one sqlite3.Connection object
    across calls (which isn't safe for concurrent use)."""
    import evil.mcp_server as mcp_server_module

    opened_paths = []
    original = mcp_server_module.connect_readonly

    def _tracking_connect_readonly(path):
        opened_paths.append(path)
        return original(path)

    server = create_server(db_path=evil_db_path)          # reads the schema once, for read_only_sql's description
    mcp_server_module.connect_readonly = _tracking_connect_readonly
    try:
        asyncio.run(server.call_tool("get_turn", {"run_id": "run-1", "turn_name": "Turn 3"}))
        asyncio.run(server.call_tool("get_turn", {"run_id": "run-1", "turn_name": "Turn 3"}))
    finally:
        mcp_server_module.connect_readonly = original

    assert len(opened_paths) == 2
