"""In-process tests against MCPServer's own call_tool()/list_tools() API --
fast, no network needed. The real wire protocol (a client actually talking
HTTP to a running server) is exercised separately by
tests/e2e/smoke_mcp_server.sh, since that's the part these tests can't see:
call_tool() bypasses the transport layer entirely.
"""

from __future__ import annotations

import asyncio

import pytest
from mcp.server.mcpserver.exceptions import UnexpectedToolError

from evil.mcp_server import create_server


def test_registers_expected_tools_with_schemas(evil_db_path):
    server = create_server(db_path=evil_db_path)

    tools = asyncio.run(server.list_tools())

    names = {t.name for t in tools}
    assert names == {
        "get_turn",
        "compare_turn_instances",
        "compare_laps",
        "list_runs",
        "list_turns",
        "list_laps",
        "list_straights",
        "find_nas_files",
        "read_only_sql",
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
    """call_tool() bypasses the wire protocol's error translation, so the
    guard's ValueError surfaces as this wrapper exception here -- a real
    client over HTTP gets a proper MCP error result instead, see the e2e
    smoke script."""
    server = create_server(db_path=evil_db_path)

    with pytest.raises(UnexpectedToolError):
        asyncio.run(server.call_tool("read_only_sql", {"query": "DELETE FROM turns"}))


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

    mcp_server_module.connect_readonly = _tracking_connect_readonly
    try:
        server = create_server(db_path=evil_db_path)
        asyncio.run(server.call_tool("get_turn", {"run_id": "run-1", "turn_name": "Turn 3"}))
        asyncio.run(server.call_tool("get_turn", {"run_id": "run-1", "turn_name": "Turn 3"}))
    finally:
        mcp_server_module.connect_readonly = original

    assert len(opened_paths) == 2
