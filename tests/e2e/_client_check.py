"""Real MCP client hitting a real running server over HTTP. This is the part
tests/test_mcp_server.py's in-process call_tool() calls can't cover -- it
exercises the actual Streamable HTTP wire protocol.
"""

import asyncio
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

url = sys.argv[1]


async def main() -> int:
    """The list_runs check below reads structured_content, not
    content[0].text: the SDK gives one TextContent block PER LIST ITEM for a
    list[dict]-returning tool, not one JSON array, so content[0].text alone
    would silently only see the first run (and crash on zero runs).
    structured_content reliably holds the full list regardless of item
    count -- see MCPToolClient's fix in evil-ui/tern-llm for the
    client-side version of this same gotcha."""
    async with streamable_http_client(url) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            names = {t.name for t in tools.tools}
            expected = {
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
            if names != expected:
                print(f"FAIL: expected tools {expected}, got {names}")
                return 1

            result = await session.call_tool(
                "get_turn", {"run_id": "run-1", "turn_name": "Turn 3"}
            )
            text = result.content[0].text
            if result.is_error or '"found": true' not in text.lower():
                print(f"FAIL: get_turn returned unexpected result: {text}")
                return 1
            if "40.0" not in text:
                print(f"FAIL: get_turn did not return the most recent occurrence: {text}")
                return 1

            runs_result = await session.call_tool("list_runs", {})
            runs = (runs_result.structured_content or {}).get("result")
            if runs_result.is_error or runs != [
                {"run_id": "run-1", "sample_count": 2, "start_ts": 0.0, "end_ts": 42.0}
            ]:
                print(f"FAIL: list_runs did not report the seeded run correctly: {runs}")
                return 1

            turns_result = await session.call_tool("list_turns", {"run_id": "run-1"})
            turns_text = turns_result.content[0].text
            if turns_result.is_error or '"total": 2' not in turns_text:
                print(f"FAIL: list_turns did not report both seeded turns: {turns_text}")
                return 1

            laps_result = await session.call_tool("compare_laps", {"run_id": "run-1", "lap_a": 1, "lap_b": 2})
            laps_text = laps_result.content[0].text
            if laps_result.is_error or '"found": true' not in laps_text.lower():
                print(f"FAIL: compare_laps returned unexpected result: {laps_text}")
                return 1

            nas_result = await session.call_tool(
                "find_nas_files", {"run_id": "run-1", "start_ts": 5.0, "end_ts": 15.0}
            )
            nas_files = (nas_result.structured_content or {}).get("result")
            if nas_result.is_error or nas_files != [
                {"file_id": 1, "run_id": "run-1", "path": "/nas/run-1.bag", "kind": "rosbag", "start_ts": 0.0, "end_ts": 42.0}
            ]:
                print(f"FAIL: find_nas_files did not report the seeded recording: {nas_files}")
                return 1

            guarded = await session.call_tool("read_only_sql", {"query": "DELETE FROM turns"})
            if not guarded.is_error:
                print("FAIL: read_only_sql accepted a DELETE over the real wire protocol")
                return 1

            print(
                "OK: tool listing, get_turn, list_runs, list_turns, compare_laps, "
                "find_nas_files, and the read_only_sql guard all work over HTTP"
            )
            return 0


sys.exit(asyncio.run(main()))
