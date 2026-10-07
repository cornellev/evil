"""MCP server exposing EVIL's tools over Streamable HTTP. Runs co-located
with EVIL's database on the same device, reached by any client (tern-llm's
own agent, a CEV member's own Claude client, evil-ui) over Tailscale -- not
stdio, which would require the server to run on each person's own machine
next to the database file.

Concurrency: each tool call opens its own short-lived read-only connection
and runs the blocking sqlite call via a thread offload, so many concurrent
callers can't serialize behind one shared connection or block the server's
event loop. There is deliberately no single-in-flight lock here (unlike
tern-llm's /ask endpoint) -- that lock protects one scarce local LLM
inference slot, which doesn't exist on this side: external MCP clients run
their own model inference elsewhere, this server only ever does DB reads.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Callable

from mcp.server.mcpserver import MCPServer

from evil import catalog
from evil.db import connect_readonly
from evil.tools.compare_laps import compare_laps
from evil.tools.compare_turn_instances import compare_turn_instances
from evil.tools.get_turn import get_turn
from evil.tools.list_laps import list_laps
from evil.tools.list_runs import list_runs
from evil.tools.list_straights import list_straights
from evil.tools.list_turns import list_turns
from evil.tools.catalog_tools import describe_recording, find_nas_files, list_recordings
from evil.tools.read_only_sql import read_only_sql

DEFAULT_DB_PATH = os.getenv("EVIL_DB_PATH", "evil.db")
DEFAULT_HOST = os.getenv("EVIL_MCP_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.getenv("EVIL_MCP_PORT", "8765"))


def _run_readonly(path: str, fn: Callable[..., Any], **kwargs: Any) -> Any:
    """Every tool handler's body: open a fresh connection, run the pure
    function, close it -- factored out once a fourth/fifth/sixth tool
    would have repeated this open/try/finally boilerplate verbatim."""
    conn = connect_readonly(path)
    try:
        return fn(conn, **kwargs)
    finally:
        conn.close()


def _run_catalog(catalog_path: str, fn: Callable[..., Any], **kwargs: Any) -> Any:
    """Like _run_readonly but over catalog.db. The MCP server never creates or
    writes the catalog; before the first upload there is none, and tools see None."""
    conn = catalog.connect_catalog_readonly(catalog_path)
    try:
        return fn(conn, **kwargs)
    finally:
        if conn is not None:
            conn.close()


def default_catalog_path() -> str:
    return os.getenv("EVIL_CATALOG_PATH") or str(catalog.data_root_from_env().catalog_db)


def create_server(db_path: str | None = None, catalog_path: str | None = None) -> MCPServer:
    """Factory (not a module-level singleton) so tests can point a server at
    a throwaway database instead of the production EVIL_DB_PATH."""
    path = db_path or DEFAULT_DB_PATH
    cat_path = catalog_path or default_catalog_path()
    server = MCPServer("evil")

    @server.tool(name="get_turn")
    async def get_turn_handler(run_id: str, turn_name: str, occurrence: str = "latest") -> dict:
        """Get start/end time, entry speed, exit speed, and duration for one turn
        instance in a run, by turn name. Defaults to the most recent occurrence."""
        return await asyncio.to_thread(
            _run_readonly, path, get_turn, run_id=run_id, turn_name=turn_name, occurrence=occurrence
        )

    @server.tool(name="compare_turn_instances")
    async def compare_turn_instances_handler(run_id: str, turn_name: str, limit: int = 10) -> dict:
        """Compare every instance of a named turn within a run (entry/exit speed,
        duration) to answer what could be improved compared to earlier attempts."""
        return await asyncio.to_thread(
            _run_readonly, path, compare_turn_instances, run_id=run_id, turn_name=turn_name, limit=limit
        )

    @server.tool(name="list_runs")
    async def list_runs_handler() -> list[dict]:
        """List every run_id with its sample count and time range, for browsing."""
        return await asyncio.to_thread(_run_readonly, path, list_runs)

    @server.tool(name="list_turns")
    async def list_turns_handler(run_id: str, limit: int = 50, offset: int = 0) -> dict:
        """List every turn in a run, paginated, for browsing (not a specific lookup)."""
        return await asyncio.to_thread(
            _run_readonly, path, list_turns, run_id=run_id, limit=limit, offset=offset
        )

    @server.tool(name="list_laps")
    async def list_laps_handler(run_id: str, limit: int = 50, offset: int = 0) -> dict:
        """List every lap in a run, paginated, for browsing."""
        return await asyncio.to_thread(
            _run_readonly, path, list_laps, run_id=run_id, limit=limit, offset=offset
        )

    @server.tool(name="list_straights")
    async def list_straights_handler(run_id: str, limit: int = 50, offset: int = 0) -> dict:
        """List every straight in a run, paginated, for browsing."""
        return await asyncio.to_thread(
            _run_readonly, path, list_straights, run_id=run_id, limit=limit, offset=offset
        )

    @server.tool(name="compare_laps")
    async def compare_laps_handler(run_id: str, lap_a: int, lap_b: int) -> dict:
        """Compare two laps in a run by lap number (duration, turn count, energy,
        average speed) to answer what changed or what could be improved."""
        return await asyncio.to_thread(
            _run_readonly, path, compare_laps, run_id=run_id, lap_a=lap_a, lap_b=lap_b
        )

    @server.tool(name="find_nas_files")
    async def find_nas_files_handler(
        run_id: str, start_ts: float | None = None, end_ts: float | None = None
    ) -> list[dict]:
        """Find the stored raw recordings (bag/csv/video/lidar files) covering a run,
        optionally only those overlapping a time range (e.g. a turn's start/end time)."""
        return await asyncio.to_thread(
            _run_catalog, cat_path, find_nas_files, run_id=run_id, start_ts=start_ts, end_ts=end_ts
        )

    @server.tool(name="list_recordings")
    async def list_recordings_handler(
        since: float | None = None,
        until: float | None = None,
        category: str | None = None,
        car: str | None = None,
        parse_status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        """List uploaded recordings, newest first (since/until are epoch seconds;
        category is competition/testing/bench/sim/other). Returns every candidate
        with its date, category, car, location and parse state so similar ones can
        be told apart; if several match, list them and ask which one is meant."""
        return await asyncio.to_thread(
            _run_catalog, cat_path, list_recordings, since=since, until=until, category=category,
            car=car, parse_status=parse_status, limit=limit, offset=offset,
        )

    @server.tool(name="describe_recording")
    async def describe_recording_handler(recording_id: str) -> dict:
        """Everything the catalog knows about one recording: files, streams (topics
        and message counts), time range, location, and how parsing went."""
        return await asyncio.to_thread(_run_catalog, cat_path, describe_recording, recording_id=recording_id)

    @server.tool(name="read_only_sql")
    async def read_only_sql_handler(query: str) -> list[dict]:
        """Run a single read-only SELECT against the EVIL database for questions
        the other tools don't cover. No writes are possible regardless of the
        query text."""
        return await asyncio.to_thread(_run_readonly, path, read_only_sql, query=query)

    return server


def main() -> None:
    server = create_server()
    server.run(transport="streamable-http", host=DEFAULT_HOST, port=DEFAULT_PORT)


if __name__ == "__main__":
    main()
