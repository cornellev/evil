"""Binds the pure tool functions to a live connection and exposes them as
Ollama/OpenAI-shaped tool schemas. Used both by evil's own MCP server
(mcp_server.py) and directly in tests -- moved here from tern-llm because
this code is schema-coupled to EVIL's tables, not to the harness.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Callable

from evil.tools.compare_laps import compare_laps
from evil.tools.compare_turn_instances import compare_turn_instances
from evil.tools.get_straight import get_straight
from evil.tools.get_turn import get_turn
from evil.tools.list_laps import list_laps
from evil.tools.list_runs import list_runs
from evil.tools.list_straights import list_straights
from evil.tools.list_turns import list_turns
from evil.tools.catalog_tools import describe_recording, find_nas_files, list_recordings
from evil.tools.read_only_sql import read_only_sql
from evil.tools.segment_tools import list_track_segments


@dataclass(frozen=True)
class ToolDef:
    name: str
    description: str
    parameters: dict[str, Any]
    handler: Callable[..., Any]


class ToolRegistry:
    def __init__(self, tools: list[ToolDef]):
        self._by_name = {t.name: t for t in tools}

    def schemas(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters,
                },
            }
            for t in self._by_name.values()
        ]

    def invoke(self, name: str, arguments: dict[str, Any]) -> Any:
        if name not in self._by_name:
            raise KeyError(f"unknown tool: {name}")
        return self._by_name[name].handler(**arguments)


def build_registry(conn: sqlite3.Connection, catalog_conn: sqlite3.Connection | None = None) -> ToolRegistry:
    tools = [
            ToolDef(
                name="get_turn",
                description=(
                    "Get start/end time, entry/exit speed, duration, distance, energy (Wh) and "
                    "efficiency (mi/kWh) for one turn instance in a run, by turn name or official "
                    "number. Defaults to the most recent occurrence."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "turn_name": {"type": "string"},
                        "occurrence": {"type": "string", "enum": ["latest", "first"]},
                    },
                    "required": ["run_id", "turn_name"],
                },
                handler=lambda **kw: get_turn(conn, **kw),
            ),
            ToolDef(
                name="get_straight",
                description=(
                    "Get time, entry/exit speed, distance, energy (Wh) and efficiency (mi/kWh) for one "
                    "pass down a named straight (e.g. 'Straight 6-7'). Defaults to the most recent occurrence."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "straight_name": {"type": "string"},
                        "occurrence": {"type": "string", "enum": ["latest", "first"]},
                    },
                    "required": ["run_id", "straight_name"],
                },
                handler=lambda **kw: get_straight(conn, **kw),
            ),
            ToolDef(
                name="list_track_segments",
                description="The track's turns and straights in lap order (name, kind, length, official turn numbers).",
                parameters={"type": "object", "properties": {}},
                handler=lambda **kw: list_track_segments(conn, **kw),
            ),
            ToolDef(
                name="compare_turn_instances",
                description=(
                    "Compare every instance of a named turn within a run (entry/exit speed, "
                    "duration) to answer questions like what could be improved compared to "
                    "earlier attempts at the same turn."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "turn_name": {"type": "string"},
                        "limit": {"type": "integer"},
                    },
                    "required": ["run_id", "turn_name"],
                },
                handler=lambda **kw: compare_turn_instances(conn, **kw),
            ),
            ToolDef(
                name="list_runs",
                description="List every run_id with its sample count and time range, for browsing.",
                parameters={"type": "object", "properties": {}},
                handler=lambda **kw: list_runs(conn, **kw),
            ),
            ToolDef(
                name="list_turns",
                description="List every turn in a run, paginated, for browsing (not for a specific lookup).",
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                    },
                    "required": ["run_id"],
                },
                handler=lambda **kw: list_turns(conn, **kw),
            ),
            ToolDef(
                name="list_laps",
                description="List every lap in a run, paginated, for browsing.",
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                    },
                    "required": ["run_id"],
                },
                handler=lambda **kw: list_laps(conn, **kw),
            ),
            ToolDef(
                name="list_straights",
                description="List every straight in a run, paginated, for browsing.",
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                    },
                    "required": ["run_id"],
                },
                handler=lambda **kw: list_straights(conn, **kw),
            ),
            ToolDef(
                name="compare_laps",
                description=(
                    "Compare two laps in a run by lap number (duration, turn count, energy, "
                    "average speed) to answer what changed or what could be improved."
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "run_id": {"type": "string"},
                        "lap_a": {"type": "integer"},
                        "lap_b": {"type": "integer"},
                    },
                    "required": ["run_id", "lap_a", "lap_b"],
                },
                handler=lambda **kw: compare_laps(conn, **kw),
            ),
            ToolDef(
                name="read_only_sql",
                description=(
                    "Run a single read-only SELECT against the EVIL database for questions "
                    "the other tools don't cover. No writes are possible regardless of the "
                    "query text."
                ),
                parameters={
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
                handler=lambda **kw: read_only_sql(conn, **kw),
            ),
    ]
    if catalog_conn is not None:
        tools += _catalog_tools(catalog_conn)
    return ToolRegistry(tools)


def _catalog_tools(catalog_conn: sqlite3.Connection) -> list[ToolDef]:
    return [
        ToolDef(
            name="find_nas_files",
            description=(
                "Find the stored raw recordings (bag/csv/video/lidar files) covering a run, "
                "optionally only those overlapping a time range (e.g. a turn's start/end time) "
                "to find the exact clip covering a moment."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "start_ts": {"type": "number"},
                    "end_ts": {"type": "number"},
                },
                "required": ["run_id"],
            },
            handler=lambda **kw: find_nas_files(catalog_conn, **kw),
        ),
        ToolDef(
            name="list_recordings",
            description=(
                "List uploaded recordings, newest first, with date, category, car, location and "
                "parse state. If several match, list them and ask which one is meant."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "since": {"type": "number"},
                    "until": {"type": "number"},
                    "category": {"type": "string"},
                    "car": {"type": "string"},
                    "parse_status": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
            },
            handler=lambda **kw: list_recordings(catalog_conn, **kw),
        ),
        ToolDef(
            name="describe_recording",
            description="Everything the catalog knows about one recording: files, streams, time range, location, parse state.",
            parameters={
                "type": "object",
                "properties": {"recording_id": {"type": "string"}},
                "required": ["recording_id"],
            },
            handler=lambda **kw: describe_recording(catalog_conn, **kw),
        ),
    ]
