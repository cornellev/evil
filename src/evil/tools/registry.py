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
from evil.tools.get_turn import get_turn
from evil.tools.list_laps import list_laps
from evil.tools.list_runs import list_runs
from evil.tools.list_straights import list_straights
from evil.tools.list_turns import list_turns
from evil.tools.nas_index import find_nas_files
from evil.tools.read_only_sql import read_only_sql


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


def build_registry(conn: sqlite3.Connection) -> ToolRegistry:
    return ToolRegistry(
        [
            ToolDef(
                name="get_turn",
                description=(
                    "Get start/end time, entry speed, exit speed, and duration for one turn "
                    "instance in a run, by turn name. Defaults to the most recent occurrence."
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
                name="find_nas_files",
                description=(
                    "Find raw autonomy recordings (bag/video/lidar) on NAS for a run, optionally "
                    "only those overlapping a time range (e.g. a turn's start/end time) to find "
                    "the exact clip covering a moment."
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
                handler=lambda **kw: find_nas_files(conn, **kw),
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
    )
