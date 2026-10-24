"""ToolCallDetails decorates the MCP SDK's own tools/call span; these drive a real in-memory MCP
client against a real server so the middleware chain (SDK span outside, ours inside) is the
one production runs. Spans go to an in-memory exporter installed once for this module."""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from mcp.client.client import Client
from mcp.types import Implementation
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from evil import telemetry
from evil.mcp_server import create_server

_EXPORTER = InMemorySpanExporter()


@pytest.fixture(scope="module", autouse=True)
def _provider():
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(_EXPORTER))
    trace.set_tracer_provider(provider)  # first and only set in this process; later sets are ignored
    yield


@pytest.fixture(autouse=True)
def _clear():
    _EXPORTER.clear()


def _call(server, name, arguments, client_name="test-client"):
    async def go():
        async with Client(server, client_info=Implementation(name=client_name, version="1.2")) as client:
            return await client.call_tool(name, arguments)
    return asyncio.run(go())


def _tool_spans():
    return [s for s in _EXPORTER.get_finished_spans() if s.attributes.get("mcp.method.name") == "tools/call"]


def test_tool_call_span_carries_arguments_result_and_client(evil_db_path):
    _call(create_server(db_path=evil_db_path), "get_turn", {"run_id": "run-1", "turn_name": "Turn 3"})

    (span,) = _tool_spans()
    a = span.attributes
    assert a["tool.name"] == "get_turn"
    assert a["openinference.span.kind"] == "TOOL"
    assert json.loads(a["input.value"]) == {"run_id": "run-1", "turn_name": "Turn 3"}
    assert json.loads(a["output.value"])["found"] is True
    assert a["evil.result.bytes"] > 0
    assert a["evil.client.name"] == "test-client"
    assert a["evil.client.version"] == "1.2"


def test_list_results_record_a_row_count(evil_db_path):
    _call(create_server(db_path=evil_db_path), "read_only_sql", {"query": "SELECT 1 AS a UNION ALL SELECT 2"})

    (span,) = _tool_spans()
    assert span.attributes["evil.result.rows"] == 2


def test_large_results_are_truncated_in_the_span_not_in_the_reply(evil_db_path, monkeypatch):
    monkeypatch.setattr(telemetry, "OUTPUT_LIMIT_BYTES", 20)
    result = _call(create_server(db_path=evil_db_path), "get_turn", {"run_id": "run-1", "turn_name": "Turn 3"})

    (span,) = _tool_spans()
    assert "truncated" in span.attributes["output.value"]
    assert "truncated" not in result.content[0].text  # the client still gets everything


def test_non_tool_messages_are_left_alone(evil_db_path):
    async def go():
        async with Client(create_server(db_path=evil_db_path)) as client:
            await client.list_tools()
    asyncio.run(go())

    spans = _EXPORTER.get_finished_spans()
    assert spans and not any("input.value" in s.attributes for s in spans)


def test_setup_tracing_is_off_without_an_endpoint(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    assert telemetry.setup_tracing("evil-mcp") is False


def test_an_unreachable_collector_never_blocks_or_raises():
    """The batch exporter runs off the request path; this checks the exporter itself fails
    quietly and quickly when Phoenix isn't there."""
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint="http://127.0.0.1:9/v1/traces", timeout=1)))
    start = time.monotonic()
    with provider.get_tracer("t").start_as_current_span("s"):
        pass
    assert time.monotonic() - start < 0.5
    provider.shutdown()


def test_session_housekeeping_spans_are_dropped_but_tool_calls_kept():
    sampler = telemetry._sampler()
    from opentelemetry.sdk.trace.sampling import Decision

    def decision(name):
        return sampler.should_sample(None, 1, name).decision

    for name in ("initialize", "tools/list", "MCP send initialize", "MCP send tools/list",
                 "notifications/initialized", "MCP send notifications/initialized", "ping"):
        assert decision(name) == Decision.DROP, name
    for name in ("tools/call get_turn", "MCP send tools/call get_turn", "ask", "agent"):
        assert decision(name) == Decision.RECORD_AND_SAMPLE, name
