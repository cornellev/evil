"""Tracing for the MCP servers: every tool call, from every client, as a span in Phoenix.

The MCP SDK already opens one SERVER span per message (its OpenTelemetryMiddleware is on by
default) and nests it under a caller's trace when the request carries a W3C traceparent in
`_meta` -- which the SDK's own client sends, so Tern's questions get EVIL's spans nested under
them for free. All it lacks is somewhere to send them, and the details worth looking at:
arguments, result, and who called. setup_tracing() supplies the first, ToolCallDetails the second;
who called comes from the tailnet (tailnet.py).

Off unless OTEL_EXPORTER_OTLP_ENDPOINT is set (tests and laptops export nothing). Spans leave in
a background batch, so a tool call never waits on Phoenix, and if Phoenix is down they are
dropped -- tracing can never fail a tool call. See harness/observability-design.md.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import anyio
from opentelemetry import trace

from evil import tailnet

logger = logging.getLogger(__name__)

PROJECT_ENV = "PHOENIX_PROJECT_NAME"
DEFAULT_PROJECT = "evil"
OUTPUT_LIMIT_BYTES = 4096
CLIENT_INFO_META_KEY = "io.modelcontextprotocol/clientInfo"


# Of the MCP SDK's own spans (both ends tag them with mcp.method.name when they start), only tool
# calls are worth reading. The rest is session traffic -- initialize, tools/list, ping, and newer
# clients' polling (server/discover, subscriptions/listen, resources/list, prompts/list) -- which
# outnumbered tool calls by far more than ten to one. Allowlisted rather than blocklisted, so a
# method a future client invents doesn't flood the traces. Spans without the attribute (Tern's
# ask/agent/llm turns) are kept. Dropped at start; their children follow (ParentBased).
TRACED_MCP_METHODS = frozenset({"tools/call"})


def _sampler():
    from opentelemetry.sdk.trace.sampling import ALWAYS_ON, Decision, ParentBased, Sampler, SamplingResult

    class ToolCallsOnly(Sampler):
        def __init__(self) -> None:
            self._base = ParentBased(ALWAYS_ON)

        def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None, trace_state=None):
            method = (attributes or {}).get("mcp.method.name")
            if method is not None and method not in TRACED_MCP_METHODS:
                return SamplingResult(Decision.DROP)
            return self._base.should_sample(parent_context, trace_id, name, kind, attributes, links, trace_state)

        def get_description(self) -> str:
            return "ToolCallsOnly"

    return ToolCallsOnly()


def setup_tracing(service_name: str) -> bool:
    """Installs an OTLP/HTTP exporter as the global tracer provider. Returns whether it did."""
    if not (os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")):
        return False
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create({
        "service.name": service_name,
        # Phoenix files spans into a project by this resource attribute.
        "openinference.project.name": os.getenv(PROJECT_ENV, DEFAULT_PROJECT),
    })
    provider = TracerProvider(resource=resource, sampler=_sampler())
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    logger.info("tracing to %s as %s", os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), service_name)
    return True


def _client_info(ctx: Any) -> dict[str, Any]:
    """The client's name/version: from the initialize handshake, or (newer protocol versions,
    which can skip the handshake) from the request's own _meta."""
    params = getattr(ctx.session, "client_params", None)
    info = getattr(params, "client_info", None)
    if info is not None:
        return {"name": getattr(info, "name", None), "version": getattr(info, "version", None)}
    meta = ctx.meta if isinstance(ctx.meta, dict) else (getattr(ctx.meta, "model_extra", None) or {})
    raw = meta.get(CLIENT_INFO_META_KEY)
    return raw if isinstance(raw, dict) else {}


def _serialize(result: Any) -> tuple[str, int | None]:
    """(JSON text of what the tool returned, row count if it returned a list)."""
    if hasattr(result, "model_dump"):
        result = result.model_dump(mode="json", by_alias=True, exclude_none=True)
    if isinstance(result, dict):
        structured = result.get("structuredContent")
        if isinstance(structured, dict) and set(structured) == {"result"}:
            structured = structured["result"]
        texts = [c.get("text") for c in result.get("content") or [] if isinstance(c, dict) and c.get("type") == "text"]
        if structured is not None:
            result = structured
        elif texts:
            # No structured copy (a dict-returning tool): one JSON text block per value, as
            # tern-llm's mcp_client.py also reads it.
            parsed = []
            for t in texts:
                try:
                    parsed.append(json.loads(t))
                except (TypeError, ValueError):
                    parsed.append(t)
            result = parsed[0] if len(parsed) == 1 else parsed
    rows = len(result) if isinstance(result, list) else None
    return json.dumps(result, default=str), rows


class ToolCallDetails:
    """Server middleware: adds arguments, result, client and caller to the SDK's tools/call span.
    Runs inside that span, so it decorates the current span rather than opening its own."""

    async def __call__(self, ctx: Any, call_next: Any) -> Any:
        if ctx.method != "tools/call":
            return await call_next(ctx)
        span = trace.get_current_span()
        if not span.is_recording():
            return await call_next(ctx)

        params = ctx.params or {}
        try:
            span.set_attributes({
                "openinference.span.kind": "TOOL",
                "tool.name": str(params.get("name")),
                "input.value": json.dumps(params.get("arguments") or {}, default=str),
                "input.mime_type": "application/json",
            })
            client = _client_info(ctx)
            for key in ("name", "version"):
                if client.get(key):
                    span.set_attribute(f"evil.client.{key}", str(client[key]))
            request = ctx.request
            host = getattr(getattr(request, "client", None), "host", None)
            if host:
                span.set_attribute("evil.client.ip", host)
                who = await anyio.to_thread.run_sync(tailnet.lookup, host)
                for key, attr in (("user", "evil.user"), ("user_name", "evil.user.name"),
                                  ("device", "evil.device"), ("tags", "evil.device.tags")):
                    if who.get(key):
                        span.set_attribute(attr, who[key])
                if who.get("user"):
                    span.set_attribute("user.id", who["user"])  # OpenInference's user field
        except Exception:  # never let decorating a span break the call itself
            logger.debug("could not add tool call details", exc_info=True)

        result = await call_next(ctx)

        try:
            text, rows = _serialize(result)
            span.set_attribute("evil.result.bytes", len(text.encode()))
            if rows is not None:
                span.set_attribute("evil.result.rows", rows)
            if len(text) > OUTPUT_LIMIT_BYTES:
                text = text[:OUTPUT_LIMIT_BYTES] + f"... [truncated, {len(text)} chars]"
            span.set_attributes({"output.value": text, "output.mime_type": "application/json"})
        except Exception:
            logger.debug("could not record tool result", exc_info=True)
        return result
