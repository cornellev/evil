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


def test_only_tool_calls_are_kept_of_the_sdk_mcp_spans():
    sampler = telemetry._sampler()
    from opentelemetry.sdk.trace.sampling import Decision

    def decision(name, method=None):
        attrs = {"mcp.method.name": method} if method else None
        return sampler.should_sample(None, 1, name, attributes=attrs).decision

    for method in ("initialize", "tools/list", "notifications/initialized", "ping", "server/discover",
                   "subscriptions/listen", "resources/list", "resources/templates/list", "prompts/list",
                   "some/method-a-future-client-invents"):
        assert decision(method, method) == Decision.DROP, method
        assert decision(f"MCP send {method}", method) == Decision.DROP, method
    assert decision("tools/call get_turn", "tools/call") == Decision.RECORD_AND_SAMPLE
    assert decision("MCP send tools/call get_turn", "tools/call") == Decision.RECORD_AND_SAMPLE
    for name in ("ask", "agent", "llm turn 1"):  # Tern's own spans carry no MCP method
        assert decision(name) == Decision.RECORD_AND_SAMPLE, name


def test_the_sdk_tags_its_spans_with_the_method_when_they_start(evil_db_path):
    """The sampler only sees attributes given at span start; this pins that the SDK gives the
    method there (an SDK upgrade that sets it later would silently turn the filter off)."""
    seen = []

    class Recording(type(telemetry._sampler())):
        def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None, trace_state=None):
            seen.append((name, (attributes or {}).get("mcp.method.name")))
            return super().should_sample(parent_context, trace_id, name, kind, attributes, links, trace_state)

    import mcp.shared._otel as sdk_otel

    provider = TracerProvider(sampler=Recording())
    real_tracer = sdk_otel._tracer
    sdk_otel._tracer = provider.get_tracer("mcp")
    try:
        _call(create_server(db_path=evil_db_path), "list_runs", {})
    finally:
        sdk_otel._tracer = real_tracer
    assert ("tools/call list_runs", "tools/call") in seen
    # and session traffic (server/discover or initialize, tools/list, depending on protocol version)
    assert any(method not in (None, "tools/call") and name == method for name, method in seen)



# --- who called: tailnet identity (tailnet.py) ---

from evil import tailnet  # noqa: E402

PERSON = {"Node": {"Name": "ajayslaptop.tail4ccb95.ts.net.", "ComputedName": "ajayslaptop"},
          "UserProfile": {"LoginName": "AjayParthibha@github", "DisplayName": "Ajay Parthibha"}}
SHARED = {"Node": {"Name": "thor.tail4ccb95.ts.net.", "ComputedName": "thor", "Tags": ["tag:general"]},
          "UserProfile": {"LoginName": "tagged-devices", "DisplayName": "Tagged Devices"}}


def test_whois_names_a_person_and_their_device():
    assert tailnet.parse_whois(PERSON) == {"device": "ajayslaptop", "user": "AjayParthibha@github",
                                          "user_name": "Ajay Parthibha"}


def test_whois_of_a_shared_machine_names_no_person():
    assert tailnet.parse_whois(SHARED) == {"device": "thor", "tags": "tag:general"}


@pytest.fixture
def whois_service(tmp_path, monkeypatch):
    """A fake tailscaled on a unix socket, and the real whois service in front of it."""
    import socketserver
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    known = {"100.65.206.31": PERSON, "100.94.168.55": SHARED}

    class Tailscaled(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.headers["Host"] == "local-tailscaled.sock"
            ip = self.path.split("addr=", 1)[1]
            if ip not in known:
                self.send_error(404, "no match for IP:port")
                return
            body = json.dumps(known[ip]).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
        daemon_threads = True

        def get_request(self):  # BaseHTTPRequestHandler wants a (host, port) client address
            sock, _ = super().get_request()
            return sock, ("local", 0)

    sock_path = str(tmp_path / "tailscaled.sock")
    fake = UnixServer(sock_path, Tailscaled)
    service = ThreadingHTTPServer(("127.0.0.1", 0), tailnet._Handler)
    for server in (fake, service):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv(tailnet.SOCKET_ENV, sock_path)
    monkeypatch.setenv(tailnet.WHOIS_URL_ENV, f"http://127.0.0.1:{service.server_address[1]}")
    monkeypatch.setattr(tailnet, "_cache", {})
    yield
    service.shutdown()
    fake.shutdown()


def test_lookup_goes_through_the_whois_service_to_tailscaled(whois_service):
    assert tailnet.lookup("100.65.206.31")["user"] == "AjayParthibha@github"
    assert tailnet.lookup("100.94.168.55") == {"device": "thor", "tags": "tag:general"}
    assert tailnet.lookup("172.18.0.1") == {}  # not on the tailnet (Docker's own bridge)


def test_lookup_is_off_without_a_service_url(monkeypatch):
    monkeypatch.delenv(tailnet.WHOIS_URL_ENV, raising=False)
    assert tailnet.lookup("100.65.206.31") == {}


def test_lookup_caches_failures_so_an_outage_costs_one_timeout_per_ip(monkeypatch):
    monkeypatch.setenv(tailnet.WHOIS_URL_ENV, "http://127.0.0.1:9")
    monkeypatch.setattr(tailnet, "_cache", {})
    calls = []
    real = tailnet.urllib.request.urlopen
    monkeypatch.setattr(tailnet.urllib.request, "urlopen", lambda *a, **k: calls.append(1) or real(*a, **k))
    assert tailnet.lookup("100.65.206.31") == {}
    assert tailnet.lookup("100.65.206.31") == {}
    assert len(calls) == 1


def test_tool_span_names_the_caller_from_the_tailnet(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(tailnet, "lookup", lambda ip: tailnet.parse_whois(PERSON) if ip == "100.65.206.31" else {})
    ctx = SimpleNamespace(method="tools/call", params={"name": "list_runs", "arguments": {}}, meta=None,
                          session=SimpleNamespace(client_params=None),
                          request=SimpleNamespace(client=SimpleNamespace(host="100.65.206.31")))

    async def call_next(_):
        return {"content": [], "structuredContent": {"result": []}}

    async def go():
        with trace.get_tracer("t").start_as_current_span("tools/call list_runs"):
            await telemetry.ToolCallDetails()(ctx, call_next)
    asyncio.run(go())

    (span,) = _EXPORTER.get_finished_spans()
    a = span.attributes
    assert (a["evil.user"], a["user.id"], a["evil.user.name"], a["evil.device"]) == (
        "AjayParthibha@github", "AjayParthibha@github", "Ajay Parthibha", "ajayslaptop")
    assert a["evil.client.ip"] == "100.65.206.31"


# --- who called, through a Tailscale Service (Serve's identity headers) ---

def _fake_whois(monkeypatch):
    known = {"100.65.206.31": tailnet.parse_whois(PERSON), "100.94.168.55": tailnet.parse_whois(SHARED)}
    monkeypatch.setattr(tailnet, "lookup", lambda ip: known.get(ip, {}))
    monkeypatch.delenv(tailnet.TRUSTED_PROXIES_ENV, raising=False)


SERVE_HEADERS = {"tailscale-user-login": "AjayParthibha@github", "tailscale-user-name": "Ajay Parthibha",
                 "x-forwarded-for": "100.65.206.31"}


def test_via_tailscale_serve_the_headers_name_the_caller(monkeypatch):
    _fake_whois(monkeypatch)
    ip, who = tailnet.identify("172.18.0.1", SERVE_HEADERS)  # Docker bridge gateway: tailscaled's proxy
    assert ip == "100.65.206.31"
    assert who == {"device": "ajayslaptop", "user": "AjayParthibha@github", "user_name": "Ajay Parthibha"}


def test_a_direct_tailnet_caller_cannot_claim_someone_else(monkeypatch):
    _fake_whois(monkeypatch)
    ip, who = tailnet.identify("100.94.168.55", {"tailscale-user-login": "AjayParthibha@github"})
    assert ip == "100.94.168.55" and who == {"device": "thor", "tags": "tag:general"}


def test_a_lan_caller_cannot_claim_an_identity(monkeypatch):
    _fake_whois(monkeypatch)
    assert tailnet.identify("192.168.1.50", SERVE_HEADERS) == ("192.168.1.50", {})


def test_only_the_proxys_own_forwarded_entry_counts(monkeypatch):
    _fake_whois(monkeypatch)
    ip, who = tailnet.identify("127.0.0.1", {**SERVE_HEADERS, "x-forwarded-for": "100.94.168.55, 100.65.206.31"})
    assert ip == "100.65.206.31" and who["device"] == "ajayslaptop"


def test_without_a_tailnet_forwarded_address_the_headers_still_name_the_person(monkeypatch):
    _fake_whois(monkeypatch)
    ip, who = tailnet.identify("127.0.0.1", {"tailscale-user-login": "AjayParthibha@github"})
    assert ip == "127.0.0.1" and who == {"user": "AjayParthibha@github"}


def test_a_non_ascii_display_name_is_decoded(monkeypatch):
    _fake_whois(monkeypatch)
    _, who = tailnet.identify("127.0.0.1", {"tailscale-user-login": "jose@github",
                                            "tailscale-user-name": "=?utf-8?q?Jos=C3=A9?="})
    assert who["user_name"] == "José"


def test_trusted_proxy_addresses_are_configurable(monkeypatch):
    _fake_whois(monkeypatch)
    monkeypatch.setenv(tailnet.TRUSTED_PROXIES_ENV, "10.0.0.0/8")
    assert tailnet.identify("172.18.0.1", SERVE_HEADERS) == ("172.18.0.1", {})
    assert tailnet.identify("10.1.2.3", SERVE_HEADERS)[1]["user"] == "AjayParthibha@github"


def test_tool_span_via_serve_records_the_person_and_the_proxy(monkeypatch):
    from types import SimpleNamespace

    _fake_whois(monkeypatch)
    ctx = SimpleNamespace(method="tools/call", params={"name": "list_runs", "arguments": {}}, meta=None,
                          session=SimpleNamespace(client_params=None),
                          request=SimpleNamespace(client=SimpleNamespace(host="172.18.0.1"), headers=SERVE_HEADERS))

    async def call_next(_):
        return {"content": [], "structuredContent": {"result": []}}

    async def go():
        with trace.get_tracer("t").start_as_current_span("tools/call list_runs"):
            await telemetry.ToolCallDetails()(ctx, call_next)
    asyncio.run(go())

    (span,) = _EXPORTER.get_finished_spans()
    a = span.attributes
    assert (a["evil.user"], a["evil.client.ip"], a["evil.client.via"], a["evil.device"]) == (
        "AjayParthibha@github", "100.65.206.31", "172.18.0.1", "ajayslaptop")
