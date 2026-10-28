"""Who is calling, from the tailnet: maps a caller's Tailscale IP to its login and device, for traces.

The answer comes from the NUC's tailscaled (its LocalAPI whois, what `tailscale whois` prints), so
it can't be self-declared and needs nothing configured on anyone's machine. Shared machines (thor,
the DAQ) are tagged devices: they get a device name and tags but no person.

Two halves, because tailscaled gives full admin rights to anything connecting as root, and EVIL's
servers run as root (and mcp-raw reads files anyone can upload). So only a tiny separate service
holds the socket, as an unprivileged user, which tailscaled limits to read-only calls like whois:

- `python -m evil.tailnet` is that service (compose `tailnet-whois`, internal network only):
  GET /whois?ip=100.x.y.z -> {"user", "user_name", "device"} or {"device", "tags"}, {} if unknown.
- `lookup(ip)` is what the MCP servers call, via EVIL_WHOIS_URL. Cached per IP, short timeout,
  and any failure is just {} -- a trace without a name, never a failed tool call.

Callers that come through a Tailscale Service (https://cev-mcp...) reach EVIL via tailscaled's
own proxy, so the source address is the NUC itself and whois can't name them. Serve adds
Tailscale-User-Login / -Name headers instead (and strips copies a client sends), so `identify()`
reads those -- but only on requests from a trusted local proxy address; a request straight from
a tailnet or LAN address could have written them itself.
"""

from __future__ import annotations

import http.client
import json
import logging
import os
import socket
import time
import urllib.parse
import urllib.request
from email.header import decode_header, make_header
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import ip_address, ip_network
from typing import Any

logger = logging.getLogger(__name__)

SOCKET_ENV = "EVIL_TAILSCALE_SOCKET"
DEFAULT_SOCKET = "/var/run/tailscale/tailscaled.sock"
WHOIS_URL_ENV = "EVIL_WHOIS_URL"
PORT_ENV = "EVIL_WHOIS_PORT"
DEFAULT_PORT = 8770
TIMEOUT_S = 0.5
CACHE_TTL_S = 600
# Where a local proxy's requests appear from: loopback, and Docker's bridge networks (a request
# tailscaled proxies to localhost:8765 reaches the container from its bridge gateway).
TRUSTED_PROXIES_ENV = "EVIL_TRUSTED_PROXIES"
DEFAULT_TRUSTED_PROXIES = "127.0.0.0/8,::1/128,172.16.0.0/12"
TAILNET_NETS = (ip_network("100.64.0.0/10"), ip_network("fd7a:115c:a1e0::/48"))


def parse_whois(data: dict[str, Any]) -> dict[str, str]:
    """tailscaled's whois JSON -> the few fields a trace needs."""
    node = data.get("Node") or {}
    device = node.get("ComputedName") or (node.get("Name") or "").split(".")[0]
    out = {"device": device} if device else {}
    tags = node.get("Tags") or []
    if tags:  # a shared machine: its "user" is the placeholder tagged-devices
        out["tags"] = ",".join(tags)
        return out
    profile = data.get("UserProfile") or {}
    if profile.get("LoginName"):
        out["user"] = profile["LoginName"]
    if profile.get("DisplayName"):
        out["user_name"] = profile["DisplayName"]
    return out


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float) -> None:
        # tailscaled refuses LocalAPI requests whose Host isn't this name.
        super().__init__("local-tailscaled.sock", timeout=timeout)
        self._path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


def whois_from_socket(ip: str, path: str | None = None) -> dict[str, str]:
    conn = _UnixHTTPConnection(path or os.getenv(SOCKET_ENV, DEFAULT_SOCKET), TIMEOUT_S * 4)
    try:
        conn.request("GET", "/localapi/v0/whois?addr=" + urllib.parse.quote(ip))
        resp = conn.getresponse()
        body = resp.read()
    finally:
        conn.close()
    if resp.status != 200:  # 404 = not a tailnet address (e.g. a Docker bridge IP)
        return {}
    return parse_whois(json.loads(body))


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        url = urllib.parse.urlsplit(self.path)
        ip = urllib.parse.parse_qs(url.query).get("ip", [""])[0]
        if url.path != "/whois" or not ip:
            self.send_error(404)
            return
        try:
            info = whois_from_socket(ip)
        except Exception:
            logger.warning("whois %s failed", ip, exc_info=True)
            info = {}
        body = json.dumps(info).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # one line per tool call is noise
        pass


_cache: dict[str, tuple[float, dict[str, str]]] = {}


def lookup(ip: str) -> dict[str, str]:
    """The caller's tailnet identity, {} when unknown or when EVIL_WHOIS_URL isn't set. Blocking
    (call it off the event loop); cached, including misses, so an outage costs one timeout per IP."""
    base = os.getenv(WHOIS_URL_ENV)
    if not base or not ip:
        return {}
    now = time.monotonic()
    hit = _cache.get(ip)
    if hit and hit[0] > now:
        return hit[1]
    info: dict[str, str] = {}
    try:
        with urllib.request.urlopen(f"{base.rstrip('/')}/whois?ip={urllib.parse.quote(ip)}", timeout=TIMEOUT_S) as r:
            data = json.load(r)
        if isinstance(data, dict):
            info = {k: str(v) for k, v in data.items()}
    except Exception:
        logger.debug("tailnet lookup for %s failed", ip, exc_info=True)
    _cache[ip] = (now + CACHE_TTL_S, info)
    return info


def _within(ip: str, nets) -> bool:
    try:
        addr = ip_address(ip)
    except ValueError:
        return False
    return any(addr.version == n.version and addr in n for n in nets)


def _trusted_proxies():
    raw = os.getenv(TRUSTED_PROXIES_ENV, DEFAULT_TRUSTED_PROXIES)
    return [ip_network(part.strip(), strict=False) for part in raw.split(",") if part.strip()]


def _header_text(value: str | None) -> str | None:
    """Tailscale sends a non-ASCII display name RFC 2047-encoded (=?utf-8?q?...?=)."""
    if not value:
        return None
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def identify(source_ip: str | None, headers: Any) -> tuple[str | None, dict[str, str]]:
    """(the caller's address, who they are) for one request. Blocking, like lookup()."""
    if not source_ip:
        return None, {}
    if not _within(source_ip, _trusted_proxies()):
        return source_ip, lookup(source_ip)  # direct: the address is the caller; headers ignored
    get = headers.get if headers is not None else (lambda _key: None)
    # A reverse proxy appends the address it saw, so only the LAST entry is the proxy's own.
    forwarded = (get("x-forwarded-for") or "").split(",")[-1].strip()
    client = forwarded if _within(forwarded, TAILNET_NETS) else source_ip
    who = dict(lookup(client)) if client != source_ip else {}
    login = get("tailscale-user-login")
    if login:
        who.pop("tags", None)
        who["user"] = login[:200]
        name = _header_text(get("tailscale-user-name"))
        if name:
            who["user_name"] = name[:200]
    return client, who


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    port = int(os.getenv(PORT_ENV, DEFAULT_PORT))
    logger.info("tailnet whois on :%d, socket %s", port, os.getenv(SOCKET_ENV, DEFAULT_SOCKET))
    ThreadingHTTPServer(("0.0.0.0", port), _Handler).serve_forever()


if __name__ == "__main__":
    main()
