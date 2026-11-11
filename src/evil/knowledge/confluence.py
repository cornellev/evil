"""Read-only Confluence Data Center client (REST API, personal access token).

Only GET requests exist in this module: Tern's access is read-only because of the code, not the token (a Data
Center PAT carries its owner's full permissions). Cornell's Confluence sits behind a CloudFront WAF that
challenges non-browser clients (202, empty body), so requests carry a browser User-Agent, the same fix as
~/.claude/local-tools/confluence_waf_proxy.py. IPv4 is forced by the indexer process (force_ipv4) because
confluence.cornell.edu has an AAAA record that doesn't route everywhere.

Standard library only (urllib), so the EVIL image needs no new dependency.
"""

from __future__ import annotations

import json
import logging
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

EXPAND = ",".join([
    "body.storage", "version", "ancestors", "metadata.labels", "space",
    "restrictions.read.restrictions.user", "restrictions.read.restrictions.group",
])

# CQL's date literal; Confluence reads it in the token owner's time zone, so callers pass a generous margin.
CQL_DATE_FORMAT = "%Y/%m/%d %H:%M"


class ConfluenceError(RuntimeError):
    pass


@dataclass
class Page:
    id: str
    title: str
    url: str
    space: str
    updated: str                       # ISO 8601, as Confluence reports it
    updated_by: str | None
    version: int
    labels: list[str]
    ancestors: list[tuple[str, str]]   # (id, title), root first
    restricted: bool                   # the page itself carries a view restriction
    storage: str                       # body in storage format (XHTML)
    raw_links: dict[str, Any] = field(default_factory=dict)


Fetch = Callable[[str, dict[str, str]], tuple[int, bytes]]


def _urllib_fetch(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read() or b""


def force_ipv4() -> None:
    """Resolve names to IPv4 only for this process (the indexer; never imported by the server)."""
    original = socket.getaddrinfo

    def ipv4_first(host, *args, **kwargs):
        infos = original(host, *args, **kwargs)
        return [i for i in infos if i[0] == socket.AF_INET] or infos

    socket.getaddrinfo = ipv4_first


def _is_restricted(content: dict) -> bool:
    read = ((content.get("restrictions") or {}).get("read") or {}).get("restrictions") or {}
    for kind in ("user", "group"):
        if ((read.get(kind) or {}).get("results")):
            return True
    return False


def parse_page(content: dict, base_url: str) -> Page:
    links = content.get("_links") or {}
    version = content.get("version") or {}
    by = version.get("by") or {}
    return Page(
        id=str(content["id"]),
        title=content.get("title") or "",
        url=base_url.rstrip("/") + (links.get("webui") or f"/pages/viewpage.action?pageId={content['id']}"),
        space=((content.get("space") or {}).get("key") or "").lower(),
        updated=version.get("when") or "",
        updated_by=by.get("displayName"),
        version=int(version.get("number") or 0),
        labels=[l.get("name", "") for l in ((content.get("metadata") or {}).get("labels") or {}).get("results") or []],
        ancestors=[(str(a["id"]), a.get("title") or "") for a in content.get("ancestors") or []],
        restricted=_is_restricted(content),
        storage=((content.get("body") or {}).get("storage") or {}).get("value") or "",
        raw_links=links,
    )


class ConfluenceClient:
    def __init__(self, base_url: str, token: str, fetch: Fetch | None = None, page_size: int = 50,
                 retries: int = 3, backoff_sec: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token
        self._fetch = fetch or _urllib_fetch
        self.page_size = page_size
        self.retries = retries
        self.backoff_sec = backoff_sec
        self.requests_made = 0

    def _get(self, path: str, params: dict[str, Any]) -> dict:
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(params)}"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "User-Agent": BROWSER_USER_AGENT,
        }
        last = ""
        for attempt in range(self.retries):
            self.requests_made += 1
            status, body = self._fetch(url, headers)
            if status == 200 and body:
                return json.loads(body)
            last = f"HTTP {status}" + (" (empty body: WAF challenge?)" if status in (200, 202) and not body else "")
            if status in (401, 403, 404, 400):
                break                              # not worth retrying; the message says why
            time.sleep(self.backoff_sec * (attempt + 1))
        raise ConfluenceError(f"GET {path} failed: {last}")

    def pages(self, space: str, modified_since: datetime | None = None) -> Iterator[Page]:
        """Every page of `space` (optionally only those modified at or after `modified_since`), with body,
        labels, ancestors and the page's own read restrictions."""
        cql = f'space = "{space}" AND type = page'
        if modified_since is not None:
            cql += f' AND lastmodified >= "{modified_since.strftime(CQL_DATE_FORMAT)}"'
        cql += " ORDER BY lastmodified"
        start = 0
        while True:
            data = self._get("/rest/api/content/search",
                             {"cql": cql, "expand": EXPAND, "limit": self.page_size, "start": start})
            results = data.get("results") or []
            for content in results:
                yield parse_page(content, self.base_url)
            if not results or not (data.get("_links") or {}).get("next"):
                return
            start += len(results)
