"""Real MCP client against a running cev-knowledge server over Streamable HTTP, plus /healthz."""

import asyncio
import json
import sys
import urllib.request

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

base = sys.argv[1].rstrip("/")


async def main() -> int:
    async with streamable_http_client(f"{base}/mcp") as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            names = {t.name for t in (await session.list_tools()).tools}
            if names != {"search_team_knowledge", "get_document"}:
                print(f"FAIL: tools {names}")
                return 1
            res = await session.call_tool("search_team_knowledge", {"query": "where are the joulemeter specs?"})
            hits = (res.structured_content or {}).get("result") or []
            if res.is_error or not hits or hits[0]["title"] != "Joulemeter":
                print(f"FAIL: search returned {hits!r}")
                return 1
            if not any(h["source"] == "drive" and h["title"] == "PDR deck" for h in hits):
                print(f"FAIL: no Drive fallback link in {hits!r}")
                return 1
            doc = await session.call_tool("get_document", {"source": "confluence", "id": hits[0]["id"]})
            text = doc.content[0].text
            if doc.is_error or "Shunt resistor" not in text:
                print(f"FAIL: get_document returned {text[:300]}")
                return 1
            bad = await session.call_tool("get_document", {"source": "drive", "id": "x"})
            if not bad.is_error:
                print("FAIL: reading a Drive document should be refused for now")
                return 1
    health = json.loads(urllib.request.urlopen(f"{base}/healthz", timeout=5).read())
    if not health.get("ok") or health.get("pages") != 3:
        print(f"FAIL: healthz {health}")
        return 1
    print(f"ok: {len(hits)} hits, healthz {health['pages']} pages / {health['chunks']} chunks")
    return 0


sys.exit(asyncio.run(main()))
