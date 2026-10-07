"""Fires N concurrent real MCP client sessions at a real running server,
mixing read_only_sql and list_runs. Correctness-only: every one of N calls
must come back right, no crash, no hang. Deliberately not a timing/perf
assertion -- the smoke_full_stack_real_llm.sh timeout-race bug (client
timeout == server timeout, no margin) is a cautionary example of exactly
the kind of flaky test a hard timing assertion invites. Wall-clock time is
printed for visibility only.
"""

from __future__ import annotations

import asyncio
import sys
import time

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

url = sys.argv[1]
CONCURRENCY = 15


async def _one_read_only_sql(i: int) -> tuple[int, bool, str]:
    async with streamable_http_client(url) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("read_only_sql", {"query": "SELECT COUNT(*) AS n FROM turns"})
            structured = (result.structured_content or {}).get("result")
            ok = not result.is_error and structured == [{"n": 2}]
            return i, ok, str(structured)


async def _one_list_runs(i: int) -> tuple[int, bool, str]:
    async with streamable_http_client(url) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("list_runs", {})
            structured = (result.structured_content or {}).get("result")
            ok = not result.is_error and structured == [
                {"run_id": "run-1", "sample_count": 2, "start_ts": 0.0, "end_ts": 42.0,
                 "distance_m": None, "energy_wh": None, "efficiency_mi_per_kwh": None}
            ]
            return i, ok, str(structured)


async def main() -> int:
    calls = [_one_read_only_sql(i) if i % 2 == 0 else _one_list_runs(i) for i in range(CONCURRENCY)]

    start = time.monotonic()
    results = await asyncio.gather(*calls, return_exceptions=True)
    elapsed = time.monotonic() - start

    failures = []
    for i, outcome in enumerate(results):
        if isinstance(outcome, BaseException):
            failures.append(f"call {i} raised: {outcome!r}")
            continue
        idx, ok, detail = outcome
        if not ok:
            failures.append(f"call {idx} got wrong result: {detail}")

    print(f"[concurrency] {CONCURRENCY} concurrent calls finished in {elapsed:.2f}s")

    if failures:
        print(f"FAIL: {len(failures)}/{CONCURRENCY} concurrent calls were wrong:")
        for f in failures:
            print(f"  - {f}")
        return 1

    print(f"OK: all {CONCURRENCY} concurrent calls (mixed read_only_sql + list_runs) returned correctly")
    return 0


sys.exit(asyncio.run(main()))
