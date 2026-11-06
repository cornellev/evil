"""The contract every IngestionSource must meet (base.py), as a reusable check: ReplaySource
and Ros2Source are tested against it, and a future ZenohSource just has to pass it too.

`check_source(make_source, send)`:
  make_source() -> the source under test; send(texts) delivers raw message texts to it
  (in-memory for ReplaySource, a real topic for Ros2Source). The check sends a fixed script
  (a v1 payload with bare NaN, broken JSON, a non-object JSON value, an empty string) and
  verifies what comes out of envelopes():
    - one Envelope per message, in order, none skipped or merged
    - ts is a float receive time (epoch seconds), non-decreasing
    - decodable messages: payload is the decoded JSON (NaN intact), error None
    - undecodable ones: payload None, error set, NOT dropped
    - raw is the exact text received (live captures are byte for byte)
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from typing import Callable

from evil.ingestion.pipeline import Envelope

SCRIPT = [
    '{"seq": 2, "global_ts": 1000, "gps": {"lat": NaN, "long": -86.2}, "_t_publish_ns": 1775000000000000000}',
    '{"seq": 4, "gps": {"lat": 39.79',
    '[1, 2, 3]',
    '',
    '{"seq": 6}',
]


def check_envelopes(envs: list[Envelope], sent: list[str], started_at: float) -> None:
    assert len(envs) == len(sent), f"{len(envs)} envelopes for {len(sent)} messages"
    prev = started_at - 1.0
    for env, text in zip(envs, sent):
        assert isinstance(env, Envelope)
        assert isinstance(env.ts, float) and env.ts >= prev and env.ts > 1e9, f"bad receive time {env.ts!r}"
        prev = env.ts
        assert env.raw == text, f"raw text changed: {env.raw!r} != {text!r}"
        try:
            expected = json.loads(text)
        except ValueError:
            assert env.payload is None and env.error, f"undecodable message not flagged: {text!r}"
            continue
        assert env.error is None
        if isinstance(expected, dict) and "gps" in expected and isinstance(expected["gps"].get("lat"), float):
            assert math.isnan(env.payload["gps"]["lat"]) == math.isnan(expected["gps"]["lat"])
            expected = {**expected, "gps": {**expected["gps"], "lat": "nan"}}
            got = {**env.payload, "gps": {**env.payload["gps"], "lat": "nan"}}
            assert got == expected
        else:
            assert env.payload == expected


async def collect(source, n: int, timeout: float = 30.0) -> list[Envelope]:
    out: list[Envelope] = []

    async def drain():
        async for env in source.envelopes():
            out.append(env)
            if len(out) >= n:
                return

    await asyncio.wait_for(drain(), timeout)
    return out


def check_source(make_source: Callable[[list[str]], object], script: list[str] = SCRIPT) -> None:
    """For in-memory sources: make_source(texts) returns a finite source carrying them."""
    started = time.time()
    envs = asyncio.run(collect(make_source(script), len(script)))
    check_envelopes(envs, script, started)
