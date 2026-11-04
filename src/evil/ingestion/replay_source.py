"""An IngestionSource backed by an in-memory list of envelopes (or payload dicts). Used by
tests, and by offline replay without a live ROS2/Zenoh connection."""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Iterable

from evil.ingestion.pipeline import Envelope


class ReplaySource:
    def __init__(self, items: Iterable[Envelope | dict], delay_s: float = 0.0, start_ts: float = 0.0,
                 step_s: float = 0.02):
        """items: Envelopes as-is, or payload dicts stamped start_ts + i * step_s.
        delay_s: wall-clock pause between yields, for tests that need messages to trickle in
        (e.g. the live compiler's wake-on-ingestion behavior)."""
        self._items = [it if isinstance(it, Envelope) else Envelope(start_ts + i * step_s, it)
                       for i, it in enumerate(items)]
        self._delay_s = delay_s

    async def envelopes(self) -> AsyncIterator[Envelope]:
        for env in self._items:
            if self._delay_s:
                await asyncio.sleep(self._delay_s)
            yield env
