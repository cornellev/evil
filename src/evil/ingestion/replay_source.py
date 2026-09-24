"""An IngestionSource backed by an in-memory list of RawSamples. Used by
tests, and by offline replay of a recorded run (e.g. a converted rosbag)
without needing a live ROS2/Zenoh connection."""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Iterable

from evil.models import RawSample


class ReplaySource:
    def __init__(self, samples: Iterable[RawSample], delay_s: float = 0.0):
        """delay_s: pause between yields, for tests that need samples to
        trickle in over real wall-clock time (e.g. exercising the live
        compiler service's wake-on-ingestion behavior) rather than arriving
        all at once. Defaults to 0 for the common case of just draining a
        list as fast as possible."""
        self._samples = list(samples)
        self._delay_s = delay_s

    async def samples(self) -> AsyncIterator[RawSample]:
        for sample in self._samples:
            if self._delay_s:
                await asyncio.sleep(self._delay_s)
            yield sample
