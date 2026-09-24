"""The seam that keeps EVIL's storage independent of the transport it reads
from. Today that's ROS2 (rmw over Fast DDS, per tailscale-ros-telemetry).
Autonomy may move to Zenoh, either as a ROS2 rmw swap (this seam wouldn't
even need a new adapter) or as a native Zenoh pub/sub with a different wire
format (a new IngestionSource implementation, nothing else changes). See
inference-agent/4.md section 6.
"""

from __future__ import annotations

from typing import AsyncIterator, Protocol

from evil.models import RawSample


class IngestionSource(Protocol):
    def samples(self) -> AsyncIterator[RawSample]:
        """Yield normalized samples for as long as the source is live."""
        ...
