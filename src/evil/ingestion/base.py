"""The seam that keeps EVIL's storage independent of the transport it reads
from. Today that's ROS2 (rmw over Fast DDS, per tailscale-ros-telemetry).
Autonomy may move to Zenoh, either as a ROS2 rmw swap (this seam wouldn't
even need a new adapter) or as a native Zenoh pub/sub with a different wire
format (a new IngestionSource implementation, nothing else changes). See
inference-agent/4.md section 6.

A source only receives and decodes: it yields one `Envelope(receive_ts, payload, error)`
per message and never validates, maps or drops anything. Schema detection, validation,
repeat dropping and mapping happen in ingestion/pipeline.py, the same code uploads use,
so a new transport cannot change what gets stored.
"""

from __future__ import annotations

from typing import AsyncIterator, Protocol

from evil.ingestion.pipeline import Envelope


class IngestionSource(Protocol):
    def envelopes(self) -> AsyncIterator[Envelope]:
        """Yield one envelope per received message for as long as the source is live
        (a finite source ends; a live one runs until cancelled). `ts` is the receive
        time in epoch seconds; `payload` is the decoded JSON (bare NaN allowed);
        undecodable messages are yielded with `error` set, not skipped."""
        ...


def decode_json(text: str, receive_ts: float) -> Envelope:
    """The shared decode step for text transports (ROS2 std_msgs/String, Zenoh bytes)."""
    import json
    try:
        return Envelope(receive_ts, json.loads(text), raw=text)
    except (json.JSONDecodeError, TypeError) as exc:
        return Envelope(receive_ts, None, f"not JSON: {getattr(exc, 'msg', exc)}", raw=text)


class ThreadBridge:
    """Hands messages from a transport's own thread (an rclpy executor, a Zenoh callback
    thread) to the asyncio loop without ever blocking either side.

    The transport thread calls `push(envelope)`; it is scheduled onto the loop with
    `call_soon_threadsafe` into a bounded asyncio.Queue. When the queue is full the message
    is dropped and counted (`overflow`): EVIL falling behind is visible, never silent, and
    the transport thread never stalls (which would make DDS drop upstream instead, where
    nobody counts). `received` counts every push.
    """

    def __init__(self, loop, maxsize: int = 5000):
        import asyncio
        self._loop = loop
        self.queue: "asyncio.Queue[Envelope]" = asyncio.Queue(maxsize=maxsize)
        self.received = 0
        self.overflow = 0
        self.max_depth = 0

    def _put(self, env: Envelope) -> None:
        import asyncio
        try:
            self.queue.put_nowait(env)
        except asyncio.QueueFull:
            self.overflow += 1
            return
        self.max_depth = max(self.max_depth, self.queue.qsize())

    def push(self, env: Envelope) -> None:
        """Thread-safe; called from the transport's thread."""
        self.received += 1
        try:
            self._loop.call_soon_threadsafe(self._put, env)
        except RuntimeError:      # loop already closed during shutdown
            pass

    def stats(self) -> dict:
        return {"received": self.received, "overflow": self.overflow, "queue_depth": self.queue.qsize(),
                "max_queue_depth": self.max_depth}
