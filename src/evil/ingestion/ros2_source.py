"""IngestionSource for the ROS2 topic tailscale-ros-telemetry publishes today
(`/spi_data`, a std_msgs/String carrying a JSON snapshot). Requires rclpy,
which isn't installed outside a ROS2 environment, so the import is deferred
until construction rather than module load -- this file can be imported (and
this package's other tests can run) on a machine with no ROS2 install.

The rclpy executor spins in its own thread; each message is decoded there and handed to
the asyncio loop through a bounded ThreadBridge (base.py), so nothing blocks the event
loop and a slow consumer shows up as counted overflow, not silent loss. Discovery (Fast DDS
discovery server or multicast) is configured outside this code, by the container
entrypoint (docker/ros2_entrypoint.sh).

Exercised in the ROS2 container by mock-daq's cross-container smoke test and the E2E
stack; the bridge and everything after it are unit-tested without rclpy.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import AsyncIterator

from evil.ingestion.base import ThreadBridge, decode_json
from evil.ingestion.pipeline import Envelope

DEFAULT_QOS_DEPTH = 100          # 2 s at 50 Hz of transport-side buffering
DEFAULT_QUEUE_MAX = 5000         # 100 s at 50 Hz between the executor thread and the loop


class Ros2Source:
    def __init__(self, topic: str = "spi_data", qos_depth: int = DEFAULT_QOS_DEPTH,
                 reliability: str = "reliable", queue_max: int = DEFAULT_QUEUE_MAX):
        """reliability: 'reliable' (matches tailscale-ros-telemetry's publisher today) or
        'best_effort'. A RELIABLE subscriber does not match a BEST_EFFORT publisher, so this
        must follow the car."""
        try:
            import rclpy  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "Ros2Source requires rclpy. Run inside a ROS2 environment "
                "(this is the same /spi_data topic tailscale-ros-telemetry publishes)."
            ) from exc
        if reliability not in ("reliable", "best_effort"):
            raise ValueError(f"reliability must be 'reliable' or 'best_effort', not {reliability!r}")
        self._topic = topic
        self._qos_depth = qos_depth
        self._reliability = reliability
        self._queue_max = queue_max
        self.bridge: ThreadBridge | None = None

    def stats(self) -> dict:
        return self.bridge.stats() if self.bridge else {}

    async def envelopes(self) -> AsyncIterator[Envelope]:
        import rclpy
        from rclpy.executors import ExternalShutdownException, SingleThreadedExecutor
        from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
        from std_msgs.msg import String

        bridge = self.bridge = ThreadBridge(asyncio.get_running_loop(), self._queue_max)
        context = rclpy.Context()
        rclpy.init(args=None, context=context)
        node = rclpy.create_node("evil_ros2_source", context=context)
        qos = QoSProfile(
            history=HistoryPolicy.KEEP_LAST, depth=self._qos_depth,
            reliability=ReliabilityPolicy.RELIABLE if self._reliability == "reliable" else ReliabilityPolicy.BEST_EFFORT)

        def _on_message(msg: String) -> None:
            # Receive time on EVIL's own clock; validation and mapping are the pipeline's job.
            bridge.push(decode_json(msg.data, time.time()))

        node.create_subscription(String, self._topic, _on_message, qos)
        executor = SingleThreadedExecutor(context=context)
        executor.add_node(node)

        def _spin() -> None:
            try:
                executor.spin()
            except ExternalShutdownException:
                pass

        thread = threading.Thread(target=_spin, name="evil-ros2-executor", daemon=True)
        thread.start()
        try:
            while True:
                yield await bridge.queue.get()
        finally:
            executor.shutdown(timeout_sec=1.0)
            node.destroy_node()
            if context.ok():
                rclpy.shutdown(context=context)
            thread.join(timeout=2.0)
