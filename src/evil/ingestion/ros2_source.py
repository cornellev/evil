"""IngestionSource for the ROS2 topic tailscale-ros-telemetry publishes today
(`/spi_data`, a std_msgs/String carrying a JSON snapshot). Requires rclpy,
which isn't installed outside a ROS2 environment, so the import is deferred
until construction rather than module load -- this file can be imported (and
this package's other tests can run) on a machine with no ROS2 install.

Not covered by this package's test suite: it needs a live rclpy runtime to
exercise, which the classifier/ingest tests deliberately avoid depending on.
Exercise it against a real ROS2 environment before relying on it.
"""

from __future__ import annotations

import time
from typing import AsyncIterator

from evil.ingestion.base import decode_json
from evil.ingestion.pipeline import Envelope


class Ros2Source:
    def __init__(self, topic: str = "spi_data"):
        try:
            import rclpy  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "Ros2Source requires rclpy. Run inside a ROS2 environment "
                "(this is the same /spi_data topic tailscale-ros-telemetry publishes)."
            ) from exc

        self._topic = topic

    async def envelopes(self) -> AsyncIterator[Envelope]:
        import rclpy
        from rclpy.node import Node
        from rclpy.qos import QoSProfile
        from std_msgs.msg import String

        rclpy.init(args=None)
        node = Node("evil_ros2_source")
        queue: list[Envelope] = []

        def _on_message(msg: String) -> None:
            # Receive time on EVIL's own clock; validation and mapping are the pipeline's job.
            queue.append(decode_json(msg.data, time.time()))

        node.create_subscription(String, self._topic, _on_message, QoSProfile(depth=1))
        try:
            while rclpy.ok():
                rclpy.spin_once(node, timeout_sec=0.1)
                while queue:
                    yield queue.pop(0)
        finally:
            node.destroy_node()
            rclpy.shutdown()
