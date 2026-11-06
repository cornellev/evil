"""IngestionSource for the ROS2 topic tailscale-ros-telemetry publishes today
(`/spi_data`, a std_msgs/String carrying a JSON snapshot). Requires rclpy, which isn't
installed outside a ROS2 environment, so the import is deferred until construction rather
than module load -- this file can be imported (and this package's other tests can run) on a
machine with no ROS2 install.

Reception runs in a CHILD PROCESS (`ros_receiver`): rclpy's executor costs ~1.1 ms of CPU per
message in Python (measured 11-05) and, in the same process, fought the event loop for the
GIL, capping the live compiler at ~250 msg/s (5x the car). The child spins rclpy, stamps each
message with its receive time and sends batches over a pipe every FLUSH_S; the parent decodes
them into the bounded ThreadBridge (base.py), so a slow consumer still shows up as counted
overflow, never silent loss. The child also rejoins DDS discovery after REJOIN_AFTER_S of
silence; if it dies, the parent restarts it. Discovery (Fast DDS discovery server or
multicast) is configured outside this code, by the container entrypoint
(docker/ros2_entrypoint.sh), and inherited by the child through the environment.

Exercised for real by the E2E stack (mock-daq/e2e); the parent side is unit-tested with a fake
receiver (tests/test_ros2_source.py), no ROS needed.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import threading
import time
from typing import AsyncIterator, Callable

from evil.ingestion.base import ThreadBridge, decode_json
from evil.ingestion.pipeline import Envelope

DEFAULT_QOS_DEPTH = 100          # 2 s at 50 Hz of transport-side buffering
DEFAULT_QUEUE_MAX = 5000         # 100 s at 50 Hz between the receiver and the event loop
# Silence after which the DDS participant is recreated. Found 11-05 in the E2E stack: after the
# Fast DDS discovery server restarts (the car rebooting), an already-running subscriber keeps
# old matches but is never matched to NEW publishers until it rejoins. Fast DDS 2.6 (Humble)
# server-side fix: run the discovery server with --backup; asked of the DAQ owner.
REJOIN_AFTER_S = 20.0
FLUSH_S = 0.01                   # child -> parent batch interval
RESTART_AFTER_S = 1.0            # delay before restarting a dead receiver

log = logging.getLogger("evil.ros2")

_DIED = object()                 # pushed by the pipe reader when the child goes away


def ros_receiver(conn, topic: str, qos_depth: int, reliability: str, rejoin_after_s: float,
                 flush_s: float = FLUSH_S) -> None:
    """Child process: subscribe, batch (receive_ts, text) over `conn`, rejoin on silence.
    Messages: ("m", [(ts, text), ...]) and ("rejoin", count). Exits when the parent is gone."""
    import rclpy
    from rclpy.executors import ExternalShutdownException, SingleThreadedExecutor
    from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
    from std_msgs.msg import String

    lock = threading.Lock()
    batch: list[tuple[float, str]] = []
    qos = QoSProfile(history=HistoryPolicy.KEEP_LAST, depth=qos_depth,
                     reliability=ReliabilityPolicy.RELIABLE if reliability == "reliable"
                     else ReliabilityPolicy.BEST_EFFORT)

    def on_message(msg: String) -> None:
        t = time.time()
        with lock:
            batch.append((t, msg.data))

    def join():
        context = rclpy.Context()
        rclpy.init(args=None, context=context)
        node = rclpy.create_node("evil_ros2_source", context=context)
        node.create_subscription(String, topic, on_message, qos)
        executor = SingleThreadedExecutor(context=context)
        executor.add_node(node)

        def spin() -> None:
            try:
                executor.spin()
            except ExternalShutdownException:
                pass

        thread = threading.Thread(target=spin, name="evil-ros2-executor", daemon=True)
        thread.start()
        return context, node, executor, thread

    def leave(handles) -> None:
        context, node, executor, thread = handles
        executor.shutdown(timeout_sec=1.0)
        node.destroy_node()
        if context.ok():
            rclpy.shutdown(context=context)
        thread.join(timeout=2.0)

    handles = join()
    last_msg = time.monotonic()
    rejoins = 0
    try:
        while True:
            time.sleep(flush_s)
            with lock:
                out = batch[:]
                batch.clear()
            if out:
                conn.send(("m", out))
                last_msg = time.monotonic()
            elif time.monotonic() - last_msg > rejoin_after_s:
                leave(handles)
                handles = join()
                rejoins += 1
                last_msg = time.monotonic()
                conn.send(("rejoin", rejoins))
    except (BrokenPipeError, EOFError, OSError, KeyboardInterrupt):
        pass                                   # parent gone or stopping
    finally:
        try:
            leave(handles)
        except Exception:
            pass


class Ros2Source:
    def __init__(self, topic: str = "spi_data", qos_depth: int = DEFAULT_QOS_DEPTH,
                 reliability: str = "reliable", queue_max: int = DEFAULT_QUEUE_MAX,
                 rejoin_after_s: float = REJOIN_AFTER_S, *, receiver: Callable | None = None,
                 start_method: str = "spawn"):
        """reliability: 'reliable' (matches tailscale-ros-telemetry's publisher today) or
        'best_effort'. A RELIABLE subscriber does not match a BEST_EFFORT publisher, so this
        must follow the car. `receiver` / `start_method`: tests substitute a fake child."""
        if receiver is None:
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
        self._rejoin_after_s = rejoin_after_s
        self._receiver = receiver or ros_receiver
        self._ctx = multiprocessing.get_context(start_method)
        self.rejoins = 0
        self.restarts = 0
        self.bridge: ThreadBridge | None = None

    def stats(self) -> dict:
        return {**(self.bridge.stats() if self.bridge else {}), "rejoins": self.rejoins,
                "receiver_restarts": self.restarts}

    def _start(self, bridge: ThreadBridge):
        loop = asyncio.get_running_loop()
        parent, child = self._ctx.Pipe(duplex=False)
        proc = self._ctx.Process(target=self._receiver, name="evil-ros2-receiver", daemon=True,
                                 args=(child, self._topic, self._qos_depth, self._reliability, self._rejoin_after_s))
        proc.start()
        child.close()

        def read() -> None:
            try:
                while True:
                    kind, payload = parent.recv()
                    if kind == "m":
                        for ts, text in payload:
                            bridge.push(decode_json(text, ts))
                    elif kind == "rejoin":
                        self.rejoins = payload
                        if payload == 1 or payload % 30 == 0:
                            log.info("no messages for %.0f s: rejoined DDS discovery (%d times)",
                                     self._rejoin_after_s, payload)
            except (EOFError, OSError):
                pass
            # Not bridge.push: the death notice must neither count as a message nor be dropped
            # as overflow when the queue is full; wait for room instead.
            try:
                asyncio.run_coroutine_threadsafe(bridge.queue.put(_DIED), loop).result(timeout=60)
            except Exception:
                pass

        reader = threading.Thread(target=read, name="evil-ros2-pipe", daemon=True)
        reader.start()
        return proc, parent, reader

    @staticmethod
    def _stop(proc, parent, reader) -> None:
        if proc.is_alive():
            proc.terminate()
        proc.join(timeout=3.0)
        if proc.is_alive():
            proc.kill()
        parent.close()
        reader.join(timeout=2.0)

    async def envelopes(self) -> AsyncIterator[Envelope]:
        bridge = self.bridge = ThreadBridge(asyncio.get_running_loop(), self._queue_max)
        handles = self._start(bridge)
        try:
            while True:
                item = await bridge.queue.get()
                if item is _DIED:
                    proc = handles[0]
                    log.error("ROS receiver process exited (code %s); restarting in %.0f s",
                              proc.exitcode, RESTART_AFTER_S)
                    await asyncio.to_thread(self._stop, *handles)
                    await asyncio.sleep(RESTART_AFTER_S)
                    self.restarts += 1
                    handles = self._start(bridge)
                    continue
                yield item
        finally:
            await asyncio.to_thread(self._stop, *handles)
