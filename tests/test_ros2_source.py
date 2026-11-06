"""Ros2Source's parent side (child process, pipe, bridge, restart), with fake receivers in
place of rclpy. The real rclpy child is exercised by the E2E stack (mock-daq/e2e)."""

import asyncio
import json
import time

from evil.ingestion import contract
from evil.ingestion.ros2_source import Ros2Source


def fake_receiver(conn, topic, qos_depth, reliability, rejoin_after_s):
    """Sends the contract script in two batches, a rejoin notice, then waits."""
    now = time.time()
    script = contract.SCRIPT
    conn.send(("m", [(now + i * 0.01, t) for i, t in enumerate(script[:2])]))
    conn.send(("rejoin", 1))
    conn.send(("m", [(now + (i + 2) * 0.01, t) for i, t in enumerate(script[2:])]))
    time.sleep(30)


def crashing_receiver(conn, topic, qos_depth, reliability, rejoin_after_s):
    """Sends one message and dies (a crash in rclpy, say)."""
    conn.send(("m", [(time.time(), json.dumps({"n": 1}))]))
    raise SystemExit(3)


async def _take(src, n, timeout=20):
    out = []

    async def drain():
        async for env in src.envelopes():
            out.append(env)
            if len(out) >= n:
                return
    await asyncio.wait_for(drain(), timeout)
    return out


def test_the_child_process_source_meets_the_contract():
    src = Ros2Source(receiver=fake_receiver, start_method="fork")
    started = time.time()
    envs = asyncio.run(_take(src, len(contract.SCRIPT)))
    contract.check_envelopes(envs, contract.SCRIPT, started)
    assert src.stats()["rejoins"] == 1


def test_a_dead_receiver_is_restarted(monkeypatch):
    import evil.ingestion.ros2_source as mod
    monkeypatch.setattr(mod, "RESTART_AFTER_S", 0.1)
    src = Ros2Source(receiver=crashing_receiver, start_method="fork")
    envs = asyncio.run(_take(src, 3))
    assert [e.payload for e in envs] == [{"n": 1}] * 3            # one message per (re)start
    assert src.stats()["receiver_restarts"] == 2
    assert src.stats()["received"] == 3                            # deaths are not counted as messages
