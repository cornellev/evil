"""Every IngestionSource meets the same contract (ingestion/contract.py)."""

import time

from evil.ingestion import contract
from evil.ingestion.base import decode_json
from evil.ingestion.replay_source import ReplaySource


def test_replay_source_meets_the_contract():
    now = time.time()
    contract.check_source(lambda texts: ReplaySource([decode_json(t, now + i * 0.02) for i, t in enumerate(texts)]))


def test_a_source_that_drops_bad_messages_fails_the_contract():
    now = time.time()
    try:
        contract.check_source(lambda texts: ReplaySource(
            [decode_json(t, now + i) for i, t in enumerate(texts) if decode_json(t, 0).error is None]))
    except Exception as exc:
        assert "envelopes for" in str(exc) or isinstance(exc, TimeoutError)
    else:
        raise AssertionError("the contract should reject a source that drops messages")


def test_the_thread_bridge_path_meets_the_contract():
    """Ros2Source and a future ZenohSource hand messages over from a transport thread through
    ThreadBridge; this exercises that path without ROS."""
    import asyncio
    import threading

    from evil.ingestion.base import ThreadBridge

    class ThreadedSource:
        def __init__(self, texts):
            self.texts = texts

        async def envelopes(self):
            bridge = ThreadBridge(asyncio.get_running_loop())
            threading.Thread(target=lambda: [bridge.push(decode_json(t, time.time())) for t in self.texts],
                             daemon=True).start()
            while True:
                yield await bridge.queue.get()

    contract.check_source(ThreadedSource)
