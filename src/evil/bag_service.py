"""Client for the car's bag service (cornellev/tailscale-ros-telemetry `api/server.py`,
`http://<car tailscale ip>:8080`). RED's record button calls its `/bag/start` and
`/bag/stop`; the live compiler polls `GET /bag/status` so a live run follows the onboard
recording however it was started (RED, CLI, another laptop, RED closed).

`/bag/status` answers 200 `{"id", "name", "status"}` for the `rosbag` container
(`status == "running"` while recording) and 404 when there is no such container (never
started, or stopped and removed). Anything else (timeout, refused, 5xx docker error) is
"unreachable": the caller decides what that means (the live compiler falls back to
gap-based sessions after a few in a row).
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class BagState:
    reachable: bool
    running: bool = False
    bag_id: str | None = None        # the rosbag container id (new per recording)
    detail: str | None = None


def parse_status(code: int, body: bytes) -> BagState:
    if code == 404:
        return BagState(True, False, None, "no rosbag container")
    if code != 200:
        return BagState(False, detail=f"HTTP {code}")
    try:
        data = json.loads(body)
    except ValueError:
        return BagState(False, detail="status is not JSON")
    running = data.get("status") == "running"
    return BagState(True, running, data.get("id") if running else None, data.get("status"))


class BagServiceClient:
    def __init__(self, base_url: str, timeout_s: float = 2.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s

    def _get(self) -> BagState:
        try:
            with urllib.request.urlopen(f"{self.base_url}/bag/status", timeout=self.timeout_s) as r:
                return parse_status(r.status, r.read())
        except urllib.error.HTTPError as exc:
            return parse_status(exc.code, exc.read() or b"")
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            return BagState(False, detail=str(getattr(exc, "reason", exc)))

    async def poll(self) -> BagState:
        return await asyncio.to_thread(self._get)
