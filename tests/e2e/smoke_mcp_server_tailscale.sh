#!/usr/bin/env bash
# Sibling to smoke_mcp_server.sh: proves the 0.0.0.0 bind actually serves an
# external interface, not just loopback, by connecting the client through
# this machine's real Tailscale IP instead of 127.0.0.1. This machine is on
# CEV's actual tailnet (cev-nuc is visible in `tailscale status`), so this is
# a genuine network-reachability proof, not a simulated one -- though it
# still only proves this machine's own tailnet identity works, not that
# cev-nuc specifically can serve MCP; deploying there is a separate,
# deliberate step this script does not take.
#
# Skips cleanly (not a failure) if `tailscale` isn't installed/connected on
# whatever machine runs this.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV_PYTHON="$REPO_ROOT/.venv/bin/python"
WORKDIR="$(mktemp -d)"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

if ! command -v tailscale >/dev/null 2>&1; then
    echo "[smoke] SKIP -- tailscale not installed on this machine"
    exit 0
fi

TAILSCALE_IP="$(tailscale ip -4 2>/dev/null || true)"
if [[ -z "$TAILSCALE_IP" ]]; then
    echo "[smoke] SKIP -- tailscale not connected on this machine"
    exit 0
fi
echo "[smoke] this machine's tailnet IP: $TAILSCALE_IP"

echo "[smoke] seeding throwaway db at $WORKDIR/evil.db"
DB_PATH="$("$VENV_PYTHON" "$REPO_ROOT/tests/e2e/_seed_db.py" "$WORKDIR/evil.db")"

PORT=$("$VENV_PYTHON" -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")

echo "[smoke] starting evil MCP server bound to 0.0.0.0:$PORT"
EVIL_DB_PATH="$DB_PATH" EVIL_MCP_HOST="0.0.0.0" EVIL_MCP_PORT="$PORT" \
    PYTHONPATH="$REPO_ROOT/src" "$VENV_PYTHON" -m evil.mcp_server \
    > "$WORKDIR/server.log" 2>&1 &
SERVER_PID=$!

echo "[smoke] waiting for the server to come up on the tailnet interface"
for _ in $(seq 1 30); do
    if "$VENV_PYTHON" -c "
import socket, sys
s = socket.socket()
s.settimeout(0.5)
try:
    s.connect(('$TAILSCALE_IP', $PORT))
    sys.exit(0)
except OSError:
    sys.exit(1)
"; then
        break
    fi
    sleep 0.2
done

echo "[smoke] running real MCP client against http://$TAILSCALE_IP:$PORT/mcp (not 127.0.0.1)"
if "$VENV_PYTHON" "$REPO_ROOT/tests/e2e/_client_check.py" "http://$TAILSCALE_IP:$PORT/mcp"; then
    echo "[smoke] PASS -- the 0.0.0.0 bind is reachable over this machine's real tailnet interface"
    exit 0
else
    echo "[smoke] FAIL -- server log:"
    cat "$WORKDIR/server.log"
    exit 1
fi
