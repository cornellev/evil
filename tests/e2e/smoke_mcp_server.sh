#!/usr/bin/env bash
# End-to-end smoke test: starts a real evil MCP server over Streamable HTTP
# and hits it with a real MCP client, proving the actual protocol wiring
# works -- not just the Python functions behind it (tests/test_mcp_server.py
# already covers those via the in-process call_tool() API, which bypasses
# the wire protocol entirely).
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

echo "[smoke] seeding throwaway db at $WORKDIR/evil.db"
DB_PATH="$("$VENV_PYTHON" "$REPO_ROOT/tests/e2e/_seed_db.py" "$WORKDIR/evil.db")"

PORT=$("$VENV_PYTHON" -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")

echo "[smoke] starting evil MCP server on port $PORT"
EVIL_DB_PATH="$DB_PATH" EVIL_MCP_HOST="127.0.0.1" EVIL_MCP_PORT="$PORT" \
    PYTHONPATH="$REPO_ROOT/src" "$VENV_PYTHON" -m evil.mcp_server \
    > "$WORKDIR/server.log" 2>&1 &
SERVER_PID=$!

echo "[smoke] waiting for server to come up"
for _ in $(seq 1 30); do
    if "$VENV_PYTHON" -c "
import socket, sys
s = socket.socket()
s.settimeout(0.5)
try:
    s.connect(('127.0.0.1', $PORT))
    sys.exit(0)
except OSError:
    sys.exit(1)
"; then
        break
    fi
    sleep 0.2
done

echo "[smoke] running real MCP client against http://127.0.0.1:$PORT/mcp"
if "$VENV_PYTHON" "$REPO_ROOT/tests/e2e/_client_check.py" "http://127.0.0.1:$PORT/mcp"; then
    echo "[smoke] PASS"
    exit 0
else
    echo "[smoke] FAIL -- server log:"
    cat "$WORKDIR/server.log"
    exit 1
fi
