#!/usr/bin/env bash
# End-to-end smoke test for cev-knowledge: indexes the fake Confluence corpus into a throwaway knowledge.db
# (real indexer code), starts the real server over Streamable HTTP and checks it with a real MCP client.
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

echo "[smoke] indexing fake corpus into $WORKDIR/knowledge.db"
PYTHONPATH="$REPO_ROOT/src" "$VENV_PYTHON" "$REPO_ROOT/tests/e2e/_knowledge_seed.py" "$WORKDIR/knowledge.db" >/dev/null

PORT=$("$VENV_PYTHON" -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
echo "[smoke] starting cev-knowledge on port $PORT"
EVIL_KNOWLEDGE_DB="$WORKDIR/knowledge.db" EVIL_KNOWLEDGE_HOST=127.0.0.1 EVIL_KNOWLEDGE_PORT="$PORT" \
    PYTHONPATH="$REPO_ROOT/src" "$VENV_PYTHON" -m evil.knowledge.server > "$WORKDIR/server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 50); do
    "$VENV_PYTHON" -c "import socket,sys; s=socket.socket(); s.settimeout(0.3); sys.exit(s.connect_ex(('127.0.0.1',$PORT)))" && break
    sleep 0.2
done

if PYTHONPATH="$REPO_ROOT/src" "$VENV_PYTHON" "$REPO_ROOT/tests/e2e/_knowledge_client_check.py" "http://127.0.0.1:$PORT"; then
    echo "[smoke] PASS"
else
    echo "[smoke] FAIL -- server log:"; cat "$WORKDIR/server.log"; exit 1
fi
