"""Small HTTP upload endpoint for bulk-loading a recorded CSV/rosbag via a
UI. Deliberately separate from mcp_server.py: MCP is EVIL's read interface,
uploading is a write, same reasoning that keeps register_nas_file and
ingest_recording as non-MCP paths -- a bad write is a categorically worse
failure than a bad read, so writes get their own narrow, purpose-built
surface instead of living behind the same generic interface as reads.

Runs as its own process/container from the same image (see
docker-compose.yml's `upload` service), reusing ingest_recording() rather
than re-implementing parsing -- the whole point of that function already
being format-agnostic (CSV vs .db3 by extension).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from evil.scripts.ingest_recording import ingest_recording

DEFAULT_DB_PATH = os.getenv("EVIL_DB_PATH", "evil.db")
MAX_UPLOAD_BYTES = int(os.getenv("EVIL_MAX_UPLOAD_BYTES", str(500 * 1024 * 1024)))
SUPPORTED_SUFFIXES = (".csv", ".db3")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.post("/upload")
async def upload(run_id: str = Form(...), file: UploadFile = File(...)) -> dict:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported file type {suffix!r}; supported: {', '.join(SUPPORTED_SUFFIXES)}",
        )

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="file too large")

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        # ingest_recording() calls asyncio.run() internally, which raises if
        # called from inside a route handler's already-running event loop --
        # same reason tern-llm's api.py and evil-ui's main.py offload their
        # own sync calls to a thread instead of calling them directly.
        result = await asyncio.to_thread(ingest_recording, tmp_path, run_id, DEFAULT_DB_PATH)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if tmp_path is not None:
            os.remove(tmp_path)

    return result
