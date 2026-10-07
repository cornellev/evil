"""Small HTTP upload endpoint for bulk-loading a recorded CSV/rosbag via a
UI. Deliberately separate from mcp_server.py: MCP is EVIL's read interface,
uploading is a write, same reasoning that keeps register_nas_file and
ingest_recording as non-MCP paths -- a bad write is a categorically worse
failure than a bad read, so writes get their own narrow, purpose-built
surface instead of living behind the same generic interface as reads.

Two upload paths live here. `POST /recordings` (catalog.py) stores any file
set raw and cataloged, synchronously: it returns only once every file has
fully arrived and been written. Parsing is not part of it (Phase 2 of
recording-catalog-design.md). The legacy `POST /upload` below still parses
a single CSV/.db3 straight into evil.db until Phase 2 replaces it.

Runs as its own process/container from the same image (see
docker-compose.yml's `upload` service), reusing ingest_recording() rather
than re-implementing parsing -- the whole point of that function already
being format-agnostic (CSV vs .db3 by extension).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from evil import catalog
from evil.scripts.ingest_recording import ingest_recording

log = logging.getLogger("evil.upload")

DEFAULT_DB_PATH = os.getenv("EVIL_DB_PATH", "evil.db")
MAX_UPLOAD_BYTES = int(os.getenv("EVIL_MAX_UPLOAD_BYTES", str(500 * 1024 * 1024)))
SUPPORTED_SUFFIXES = (".csv", ".db3")

MAX_RECORDING_BYTES = int(os.getenv("EVIL_MAX_RECORDING_BYTES", str(10 * 1024**3)))
MIN_FREE_BYTES = int(os.getenv("EVIL_MIN_FREE_BYTES", str(1024**3)))
MAINTENANCE_INTERVAL_SEC = 3600


def _root() -> catalog.DataRoot:
    root = catalog.data_root_from_env()
    root.ensure()
    # Starlette spools multipart bodies to the temp dir: keep that on the data volume.
    tempfile.tempdir = str(root.spool)
    return root


def _run_maintenance(root: catalog.DataRoot) -> None:
    conn = catalog.connect_catalog(root)
    try:
        catalog.ensure_parsed_db(root)
        swept = catalog.sweep_incoming(root)
        adopted = catalog.reconcile(conn, root)
        catalog.backup_catalog(conn, root)
        if swept or adopted:
            log.info("maintenance: swept %d stale staging entries, adopted %d recordings", swept, len(adopted))
    finally:
        conn.close()


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    root = _root()

    async def loop() -> None:
        while True:
            try:
                await asyncio.to_thread(_run_maintenance, root)
            except Exception:  # never let maintenance kill the server
                log.exception("catalog maintenance failed")
            await asyncio.sleep(MAINTENANCE_INTERVAL_SEC)

    task = asyncio.create_task(loop())
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.post("/upload")
async def upload(run_id: str = Form(...), file: UploadFile = File(...)) -> dict:
    """Saves the upload to a temp file and offloads to a thread: ingest_recording()
    calls asyncio.run() internally, which raises if called from inside a route
    handler's already-running event loop -- same reason tern-llm's api.py and
    evil-ui's main.py offload their own sync calls to a thread instead of
    calling them directly."""
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

        result = await asyncio.to_thread(ingest_recording, tmp_path, run_id, DEFAULT_DB_PATH)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if tmp_path is not None:
            os.remove(tmp_path)

    return result


# ---- recording catalog ---------------------------------------------------

def _store_recording(root: catalog.DataRoot, parts: list, fields: catalog.UploadFields) -> tuple[catalog.CommitResult, int, int]:
    """Blocking: copy every part into staging (hashing as it goes), then commit.
    Staging is discarded on any failure, so nothing half-stored survives."""
    staging = catalog.Staging(root, MAX_RECORDING_BYTES)
    conn = catalog.connect_catalog(root)
    try:
        for part in parts:
            staging.add(part.filename or "unnamed", part.file)
        n_files = len(staging.files)
        total = sum(f.size_bytes for f in staging.files)
        result = catalog.commit(conn, root, staging, fields)
        return result, n_files, total
    except BaseException:
        staging.discard()
        raise
    finally:
        conn.close()


@app.post("/recordings")
async def create_recording(request: Request):
    """Multipart: `files` (one or many; filenames may carry a relative path for
    folder uploads) plus optional source/category/car/event/label/notes/uploader.
    Returns only after everything is stored and cataloged: 201 for a new
    recording, 200 if identical content was already stored."""
    root = _root()
    declared = int(request.headers.get("content-length") or 0)
    if declared > MAX_RECORDING_BYTES:
        raise HTTPException(status_code=413, detail="upload too large")
    if shutil.disk_usage(root.path).free < declared + MIN_FREE_BYTES:
        raise HTTPException(status_code=507, detail="not enough free disk space for this upload")

    try:
        form = await request.form(max_files=1000, max_fields=50)
    except ClientDisconnect:
        # Upload interrupted before it fully arrived: nothing was staged or cataloged.
        log.info("upload interrupted by client; nothing stored")
        return Response(status_code=499)
    try:
        parts = [p for p in form.getlist("files") if hasattr(p, "file")]
        if not parts:
            raise HTTPException(status_code=400, detail="no files in upload (send them as multipart field 'files')")
        try:
            fields = catalog.fields_from_form(**{
                k: form.get(k) for k in ("source", "category", "car", "event", "label", "notes", "uploader")
            })
            result, n_files, total = await asyncio.to_thread(_store_recording, root, parts, fields)
        except catalog.UploadTooLarge as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        except catalog.CatalogError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        await form.close()

    body = {"recording_id": result.recording_id, "deduplicated": result.deduplicated, "files": n_files,
            "total_bytes": total, "parse_status": "pending"}
    if result.deduplicated:
        existing = await asyncio.to_thread(_get, root, result.recording_id)
        body["parse_status"] = existing["parse_status"] if existing else "pending"
    return JSONResponse(body, status_code=200 if result.deduplicated else 201)


def _get(root: catalog.DataRoot, recording_id: str):
    conn = catalog.connect_catalog(root)
    try:
        return catalog.get_recording(conn, recording_id)
    finally:
        conn.close()


def _parse_when(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"bad date {value!r}; use YYYY-MM-DD or epoch seconds") from exc
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


@app.get("/recordings")
async def list_recordings(since: str | None = None, until: str | None = None, category: str | None = None,
                          car: str | None = None, parse_status: str | None = None, source: str | None = None,
                          limit: int = 100, offset: int = 0) -> list[dict]:
    root = _root()

    def run():
        conn = catalog.connect_catalog(root)
        try:
            return catalog.list_recordings(conn, since=_parse_when(since), until=_parse_when(until),
                                           category=category, car=car, parse_status=parse_status,
                                           source=source, limit=limit, offset=offset)
        finally:
            conn.close()

    return await asyncio.to_thread(run)


@app.get("/recordings/{recording_id}")
async def get_recording(recording_id: str) -> dict:
    rec = await asyncio.to_thread(_get, _root(), recording_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="recording not found")
    return rec


@app.patch("/recordings/{recording_id}")
async def patch_recording(recording_id: str, changes: dict) -> dict:
    root = _root()

    def run():
        conn = catalog.connect_catalog(root)
        try:
            return catalog.update_recording(conn, root, recording_id, changes)
        finally:
            conn.close()

    try:
        return await asyncio.to_thread(run)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="recording not found") from exc
    except catalog.CatalogError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---- reparse, status, locations -----------------------------------------

async def _with_catalog(fn):
    root = _root()

    def run():
        conn = catalog.connect_catalog(root)
        try:
            return fn(conn, root)
        finally:
            conn.close()

    return await asyncio.to_thread(run)


@app.post("/recordings/{recording_id}/reparse", status_code=202)
async def reparse_recording(recording_id: str, rescan: bool = False) -> dict:
    """Queue a fresh parse (optionally a fresh scan first) of a stored recording.
    Idempotent: a parse already queued or running is reused."""
    job_id = await _with_catalog(lambda c, r: catalog.request_reparse(c, recording_id, rescan=rescan))
    if job_id is None:
        raise HTTPException(status_code=404, detail="recording not found")
    return {"recording_id": recording_id, "job_id": job_id, "status": "queued"}


@app.get("/system/status")
async def system_status() -> dict:
    """The upload page's status panel: job queue plus CPU, memory and disk."""
    return await _with_catalog(lambda c, r: catalog.system_status(c, r))


@app.get("/locations")
async def get_locations() -> list[dict]:
    return await _with_catalog(lambda c, r: catalog.list_locations(c))


@app.post("/locations", status_code=201)
async def post_location(body: dict) -> dict:
    """Create or update a named location (circle) and relabel recordings against it."""
    try:
        name, lat, lon, radius = body["name"], float(body["lat"]), float(body["lon"]), float(body["radius_m"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="send name, lat, lon, radius_m") from exc
    try:
        location_id = await _with_catalog(
            lambda c, r: catalog.add_location(c, name, lat, lon, radius, body.get("default_category")))
    except catalog.CatalogError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"location_id": location_id, "name": name}


# ---- CAT cache -------------------------------------------------------------

@app.get("/cat/recordings")
async def cat_recordings(kind: str = "all", limit: int = 500, offset: int = 0) -> list[dict]:
    """What CAT lists: rosbag2 and CSV recordings with a display name and their cache state."""
    if kind not in ("all", "bag", "csv"):
        raise HTTPException(status_code=400, detail="kind must be all, bag or csv")
    return await _with_catalog(lambda c, r: catalog.cat_recordings(c, kind=kind, limit=limit, offset=offset))


@app.post("/recordings/{recording_id}/cache", status_code=202)
async def request_cache(recording_id: str) -> dict:
    """Make sure CAT's cache for this recording exists: queues a build if needed (CAT shows
    'preparing...' until it is ready). A ready cache is just marked as used."""
    try:
        return await _with_catalog(lambda c, r: catalog.request_cache(c, recording_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="recording not found") from exc
    except catalog.CatalogError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/recordings/{recording_id}/cache")
async def get_cache(recording_id: str) -> dict:
    state = await _with_catalog(lambda c, r: catalog.cache_state(c, recording_id))
    if state is None:
        raise HTTPException(status_code=404, detail="recording not found")
    return state


@app.post("/recordings/{recording_id}/cache/touch")
async def touch_cache(recording_id: str) -> dict:
    """CAT calls this when a user opens a recording, so a cache in use is not evicted."""
    touched = await _with_catalog(lambda c, r: catalog.touch_cache(c, recording_id))
    return {"recording_id": recording_id, "touched": touched}


@app.get("/cache")
async def cache_overview() -> dict:
    def run(c, r):
        entries = [dict(row) for row in c.execute(
            "SELECT recording_id, built_at, last_access_at, size_bytes, messages FROM cache_entries ORDER BY last_access_at DESC")]
        return {"summary": catalog.cache_summary(c), "entries": entries}
    return await _with_catalog(run)
