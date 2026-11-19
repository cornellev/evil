"""The team's shared Google Drive -> knowledge.db (the indexer's second job; user 11-18, reversing 11-06's "Drive is
searched live, never copied": "what changed", "who edited", "list everything about X" need an index).

- **Every file gets a row** of `pages` (source 'drive'): name, folder path, type, size, created / modified, last
  editor, link. Photos, video and audio (~42k of the ~70k files) are rows without sections, so SQL can list them but
  search doesn't wade through "IMG_2041.jpg"; other files (CAD, archives, big PDFs) get one section naming the file.
- **Text** of Google Docs / Slides / Sheets (row cap) and PDF / Word / PowerPoint / text files up to
  drive.MAX_READ_BYTES: read with the same code the server used live (drive.py), the same credential scrub and
  chunker as Confluence. Credentials-named files are link-only rows and never read. Reading has a time budget per
  pass (the NUC's link is slow); what doesn't fit stays `pending` (a row with its name, readable live by the server)
  until a later pass. Newest files first.
- **Full listing** (first run, nightly with Confluence's, or after DRIVE_VERSION / the chunker changes): every file
  (~70 requests); files no longer listed (deleted, trashed, unshared) are removed. **Incremental**: files modified
  since the last pass (minus a margin). Folders are listed in full every pass (~4 requests), so renamed folders fix
  their files' paths on the next full listing.

Logs carry ids, names and counts, never file text.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from evil.knowledge import store
from evil.knowledge.chunker import CHUNKER_VERSION, LINK_ONLY_KIND, Chunk, chunk
from evil.knowledge.drive import (FOLDER, DriveClient, DriveUnavailable, NotReadable, file_url, is_link_only,
                                  kind_label)
from evil.knowledge.filters import CREDENTIAL_TITLE, scrub
from evil.knowledge.textify import Block

logger = logging.getLogger("evil.knowledge.drive_index")

DRIVE_VERSION = "2"          # bump to re-read every Drive file on the next pass (2: Docs' inline pictures dropped)
PENDING = "pending"          # chunker_version of a readable file whose text isn't read yet
TEXT_BUDGET_SEC = float(os.getenv("KNOWLEDGE_DRIVE_TEXT_BUDGET_SEC", "1800"))
INCREMENTAL_MARGIN = timedelta(hours=2)
READ_WORKERS = 4
READ_BATCH = 16
MAX_FAILURES = 6           # batches in a row with Drive refusing, then the rest waits for the next pass
BACKOFF_SEC = 30.0
MAX_PATH_DEPTH = 12
_MEDIA = ("image/", "video/", "audio/")


@dataclass
class DriveResult:
    mode: str
    listed: int = 0
    indexed: int = 0
    unchanged: int = 0
    moved: int = 0
    removed: int = 0
    read: int = 0
    unreadable: int = 0
    pending: int = 0
    scrubbed: int = 0
    requests: int = 0
    error: str | None = None


def state() -> str:
    return f"d{DRIVE_VERSION}-{CHUNKER_VERSION}"


def _rfc3339(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _size(n: int | None) -> str:
    if not n:
        return ""
    return f"{n / 1e6:.1f} MB" if n >= 100_000 else f"{max(1, round(n / 1e3))} KB"


def mime_of(meta: dict) -> str | None:
    """A shortcut is stored with the type of what it points to, so a shortcut to a deck lists as Google Slides (seen
    11-18: "the slides in that folder" missed the Telemetry Workshop Template, a shortcut)."""
    return (meta.get("shortcutDetails") or {}).get("targetMimeType") or meta.get("mimeType")


def kind_of(meta: dict) -> tuple[str, bool]:
    """(kind, whether its text is read)."""
    mime, name = meta.get("mimeType", ""), meta.get("name", "")
    if mime == FOLDER:
        return "drive_folder", False
    if CREDENTIAL_TITLE.search(name):
        return LINK_ONLY_KIND, False
    if mime.startswith(_MEDIA):
        return "drive_media", False
    if not is_link_only(meta):
        return "drive_doc", True
    return "drive_link_only", False


def about(name: str, mime: str | None, size: int | None, path: list[str]) -> str:
    where = f", folder {' > '.join(path)}" if path else ""
    big = f" ({_size(size)})" if size else ""
    return f"{kind_label(mime, name)} in the team Drive{where}{big}."


class _Folders:
    def __init__(self, folders: dict[str, tuple[str, str | None]]):
        self.folders = folders

    def path(self, parent: str | None) -> list[str]:
        out: list[str] = []
        while parent in self.folders and len(out) < MAX_PATH_DEPTH:
            name, parent = self.folders[parent]
            out.insert(0, name)
        return out


def _write_row(conn, meta: dict, path: list[str], now: datetime) -> None:
    kind, needs_text = kind_of(meta)
    size = int(meta["size"]) if meta.get("size") else None
    name = meta.get("name") or "Google Drive file"
    if kind in ("drive_media", LINK_ONLY_KIND):
        chunks = [] if kind == "drive_media" else [Chunk(0, "", "", [])]
    else:
        shortcut = " (a shortcut)" if meta.get("shortcutDetails") else ""
        chunks = [Chunk(0, "", about(name, mime_of(meta), size, path)[:-1] + shortcut + ".", [])]
    store.upsert_page(
        conn, page_id=meta["id"], title=name, url=file_url(meta), space="drive", breadcrumb=path,
        updated=meta.get("modifiedTime") or "", version=int(meta.get("version") or 0), labels=[], kind=kind,
        chunker_version=PENDING if needs_text else state(), indexed_at=now.isoformat(), chunks=chunks,
        source="drive", created=meta.get("createdTime"),
        last_editor=(meta.get("lastModifyingUser") or {}).get("displayName"), mime_type=mime_of(meta),
        size=size, folder=(meta.get("parents") or [None])[0],
    )


def _sync_row(conn, meta: dict, folders: _Folders, now: datetime, result: DriveResult) -> None:
    path = folders.path((meta.get("parents") or [None])[0])
    row = conn.execute("SELECT version, chunker_version, breadcrumb, mime_type FROM pages WHERE id = ?",
                       (meta["id"],)).fetchone()
    if (row and row["version"] == int(meta.get("version") or 0) and row["chunker_version"] in (state(), PENDING)
            and row["mime_type"] == mime_of(meta)):
        if json.loads(row["breadcrumb"]) != path:              # a folder above it was renamed or moved
            conn.execute("UPDATE pages SET breadcrumb = ? WHERE id = ?", (json.dumps(path), meta["id"]))
            result.moved += 1
        else:
            result.unchanged += 1
        return
    _write_row(conn, meta, path, now)
    result.indexed += 1


def _read_one(client: DriveClient, row) -> tuple:
    meta = {"id": row["id"], "name": row["title"], "mimeType": row["mime_type"], "size": row["size"]}
    try:
        return row, client.blocks(meta), None
    except Exception as exc:                     # NotReadable, DriveUnavailable, a file the parser can't handle
        return row, None, exc


def read_pending(conn, client: DriveClient, budget_sec: float, now: datetime, result: DriveResult) -> None:
    """Reads the text of files still `pending`, newest first, until the budget runs out or Drive keeps failing
    (rate limits come and go: seen 11-18, two in a row stopped a pass at 1,115 of 9,900 files)."""
    rows = conn.execute("SELECT id, title, mime_type, size, breadcrumb FROM pages WHERE source = 'drive' AND "
                        "chunker_version = ? ORDER BY updated DESC, id", (PENDING,)).fetchall()
    deadline = time.monotonic() + budget_sec
    failures = 0
    done = 0
    queue = list(rows)
    tries: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=READ_WORKERS, thread_name_prefix="drive-read") as pool:
        while queue:
            if time.monotonic() > deadline or failures >= MAX_FAILURES:
                break
            batch, queue = queue[:READ_BATCH], queue[READ_BATCH:]
            down = None
            for row, blocks, exc in pool.map(lambda r: _read_one(client, r), batch):
                if isinstance(exc, DriveUnavailable):    # refused for now: tried again after the back-off
                    down = exc
                    tries[row["id"]] = tries.get(row["id"], 0) + 1
                    if tries[row["id"]] < 2:              # one file that always times out waits for the next pass
                        queue.append(row)
                    continue
                done += 1
                path = json.loads(row["breadcrumb"])
                note = about(row["title"], row["mime_type"], row["size"], path)
                if exc is not None:
                    why = str(exc) if isinstance(exc, NotReadable) else "not readable as text"
                    logger.info("drive file %s %r: %s", row["id"], row["title"], why)
                    store.replace_chunks(conn, row["id"], row["title"], [Chunk(0, "", f"{note} Link only: {why}.", [])],
                                         kind="drive_link_only", chunker_version=state(), indexed_at=now.isoformat())
                    result.unreadable += 1
                    continue
                clean, stats = scrub(blocks)
                if stats.total:
                    result.scrubbed += stats.total
                    logger.info("drive file %s %r: removed %d credential item(s)", row["id"], row["title"], stats.total)
                if chunk(clean):
                    chunks, kind = chunk([Block("para", note), *clean]), "drive_doc"
                else:                                  # an empty doc or a scanned PDF: its name is all there is
                    chunks, kind = [Chunk(0, "", note, [])], "drive_link_only"
                store.replace_chunks(conn, row["id"], row["title"], chunks, kind=kind, chunker_version=state(),
                                     indexed_at=now.isoformat())
                result.read += 1
            conn.commit()
            if down is not None:
                failures += 1
                logger.warning("drive reads: %s (%d/%d)", down, failures, MAX_FAILURES)
                if failures < MAX_FAILURES:
                    time.sleep(min(BACKOFF_SEC * 2 ** (failures - 1), 300))
            else:
                failures = 0
    result.pending = len(rows) - done


def sync_drive(conn, client: DriveClient, full: bool | None = None, now: datetime | None = None,
               text_budget_sec: float = TEXT_BUDGET_SEC) -> DriveResult:
    now = now or datetime.now(timezone.utc)
    last = store.get_meta(conn, "drive_last_success")
    if full is None:
        full = last is None or store.get_meta(conn, "drive_version") != state()
    result = DriveResult(mode="full" if full else "incremental")
    before = client.requests_made
    try:
        listed = list(client.list_files(f"mimeType = '{FOLDER}' and trashed = false"))
        folders = _Folders({f["id"]: (f.get("name", ""), (f.get("parents") or [None])[0]) for f in listed})
        q = f"mimeType != '{FOLDER}' and trashed = false"
        if not full:
            q += f" and modifiedTime > '{_rfc3339(datetime.fromisoformat(last) - INCREMENTAL_MARGIN)}'"
        seen = set()
        for meta in itertools.chain(listed, client.list_files(q)):      # streamed: ~70k files
            seen.add(meta["id"])
            result.listed += 1
            _sync_row(conn, meta, folders, now, result)
            if result.listed % 1000 == 0:
                conn.commit()
        if full:
            gone = {r[0] for r in conn.execute("SELECT id FROM pages WHERE source = 'drive'")} - seen
            for fid in gone:
                store.delete_page(conn, fid)
            result.removed = len(gone)
        store.set_meta(conn, "drive_last_success", now.isoformat())
        store.set_meta(conn, "drive_version", state())
        conn.commit()
    except Exception as exc:                     # a failed listing keeps what's indexed
        conn.rollback()
        result.error = f"{type(exc).__name__}: {exc}"
        logger.error("drive listing failed: %s", result.error)
    try:
        read_pending(conn, client, text_budget_sec, now, result)
    except Exception as exc:
        conn.rollback()
        result.error = (result.error + "; " if result.error else "") + f"reading: {type(exc).__name__}: {exc}"
        logger.error("drive reading failed: %s", result.error)
    result.requests = client.requests_made - before
    return result
