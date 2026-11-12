"""Live Google Drive search for cev-knowledge (phase 4). Nothing from Drive is ever written anywhere.

- Access: a service account (`tern-drive@cev-tern.iam.gserviceaccount.com`, created by the user, 11-11) that was given
  Viewer on the team's shared drive. It sees only what is shared with it, so no folder list is configured: everything
  it can see is in scope. Scope `drive.readonly`. The JSON key is mounted read-only into the `knowledge` server only
  (`secrets/drive-key.json` on the NUC, root-owned, mode 600); the indexer never has it.
- Search: Drive's own full-text search, live, per question (~0.5-1 s on the ~69k-file drive). Results keep Drive's
  order; copies like "X", "X.pdf", "X.pdf(1)", "Copy of X" collapse to one. Folder names for the breadcrumb come
  from a small in-memory id -> name map (names only).
- Read: Google Docs (as markdown), Slides (text), Sheets (first sheet as CSV, row cap), and PDF / Word / PowerPoint /
  text files up to MAX_READ_BYTES are fetched into memory, turned into text, run through the same credential filter as
  Confluence (filters.scrub) and returned in parts. Everything else (photos, CAD, video, big files) is a link + metadata only.
  Files whose name looks like credentials are link-only and never read, like the Confluence password pages.
- Down or slow (timeouts, a revoked key): search returns Confluence alone and stops trying Drive for a minute.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from evil.knowledge.filters import CREDENTIAL_TITLE, scrub
from evil.knowledge.search import LINK_ONLY_NOTE, STOPWORDS
from evil.knowledge.extract import DOCX, PPTX, _docx_text, _lines_to_blocks, _pdf_text, _pptx_text
from evil.knowledge.textify import Block

logger = logging.getLogger("evil.knowledge.drive")

DEFAULT_KEY_FILE = os.getenv("KNOWLEDGE_DRIVE_KEY_FILE", "/run/secrets/knowledge/drive-key.json")
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
API = "https://www.googleapis.com/drive/v3/files"
SEARCH_TIMEOUT_SEC = float(os.getenv("KNOWLEDGE_DRIVE_TIMEOUT_SEC", "4"))
READ_TIMEOUT_SEC = 20.0
# Whole-download budget. The NUC's internet measured ~150-750 KB/s (11-11): a 4 MB deck took 30 s, and Tern gives a
# whole question 60 s. A file not fetched in time is answered as a link, not waited for.
READ_BUDGET_SEC = float(os.getenv("KNOWLEDGE_DRIVE_READ_BUDGET_SEC", "25"))
# 10 MB (11-11): 97% of the drive's 1,076 PDFs (3 MB: 90%) and the Joulemeter PDR deck (4.3 MB); in memory only
MAX_READ_BYTES = int(os.getenv("KNOWLEDGE_DRIVE_MAX_BYTES", str(10 * 1024 * 1024)))
MAX_SHEET_ROWS = 300
PART_CHARS = 12_000
MAX_TERMS = 4
RETRY_AFTER_SEC = 60.0
_FIELDS = "id, name, mimeType, size, modifiedTime, webViewLink, parents, description, trashed"
_COMMON = {"supportsAllDrives": "true", "includeItemsFromAllDrives": "true", "corpora": "allDrives"}

G = "application/vnd.google-apps."
FOLDER = G + "folder"
_EXPORT = {G + "document": "text/markdown", G + "presentation": "text/plain", G + "spreadsheet": "text/csv"}
_DOWNLOAD = {"application/pdf", DOCX, PPTX, "text/plain", "text/markdown", "text/csv"}
_KIND_LABEL = {G + "document": "Google Doc", G + "presentation": "Google Slides", G + "spreadsheet": "Google Sheet",
               FOLDER: "folder", "application/pdf": "PDF", DOCX: "Word document", PPTX: "PowerPoint"}

_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*")
_COPY_SUFFIX = re.compile(r"(\s*\(\d+\))+$")
_COPY_PREFIX = re.compile(r"^(copy of\s+)+", re.I)
_EXT = re.compile(r"\.(pdf|docx?|pptx?|xlsx?|txt|md)$", re.I)


class DriveUnavailable(RuntimeError):
    pass


class NotReadable(Exception):
    """This one file can't be read now (too big or too slow to fetch); Drive itself is fine."""


def _terms(query: str) -> list[str]:
    out = []
    for w in _TOKEN.findall(query.lower()):
        w = w.strip("._+-")
        if len(w) >= 2 and w not in STOPWORDS and w not in out:
            out.append(w)
    return out[:MAX_TERMS]


def _q_literal(term: str) -> str:
    return "'" + term.replace("\\", "\\\\").replace("'", "\\'") + "'"


def drive_query(terms: list[str]) -> str:
    return " and ".join([f"fullText contains {_q_literal(t)}" for t in terms] + ["trashed = false"])


def _base_name(name: str) -> str:
    n = _COPY_SUFFIX.sub("", _COPY_PREFIX.sub("", name.strip()))
    while True:
        m = _EXT.search(n)
        if not m:
            break
        n = _COPY_SUFFIX.sub("", n[: m.start()])
    return n.lower()


def file_url(meta: dict) -> str:
    """The file's Drive link without Google's query string (usp=drivesdk, ouid=<owner's Google user id>, ...)."""
    link = meta.get("webViewLink")
    return link.split("?")[0].split("#")[0] if link else f"https://drive.google.com/open?id={meta['id']}"


def is_link_only(meta: dict) -> bool:
    if CREDENTIAL_TITLE.search(meta.get("name", "")):
        return True
    mime = meta.get("mimeType", "")
    if mime in _EXPORT:
        return False
    if mime in _DOWNLOAD:
        return int(meta.get("size") or 0) > MAX_READ_BYTES
    return True


class DriveClient:
    """`session` is anything with requests' .get(url, params=, timeout=) (tests pass a fake). Without one, the
    service account key at `key_file` makes a google-auth AuthorizedSession; no key file = Drive off."""

    def __init__(self, key_file: str = DEFAULT_KEY_FILE, session: Any = None):
        self._session = session
        self._key_file = key_file
        self._names: dict[str, tuple[str, str | None]] = {}       # folder id -> (name, parent id)
        self._down_until = 0.0
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._session is not None or os.path.isfile(self._key_file)

    def _sess(self):
        if self._session is None:
            from google.auth.transport.requests import AuthorizedSession
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_file(self._key_file, scopes=SCOPES)
            self._session = AuthorizedSession(creds)
        return self._session

    def _get(self, url: str, params: dict, timeout: float, stream: bool = False):
        with self._lock:
            if time.monotonic() < self._down_until:
                raise DriveUnavailable("Drive failed recently; Confluence only for now")
        try:
            r = self._sess().get(url, params=params, timeout=timeout, **({"stream": True} if stream else {}))
        except Exception as exc:                       # network, auth (revoked key), timeouts
            self._mark_down()
            raise DriveUnavailable(f"{type(exc).__name__}: {exc}") from exc
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            if r.status_code >= 500 or r.status_code in (401, 403, 429):
                self._mark_down()
            raise DriveUnavailable(f"Drive API {r.status_code}")
        return r

    def _mark_down(self) -> None:
        with self._lock:
            self._down_until = time.monotonic() + RETRY_AFTER_SEC

    # --- metadata ---------------------------------------------------------------------------------------------

    def meta(self, file_id: str, timeout: float = SEARCH_TIMEOUT_SEC) -> dict | None:
        r = self._get(f"{API}/{file_id}", {"supportsAllDrives": "true", "fields": _FIELDS}, timeout)
        if r is None:
            return None
        m = r.json()
        return None if m.get("trashed") else m

    def _folder(self, folder_id: str, timeout: float) -> tuple[str, str | None] | None:
        if folder_id in self._names:
            return self._names[folder_id]
        r = self._get(f"{API}/{folder_id}", {"supportsAllDrives": "true", "fields": "name, parents"}, timeout)
        if r is None:
            return None
        d = r.json()
        entry = (d.get("name", ""), (d.get("parents") or [None])[0])
        self._names[folder_id] = entry
        return entry

    def breadcrumb(self, meta: dict, levels: int = 2, timeout: float = SEARCH_TIMEOUT_SEC) -> list[str]:
        """Up to `levels` folder names above the file, outermost first (names only, cached in memory)."""
        path: list[str] = []
        parent = (meta.get("parents") or [None])[0]
        while parent and len(path) < levels:
            try:
                entry = self._folder(parent, timeout)
            except DriveUnavailable:
                break
            if entry is None:
                break
            path.insert(0, entry[0])
            parent = entry[1]
        return path

    def hit(self, meta: dict, via: dict | None = None) -> dict:
        link_only = is_link_only(meta)
        credentials = bool(CREDENTIAL_TITLE.search(meta.get("name", "")))
        label = _KIND_LABEL.get(meta.get("mimeType", ""), meta.get("mimeType", "file"))
        h = {
            "source": "drive",
            "id": meta["id"],
            "title": meta.get("name") or "Google Drive file",
            "url": file_url(meta),
            "updated": meta.get("modifiedTime"),
            "breadcrumb": self.breadcrumb(meta),
            "excerpt": (LINK_ONLY_NOTE if credentials else
                        f"{label}" + (" (link only: not readable as text)" if link_only else
                                      "; read it with get_document")),
            "kind": "credentials_link_only" if credentials else ("drive_link_only" if link_only else "drive_doc"),
        }
        if via:
            h["via"] = via
        return h

    # --- search -----------------------------------------------------------------------------------------------

    def search(self, query: str, limit: int = 3) -> list[dict]:
        terms = _terms(query)
        if not terms:
            return []
        files = self._list(drive_query(terms), limit * 3)
        if not files and len(terms) > 2:            # a long question rarely has every word in one file
            files = self._list(drive_query(sorted(terms, key=len, reverse=True)[:2]), limit * 3)
        picked, seen = [], set()
        for f in files:
            key = _base_name(f.get("name", ""))
            if key in seen:
                continue
            seen.add(key)
            picked.append(f)
            if len(picked) >= limit:
                break
        with ThreadPoolExecutor(max_workers=max(1, len(picked))) as pool:    # folder names: one lookup per level
            return list(pool.map(self.hit, picked))

    def _list(self, q: str, page_size: int) -> list[dict]:
        r = self._get(API, dict(_COMMON, q=q, pageSize=str(page_size), fields=f"files({_FIELDS})"),
                      SEARCH_TIMEOUT_SEC)
        return [f for f in (r.json().get("files", []) if r is not None else []) if not f.get("trashed")]

    # --- read -------------------------------------------------------------------------------------------------

    def read(self, file_id: str, section: str | int | None = None) -> dict | None:
        meta = self.meta(file_id, timeout=READ_TIMEOUT_SEC)
        if meta is None:
            return None
        doc = {"source": "drive", "id": meta["id"], "title": meta.get("name", ""),
               "url": file_url(meta),
               "updated": meta.get("modifiedTime"), "breadcrumb": self.breadcrumb(meta, levels=3),
               "kind": "drive_doc", "sections": [], "text": "", "truncated": False, "drive_links": []}
        if CREDENTIAL_TITLE.search(meta.get("name", "")):
            return dict(doc, kind="credentials_link_only", text=LINK_ONLY_NOTE)
        if is_link_only(meta):
            size = int(meta.get("size") or 0)
            why = (f"too large to read ({size / 1e6:.1f} MB; the limit is {MAX_READ_BYTES / 1e6:.0f} MB)"
                   if meta.get("mimeType") in _DOWNLOAD else f"not a text document ({meta.get('mimeType')})")
            return dict(doc, kind="drive_link_only", text=f"This file is {why}. Give the user the link.")
        try:
            blocks = self._blocks(meta)
        except NotReadable as exc:
            return dict(doc, kind="drive_link_only", text=f"This file is {exc}. Give the user the link.")
        clean, stats = scrub(blocks)
        if stats.total:
            logger.info("drive file %s: removed %d credential item(s)", meta["id"], stats.total)
        text = "\n\n".join(("#" * b.level + " " + b.text) if b.kind == "heading" else b.render()
                             for b in clean if b.render().strip())
        parts = [text[i:i + PART_CHARS] for i in range(0, len(text), PART_CHARS)] or [""]
        n = 1
        if section is not None and str(section).strip().isdigit():
            n = min(max(1, int(str(section).strip())), len(parts))
        return dict(doc, sections=[{"section_id": i + 1, "section": f"part {i + 1} of {len(parts)}"}
                                   for i in range(len(parts))],
                    text=parts[n - 1] + (f"\n\n[part {n} of {len(parts)}: ask for section {n + 1} for more]"
                                         if n < len(parts) else ""),
                    truncated=n < len(parts))

    def _content(self, meta: dict) -> bytes:
        mime = meta["mimeType"]
        if mime in _EXPORT:
            r = self._get(f"{API}/{meta['id']}/export", {"mimeType": _EXPORT[mime]}, READ_TIMEOUT_SEC, stream=True)
        else:
            r = self._get(f"{API}/{meta['id']}", {"alt": "media", "supportsAllDrives": "true"}, READ_TIMEOUT_SEC,
                          stream=True)
        if r is None:
            raise NotReadable("it was deleted or moved while being read")
        if not hasattr(r, "iter_content"):
            return r.content
        deadline = time.monotonic() + READ_BUDGET_SEC
        buf = bytearray()
        try:
            for piece in r.iter_content(64 * 1024):
                buf += piece
                if len(buf) > MAX_READ_BYTES:                  # exports have no size until they are made
                    raise NotReadable(f"too large to read (over {MAX_READ_BYTES / 1e6:.0f} MB)")
                if time.monotonic() > deadline:
                    raise NotReadable(f"too slow to fetch right now (over {READ_BUDGET_SEC:.0f} s)")
        except NotReadable:
            raise
        except Exception as exc:
            raise DriveUnavailable(f"{type(exc).__name__}: {exc}") from exc
        finally:
            close = getattr(r, "close", None)
            if close:
                close()
        return bytes(buf)

    def _blocks(self, meta: dict) -> list[Block]:
        data = self._content(meta)
        mime = meta["mimeType"]
        if mime == G + "spreadsheet" or mime == "text/csv":
            rows = list(csv.reader(io.StringIO(data.decode("utf-8", "replace"))))
            rows = [r for r in rows if any(c.strip() for c in r)][:MAX_SHEET_ROWS]
            return [Block("table", rows=rows, header=True)] if rows else []
        if mime == "application/pdf":
            return _lines_to_blocks(_pdf_text(data))
        if mime == DOCX:
            return _lines_to_blocks(_docx_text(data))
        if mime == PPTX:
            return _lines_to_blocks(_pptx_text(data))
        return _lines_to_blocks(data.decode("utf-8", "replace"))


def merge(confluence_hits: list[dict], linked: list[dict], found: list[dict], max_drive: int) -> list[dict]:
    """Confluence hits first, then Drive: files linked from those pages, then Drive's own search hits; each file
    once, at most `max_drive` of them."""
    out = [h for h in confluence_hits if h["source"] != "drive"]
    seen: set[str] = set()
    drive: list[dict] = []
    for h in linked + found:
        if h["id"] in seen or len(drive) >= max_drive:
            continue
        seen.add(h["id"])
        drive.append(h)
    return out + drive

