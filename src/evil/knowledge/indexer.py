"""The index job: Confluence `cev` space -> knowledge.db (the `knowledge-indexer` service).

- **Full pass** (first run, nightly, or after a chunker change): fetches every page of the space (~12 requests
  for ~560 pages), rebuilds the set of restricted pages from scratch, removes pages that were deleted, became
  restricted (themselves or through an ancestor: restriction changes don't bump lastModified, so only this pass
  sees them), got the `no-tern` label, or moved out of the space.
- **Incremental pass** (hourly): only pages modified since the last successful pass, minus a 26 h margin
  (CQL reads dates in the token owner's time zone); pages whose version didn't change are skipped, so the
  margin costs a request or two, not a reindex.

- **Attachments** (after the pages, phase 6): files attached to indexed pages, minus images (screenshots shown on
  their page: 93% of the space's ~2,000). PDF / Word / PowerPoint files up to 10 MB are downloaded and indexed as text
  (same scrub and chunker as pages, the first section says which page they are on); everything else, and anything
  bigger, is an entry with name, type, size and link only. An attachment is indexed only while its page is (a
  restricted, `no-tern`, credentials or deleted page takes its attachments with it); a credentials-named file is a
  link-only pointer and is never downloaded. Listing all attachments is ~11 requests, done on full passes and while
  downloads are still pending; hourly passes list only changed ones. Downloads get a time budget per pass (the
  NUC's link is slow); what doesn't fit waits for the next pass.
- **Embeddings** (after each pass): sections with no vector for KNOWLEDGE_EMBED_MODEL yet are embedded through
  Tern's /embed on thor (embeddings.py). thor down -> they stay pending until the next pass; search is keyword-only
  for them meanwhile. The first run embeds everything (~2k sections, about a minute).

Only this process has the Confluence token (KNOWLEDGE_CONFLUENCE_TOKEN, from the NUC's knowledge.env). Logs carry
ids, titles and counts, never page text.

Run: `python -m evil.knowledge.indexer` (loop) or `--once [--full]`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from evil.knowledge import embeddings as emb
from evil.knowledge import store
from evil.knowledge.chunker import CHUNKER_VERSION, LINK_ONLY_KIND, Chunk, chunk, page_kind
from evil.knowledge.confluence import Attachment, ConfluenceClient, ConfluenceError, Page, force_ipv4
from evil.knowledge.extract import LABEL, READABLE, blocks_for
from evil.knowledge.filters import CREDENTIAL_TITLE, mentions_credential, redact_keys, scrub, skip_reason
from evil.knowledge.textify import Block, textify

logger = logging.getLogger("evil.knowledge.indexer")

DEFAULT_DB = os.getenv("EVIL_KNOWLEDGE_DB", "/knowledge/knowledge.db")
DEFAULT_URL = os.getenv("KNOWLEDGE_CONFLUENCE_URL", "https://confluence.cornell.edu")
DEFAULT_SPACE = os.getenv("KNOWLEDGE_SPACE", "cev")
INCREMENTAL_MARGIN = timedelta(hours=26)
LOCAL_TZ = ZoneInfo(os.getenv("KNOWLEDGE_TZ", "America/New_York"))   # for the nightly full hour
ATTACHMENT_MAX_BYTES = int(os.getenv("KNOWLEDGE_ATTACHMENT_MAX_BYTES", str(10 * 1024 * 1024)))
ATTACHMENT_BUDGET_SEC = float(os.getenv("KNOWLEDGE_ATTACHMENT_BUDGET_SEC", "900"))   # downloads per pass
ATTACHMENTS_VERSION = "1"     # bump to re-read every attachment on the next pass


@dataclass
class RunResult:
    mode: str
    fetched: int = 0
    indexed: int = 0
    unchanged: int = 0
    removed: int = 0
    skipped: int = 0
    scrubbed: int = 0
    requests: int = 0
    attachments_indexed: int = 0
    attachments_removed: int = 0
    attachments_pending: int = 0
    error: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _index_page(conn, page: Page, now: datetime) -> tuple[int, int]:
    blocks, scrub_stats = scrub(textify(page.storage))
    # a page with no text of its own (a parent page, attachments or macros only) still gets a title-only entry, so
    # it can be found by name: 115 of the first 559 pages had none
    chunks = chunk(blocks) or [Chunk(0, "", "", [])]
    store.upsert_page(
        conn, page_id=page.id, title=page.title, url=page.url, space=page.space,
        breadcrumb=[t for _, t in page.ancestors], updated=page.updated, version=page.version,
        labels=page.labels, kind=page_kind(page.title, page.labels), chunker_version=CHUNKER_VERSION,
        indexed_at=now.isoformat(), chunks=chunks,
    )
    if scrub_stats.total:
        logger.info("page %s %r: removed %d credential item(s)", page.id, page.title, scrub_stats.total)
    return len(chunks), scrub_stats.total


def _index_link_only(conn, page: Page, now: datetime) -> None:
    store.upsert_page(
        conn, page_id=page.id, title=page.title, url=page.url, space=page.space,
        breadcrumb=[t for _, t in page.ancestors], updated=page.updated, version=page.version,
        labels=page.labels, kind=LINK_ONLY_KIND, chunker_version=CHUNKER_VERSION,
        indexed_at=now.isoformat(), chunks=[Chunk(0, "", "", [])],
    )
    logger.info("page %s %r: credentials page, stored as a link only", page.id, page.title)


def _size(n: int) -> str:
    return f"{n / 1e6:.1f} MB" if n >= 100_000 else f"{max(1, round(n / 1e3))} KB"


def _type_label(att: Attachment) -> str:
    if att.media_type in LABEL:
        return LABEL[att.media_type]
    ext = att.title.rsplit(".", 1)[-1].upper() if "." in att.title else ""
    return f"{ext} file" if ext and len(ext) <= 5 else "file"


# photos Confluence doesn't type as images (iPhone HEIC came through as octet-stream, 11-12)
IMAGE_EXT = re.compile(r"\.(png|jpe?g|gif|heic|heif|webp|bmp|tiff?|svg)$", re.I)


def _is_image(att: Attachment) -> bool:
    return att.media_type.startswith("image/") or bool(IMAGE_EXT.search(att.title))


def _attachment_state(att: Attachment) -> tuple[int, str]:
    return att.version, f"a{ATTACHMENTS_VERSION}-{CHUNKER_VERSION}"


def sync_attachments(conn, client: ConfluenceClient, space: str, full: bool, since: datetime | None,
                     now: datetime, result: RunResult) -> None:
    pending_before = int(store.get_meta(conn, "attachments_pending") or "0")
    listing_full = full or pending_before > 0 or store.get_meta(conn, "attachments_version") != ATTACHMENTS_VERSION
    deadline = time.monotonic() + ATTACHMENT_BUDGET_SEC
    seen: set[str] = set()
    pending = 0
    for att in client.attachments(space, None if listing_full else since):
        if _is_image(att):                  # dropped from the index by the next full listing if stored earlier
            continue
        parent = None
        if att.page_id:
            parent = conn.execute("SELECT title, breadcrumb, kind FROM pages WHERE id = ? AND parent IS NULL",
                                  (att.page_id,)).fetchone()
        if parent is None or parent["kind"] == LINK_ONLY_KIND:     # its page isn't served, so neither is the file
            if store.delete_page(conn, att.id):
                result.attachments_removed += 1
            continue
        seen.add(att.id)
        version, state = _attachment_state(att)
        if store.page_state(conn, att.id) == (version, state):
            continue
        breadcrumb = [*json.loads(parent["breadcrumb"]), parent["title"]]
        comment = "" if mentions_credential(att.comment) else redact_keys(att.comment)
        about = f'{_type_label(att)} attached to the page "{parent["title"]}" ({_size(att.size)}).' + (
            f" {comment}" if comment else "")
        kind, chunks = "attachment_link_only", [Chunk(0, "", about, [])]
        if CREDENTIAL_TITLE.search(att.title):
            kind, chunks = LINK_ONLY_KIND, [Chunk(0, "", "", [])]          # never downloaded
        elif att.media_type in READABLE and att.size <= ATTACHMENT_MAX_BYTES:
            if time.monotonic() > deadline:
                pending += 1
                continue
            try:
                blocks = blocks_for(att.media_type, client.download(att))
            except ConfluenceError as exc:              # try again next pass; a stored older version stays
                logger.warning("attachment %s %r: %s", att.id, att.title, exc)
                pending += 1
                continue
            except Exception as exc:                    # a file the extractor can't parse: link only
                logger.info("attachment %s %r: not readable as text (%s)", att.id, att.title, type(exc).__name__)
                blocks = []
            clean, scrub_stats = scrub(blocks)
            if scrub_stats.total:
                result.scrubbed += scrub_stats.total
                logger.info("attachment %s %r: removed %d credential item(s)", att.id, att.title, scrub_stats.total)
            if chunk(clean):                            # a scanned PDF has no text: stays link only
                kind, chunks = "attachment", chunk([Block("para", about), *clean])
        store.upsert_page(
            conn, page_id=att.id, title=att.title, url=att.url, space=space.lower(), breadcrumb=breadcrumb,
            updated=att.updated, version=version, labels=[], kind=kind, chunker_version=state,
            indexed_at=now.isoformat(), chunks=chunks, parent=att.page_id,
        )
        result.attachments_indexed += 1
    if listing_full:
        for gone in store.attachment_ids(conn) - seen:
            store.delete_page(conn, gone)
            result.attachments_removed += 1
    result.attachments_pending = pending
    store.set_meta(conn, "attachments_pending", str(pending))
    store.set_meta(conn, "attachments_version", ATTACHMENTS_VERSION)


def index_once(conn, client: ConfluenceClient, space: str, full: bool | None = None,
               now: datetime | None = None) -> RunResult:
    """One pass. `full=None` decides by itself: full on the first run or after a chunker change."""
    now = now or _now()
    last_success = store.get_meta(conn, "last_success")
    if full is None:
        full = last_success is None or store.get_meta(conn, "chunker_version") != CHUNKER_VERSION
    result = RunResult(mode="full" if full else "incremental")
    run_id = conn.execute("INSERT INTO index_runs (started, mode) VALUES (?, ?)",
                          (now.isoformat(), result.mode)).lastrowid
    conn.commit()
    requests_before = client.requests_made
    try:
        since = None if full else datetime.fromisoformat(last_success) - INCREMENTAL_MARGIN
        pages = list(client.pages(space, since))
        result.fetched = len(pages)

        restricted = set() if full else store.restricted_ids(conn)
        for p in pages:
            if p.restricted:
                restricted.add(p.id)
            else:
                restricted.discard(p.id)

        seen = set()
        for p in pages:
            seen.add(p.id)
            reason = skip_reason(p, restricted, space)
            if reason == "credentials title":
                # a pointer only: title + link, the body is never read, so "where's the router password" finds the
                # page and Confluence decides who can open it
                result.skipped += 1
                if store.page_state(conn, p.id) != (p.version, CHUNKER_VERSION):
                    _index_link_only(conn, p, now)
                continue
            if reason:
                if store.delete_page(conn, p.id):
                    result.removed += 1
                    logger.info("removed page %s (%s)", p.id, reason)
                result.skipped += 1
                continue
            state = store.page_state(conn, p.id)
            if state == (p.version, CHUNKER_VERSION):
                result.unchanged += 1
                continue
            _, scrubbed = _index_page(conn, p, now)
            result.indexed += 1
            result.scrubbed += scrubbed

        if full:
            for gone in store.page_ids(conn) - seen:      # deleted, moved, or no longer visible to the token
                store.delete_page(conn, gone)
                result.removed += 1
                logger.info("removed page %s (no longer in the space)", gone)
            store.set_meta(conn, "last_full", now.isoformat())
        store.replace_restricted(conn, restricted)
        store.set_meta(conn, "last_success", now.isoformat())
        store.set_meta(conn, "chunker_version", CHUNKER_VERSION)
        conn.commit()
    except Exception as exc:              # a failed pass leaves the last good index in place
        conn.rollback()
        result.error = f"{type(exc).__name__}: {exc}"
        logger.error("index pass failed: %s", result.error)
    else:
        try:                              # pages are committed; attachments can fail on their own
            sync_attachments(conn, client, space, full, since, now, result)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            result.error = f"attachments: {type(exc).__name__}: {exc}"
            logger.error("attachment pass failed: %s", result.error)
    result.requests = client.requests_made - requests_before
    conn.execute(
        "UPDATE index_runs SET finished = ?, fetched = ?, indexed = ?, unchanged = ?, removed = ?, skipped = ?,"
        " scrubbed = ?, requests = ?, error = ? WHERE id = ?",
        (_now().isoformat(), result.fetched, result.indexed, result.unchanged, result.removed, result.skipped,
         result.scrubbed, result.requests, result.error, run_id),
    )
    conn.commit()
    return result


def _full_due(conn, now: datetime, full_hour: int) -> bool:
    last_full = store.get_meta(conn, "last_full")
    if last_full is None:
        return True
    age = now - datetime.fromisoformat(last_full)
    # the hourly loop drifts by each pass's duration, so it can step over the full hour: 26 h is the backstop
    return (now.astimezone(LOCAL_TZ).hour == full_hour and age > timedelta(hours=20)) or age > timedelta(hours=26)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--once", action="store_true", help="one pass, then exit")
    parser.add_argument("--full", action="store_true", help="with --once: force a full pass")
    parser.add_argument("--interval-sec", type=int, default=int(os.getenv("KNOWLEDGE_INTERVAL_SEC", "3600")))
    parser.add_argument("--full-hour", type=int, default=int(os.getenv("KNOWLEDGE_FULL_HOUR", "3")),
                        help="local hour for the nightly full pass")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    token = os.getenv("KNOWLEDGE_CONFLUENCE_TOKEN", "").strip()
    if os.getenv("KNOWLEDGE_FORCE_IPV4", "1") != "0":
        force_ipv4()
    conn = store.connect(args.db)
    embedder = emb.EmbedClient()
    if not embedder.enabled:
        logger.info("KNOWLEDGE_EMBED_URL not set: no embeddings, search stays keyword-only")

    while True:
        if not token:
            logger.error("KNOWLEDGE_CONFLUENCE_TOKEN is not set (knowledge.env): nothing to index")
            if args.once:
                return 2
            time.sleep(args.interval_sec)
            continue
        client = ConfluenceClient(DEFAULT_URL, token)
        now = _now()
        full = True if args.full else (None if args.once else (_full_due(conn, now, args.full_hour) or None))
        r = index_once(conn, client, DEFAULT_SPACE, full=full, now=now)
        logger.info("%s pass: fetched %d, indexed %d, unchanged %d, removed %d, skipped %d, scrubbed %d; "
                    "attachments indexed %d, removed %d, pending %d; %d request(s)%s", r.mode, r.fetched,
                    r.indexed, r.unchanged, r.removed, r.skipped, r.scrubbed, r.attachments_indexed,
                    r.attachments_removed, r.attachments_pending, r.requests, f", error: {r.error}" if r.error else "")
        if embedder.enabled:
            done, left = emb.embed_pending(conn, embedder)
            logger.info("embeddings (%s): %d section(s) embedded, %d pending", embedder.model, done, left)
        if args.once:
            return 1 if r.error else 0
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    sys.exit(main())
