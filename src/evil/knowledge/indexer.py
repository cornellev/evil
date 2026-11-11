"""The index job: Confluence `cev` space -> knowledge.db (the `knowledge-indexer` service).

- **Full pass** (first run, nightly, or after a chunker change): fetches every page of the space (~12 requests
  for ~560 pages), rebuilds the set of restricted pages from scratch, removes pages that were deleted, became
  restricted (themselves or through an ancestor: restriction changes don't bump lastModified, so only this pass
  sees them), got the `no-tern` label, or moved out of the space.
- **Incremental pass** (hourly): only pages modified since the last successful pass, minus a 26 h margin
  (CQL reads dates in the token owner's time zone); pages whose version didn't change are skipped, so the
  margin costs a request or two, not a reindex.

Only this process has the Confluence token (KNOWLEDGE_CONFLUENCE_TOKEN, from the NUC's knowledge.env). Logs carry
ids, titles and counts, never page text.

Run: `python -m evil.knowledge.indexer` (loop) or `--once [--full]`.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from evil.knowledge import store
from evil.knowledge.chunker import CHUNKER_VERSION, chunk, page_kind
from evil.knowledge.confluence import ConfluenceClient, Page, force_ipv4
from evil.knowledge.filters import scrub, skip_reason
from evil.knowledge.textify import textify

logger = logging.getLogger("evil.knowledge.indexer")

DEFAULT_DB = os.getenv("EVIL_KNOWLEDGE_DB", "/knowledge/knowledge.db")
DEFAULT_URL = os.getenv("KNOWLEDGE_CONFLUENCE_URL", "https://confluence.cornell.edu")
DEFAULT_SPACE = os.getenv("KNOWLEDGE_SPACE", "cev")
INCREMENTAL_MARGIN = timedelta(hours=26)
LOCAL_TZ = ZoneInfo(os.getenv("KNOWLEDGE_TZ", "America/New_York"))   # for the nightly full hour


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
    error: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _index_page(conn, page: Page, now: datetime) -> tuple[int, int]:
    blocks, scrub_stats = scrub(textify(page.storage))
    chunks = chunk(blocks)
    store.upsert_page(
        conn, page_id=page.id, title=page.title, url=page.url, space=page.space,
        breadcrumb=[t for _, t in page.ancestors], updated=page.updated, version=page.version,
        labels=page.labels, kind=page_kind(page.title, page.labels), chunker_version=CHUNKER_VERSION,
        indexed_at=now.isoformat(), chunks=chunks,
    )
    if scrub_stats.total:
        logger.info("page %s %r: removed %d credential item(s)", page.id, page.title, scrub_stats.total)
    return len(chunks), scrub_stats.total


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
        logger.info("%s pass: fetched %d, indexed %d, unchanged %d, removed %d, skipped %d, scrubbed %d, "
                    "%d request(s)%s", r.mode, r.fetched, r.indexed, r.unchanged, r.removed, r.skipped,
                    r.scrubbed, r.requests, f", error: {r.error}" if r.error else "")
        if args.once:
            return 1 if r.error else 0
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    sys.exit(main())
