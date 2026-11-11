"""Search over knowledge.db (phase 1: keywords only; embeddings + rank fusion come in phase 3).

- The question becomes an OR of its content words (FTS5 is AND by default, which natural-language questions
  almost never satisfy); porter stemming matches "measuring" to "measure".
- BM25 with title hits weighted highest, then headings, then text.
- Work logs and meeting notes are pushed down (they match everything with one stray line).
- One section per page by default: a page's best section represents it, so one long page can't fill the list.
- Basic Drive fallback: Drive links found in the returned sections are listed after them as link-only hits (title =
  the link text). No Google access; nothing about the file is known beyond its link.
"""

from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

TITLE_WEIGHT, HEADING_WEIGHT, TEXT_WEIGHT = 8.0, 3.0, 1.0
KIND_FACTOR = {"page": 1.0, "work_log": 0.5, "meeting_notes": 0.6, "credentials_link_only": 1.0}
LINK_ONLY_NOTE = ("This page holds team credentials, so its contents are not available here. Give the user the "
                  "link; Confluence decides who can open it.")
CANDIDATES = 200
MAX_LIMIT = 10
MAX_DRIVE_LINKS = 3
DOCUMENT_MAX_CHARS = 12_000

STOPWORDS = set("""
a an the and or but if then else of to in on at by for with from into onto about over under as is are was were be been
being do does did done have has had having it its this that these those there here what which who whom whose where when
why how can could should would will shall may might must i me my we our us you your he she they them their not no yes
any all some each every more most less few many much very just also only so than too s t find show tell give get got
need want know please page pages doc docs document documents info information thing things about our team
""".split())

_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*")
_DRIVE_ID = re.compile(r"/(?:d|folders)/([A-Za-z0-9_-]{10,})|[?&]id=([A-Za-z0-9_-]{10,})")


def fts_query(query: str, glossary: dict[str, list[str]] | None = None) -> str | None:
    words = []
    for w in _TOKEN.findall(query.lower()):
        w = w.strip("._+-")
        if len(w) < 2 or w in STOPWORDS:
            continue
        words.append(w)
        for syn in (glossary or {}).get(w, []):
            words.append(syn.lower())
    seen, terms = set(), []
    for w in words:
        if w not in seen:
            seen.add(w)
            terms.append('"' + w.replace('"', "") + '"')
    return " OR ".join(terms) if terms else None


def drive_file_id(url: str) -> str:
    m = _DRIVE_ID.search(url)
    return (m.group(1) or m.group(2)) if m else url


def search(conn: sqlite3.Connection, query: str, limit: int = 5, sources: list[str] | None = None,
           glossary: dict[str, list[str]] | None = None) -> list[dict[str, Any]]:
    sources = [s.lower() for s in (sources or ["confluence", "drive"])]
    limit = max(1, min(int(limit), MAX_LIMIT))
    match = fts_query(query, glossary)
    if match is None:
        return []
    rows = conn.execute(
        f"""SELECT c.id, c.page_id, c.ordinal, c.heading, c.drive_links,
                   p.title, p.url, p.updated, p.breadcrumb, p.kind,
                   snippet(chunks_fts, 2, '', '', ' ... ', 48) AS excerpt,
                   bm25(chunks_fts, {TITLE_WEIGHT}, {HEADING_WEIGHT}, {TEXT_WEIGHT}) AS rank
            FROM chunks_fts
            JOIN chunks c ON c.id = chunks_fts.rowid
            JOIN pages p ON p.id = c.page_id
            WHERE chunks_fts MATCH ?
            ORDER BY rank LIMIT {CANDIDATES}""",
        (match,),
    ).fetchall()

    best: dict[str, tuple[float, sqlite3.Row]] = {}
    for r in rows:
        score = -r["rank"] * KIND_FACTOR.get(r["kind"], 1.0)     # bm25() is lower-is-better
        if r["page_id"] not in best or score > best[r["page_id"]][0]:
            best[r["page_id"]] = (score, r)
    ranked = sorted(best.values(), key=lambda t: t[0], reverse=True)[:limit]

    hits: list[dict[str, Any]] = []
    drive: list[dict[str, Any]] = []
    seen_drive: set[str] = set()
    for score, r in ranked:
        if "confluence" in sources:
            hits.append({
                "source": "confluence",
                "id": r["page_id"],
                "title": r["title"],
                "url": r["url"],
                "updated": r["updated"],
                "breadcrumb": json.loads(r["breadcrumb"]),
                "section": r["heading"],
                "section_id": r["ordinal"],
                "excerpt": LINK_ONLY_NOTE if r["kind"] == "credentials_link_only" else r["excerpt"],
                "score": round(score, 3),
                "kind": r["kind"],
            })
        if "drive" in sources:
            for url, text in json.loads(r["drive_links"]):
                fid = drive_file_id(url)
                if fid in seen_drive or len(drive) >= MAX_DRIVE_LINKS:
                    continue
                seen_drive.add(fid)
                drive.append({
                    "source": "drive",
                    "id": fid,
                    "title": text if text and text != url else "Google Drive file",
                    "url": url,
                    "updated": None,
                    "excerpt": "",
                    "kind": "drive_link_only",
                    "via": {"source": "confluence", "id": r["page_id"], "title": r["title"]},
                })
    return hits + drive


def get_document(conn: sqlite3.Connection, page_id: str, section: str | int | None = None,
                 max_chars: int = DOCUMENT_MAX_CHARS) -> dict[str, Any] | None:
    page = conn.execute("SELECT * FROM pages WHERE id = ?", (str(page_id),)).fetchone()
    if page is None:
        return None
    if page["kind"] == "credentials_link_only":
        return {"source": "confluence", "id": page["id"], "title": page["title"], "url": page["url"],
                "updated": page["updated"], "breadcrumb": json.loads(page["breadcrumb"]), "kind": page["kind"],
                "sections": [], "text": LINK_ONLY_NOTE, "truncated": False, "drive_links": []}
    chunks = conn.execute("SELECT ordinal, heading, text, drive_links FROM chunks WHERE page_id = ? ORDER BY ordinal",
                          (page["id"],)).fetchall()
    selected = chunks
    if section is not None and str(section).strip() != "":
        s = str(section).strip()
        if s.isdigit():
            selected = [c for c in chunks if c["ordinal"] == int(s)]
        else:
            selected = [c for c in chunks if s.lower() in c["heading"].lower()]
        if not selected:
            selected = chunks           # unknown section: whole page, and the list of sections says what exists
    parts = []
    for c in selected:
        parts.append((f"## {c['heading']}\n\n" if c["heading"] else "") + c["text"])
    text = "\n\n".join(parts)
    truncated = len(text) > max_chars
    links = []
    for c in selected:
        for url, label in json.loads(c["drive_links"]):
            links.append({"url": url, "title": label})
    return {
        "source": "confluence",
        "id": page["id"],
        "title": page["title"],
        "url": page["url"],
        "updated": page["updated"],
        "breadcrumb": json.loads(page["breadcrumb"]),
        "kind": page["kind"],
        "sections": [{"section_id": c["ordinal"], "section": c["heading"]} for c in chunks],
        "text": text[:max_chars] + ("\n\n[truncated: ask for one section]" if truncated else ""),
        "truncated": truncated,
        "drive_links": links,
    }
