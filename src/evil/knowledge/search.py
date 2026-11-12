"""Search over knowledge.db: keywords (BM25), fused with meaning-based matches (embeddings.py) when available.

- The question becomes an OR of its content words (FTS5 is AND by default, which natural-language questions
  almost never satisfy); porter stemming matches "measuring" to "measure".
- BM25 with title hits weighted highest, then headings, then text.
- Work logs and meeting notes are pushed down (they match everything with one stray line).
- Slight edges on top of BM25: a page whose title holds the query's words (BM25 alone let long reports that mention
  "joulemeter" often outrank the Joulemeter page), and a recently updated page (fades over about a year).
- Deterministic: ties are broken by last update, then page id, so the same query on the same index gives the same list.
- Meaning-based matches (phase 3): the query's embedding against every section's; pages ranked by their closest
  section, merged with the keyword ranking by a weighted sum that trusts keywords more the better they cover the
  query (see fuse). Finds pages for questions asked in other words than the page uses. Off or thor down -> the
  keyword ranking alone, unchanged.
- One section per page by default: a page's best section represents it, so one long page can't fill the list.
- Confluence attachments (PDFs, decks, docs on a page) are results like pages: kind "attachment" (text indexed,
  readable with get_document) or "attachment_link_only" (name, type, size, link); breadcrumb ends with their page.
- Basic Drive fallback: Drive links found in the returned sections are listed after them as link-only hits (title =
  the link text). No Google access; nothing about the file is known beyond its link.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

TITLE_WEIGHT, HEADING_WEIGHT, TEXT_WEIGHT = 8.0, 3.0, 1.0
# attachments a little below pages, so a page usually ranks above the files attached to it
KIND_FACTOR = {"page": 1.0, "work_log": 0.5, "meeting_notes": 0.6, "credentials_link_only": 1.0,
               "attachment": 0.9, "attachment_link_only": 0.9}
LINK_ONLY_NOTE = ("This page holds team credentials, so its contents are not available here. Give the user the "
                  "link; Confluence decides who can open it.")
TITLE_BOOST = 0.5           # x(1 + TITLE_BOOST * share of query words in the title)
TITLE_EXACT_BOOST = 0.25    # and this much more when the title is just the query's words
RECENCY_BOOST = 0.1         # x(1 + RECENCY_BOOST * 0.5 ** (age / half-life)): at most +10%
RECENCY_HALF_LIFE_DAYS = 365
EMPTY_PAGE_FACTOR = 0.3     # title-only entries are tiny, so BM25 overrates them on a single title word
CANDIDATES = 200
VECTOR_CANDIDATES = 50      # closest sections by meaning, before one-per-page
# Fusion (tests/e2e/knowledge_selfeval.py, 11-11): keyword weight = floor + (1 - floor) * coverage ** power, where
# coverage = share of the query's words found in the top keyword hit. Plain rank fusion lost passage questions
# (hit@3 0.99 -> 0.70) because a confident keyword #1 lost to pages middling in both lists.
KEYWORD_WEIGHT_FLOOR = 0.2
COVERAGE_POWER = 2.0
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


def _stem(word: str) -> str:
    """Crude plural/tense folding, enough to match "brakes" to the title "Brake Fluid Sensor"."""
    for suffix in ("ing", "es", "ed", "s"):
        if len(word) > len(suffix) + 3 and word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def _content_stems(text: str) -> list[str]:
    out = []
    for w in _TOKEN.findall(text.lower()):
        w = w.strip("._+-")
        if len(w) >= 2 and w not in STOPWORDS:
            out.append(_stem(w))
    return out


def title_share(query_stems: set[str], title: str) -> float:
    """Share of the query's words that are in the title (0..1)."""
    if not query_stems:
        return 0.0
    return len(query_stems & set(_content_stems(title))) / len(query_stems)


def title_factor(query_stems: set[str], title: str) -> float:
    share = title_share(query_stems, title)
    title_stems = set(_content_stems(title))
    exact = TITLE_EXACT_BOOST if title_stems and title_stems <= query_stems and share == 1 else 0.0
    return 1.0 + TITLE_BOOST * share + exact


def recency_factor(updated: str, now: datetime) -> float:
    try:
        when = datetime.fromisoformat(updated.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return 1.0
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    age_days = max(0.0, (now - when).total_seconds() / 86400)
    return 1.0 + RECENCY_BOOST * 0.5 ** (age_days / RECENCY_HALF_LIFE_DAYS)


def drive_file_id(url: str) -> str:
    m = _DRIVE_ID.search(url)
    return (m.group(1) or m.group(2)) if m else url


_ROW_COLS = """c.id, c.page_id, c.ordinal, c.heading, c.drive_links, c.text = '' AS empty,
               p.title, p.url, p.updated, p.breadcrumb, p.kind"""


def _keyword_ranked(conn: sqlite3.Connection, query: str, glossary, now: datetime) -> list[tuple[float, Any]]:
    """Pages by their best keyword-matching section: BM25 x kind x title/recency edges (best first)."""
    match = fts_query(query, glossary)
    if match is None:
        return []
    sql = f"""SELECT {_ROW_COLS},
                   snippet(chunks_fts, 2, '', '', ' ... ', 48) AS excerpt,
                   bm25(chunks_fts, {TITLE_WEIGHT}, {HEADING_WEIGHT}, {TEXT_WEIGHT}) AS rank
            FROM chunks_fts
            JOIN chunks c ON c.id = chunks_fts.rowid
            JOIN pages p ON p.id = c.page_id
            WHERE chunks_fts MATCH ?
            ORDER BY rank, c.id LIMIT {CANDIDATES}"""
    rows = conn.execute(sql, (match,)).fetchall()

    query_stems = set(_content_stems(query))
    best: dict[str, tuple[float, Any]] = {}
    for r in rows:
        score = -r["rank"] * KIND_FACTOR.get(r["kind"], 1.0)     # bm25() is lower-is-better
        if r["empty"] and r["kind"] != "credentials_link_only":
            # only a title to go on: full weight when the query names the page, less on one stray title word
            share = title_share(query_stems, r["title"])
            score *= EMPTY_PAGE_FACTOR + (1 - EMPTY_PAGE_FACTOR) * share
        if r["page_id"] not in best or score > best[r["page_id"]][0]:
            best[r["page_id"]] = (score, dict(r))
    scored = []
    for score, r in best.values():          # page-level edges: the same for every section of a page
        score *= title_factor(query_stems, r["title"]) * recency_factor(r["updated"], now)
        scored.append((round(score, 6), r))
    return _sorted(scored)


def _vector_ranked(conn: sqlite3.Connection, vectors, query_vec) -> list[tuple[float, Any]]:
    """Pages by their most similar section (cosine x kind, best first)."""
    top = vectors.top(query_vec, VECTOR_CANDIDATES)
    if not top:
        return []
    sims = dict(top)
    placeholders = ",".join("?" * len(sims))
    rows = conn.execute(f"SELECT {_ROW_COLS}, c.text FROM chunks c JOIN pages p ON p.id = c.page_id "
                        f"WHERE c.id IN ({placeholders})", list(sims)).fetchall()
    best: dict[str, tuple[float, Any]] = {}
    for r in rows:
        score = sims[r["id"]] * KIND_FACTOR.get(r["kind"], 1.0)
        if r["page_id"] not in best or score > best[r["page_id"]][0]:
            d = dict(r)
            words = d.pop("text").split()
            d["excerpt"] = " ".join(words[:48]) + (" ..." if len(words) > 48 else "")
            best[r["page_id"]] = (score, d)
    return _sorted([(round(s, 6), r) for s, r in best.values()])


def _sorted(scored: list[tuple[float, Any]]) -> list[tuple[float, Any]]:
    scored.sort(key=lambda t: t[1]["page_id"])
    scored.sort(key=lambda t: t[1]["updated"] or "", reverse=True)
    scored.sort(key=lambda t: t[0], reverse=True)               # stable: ties keep newer-first, then id order
    return scored


def keyword_coverage(conn: sqlite3.Connection, query: str, keyword: list[tuple[float, Any]]) -> float:
    """Share of the query's words found in the top keyword hit (title + heading + text): 1.0 for "joulemeter specs"
    or a few distinctive words, ~0.5 for a question in plain words that only shares "car" and "power"."""
    stems = set(_content_stems(query))
    if not keyword or not stems:
        return 0.0
    top = keyword[0][1]
    text = conn.execute("SELECT text FROM chunks WHERE id = ?", (top["id"],)).fetchone()
    have = set(_content_stems(" ".join([top["title"], top["heading"], text[0] if text else ""])))
    return len(stems & have) / len(stems)


def fuse(keyword: list[tuple[float, Any]], vector: list[tuple[float, Any]], coverage: float) -> list[tuple[float, Any]]:
    """Weighted sum of the two lists' normalized scores (keyword: / its best; meaning: min-max over the
    candidates). The keyword list weighs more the better its top hit covers the query, so exact lookups keep
    their keyword ranking and plain-language questions lean on meaning. A page shows its keyword section when it
    has one (that comes with a highlighted excerpt), else its closest section by meaning."""
    w = KEYWORD_WEIGHT_FLOOR + (1 - KEYWORD_WEIGHT_FLOOR) * coverage ** COVERAGE_POWER
    kmax = max((sc for sc, _ in keyword), default=0.0) or 1.0
    sims = [sc for sc, _ in vector]
    vmax, vmin = (max(sims), min(sims)) if sims else (1.0, 0.0)
    fused: dict[str, list] = {}
    for sc, r in keyword:
        fused[r["page_id"]] = [w * sc / kmax, r]
    for sc, r in vector:
        v = (sc - vmin) / (vmax - vmin) if vmax > vmin else 1.0
        entry = fused.setdefault(r["page_id"], [0.0, r])
        entry[0] += (1 - w) * v
    return _sorted([(round(sc, 6), r) for sc, r in fused.values()])


def search(conn: sqlite3.Connection, query: str, limit: int = 5, sources: list[str] | None = None,
           glossary: dict[str, list[str]] | None = None, now: datetime | None = None,
           vectors=None, query_vec=None) -> list[dict[str, Any]]:
    """Keyword search, fused with meaning-based matches when `vectors` (embeddings.VectorIndex) and the query's
    `query_vec` are given; without them (embedder off or down) it is the keyword ranking alone."""
    sources = [s.lower() for s in (sources or ["confluence", "drive"])]
    limit = max(1, min(int(limit), MAX_LIMIT))
    now = now or datetime.now(timezone.utc)
    ranked = _keyword_ranked(conn, query, glossary, now)
    if vectors is not None and query_vec is not None:
        ranked = fuse(ranked, _vector_ranked(conn, vectors, query_vec), keyword_coverage(conn, query, ranked))
    ranked = ranked[:limit]

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
                "excerpt": (LINK_ONLY_NOTE if r["kind"] == "credentials_link_only"
                            else r["excerpt"] or "(no text of its own: likely a parent page, or attachments only)"),
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
