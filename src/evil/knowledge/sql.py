"""knowledge_sql: one read-only SELECT over the team docs index (user 11-18: "u should be able to ask anything and it
can direct u right"). Search can't answer list / count / date / who-edited questions ("what changed in Confluence
these past 2 weeks", "what's new in Drive"); SQL over the index's metadata can.

Two TEMP views on a read-only connection (the file is never written): `docs` (one row per page, attachment, Drive
file or folder) and `sections` (their text). Credentials pages have no text here, the same as in search. Runs through
EVIL's read_only_sql guard (one SELECT, a step budget, a row cap); long text values are cut.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from evil.knowledge.drive import kind_label
from evil.knowledge.search import STOPWORDS, TEAM_WORDS, _TOKEN, _stem
from evil.tools.read_only_sql import read_only_sql

TIMEZONE = "America/New_York"
ROW_LIMIT = 200
VALUE_MAX_CHARS = 600

NOTES = (
    "One read-only SQL SELECT (SQLite) over the team's docs index: Confluence (the cev space: pages and files attached "
    "to them), the team's shared Google Drive (every file and folder) and the team's GitHub repos (one row per repo, "
    "and their READMEs, docs and text / data files; not code: use search_code / get_document for code). Use it for lists, counts, dates and people: "
    "what changed or is new in a time window, who last edited something, everything in a folder or about a topic, "
    "how many. For how / why questions use search_team_knowledge instead.\n"
    "View docs: id, source ('confluence' | 'drive' | 'github'), title, url, path (folders or parent pages, 'A > B'), kind "
    "('page', 'work_log', 'meeting_notes', 'attachment', 'credentials' on Confluence; 'file', 'photo', 'video', "
    "'folder' on Drive; 'repo', 'repo_file' on GitHub), repo (GitHub: the repo name), private (1 = a private repo), type ('Google Doc', 'Google Slides', 'PDF', 'photo', 'SLDPRT file', ...), "
    "created, modified (UTC 'YYYY-MM-DD HH:MM:SS'), created_local, modified_local (America/New_York), last_editor "
    "(a person's name), size_bytes, sections (how many text sections).\n"
    "type says what a file is: slides / decks = type IN ('Google Slides', 'PowerPoint'), sheets = 'Google Sheet', "
    "docs = 'Google Doc'; sections > 0 = its text can be read with get_document. Paths are full ('Telemetry > 2026-2027 > "
    "Onboarding'), so never guess one: a folder people name ('the telemetry onboarding folder') is found with "
    "SELECT path || ' > ' || title AS folder FROM docs WHERE kind = 'folder' AND title LIKE '%Onboarding%' AND "
    "path LIKE '%Telemetry%', and what's in it with WHERE path = '<that folder>' (every type: slides, docs, "
    "shortcuts, files), not words of titles.\n"
    "What a doc is about is in its text more than its title (a deck titled 'Onboarding 2' lists the freshman "
    "projects): filter topics with matches(text, 'words'), which ignores case and plurals and knows the team's "
    "spellings (freshman / freshmen / frosh, elec / electrical): SELECT d.id, d.source, d.title, d.path FROM docs d "
    "WHERE d.path LIKE '%Electrical%' AND (d.id IN (SELECT doc_id FROM sections WHERE matches(text, 'freshman "
    "projects')) OR matches(d.title || ' ' || d.path, 'freshman projects')). Never LIKE on title words alone for "
    "a topic.\n"
    "GitHub: modified / last_editor of a repo_file are its last commit; of a repo, its last commit on the default branch. "
    "Repos and their docs: SELECT title, modified_local, url FROM docs WHERE kind = 'repo' ORDER BY modified DESC; repos "
    "about a topic (their names, descriptions and READMEs; docs has no text column): SELECT DISTINCT d.repo, r.url FROM "
    "docs d JOIN docs r ON r.kind = 'repo' AND r.title = d.repo WHERE d.source = 'github' AND (matches(d.title, 'lidar') "
    "OR d.id IN (SELECT doc_id FROM sections WHERE matches(text, 'lidar'))); a repo's "
    "READMEs: SELECT title, url FROM docs WHERE repo = 'evil' AND title LIKE '%README%'. For commits, pull requests and "
    "issues use repo_activity.\n"
    "A question about one source filters on it: Confluence = source = 'confluence', Drive = source = 'drive', GitHub = "
    "source = 'github' (seen 11-19: \"what changed in Drive\" without it listed GitHub folders as Drive ones).\n"
    "To open a row afterwards with get_document, select its id and source (get_document(source, id)).\n"
    "View sections: doc_id (= docs.id), section_id, heading, text.\n"
    "Compare times with datetime('now', '-14 days'). Examples: what changed on Confluence in the last 2 weeks: SELECT "
    "title, modified_local, last_editor, url FROM docs WHERE source = 'confluence' AND kind != 'attachment' AND "
    "modified >= datetime('now', '-14 days') ORDER BY modified DESC. New in Drive this week, by folder, with the total: "
    "SELECT path, COUNT(*) AS files, SUM(COUNT(*)) OVER () AS all_files, MAX(modified_local) AS latest FROM docs "
    "WHERE source = 'drive' AND kind != 'folder' AND created >= datetime('now', '-7 days') GROUP BY path ORDER BY "
    "files DESC (a grouped result has one row per group: the total is all_files, never the number of rows; "
    "'new' = created, 'changed' / 'updated' = modified). Who edits the most: SELECT last_editor, "
    "COUNT(*) FROM docs WHERE modified >= datetime('now', '-30 days') GROUP BY last_editor ORDER BY 2 DESC. Docs that "
    "mention a word: SELECT DISTINCT d.title, d.url FROM sections s JOIN docs d ON d.id = s.doc_id WHERE s.text LIKE "
    "'%joulemeter%' LIMIT 20. Match titles on words, not short abbreviations (LIKE '%DR%' also matches 'Drawings'; use '%design review%'). "
    "Drive holds ~70k files, most of them photos: filter by kind or type, and GROUP BY path "
    "to summarize many rows. At most 200 rows; text values over 600 characters are cut (read a page with "
    "get_document)."
)

_KIND = """CASE p.kind
    WHEN 'attachment_link_only' THEN 'attachment' WHEN 'credentials_link_only' THEN 'credentials'
    WHEN 'drive_doc' THEN 'file' WHEN 'drive_link_only' THEN 'file' WHEN 'drive_folder' THEN 'folder'
    WHEN 'drive_media' THEN CASE WHEN p.mime_type LIKE 'video/%' THEN 'video' ELSE 'photo' END
    WHEN 'repo_doc' THEN 'repo_file'
    ELSE p.kind END"""


def _local(value: str | None) -> str | None:
    if not value:
        return None
    try:
        when = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return when.astimezone(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")


def _path(breadcrumb: str | None) -> str:
    try:
        return " > ".join(json.loads(breadcrumb or "[]"))
    except ValueError:
        return ""


def _type(mime: str | None, name: str | None, source: str) -> str | None:
    if source == "confluence" and not mime:
        return "Confluence page"
    if source == "github":
        return "GitHub repo" if "/" not in (name or "") else kind_label(None, name or "")
    return kind_label(mime, name or "")


def _words(text: str) -> set[str]:
    return {_stem(w.strip("._+-")) for w in _TOKEN.findall((text or "").lower())}


_QUERY_CACHE: dict[str, list[set[str]]] = {}


def matches(text: str | None, query: str | None) -> int:
    """1 when every content word of `query` is in `text`, with the same stemming and team word variants as search
    (freshman / freshmen / frosh, elec / electrical ...): the way to filter on what docs say, not just titles (seen
    11-18: "mech freshman projects" matched title words and missed "UC27 Freshman Project Master Doc")."""
    if not text or not query:
        return 0
    need = _QUERY_CACHE.get(query)
    if need is None:
        need = []
        for w in _TOKEN.findall(query.lower()):
            w = w.strip("._+-")
            if len(w) >= 2 and w not in STOPWORDS:
                need.append({_stem(v) for v in [w, *TEAM_WORDS.get(w, [])]})
        _QUERY_CACHE[query] = need
    low = text.lower()
    if not all(any(v in low for v in alts) for alts in need):     # most sections fail here, in C (11-18: 9 s -> <1 s)
        return 0
    have = _words(text)
    return int(all(alts & have for alts in need))


def add_views(conn: sqlite3.Connection) -> None:
    conn.create_function("local_time", 1, _local, deterministic=True)
    conn.create_function("matches", 2, matches, deterministic=True)
    conn.create_function("_path", 1, _path, deterministic=True)
    conn.create_function("_type", 3, _type, deterministic=True)
    conn.execute(f"""CREATE TEMP VIEW IF NOT EXISTS docs AS
        SELECT p.id, p.source, p.title, p.url, _path(p.breadcrumb) AS path, {_KIND} AS kind,
               _type(p.mime_type, p.title, p.source) AS type,
               datetime(p.created) AS created, datetime(p.updated) AS modified,
               local_time(p.created) AS created_local, local_time(p.updated) AS modified_local,
               p.last_editor, p.size AS size_bytes,
               CASE WHEN p.source = 'github' THEN
                   CASE WHEN p.kind = 'repo' THEN p.title ELSE json_extract(p.breadcrumb, '$[0]') END END AS repo,
               CASE WHEN p.labels LIKE '%"private"%' THEN 1 ELSE 0 END AS private,
               (SELECT COUNT(*) FROM chunks c WHERE c.page_id = p.id AND c.text != '') AS sections
        FROM pages p""")
    conn.execute("""CREATE TEMP VIEW IF NOT EXISTS sections AS
        SELECT c.page_id AS doc_id, c.ordinal AS section_id, c.heading, c.text
        FROM chunks c JOIN pages p ON p.id = c.page_id WHERE p.kind != 'credentials_link_only'""")


def _cut(value: Any) -> Any:
    if isinstance(value, str) and len(value) > VALUE_MAX_CHARS:
        return value[:VALUE_MAX_CHARS] + " ..."
    return value


def with_aliases(query: str) -> str:
    """`d.` / `s.` used without defining them get docs d / sections s (seen 11-18: the model wrote FROM docs with
    d.id three times in a row, error hint and all, and ran out of steps)."""
    for alias, view in (("d", "docs"), ("s", "sections")):
        if not re.search(rf"\b{alias}\.\w", query):
            continue
        if re.search(rf"\b\w+\s+(?:AS\s+)?{alias}\b(?!\.)", query, re.I) and \
                re.search(rf"\b{view}\s+(?:AS\s+)?{alias}\b", query, re.I):
            continue                                  # already defined
        if re.search(rf"\b(?:FROM|JOIN)\s+\w+\s+(?:AS\s+)?{alias}\b(?!\.)", query, re.I):
            continue                                  # the alias names something else: leave it to SQLite
        query, n = re.subn(rf"\b(FROM|JOIN)\s+{view}\b(?!\s+(?:AS\s+)?\w+\s*(?:,|WHERE|JOIN|ON|GROUP|ORDER|"
                           rf"LIMIT|$))", rf"\1 {view} {alias}", query, count=1, flags=re.I)
    return query


def run(conn: sqlite3.Connection, query: str) -> list[dict[str, Any]]:
    add_views(conn)
    query = with_aliases(query)
    rows = read_only_sql(conn, query=query, row_limit=ROW_LIMIT)
    return [{k: _cut(v) for k, v in r.items()} for r in rows]


_ALIAS = re.compile(r"no such column: (\w+)\.\w+")


def explain(exc: Exception, query: str = "") -> str:
    """SQLite's reason, plus what to change: a missing alias names the fix (seen 11-18: "d.id" with FROM docs and no
    `d`); another unknown name gets the views' columns."""
    msg = str(exc)
    m = _ALIAS.search(msg)
    if m and not re.search(rf"\b(?:docs|sections)\s+(?:AS\s+)?{m.group(1)}\b", query, re.I):
        return (f"{msg}: the query uses the alias {m.group(1)}. but never defines it; write FROM docs {m.group(1)} "
                f"(or sections {m.group(1)}), or drop the {m.group(1)}. prefix.")
    if "no such table" in msg or "no such column" in msg:
        msg += "\n" + NOTES
    return msg
