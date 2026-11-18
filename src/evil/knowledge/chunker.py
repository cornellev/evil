"""Blocks -> sections (chunks) for search: split at headings, ~200-600 words each.

- A new section starts at every heading (h1-h4); h5/h6 stay inside their parent section.
- Sections under ~40 words are merged into the next one (a heading with one line under it isn't worth a result).
  A merged section is labelled with the heading path its parts share, and each part's own heading stays in its
  text as a line (seen 11-17: Software Home's two layout columns, Autonomy and Telemetry, merged under "Autonomy >
  Current Members", so a Telemetry member read as Autonomy). Headings with nothing under them ("Leads: A, B") count.
- Sections over ~600 words are split at block boundaries into ~400-word pieces; a table is never split.
- Work logs and meeting notes get the same treatment: their dated headings become the sections, so one entry
  is one result instead of the whole 100-version page.
Each chunk carries its heading path and the Drive links found in it (the basic Drive fallback).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from evil.knowledge.textify import Block, drive_links

CHUNKER_VERSION = "4"      # bump to re-chunk every page on the next pass (4: merged sections keep every heading)
# credentials pages: title + link only, never their text (filters.skip_reason "credentials title")
LINK_ONLY_KIND = "credentials_link_only"

MAX_SECTION_LEVEL = 4
MIN_WORDS = 40
MAX_WORDS = 600
TARGET_WORDS = 400

WORK_LOG = re.compile(r"\bwork\s*-?\s*logs?\b|\bworklogs?\b|\bdaily\s+logs?\b", re.I)
MEETING = re.compile(r"\bmeeting\s+(notes|minutes)\b|\bminutes\b|\bstandup\b|\bstand-up\b", re.I)


@dataclass
class Chunk:
    ordinal: int
    heading: str                 # "Electrical > Joulemeter" style path inside the page ("" = page intro)
    text: str
    drive_links: list[tuple[str, str]] = field(default_factory=list)

    @property
    def words(self) -> int:
        return len(self.text.split())


def page_kind(title: str, labels: list[str]) -> str:
    joined = " ".join([title, *labels])
    if WORK_LOG.search(joined) or {"work-log", "worklog", "work_log"} & {l.lower() for l in labels}:
        return "work_log"
    if MEETING.search(joined) or {"meeting-notes", "meeting_notes"} & {l.lower() for l in labels}:
        return "meeting_notes"
    return "page"


def _words(block: Block) -> int:
    return len(block.render().split())


def _join(group: list[tuple[str, list[Block]]]) -> tuple[str, list[Block]]:
    """One section from several small ones: the heading path they share (or, sharing none, each part's top heading),
    each part's own heading as a line."""
    if len(group) == 1:
        return group[0]
    paths = [h.split(" > ") if h else [] for h, _ in group]
    prefix: list[str] = []
    for parts in zip(*paths):
        if len(set(parts)) != 1:
            break
        prefix.append(parts[0])
    body: list[Block] = []
    for parts, (_, part) in zip(paths, group):
        rest = parts[len(prefix):]
        if rest:
            body.append(Block("para", " > ".join(rest)))
        body.extend(part)
    if prefix:
        return " > ".join(prefix), body
    # nothing shared: every part's top heading, so a work log's dates and a page's columns are all findable
    return " · ".join(dict.fromkeys(p[0] for p in paths if p)), body


def chunk(blocks: list[Block]) -> list[Chunk]:
    sections: list[tuple[str, list[Block]]] = []
    path: list[tuple[int, str]] = []
    current: list[Block] = []
    current_heading = ""
    for block in blocks:
        if block.kind == "heading" and block.level <= MAX_SECTION_LEVEL:
            if current or current_heading:       # an empty section keeps its heading (it may be the content)
                sections.append((current_heading, current))
            path = [(lvl, t) for lvl, t in path if lvl < block.level] + [(block.level, block.text)]
            current_heading = " > ".join(t for _, t in path)
            current = []
            continue
        current.append(block)
    if current:
        sections.append((current_heading, current))

    # split large sections, then merge small ones forward
    pieces: list[tuple[str, list[Block]]] = []
    for heading, body in sections:
        if sum(_words(b) for b in body) <= MAX_WORDS:
            pieces.append((heading, body))
            continue
        part: list[Block] = []
        count = 0
        for b in body:
            w = _words(b)
            if part and count + w > TARGET_WORDS:
                pieces.append((heading, part))
                part, count = [], 0
            part.append(b)
            count += w
        if part:
            pieces.append((heading, part))

    groups: list[list[tuple[str, list[Block]]]] = []
    group: list[tuple[str, list[Block]]] = []
    for heading, body in pieces:
        group.append((heading, body))
        if sum(_words(b) for _, part in group for b in part) >= MIN_WORDS:
            groups.append(group)
            group = []
    if group:
        if groups:
            groups[-1].extend(group)
        else:
            groups.append(group)
    merged = [_join(g) for g in groups]

    chunks = []
    for i, (heading, body) in enumerate(merged):
        text = "\n\n".join(t for t in (b.render() for b in body) if t.strip())
        links = []
        seen = set()
        for b in body:
            for url, label in drive_links(b.links):
                if url not in seen:
                    seen.add(url)
                    links.append((url, label))
        if text.strip() or links:
            chunks.append(Chunk(len(chunks), heading, text, links))
    return chunks
