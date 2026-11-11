"""Blocks -> sections (chunks) for search: split at headings, ~200-600 words each.

- A new section starts at every heading (h1-h4); h5/h6 stay inside their parent section.
- Sections under ~40 words are merged into the next one (a heading with one line under it isn't worth a result).
- Sections over ~600 words are split at block boundaries into ~400-word pieces; a table is never split.
- Work logs and meeting notes get the same treatment: their dated headings become the sections, so one entry
  is one result instead of the whole 100-version page.
Each chunk carries its heading path and the Drive links found in it (the basic Drive fallback).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from evil.knowledge.textify import Block, drive_links

CHUNKER_VERSION = "1"      # bump to re-chunk every page on the next pass

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


def chunk(blocks: list[Block]) -> list[Chunk]:
    sections: list[tuple[str, list[Block]]] = []
    path: list[tuple[int, str]] = []
    current: list[Block] = []
    current_heading = ""
    for block in blocks:
        if block.kind == "heading" and block.level <= MAX_SECTION_LEVEL:
            if current:
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

    merged: list[tuple[str, list[Block]]] = []
    carry: tuple[str, list[Block]] | None = None
    for heading, body in pieces:
        if carry is not None:
            carry_heading, carry_body = carry
            # the merged section keeps the next section's heading as a line, so its text still says where it is
            joint = [Block("para", heading.split(" > ")[-1])] if heading and heading != carry_heading else []
            heading = carry_heading or heading
            body = carry_body + joint + body
            carry = None
        if sum(_words(b) for b in body) < MIN_WORDS:
            carry = (heading, body)
            continue
        merged.append((heading, body))
    if carry is not None:
        if merged:
            h, b = merged[-1]
            merged[-1] = (h, b + carry[1])
        else:
            merged.append(carry)

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
