"""What must never be stored, and therefore can never be served (cev-knowledge plan, "Decided").

Tern answers everyone in a channel, and the index is built with one member's token, so:
- whole pages are skipped when they (or any ancestor: Confluence view restrictions are inherited) carry a view
  restriction, carry the `no-tern` label, or are about credentials by their title;
- inside pages, credential-looking content is removed: a section under a credentials heading, a table whose header
  names a credential column, a row or line that names one (and the line after it, where the value often sits), and
  key-shaped strings anywhere.
The rules are deliberately greedy: losing a harmless line about "changing your password" costs little.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from evil.knowledge.confluence import Page
from evil.knowledge.textify import Block

NO_TERN_LABELS = {"no-tern", "no_tern", "notern"}

_STRONG = re.compile(
    r"\b(passwords?|passwd|passcodes?|passphrases?|pwd|api[\s_-]?keys?|secrets?|credentials?|"
    r"ssh[\s_-]?keys?|private[\s_-]?keys?)\b",
    re.I,
)
# Words that are fine in normal prose ("log in to RED"): only count them next to a value
# marker such as "Login: cev" or "token = ...". Not "pin" or "pass": pinout tables ("Pin 1: VCC") and "Pass 2: ..." are everywhere here.
_WEAK = re.compile(r"\b(log[\s-]?ins?|tokens?)\b", re.I)
_VALUE_MARKER = re.compile(r"[:=]")
CREDENTIAL_TITLE = re.compile(r"\b(passwords?|credentials?|logins?|secrets?)\b", re.I)

KEY_SHAPED = [
    re.compile(r"\b(?:ghp|gho|ghu|ghs|github_pat)_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bxox[abpr]-[A-Za-z0-9-]{10,}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)"),
    re.compile(r"(?<=://)[^/\s:@]+:[^/\s@]+(?=@)"),                       # user:pass@ in URLs
    # long mixed letter+digit blobs, but not ids inside URLs (Drive file ids are 33-44 chars)
    re.compile(r"(?<![/=?&A-Za-z0-9+_-])(?=[A-Za-z0-9+_-]*\d)(?=[A-Za-z0-9+_-]*[A-Za-z])[A-Za-z0-9+_-]{32,}={0,2}"),
]
REDACTED = "[redacted]"


@dataclass
class ScrubStats:
    lines: int = 0
    rows: int = 0
    tables: int = 0
    sections: int = 0
    keys: int = 0

    @property
    def total(self) -> int:
        return self.lines + self.rows + self.tables + self.sections + self.keys


def mentions_credential(text: str, header: bool = False) -> bool:
    """`header`: a table header cell, where a bare "Login" / "PIN" / "Pass" column is enough."""
    text = text or ""
    if _STRONG.search(text):
        return True
    return bool(_WEAK.search(text) and (header or _VALUE_MARKER.search(text)))


def skip_reason(page: Page, restricted_ids: set[str], space: str) -> str | None:
    """Why a page must not be indexed, or None."""
    if page.space and page.space != space.lower():
        return "other space"
    if page.restricted or page.id in restricted_ids:
        return "restricted"
    if any(aid in restricted_ids for aid, _ in page.ancestors):
        return "restricted ancestor"
    if NO_TERN_LABELS & {l.lower() for l in page.labels}:
        return "no-tern label"
    if CREDENTIAL_TITLE.search(page.title):
        return "credentials title"
    return None


def redact_keys(text: str, stats: ScrubStats | None = None) -> str:
    for pattern in KEY_SHAPED:
        text, n = pattern.subn(REDACTED, text)
        if stats is not None:
            stats.keys += n
    return text


def scrub(blocks: list[Block]) -> tuple[list[Block], ScrubStats]:
    stats = ScrubStats()
    out: list[Block] = []
    skip_below: int | None = None       # inside a credentials section: drop until a heading at this level or higher
    drop_next = False
    for block in blocks:
        if block.kind == "heading":
            if skip_below is not None and block.level <= skip_below:
                skip_below = None
            if skip_below is None and mentions_credential(block.text):
                skip_below = block.level
                stats.sections += 1
                continue
            drop_next = False
        if skip_below is not None:
            continue
        if block.kind == "table" and block.rows:
            if block.header and any(mentions_credential(c, header=True) for c in block.rows[0]):
                stats.tables += 1
                out.append(Block("para", "[table removed: credentials]"))
                continue
            rows = []
            for row in block.rows:
                if any(mentions_credential(c) for c in row):
                    stats.rows += 1
                    continue
                rows.append([redact_keys(c, stats) for c in row])
            if rows:
                out.append(Block("table", rows=rows, header=block.header, links=block.links))
            drop_next = False
            continue
        if block.kind in ("para", "code"):
            kept = []
            for line in block.text.split("\n"):
                if drop_next and line.strip():
                    drop_next = False
                    stats.lines += 1
                    continue
                if mentions_credential(line):
                    stats.lines += 1
                    # the value often sits on the next line ("Username & Password" / "cev / hunter2")
                    drop_next = len(line) < 80
                    continue
                kept.append(redact_keys(line, stats))
            text = "\n".join(kept).strip()
            if text or block.links:
                out.append(Block(block.kind, text, links=block.links))
            continue
        out.append(Block(block.kind, redact_keys(block.text, stats), level=block.level, links=block.links))
    return out, stats
