"""Confluence storage format (XHTML with ac:/ri: macro tags) -> a flat list of blocks.

Keeps what search and answers need: headings (for sections), paragraphs and list items, tables (rows of cells,
kept whole), code/noformat macro bodies, and every link with its text (Drive links are what the basic Drive
fallback returns). Macro parameters, images and other markup are dropped. Standard library only (HTMLParser).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from html.parser import HTMLParser

DRIVE_URL = re.compile(r"https?://(?:docs|drive|sheets|slides)\.google\.com/[^\s\"'<>\]\)]+", re.I)
_URL = re.compile(r"https?://[^\s\"'<>\]\)]+")

_HEADINGS = {f"h{i}": i for i in range(1, 7)}
_BLOCK_TAGS = {"p", "div", "li", "blockquote", "pre", "ac:task-body", "dd", "dt", "caption"}
_SKIP_TAGS = {"ac:parameter", "ac:image", "script", "style", "ri:attachment"}
_CODE_MACROS = {"code", "noformat"}


@dataclass
class Block:
    kind: str                                  # heading | para | table | code
    text: str = ""
    level: int = 0                             # heading level
    rows: list[list[str]] | None = None        # table rows (first row = header if it had <th>)
    header: bool = False                       # the table's first row is a header row
    links: list[tuple[str, str]] = field(default_factory=list)  # (url, link text)

    def render(self) -> str:
        if self.kind == "table" and self.rows:
            lines = []
            for i, row in enumerate(self.rows):
                lines.append("| " + " | ".join(c.replace("|", "/") for c in row) + " |")
                if i == 0 and self.header:
                    lines.append("|" + "---|" * len(row))
            return "\n".join(lines)
        if self.kind == "code":
            return f"```\n{self.text}\n```"
        return self.text


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


class _Parser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[Block] = []
        self._buf: list[str] = []
        self._links: list[tuple[str, str]] = []
        self._heading: int = 0
        self._skip = 0
        self._href: str | None = None
        self._href_text: list[str] = []
        self._ac_link_title: str | None = None
        self._macro_stack: list[str] = []
        self._code: list[str] | None = None
        self._param_name: str | None = None
        # tables: only the outermost table becomes a block; nested ones become cell text
        self._table_depth = 0
        self._rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._table_header = False
        self._table_links: list[tuple[str, str]] = []

    # -- helpers
    def _text_sink(self) -> list[str]:
        if self._cell is not None:
            return self._cell
        return self._buf

    def _add_link(self, url: str, text: str) -> None:
        link = (url, _clean(text) or url)
        (self._table_links if self._table_depth else self._links).append(link)

    def _flush(self) -> None:
        if self._table_depth or self._code is not None:
            return
        text = _clean("".join(self._buf))
        if text:
            if self._heading:
                self.blocks.append(Block("heading", text, level=self._heading, links=self._links))
            else:
                self.blocks.append(Block("para", text, links=self._links))
        elif self._links:
            self.blocks.append(Block("para", "", links=self._links))
        self._buf, self._links = [], []

    # -- parser callbacks
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if tag == "ac:structured-macro" or tag == "ac:macro":
            self._macro_stack.append((a.get("ac:name") or "").lower())
            if self._macro_stack[-1] in _CODE_MACROS and not self._table_depth:
                self._flush()
                self._code = []
            return
        if tag == "ac:parameter":
            self._param_name = (a.get("ac:name") or "").lower()
            self._skip += 1
            return
        if tag in _SKIP_TAGS:
            self._skip += 1
            return
        if tag == "ri:url" and a.get("ri:value"):
            self._add_link(a["ri:value"], a["ri:value"])
            return
        if tag == "ri:page":
            self._ac_link_title = a.get("ri:content-title")
            return
        if tag == "ac:link":
            self._ac_link_title = None
            return
        if tag == "a":
            self._href = a.get("href")
            self._href_text = []
            return
        if tag == "br":
            self._text_sink().append("\n" if self._code is not None else " ")
            return
        if tag == "table":
            if self._table_depth == 0:
                self._flush()
                self._rows, self._table_header, self._table_links = [], False, []
            self._table_depth += 1
            return
        if self._table_depth == 1:
            if tag == "tr":
                self._row = []
                return
            if tag in ("td", "th"):
                if tag == "th" and not self._rows:
                    self._table_header = True
                self._cell = []
                return
        if tag in _HEADINGS and not self._table_depth:
            self._flush()
            self._heading = _HEADINGS[tag]
            return
        if tag in _BLOCK_TAGS and not self._table_depth:
            self._flush()
            if tag == "li":
                self._buf.append("- ")
            return
        if self._table_depth and tag in ("li", "p", "div"):
            self._text_sink().append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag == "ac:structured-macro" or tag == "ac:macro":
            name = self._macro_stack.pop() if self._macro_stack else ""
            if name in _CODE_MACROS and self._code is not None:
                code = "".join(self._code).strip("\n")
                self._code = None
                if code.strip():
                    self.blocks.append(Block("code", code))
            return
        if tag == "ac:parameter":
            self._skip = max(0, self._skip - 1)
            self._param_name = None
            return
        if tag in _SKIP_TAGS:
            self._skip = max(0, self._skip - 1)
            return
        if tag == "ac:link":
            if self._ac_link_title and not self._href:
                self._text_sink().append(self._ac_link_title)
            self._ac_link_title = None
            return
        if tag == "a":
            if self._href:
                self._add_link(self._href, "".join(self._href_text))
            self._href = None
            return
        if tag == "table" and self._table_depth:
            self._table_depth -= 1
            if self._table_depth == 0:
                rows = [r for r in self._rows if any(c for c in r)]
                if rows:
                    self.blocks.append(Block("table", rows=rows, header=self._table_header, links=self._table_links))
                self._rows, self._table_links = [], []
            return
        if self._table_depth == 1:
            if tag in ("td", "th") and self._cell is not None and self._row is not None:
                self._row.append(_clean("".join(self._cell)))
                self._cell = None
                return
            if tag == "tr" and self._row is not None:
                self._rows.append(self._row)
                self._row = None
                return
        if tag in _HEADINGS and self._heading and not self._table_depth:
            self._flush()
            self._heading = 0
            return
        if tag in _BLOCK_TAGS and not self._table_depth:
            self._flush()

    def handle_data(self, data: str) -> None:
        if self._skip:
            if self._param_name == "url" and _URL.match(data.strip()):
                self._add_link(data.strip(), data.strip())
            return
        if self._code is not None:
            self._code.append(data)
            return
        if self._href is not None:
            self._href_text.append(data)
        self._text_sink().append(data)

    def unknown_decl(self, data: str) -> None:
        # <![CDATA[...]]>: code macro bodies and link bodies
        if data.startswith("CDATA["):
            self.handle_data(data[len("CDATA["):])

    def close(self) -> None:
        super().close()
        self._heading = self._heading if _clean("".join(self._buf)) else 0
        self._flush()


def textify(storage: str) -> list[Block]:
    parser = _Parser()
    parser.feed(storage or "")
    parser.close()
    blocks = parser.blocks
    # bare URLs typed as text count as links too (Drive links are often pasted, not linked)
    for b in blocks:
        known = {u for u, _ in b.links}
        for text in ([b.text] if b.kind != "table" else [c for r in b.rows or [] for c in r]):
            for m in _URL.finditer(text or ""):
                if m.group(0) not in known:
                    b.links.append((m.group(0), m.group(0)))
                    known.add(m.group(0))
    return blocks


def drive_links(links: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return [(u, t) for u, t in links if DRIVE_URL.match(u)]
