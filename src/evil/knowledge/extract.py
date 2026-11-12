"""Text out of document files, in memory: PDF, Word (.docx), PowerPoint (.pptx), plain text / Markdown.

Used by Drive reads (drive.py: live, nothing kept) and by Confluence attachments (indexer.py: indexed like page
text). The output is blocks (textify.Block), so the same credential scrub and chunker apply as for pages.
"""

from __future__ import annotations

import io
import re
import zipfile
from xml.etree import ElementTree

from evil.knowledge.textify import Block

PDF = "application/pdf"
DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
PPTX = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
TEXT = {"text/plain", "text/markdown"}
READABLE = {PDF, DOCX, PPTX, *TEXT}
LABEL = {PDF: "PDF", DOCX: "Word document", PPTX: "PowerPoint", "text/plain": "text file",
         "text/markdown": "Markdown file"}


def blocks_for(mime: str, data: bytes) -> list[Block]:
    """Blocks of a READABLE file (anything else: ValueError)."""
    if mime == PDF:
        return _lines_to_blocks(_pdf_text(data))
    if mime == DOCX:
        return _lines_to_blocks(_docx_text(data))
    if mime == PPTX:
        return _lines_to_blocks(_pptx_text(data))
    if mime in TEXT:
        return _lines_to_blocks(data.decode("utf-8", "replace"))
    raise ValueError(f"not a readable type: {mime}")


def _lines_to_blocks(text: str) -> list[Block]:
    """Markdown-ish text -> blocks: '#' headings (so a credentials section can be dropped whole), '|' tables, the
    rest as paragraphs split at blank lines."""
    blocks: list[Block] = []
    para: list[str] = []
    table: list[list[str]] = []

    def flush_para():
        if para:
            blocks.append(Block("para", "\n".join(para)))
            para.clear()

    def flush_table():
        if table:
            header = len(table) > 1 and all(re.fullmatch(r":?-{2,}:?", c.strip() or "--") for c in table[1])
            rows = [table[0]] + table[2:] if header else list(table)
            blocks.append(Block("table", rows=rows, header=header))
            table.clear()

    for raw in text.splitlines():
        line = raw.rstrip()
        m = re.match(r"^(#{1,6})\s+(.*)", line)
        if m:
            flush_para(); flush_table()
            blocks.append(Block("heading", m.group(2).strip("# ").strip(), level=len(m.group(1))))
        elif line.lstrip().startswith("|") and line.rstrip().endswith("|"):
            flush_para()
            table.append([c.strip() for c in line.strip().strip("|").split("|")])
        elif not line.strip():
            flush_para(); flush_table()
        else:
            flush_table()
            para.append(line)
    flush_para(); flush_table()
    return blocks


def _pdf_text(data: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    return "\n\n".join((page.extract_text() or "") for page in reader.pages)


def _pptx_text(data: bytes) -> str:
    """One '## Slide n' section per slide, its text boxes as lines (speaker notes are left out)."""
    ns = "{http://schemas.openxmlformats.org/drawingml/2006/main}"
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        names = [n for n in z.namelist() if re.fullmatch(r"ppt/slides/slide\d+\.xml", n)]
        number = {n: int(re.search(r"(\d+)", n.rsplit("/", 1)[1]).group(1)) for n in names}
        out = []
        for name in sorted(names, key=number.get):
            i = number[name]
            root = ElementTree.fromstring(z.read(name))
            lines = []
            for para in root.iter(f"{ns}p"):
                text = "".join(t.text or "" for t in para.iter(f"{ns}t")).strip()
                if text:
                    lines.append(text)
            if lines:
                out.append(f"## Slide {i}\n\n" + "\n".join(lines))
    return "\n\n".join(out)


def _docx_text(data: bytes) -> str:
    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        root = ElementTree.fromstring(z.read("word/document.xml"))
    lines = []
    for p in root.iter(f"{ns}p"):
        style = p.find(f"{ns}pPr/{ns}pStyle")
        text = "".join(t.text or "" for t in p.iter(f"{ns}t"))
        level = re.match(r"Heading(\d)", style.get(f"{ns}val", "")) if style is not None else None
        lines.append(("#" * int(level.group(1)) + " " + text) if level and text else text)
        if level:
            lines.append("")
    return "\n".join(lines)
