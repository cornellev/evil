"""The cornellev GitHub org -> knowledge.db (the indexer's third job; roadmap phase 3a, user 11-19: "indexing readmes
and nested readmes, and other readable files like reasonable sized txt/csv").

- **One row per repo** (kind `repo`): name, description, language, topics, default branch, last push, private /
  archived, so "which repos are about CAN" and "what did we touch this month" have an answer.
- **One row per readable file** (kind `repo_doc`): every README at every level, and Markdown / reStructuredText /
  text / CSV / TSV / JSON / YAML / TOML files up to github.MAX_FILE_BYTES (200 KB). Title `<repo>/<path>`, folder path
  = [repo, dirs...], so a nested README ranks by where it sits; url = the file on GitHub at the default branch;
  modified / last editor = the last commit that touched it. Same credential scrub and chunker as Confluence / Drive.
- **Left out:** source code (3b's live tools read it), vendored / generated / build folders, lockfiles, minified
  files, licenses, anything over the size cap, forks (copies of other people's projects, not team docs), and the
  repos in KNOWLEDGE_GITHUB_SKIP_REPOS (CEVWiki, user 11-19).
- **Incremental:** per repo, the default branch's head commit. When it moved, the tree says which files changed (blob
  sha); only those are read. A full pass (nightly with Confluence and Drive, or after GITHUB_VERSION / the chunker
  changes) re-lists everything and removes repos and files that are gone.

Ranking starts plain (user 11-19: measure with probe questions first, then tune; roadmap 3a lists the candidate
fixes). Logs carry repo names, paths and counts, never file text.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath

from evil.knowledge import store
from evil.knowledge.chunker import CHUNKER_VERSION, LINK_ONLY_KIND, Chunk, chunk
from evil.knowledge.extract import _lines_to_blocks
from evil.knowledge.filters import CREDENTIAL_TITLE, scrub
from evil.knowledge.github import MAX_FILE_BYTES, GitHubClient, GitHubUnavailable, NotFound
from evil.knowledge.github import skipped as gh_skipped
from evil.knowledge.textify import Block

logger = logging.getLogger("evil.knowledge.github_index")

GITHUB_VERSION = "2"        # bump to re-read every repo file on the next pass (2: build / config manifests dropped)
BUDGET_SEC = float(os.getenv("KNOWLEDGE_GITHUB_BUDGET_SEC", "900"))
MAX_ROWS = 300              # CSV / TSV rows kept, like Drive sheets
README = re.compile(r"^readme(\.[a-z0-9]+)?$", re.I)
TEXT_EXT = {".md", ".markdown", ".rst", ".txt", ".csv", ".tsv", ".json", ".yaml", ".yml", ".toml"}
SKIP_DIRS = {"node_modules", "vendor", "vendors", "third_party", "third-party", "thirdparty", "external", "build",
             "dist", "out", "target", ".venv", "venv", "env", "__pycache__", "site-packages", ".next", ".nuxt",
             "coverage", ".tox", ".mypy_cache", ".pytest_cache", "bower_components", ".git", ".idea", ".vscode"}
LOCKFILES = {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "poetry.lock", "cargo.lock", "uv.lock",
             "pipfile.lock", "composer.lock", "gemfile.lock", "go.sum", "flake.lock", "npm-shrinkwrap.json"}
NOISE = re.compile(r"(\.min\.\w+$|^(license|licence|copying|notice)(\.\w+)?$|^\.)", re.I)
# Build and config manifests carry the folder's name in their path but say nothing a person asks about (seen 11-19:
# three chuds CMakeLists.txt files outranked the Confluence CHUDS page and became the answer's sources).
MANIFESTS = re.compile(r"^(cmakelists\.txt|requirements([-_.].*)?\.txt|constraints\.txt|.*\.cmake|"
                       r"(cargo|pyproject|poetry|setup|tsconfig|jsconfig|package|composer|deno|bunfig|"
                       r"\.?eslintrc|\.?prettierrc|renovate|dependabot|codecov|mkdocs|netlify|vercel|"
                       r"docker-compose|compose|pnpm-workspace|turbo|nx|lerna|angular|firebase)([-_.].*)?\."
                       r"(json|toml|ya?ml|cfg|txt))$", re.I)
CONFIG_EXT = {".json", ".yaml", ".yml", ".toml"}      # kept only in docs folders: elsewhere they are config
DOC_DIRS = {"docs", "doc", "documentation", "wiki", "guides", "manual"}


@dataclass
class GitHubResult:
    mode: str
    repos: int = 0
    forks_skipped: int = 0
    repos_unchanged: int = 0
    files: int = 0
    indexed: int = 0
    unchanged: int = 0
    removed: int = 0
    scrubbed: int = 0
    truncated: list | None = None      # repos whose tree GitHub cut short
    requests: int = 0
    error: str | None = None


def state() -> str:
    return f"g{GITHUB_VERSION}-{CHUNKER_VERSION}"


def readable(path: str, size: int | None) -> bool:
    """A file worth indexing: a README anywhere, or a docs / text / data file, outside vendored and build folders."""
    p = PurePosixPath(path)
    if any(part.lower() in SKIP_DIRS or part.startswith(".") for part in p.parts[:-1]):   # .github/, .devcontainer/
        return False
    name = p.name.lower()
    if name in LOCKFILES or (size or 0) > MAX_FILE_BYTES or not size:
        return False
    if README.match(p.name):
        return True
    if NOISE.search(p.name) or MANIFESTS.match(p.name):
        return False
    if p.suffix.lower() in CONFIG_EXT:
        return any(part.lower() in DOC_DIRS for part in p.parts[:-1])
    return p.suffix.lower() in TEXT_EXT


def repo_id(full_name: str) -> str:
    return f"gh:{full_name}"


def file_id(full_name: str, path: str) -> str:
    return f"gh:{full_name}:{path}"


def _code_aware_blocks(text: str) -> list[Block]:
    """Markdown -> blocks with fenced code kept as code (a '# comment' in a shell block is not a heading)."""
    out: list[Block] = []
    plain: list[str] = []
    code: list[str] | None = None
    for line in text.splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            if code is None:
                out += _lines_to_blocks("\n".join(plain))
                plain, code = [], []
            else:
                out.append(Block("code", "\n".join(code)))
                code = None
            continue
        (plain if code is None else code).append(line)
    if code is not None:
        out.append(Block("code", "\n".join(code)))
    out += _lines_to_blocks("\n".join(plain))
    return [b for b in out if b.kind == "table" or b.text.strip()]


def to_blocks(path: str, data: bytes) -> list[Block]:
    text = data.decode("utf-8", "replace").replace("\r\n", "\n").replace("\r", "\n")
    ext = PurePosixPath(path).suffix.lower()
    if ext in (".csv", ".tsv"):
        rows = list(csv.reader(io.StringIO(text), delimiter="\t" if ext == ".tsv" else ","))
        rows = [r for r in rows if any(c.strip() for c in r)][:MAX_ROWS]
        return [Block("table", rows=rows, header=True)] if rows else []
    if ext in (".json", ".yaml", ".yml", ".toml"):
        return [Block("code", text.strip())] if text.strip() else []
    return _code_aware_blocks(text)


def about_repo(repo: dict, head: dict | None) -> str:
    bits = [f"GitHub repository {repo['full_name']}" + (" (private)" if repo.get("private") else "") +
            (" (archived)" if repo.get("archived") else "") + "."]
    if repo.get("description"):
        bits.append(repo["description"].strip().rstrip(".") + ".")
    if repo.get("language"):
        bits.append(f"Main language: {repo['language']}.")
    if repo.get("topics"):
        bits.append("Topics: " + ", ".join(repo["topics"]) + ".")
    bits.append(f"Default branch: {repo.get('default_branch')}.")
    if head:
        bits.append(f"Last commit: {head['date'][:10]}.")
    return " ".join(bits)


def _write_repo(conn, repo: dict, head: dict | None, now: datetime) -> None:
    labels = [x for x, on in (("private", repo.get("private")), ("archived", repo.get("archived"))) if on]
    store.upsert_page(
        conn, page_id=repo_id(repo["full_name"]), title=repo["name"], url=repo["html_url"], space="github",
        breadcrumb=[], updated=(head or {}).get("date") or repo.get("pushed_at") or "", version=0, labels=labels,
        kind="repo", chunker_version=state(), indexed_at=now.isoformat(),
        chunks=[Chunk(0, "", about_repo(repo, head), [])], source="github", created=repo.get("created_at"),
        sha=(head or {}).get("sha"))


def _write_file(conn, client: GitHubClient, repo: dict, entry: dict, now: datetime, result: GitHubResult) -> None:
    full, path, branch = repo["full_name"], entry["path"], repo["default_branch"]
    p = PurePosixPath(path)
    title = f"{repo['name']}/{path}"
    url = f"{repo['html_url']}/blob/{branch}/{path}"
    labels = ["private"] if repo.get("private") else []
    last = client.last_commit(full, path, branch)
    common = dict(page_id=file_id(full, path), title=title, url=url, space="github",
                  breadcrumb=[repo["name"], *p.parts[:-1]], updated=(last or {}).get("date") or "", version=0,
                  labels=labels, chunker_version=state(), indexed_at=now.isoformat(), source="github",
                  last_editor=(last or {}).get("author"), mime_type=None, size=entry.get("size"), sha=entry["sha"])
    if CREDENTIAL_TITLE.search(p.name):          # like the Confluence password pages: a pointer, never read
        store.upsert_page(conn, kind=LINK_ONLY_KIND, chunks=[Chunk(0, "", "", [])], **common)
        return
    clean, stats = scrub(to_blocks(path, client.file(full, path, branch)))
    if stats.total:
        result.scrubbed += stats.total
        logger.info("github %s %s: removed %d credential item(s)", full, path, stats.total)
    note = Block("para", f"File {path} in the GitHub repository {full}" + (" (private)" if repo.get("private") else "")
                 + ".")
    chunks = chunk([note, *clean]) if chunk(clean) else [Chunk(0, "", note.text, [])]
    store.upsert_page(conn, kind="repo_doc", chunks=chunks, **common)


def _write_unreadable(conn, repo: dict, entry: dict, now: datetime) -> None:
    full, path = repo["full_name"], entry["path"]
    store.upsert_page(
        conn, page_id=file_id(full, path), title=f"{repo['name']}/{path}",
        url=f"{repo['html_url']}/blob/{repo['default_branch']}/{path}", space="github",
        breadcrumb=[repo["name"], *PurePosixPath(path).parts[:-1]], updated="", version=0,
        labels=["private"] if repo.get("private") else [], kind="repo_doc", chunker_version=state(),
        indexed_at=now.isoformat(), source="github", size=entry.get("size"), sha=entry["sha"],
        chunks=[Chunk(0, "", f"File {path} in the GitHub repository {full}. Link only: not readable as text.", [])])


def _sync_repo(conn, client: GitHubClient, repo: dict, full: bool, now: datetime, deadline: float,
               result: GitHubResult) -> bool:
    """One repo; False when the time budget ran out (the head isn't recorded, so the next pass carries on)."""
    name = repo["full_name"]
    try:
        head = client.head(name, repo["default_branch"])
    except NotFound:                              # an empty repo: no commits yet
        head = None
    _write_repo(conn, repo, head, now)
    key = f"github_head:{name}"
    if head is None or (not full and store.get_meta(conn, key) == head["sha"]):
        result.repos_unchanged += 1
        return True
    entries, cut = client.tree(name, head["tree"])
    if cut:
        result.truncated = (result.truncated or []) + [name]
        logger.warning("github %s: tree listing truncated by GitHub, indexing what was listed", name)
    wanted = {file_id(name, e["path"]): e for e in entries if readable(e["path"], e.get("size"))}
    have = {r["id"]: (r["sha"], r["chunker_version"]) for r in conn.execute(
        "SELECT id, sha, chunker_version FROM pages WHERE source = 'github' AND kind != 'repo' AND id LIKE ? ESCAPE '\\'",
        (repo_id(name).replace("%", "\\%").replace("_", "\\_") + ":%",))}
    result.files += len(wanted)
    for fid, e in wanted.items():
        if have.get(fid) == (e["sha"], state()):
            result.unchanged += 1
            continue
        if time.monotonic() > deadline:
            conn.commit()
            return False
        try:
            _write_file(conn, client, repo, e, now, result)
        except (GitHubUnavailable, NotFound):
            raise
        except Exception as exc:       # one odd file (seen 11-19: a CSV the parser refused) never stops the pass
            logger.info("github %s %s: not readable as text (%s)", name, e["path"], type(exc).__name__)
            _write_unreadable(conn, repo, e, now)
        result.indexed += 1
    for gone in set(have) - set(wanted):
        store.delete_page(conn, gone)
        result.removed += 1
    store.set_meta(conn, key, head["sha"])
    conn.commit()
    return True


def sync_github(conn, client: GitHubClient, full: bool | None = None, now: datetime | None = None,
                budget_sec: float = BUDGET_SEC) -> GitHubResult:
    now = now or datetime.now(timezone.utc)
    if full is None:
        full = store.get_meta(conn, "github_version") != state()
    result = GitHubResult(mode="full" if full else "incremental")
    before = client.requests_made
    deadline = time.monotonic() + budget_sec
    try:
        repos = list(client.repos())
        listed = set()
        finished = True
        for repo in sorted(repos, key=lambda r: r.get("pushed_at") or "", reverse=True):   # active repos first
            if repo.get("fork") or gh_skipped(repo["full_name"]):
                result.forks_skipped += 1
                continue
            listed.add(repo_id(repo["full_name"]))
            result.repos += 1
            if not _sync_repo(conn, client, repo, full, now, deadline, result):
                finished = False
                break
        if finished:
            # a repo no longer installed (or now a fork) goes with its files
            known = {r[0] for r in conn.execute("SELECT id FROM pages WHERE source = 'github' AND kind = 'repo'")}
            for gone in known - listed:
                for (fid,) in conn.execute("SELECT id FROM pages WHERE source = 'github' AND id LIKE ?",
                                           (gone + ":%",)).fetchall():
                    store.delete_page(conn, fid)
                store.delete_page(conn, gone)
                result.removed += 1
            store.set_meta(conn, "github_version", state())
        store.set_meta(conn, "github_last_success", now.isoformat())
        conn.commit()
    except (GitHubUnavailable, NotFound, OSError, ValueError) as exc:     # keeps what's indexed
        conn.rollback()
        result.error = f"{type(exc).__name__}: {exc}"
        logger.error("github pass failed: %s", result.error)
    result.requests = client.requests_made - before
    return result
