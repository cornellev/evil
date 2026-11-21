"""GitHub for cev-knowledge: the cornellev org's repos through a read-only GitHub App (roadmap phase 3, user 11-19).

- Access: the App `cev-evil-reader` (owned by the org, not a person), permissions Contents / Metadata / Pull
  requests / Issues: read, installed on all repos. Its private key (`secrets/github-app.pem` on the NUC, root-owned,
  mode 600) is mounted read-only into the indexer and the knowledge server, like the Drive key. The key signs a
  10-minute JWT; GitHub swaps it for an installation token (1 hour), refreshed before it runs out. Uninstalling the
  App or deleting its key cuts access at once.
- Used by github_index.py (repo docs and readable files into knowledge.db) and the live tools in server.py
  (repo_activity, read_repo_file, search_code). Read-only: only GET requests, and the search endpoint.
- Down, rate-limited or slow: GitHubUnavailable; the indexer keeps what it has and tries next pass.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Iterator
from urllib.parse import quote

logger = logging.getLogger("evil.knowledge.github")

API = "https://api.github.com"
DEFAULT_KEY_FILE = os.getenv("KNOWLEDGE_GITHUB_KEY_FILE", "/run/secrets/knowledge/github-app.pem")
TIMEOUT_SEC = float(os.getenv("KNOWLEDGE_GITHUB_TIMEOUT_SEC", "15"))
RETRIES = 3
BACKOFF_SEC = 5.0
TOKEN_MARGIN_SEC = 300          # refresh the installation token this long before it expires
PER_PAGE = 100
# Repos left out of everything (index and live tools), by name, comma-separated (user 11-19: CEVWiki, an old wiki
# that overlaps Confluence).
SKIP_REPO_NAMES = tuple(n.strip() for n in os.getenv("KNOWLEDGE_GITHUB_SKIP_REPOS", "").split(",") if n.strip())
SKIP_REPOS = frozenset(n.lower() for n in SKIP_REPO_NAMES)


def left_out_note() -> str:
    """For the tool descriptions: the left-out repos exist but can't be read (seen 11-21: "what's in CEVWiki" got
    "I couldn't find a repo named CEVWiki")."""
    if not SKIP_REPO_NAMES:
        return ""
    return (f" Left out on purpose (they exist on GitHub but nothing here can read them): {', '.join(SKIP_REPO_NAMES)}."
            " Asked about one, say it's left out of what you can read; don't say it doesn't exist.")


def skipped(full_name: str) -> bool:
    return full_name.split("/")[-1].lower() in SKIP_REPOS


MAX_FILE_BYTES = int(os.getenv("KNOWLEDGE_GITHUB_MAX_BYTES", str(200 * 1024)))
_HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "cev-knowledge"}


class GitHubUnavailable(Exception):
    """GitHub refused or didn't answer (rate limit, outage, revoked App)."""


class NotFound(Exception):
    pass


class GitHubClient:
    def __init__(self, app_id: str | None = None, installation_id: str | None = None,
                 key_file: str = DEFAULT_KEY_FILE, session: Any = None, retry_sec: float = BACKOFF_SEC):
        self.app_id = app_id if app_id is not None else os.getenv("KNOWLEDGE_GITHUB_APP_ID", "")
        self.installation_id = (installation_id if installation_id is not None
                                else os.getenv("KNOWLEDGE_GITHUB_INSTALLATION_ID", ""))
        self.key_file = key_file
        self._session = session
        self.retry_sec = retry_sec
        self._token: str | None = None
        self._token_until = 0.0
        self._lock = threading.Lock()
        self.requests_made = 0

    @property
    def enabled(self) -> bool:
        return bool(self.app_id and self.installation_id and (self._session is not None or
                                                               os.path.exists(self.key_file)))

    def _sess(self):
        if self._session is None:
            import requests
            self._session = requests.Session()
        return self._session

    def _jwt(self) -> str:
        from google.auth import crypt, jwt         # google-auth is here for Drive already: RS256 without a new dep
        with open(self.key_file, "rb") as f:
            signer = crypt.RSASigner.from_string(f.read())
        now = int(time.time())
        token = jwt.encode(signer, {"iat": now - 60, "exp": now + 540, "iss": str(self.app_id)},
                           header={"alg": "RS256", "typ": "JWT"})
        return token.decode() if isinstance(token, bytes) else token

    def _installation_token(self) -> str:
        with self._lock:
            if self._token and time.time() < self._token_until - TOKEN_MARGIN_SEC:
                return self._token
            r = self._sess().post(f"{API}/app/installations/{self.installation_id}/access_tokens",
                                  headers={**_HEADERS, "Authorization": f"Bearer {self._jwt()}"}, timeout=TIMEOUT_SEC)
            self.requests_made += 1
            if r.status_code != 201:
                raise GitHubUnavailable(f"installation token refused ({r.status_code})")
            body = r.json()
            self._token = body["token"]
            from datetime import datetime
            self._token_until = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00")).timestamp()
            return self._token

    def get(self, path: str, params: dict | None = None, raw: bool = False, accept: str | None = None) -> Any:
        """GET with the installation token; JSON, or bytes when raw. NotFound on 404; GitHubUnavailable after
        retries on rate limits, 5xx and timeouts."""
        url = path if path.startswith("http") else f"{API}{path}"
        headers = {**_HEADERS, "Authorization": f"Bearer {self._installation_token()}"}
        if raw:
            headers["Accept"] = "application/vnd.github.raw+json"
        elif accept:
            headers["Accept"] = accept
        last = ""
        for attempt in range(RETRIES):
            try:
                r = self._sess().get(url, params=params, headers=headers, timeout=TIMEOUT_SEC)
            except Exception as exc:              # timeouts, resets
                last = f"{type(exc).__name__}: {exc}"
            else:
                self.requests_made += 1
                if r.status_code in (404, 409):          # 409: an empty repo has no commits (seen 11-19)
                    raise NotFound(path)
                if r.status_code < 300:
                    return r.content if raw else r.json()
                remaining = r.headers.get("X-RateLimit-Remaining")
                last = f"{r.status_code}" + (" (rate limit)" if remaining == "0" or r.status_code == 429 else "")
                if r.status_code in (401,):       # the token went bad early: get a new one once
                    self._token = None
                    headers["Authorization"] = f"Bearer {self._installation_token()}"
                elif r.status_code < 500 and r.status_code not in (403, 429):
                    raise GitHubUnavailable(f"GET {path}: {last}")
            if attempt + 1 < RETRIES:
                time.sleep(self.retry_sec * 2 ** attempt)
        raise GitHubUnavailable(f"GET {path}: {last}")

    def graphql(self, query: str, variables: dict) -> dict:
        """One GraphQL query (blame has no REST endpoint). Read-only: queries only, never mutations."""
        if query.lstrip().lower().startswith("mutation"):
            raise ValueError("read-only: no mutations")
        headers = {**_HEADERS, "Authorization": f"Bearer {self._installation_token()}"}
        last = ""
        for attempt in range(RETRIES):
            try:
                r = self._sess().post(f"{API}/graphql", json={"query": query, "variables": variables},
                                      headers=headers, timeout=TIMEOUT_SEC)
            except Exception as exc:
                last = f"{type(exc).__name__}: {exc}"
            else:
                self.requests_made += 1
                if r.status_code < 300:
                    body = r.json()
                    if body.get("errors"):
                        msg = "; ".join(str(e.get("message")) for e in body["errors"])[:200]
                        if "Could not resolve" in msg or "NOT_FOUND" in str(body["errors"]):
                            raise NotFound(msg)
                        raise GitHubUnavailable(f"GraphQL: {msg}")
                    return body.get("data") or {}
                last = str(r.status_code)
                if r.status_code < 500 and r.status_code not in (403, 429):
                    raise GitHubUnavailable(f"GraphQL: {last}")
            if attempt + 1 < RETRIES:
                time.sleep(self.retry_sec * 2 ** attempt)
        raise GitHubUnavailable(f"GraphQL: {last}")

    def get_text_matches(self, path: str, params: dict) -> Any:
        """A search with the matching fragments (code search)."""
        return self.get(path, params, accept="application/vnd.github.text-match+json")

    def pages(self, path: str, params: dict | None = None, key: str | None = None) -> Iterator[dict]:
        """Every item of a paginated list (`key`: the list's field when the body is an object)."""
        page = 1
        while True:
            body = self.get(path, {**(params or {}), "per_page": PER_PAGE, "page": page})
            items = body.get(key, []) if key else body
            yield from items
            if len(items) < PER_PAGE:
                return
            page += 1

    # ---- what the indexer and the live tools use -----------------------------------------------------------------

    def repos(self) -> Iterator[dict]:
        return (r for r in self.pages("/installation/repositories", key="repositories") if not skipped(r["full_name"]))

    def head(self, full_name: str, branch: str) -> dict:
        """The default branch's head commit: {sha, date, tree}."""
        c = self.get(f"/repos/{full_name}/commits/{quote(branch, safe='')}")
        return {"sha": c["sha"], "date": c["commit"]["committer"]["date"], "tree": c["commit"]["tree"]["sha"]}

    def tree(self, full_name: str, tree_sha: str) -> tuple[list[dict], bool]:
        """(every file in the tree: path, sha, size), and whether GitHub cut the listing short (huge repos)."""
        t = self.get(f"/repos/{full_name}/git/trees/{tree_sha}", {"recursive": "1"})
        return [e for e in t.get("tree", []) if e.get("type") == "blob"], bool(t.get("truncated"))

    def file(self, full_name: str, path: str, ref: str | None = None) -> bytes:
        return self.get(f"/repos/{full_name}/contents/{quote(path)}", {"ref": ref} if ref else None, raw=True)

    def last_commit(self, full_name: str, path: str, branch: str) -> dict | None:
        """The newest commit that touched `path`: {date, author} (the author's name as written in the commit)."""
        items = self.get(f"/repos/{full_name}/commits", {"path": path, "sha": branch, "per_page": 1})
        if not items:
            return None
        c = items[0]["commit"]
        return {"date": c["committer"]["date"], "author": (c.get("author") or {}).get("name")}
