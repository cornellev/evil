"""GitHub in cev-knowledge (roadmap 3a / 3b, 11-19): the App client, the indexer and the live tools, against fakes."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from evil.knowledge import github as gh
from evil.knowledge import github_index as gi
from evil.knowledge import search as search_mod
from evil.knowledge import sql as sql_mod
from evil.knowledge import store

NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
REPO = {"name": "evil", "full_name": "cornellev/evil", "html_url": "https://github.com/cornellev/evil",
        "default_branch": "main", "private": True, "archived": False, "fork": False, "description": "Telemetry store",
        "language": "Python", "topics": ["telemetry"], "created_at": "2025-01-01T00:00:00Z",
        "pushed_at": "2026-11-18T00:00:00Z"}
FORK = dict(REPO, name="ros2", full_name="cornellev/ros2", fork=True, html_url="https://github.com/cornellev/ros2")


class FakeGitHub:
    """What github_index and the live tools ask GitHubClient for."""

    def __init__(self):
        self.requests_made = 0
        self.head_sha = "h1"
        self.files = {
            "README.md": b"# EVIL\n\nThe car's telemetry store.\n\n```bash\n# not a heading\nmake up\n```\n",
            "docs/setup.md": b"# Setup\n\nInstall docker. password: hunter2\n",
            "src/evil/knowledge/README.md": b"# Knowledge\n\nConfluence and Drive index.\n",
            "data/laps.csv": b"lap,wh\n1,82\n2,79\n",
            "src/evil/telemetry.py": b"def setup_tracing():\n    pass\n",
            "node_modules/x/README.md": b"# vendored\n",
            "package-lock.json": b"{}",
            "LICENSE.txt": b"MIT",
            "secrets/passwords.md": b"the router password is x",
        }
        self.shas = {p: f"s-{p}" for p in self.files}

    def repos(self):
        return iter([REPO, FORK])

    def head(self, full, branch):
        return {"sha": self.head_sha, "date": "2026-11-18T10:00:00Z", "tree": "t"}

    def tree(self, full, sha):
        return [{"path": p, "sha": self.shas[p], "size": len(b), "type": "blob"} for p, b in self.files.items()], False

    def file(self, full, path, ref=None):
        self.requests_made += 1
        if path not in self.files:
            raise gh.NotFound(path)
        return self.files[path]

    def last_commit(self, full, path, branch):
        return {"date": "2026-11-17T09:00:00Z", "author": "Ann Lee"}


def test_which_files_are_indexed():
    assert gi.readable("README.md", 10) and gi.readable("a/b/readme.rst", 10) and gi.readable("docs/x.md", 10)
    assert gi.readable("data/laps.csv", 10) and gi.readable("docs/api.yaml", 10) and gi.readable("notes.txt", 10)
    for path in ("config/robot.yaml", "chuds/examples/CMakeLists.txt", "Cargo.toml", "package.json", "pyproject.toml",
                 "requirements-dev.txt", ".github/copilot-instructions.md", "tsconfig.base.json"):
        assert not gi.readable(path, 10), path       # build / config manifests (11-19: they outranked real docs)
    assert not gi.readable("src/evil/telemetry.py", 10)                  # code: the live tools read it
    for path in ("node_modules/x/README.md", "build/notes.md", "package-lock.json", "LICENSE.txt", "app.min.json",
                 ".eslintrc.json"):
        assert not gi.readable(path, 10), path
    assert not gi.readable("big.md", gh.MAX_FILE_BYTES + 1) and not gi.readable("empty.md", 0)


def test_markdown_code_fences_stay_code():
    blocks = gi.to_blocks("README.md", b"# Title\n\ntext\n\n```\n# comment\n```\n")
    assert [b.kind for b in blocks] == ["heading", "para", "code"] and blocks[2].text == "# comment"
    assert gi.to_blocks("mac.csv", b"a,b\r1,2\r")[0].rows == [["a", "b"], ["1", "2"]]   # old Mac line endings
    table = gi.to_blocks("x.tsv", b"a\tb\n1\t2\n")[0]
    assert table.kind == "table" and table.rows == [["a", "b"], ["1", "2"]]


@pytest.fixture
def gdb(tmp_path):
    conn = store.connect(tmp_path / "k.db")
    yield conn
    conn.close()


def test_a_pass_indexes_repos_and_their_docs_and_scrubs(gdb):
    fake = FakeGitHub()
    r = gi.sync_github(gdb, fake, now=NOW)
    assert r.error is None and r.repos == 1 and r.forks_skipped == 1
    rows = {row["id"]: row for row in gdb.execute("SELECT * FROM pages WHERE source = 'github'")}
    assert set(rows) == {"gh:cornellev/evil", "gh:cornellev/evil:README.md", "gh:cornellev/evil:docs/setup.md",
                         "gh:cornellev/evil:src/evil/knowledge/README.md", "gh:cornellev/evil:data/laps.csv",
                         "gh:cornellev/evil:secrets/passwords.md"}
    readme = rows["gh:cornellev/evil:src/evil/knowledge/README.md"]
    assert readme["title"] == "evil/src/evil/knowledge/README.md" and readme["kind"] == "repo_doc"
    assert json.loads(readme["breadcrumb"]) == ["evil", "src", "evil", "knowledge"]
    assert readme["url"] == "https://github.com/cornellev/evil/blob/main/src/evil/knowledge/README.md"
    assert readme["last_editor"] == "Ann Lee" and readme["updated"].startswith("2026-11-17")
    assert rows["gh:cornellev/evil:secrets/passwords.md"]["kind"] == "credentials_link_only"
    text = " ".join(t for (t,) in gdb.execute("SELECT text FROM chunks WHERE page_id LIKE 'gh:%'"))
    assert "hunter2" not in text and "router password is" not in text and r.scrubbed >= 1
    assert "Telemetry store" in text and "Main language: Python" in text


def test_a_file_that_cant_be_parsed_is_a_link_not_a_failed_pass(gdb, monkeypatch):
    real = gi.to_blocks
    monkeypatch.setattr(gi, "to_blocks", lambda path, data: (_ for _ in ()).throw(ValueError("odd"))
                        if path.endswith(".csv") else real(path, data))
    r = gi.sync_github(gdb, FakeGitHub(), now=NOW)
    assert r.error is None
    row = gdb.execute("SELECT kind FROM pages WHERE id = 'gh:cornellev/evil:data/laps.csv'").fetchone()
    assert row["kind"] == "repo_doc"


def test_the_next_pass_reads_only_what_changed_and_drops_what_is_gone(gdb):
    fake = FakeGitHub()
    gi.sync_github(gdb, fake, now=NOW)
    again = gi.sync_github(gdb, fake, now=NOW)
    assert again.repos_unchanged == 1 and again.indexed == 0          # head didn't move: no tree, no reads
    fake.head_sha = "h2"
    fake.files["docs/setup.md"] = b"# Setup\n\nInstall podman.\n"
    fake.shas["docs/setup.md"] = "s2"
    del fake.files["data/laps.csv"]
    reads = fake.requests_made
    r = gi.sync_github(gdb, fake, now=NOW)
    assert (r.indexed, r.removed) == (1, 1) and fake.requests_made - reads == 1
    assert gdb.execute("SELECT count(*) FROM pages WHERE id = 'gh:cornellev/evil:data/laps.csv'").fetchone()[0] == 0


def test_repo_docs_rank_with_the_rest_and_sql_knows_the_repo(gdb):
    gi.sync_github(gdb, FakeGitHub(), now=NOW)
    gdb.commit()
    hits = search_mod.search(gdb, "knowledge index confluence drive")
    assert hits[0]["source"] == "github" and hits[0]["title"] == "evil/src/evil/knowledge/README.md"
    assert search_mod.search(gdb, "telemetry store", sources=["confluence"]) == []
    rows = sql_mod.run(gdb, "SELECT title, repo, private, kind, type FROM docs WHERE source = 'github' ORDER BY title")
    assert {"title": "evil", "repo": "evil", "private": 1, "kind": "repo", "type": "GitHub repo"} in rows
    assert any(r["kind"] == "repo_file" and r["repo"] == "evil" for r in rows)


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def _next(self, method, url, **kw):
        self.calls.append((method, url, kw))
        status, body = self.responses.pop(0)

        class R:
            status_code = status
            headers = {}
            content = body if isinstance(body, bytes) else json.dumps(body).encode()

            def json(self):
                return body
        return R()

    def post(self, url, **kw):
        return self._next("POST", url, **kw)

    def get(self, url, **kw):
        return self._next("GET", url, **kw)


def _client(responses, monkeypatch):
    c = gh.GitHubClient("1", "2", session=FakeSession(responses), retry_sec=0)
    monkeypatch.setattr(c, "_jwt", lambda: "jwt")
    return c


TOKEN = (201, {"token": "t1", "expires_at": "2099-01-01T00:00:00Z"})


def test_the_client_gets_a_token_once_retries_and_maps_404(monkeypatch):
    c = _client([TOKEN, (502, {}), (200, [1]), (404, {}), (500, {}), (500, {}), (500, {})], monkeypatch)
    assert c.get("/x") == [1]
    with pytest.raises(gh.NotFound):
        c.get("/y")
    with pytest.raises(gh.GitHubUnavailable):
        c.get("/z")
    methods = [m for m, _, _ in c._session.calls]
    assert methods.count("POST") == 1                                  # one installation token, cached
    assert c._session.calls[1][2]["headers"]["Authorization"] == "Bearer t1"


def test_an_empty_repo_is_not_found_not_a_failed_pass(monkeypatch):
    c = _client([TOKEN, (409, {"message": "Git Repository is empty."})], monkeypatch)
    with pytest.raises(gh.NotFound):
        c.head("cornellev/empty", "main")


def test_a_refused_app_is_unavailable_not_a_crash(monkeypatch):
    c = _client([(401, {})], monkeypatch)
    with pytest.raises(gh.GitHubUnavailable):
        c.get("/x")


# ---- the live tools (server.py) -------------------------------------------------------------------------------

from evil.knowledge import server as server_mod  # noqa: E402


class LiveFake(FakeGitHub):
    enabled = True

    def get(self, path, params=None, raw=False, accept=None):
        if path == "/repos/cornellev/evil":
            return {"html_url": REPO["html_url"], "private": True, "default_branch": "main"}
        if path.endswith("/commits"):
            return [{"sha": "abcdef123", "html_url": "u", "commit": {"committer": {"date": "2026-11-18T00:00:00Z"},
                                                                     "author": {"name": "Ann"},
                                                                     "message": "Fix CAN parser\n\nlong body"}}]
        if path.endswith("/pulls"):
            if params["state"] == "open":
                return [{"number": 7, "title": "Add BMS view", "user": {"login": "bo"}, "draft": False,
                         "updated_at": "2026-11-18", "html_url": "p7"}]
            return [{"number": 6, "title": "Old", "merged_at": "2026-01-01T00:00:00Z", "html_url": "p6"},
                    {"number": 5, "title": "New", "merged_at": "2099-01-01T00:00:00Z", "html_url": "p5"}]
        if path.endswith("/issues"):
            return [{"number": 8, "title": "Lap split wrong", "updated_at": "2026-11-17", "html_url": "i8"},
                    {"number": 7, "title": "a PR", "pull_request": {}, "updated_at": "x", "html_url": "p7"}]
        raise AssertionError(path)

    def get_text_matches(self, path, params):
        self.q = params["q"]
        return {"items": [{"repository": {"full_name": "cornellev/evil"}, "path": "src/a.py", "html_url": "h",
                           "text_matches": [{"fragment": "api_key = 'sk-abc123'"},
                                            {"fragment": "def decode(frame):\n    return parse_can(frame)"}]}]}


def _call(server, name, args):
    return asyncio.run(server.call_tool(name, args))


def _data(result):
    structured = getattr(result, "structured_content", None) or getattr(result, "structuredContent", None)
    if isinstance(structured, dict) and set(structured) == {"result"}:
        return structured["result"]
    return json.loads(result.content[0].text)


def test_live_tools_activity_code_search_and_reading_code(gdb, tmp_path):
    gi.sync_github(gdb, FakeGitHub(), now=NOW)
    gdb.commit()
    fake = LiveFake()
    server = server_mod.create_knowledge_server(str(tmp_path / "k.db"), github=fake)
    act = _data(_call(server, "repo_activity", {"repo": "evil"}))["repos"][0]
    assert act["commits"][0]["message"] == "Fix CAN parser" and act["open_pull_requests"][0]["number"] == 7
    assert [i["number"] for i in act["open_issues"]] == [8]              # the PR listed as an issue is left out
    assert [p["number"] for p in act["merged_pull_requests"]] == [5]     # merged inside the window only
    hits = _data(_call(server, "search_code", {"query": "parse_can"}))
    assert fake.q == "parse_can org:cornellev" and "sk-abc123" not in json.dumps(hits)
    assert "parse_can(frame)" in hits[0]["fragments"][0]
    doc = _data(_call(server, "get_document", {"source": "github", "id": "evil/README.md"}))
    assert doc["kind"] == "repo_doc" and "telemetry store" in doc["text"]          # from the index
    code = _data(_call(server, "get_document", {"source": "github", "id": "evil/src/evil/telemetry.py"}))
    assert code["kind"] == "repo_file" and "def setup_tracing" in code["text"]     # live: code isn't indexed
    url = _data(_call(server, "get_document", {"source": "github",
                                               "id": "https://github.com/cornellev/evil/blob/main/src/evil/telemetry.py"}))
    assert url["id"] == "gh:cornellev/evil:src/evil/telemetry.py"


def test_code_search_falls_back_to_the_repos_own_files_when_githubs_index_misses_it(tmp_path):
    import io
    import tarfile
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, body in (("evil-abc/src/evil/telemetry.py", b"import os\n\ndef setup_tracing(name):\n    pass\n"),
                           ("evil-abc/node_modules/x/a.js", b"setup_tracing()"), ("evil-abc/logo.png", b"setup_tracing")):
            info = tarfile.TarInfo(name)
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))

    class Missing(LiveFake):
        def get_text_matches(self, path, params):
            return {"items": []}

        def get(self, path, params=None, raw=False, accept=None):
            if path == "/repos/cornellev/evil":
                return {"html_url": REPO["html_url"], "default_branch": "main", "size": 900}
            if path == "/repos/cornellev/evil/tarball/main":
                return buf.getvalue()
            return super().get(path, params, raw, accept)
    server = server_mod.create_knowledge_server(str(tmp_path / "k.db"), github=Missing())
    hits = _data(_call(server, "search_code", {"query": "setup_tracing", "repo": "evil"}))
    assert [h["path"] for h in hits] == ["src/evil/telemetry.py"] and "def setup_tracing" in hits[0]["fragments"][0]
    note = _data(_call(server, "search_code", {"query": "setup_tracing"}))
    assert "repo set" in note[0]["note"]


def test_a_left_out_repo_is_never_indexed_or_read(gdb, tmp_path, monkeypatch):
    monkeypatch.setattr(gh, "SKIP_REPOS", frozenset({"evil"}))
    r = gi.sync_github(gdb, FakeGitHub(), now=NOW)
    assert r.repos == 0 and gdb.execute("SELECT count(*) FROM pages WHERE source = 'github'").fetchone()[0] == 0
    server = server_mod.create_knowledge_server(str(tmp_path / "k.db"), github=LiveFake())
    for tool, args in (("repo_activity", {"repo": "evil"}), ("get_document", {"source": "github", "id": "evil/README.md"}),
                       ("search_code", {"query": "x", "repo": "cornellev/evil"})):
        with pytest.raises(Exception, match="left out"):
            _call(server, tool, args)
    hits = _data(_call(server, "search_code", {"query": "parse_can"}))
    assert all(h.get("repo") != "cornellev/evil" for h in hits)   # org-wide hits in it are dropped


def test_one_repo_folder_cant_fill_the_results(gdb):
    fake = FakeGitHub()
    for name in ("a.md", "b.md", "c.md", "d.md"):
        fake.files[f"lib/chuds/{name}"] = b"# chuds\n\nchuds serialization notes\n"
        fake.shas[f"lib/chuds/{name}"] = name
    gi.sync_github(gdb, fake, now=NOW)
    gdb.commit()
    hits = search_mod.search(gdb, "chuds serialization", limit=5)
    assert sum(1 for h in hits if h["breadcrumb"][:3] == ["evil", "lib", "chuds"]) == 2


def test_activity_with_no_repo_names_every_repo_pushed(tmp_path):
    import re as re_mod

    class Many(LiveFake):
        def repos(self):
            return iter([dict(REPO, name=f"r{i}", full_name=f"cornellev/r{i}", html_url=f"u{i}",
                              pushed_at="2099-01-01T00:00:00Z") for i in range(12)])

        def get(self, path, params=None, raw=False, accept=None):       # every repo answers like evil
            return LiveFake.get(self, re_mod.sub(r"^/repos/cornellev/r\d+", "/repos/cornellev/evil", path), params)
    out = _data(_call(server_mod.create_knowledge_server(str(tmp_path / "k.db"), github=Many()), "repo_activity", {}))
    assert len(out["all_repos_pushed"]) == 12 and out["detailed"] == server_mod.MAX_ACTIVE_REPOS
