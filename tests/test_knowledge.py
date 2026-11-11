"""cev-knowledge phase 1: Confluence client, parsing, filters, chunking, indexer passes, search, MCP server.

A fake Confluence (FakeConfluence) answers the REST search the client makes, so every pass runs the real code
path without a network or a token.
"""

from __future__ import annotations

import asyncio
import json
import urllib.parse
from datetime import datetime, timedelta, timezone

import pytest

from evil.knowledge import store
from evil.knowledge.chunker import CHUNKER_VERSION, chunk, page_kind
from evil.knowledge.confluence import BROWSER_USER_AGENT, ConfluenceClient, ConfluenceError, parse_page
from evil.knowledge.filters import mentions_credential, scrub, skip_reason
from evil.knowledge.indexer import _full_due, index_once
from evil.knowledge.search import drive_file_id, fts_query, get_document, search
from evil.knowledge.textify import textify

BASE = "https://confluence.example.edu"
DRIVE_URL = "https://docs.google.com/presentation/d/1AbCdEfGhIjKlMnOpQrStUvWxYz012345/edit"


def content(pid, title, body, *, version=1, labels=(), ancestors=(), restricted=False, space="CEV",
            when="2026-11-01T10:00:00.000-04:00"):
    return {
        "id": str(pid), "type": "page", "title": title,
        "space": {"key": space},
        "version": {"number": version, "when": when, "by": {"displayName": "Someone"}},
        "metadata": {"labels": {"results": [{"name": l} for l in labels]}},
        "ancestors": [{"id": str(a), "title": f"Parent {a}"} for a in ancestors],
        "restrictions": {"read": {"restrictions": {
            "user": {"results": [{"username": "x"}] if restricted else []},
            "group": {"results": []}}}},
        "body": {"storage": {"value": body}},
        "_links": {"webui": f"/display/CEV/{title.replace(' ', '+')}"},
    }


class FakeConfluence:
    """Answers /rest/api/content/search with the current pages, paginated; records every request."""

    def __init__(self, pages=None, page_size=2):
        self.pages = {p["id"]: p for p in (pages or [])}
        self.page_size = page_size
        self.requests = []

    def set(self, page):
        self.pages[page["id"]] = page

    def remove(self, pid):
        self.pages.pop(str(pid), None)

    def __call__(self, url, headers):
        self.requests.append((url, headers))
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        start, limit = int(q["start"][0]), int(q["limit"][0])
        items = list(self.pages.values())
        if "lastmodified >=" in q["cql"][0]:
            since = q["cql"][0].split('lastmodified >= "')[1].split('"')[0]
            since_dt = datetime.strptime(since, "%Y/%m/%d %H:%M")
            items = [p for p in items if datetime.fromisoformat(p["version"]["when"]).replace(tzinfo=None) >= since_dt]
        chunk_ = items[start:start + limit]
        links = {"next": "/next"} if start + limit < len(items) else {}
        return 200, json.dumps({"results": chunk_, "size": len(chunk_), "_links": links}).encode()


def client_for(fake):
    return ConfluenceClient(BASE, "secret-token", fetch=fake, page_size=fake.page_size, backoff_sec=0)


JOULEMETER = f"""<h1>Joulemeter</h1><p>The joulemeter measures the energy used during a race, sampled at 10 Hz.
It logs voltage and current from the main battery so the team can score energy per lap at competition.</p>
<p>Design reviewed at the <a href="{DRIVE_URL}">PDR deck</a>.</p>
<h2>Specs</h2><table><tbody><tr><th>Item</th><th>Value</th></tr><tr><td>Voltage range</td><td>0-60 V</td></tr></tbody></table>
<p>Shunt resistor rated for 100 A continuous; calibrated against a bench supply before every competition.</p>"""

WORK_LOG = "".join(
    f"<h2>2026-10-{d:02d}</h2><p>Worked on the joulemeter wiring and the energy logging script today. "
    "Tested the board, fixed a loose connector, wrote notes for the next session and checked the battery.</p>"
    for d in range(1, 6))

SECRETS = """<h1>Ordering Guide</h1><p>Order parts through the department account. Ask the lead before ordering
anything over 200 dollars; include a link and the quantity in the request form.</p>
<p>Username &amp; Password</p><p>cevorders / Hunter2!</p>
<table><tbody><tr><th>Vendor</th><th>Login</th><th>Password</th></tr><tr><td>DigiKey</td><td>cev</td><td>pw1</td></tr></tbody></table>
<p>Use https://user:pa55word@vendor.example.com/api for the API.</p>"""


# ---------------------------------------------------------------- client

def test_client_paginates_sends_browser_ua_and_bearer_token():
    fake = FakeConfluence([content(i, f"Page {i}", "<p>x</p>") for i in range(5)], page_size=2)
    pages = list(client_for(fake).pages("cev"))
    assert [p.id for p in pages] == ["0", "1", "2", "3", "4"]
    assert len(fake.requests) == 3
    url, headers = fake.requests[0]
    assert headers["User-Agent"] == BROWSER_USER_AGENT
    assert headers["Authorization"] == "Bearer secret-token"
    cql = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["cql"][0]
    assert cql.startswith('space = "cev" AND type = page')
    assert "restrictions.read.restrictions.user" in urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["expand"][0]


def test_client_incremental_cql_has_date_and_errors_name_waf_challenge():
    fake = FakeConfluence([])
    list(client_for(fake).pages("cev", datetime(2026, 11, 7, 9, 5)))
    cql = urllib.parse.parse_qs(urllib.parse.urlparse(fake.requests[0][0]).query)["cql"][0]
    assert 'lastmodified >= "2026/11/07 09:05"' in cql

    c = ConfluenceClient(BASE, "t", fetch=lambda u, h: (202, b""), retries=2, backoff_sec=0)
    with pytest.raises(ConfluenceError, match="WAF"):
        list(c.pages("cev"))


def test_client_only_issues_get_requests():
    """Read-only by construction: the module has no other HTTP method anywhere."""
    import inspect
    from evil.knowledge import confluence
    src = inspect.getsource(confluence)
    assert 'method="GET"' in src
    for verb in ("POST", "PUT", "DELETE", "PATCH"):
        assert f'method="{verb}"' not in src


def test_parse_page_reads_restrictions_labels_ancestors():
    p = parse_page(content(7, "T", "<p>x</p>", labels=["no-tern"], ancestors=[1, 2], restricted=True), BASE)
    assert p.restricted and p.labels == ["no-tern"] and [a for a, _ in p.ancestors] == ["1", "2"]
    assert p.url == f"{BASE}/display/CEV/T" and p.space == "cev"


# ---------------------------------------------------------------- textify

def test_textify_headings_tables_code_links():
    blocks = textify(
        '<h2>Wiring</h2><p>See <a href="https://example.com">site</a>.</p>'
        '<ac:structured-macro ac:name="code"><ac:parameter ac:name="language">py</ac:parameter>'
        '<ac:plain-text-body><![CDATA[x = 1\ny = 2]]></ac:plain-text-body></ac:structured-macro>'
        '<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td><p>two</p></td></tr></table>'
        '<p><ac:link><ri:page ri:content-title="BMS Architecture"/></ac:link></p>')
    kinds = [b.kind for b in blocks]
    assert kinds == ["heading", "para", "code", "table", "para"]
    assert blocks[0].level == 2 and blocks[1].links == [("https://example.com", "site")]
    assert blocks[2].text == "x = 1\ny = 2"           # code kept verbatim, macro parameter dropped
    assert blocks[3].rows == [["A", "B"], ["1", "two"]] and blocks[3].header
    assert blocks[4].text == "BMS Architecture"


def test_textify_finds_pasted_drive_urls():
    blocks = textify(f"<p>deck: {DRIVE_URL}</p>")
    assert (DRIVE_URL, DRIVE_URL) in blocks[0].links


# ---------------------------------------------------------------- filters

def test_scrub_removes_credentials_but_keeps_pinouts_and_prose():
    blocks, stats = scrub(textify(SECRETS))
    text = "\n".join(b.render() for b in blocks)
    assert "Hunter2" not in text and "cevorders" not in text      # label line + the value line after it
    assert "pw1" not in text and "[table removed: credentials]" in text
    assert "pa55word" not in text
    assert "Order parts through the department account" in text
    assert stats.total >= 3

    kept, _ = scrub(textify("<p>Pin 1: VCC</p><p>Pass 2: tighter line through Turn 3</p><p>log in to RED</p>"))
    assert [b.text for b in kept] == ["Pin 1: VCC", "Pass 2: tighter line through Turn 3", "log in to RED"]


def test_scrub_drops_credential_sections_and_key_shaped_strings():
    blocks, stats = scrub(textify(
        "<h2>Passwords</h2><p>nuc: abc</p><h3>more</h3><p>still secret area</p>"
        "<h2>Wiring</h2><p>token ghp_abcdefghijklmnopqrstuvwxyz0123456789 here</p>"))
    text = " ".join(b.render() for b in blocks)
    assert "nuc: abc" not in text and "still secret area" not in text
    assert "Wiring" in text and "ghp_" not in text and "[redacted]" in text
    assert stats.sections == 1 and stats.keys == 1


def test_key_redaction_leaves_drive_ids_in_urls():
    blocks, _ = scrub(textify(f"<p>{DRIVE_URL}</p>"))
    assert blocks[0].text == DRIVE_URL


def test_mentions_credential_weak_words_need_a_value():
    assert mentions_credential("Login: cev") and not mentions_credential("how to log in to the dashboard")
    assert mentions_credential("Login", header=True) and mentions_credential("the API key")


def test_skip_reason_restrictions_inherit_and_labels():
    def page(**kw):
        return parse_page(content(kw.pop("pid", 5), kw.pop("title", "Ok"), "", **kw), BASE)
    assert skip_reason(page(restricted=True), set(), "cev") == "restricted"
    assert skip_reason(page(ancestors=[1, 2]), {"2"}, "cev") == "restricted ancestor"
    assert skip_reason(page(labels=["No-Tern"]), set(), "cev") == "no-tern label"
    assert skip_reason(page(title="Software Important Usernames and Passwords"), set(), "cev") == "credentials title"
    assert skip_reason(page(space="OTHER"), set(), "cev") == "other space"
    assert skip_reason(page(), set(), "cev") is None


# ---------------------------------------------------------------- chunker

def test_chunker_splits_by_heading_merges_small_and_splits_large():
    para = "<p>" + " ".join(["word"] * 100) + "</p>"
    chunks = chunk(textify("<h1>A</h1><p>tiny</p><h2>B</h2>" + para + "<h2>C</h2>" + para * 9))
    assert chunks[0].heading == "A" and "B" in chunks[0].text      # tiny A merged into B, B's heading kept as a line
    c_parts = [c for c in chunks if c.heading == "A > C"]
    assert len(c_parts) >= 2 and all(c.words <= 600 for c in c_parts)


def test_chunker_never_splits_a_table_and_keeps_drive_links():
    rows = "".join(f"<tr><td>r{i}</td><td>{'x ' * 20}</td></tr>" for i in range(40))
    chunks = chunk(textify(f"<h1>T</h1><table>{rows}</table><p><a href='{DRIVE_URL}'>deck</a></p>"))
    assert sum(c.text.count("| r") for c in chunks) == 40
    assert len([c for c in chunks if "| r0 |" in c.text and "| r39 |" in c.text]) == 1
    assert any((DRIVE_URL, "deck") in c.drive_links for c in chunks)


def test_page_kind():
    assert page_kind("Ajay's Work Logs 2026-27", []) == "work_log"
    assert page_kind("Elec meeting notes 10/03", []) == "meeting_notes"
    assert page_kind("Joulemeter", ["electrical"]) == "page"


# ---------------------------------------------------------------- indexer

@pytest.fixture
def kdb(tmp_path):
    path = tmp_path / "knowledge.db"
    conn = store.connect(path)
    yield conn, path
    conn.close()


def corpus():
    return [
        content(1, "Joulemeter", JOULEMETER),
        content(2, "Ajay's Work Logs 2026-27", WORK_LOG),
        content(3, "Ordering Guide", SECRETS),
        content(4, "Software Important Usernames and Passwords", "<p>root / toor</p>"),
        content(10, "Private Elec", "<p>private stuff about the joulemeter</p>", restricted=True),
        content(11, "Child of Private", "<p>joulemeter child page</p>", ancestors=[10]),
        content(12, "Opted out", "<p>joulemeter notes</p>", labels=["no-tern"]),
    ]


def test_full_pass_indexes_only_what_may_be_served(kdb):
    conn, _ = kdb
    fake = FakeConfluence(corpus())
    r = index_once(conn, client_for(fake), "cev")
    assert r.mode == "full" and r.error is None and r.fetched == 7
    assert store.page_ids(conn) == {"1", "2", "3"}
    assert r.skipped == 4 and r.scrubbed >= 3
    all_text = " ".join(row[0] for row in conn.execute("SELECT text FROM chunks"))
    assert "Hunter2" not in all_text and "toor" not in all_text and "private stuff" not in all_text
    assert store.restricted_ids(conn) == {"10"}


def test_incremental_pass_updates_changed_pages_only(kdb):
    conn, _ = kdb
    fake = FakeConfluence(corpus())
    t0 = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)
    index_once(conn, client_for(fake), "cev", now=t0)

    fake.set(content(1, "Joulemeter", JOULEMETER.replace("10 Hz", "50 Hz"), version=2,
                     when="2026-11-03T09:00:00.000-04:00"))
    fake.set(content(13, "New page", "<p>New BMS architecture decision: centralised.</p>",
                     when="2026-11-03T09:30:00.000-04:00"))
    r = index_once(conn, client_for(fake), "cev", now=t0 + timedelta(days=1))
    assert r.mode == "incremental" and r.indexed == 2
    assert "50 Hz" in get_document(conn, "1")["text"] and "13" in store.page_ids(conn)
    # a child of the stored restricted page stays out on incremental passes too
    fake.set(content(14, "Another child", "<p>x</p>", ancestors=[10], when="2026-11-03T10:00:00.000-04:00"))
    index_once(conn, client_for(fake), "cev", now=t0 + timedelta(days=1, hours=1))
    assert "14" not in store.page_ids(conn)


def test_full_pass_removes_deleted_and_newly_restricted(kdb):
    conn, _ = kdb
    fake = FakeConfluence(corpus() + [content(20, "Parent", "<p>parent</p>"),
                                      content(21, "Kid", "<p>kid</p>", ancestors=[20])])
    index_once(conn, client_for(fake), "cev")
    assert {"20", "21"} <= store.page_ids(conn)
    fake.remove(2)
    fake.set(content(20, "Parent", "<p>parent</p>", restricted=True))   # same version: only a full pass sees it
    r = index_once(conn, client_for(fake), "cev", full=True)
    ids = store.page_ids(conn)
    assert "2" not in ids and "20" not in ids and "21" not in ids
    assert r.removed == 3


def test_failed_pass_keeps_the_last_good_index(kdb):
    conn, _ = kdb
    fake = FakeConfluence(corpus())
    index_once(conn, client_for(fake), "cev")
    broken = ConfluenceClient(BASE, "t", fetch=lambda u, h: (500, b""), retries=1, backoff_sec=0)
    r = index_once(conn, broken, "cev", full=True)
    assert r.error and store.page_ids(conn) == {"1", "2", "3"}
    assert conn.execute("SELECT error FROM index_runs ORDER BY id DESC LIMIT 1").fetchone()[0]


def test_chunker_version_change_forces_full_pass(kdb):
    conn, _ = kdb
    index_once(conn, client_for(FakeConfluence(corpus())), "cev")
    store.set_meta(conn, "chunker_version", "old")
    conn.commit()
    r = index_once(conn, client_for(FakeConfluence(corpus())), "cev")
    assert r.mode == "full" and store.get_meta(conn, "chunker_version") == CHUNKER_VERSION


def test_full_due_nightly_with_backstop(kdb):
    conn, _ = kdb
    now = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)
    assert _full_due(conn, now, 3)
    store.set_meta(conn, "last_full", (now - timedelta(hours=2)).isoformat())
    assert not _full_due(conn, now, 3)
    store.set_meta(conn, "last_full", (now - timedelta(hours=27)).isoformat())
    assert _full_due(conn, now, 3)


# ---------------------------------------------------------------- search

@pytest.fixture
def indexed(kdb):
    conn, path = kdb
    index_once(conn, client_for(FakeConfluence(corpus())), "cev")
    return conn, path


def test_fts_query_drops_stopwords_and_ors_terms():
    assert fts_query("Where are the joulemeter specs?") == '"joulemeter" OR "specs"'
    assert fts_query("the of and") is None
    assert fts_query("energy meter", {"meter": ["joulemeter"]}) == '"energy" OR "meter" OR "joulemeter"'


def test_search_ranks_page_over_work_log_one_section_per_page(indexed):
    conn, _ = indexed
    hits = search(conn, "how do we measure energy used during a race joulemeter")
    confluence = [h for h in hits if h["source"] == "confluence"]
    assert confluence[0]["title"] == "Joulemeter"
    assert len({h["id"] for h in confluence}) == len(confluence)
    work = [h for h in confluence if h["kind"] == "work_log"]
    assert work and work[0]["score"] < confluence[0]["score"]
    assert confluence[0]["url"].startswith(BASE) and confluence[0]["updated"]


def test_search_lists_drive_links_found_in_hits(indexed):
    conn, _ = indexed
    hits = search(conn, "joulemeter")
    drive = [h for h in hits if h["source"] == "drive"]
    assert drive and drive[0]["kind"] == "drive_link_only" and drive[0]["title"] == "PDR deck"
    assert drive[0]["id"] == "1AbCdEfGhIjKlMnOpQrStUvWxYz012345" and drive[0]["via"]["title"] == "Joulemeter"
    assert not [h for h in search(conn, "joulemeter", sources=["confluence"]) if h["source"] == "drive"]


def test_drive_file_id():
    assert drive_file_id("https://drive.google.com/open?id=1ZZZZyyyyXXXX") == "1ZZZZyyyyXXXX"
    assert drive_file_id("https://drive.google.com/drive/folders/1FolderIdAbc123") == "1FolderIdAbc123"


def test_get_document_whole_page_section_and_unknown(indexed):
    conn, _ = indexed
    doc = get_document(conn, "1")
    assert doc["title"] == "Joulemeter" and "Shunt resistor" in doc["text"] and doc["sections"]
    sec = get_document(conn, "2", "2026-10-03")
    assert "2026-10-03" in sec["text"] and "2026-10-01" not in sec["text"]
    assert get_document(conn, "999") is None


# ---------------------------------------------------------------- server

def test_server_exposes_exactly_two_tools(indexed):
    from evil.knowledge.server import create_knowledge_server
    _, path = indexed
    server = create_knowledge_server(str(path))
    names = {t.name for t in asyncio.run(server.list_tools())}
    assert names == {"search_team_knowledge", "get_document"}

    result = asyncio.run(server.call_tool("search_team_knowledge", {"query": "joulemeter specs"}))
    assert "Joulemeter" in result.content[0].text
    doc = asyncio.run(server.call_tool("get_document", {"source": "confluence", "id": "1"}))
    assert "Shunt resistor" in doc.content[0].text


def test_server_without_index_says_so(tmp_path):
    from evil.knowledge.server import create_knowledge_server
    server = create_knowledge_server(str(tmp_path / "missing.db"))
    with pytest.raises(Exception, match="not been built"):
        asyncio.run(server.call_tool("search_team_knowledge", {"query": "x"}))


def test_healthz_only_ok_after_a_successful_pass(tmp_path):
    from starlette.testclient import TestClient
    from evil.knowledge.server import create_knowledge_server
    path = tmp_path / "knowledge.db"
    store.connect(path).close()                     # what the indexer leaves behind without a token
    app = create_knowledge_server(str(path)).streamable_http_app()
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 503
        conn = store.connect(path)
        index_once(conn, client_for(FakeConfluence(corpus())), "cev")
        conn.close()
        r = client.get("/healthz")
        assert r.status_code == 200 and r.json()["pages"] == 3
