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
from evil.knowledge.filters import mentions_credential, redact_keys, scrub, skip_reason
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
    """Answers /rest/api/content/search with the current pages (or attachments, for `type = attachment`),
    paginated, and attachment downloads; records every request."""

    def __init__(self, pages=None, page_size=2, attachments=None):
        self.pages = {p["id"]: p for p in (pages or [])}
        self.attachments = {a["id"]: a for a in (attachments or [])}
        self.files: dict[str, bytes] = {}          # download path -> bytes
        self.fail_downloads = 0
        self.page_size = page_size
        self.requests = []
        self.downloads = []

    def set(self, page):
        self.pages[page["id"]] = page

    def remove(self, pid):
        self.pages.pop(str(pid), None)

    def __call__(self, url, headers):
        self.requests.append((url, headers))
        path = urllib.parse.urlparse(url).path
        if path.startswith("/download/"):
            self.downloads.append(path)
            if self.fail_downloads:
                self.fail_downloads -= 1
                return 500, b""
            return (200, self.files[path]) if path in self.files else (404, b"")
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        start, limit = int(q["start"][0]), int(q["limit"][0])
        items = list((self.attachments if "type = attachment" in q["cql"][0] else self.pages).values())
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


def test_merged_sections_keep_every_heading_and_two_columns_stay_apart():
    # seen 11-17: Software Home's two layout columns merged under "Autonomy > Current Members", so a member listed in
    # the Telemetry column was read as Autonomy; the "Leads: ..." headings (nothing under them) were dropped
    page = ('<ac:layout><ac:layout-section ac:type="two_equal"><ac:layout-cell><h2><ac:link><ri:page '
            'ri:content-title="Autonomy" /></ac:link></h2><h3>Leads: Cam M, Lucas L</h3><h3>Current Members:</h3>'
            + "".join(f"<p>Auto Person{i}</p>" for i in range(4)) + '<h3><strong>Alumni:</strong></h3>'
            + "".join(f"<p>Old Person{i}</p>" for i in range(8)) + '</ac:layout-cell><ac:layout-cell><h2><ac:link>'
            '<ri:page ri:content-title="Telemetry Home" /></ac:link></h2><h3>Leads: Rhea A, Katie X</h3>'
            '<h3>Members:</h3>' + "".join(f"<p>Telem Person{i}</p>" for i in range(7))
            + '</ac:layout-cell></ac:layout-section></ac:layout>')
    [only] = chunk(textify(page))
    assert only.heading == "Autonomy · Telemetry Home"
    assert "Leads: Cam M, Lucas L" in only.text and "Leads: Rhea A, Katie X" in only.text
    assert only.text.index("Telemetry Home > Members:") < only.text.index("Telem Person0")
    assert only.text.index("Autonomy > Alumni:") < only.text.index("Old Person0") < only.text.index("Telemetry Home")


def test_chunker_never_splits_a_table_and_keeps_drive_links():
    rows = "".join(f"<tr><td>r{i}</td><td>{'x ' * 20}</td></tr>" for i in range(40))
    chunks = chunk(textify(f"<h1>T</h1><table>{rows}</table><p><a href='{DRIVE_URL}'>deck</a></p>"))
    assert sum(c.text.count("| r") for c in chunks) == 40
    assert len([c for c in chunks if "| r0 |" in c.text and "| r39 |" in c.text]) == 1
    assert any((DRIVE_URL, "deck") in c.drive_links for c in chunks)


def test_page_kind():
    assert page_kind("Ann's Work Logs 2026-27", []) == "work_log"
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
        content(2, "Ann's Work Logs 2026-27", WORK_LOG),
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
    assert store.page_ids(conn) == {"1", "2", "3", "4"}          # 4 = the credentials page, as a link only
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
    assert r.error and store.page_ids(conn) == {"1", "2", "3", "4"}
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
    # by title (seen 11-17: id="Tailscale"), any case; a title matching nothing or several pages is not a page
    assert get_document(conn, "joulemeter")["id"] == "1" and get_document(conn, "No Such Page") is None


# ---------------------------------------------------------------- server

def test_server_exposes_exactly_three_tools(indexed):
    from evil.knowledge.server import create_knowledge_server
    _, path = indexed
    server = create_knowledge_server(str(path))
    names = {t.name for t in asyncio.run(server.list_tools())}
    assert names == {"search_team_knowledge", "get_document", "knowledge_sql"}

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
        assert r.status_code == 200 and r.json()["pages"] == 4


def test_task_list_bookkeeping_is_not_text():
    storage = ('<ac:task-list><ac:task><ac:task-id>7</ac:task-id>'
               '<ac:task-uuid>3f2b9c1e-8a4d-4e6f-9b2a-1c3d5e7f9a0b</ac:task-uuid>'
               '<ac:task-status>complete</ac:task-status><ac:task-body>Order the motor controller</ac:task-body>'
               '</ac:task></ac:task-list>')
    text = "\n".join(b.render() for b in textify(storage))
    assert "Order the motor controller" in text
    assert "3f2b9c1e" not in text and "complete" not in text


def test_long_blobs_redacted_outside_urls_only():
    secret = "q8WuWgs7Kd93LmXo2PzR5tY1vB6nH4jA0eCf"
    assert redact_keys(f"token is {secret}") == "token is [redacted]"
    url = "https://www.amazon.com/dp/B0X?dib=eyJ2IjoiMSJ9.fALLkylnMYFgxvyvU96vQgJ0dib_tag-is-very-long-indeed-xx"
    assert redact_keys(f"bought it at {url} today") == f"bought it at {url} today"
    assert redact_keys("https://maps.example.com/api?key=AIzaSyA1b2C3d4&x=1") == \
        "https://maps.example.com/api?[redacted]&x=1"
    assert redact_keys("postgres://cev:hunter2@db.example.com/x") == "postgres://[redacted]@db.example.com/x"


def test_short_pw_with_a_value_is_a_credential():
    assert mentions_credential("Pi login - user: cev, pw: abc")
    assert not mentions_credential("PW stands for pulse width here")


def test_credentials_page_is_a_link_only_pointer(kdb):
    conn, _ = kdb
    index_once(conn, client_for(FakeConfluence(corpus())), "cev")
    hits = search(conn, "where is the nuc password")
    top = [h for h in hits if h["id"] == "4"]
    assert top and top[0]["kind"] == "credentials_link_only" and "toor" not in json.dumps(hits)
    assert top[0]["url"].startswith("https://")
    doc = get_document(conn, "4")
    assert doc["kind"] == "credentials_link_only" and "toor" not in json.dumps(doc) and doc["sections"] == []
    # unchanged on the next pass, and never read
    r = index_once(conn, client_for(FakeConfluence(corpus())), "cev", full=True)
    assert "4" in store.page_ids(conn) and r.error is None


def _ranking_corpus():
    long_report = "<h2>Data acquisition</h2><p>" + " ".join(["The joulemeter feeds the DAQ."] * 30) + "</p>"
    return [
        content(30, "DAQ 2019-20 Technical Report", long_report),
        content(31, "Joulemeter", "<p>Measures race energy. Shunt and ADC.</p>"),
        content(32, "Kiwi Kruiser", '<ac:structured-macro ac:name="children"/>'),        # parent page, no text
        content(33, "Old wiring notes", "<p>harness connector crimp</p>", when="2019-01-01T10:00:00.000-05:00"),
        content(34, "New wiring notes", "<p>harness connector crimp</p>", when="2026-11-01T10:00:00.000-04:00"),
    ]


def test_title_edge_beats_a_long_report_mentioning_the_word(kdb):
    conn, _ = kdb
    index_once(conn, client_for(FakeConfluence(_ranking_corpus())), "cev")
    assert search(conn, "joulemeter")[0]["title"] == "Joulemeter"


def test_page_without_text_is_findable_by_title(kdb):
    conn, _ = kdb
    index_once(conn, client_for(FakeConfluence(_ranking_corpus())), "cev")
    hits = search(conn, "kiwi kruiser")
    assert hits[0]["title"] == "Kiwi Kruiser" and "no text of its own" in hits[0]["excerpt"]


def test_recency_breaks_near_ties_and_order_is_deterministic(kdb):
    conn, _ = kdb
    index_once(conn, client_for(FakeConfluence(_ranking_corpus())), "cev")
    now = datetime(2026, 11, 11, tzinfo=timezone.utc)
    first = [h["title"] for h in search(conn, "harness connector crimp", now=now)]
    assert first[:2] == ["New wiring notes", "Old wiring notes"]
    assert all([h["title"] for h in search(conn, "harness connector crimp", now=now)] == first for _ in range(5))


# ---------------------------------------------------------------- embeddings (phase 3)

from evil.knowledge import embeddings as emb  # noqa: E402

_CONCEPTS = {  # word -> dimension: a toy "meaning" space for the fake embedder
    "energy": 0, "joulemeter": 0, "power": 0, "consumption": 0, "shunt": 0, "voltage": 0,
    "order": 1, "ordering": 1, "buy": 1, "purchase": 1, "vendor": 1, "components": 1, "parts": 1,
    "worked": 2, "session": 2,
}


class FakeEmbed(emb.EmbedClient):
    def __init__(self, model="fake-embed", down=False):
        super().__init__(url="http://thor.invalid/embed", model=model)
        self.down = down
        self.calls = []

    def embed(self, texts, timeout=emb.BATCH_TIMEOUT_SEC):
        self.calls.append(len(texts))
        if self.down:
            raise emb.EmbedUnavailable("thor down")
        import numpy as np
        import zlib
        out = np.zeros((len(texts), 16), dtype="float32")
        for i, t in enumerate(texts):
            for w in t.lower().replace(">", " ").split():
                w = w.strip(".,:;?!()|")
                d = _CONCEPTS.get(w)
                if d is not None:
                    out[i, d] += 1
                elif w:                            # other words: weak, spread over the remaining dimensions
                    out[i, 3 + zlib.crc32(w.encode()) % 13] += 0.1
        return emb.normalize(out)


def _embedded(conn, model="fake-embed"):
    return conn.execute("SELECT count(*) FROM embeddings WHERE model = ?", (model,)).fetchone()[0]


def test_embed_pending_embeds_each_section_once_and_follows_page_changes(kdb):
    conn, _ = kdb
    fake = FakeConfluence(corpus())
    index_once(conn, client_for(fake), "cev")
    chunks = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]
    e = FakeEmbed()
    assert emb.embed_pending(conn, e) == (chunks, 0) and _embedded(conn) == chunks
    assert emb.embed_pending(conn, e) == (0, 0)                  # nothing new: no call at all
    assert len(e.calls) == 1

    fake.set(content(1, "Joulemeter", JOULEMETER + "<p>Now with a new shunt.</p>", version=2,
                     when="2026-11-02T10:00:00.000-04:00"))
    index_once(conn, client_for(fake), "cev", full=True)
    assert _embedded(conn) < chunks                              # the old page's vectors went with its sections
    done, left = emb.embed_pending(conn, e)
    assert done == conn.execute("SELECT count(*) FROM chunks WHERE page_id = '1'").fetchone()[0] and left == 0
    assert _embedded(conn) == conn.execute("SELECT count(*) FROM chunks").fetchone()[0]


def test_embed_pending_keeps_sections_pending_while_thor_is_down(indexed):
    conn, _ = indexed
    chunks = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]
    assert emb.embed_pending(conn, FakeEmbed(down=True)) == (0, chunks) and _embedded(conn) == 0
    assert emb.embed_pending(conn, FakeEmbed()) == (chunks, 0)


def test_switching_models_rebuilds_the_vectors(indexed):
    conn, _ = indexed
    emb.embed_pending(conn, FakeEmbed())
    emb.embed_pending(conn, FakeEmbed(model="other-embed"))
    assert _embedded(conn) == 0 and _embedded(conn, "other-embed") > 0


def test_meaning_match_finds_a_page_that_shares_no_keyword(indexed):
    conn, _ = indexed
    e = FakeEmbed()
    emb.embed_pending(conn, e)
    vectors = emb.load_vectors(conn, e.model)
    query = "how do I buy components"
    assert not [h for h in search(conn, query) if h["source"] == "confluence"]          # keywords alone: nothing
    hits = search(conn, query, vectors=vectors, query_vec=e.embed_query(query))
    assert hits[0]["title"] == "Ordering Guide"
    assert "Hunter2" not in json.dumps(hits)
    # and a keyword hit still leads when there is one
    hits = search(conn, "joulemeter specs", vectors=vectors, query_vec=e.embed_query("joulemeter specs"))
    assert hits[0]["title"] == "Joulemeter"


def test_vector_cache_reloads_after_a_reindex_even_if_ids_are_reused(kdb):
    conn, path = kdb
    fake = FakeConfluence(corpus())
    index_once(conn, client_for(fake), "cev")
    e = FakeEmbed()
    emb.embed_pending(conn, e)
    cache = emb.VectorCache()
    ro = store.connect_readonly(path)
    first = cache.get(ro, e.model)
    assert cache.get(ro, e.model) is first                       # unchanged index: same object
    fake.set(content(4, "Software Important Usernames and Passwords", "<p>x</p>", version=2,
                     when="2026-11-02T10:00:00.000-04:00"))
    index_once(conn, client_for(fake), "cev", full=True)
    emb.embed_pending(conn, e)
    assert cache.get(ro, e.model) is not first
    ro.close()


def test_server_uses_meanings_when_thor_answers_and_keywords_when_not(indexed):
    from evil.knowledge.server import create_knowledge_server
    conn, path = indexed
    emb.embed_pending(conn, FakeEmbed())
    query = {"query": "how do I buy components"}

    up = create_knowledge_server(str(path), embedder=FakeEmbed())
    result = asyncio.run(up.call_tool("search_team_knowledge", query))
    assert "Ordering Guide" in json.dumps(result.structured_content)

    down = create_knowledge_server(str(path), embedder=FakeEmbed(down=True))
    result = asyncio.run(down.call_tool("search_team_knowledge", query))
    assert "Ordering Guide" not in json.dumps(result.structured_content)       # keyword-only, still answers
    result = asyncio.run(down.call_tool("search_team_knowledge", {"query": "joulemeter"}))
    assert "Joulemeter" in json.dumps(result.structured_content)


def test_doc_and_query_text_use_each_models_format():
    assert emb.query_text("embeddinggemma", "brakes").startswith("task: search result | query: ")
    assert emb.query_text("qwen3-embedding:0.6b", "brakes").startswith("Instruct: ")
    d = emb.doc_text("embeddinggemma", "Brake Fluid Sensor", ["CEV Home", "Electrical"], "Wiring", "x" * 9000)
    assert d.startswith("title: Brake Fluid Sensor | text: CEV Home > Electrical > Brake Fluid Sensor > Wiring")
    assert len(d) <= emb.DOC_MAX_CHARS


# ---------------------------------------------------------------- live Google Drive (phase 4)

from evil.knowledge import drive as drive_mod  # noqa: E402

G = "application/vnd.google-apps."
# DRIVE_URL (the Joulemeter page's link) points at this id
DECK_ID = "1AbCdEfGhIjKlMnOpQrStUvWxYz012345"


class _Resp:
    def __init__(self, status=200, body=None, content=b""):
        self.status_code, self._body, self.content = status, body, content

    def json(self):
        return self._body


class FakeDrive:
    """Drive v3 as the client uses it: files.list (q), files.get (meta / alt=media), files.export."""

    def __init__(self, down=False):
        self.down = down
        self.calls = []
        self.files = {
            DECK_ID: {"id": DECK_ID, "name": "Joulemeter PDR", "mimeType": G + "presentation", "parents": ["fJ"],
                      "modifiedTime": "2025-10-01T00:00:00Z", "webViewLink": "https://docs.google.com/p/deck"},
            "d1": {"id": "d1", "name": "FA18 Joulemeter Report", "mimeType": G + "document", "parents": ["fJ"],
                   "modifiedTime": "2019-01-01T00:00:00Z", "webViewLink": "https://docs.google.com/d/d1"},
            "d1pdf": {"id": "d1pdf", "name": "FA18 Joulemeter Report.pdf(1)", "mimeType": "application/pdf",
                      "size": "5000", "parents": ["fJ"], "modifiedTime": "2019-01-02T00:00:00Z"},
            "cad": {"id": "cad", "name": "joulemeter enclosure.SLDPRT", "mimeType": "application/octet-stream",
                    "size": "900000", "parents": ["fJ"], "modifiedTime": "2019-02-01T00:00:00Z"},
            "pw": {"id": "pw", "name": "Lab Passwords", "mimeType": G + "document", "parents": ["fE"],
                   "modifiedTime": "2024-01-01T00:00:00Z"},
            "big": {"id": "big", "name": "Huge joulemeter datasheet.pdf", "mimeType": "application/pdf",
                    "size": str(80 * 1024 * 1024), "parents": ["fJ"]},
            "sheet": {"id": "sheet", "name": "Pinout", "mimeType": G + "spreadsheet", "parents": ["fJ"]},
            "fJ": {"id": "fJ", "name": "Joulemeter", "mimeType": G + "folder", "parents": ["fE"]},
            "fE": {"id": "fE", "name": "Electrical", "mimeType": G + "folder", "parents": ["root"]},
        }
        self.exports = {
            "d1": (b"# Joulemeter report\n\nMeasures pack energy at 10 Hz.\n\n## Lab access\n\nWifi password: "
                   b"hunter22\n\n## Results\n\n| Item | Value |\n|---|---|\n| Range | 0-60 V |\n"),
            "sheet": b"Pin,Signal\n1,VBAT\n2,GND\nwifi password,hunter22\n",
        }

    def get(self, url, params=None, timeout=None, **kw):
        self.calls.append((url, dict(params or {})))
        if self.down:
            raise ConnectionError("no route to googleapis")
        tail = url[len(drive_mod.API):].strip("/")
        if not tail:                                       # files.list: the indexer's listing
            q = params["q"]
            files = [dict({"version": "1", "createdTime": f.get("modifiedTime", "2019-01-01T00:00:00Z"),
                           "lastModifyingUser": {"displayName": "Someone"}}, **f) for f in self.files.values()]
            if f"mimeType = '{G}folder'" in q:
                files = [f for f in files if f["mimeType"] == G + "folder"]
            if f"mimeType != '{G}folder'" in q:
                files = [f for f in files if f["mimeType"] != G + "folder"]
            m = re.search(r"modifiedTime > '([^']+)'", q)
            if m:
                files = [f for f in files if f.get("modifiedTime", "") > m.group(1)]
            start = int(params.get("pageToken") or 0)
            size = int(params["pageSize"])
            body = {"files": files[start:start + size]}
            if start + size < len(files):
                body["nextPageToken"] = str(start + size)
            return _Resp(body=body)
        fid, _, op = tail.partition("/")
        if fid not in self.files:
            return _Resp(404)
        if op == "export":
            return _Resp(content=self.exports[fid])
        if params.get("alt") == "media":
            return _Resp(content=b"plain text")
        return _Resp(body=self.files[fid])


import re  # noqa: E402


def test_drive_read_filters_credentials_like_confluence():
    client = drive_mod.DriveClient(session=FakeDrive())
    doc = client.read("d1")
    assert "10 Hz" in doc["text"] and "0-60 V" in doc["text"]
    assert "hunter22" not in doc["text"] and "## Lab access" in doc["text"]     # heading kept, the value gone
    sheet = client.read("sheet")
    assert "VBAT" in sheet["text"] and "hunter22" not in sheet["text"]


def test_drive_credentials_and_binaries_are_never_downloaded():
    fake = FakeDrive()
    client = drive_mod.DriveClient(session=fake)
    assert client.read("pw")["kind"] == "credentials_link_only"
    assert client.read("cad")["kind"] == "drive_link_only"
    assert "too large" in client.read("big")["text"]
    assert not [u for u, p in fake.calls if u.endswith("/export") or p.get("alt") == "media"]
    assert client.read("nope") is None


def test_drive_long_documents_come_in_parts():
    fake = FakeDrive()
    fake.exports["d1"] = ("# Report\n\n" + "\n\n".join(f"paragraph {i} " + "x" * 100 for i in range(400))).encode()
    client = drive_mod.DriveClient(session=fake)
    first = client.read("d1")
    assert first["truncated"] and len(first["sections"]) >= 3 and "paragraph 0 " in first["text"]
    second = client.read("d1", "2")
    assert "paragraph 0 " not in second["text"] and second["text"]


def test_drive_down_backs_off_instead_of_failing_every_call():
    fake = FakeDrive(down=True)
    client = drive_mod.DriveClient(session=fake)
    with pytest.raises(drive_mod.DriveUnavailable):
        client.read("d1")
    n = len(fake.calls)
    with pytest.raises(drive_mod.DriveUnavailable, match="recently"):
        client.read("d1")
    assert len(fake.calls) == n                                  # no second network call within the minute


def test_drive_read_writes_nothing_to_disk(monkeypatch, tmp_path):
    import builtins
    real_open = builtins.open

    def guarded(file, mode="r", *a, **k):
        assert not any(c in mode for c in "wax+"), f"Drive code tried to write {file}"
        return real_open(file, mode, *a, **k)

    monkeypatch.setattr(builtins, "open", guarded)
    monkeypatch.chdir(tmp_path)
    client = drive_mod.DriveClient(session=FakeDrive())
    client.read("d1")
    client.read("sheet")
    assert list(tmp_path.iterdir()) == []


def test_drive_reads_uploaded_powerpoint_by_slide():
    import io as _io
    import zipfile as _zip
    buf = _io.BytesIO()
    a = 'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"'
    with _zip.ZipFile(buf, "w") as z:
        for n, words in ((1, ["Joulemeter V1.4", "Design review"]), (2, ["Shunt: 100 A", "Login: cev / hunter22"]),
                         (10, ["Next steps"])):
            body = "".join(f"<a:p><a:r><a:t>{w}</a:t></a:r></a:p>" for w in words)
            z.writestr(f"ppt/slides/slide{n}.xml", f"<p:sld {a} xmlns:p='p'><p:txBody>{body}</p:txBody></p:sld>")
    fake = FakeDrive()
    fake.files["pp"] = {"id": "pp", "name": "Joulemeter V1.4 PDR/CDR.pptx", "mimeType": drive_mod.PPTX,
                        "size": str(len(buf.getvalue())), "parents": ["fJ"]}
    fake.files["pp2"] = dict(fake.files["pp"], id="pp2", name="Copy of Joulemeter V1.4 PDR/CDR.pptx")
    real_get = fake.get
    fake.get = lambda url, params=None, timeout=None, **kw: (
        _Resp(content=buf.getvalue()) if url.endswith("/pp") and (params or {}).get("alt") == "media"
        else real_get(url, params, timeout))
    client = drive_mod.DriveClient(session=fake)
    doc = client.read("pp")
    assert doc["kind"] == "drive_doc"
    t = doc["text"]
    assert t.index("## Slide 1") < t.index("## Slide 2") < t.index("## Slide 10")       # numeric slide order
    assert "Shunt: 100 A" in t and "hunter22" not in t


def test_drive_slow_download_becomes_a_link_not_a_timeout(monkeypatch):
    class Slow(_Resp):
        def iter_content(self, n):
            for _ in range(100):
                yield b"x" * 10
    fake = FakeDrive()
    real_get = fake.get
    fake.get = lambda url, params=None, timeout=None, **kw: (
        Slow() if (params or {}).get("alt") == "media" else real_get(url, params, timeout))
    clock = iter(range(0, 10_000, 10))                    # every check is 10 s later
    monkeypatch.setattr(drive_mod.time, "monotonic", lambda: next(clock))
    client = drive_mod.DriveClient(session=fake)
    doc = client.read("d1pdf")
    assert doc["kind"] == "drive_link_only" and "too slow" in doc["text"]
    assert client.read("d1")["kind"] == "drive_doc"       # a slow file doesn't mark Drive as down


def test_drive_links_drop_google_tracking_params():
    from evil.knowledge.drive import file_url
    link = "https://docs.google.com/presentation/d/abc/edit?usp=drivesdk&ouid=1035&rtpof=true&sd=true"
    assert file_url({"id": "abc", "webViewLink": link}) == "https://docs.google.com/presentation/d/abc/edit"
    assert file_url({"id": "abc"}) == "https://drive.google.com/open?id=abc"


# ---------------------------------------------------------------- attachments (phase 6)

PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"


def _pptx(slides):
    import io as _io
    import zipfile as _zip
    buf = _io.BytesIO()
    a = 'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"'
    with _zip.ZipFile(buf, "w") as z:
        for n, words in enumerate(slides, 1):
            body = "".join(f"<a:p><a:r><a:t>{w}</a:t></a:r></a:p>" for w in words)
            z.writestr(f"ppt/slides/slide{n}.xml", f"<p:sld {a} xmlns:p='p'><p:txBody>{body}</p:txBody></p:sld>")
    return buf.getvalue()


def attachment(aid, page_id, title, mime, size=50_000, version=1, when="2026-11-01T10:00:00.000-04:00", comment=""):
    return {
        "id": aid, "type": "attachment", "title": title,
        "version": {"number": version, "when": when},
        "container": {"id": str(page_id), "type": "page"},
        "metadata": {"mediaType": mime},
        "extensions": {"mediaType": mime, "fileSize": size, "comment": comment},
        "_links": {"webui": f"/pages/viewpageattachments.action?pageId={page_id}&preview={title}",
                   "download": f"/download/attachments/{page_id}/{aid}?version={version}"},
    }


DECK = _pptx([["Joulemeter PDR", "Shunt amplifier gain 50 V/V"], ["Bench login: cev / hunter22"]])


def att_fake():
    fake = FakeConfluence(corpus(), attachments=[
        attachment("att1", 1, "Joulemeter PDR.pptx", PPTX_MIME, size=len(DECK)),
        attachment("att2", 1, "image2023-10-1_12-0-0.png", "image/png"),
        attachment("att8", 1, "IMG_1295.HEIC", "application/octet-stream"),   # a photo Confluence doesn't type
        attachment("att3", 1, "firmware v2.zip", "application/zip", size=3_200_000, comment="build from main"),
        attachment("att4", 1, "router passwords.pdf", "application/pdf"),
        attachment("att5", 1, "huge scan.pdf", "application/pdf", size=40_000_000),
        attachment("att6", 10, "private.pptx", PPTX_MIME),                  # on the restricted page
        attachment("att7", 12, "opted out.pptx", PPTX_MIME),                # on the no-tern page
    ])
    fake.files["/download/attachments/1/att1"] = DECK
    return fake


def test_attachments_indexed_under_their_page_with_the_page_filters(kdb):
    conn, _ = kdb
    fake = att_fake()
    r = index_once(conn, client_for(fake), "cev")
    assert r.error is None
    kinds = dict(conn.execute("SELECT id, kind FROM pages WHERE parent IS NOT NULL").fetchall())
    # the screenshot is left out; files on skipped pages too
    assert kinds == {"att1": "attachment", "att3": "attachment_link_only", "att4": "credentials_link_only",
                     "att5": "attachment_link_only"}
    assert fake.downloads == ["/download/attachments/1/att1"]     # not the credentials file, the zip or the 40 MB pdf
    assert store.page_ids(conn) == {"1", "2", "3", "4"}           # attachments are not counted as pages
    assert store.stats(conn)["attachments"] == 4
    all_text = " ".join(row[0] for row in conn.execute("SELECT text FROM chunks"))
    assert "Shunt amplifier gain" in all_text and "hunter22" not in all_text

    hits = search(conn, "shunt amplifier gain deck", sources=["confluence"])
    deck = next(h for h in hits if h["id"] == "att1")
    assert deck["breadcrumb"][-1] == "Joulemeter" and deck["kind"] == "attachment"
    doc = get_document(conn, "att1")
    assert 'attached to the page "Joulemeter"' in doc["text"] and "Slide 1" in doc["text"]
    zip_hit = next(h for h in search(conn, "firmware zip", sources=["confluence"]) if h["id"] == "att3")
    assert "ZIP file" in zip_hit["excerpt"] and "3.2 MB" in zip_hit["excerpt"]
    assert search(conn, "router passwords", sources=["confluence"])[0]["excerpt"].startswith("This page holds")


def test_attachments_follow_their_page_and_versions(kdb):
    conn, _ = kdb
    fake = att_fake()
    t0 = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)
    index_once(conn, client_for(fake), "cev", now=t0)
    fake.downloads.clear()
    # unchanged: nothing downloaded again
    index_once(conn, client_for(fake), "cev", now=t0 + timedelta(hours=1))
    assert fake.downloads == []
    # a new version is re-read
    deck2 = _pptx([["Joulemeter PDR rev B", "Shunt amplifier gain 20 V/V"]])
    fake.attachments["att1"] = attachment("att1", 1, "Joulemeter PDR.pptx", PPTX_MIME, size=len(deck2), version=2,
                                          when="2026-11-03T09:00:00.000-04:00")
    fake.files["/download/attachments/1/att1"] = deck2
    index_once(conn, client_for(fake), "cev", now=t0 + timedelta(days=1))
    assert "20 V/V" in get_document(conn, "att1")["text"]
    # a file deleted from Confluence goes on the next full pass
    fake.attachments.pop("att3")
    index_once(conn, client_for(fake), "cev", full=True, now=t0 + timedelta(days=1, hours=1))
    assert "att3" not in store.attachment_ids(conn)
    # the page opts out -> its attachments go with it, also on an hourly pass
    fake.set(content(1, "Joulemeter", JOULEMETER, version=2, labels=["no-tern"], when="2026-11-04T09:00:00.000-04:00"))
    index_once(conn, client_for(fake), "cev", now=t0 + timedelta(days=2))
    assert store.attachment_ids(conn) == set()


def test_failed_attachment_download_is_retried_next_pass(kdb):
    conn, _ = kdb
    fake = att_fake()
    fake.fail_downloads = 3                      # the client doesn't retry downloads; one failure is enough
    t0 = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)
    r = index_once(conn, client_for(fake), "cev", now=t0)
    assert r.error is None and r.attachments_pending == 1 and "att1" not in store.attachment_ids(conn)
    fake.fail_downloads = 0
    r = index_once(conn, client_for(fake), "cev", now=t0 + timedelta(hours=1))   # hourly, but lists everything again
    assert r.attachments_pending == 0 and "att1" in store.attachment_ids(conn)


def test_page_update_keeps_its_attachments_and_old_index_gets_the_column(kdb):
    conn, path = kdb
    fake = att_fake()
    t0 = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)
    index_once(conn, client_for(fake), "cev", now=t0)
    fake.set(content(1, "Joulemeter", JOULEMETER.replace("10 Hz", "50 Hz"), version=2,
                     when="2026-11-03T09:00:00.000-04:00"))
    fake.downloads.clear()
    index_once(conn, client_for(fake), "cev", now=t0 + timedelta(days=1))
    assert "att1" in store.attachment_ids(conn) and fake.downloads == []

    old = store.connect(path.parent / "old.db")
    old.execute("ALTER TABLE pages DROP COLUMN parent")
    old.commit()
    old.close()
    again = store.connect(path.parent / "old.db")
    assert "parent" in {r[1] for r in again.execute("PRAGMA table_info(pages)")}
    again.close()


# ---------------------------------------------------------------- Drive in the index (11-18)

from evil.knowledge import drive_index  # noqa: E402
from evil.knowledge import sql as sql_mod  # noqa: E402


def _drive_fake():
    fake = FakeDrive()
    fake.files["photo"] = {"id": "photo", "name": "IMG_2041.jpg", "mimeType": "image/jpeg", "size": "3000000",
                           "parents": ["fJ"], "modifiedTime": "2026-11-17T12:00:00Z"}
    fake.files["d1"]["modifiedTime"] = "2026-11-16T15:00:00Z"
    fake.exports[DECK_ID] = b"Joulemeter PDR\n\nShunt rated 100 A.\n"
    return fake


def _drive_client(fake):
    return drive_mod.DriveClient(session=fake, retry_after_sec=0)


def test_drive_full_listing_indexes_every_file_and_reads_text(indexed):
    conn, _ = indexed
    fake = _drive_fake()
    r = drive_index.sync_drive(conn, _drive_client(fake), now=datetime(2026, 11, 18, tzinfo=timezone.utc))
    assert r.mode == "full" and r.error is None and r.pending == 0
    kinds = dict(conn.execute("SELECT id, kind FROM pages WHERE source = 'drive'").fetchall())
    assert kinds["d1"] == "drive_doc" and kinds["sheet"] == "drive_doc" and kinds["photo"] == "drive_media"
    assert kinds["cad"] == "drive_link_only" and kinds["big"] == "drive_link_only" and kinds["fJ"] == "drive_folder"
    assert kinds["pw"] == "credentials_link_only"
    text = " ".join(t for (t,) in conn.execute("SELECT text FROM chunks WHERE page_id IN ('d1', 'sheet')"))
    assert "10 Hz" in text and "VBAT" in text and "hunter22" not in text            # scrubbed like Confluence
    row = conn.execute("SELECT breadcrumb, last_editor FROM pages WHERE id = 'd1'").fetchone()
    assert json.loads(row["breadcrumb"]) == ["Electrical", "Joulemeter"] and row["last_editor"] == "Someone"
    assert not conn.execute("SELECT 1 FROM chunks WHERE page_id = 'photo'").fetchone()   # never in search
    downloaded = {u.split("/files/")[1].split("/")[0] for u, p in fake.calls if u.endswith("/export") or p.get("alt")}
    assert "pw" not in downloaded and "cad" not in downloaded and "big" not in downloaded
    assert store.page_ids(conn) == {"1", "2", "3", "4"}                 # Confluence's own set is untouched


def test_confluence_full_pass_keeps_drive_rows(indexed):
    conn, _ = indexed
    drive_index.sync_drive(conn, _drive_client(_drive_fake()))
    index_once(conn, client_for(FakeConfluence(corpus())), "cev", full=True)
    assert conn.execute("SELECT count(*) FROM pages WHERE source = 'drive'").fetchone()[0] == 10


def test_drive_incremental_rereads_changed_files_and_full_removes_gone_ones(indexed):
    conn, _ = indexed
    fake = _drive_fake()
    client = _drive_client(fake)
    drive_index.sync_drive(conn, client, now=datetime(2026, 11, 18, tzinfo=timezone.utc))
    fake.files["d1"].update(version="2", modifiedTime="2026-11-18T13:00:00Z")
    fake.exports["d1"] = b"# Joulemeter report\n\nNow sampled at 50 Hz.\n"
    r = drive_index.sync_drive(conn, client, now=datetime(2026, 11, 18, 14, tzinfo=timezone.utc))
    assert r.mode == "incremental" and r.indexed == 1 and r.read == 1
    assert "50 Hz" in " ".join(t for (t,) in conn.execute("SELECT text FROM chunks WHERE page_id = 'd1'"))
    del fake.files["cad"]
    fake.files["fJ"]["name"] = "Joulemeter 2"
    r = drive_index.sync_drive(conn, client, full=True)
    assert r.removed == 1 and not conn.execute("SELECT 1 FROM pages WHERE id = 'cad'").fetchone()
    assert json.loads(conn.execute("SELECT breadcrumb FROM pages WHERE id = 'sheet'").fetchone()[0])[-1] == "Joulemeter 2"


def test_drive_text_budget_leaves_files_pending_and_the_server_reads_them_live(indexed):
    conn, path = indexed
    fake = _drive_fake()
    r = drive_index.sync_drive(conn, _drive_client(fake), text_budget_sec=-1)
    assert r.pending == 4 and r.read == 0                     # deck, d1, d1pdf, sheet: names only for now
    conn.commit()
    from evil.knowledge.server import create_knowledge_server
    server = create_knowledge_server(str(path), drive=drive_mod.DriveClient(session=fake))
    doc = asyncio.run(server.call_tool("get_document", {"source": "drive", "id": "d1"}))
    assert "10 Hz" in doc.content[0].text and any(u.endswith("/d1/export") for u, _ in fake.calls)   # live


def test_server_reads_an_indexed_drive_file_from_the_index(indexed):
    conn, path = indexed
    drive_index.sync_drive(conn, _drive_client(_drive_fake()))
    conn.commit()
    from evil.knowledge.server import create_knowledge_server
    live = FakeDrive(down=True)                               # any live call would fail
    server = create_knowledge_server(str(path), drive=drive_mod.DriveClient(session=live))
    doc = json.loads(asyncio.run(server.call_tool("get_document", {"source": "drive", "id": "d1"})).content[0].text)
    assert "10 Hz" in doc["text"] and doc["source"] == "drive" and not live.calls


def test_search_ranks_drive_files_with_pages_and_collapses_copies(indexed):
    conn, _ = indexed
    drive_index.sync_drive(conn, _drive_client(_drive_fake()))
    hits = search(conn, "joulemeter report", limit=10)
    titles = [h["title"] for h in hits]
    assert "FA18 Joulemeter Report" in titles and "FA18 Joulemeter Report.pdf(1)" not in titles
    drive_hit = next(h for h in hits if h["title"] == "FA18 Joulemeter Report")
    assert drive_hit["source"] == "drive" and drive_hit["kind"] == "drive_doc"
    assert drive_hit["breadcrumb"] == ["Electrical", "Joulemeter"]
    assert all(h["source"] == "drive" for h in search(conn, "joulemeter", sources=["drive"]))
    assert all(h["source"] == "confluence" for h in search(conn, "joulemeter", sources=["confluence"]))
    titles = [h["title"] for h in search(conn, "joulemeter specs", limit=10)]
    assert titles.count("Joulemeter PDR") == 1 and "PDR deck" not in titles   # the page's link is the indexed deck
    only_page = [h for h in search(conn, "joulemeter specs", limit=1, sources=["confluence", "drive"])
                 + search(conn, "shunt calibrated bench supply", limit=1) if h.get("via")]
    assert only_page and only_page[0]["title"] == "Joulemeter PDR" and only_page[0]["kind"] == "drive_doc"


def test_drive_rows_without_text_are_not_embedded(indexed):
    conn, _ = indexed
    drive_index.sync_drive(conn, _drive_client(_drive_fake()))
    kinds = {r["kind"] for r in conn.execute(
        "SELECT DISTINCT p.kind FROM chunks c JOIN pages p ON p.id = c.page_id WHERE c.id IN "
        "(SELECT id FROM chunks) AND p.source = 'drive' AND c.id IN ({})".format(
            ",".join(str(r["id"]) for r in emb.pending(conn, "fake-embed"))))}
    assert kinds == {"drive_doc"}


def test_knowledge_sql_answers_what_changed_and_who(indexed):
    conn, path = indexed
    drive_index.sync_drive(conn, _drive_client(_drive_fake()))
    conn.commit()
    ro = store.connect_readonly(path)
    rows = sql_mod.run(ro, "SELECT title, modified_local, last_editor, kind, path FROM docs WHERE source = 'drive' "
                           "AND kind != 'folder' AND modified >= '2026-11-15' ORDER BY modified DESC")
    assert [r["title"] for r in rows] == ["IMG_2041.jpg", "FA18 Joulemeter Report"]
    assert rows[0]["kind"] == "photo" and rows[1]["kind"] == "file" and rows[1]["path"] == "Electrical > Joulemeter"
    assert rows[1]["modified_local"] == "2026-11-16 10:00:00" and rows[1]["last_editor"] == "Someone"
    conf = sql_mod.run(ro, "SELECT title, type, last_editor, created FROM docs WHERE source = 'confluence' "
                           "AND kind = 'page'")
    assert {r["title"] for r in conf} == {"Joulemeter", "Ordering Guide"} and conf[0]["last_editor"] == "Someone"
    hits = sql_mod.run(ro, "SELECT d.title FROM sections s JOIN docs d ON d.id = s.doc_id WHERE s.text LIKE '%VBAT%'")
    assert hits == [{"title": "Pinout"}]
    assert not sql_mod.run(ro, "SELECT * FROM sections s JOIN docs d ON d.id = s.doc_id WHERE d.kind = 'credentials'")
    with pytest.raises(ValueError):
        sql_mod.run(ro, "DELETE FROM pages")
    ro.close()


def test_knowledge_sql_tool_explains_a_bad_column(indexed):
    from evil.knowledge.server import create_knowledge_server
    _, path = indexed
    server = create_knowledge_server(str(path))
    with pytest.raises(Exception, match="View docs"):
        asyncio.run(server.call_tool("knowledge_sql", {"query": "SELECT updated_at FROM docs"}))
    ok = asyncio.run(server.call_tool("knowledge_sql", {"query": "SELECT COUNT(*) AS n FROM docs"}))
    assert '"n"' in ok.content[0].text


def test_drive_docs_inline_pictures_are_dropped_not_counted_as_credentials():
    # seen 11-18: Docs' markdown export inlines every picture as base64; the key redactor counted 194k "credentials"
    fake = FakeDrive()
    fake.exports["d1"] = (b"# Report\n\n![][image1]\n\nShunt rated 100 A.\n\n[image1]: <data:image/png;base64,"
                          + b"iVBORw0KGgo" * 2000 + b">\n")
    blocks = drive_mod.DriveClient(session=fake).blocks(fake.files["d1"])
    clean, stats = scrub(blocks)
    text = " ".join(b.render() for b in clean)
    assert "100 A" in text and "base64" not in text and "image1" not in text and stats.total == 0


def test_one_file_too_big_to_export_does_not_stop_drive(indexed):
    conn, _ = indexed
    fake = _drive_fake()
    real_get = fake.get
    big = _Resp(403, {"error": {"errors": [{"reason": "exportSizeLimitExceeded"}]}})
    fake.get = lambda url, params=None, timeout=None, **kw: (
        big if url.endswith("/sheet/export") else real_get(url, params, timeout))
    r = drive_index.sync_drive(conn, _drive_client(fake))
    assert r.pending == 0 and r.unreadable == 2 and r.read == 2           # the sheet, and a fake PDF
    assert "too large for Google to export" in conn.execute(
        "SELECT text FROM chunks WHERE page_id = 'sheet'").fetchone()[0]


def test_drive_rate_limits_back_off_and_reading_goes_on(indexed, monkeypatch):
    conn, _ = indexed
    monkeypatch.setattr(drive_index.time, "sleep", lambda s: None)
    fake = _drive_fake()
    real_get = fake.get
    refusals = [3]

    def get(url, params=None, timeout=None, **kw):
        if url.endswith("/export") and refusals[0]:
            refusals[0] -= 1
            return _Resp(429, {"error": {"errors": [{"reason": "rateLimitExceeded"}]}})
        return real_get(url, params, timeout)
    fake.get = get
    client = _drive_client(fake)
    monkeypatch.setattr(drive_index, "READ_BATCH", 1)
    r = drive_index.sync_drive(conn, client)
    assert r.read == 3 and r.pending == 0              # deck, d1, sheet all read after the refusals


def test_resumes_and_rosters_rank_below_project_docs_unless_asked_for(indexed):
    # seen 11-18: a member's name returned their resume first, and "what does he work on" was answered from it
    conn, _ = indexed
    fake = _drive_fake()
    fake.files["cv"] = {"id": "cv", "name": "Jo_Smith_Resume.pdf", "mimeType": "application/pdf", "size": "5000",
                        "parents": ["fR"], "modifiedTime": "2026-11-01T00:00:00Z"}
    fake.files["fR"] = {"id": "fR", "name": "Resume Book", "mimeType": G + "folder", "parents": ["fE"]}
    fake.files["proj"] = {"id": "proj", "name": "SP27 Telemetry Projects", "mimeType": G + "document",
                          "parents": ["fJ"], "modifiedTime": "2026-11-01T00:00:00Z"}
    fake.exports["proj"] = b"# Projects\n\nCAN logger rewrite: Jo Smith.\n"
    real_get = fake.get
    fake.get = lambda url, params=None, timeout=None, **kw: (
        _Resp(content=b"Jo Smith Jo Smith resume, CAN logger, Jo Smith") if url.endswith("/cv")
        and (params or {}).get("alt") else real_get(url, params, timeout))
    orig = drive_mod._pdf_text
    drive_mod._pdf_text = lambda data: data.decode()
    try:
        drive_index.sync_drive(conn, _drive_client(fake))
    finally:
        drive_mod._pdf_text = orig
    titles = [h["title"] for h in search(conn, "Jo Smith")]
    assert titles.index("SP27 Telemetry Projects") < titles.index("Jo_Smith_Resume.pdf")
    assert search(conn, "Jo Smith resume")[0]["title"] == "Jo_Smith_Resume.pdf"


def test_a_drive_file_opens_by_title_and_when_asked_for_as_confluence(indexed):
    # seen 11-18: get_document(id="FA26 Fullstack Onboarding", source="drive") and then source="confluence" both
    # failed, and the answer said it could only see an excerpt
    conn, path = indexed
    drive_index.sync_drive(conn, _drive_client(_drive_fake()))
    conn.commit()
    from evil.knowledge.server import create_knowledge_server
    server = create_knowledge_server(str(path), drive=drive_mod.DriveClient(session=FakeDrive(down=True)))
    for source in ("drive", "confluence"):
        doc = json.loads(asyncio.run(server.call_tool(
            "get_document", {"source": source, "id": "FA18 Joulemeter Report"})).content[0].text)
        assert doc["id"] == "d1" and "10 Hz" in doc["text"]
    rows = sql_mod.run(store.connect_readonly(path), "SELECT title FROM docs WHERE type IN ('Google Slides', "
                       "'PowerPoint') AND kind = 'file' AND path LIKE '%Electrical > Joulemeter%'")
    assert rows == [{"title": "Joulemeter PDR"}]


def test_a_shortcut_lists_as_the_type_it_points_to(indexed):
    conn, path = indexed
    fake = _drive_fake()
    fake.files["sc"] = {"id": "sc", "name": "Workshop Template", "mimeType": G + "shortcut", "parents": ["fJ"],
                        "shortcutDetails": {"targetMimeType": G + "presentation"}}
    drive_index.sync_drive(conn, _drive_client(fake))
    conn.commit()
    rows = sql_mod.run(store.connect_readonly(path), "SELECT title FROM docs WHERE type = 'Google Slides' AND "
                       "path LIKE '%Joulemeter' ORDER BY title")
    assert [r["title"] for r in rows] == ["Joulemeter PDR", "Workshop Template"]
    assert conn.execute("SELECT kind FROM pages WHERE id = 'sc'").fetchone()[0] == "drive_link_only"   # never read


def test_a_drive_file_opens_by_its_folder_path_and_title(indexed):
    # seen 11-18: get_document(id="Electrical > Onboarding > 2026 > FA26 Freshmen Project Specs")
    conn, path = indexed
    drive_index.sync_drive(conn, _drive_client(_drive_fake()))
    conn.commit()
    from evil.knowledge.server import create_knowledge_server
    server = create_knowledge_server(str(path), drive=drive_mod.DriveClient(session=FakeDrive(down=True)))
    doc = json.loads(asyncio.run(server.call_tool(
        "get_document", {"source": "drive", "id": "Electrical > Joulemeter > FA18 Joulemeter Report"})).content[0].text)
    assert doc["id"] == "d1"


def test_team_word_variants_match_each_other():
    q = fts_query("electrical freshman projects")
    assert '"freshmen"' in q and '"freshman"' in q
    assert '"electrical"' in fts_query("elec onboarding")


def test_the_folder_path_and_recency_lift_the_right_doc():
    from evil.knowledge.search import path_factor, recency_factor, _content_stems
    q = set(_content_stems("electrical freshman projects"))
    assert path_factor(q, json.dumps(["Electrical", "Onboarding", "2026"])) > path_factor(q, json.dumps(["Mechanical"]))
    now = datetime(2026, 11, 18, tzinfo=timezone.utc)
    assert recency_factor("2026-11-01T00:00:00Z", now) / recency_factor("2019-01-01T00:00:00Z", now) > 1.25


def test_a_named_subteam_pushes_down_other_subteams_docs():
    from evil.knowledge.search import subteam_factor
    mech = json.dumps(["Mechanical", "Onboarding", "Frosh Projects 2027"])
    elec = json.dumps(["Electrical", "Onboarding", "2026"])
    assert subteam_factor("electrical freshman projects", "UC27 Freshman Project Master Doc", mech) < 1.0
    assert subteam_factor("electrical freshman projects", "FA26 Freshmen Project Specs", elec) == 1.0
    assert subteam_factor("freshman projects", "UC27 Freshman Project Master Doc", mech) == 1.0     # none named
    assert subteam_factor("elec freshman projects", "General Team", json.dumps(["CEV Home"])) == 1.0   # no subteam


def test_sql_matches_finds_topics_in_the_text_with_team_spellings(indexed):
    conn, path = indexed
    fake = _drive_fake()
    fake.files["frosh"] = {"id": "frosh", "name": "UC27 Master Doc", "mimeType": G + "document",
                           "parents": ["fJ"], "modifiedTime": "2026-11-18T00:00:00Z"}
    fake.exports["frosh"] = b"# Frosh Projects\n\nClevis: mount the shock.\n"
    drive_index.sync_drive(conn, _drive_client(fake))
    conn.commit()
    ro = store.connect_readonly(path)
    rows = sql_mod.run(ro, "SELECT d.title FROM docs d WHERE d.id IN (SELECT doc_id FROM sections WHERE "
                           "matches(text, 'freshman projects'))")
    assert rows == [{"title": "UC27 Master Doc"}]
    assert sql_mod.matches("Freshmen Project Specs", "freshman project") == 1
    assert sql_mod.matches("Freshmen Specs", "freshman project") == 0


def test_a_missing_alias_error_says_how_to_fix_it(indexed):
    # seen 11-18: "SELECT title FROM docs WHERE d.id IN (...)" -> "no such column: d.id" plus the whole schema
    from evil.knowledge.server import create_knowledge_server
    _, path = indexed
    server = create_knowledge_server(str(path))
    fixed = asyncio.run(server.call_tool("knowledge_sql", {"query": "SELECT title FROM docs WHERE d.id = '1'"}))
    assert "Joulemeter" in fixed.content[0].text                       # d. without docs d: added for it
    with pytest.raises(Exception, match="write FROM docs x"):
        asyncio.run(server.call_tool("knowledge_sql", {"query": "SELECT title FROM docs WHERE x.id = '1'"}))
    ok = asyncio.run(server.call_tool("knowledge_sql", {"query": (
        "SELECT d.id, d.source, d.title, d.path FROM docs d WHERE d.id IN (SELECT doc_id FROM sections WHERE "
        "matches(text, 'joulemeter specs'))")}))
    assert "Joulemeter" in ok.content[0].text


def test_undefined_d_and_s_aliases_are_added():
    # seen 11-18: FROM docs with d.id, three times in a row, until it ran out of steps
    w = sql_mod.with_aliases
    assert w("SELECT title FROM docs WHERE d.id = '1'") == "SELECT title FROM docs d WHERE d.id = '1'"
    assert w("SELECT d.title, s.heading FROM docs JOIN sections ON s.doc_id = d.id") == (
        "SELECT d.title, s.heading FROM docs d JOIN sections s ON s.doc_id = d.id")
    for q in ("SELECT d.title FROM docs AS d JOIN sections s ON s.doc_id = d.id", "SELECT title FROM docs",
              "SELECT d.title FROM docs d"):
        assert w(q) == q
