"""cev-knowledge: team-knowledge search for Tern and people's agents (harness/cev-knowledge-plan-2026-11.md).

Two processes, one SQLite file (`knowledge.db`, its own volume):
- indexer.py (`knowledge-indexer` service): the only holder of the Confluence token. Copies the `cev` space
  through the read-only client (confluence.py), drops what must never be served (filters.py), turns pages into
  sections (textify.py, chunker.py) and stores them (store.py). Hourly incremental, nightly full pass.
  After each pass it embeds new sections through Tern's /embed on thor (embeddings.py).
- server.py (`knowledge` service, MCP on :8768): `search_team_knowledge` + `get_document`, served from the
  index only (search.py: keywords + meaning, keyword-only when thor is down); no Confluence access, no token.

Read-only by construction: the Confluence client only issues GET requests, and nothing here writes anywhere but
knowledge.db. Everything stored has already passed the filters, so the server can return anything it holds.
"""
