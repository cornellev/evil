"""Builds a throwaway knowledge.db from the unit tests' fake Confluence corpus (real indexer code path)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evil.knowledge import store  # noqa: E402
from evil.knowledge.indexer import index_once  # noqa: E402
from tests.test_knowledge import FakeConfluence, client_for, corpus  # noqa: E402

conn = store.connect(sys.argv[1])
result = index_once(conn, client_for(FakeConfluence(corpus())), "cev")
assert result.error is None, result.error
print(sys.argv[1])
