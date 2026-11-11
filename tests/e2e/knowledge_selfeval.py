"""Self-built cev-knowledge eval: no hand-written questions needed.

- title questions: "where is the doc about <page title>" should find that page
- passage questions: 4 fairly distinctive words from a random section should find that section's page
Prints hit@1 / hit@3 / MRR per kind (11-11, real index: deployed ranking title 0.71 / 0.75 / 0.73, passage 0.93 / 1.00 / 0.96;
with title + recency edges and title-only entries for empty pages: title 0.97 / 0.98 / 0.98, passage 0.90 / 0.99 / 0.95).
On the NUC: docker compose exec -T knowledge python - /knowledge/knowledge.db < tests/e2e/knowledge_selfeval.py
"""
import collections
import math
import random
import re
import sqlite3
import sys

from evil.knowledge import search

db = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
db.row_factory = sqlite3.Row
WORD = re.compile(r"[a-z][a-z0-9]{3,}")
pages = {r["id"]: r for r in db.execute("SELECT * FROM pages")}
chunks = db.execute("SELECT page_id, ordinal, heading, text FROM chunks").fetchall()
df = collections.Counter()
for c in chunks:
    df.update(set(WORD.findall(c["text"].lower())))
N = len(chunks)
rnd = random.Random(7)
cases = []
for pid, p in sorted(pages.items()):
    if p["kind"] != "page":
        continue
    t = p["title"]
    words = [w for w in re.findall(r"[A-Za-z0-9]+", t) if len(w) > 1]
    if len(words) >= 1:
        cases.append(("title", f"where is the doc about {' '.join(words).lower()}", pid))
for c in rnd.sample([c for c in chunks if pages[c["page_id"]]["kind"] == "page" and len(c["text"].split()) >= 80], 250):
    title = set(WORD.findall(pages[c["page_id"]]["title"].lower()))
    ws = sorted({w for w in WORD.findall(c["text"].lower()) if w not in title and df[w] >= 2},
                key=lambda w: math.log(N / df[w]), reverse=True)[4:16]
    if len(ws) >= 4:
        cases.append(("passage", " ".join(rnd.sample(ws, 4)), c["page_id"]))
res = collections.defaultdict(lambda: [0, 0, 0, 0.0])
for kind, q, want in cases:
    ids = [h["id"] for h in search.search(db, q, limit=10, sources=["confluence"])]
    r = ids.index(want) + 1 if want in ids else None
    m = res[kind]; m[0] += 1; m[1] += r == 1; m[2] += bool(r and r <= 3); m[3] += 1 / r if r else 0
for kind, (n, h1, h3, mrr) in sorted(res.items()):
    print(f"{kind:8} n={n:3}  hit@1 {h1/n:.2f}  hit@3 {h3/n:.2f}  MRR {mrr/n:.3f}")
q = "motor controller design"
print("deterministic:", [h["id"] for h in search.search(db, q)] == [h["id"] for h in search.search(db, q)])
