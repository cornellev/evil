"""Section embeddings for cev-knowledge (phase 3): meaning-based matches next to the keyword index.

- The vectors come from Tern's `POST /embed` on thor (thor's Ollama is never opened to the tailnet; Tern relays
  allowlisted embedding models only). KNOWLEDGE_EMBED_URL empty = embeddings off, search is keyword-only.
- The indexer embeds sections that have no vector for the current model yet (new or changed pages: a changed page
  gets new section ids, so its old vectors go with its old sections). thor down -> they stay pending and the next
  pass picks them up; search falls back to keywords for anything not embedded.
- The server embeds each query (~20 ms on thor + ~30 ms NUC -> thor) and compares it with every section in memory
  (~2k x 768 floats: brute force, no vector database). After a failed call it stops trying for a minute, so a down
  thor costs one timeout, not one per search.
- Each model wants its own prompt format for queries vs. documents; that lives here, not in Tern.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger("evil.knowledge.embeddings")

DEFAULT_EMBED_URL = os.getenv("KNOWLEDGE_EMBED_URL", "").strip()
DEFAULT_EMBED_MODEL = os.getenv("KNOWLEDGE_EMBED_MODEL", "embeddinggemma").strip()
QUERY_TIMEOUT_SEC = float(os.getenv("KNOWLEDGE_EMBED_QUERY_TIMEOUT_SEC", "2"))
BATCH_TIMEOUT_SEC = 120.0
BATCH_SIZE = 64
DOC_MAX_CHARS = 6000          # Tern caps an item at 8000; a section is ~600 words, a table can be longer
RETRY_AFTER_SEC = 60.0

_QWEN_TASK = "Given a question from a team member, retrieve the team documentation sections that answer it"


def query_text(model: str, query: str) -> str:
    if model.startswith("embeddinggemma"):
        return f"task: search result | query: {query}"
    if model.startswith("qwen3-embedding"):
        return f"Instruct: {_QWEN_TASK}\nQuery: {query}"
    return query


def doc_text(model: str, title: str, breadcrumb: list[str], heading: str, text: str) -> str:
    path = " > ".join([*breadcrumb[-2:], title] + ([heading] if heading else []))
    body = f"{path}\n{text}".strip()[:DOC_MAX_CHARS]
    if model.startswith("embeddinggemma"):
        return f"title: {title} | text: {body}"[:DOC_MAX_CHARS]
    return body


class EmbedUnavailable(RuntimeError):
    pass


class EmbedClient:
    """Tern's /embed over plain HTTP. Vectors come back L2-normalized (cosine = dot product)."""

    def __init__(self, url: str = DEFAULT_EMBED_URL, model: str = DEFAULT_EMBED_MODEL):
        self.url = url
        self.model = model
        self._down_until = 0.0
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.url and self.model)

    def embed(self, texts: list[str], timeout: float = BATCH_TIMEOUT_SEC) -> np.ndarray:
        if not self.enabled:
            raise EmbedUnavailable("embeddings are off (KNOWLEDGE_EMBED_URL not set)")
        with self._lock:
            if time.monotonic() < self._down_until:
                raise EmbedUnavailable("embedder failed recently; keyword-only for now")
        body = json.dumps({"model": self.model, "input": texts}).encode()
        req = urllib.request.Request(self.url, data=body, headers={"content-type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                vectors = json.load(resp)["embeddings"]
        except (urllib.error.URLError, TimeoutError, OSError, ValueError, KeyError) as exc:
            with self._lock:
                self._down_until = time.monotonic() + RETRY_AFTER_SEC
            raise EmbedUnavailable(f"{type(exc).__name__}: {exc}") from exc
        if len(vectors) != len(texts):
            raise EmbedUnavailable(f"asked for {len(texts)} vectors, got {len(vectors)}")
        return normalize(np.asarray(vectors, dtype=np.float32))

    def embed_query(self, query: str) -> np.ndarray:
        return self.embed([query_text(self.model, query)], timeout=QUERY_TIMEOUT_SEC)[0]


def normalize(m: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(m, axis=-1, keepdims=True)
    return m / np.where(norms == 0, 1, norms)


# --- the indexer side ---------------------------------------------------------------------------------------

def pending(conn: sqlite3.Connection, model: str, limit: int | None = None) -> list[sqlite3.Row]:
    sql = """SELECT c.id, c.heading, c.text, p.title, p.breadcrumb FROM chunks c JOIN pages p ON p.id = c.page_id
             LEFT JOIN embeddings e ON e.chunk_id = c.id AND e.model = ?
             WHERE e.chunk_id IS NULL ORDER BY c.id"""
    rows = conn.execute(sql + (f" LIMIT {int(limit)}" if limit else ""), (model,)).fetchall()
    return rows


def embed_pending(conn: sqlite3.Connection, client: EmbedClient, batch_size: int = BATCH_SIZE) -> tuple[int, int]:
    """Embeds every section without a vector for client.model. Returns (embedded, still pending). Stops at the
    first failure (thor down): what is done stays done, the rest waits for the next pass."""
    if not client.enabled:
        return 0, 0
    switched = conn.execute("DELETE FROM embeddings WHERE model != ?", (client.model,)).rowcount  # model switch
    conn.commit()
    rows = pending(conn, client.model)
    done = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        texts = [doc_text(client.model, r["title"], json.loads(r["breadcrumb"]), r["heading"], r["text"])
                 for r in batch]
        try:
            vectors = client.embed(texts)
        except EmbedUnavailable as exc:
            logger.warning("embedding stopped after %d section(s): %s", done, exc)
            break
        conn.executemany(
            "INSERT OR REPLACE INTO embeddings (chunk_id, model, dims, vector) VALUES (?, ?, ?, ?)",
            [(r["id"], client.model, int(v.shape[0]), v.astype(np.float32).tobytes()) for r, v in zip(batch, vectors)],
        )
        done += len(batch)
        _bump(conn)
        conn.commit()
    if switched and not done:
        _bump(conn)
        conn.commit()
    left = len(rows) - done
    return done, left


def _bump(conn: sqlite3.Connection) -> None:
    # tells the server to reload its vectors (see VectorCache)
    conn.execute("INSERT INTO meta (key, value) VALUES ('embeddings_gen', ?) ON CONFLICT(key) DO UPDATE SET "
                 "value = excluded.value", (repr(time.time()),))


# --- the server side ----------------------------------------------------------------------------------------

@dataclass
class VectorIndex:
    model: str
    chunk_ids: np.ndarray        # int64, one per row of `matrix`
    matrix: np.ndarray           # float32, L2-normalized rows

    def top(self, query_vec: np.ndarray, k: int) -> list[tuple[int, float]]:
        if len(self.chunk_ids) == 0:
            return []
        sims = self.matrix @ query_vec
        k = min(k, len(sims))
        idx = np.argpartition(-sims, k - 1)[:k]
        # deterministic: by similarity, then by chunk id
        order = sorted(idx.tolist(), key=lambda i: (-float(sims[i]), int(self.chunk_ids[i])))
        return [(int(self.chunk_ids[i]), float(sims[i])) for i in order]


def load_vectors(conn: sqlite3.Connection, model: str) -> VectorIndex:
    rows = conn.execute("SELECT chunk_id, vector FROM embeddings WHERE model = ? ORDER BY chunk_id", (model,)).fetchall()
    if not rows:
        return VectorIndex(model, np.zeros(0, dtype=np.int64), np.zeros((0, 0), dtype=np.float32))
    ids = np.fromiter((r[0] for r in rows), dtype=np.int64, count=len(rows))
    matrix = np.stack([np.frombuffer(r[1], dtype=np.float32) for r in rows])
    return VectorIndex(model, ids, matrix)


class VectorCache:
    """The server's in-memory copy of one model's vectors, reloaded when the indexer changes them: after each
    index pass (last_success) or embedding batch (embeddings_gen). Row count + highest id would not do: SQLite
    reuses the ids of a just-deleted page's sections when the page is re-indexed."""

    def __init__(self):
        self._key = None
        self._index: VectorIndex | None = None
        self._lock = threading.Lock()

    def get(self, conn: sqlite3.Connection, model: str) -> VectorIndex | None:
        try:
            meta = dict(conn.execute("SELECT key, value FROM meta WHERE key IN ('last_success', 'embeddings_gen')"))
            count = conn.execute("SELECT count(*) FROM embeddings WHERE model = ?", (model,)).fetchone()[0]
        except sqlite3.OperationalError:          # index from before phase 3: no embeddings table yet
            return None
        key = (model, meta.get("last_success"), meta.get("embeddings_gen"), count)
        with self._lock:
            if key != self._key or self._index is None:
                self._index = load_vectors(conn, model)
                self._key = key
            return self._index
