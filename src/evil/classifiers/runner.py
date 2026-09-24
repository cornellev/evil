"""The incremental compiler loop. Each classifier gets its own cursor and
its own configurable trailing margin instead of one shared cursor, so new
classifiers can depend on existing ones (or just on raw data) without the
runner itself changing. See inference-agent/4.md section 4 for the design
rationale and the alternatives considered (CDC, queue offsets, dirty flags)."""

from __future__ import annotations

import sqlite3
import time

from evil.classifiers.base import Classifier
from evil.classifiers.registry import topological_order


def get_cursor(conn: sqlite3.Connection, run_id: str, name: str, version: int) -> int:
    row = conn.execute(
        """SELECT last_processed_seq FROM classifier_cursor
           WHERE run_id = ? AND classifier_name = ? AND classifier_version = ?""",
        (run_id, name, version),
    ).fetchone()
    return row["last_processed_seq"] if row else 0


def _set_cursor(
    conn: sqlite3.Connection, run_id: str, name: str, version: int, seq: int, now_ts: float
) -> None:
    conn.execute(
        """INSERT INTO classifier_cursor
               (run_id, classifier_name, classifier_version, last_processed_seq, updated_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (run_id, classifier_name, classifier_version)
           DO UPDATE SET last_processed_seq = excluded.last_processed_seq,
                         updated_at = excluded.updated_at""",
        (run_id, name, version, seq, now_ts),
    )


def _max_raw_seq_before(conn: sqlite3.Connection, run_id: str, cutoff_ts: float) -> int:
    row = conn.execute(
        "SELECT MAX(seq) AS max_seq FROM main_snapshot WHERE run_id = ? AND global_ts <= ?",
        (run_id, cutoff_ts),
    ).fetchone()
    return row["max_seq"] or 0


def _safe_until_seq(
    conn: sqlite3.Connection,
    run_id: str,
    classifier: Classifier,
    by_name: dict[str, Classifier],
    now_ts: float,
) -> int:
    bounds = []
    for dep in classifier.spec.depends_on:
        if dep == "main_snapshot":
            cutoff = now_ts - classifier.spec.lookback_margin_s
            bounds.append(_max_raw_seq_before(conn, run_id, cutoff))
        else:
            dep_classifier = by_name.get(dep)
            if dep_classifier is None:
                raise ValueError(
                    f"classifier {classifier.spec.name!r} depends_on {dep!r}, "
                    f"but it wasn't included in this tick() call -- register every "
                    f"dependency alongside the classifiers that need it"
                )
            bounds.append(get_cursor(conn, run_id, dep_classifier.spec.name, dep_classifier.spec.version))
    return min(bounds) if bounds else 0


def tick(
    conn: sqlite3.Connection,
    run_id: str,
    classifiers: list[Classifier],
    now_ts: float | None = None,
) -> dict[str, int]:
    """Run one incremental pass for run_id across all registered classifiers,
    in dependency order. Returns {classifier_name: seq it advanced to} for
    whichever classifiers actually had new safe data this tick."""
    now_ts = time.time() if now_ts is None else now_ts
    ordered = topological_order(classifiers)
    by_name = {c.spec.name: c for c in classifiers}
    advanced: dict[str, int] = {}

    for classifier in ordered:
        since = get_cursor(conn, run_id, classifier.spec.name, classifier.spec.version)
        safe_until = _safe_until_seq(conn, run_id, classifier, by_name, now_ts)
        if safe_until <= since:
            continue
        with conn:
            classifier.run(conn, run_id, since, safe_until)
            _set_cursor(conn, run_id, classifier.spec.name, classifier.spec.version, safe_until, now_ts)
        advanced[classifier.spec.name] = safe_until

    return advanced
