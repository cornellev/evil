"""The extension point for adding a new derived table. A new classifier is
one file implementing this protocol plus one line in the registry's list --
the runner, the cursor table, and every other classifier are untouched."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Protocol, Sequence


@dataclass(frozen=True)
class ClassifierSpec:
    """Attributes:
    depends_on: "main_snapshot" for a leaf classifier reading raw data, or
        another classifier's `name` to sequence after its output (e.g.
        laps -> turns).
    lookback_margin_s: How far behind the latest ingested row this
        classifier is allowed to look before it's willing to finalize a
        row. Segmentation classifiers (turns) need enough margin to see a
        segment close; a metric with no "did it end yet" question can use 0.
    """

    name: str
    version: int
    depends_on: Sequence[str]
    lookback_margin_s: float


class Classifier(Protocol):
    spec: ClassifierSpec

    def run(self, conn: sqlite3.Connection, run_id: str, since_seq: int, until_seq: int) -> None:
        """Process main_snapshot rows with since_seq < seq <= until_seq for
        run_id and upsert results into this classifier's own derived table.
        Must be safe to call again with the same range (idempotent) since a
        crash between commit and cursor advance replays the same range."""
        ...
