"""IngestionSource reading a previously recorded CSV export into RawSamples,
for bulk historical ingestion (see evil/src/evil/scripts/ingest_recording.py).
A CSV row is just a flat JSON-shaped dict with string values, so this routes
through the same normalize.to_raw_sample() every other source uses.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import AsyncIterator

from evil.ingestion.normalize import to_raw_sample
from evil.models import RawSample


class CsvFileSource:
    def __init__(self, run_id: str, path: str | Path):
        self.run_id = run_id
        self.path = Path(path)

    async def samples(self) -> AsyncIterator[RawSample]:
        with self.path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row_index, raw_row in enumerate(reader):
                yield to_raw_sample(self.run_id, dict(raw_row), default_ts=float(row_index))
