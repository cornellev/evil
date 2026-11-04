"""Registry of the strict telemetry schemas EVIL can parse, and content-based detection.

A stream is one schema when every sampled message conforms exactly to it. The known
shapes are pairwise disjoint (they differ in exact key sets), so at most one matches; a
stream matching none is "neither" (stored, cataloged, not tabulated). The same detection
is used for uploads (parser.py) and the live feed (ingestion/pipeline.py).
"""

from __future__ import annotations

from typing import Any, Iterable

from evil.schemas import telemetry_v1, telemetry_v1_dutycycle, telemetry_v2
from evil.schemas._strict import StrictSchema

SCHEMAS: tuple[StrictSchema, ...] = (telemetry_v1.SCHEMA, telemetry_v2.SCHEMA, telemetry_v1_dutycycle.SCHEMA)
BY_ID: dict[str, StrictSchema] = {s.schema_id: s for s in SCHEMAS}


def schema_of(payload: Any) -> StrictSchema | None:
    """The one schema this single payload conforms to, or None."""
    for schema in SCHEMAS:
        if schema.conforms(payload):
            return schema
    return None


def coverage(payloads: Iterable[Any]) -> tuple[dict[str, int], int]:
    """({schema_id: conforming count}, total) over a sample. Entries that are not dicts
    (undecodable messages) count toward the total and conform to nothing."""
    counts = {s.schema_id: 0 for s in SCHEMAS}
    total = 0
    for payload in payloads:
        total += 1
        schema = schema_of(payload)
        if schema is not None:
            counts[schema.schema_id] += 1
    return counts, total


def detect(payloads: Iterable[Any]) -> StrictSchema | None:
    """The schema EVERY payload in the sample conforms to; None if the sample is empty,
    mixed, or matches nothing."""
    counts, total = coverage(payloads)
    if not total:
        return None
    for schema in SCHEMAS:
        if counts[schema.schema_id] == total:
            return schema
    return None
