"""evil.telemetry.v2: the DAQ wire format since Apr 10 2026 20:26 UTC (sensor reader
commit `5dcec9e` "adding errcount to messages").

Exactly v1 plus a top-level integer `errcount`, with `motor.throttle` renamed
`motor.duty_cycle`. Whether duty_cycle and throttle are the same quantity is not assumed:
they are stored in separate columns. Data: `0411.csv` and the May B Lot CSVs in
shleeco26-analysis. See telemetry-v1-v2-shapes.md and _strict.py for what "strict" means.
"""

from __future__ import annotations

from evil.schemas._strict import CAR, StrictSchema, base_spec  # noqa: F401

SCHEMA_ID = "evil.telemetry.v2"
PARSER_VERSION = "telemetry.v2/1"

SPEC = base_spec("duty_cycle", errcount=True)
SCHEMA = StrictSchema(SCHEMA_ID, PARSER_VERSION, SPEC)
