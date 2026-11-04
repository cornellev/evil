"""evil.telemetry.v1-dutycycle: the transitional shape between two sensor reader commits
on Apr 10 2026 (`e450883` "replaced throttle with duty cycle" at 12:10 UTC, `5dcec9e`
"adding errcount" at 20:26 UTC): v1 with `motor.throttle` renamed `motor.duty_cycle`,
no `errcount`. Only known recording: thor's `bag_20260410-124638` (231 messages).
"""

from __future__ import annotations

from evil.schemas._strict import CAR, StrictSchema, base_spec  # noqa: F401

SCHEMA_ID = "evil.telemetry.v1-dutycycle"
PARSER_VERSION = "telemetry.v1-dutycycle/1"

SPEC = base_spec("duty_cycle", errcount=False)
SCHEMA = StrictSchema(SCHEMA_ID, PARSER_VERSION, SPEC)
