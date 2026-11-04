"""evil.telemetry.v1: the strict wire format of uc26's DAQ telemetry.

The source of truth is `read_snapshot_dict()` in cornellev/uc26_sensor_reader,
republished as JSON in a std_msgs/String by tailscale-ros-telemetry (which adds
`_t_publish_ns`). See recording-catalog-design.md section 4.1 and
telemetry-v1-v2-shapes.md for how this was verified against real data.

Strict means: exact key set at every level (nothing missing, extra or renamed;
case-sensitive), `seq`/`global_ts`/`_t_publish_ns` and every `ts` are integers,
every other leaf is a number where NaN is a valid number (real data is full of
it: no GPS fix, unplugged sensors). Booleans, strings, null and nested
surprises are rejected. Any change to this shape is a NEW schema id, never an
edit here. A message that does not match exactly is non-conforming.
"""

from __future__ import annotations

from evil.schemas._strict import CAR, MAX_REASONS, NUM as _NUM, INT as _INT, StrictSchema, base_spec  # noqa: F401

SCHEMA_ID = "evil.telemetry.v1"
PARSER_VERSION = "telemetry.v1/1"

SPEC = base_spec("throttle", errcount=False)
SCHEMA = StrictSchema(SCHEMA_ID, PARSER_VERSION, SPEC)

flat_keys = SCHEMA.flat_keys
validate = SCHEMA.validate
conforms = SCHEMA.conforms
repeat_key = SCHEMA.repeat_key
to_sample = SCHEMA.to_sample
