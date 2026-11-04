"""One ingestion pipeline for uploads and the live feed, so the same message produces the
same rows whichever way it arrived (live-next-steps-2026-11.md section A).

Sources (file readers, Ros2Source, a future ZenohSource) only receive and decode: they
yield `Envelope(ts, payload, error)`. Everything after that lives here:

    validate against the schema -> reject + count -> drop consecutive repeats ->
    count seq conflicts -> map to RawSample

`SchemaStream` does that for a known schema (uploads: detected up front from a sample of
the file). `LiveDetector` finds the schema of a stream that arrives one message at a time:
it buffers the first LOCK_AFTER messages, detects on them, locks, and then feeds every
message to a SchemaStream. If SWITCH_AFTER consecutive messages conform to a *different*
known schema (the DAQ was reflashed mid-session), it reports a schema change so the
caller can close the run and start a new one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from evil import schemas
from evil.models import RawSample
from evil.schemas._strict import StrictSchema

MAX_REJECT_SAMPLES = 5
LOCK_AFTER = 20      # ~0.4 s at 50 Hz
SWITCH_AFTER = 20


@dataclass(frozen=True)
class Envelope:
    """One received message. `ts` is EVIL's row time in seconds (container record time for
    files, receive time for live). `payload` is the decoded JSON value (any type: validation
    is the pipeline's job); `error` is set instead when the message could not be decoded."""
    ts: float
    payload: Any = None
    error: str | None = None


@dataclass
class Counters:
    seen: int = 0
    stored: int = 0
    duplicate: int = 0
    rejected: int = 0
    seq_conflicts: int = 0
    reject_samples: list[dict] = field(default_factory=list)
    first_ts: float | None = None
    last_ts: float | None = None

    def reject(self, index: int, ts: float, reason: str) -> None:
        self.rejected += 1
        if len(self.reject_samples) < MAX_REJECT_SAMPLES:
            self.reject_samples.append({"index": index, "ts": ts, "reason": reason})


class SchemaStream:
    """Validate -> reject -> repeat drop -> map, for one run of one known schema."""

    def __init__(self, schema: StrictSchema, run_id: str):
        self.schema = schema
        self.run_id = run_id
        self.counters = Counters()
        self._prev_key: str | None = None
        self._prev_seq: int | None = None

    def feed(self, env: Envelope) -> RawSample | None:
        """The sample to store for this message, or None (rejected or a repeat)."""
        c = self.counters
        index = c.seen
        c.seen += 1
        reason = env.error
        if reason is None:
            bad = self.schema.validate(env.payload)
            reason = "; ".join(bad) if bad else None
        if reason is not None:
            c.reject(index, env.ts, reason)
            return None
        payload = env.payload
        key = self.schema.repeat_key(payload)
        if key == self._prev_key:
            c.duplicate += 1
            return None
        if self._prev_seq is not None and payload["seq"] == self._prev_seq:
            c.seq_conflicts += 1  # same DAQ seq, different body: kept, but worth knowing
        self._prev_key, self._prev_seq = key, payload["seq"]
        c.stored += 1
        c.first_ts = env.ts if c.first_ts is None else c.first_ts
        c.last_ts = env.ts
        return self.schema.to_sample(self.run_id, payload, env.ts)


@dataclass
class Fed:
    """What LiveDetector.feed produced for one message."""
    samples: list[RawSample] = field(default_factory=list)
    locked: StrictSchema | None = None          # set on the message that locked a schema
    switched_to: StrictSchema | None = None     # set when a schema change was detected


class LiveDetector:
    """Detect, lock and watch the schema of a live stream; owns the run's SchemaStream.

    `new_run_id()` is called each time a schema is locked (first lock and after a schema
    change), so the caller decides run naming. Messages before a lock, and messages that
    match no schema while searching, are held until the lock decision, then fed in order
    (so nothing that conforms is lost) or rejected as unknown.
    """

    def __init__(self, new_run_id, lock_after: int = LOCK_AFTER, switch_after: int = SWITCH_AFTER):
        self._new_run_id = new_run_id
        self.lock_after = lock_after
        self.switch_after = switch_after
        self.stream: SchemaStream | None = None
        self.closed_streams: list[SchemaStream] = []
        self.unknown = Counters()                 # messages while no schema is locked that matched nothing
        self._pending: list[Envelope] = []        # before the first lock, or a run of other-schema messages
        self._pending_schema: StrictSchema | None = None

    @property
    def schema(self) -> StrictSchema | None:
        return self.stream.schema if self.stream else None

    def _lock(self, schema: StrictSchema, fed: Fed) -> None:
        if self.stream is not None:
            self.closed_streams.append(self.stream)
            fed.switched_to = schema
        else:
            fed.locked = schema
        self.stream = SchemaStream(schema, self._new_run_id(schema))
        for env in self._pending:
            sample = self.stream.feed(env)
            if sample is not None:
                fed.samples.append(sample)
        self._pending = []
        self._pending_schema = None

    def _reject_unknown(self, env: Envelope, reason: str) -> None:
        index = self.unknown.seen
        self.unknown.seen += 1
        self.unknown.reject(index, env.ts, reason)

    def feed(self, env: Envelope) -> Fed:
        fed = Fed()
        payload_schema = schemas.schema_of(env.payload) if env.error is None else None

        if self.stream is None:
            # Searching: hold messages until lock_after of them agree on one schema.
            if payload_schema is None:
                self._reject_unknown(env, env.error or "matches no known schema")
                return fed
            if self._pending_schema is not payload_schema:
                for held in self._pending:   # a different schema interrupted the run: those are not lockable
                    self._reject_unknown(held, f"interrupted run of {self._pending_schema.schema_id}")
                self._pending = []
                self._pending_schema = payload_schema
            self._pending.append(env)
            if len(self._pending) >= self.lock_after:
                self._lock(payload_schema, fed)
            return fed

        # Locked: conforming messages go straight through; a sustained run of another
        # known schema is a schema change.
        if payload_schema is not None and payload_schema is not self.stream.schema:
            if self._pending_schema is not payload_schema:
                self._flush_pending_into_stream(fed)
                self._pending_schema = payload_schema
            self._pending.append(env)
            if len(self._pending) >= self.switch_after:
                self._lock(payload_schema, fed)
            return fed

        self._flush_pending_into_stream(fed)
        sample = self.stream.feed(env)
        if sample is not None:
            fed.samples.append(sample)
        return fed

    def _flush_pending_into_stream(self, fed: Fed) -> None:
        """A short run of other-schema messages that did not become a switch: they are
        rejects of the locked schema (fed so they are counted the same way)."""
        for held in self._pending:
            sample = self.stream.feed(held)
            if sample is not None:          # cannot happen (they do not conform), kept for safety
                fed.samples.append(sample)
        self._pending = []
        self._pending_schema = None

    def finish(self) -> Fed:
        """End of stream: held messages are fed (a short tail of another schema) or
        reported unknown (stream ended before anything locked)."""
        fed = Fed()
        if self.stream is None:
            if len(self._pending) and self._pending_schema is not None:
                # Fewer than lock_after messages in total but all one schema: lock on them.
                self._lock(self._pending_schema, fed)
        else:
            self._flush_pending_into_stream(fed)
        return fed


class LiveClock:
    """Row time for live messages: the publisher's `_t_publish_ns`, shifted onto EVIL's
    receive clock.

    Why not receive time: a replay at 10x, a reconnect backlog flushed at 500 Hz, or network
    jitter all distort receive-time gaps, and every lap time / energy integral depends on
    those gaps. The publish stamp keeps the car-side timing, so a live run matches the
    upload of the same data. The offset (receive - publish) is anchored on the first message
    and re-anchored only when the car's clock itself jumps (NTP sync after boot): consecutive
    publish gaps disagreeing with receive gaps by more than JUMP_S. Messages without an
    integer `_t_publish_ns` (unknown shapes) keep their receive time.
    """

    JUMP_S = 30.0

    def __init__(self) -> None:
        self.offset: float | None = None
        self.clock_jumps = 0
        self._prev: tuple[float, float] | None = None   # (publish_s, receive_s)

    def reset(self) -> None:
        self.offset, self._prev = None, None

    def stamp(self, env: Envelope) -> Envelope:
        payload = env.payload
        pub = payload.get("_t_publish_ns") if isinstance(payload, dict) else None
        if not isinstance(pub, int) or isinstance(pub, bool):
            return env
        pub_s, recv_s = pub / 1e9, env.ts
        if self.offset is None:
            self.offset = recv_s - pub_s
        elif self._prev is not None:
            d_pub, d_recv = pub_s - self._prev[0], recv_s - self._prev[1]
            if abs(d_pub - d_recv) > self.JUMP_S:
                prev_ts = self._prev[0] + self.offset
                self.offset = prev_ts + max(d_recv, 0.0) - pub_s
                self.clock_jumps += 1
        self._prev = (pub_s, recv_s)
        return Envelope(pub_s + self.offset, payload, env.error)
