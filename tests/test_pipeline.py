"""Schema registry, the shared SchemaStream, live schema detection and the live clock."""

from __future__ import annotations

import pytest

from evil import schemas
from evil.ingestion.base import decode_json
from evil.ingestion.pipeline import Envelope, LiveClock, LiveDetector, SchemaStream
from evil.schemas import telemetry_v1, telemetry_v1_dutycycle, telemetry_v2
from telemetry_fixtures import payload, transitional_payload, v2_payload

V1, V2, TR = telemetry_v1.SCHEMA, telemetry_v2.SCHEMA, telemetry_v1_dutycycle.SCHEMA


# ---- registry ------------------------------------------------------------

@pytest.mark.parametrize("make,schema", [(payload, V1), (v2_payload, V2), (transitional_payload, TR)])
def test_each_shape_conforms_to_exactly_one_schema(make, schema):
    p = make()
    assert [s.schema_id for s in schemas.SCHEMAS if s.conforms(p)] == [schema.schema_id]
    assert schemas.schema_of(p) is schema


def test_v2_spec_is_v1_plus_errcount_with_throttle_renamed():
    assert V2.flat_keys() - V1.flat_keys() == {"errcount", "motor.duty_cycle"}
    assert V1.flat_keys() - V2.flat_keys() == {"motor.throttle"}
    assert TR.flat_keys() == V2.flat_keys() - {"errcount"}


def test_detect_needs_every_sampled_message_to_agree():
    assert schemas.detect([payload(seq=i) for i in range(5)]) is V1
    assert schemas.detect([v2_payload(seq=i) for i in range(5)]) is V2
    assert schemas.detect([payload(), v2_payload()]) is None          # mixed
    assert schemas.detect([payload(), {"hello": 1}]) is None           # one junk message
    assert schemas.detect([{"a": 1}, None, "x"]) is None                # neither
    assert schemas.detect([]) is None


def test_errcount_must_be_an_integer():
    p = v2_payload()
    p["errcount"] = 1.5
    assert schemas.schema_of(p) is None
    assert "errcount: expected integer" in V2.validate(p)[0]


def test_to_sample_maps_the_v2_only_fields():
    p = v2_payload(seq=4)
    p["motor"]["duty_cycle"] = 0.25
    s = V2.to_sample("r", p, 1.0)
    assert s.errcount == 35 and s.motor.duty_cycle == 0.25 and s.motor.throttle is None
    s1 = V1.to_sample("r", payload(seq=4), 1.0)
    assert s1.errcount is None and s1.motor.duty_cycle is None and s1.motor.throttle == 0.0


# ---- SchemaStream --------------------------------------------------------

def test_schema_stream_rejects_drops_repeats_and_flags_seq_conflicts():
    st = SchemaStream(V1, "r")
    a = payload(seq=2)
    repeat = dict(a, _t_publish_ns=a["_t_publish_ns"] + 1)
    conflict = payload(seq=2, speed=9.0)
    out = [st.feed(Envelope(i, p)) for i, p in enumerate([a, repeat, v2_payload(seq=4), conflict])]
    out.append(st.feed(Envelope(9.0, None, "not JSON: Expecting value")))
    assert [o is not None for o in out] == [True, False, False, True, False]
    c = st.counters
    assert (c.seen, c.stored, c.duplicate, c.rejected, c.seq_conflicts) == (5, 2, 1, 2, 1)
    assert c.reject_samples[0]["index"] == 2 and "unexpected key 'errcount'" in c.reject_samples[0]["reason"]
    assert c.reject_samples[1]["reason"].startswith("not JSON")


# ---- LiveDetector --------------------------------------------------------

def _det(lock=3, switch=3):
    ids = []

    def new_run_id(schema):
        ids.append(f"run-{len(ids) + 1}")
        return ids[-1]
    return LiveDetector(new_run_id, lock_after=lock, switch_after=switch), ids


def _feed_all(det, items):
    samples, events = [], []
    for i, item in enumerate(items):
        env = item if isinstance(item, Envelope) else Envelope(float(i), item)
        fed = det.feed(env)
        samples += fed.samples
        if fed.locked:
            events.append(("locked", fed.locked.schema_id))
        if fed.switched_to:
            events.append(("switched", fed.switched_to.schema_id))
    fed = det.finish()
    samples += fed.samples
    if fed.locked:
        events.append(("locked", fed.locked.schema_id))
    return samples, events


def test_locks_after_n_agreeing_messages_and_keeps_the_ones_it_held():
    det, ids = _det()
    samples, events = _feed_all(det, [payload(seq=2 * i) for i in range(5)])
    assert events == [("locked", V1.schema_id)]
    assert [s.device_seq for s in samples] == [0, 2, 4, 6, 8]          # nothing lost while detecting
    assert {s.run_id for s in samples} == {"run-1"}


def test_neither_is_never_stored_and_is_counted():
    det, ids = _det()
    samples, events = _feed_all(det, [{"speed": i} for i in range(10)] + [Envelope(99.0, None, "not JSON: x")])
    assert samples == [] and events == [] and ids == []
    assert det.unknown.rejected == 11 and det.unknown.reject_samples[0]["reason"] == "matches no known schema"


def test_junk_while_searching_does_not_block_the_lock():
    det, _ = _det()
    items = [payload(seq=2), {"junk": 1}, payload(seq=4), payload(seq=6)]
    samples, events = _feed_all(det, items)
    assert events == [("locked", V1.schema_id)] and len(samples) == 3 and det.unknown.rejected == 1


def test_a_short_stream_still_locks_at_the_end():
    det, _ = _det(lock=20)
    samples, events = _feed_all(det, [v2_payload(seq=2), v2_payload(seq=4)])
    assert events == [("locked", V2.schema_id)] and len(samples) == 2


def test_a_sustained_switch_closes_the_run_and_opens_a_new_one():
    det, ids = _det()
    items = [payload(seq=2 * i) for i in range(4)] + [v2_payload(seq=100 + 2 * i) for i in range(4)]
    samples, events = _feed_all(det, items)
    assert events == [("locked", V1.schema_id), ("switched", V2.schema_id)]
    assert ids == ["run-1", "run-2"]
    assert [(s.run_id, s.device_seq) for s in samples][-4:] == [("run-2", 100), ("run-2", 102), ("run-2", 104),
                                                                 ("run-2", 106)]
    assert det.closed_streams[0].counters.stored == 4 and det.stream.counters.stored == 4


def test_a_brief_burst_of_another_schema_is_rejected_not_a_switch():
    det, ids = _det()
    items = [payload(seq=2 * i) for i in range(4)] + [v2_payload(seq=50), v2_payload(seq=52)] + [payload(seq=60)]
    samples, events = _feed_all(det, items)
    assert events == [("locked", V1.schema_id)] and ids == ["run-1"]
    assert det.stream.counters.rejected == 2 and det.stream.counters.stored == 5


def test_decode_json_accepts_bare_nan_and_reports_garbage():
    env = decode_json('{"a": NaN}', 5.0)
    assert env.error is None and env.payload["a"] != env.payload["a"]
    bad = decode_json("{nope", 5.0)
    assert bad.payload is None and bad.error.startswith("not JSON")


# ---- LiveClock -----------------------------------------------------------

def _env(recv, pub_s):
    return Envelope(recv, {"_t_publish_ns": int(pub_s * 1e9)})


def test_live_clock_keeps_publisher_gaps_whatever_the_arrival_timing():
    clock = LiveClock()
    pubs = [100.0, 100.02, 100.04, 100.06]
    recvs = [5000.0, 5000.002, 5000.004, 5000.004]               # 10x replay, then a burst
    ts = [clock.stamp(_env(r, p)).ts for r, p in zip(recvs, pubs)]
    assert ts[0] == 5000.0 and [round(b - a, 6) for a, b in zip(ts, ts[1:])] == [0.02, 0.02, 0.02]


def test_live_clock_reanchors_when_the_car_clock_jumps():
    clock = LiveClock()
    t1 = clock.stamp(_env(5000.0, 100.0)).ts
    t2 = clock.stamp(_env(5000.02, 100.02)).ts
    t3 = clock.stamp(_env(5000.04, 3_700_000.0)).ts               # NTP sync after boot: +1000 h
    t4 = clock.stamp(_env(5000.06, 3_700_000.02)).ts
    assert clock.clock_jumps == 1
    assert t2 - t1 == pytest.approx(0.02) and t3 - t2 == pytest.approx(0.02) and t4 - t3 == pytest.approx(0.02)


def test_live_clock_leaves_messages_without_a_publish_stamp_on_receive_time():
    clock = LiveClock()
    assert clock.stamp(Envelope(7.0, {"x": 1})).ts == 7.0
    assert clock.stamp(Envelope(8.0, None, "not JSON")).ts == 8.0


# ---- LossMeter -----------------------------------------------------------

def _pub(t):
    return Envelope(0.0, {"_t_publish_ns": int(t * 1e9)})


def test_loss_meter_counts_clear_holes_and_ignores_jitter():
    from evil.ingestion.pipeline import LossMeter
    import random
    rng = random.Random(3)
    m = LossMeter()
    t = 1000.0
    for k in range(3000):
        if k in (500, 501, 502):         # 3 messages lost
            t += 0.02
            continue
        if 1500 <= k < 1505:             # 5 lost
            t += 0.02
            continue
        if k == 2500:                    # 1 lost: indistinguishable from jitter, by design not counted
            t += 0.02
            continue
        m.observe(_pub(t + rng.uniform(-0.004, 0.004)))   # real-data jitter
        t += 0.02
    s = m.stats()
    assert s["holes"] == 2 and s["missing"] == 8 and s["outages"] == 0
    assert abs(s["period_s"] - 0.02) < 0.001


def test_loss_meter_separates_outages_and_adapts_to_the_rate():
    from evil.ingestion.pipeline import LossMeter
    m = LossMeter()
    ts = [i * 0.1 for i in range(50)] + [4.9 + 10.0 + i * 0.1 for i in range(50)]   # 10 Hz, a 10 s outage
    for t in ts:
        m.observe(_pub(t))
    m.observe(_pub(1.0))                                                        # backwards
    s = m.stats()
    assert abs(s["period_s"] - 0.1) < 1e-6 and s["missing"] == 0
    assert s["outages"] == 1 and abs(s["outage_s"] - 10.0) < 1e-6 and s["backwards"] == 1


# ---- ThreadBridge --------------------------------------------------------

def test_thread_bridge_never_blocks_the_producer_and_counts_every_drop():
    import asyncio
    import threading
    from evil.ingestion.base import ThreadBridge

    async def scenario():
        bridge = ThreadBridge(asyncio.get_running_loop(), maxsize=100)
        n = 5000

        def producer():
            for i in range(n):
                bridge.push(Envelope(float(i), {"i": i}))

        th = threading.Thread(target=producer)
        th.start()
        th.join(timeout=5)                      # the consumer is not reading: the producer must still finish
        assert not th.is_alive()
        await asyncio.sleep(0.05)               # let the scheduled puts run
        got = []
        while not bridge.queue.empty():
            got.append(bridge.queue.get_nowait().payload["i"])
        return bridge, got, n

    bridge, got, n = asyncio.run(scenario())
    assert bridge.received == n and len(got) == 100 and bridge.overflow == n - 100
    assert got == list(range(100))              # order kept; the newest are the ones dropped
    assert bridge.stats()["max_queue_depth"] == 100
