"""The confidence test for the live path (live-next-steps-2026-11.md, section A):
the same messages, uploaded as a bag or received live, produce the same rows, turns,
straights and laps.

The live side is deliberately hostile: messages arrive at 10x speed with jitter and one
reconnect backlog flushed all at once, so any dependence on arrival timing would show.
The stream has what real data has: 50 Hz snapshots with a 1 Hz GPS fix, consecutive repeat
snapshots (only `_t_publish_ns` differs), a GPS dropout (NaN), and a few malformed
messages inside an otherwise conforming stream.
"""

from __future__ import annotations

import asyncio
import json
import random

import pytest

from evil import catalog, db, parser
from evil.ingestion.base import decode_json
from evil.ingestion.replay_source import ReplaySource
from evil.registered_classifiers import ALL_CLASSIFIERS
from evil.scripts.run_live_compiler import LiveCompiler
from telemetry_fixtures import payload, v2_payload, write_db3
from track_fixtures import RING_H, RING_W, ll, ring_point, seed_ring

P0 = 1_775_000_000.0          # publish clock at the first message (car side)
W0 = 1_790_000_000.0          # EVIL's clock when the live replay starts (unrelated to P0)
HZ = 50
SPEED = 8.0
LAPS = 2.3
RECORD_LATENCY = 0.003        # bag record time = publish + 3 ms


def _seed(conn):
    seed_ring(conn)
    lat, lon = ll(150.0, 0.0)
    conn.execute("INSERT INTO start_finish_line (center_lat, center_lon, radius_m) VALUES (?, ?, ?)", (lat, lon, 10.0))
    conn.commit()


def _stream(make, seed: int = 7) -> list[tuple[float, object]]:
    """[(publish_s, payload dict | raw text)] for LAPS laps of the ring."""
    rng = random.Random(seed)
    per = 2 * (RING_W + RING_H)
    n = int(LAPS * per / SPEED * HZ)
    out: list[tuple[float, object]] = []
    fix = None
    seq = 2
    for k in range(n):
        t = k / HZ
        if k % HZ == 0:
            x, y = ring_point(k * SPEED / HZ)
            fix = (*ll(x, y), 1_000_000_000 + int(t * 1e6))
        dropout = 40.0 <= t <= 45.0
        p = make(seq=seq, lat=None if dropout else fix[0], lon=fix[1], speed=SPEED,
                 voltage=50.0, current=8.0 + (k % 10) * 0.1, publish_ns=int((P0 + t) * 1e9))
        dev_us = 1_000_000_000 + int(t * 1e6)            # DAQ clock: real 20 ms steps
        p["global_ts"] = dev_us
        for group in ("power", "steering", "rpm_back", "motor"):
            p[group]["ts"] = dev_us - 300
        if not dropout:
            p["gps"]["ts"] = fix[2]
        out.append((P0 + t, p))
        if rng.random() < 0.3:                       # the publisher re-reading an unchanged snapshot
            again = json.loads(json.dumps(p))
            again["_t_publish_ns"] += 7_000_000
            out.append((P0 + t + 0.007, again))
        if k in (500, 4000):                         # malformed, well past the matcher's sample
            out.append((P0 + t + 0.009, "{not json"))
        if k == 2000:
            bad = json.loads(json.dumps(p))
            bad["gps"]["lat"] = "42.0"
            bad["_t_publish_ns"] += 9_000_000
            out.append((P0 + t + 0.009, bad))
        seq += 2
    return out


def _as_text(item) -> str:
    return item if isinstance(item, str) else json.dumps(item)


def _offline(tmp_path, stream, monkeypatch):
    monkeypatch.setattr(parser, "SAMPLE_HEAD", 50)
    monkeypatch.setattr(parser, "SAMPLE_SPREAD", 0)
    root = catalog.DataRoot(tmp_path / "data")
    root.ensure()
    cat = catalog.connect_catalog(root)
    path = catalog.parsed_db_path(root, "uc26")
    path.parent.mkdir(parents=True, exist_ok=True)
    c = db.connect(str(path))
    db.apply_schema(c)
    _seed(c)
    c.close()

    from telemetry_fixtures import encode_cdr_string
    bag = write_db3(tmp_path / "bag.db3", {"/spi_data": [
        (pub + RECORD_LATENCY, encode_cdr_string(_as_text(item))) for pub, item in stream]})
    staging = catalog.Staging(root, 10**10)
    with open(bag, "rb") as f:
        staging.add("bag.db3", f)
    rid = catalog.commit(cat, root, staging, catalog.UploadFields()).recording_id
    result = parser.parse_recording(root, rid)
    cat.close()
    assert result.status == "parsed", result.message
    return db.connect(str(path)), result.run_id, result


def _live(tmp_path, stream, seed: int = 11):
    rng = random.Random(seed)
    envs = []
    for i, (pub, item) in enumerate(stream):
        recv = W0 + (pub - P0) / 10 + rng.uniform(0.0, 0.004)    # 10x replay with jitter
        if 6000 <= i < 6400:
            recv = W0 + (stream[6400][0] - P0) / 10                # reconnect backlog: 400 messages at once
        envs.append(decode_json(_as_text(item), recv))
    conn = db.connect(str(tmp_path / "live.db"))
    db.apply_schema(conn)
    _seed(conn)
    compiler = LiveCompiler(conn, "live-1", ReplaySource(envs), ALL_CLASSIFIERS, max_interval_s=10.0)
    asyncio.run(asyncio.wait_for(compiler.run(), timeout=300))
    return conn, compiler


def _rows(conn, run_id):
    return [tuple(r) for r in conn.execute(
        """SELECT ms.device_seq, ms.device_global_ts_us, ms.publish_ns, ms.filtered_speed, ms.errcount,
                  g.lat, g.lon, g.speed, g.heading, g.device_ts_us, j.voltage, j.current,
                  m.rpm, m.throttle, m.duty_cycle, s.turn_angle, rb.rpm_left, rf.rpm_left
           FROM main_snapshot ms
           LEFT JOIN gps g ON g.id = ms.gps_id LEFT JOIN joulemeter j ON j.id = ms.joulemeter_id
           LEFT JOIN motor m ON m.id = ms.motor_id LEFT JOIN steering s ON s.id = ms.steering_id
           LEFT JOIN rpm_back rb ON rb.id = ms.rpm_back_id LEFT JOIN rpm_front rf ON rf.id = ms.rpm_front_id
           WHERE ms.run_id = ? ORDER BY ms.seq""", (run_id,))]


def _derived(conn, run_id, table, cols):
    return [tuple(r) for r in conn.execute(
        f"SELECT {cols} FROM {table} WHERE run_id = ? ORDER BY start_seq", (run_id,))]


@pytest.mark.parametrize("make", [payload, v2_payload], ids=["v1", "v2"])
def test_live_equals_offline(tmp_path, monkeypatch, make):
    stream = _stream(make)
    off, off_run, result = _offline(tmp_path, stream, monkeypatch)
    live, compiler = _live(tmp_path, stream)
    live_run = compiler.run_id

    assert [r for r, _ in compiler.runs] == ["live-1"]                     # one run, no false schema switch
    assert compiler.runs[0][1] == result.schema_id

    # every stored field of every row, in order
    off_rows, live_rows = _rows(off, off_run), _rows(live, live_run)
    assert len(live_rows) == len(off_rows) == result.rows_ingested
    assert live_rows == off_rows

    # the same messages were dropped as repeats and rejected, for the same reasons
    c = compiler.counters()["runs"][0]
    assert (c["duplicate"], c["rejected"]) == (result.rows_duplicate, result.rows_rejected) != (0, 0)
    assert [s["reason"] for s in c["reject_samples"]] == [s["reason"] for s in result.reject_samples]

    # row times: a constant offset apart (bag record time vs publish time on EVIL's clock), so every gap is equal
    off_ts = [r[0] for r in off.execute("SELECT global_ts FROM main_snapshot WHERE run_id = ? ORDER BY seq", (off_run,))]
    live_ts = [r[0] for r in live.execute("SELECT global_ts FROM main_snapshot WHERE run_id = ? ORDER BY seq", (live_run,))]
    shift = live_ts[0] - off_ts[0]
    assert max(abs((lt - ot) - shift) for lt, ot in zip(live_ts, off_ts)) < 1e-4

    # derived: identical boundaries and metrics
    turn_cols = "turn_def_id, start_seq - (SELECT MIN(seq) FROM main_snapshot WHERE run_id = turns.run_id), " \
                "end_seq - (SELECT MIN(seq) FROM main_snapshot WHERE run_id = turns.run_id), " \
                "ROUND(duration_s, 4), ROUND(distance_m, 3), ROUND(energy_wh, 5), entry_speed, exit_speed"
    off_turns, live_turns = _derived(off, off_run, "turns", turn_cols), _derived(live, live_run, "turns", turn_cols)
    assert len(off_turns) >= 5 and all(t[5] > 0 for t in off_turns) and live_turns == off_turns
    lap_cols = "lap_number, turn_count, ROUND(duration_s, 4), ROUND(distance_m, 3), ROUND(energy_wh, 5), " \
               "ROUND(efficiency_mi_per_kwh, 4)"
    off_laps, live_laps = _derived(off, off_run, "laps", lap_cols), _derived(live, live_run, "laps", lap_cols)
    assert len(off_laps) >= 2 and live_laps == off_laps
    straight_cols = "segment_id, ROUND(duration_s, 4), ROUND(energy_wh, 5)"
    assert _derived(live, live_run, "straights", straight_cols) == _derived(off, off_run, "straights", straight_cols)
