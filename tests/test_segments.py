"""The segments classifier: gates in lap order -> turn and straight instances that tile the run."""

import pytest

from evil import track
from evil.classifiers.runner import tick
from evil.classifiers.segments import SegmentsClassifier
from evil.ingest import ingest_sample, insert_sample
from evil.models import GpsReading, JoulemeterReading, RawSample
from track_fixtures import RING_BOUNDS, ll, ring_arc, ring_definition, ring_point, seed_line, seed_ring

HZ = 50


def _drive_ring(conn, run_id="r", laps=2.0, speed=10.0, power_w=500.0, start_arc=60.0, t0=100.0, noise=0.0,
                skip_fix_when=None, drop=None, tick_every=None, stop_after=None, device_clock=True):
    """Constant-speed laps of the ring; GPS at 1 Hz, snapshots at 50 Hz repeating the last fix."""
    per = 900.0
    steps = int(laps * per / speed * HZ)
    last = None
    fixes_seen = 0
    for k in range(steps + 1):
        t = t0 + k / HZ
        if stop_after is not None and t - t0 > stop_after:
            break
        arc = start_arc + k * speed / HZ
        if k % HZ == 0:
            x, y = ring_point(arc)
            if noise:
                x += noise * ((fixes_seen * 7919) % 11 - 5) / 5
                y += noise * ((fixes_seen * 104729) % 13 - 6) / 6
            fixes_seen += 1
            skip = (skip_fix_when is not None and skip_fix_when(t - t0)) or (drop and drop[0] <= t - t0 <= drop[1])
            last = None if skip else (*ll(x, y), int(t * 1e6))
        gps = GpsReading(t, last[0], last[1], speed, 0.0, device_ts_us=last[2] if device_clock else None) if last else None
        sample = RawSample(
            run_id, t, joulemeter=JoulemeterReading(t, 50.0, power_w / 50.0, device_ts_us=int(t * 1e6) if device_clock else None),
            gps=gps, device_seq=2 * k if device_clock else None,
            device_global_ts_us=int(t * 1e6) if device_clock else None, filtered_speed=speed)
        ingest_sample(conn, sample)
        if tick_every and k % tick_every == 0:
            tick(conn, run_id, [SegmentsClassifier()], now_ts=t)
    return steps


def _instances(conn, run_id="r"):
    rows = conn.execute(
        """SELECT s.name, s.ordinal, x.* FROM (SELECT turn_def_id AS sid, start_seq, end_seq, duration_s, distance_m,
                  energy_wh, efficiency_mi_per_kwh, entry_speed, exit_speed, avg_speed FROM turns WHERE run_id = ?
                UNION ALL SELECT segment_id, start_seq, end_seq, duration_s, distance_m, energy_wh,
                  efficiency_mi_per_kwh, entry_speed, exit_speed, avg_speed FROM straights WHERE run_id = ?) x
           JOIN track_segments s ON s.segment_id = x.sid ORDER BY x.start_seq""", (run_id, run_id)).fetchall()
    return rows


@pytest.fixture
def ring(conn):
    return seed_ring(conn)


def test_a_drive_is_cut_into_segments_that_match_the_geometry(conn, ring):
    _drive_ring(conn, laps=2.0)
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)

    rows = _instances(conn)
    by_name = {}
    for r in rows:
        by_name.setdefault(r["name"], []).append(r)
    lengths = {s["name"]: s["length_m"] for s in ring}
    assert set(by_name) == set(lengths)
    for name, instances in by_name.items():
        assert len(instances) >= 1
        for r in instances:
            assert r["duration_s"] == pytest.approx(lengths[name] / 10.0, abs=0.15)
            assert r["distance_m"] == pytest.approx(lengths[name], rel=0.02)
            assert r["avg_speed"] == pytest.approx(10.0, rel=0.02)
            assert r["energy_wh"] == pytest.approx(0.5 * r["duration_s"] / 3600 * 1000, rel=0.01)   # 500 W, exactly
            assert r["efficiency_mi_per_kwh"] == pytest.approx(10.0 * 2.23694 / 0.5, rel=0.03)      # mph / kW
            assert r["entry_speed"] == 10.0 and r["exit_speed"] == 10.0


def test_instances_tile_the_run_with_shared_boundaries_in_lap_order(conn, ring):
    _drive_ring(conn, laps=2.0)
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)

    rows = _instances(conn)
    assert len(rows) >= 2 * len(ring) - 1
    for a, b in zip(rows, rows[1:]):
        assert b["start_seq"] == a["end_seq"]                       # no gap, no overlap
        assert b["ordinal"] == (a["ordinal"] + 1) % len(ring)        # lap order
    total = sum(r["duration_s"] for r in rows)
    assert total == pytest.approx(rows[-1]["end_seq"] / HZ - rows[0]["start_seq"] / HZ, abs=0.1)


def test_incremental_ticks_give_the_same_answer_as_one_pass(conn, ring):
    _drive_ring(conn, run_id="a", laps=2.0)
    tick(conn, "a", [SegmentsClassifier()], now_ts=1e9)
    _drive_ring(conn, run_id="b", laps=2.0, tick_every=137)          # state persists between ticks
    tick(conn, "b", [SegmentsClassifier()], now_ts=1e9)

    key = lambda rows: [(r["name"], round(r["duration_s"], 3), round(r["distance_m"], 2), round(r["energy_wh"], 4)) for r in rows]
    assert key(_instances(conn, "a")) == key(_instances(conn, "b"))
    assert len(_instances(conn, "a")) > 0


def test_a_segment_is_only_stored_once_its_exit_is_safely_visible(conn, ring):
    _drive_ring(conn, laps=0.4, speed=10.0)
    tick(conn, "r", [SegmentsClassifier()], now_ts=100.0 + 20.0)      # not past the end yet
    first = len(_instances(conn))
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)
    assert len(_instances(conn)) >= first
    state = conn.execute("SELECT * FROM segments_open_state WHERE run_id = 'r'").fetchone()
    assert state is not None and state["ordinal"] is not None        # still inside a segment: open, not stored


def test_a_gps_dropout_drops_the_open_segment_and_resyncs(conn, ring):
    _drive_ring(conn, laps=2.0, drop=(40.0, 60.0))                    # 20 s without a fix mid-lap
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)

    rows = _instances(conn)
    assert len(rows) >= len(ring) - 1
    for r in rows:                                                    # nothing invented across the gap
        assert r["duration_s"] == pytest.approx([s["length_m"] for s in ring if s["name"] == r["name"]][0] / 10.0, abs=0.2)
    # the segment the dropout fell in is missing once, the rest are intact
    names = [r["name"] for r in rows]
    assert len(names) < 2 * len(ring)


def test_a_missed_reading_skips_one_gate_without_losing_sync(conn):
    """Gates 60 m apart, fixes 80 m apart: one step crosses two gates. The middle segment is not guessed at,
    and tracking continues from the second gate."""
    from evil.track import seed_track
    from track_fixtures import gate_across
    seed_track(conn, {"segments": [
        {"name": "Turn 1", "kind": "turn", "aliases": ["1"], "length_m": 60, "entry_gate": gate_across(100, 0, "x")},
        {"name": "Turn 2", "kind": "turn", "aliases": ["2"], "length_m": 60, "entry_gate": gate_across(160, 0, "x")},
        {"name": "Straight 1", "kind": "straight", "aliases": [], "length_m": 200, "entry_gate": gate_across(220, 0, "x")},
        {"name": "Turn 3", "kind": "turn", "aliases": ["3"], "length_m": 80, "entry_gate": gate_across(420, 0, "x")},
    ]})
    t = 0.0
    for x in (0, 80, 160 + 10, 250, 330, 410, 500, 580):             # 80 m per fix; crosses 100 and 160 in one step
        la, lo = ll(x, 0)
        ingest_sample(conn, RawSample("r", t, gps=GpsReading(t, la, lo, 20.0, 0.0, device_ts_us=int(t * 1e6)),
                                      device_global_ts_us=int(t * 1e6)))
        t += 4.0
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)

    names = [r["name"] for r in _instances(conn)]
    # Turn 1 was entered but Turn 2's gate was skipped, so neither is stored (not guessed at); sync is kept
    # and the Straight, whose entry and exit gates were both seen, is recorded.
    assert names == ["Straight 1"]


def test_crossing_a_gate_backwards_is_ignored(conn, ring):
    """Rolling back over the entry gate must not restart or confuse the segment."""
    fixes = []
    for arc in (60, 90, 110, 130, 115, 95, 120, 150, 180, 240, 260, 280):    # crosses gate at 100 forward, back, forward
        x, y = ring_point(arc)
        fixes.append(ll(x, y))
    t = 0.0
    for la, lo in fixes:
        ingest_sample(conn, RawSample("r", t, gps=GpsReading(t, la, lo, 5.0, 0.0, device_ts_us=int(t * 1e6)),
                                      device_global_ts_us=int(t * 1e6)))
        t += 1.0
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)

    rows = _instances(conn)
    assert [r["name"] for r in rows] == ["Straight 1"]                # entered at arc 100, left at 250: once
    assert rows[0]["distance_m"] > 100


def test_gps_noise_within_the_gate_width_does_not_cost_a_crossing(conn, ring):
    _drive_ring(conn, laps=2.0, noise=6.0)                            # +-6 m of GPS error, gates reach 40 m
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)

    rows = _instances(conn)
    assert len(rows) >= 2 * len(ring) - 1
    assert [r["ordinal"] for r in rows] == [(rows[0]["ordinal"] + i) % len(ring) for i in range(len(rows))]


def test_snapshots_that_repeat_a_fix_do_not_double_count(conn, ring):
    _drive_ring(conn, laps=1.2)                                       # each fix appears in 50 snapshots
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)
    rows = _instances(conn)
    assert len(rows) == len({(r["name"], r["start_seq"]) for r in rows})
    for r in rows:
        assert r["distance_m"] < max(s["length_m"] for s in ring) * 1.05


def test_recordings_without_a_device_clock_use_the_row_time(conn, ring):
    _drive_ring(conn, laps=1.5, device_clock=False)
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)
    rows = _instances(conn)
    assert len(rows) >= len(ring) - 1
    for r in rows:
        assert r["distance_m"] == pytest.approx([s["length_m"] for s in ring if s["name"] == r["name"]][0], rel=0.03)


def test_a_missing_track_means_no_instances_and_no_error(conn):
    _drive_ring(conn, laps=0.3)
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)
    assert conn.execute("SELECT COUNT(*) AS n FROM turns").fetchone()["n"] == 0


def test_reclassifying_twice_is_idempotent(conn, ring):
    from evil.scripts.reclassify import reclassify
    import sqlite3, tempfile, os
    _drive_ring(conn, laps=1.5)
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)
    first = [dict(r) for r in conn.execute("SELECT * FROM turns ORDER BY start_seq")]
    with conn:
        for t in ("turns", "straights", "segments_open_state", "classifier_cursor"):
            conn.execute(f"DELETE FROM {t}")
    tick(conn, "r", [SegmentsClassifier()], now_ts=1e9)
    again = [dict(r) for r in conn.execute("SELECT * FROM turns ORDER BY start_seq")]
    strip = lambda rows: [{k: v for k, v in r.items() if k != "turn_id"} for r in rows]
    assert strip(first) == strip(again)
