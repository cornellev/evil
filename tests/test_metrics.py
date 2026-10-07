"""Energy, distance and efficiency follow the Race Engineer Dashboard's formulas
(RaceEngineerDashboard/frontend/src/utils/telemetry.ts), so the two tools agree."""

import math

import pytest

from evil.classifiers import metrics
from evil.geo import haversine_m
from evil.ingest import ingest_sample
from evil.models import GpsReading, JoulemeterReading, RawSample


def test_power_is_clamped_at_zero_like_red():
    assert metrics.power_kw(10.0, 50.0) == pytest.approx(0.5)
    assert metrics.power_kw(-3.0, 50.0) == 0.0                 # regeneration never subtracts
    assert metrics.power_kw(None, 50.0) is None and metrics.power_kw(2.0, None) is None


def test_energy_is_the_trapezoid_of_power_over_time():
    # RED: (P_prev + P_cur) / 2 x dt_hours. 1 kW then 3 kW over 36 s: 2 kW x 0.01 h = 0.02 kWh
    assert metrics.trapezoid_kwh(1.0, 3.0, 0.0, 36.0) == pytest.approx(0.02)
    assert metrics.trapezoid_kwh(1.0, 3.0, 10.0, 10.0) == 0.0  # no elapsed time
    assert metrics.trapezoid_kwh(1.0, 3.0, 10.0, 5.0) == 0.0   # time never runs backwards


def test_energy_over_samples_and_cut_exactly_at_a_time():
    samples = [(0.0, 1.0), (4.0, 1.0), (8.0, 3.0)]
    full = metrics.energy_kwh(samples)
    assert full == pytest.approx((1.0 * 4 + 2.0 * 4) / 3600)
    cut = metrics.energy_kwh(samples, t0=2.0, t1=6.0)           # power at 6 s interpolates to 2 kW
    assert cut == pytest.approx((1.0 * 2 + 1.5 * 2) / 3600)
    assert metrics.energy_kwh(samples, 0.0, 8.0) == pytest.approx(full)
    # cutting at a point and at the same point again adds back to the whole: segments add up to the lap
    assert metrics.energy_kwh(samples, 0.0, 5.3) + metrics.energy_kwh(samples, 5.3, 8.0) == pytest.approx(full)


def test_readings_further_apart_than_the_gap_guard_are_not_bridged():
    assert metrics.energy_kwh([(0.0, 2.0), (metrics.MAX_GAP_S + 1, 2.0)]) == 0.0
    assert metrics.energy_kwh([(0.0, 2.0), (metrics.MAX_GAP_S, 2.0)]) > 0


def test_distance_matches_haversine_over_a_lap_scale_step():
    d = metrics.tangent_distance_m(39.79, -86.23, 39.80, -86.24)
    assert d == pytest.approx(haversine_m(39.79, -86.23, 39.80, -86.24), rel=2e-3)
    assert metrics.tangent_distance_m(39.79, -86.23, 39.79, -86.23) == pytest.approx(0.0, abs=1e-6)
    # one degree of latitude is about 111 km on the ellipsoid
    assert metrics.tangent_distance_m(0.0, 10.0, 1.0, 10.0) == pytest.approx(110_574, rel=2e-3)


def test_validity_rejects_what_red_rejects():
    assert metrics.valid_fix(39.79, -86.23)
    for bad in ((None, -86.0), (39.0, None), (float("nan"), -86.0), (39.0, float("inf")), (0.0, -86.0), (39.0, 0.0)):
        assert not metrics.valid_fix(*bad)


def test_efficiency_is_miles_per_kwh_and_none_without_energy():
    # 1609.344 m = 1 mi; 1000 Wh = 1 kWh
    assert metrics.efficiency_mi_per_kwh(1609.344, 1000.0) == pytest.approx(1.0)
    assert metrics.efficiency_mi_per_kwh(3218.688, 500.0) == pytest.approx(4.0)
    assert metrics.efficiency_mi_per_kwh(1000.0, 0.0) is None
    assert metrics.efficiency_mi_per_kwh(None, 5.0) is None


def test_instant_efficiency_is_red_calculate_efficiency():
    # 10 m/s = 22.3694 mph at 0.5 kW -> 44.74 mi/kWh; none at zero power
    assert metrics.instant_efficiency_mi_per_kwh(10.0, 0.5) == pytest.approx(44.7388)
    assert metrics.instant_efficiency_mi_per_kwh(10.0, 0.0) is None


def _run(conn, n=120, speed=10.0, power_w=400.0, repeats=50):
    """n seconds straight east at `speed`, GPS 1 Hz repeated across 50 Hz snapshots, constant power."""
    lat = 42.0
    m_lon = 111_320.0 * math.cos(math.radians(lat))
    with conn:
        for k in range(n * repeats + 1):
            t = 10.0 + k / repeats
            fix_s = int(k / repeats)
            lon = -76.0 + fix_s * speed / m_lon
            ingest_sample(conn, RawSample(
                "r", t, joulemeter=JoulemeterReading(t, 50.0, power_w / 50.0, device_ts_us=int(t * 1e6)),
                gps=GpsReading(t, lat, lon, speed, 0.0, device_ts_us=int((10.0 + fix_s) * 1e6)),
                device_global_ts_us=int(t * 1e6), filtered_speed=speed))
    return n


def test_range_metrics_over_a_steady_drive(conn):
    n = _run(conn)
    m = metrics.range_metrics(conn, "r", 1, n * 50 + 1)
    assert m["duration_s"] == pytest.approx(n)
    assert m["energy_wh"] == pytest.approx(0.4 * n / 3600 * 1000, rel=1e-6)
    assert m["distance_m"] == pytest.approx(n * 10.0, rel=0.005)
    assert m["avg_speed"] == pytest.approx(10.0, rel=0.02)
    assert m["efficiency_mi_per_kwh"] == pytest.approx(m["distance_m"] / 1609.344 / (m["energy_wh"] / 1000))


def test_repeated_snapshots_of_one_gps_reading_count_once(conn):
    _run(conn, n=30)
    rows = conn.execute(metrics._SNAPSHOTS, ("r", 1, 100000)).fetchall()
    assert len(rows) == 30 * 50 + 1
    assert len(metrics.distinct_fixes(rows)) == 31      # one per second, inclusive of both ends


def test_a_gps_glitch_is_not_counted_as_distance(conn):
    with conn:
        for k, (la, lo) in enumerate([(42.0, -76.0), (42.0, -75.9999), (45.0, -70.0), (42.0, -75.9997)]):
            ingest_sample(conn, RawSample("r", float(k), gps=GpsReading(float(k), la, lo, 5.0, 0.0, device_ts_us=k * 1_000_000),
                                          device_global_ts_us=k * 1_000_000))
    m = metrics.range_metrics(conn, "r", 1, 4)
    assert m["distance_m"] < 40            # the 400 km jump to (45, -70) and back is dropped


def test_run_summary_is_stored_and_listed(conn):
    from evil.tools.list_runs import list_runs
    n = _run(conn, n=60)
    metrics.store_run_summary(conn, "r")
    runs = list_runs(conn)
    assert runs[0]["run_id"] == "r"
    assert runs[0]["energy_wh"] == pytest.approx(0.4 * n / 3600 * 1000, rel=1e-6)
    assert runs[0]["efficiency_mi_per_kwh"] == pytest.approx(runs[0]["distance_m"] / 1609.344 / (runs[0]["energy_wh"] / 1000))
    metrics.store_run_summary(conn, "r")                                    # idempotent
    assert conn.execute("SELECT COUNT(*) FROM run_summary").fetchone()[0] == 1


def test_no_energy_means_efficiency_is_none_not_a_division_error(conn):
    with conn:
        for k in range(5):
            ingest_sample(conn, RawSample("r", float(k), gps=GpsReading(float(k), 42.0, -76.0 + k * 1e-4, 5.0, 0.0,
                                                                          device_ts_us=k * 1_000_000), device_global_ts_us=k * 1_000_000))
    m = metrics.range_metrics(conn, "r", 1, 5)
    assert m["energy_wh"] is None and m["efficiency_mi_per_kwh"] is None and m["distance_m"] > 0
