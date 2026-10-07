import copy
import math

import pytest

from evil.schemas import telemetry_v1 as v1
from telemetry_fixtures import NAN, payload, v2_payload


def test_a_real_shaped_payload_conforms_and_nan_is_a_valid_number():
    assert v1.validate(payload()) == []
    assert v1.validate(payload(lat=None)) == []  # whole GPS group NaN: still conforming


def test_exact_key_set_is_required_at_every_level():
    p = payload()
    missing_top = {k: v for k, v in p.items() if k != "seq"}
    assert any("missing key 'seq'" in r for r in v1.validate(missing_top))

    extra_top = {**p, "errcount": 3}
    assert any("unexpected key 'errcount'" in r for r in v1.validate(extra_top))

    renamed = copy.deepcopy(p)
    renamed["gps"]["lon"] = renamed["gps"].pop("long")  # README says long, not lon
    reasons = v1.validate(renamed)
    assert any("unexpected key 'gps.lon'" in r for r in reasons) and any("missing key 'gps.long'" in r for r in reasons)

    case = copy.deepcopy(p)
    case["Power"] = case.pop("power")
    assert v1.validate(case)


def test_later_shape_is_not_v1():
    reasons = v1.validate(v2_payload())
    assert any("errcount" in r for r in reasons) and any("duty_cycle" in r for r in reasons)


@pytest.mark.parametrize("path,value", [
    (("seq",), 2.0),
    (("seq",), True),
    (("seq",), "2"),
    (("global_ts",), None),
    (("_t_publish_ns",), 1.5),
    (("gps", "ts"), 1.0),
    (("gps", "lat"), "42.0"),
    (("gps", "lat"), None),
    (("power", "voltage"), True),
    (("motor", "rpm"), [1]),
])
def test_types_are_strict(path, value):
    p = payload()
    target = p
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    assert v1.validate(p), f"{path}={value!r} should not conform"


def test_non_objects_and_nested_surprises_are_rejected():
    assert v1.validate([]) and v1.validate("x") and v1.validate(None)
    p = payload()
    p["gps"] = [1, 2]
    assert v1.validate(p)


def test_ints_are_accepted_where_a_number_is_expected():
    p = payload()
    p["power"]["voltage"] = 48
    assert v1.validate(p) == []


def test_to_sample_maps_everything_and_nan_becomes_none():
    s = v1.to_sample("r", payload(seq=4, lat=None), ts=10.5)
    assert s.run_id == "r" and s.ts == 10.5
    assert s.device_seq == 4 and s.device_global_ts_us == 1_004_000
    assert s.gps is None                      # all four GPS fields NaN: no row
    assert s.rpm_front is None                # 100% NaN group in real data
    assert s.rpm_back.rpm_left == 200.0 and s.rpm_back.device_ts_us == 1_004_300
    assert s.joulemeter.voltage == 48.0 and s.joulemeter.device_ts_us == 1_004_100
    assert s.motor.throttle == 0.0 and s.steering.turn_angle == 0.2
    assert s.filtered_speed == 5.0 and s.publish_ns == 1_700_000_000_000_000_004


def test_a_partly_nan_group_keeps_its_row_with_nulls():
    p = payload()
    p["gps"]["lat"] = NAN
    p["gps"]["long"] = NAN
    s = v1.to_sample("r", p, ts=1.0)
    assert s.gps is not None and s.gps.lat is None and s.gps.lon is None and s.gps.speed == 5.0


def test_infinities_are_stored_as_none():
    p = payload()
    p["power"]["voltage"] = math.inf
    assert v1.to_sample("r", p, 1.0).joulemeter.voltage is None


def test_repeat_key_ignores_only_the_publish_time():
    a = payload(seq=2, publish_ns=1)
    b = payload(seq=2, publish_ns=999)
    c = payload(seq=2, publish_ns=1, speed=9.0)
    assert v1.repeat_key(a) == v1.repeat_key(b)
    assert v1.repeat_key(a) != v1.repeat_key(c)
    assert v1.repeat_key(payload(lat=None)) == v1.repeat_key(payload(lat=None))  # NaN-safe
