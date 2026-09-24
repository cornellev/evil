from evil.ingestion.normalize import flatten, normalize_key, to_raw_sample


def test_flatten_nests_to_dotted_keys():
    payload = {"power": {"voltage": 48.0, "current": 3.0}, "gps": {"lat": 42.0, "long": -76.0}}

    assert flatten(payload) == {
        "power.voltage": 48.0,
        "power.current": 3.0,
        "gps.lat": 42.0,
        "gps.long": -76.0,
    }


def test_flatten_is_a_no_op_on_an_already_flat_dict():
    payload = {"global_ts": "1.0", "gps.lat": "42.0"}

    assert flatten(payload) == payload


def test_normalize_key_strips_punctuation_and_case():
    assert normalize_key("GPS.Lat") == normalize_key("gps_lat") == normalize_key("GPS Lat") == "gpslat"


def test_to_raw_sample_handles_nested_json_payload_with_explicit_ts():
    payload = {"power": {"voltage": 48.0, "current": 3.0}, "gps": {"lat": 42.0, "long": -76.0, "speed": 5.0}}

    sample = to_raw_sample("run-1", payload, ts=10.0)

    assert sample.run_id == "run-1"
    assert sample.ts == 10.0
    assert sample.joulemeter.voltage == 48.0
    assert sample.gps.lat == 42.0
    assert sample.gps.lon == -76.0
    assert sample.gps.speed == 5.0


def test_to_raw_sample_ts_override_wins_over_payload_ts():
    # Ros2Source's case: wall-clock receipt time is authoritative, not
    # anything a sensor payload might also carry.
    payload = {"global_ts": 999.0, "gps": {"lat": 1.0, "long": 2.0}}

    sample = to_raw_sample("run-1", payload, ts=5.0)

    assert sample.ts == 5.0


def test_to_raw_sample_falls_back_to_payload_ts_then_default():
    with_ts = to_raw_sample("run-1", {"global_ts": "7.0"})
    assert with_ts.ts == 7.0

    without_ts = to_raw_sample("run-1", {}, default_ts=3.0)
    assert without_ts.ts == 3.0


def test_to_raw_sample_creates_planner_reading_from_nested_driver_key():
    payload = {"driver": {"planned_path": "left", "target_speed": 4.0}}

    sample = to_raw_sample("run-1", payload, ts=1.0)

    assert sample.planner is not None
    assert sample.planner.planned_path == "left"
    assert sample.planner.target_speed == 4.0


def test_to_raw_sample_missing_fields_produce_none_readings():
    sample = to_raw_sample("run-1", {"steering": {"turn_angle": 5.0}}, ts=1.0)

    assert sample.gps is None
    assert sample.joulemeter is None
    assert sample.planner is None
