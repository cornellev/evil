from evil.tools.turn_name_matching import resolve_turn_name


def test_resolves_exact_match(readonly_conn):
    assert resolve_turn_name(readonly_conn, "Turn 3") == "Turn 3"


def test_resolves_bare_number_from_a_real_model_tool_call(readonly_conn):
    # confirmed empirically: asked "how was turn 3", gemma4:e4b called
    # get_turn with turn_name="3", not the stored "Turn 3"
    assert resolve_turn_name(readonly_conn, "3") == "Turn 3"


def test_resolves_regardless_of_surrounding_text_or_case(readonly_conn):
    assert resolve_turn_name(readonly_conn, "turn 3") == "Turn 3"
    assert resolve_turn_name(readonly_conn, "T3") == "Turn 3"


def test_does_not_confuse_similar_digit_prefixes(evil_db_path):
    from evil import db

    conn = db.connect(evil_db_path)
    conn.execute(
        "INSERT INTO track_geometry (turn_name, center_lat, center_lon, radius_m) "
        "VALUES ('Turn 10', 43.0, -77.0, 10)"
    )
    conn.commit()
    conn.close()

    readonly = db.connect_readonly(evil_db_path)
    try:
        assert resolve_turn_name(readonly, "1") is None  # "Turn 3"'s digits are "3", not "1"
        assert resolve_turn_name(readonly, "10") == "Turn 10"
    finally:
        readonly.close()


def test_returns_none_when_nothing_matches(readonly_conn):
    assert resolve_turn_name(readonly_conn, "99") is None
    assert resolve_turn_name(readonly_conn, "the hairpin") is None
