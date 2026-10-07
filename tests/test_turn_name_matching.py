from evil.tools.turn_name_matching import resolve_straight_name, resolve_turn_name


def test_resolves_exact_match(readonly_conn):
    assert resolve_turn_name(readonly_conn, "Turn 3") == "Turn 3"


def test_resolves_bare_number_from_a_real_model_tool_call(readonly_conn):
    """Confirmed empirically: asked "how was turn 3", gemma4:e4b called get_turn with turn_name="3"."""
    assert resolve_turn_name(readonly_conn, "3") == "Turn 3"


def test_resolves_regardless_of_surrounding_text_or_case(readonly_conn):
    assert resolve_turn_name(readonly_conn, "turn 3") == "Turn 3"
    assert resolve_turn_name(readonly_conn, "T3") == "Turn 3"


def _with_segments(evil_db_path, rows):
    from evil import db

    conn = db.connect(evil_db_path)
    conn.executemany(
        """INSERT INTO track_segments (segment_id, ordinal, kind, name, aliases, length_m,
                                       gate_lat1, gate_lon1, gate_lat2, gate_lon2)
           VALUES (?, ?, ?, ?, ?, 100, 42.0, -76.0, 42.001, -76.0)""", rows)
    conn.commit()
    conn.close()
    return db.connect_readonly(evil_db_path)


def test_a_merged_segment_answers_to_each_official_turn_but_never_to_turn_12(evil_db_path):
    ro = _with_segments(evil_db_path, [(10, 5, "turn", "Turn 1-2", "1,2"), (11, 6, "turn", "Turn 12", "12"),
                                       (12, 7, "turn", "Turn 10", "10")])
    try:
        assert resolve_turn_name(ro, "1") == "Turn 1-2"
        assert resolve_turn_name(ro, "2") == "Turn 1-2"
        assert resolve_turn_name(ro, "turn 2") == "Turn 1-2"
        assert resolve_turn_name(ro, "Turn 1-2") == "Turn 1-2"
        assert resolve_turn_name(ro, "12") == "Turn 12"        # not confused with 1-2
        assert resolve_turn_name(ro, "10") == "Turn 10"        # nor "1" with "10"
        assert resolve_turn_name(ro, "1 and 2") is None        # two numbers: ambiguous, ask which
    finally:
        ro.close()


def test_straights_resolve_by_their_numbers(readonly_conn):
    assert resolve_straight_name(readonly_conn, "Straight 6-7") == "Straight 6-7"
    assert resolve_straight_name(readonly_conn, "6-7") == "Straight 6-7"
    assert resolve_straight_name(readonly_conn, "straight 6 7") == "Straight 6-7"
    assert resolve_straight_name(readonly_conn, "7-6") is None
    assert resolve_straight_name(readonly_conn, "6") is None


def test_returns_none_when_nothing_matches(readonly_conn):
    assert resolve_turn_name(readonly_conn, "99") is None
    assert resolve_turn_name(readonly_conn, "the hairpin") is None
    assert resolve_turn_name(readonly_conn, "") is None
