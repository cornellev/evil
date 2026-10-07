-- Straights are first-class segments, named and measured exactly like turns (see 003 and
-- track_segments): one row per pass through a straight segment, written by the segments
-- classifier. There is no "gap between turns" derivation any more.

CREATE TABLE IF NOT EXISTS straights (
    straight_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    segment_id INTEGER NOT NULL REFERENCES track_segments(segment_id),
    start_seq INTEGER NOT NULL,
    end_seq INTEGER NOT NULL,
    start_ts REAL NOT NULL,
    end_ts REAL NOT NULL,
    entry_speed REAL,
    exit_speed REAL,
    avg_speed REAL,
    energy_wh REAL,
    duration_s REAL,
    distance_m REAL,
    efficiency_mi_per_kwh REAL,
    UNIQUE (run_id, segment_id, start_seq)
);

CREATE INDEX IF NOT EXISTS idx_straights_run ON straights(run_id, start_seq);
