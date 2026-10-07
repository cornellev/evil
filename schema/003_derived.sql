-- Derived tables, written only by classifiers in src/evil/classifiers/.
--
-- `turns` holds one row per pass through a turn segment (see track_segments); `straights`
-- (006) one per pass through a straight. Both carry the same measures: duration, distance,
-- energy and efficiency (mi/kWh), computed with the Race Engineer Dashboard's formulas
-- (classifiers/metrics.py). start_seq/end_seq are snapshot sequence numbers; consecutive
-- segments share a boundary row. start_ts/end_ts are the row times of those snapshots, while
-- duration_s is exact (interpolated at the gate crossings).
--
-- segments_open_state is the segments classifier's durable position (which segment the car is
-- in and where it entered), so it survives across compiler ticks and restarts.

CREATE TABLE IF NOT EXISTS segments_open_state (
    run_id TEXT PRIMARY KEY,
    last_key_gps_us INTEGER, last_key_lat REAL, last_key_lon REAL,      -- last distinct GPS reading seen
    last_fix_t REAL, last_fix_lat REAL, last_fix_lon REAL,
    ordinal INTEGER,                                                     -- NULL until synced to a gate
    enter_t REAL, enter_seq INTEGER, enter_f REAL,
    enter_a_t REAL, enter_a_lat REAL, enter_a_lon REAL,
    enter_b_t REAL, enter_b_lat REAL, enter_b_lon REAL
);

CREATE TABLE IF NOT EXISTS turns (
    turn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    turn_def_id INTEGER NOT NULL REFERENCES track_segments(segment_id),
    start_seq INTEGER NOT NULL,
    end_seq INTEGER NOT NULL,
    start_ts REAL NOT NULL,
    end_ts REAL NOT NULL,
    entry_speed REAL,
    exit_speed REAL,
    duration_s REAL,
    distance_m REAL,
    energy_wh REAL,
    efficiency_mi_per_kwh REAL,
    avg_speed REAL,
    UNIQUE (run_id, turn_def_id, start_seq)
);

CREATE INDEX IF NOT EXISTS idx_turns_run ON turns(run_id, start_seq);

-- Whole-run totals (RED's "average efficiency" = run distance / run energy), filled after a run is
-- classified so list_runs does not rescan the run.
CREATE TABLE IF NOT EXISTS run_summary (
    run_id TEXT PRIMARY KEY,
    distance_m REAL,
    energy_wh REAL,
    efficiency_mi_per_kwh REAL,
    duration_s REAL,
    avg_speed REAL
);
