-- Derived tables, written only by classifiers in src/evil/classifiers/.
-- turns_open_state holds in-progress segments so a turn that spans more than
-- one compiler tick survives across invocations (and a restart, since it's
-- durable, not in-memory).

CREATE TABLE IF NOT EXISTS turns_open_state (
    run_id TEXT NOT NULL,
    turn_def_id INTEGER NOT NULL REFERENCES track_geometry(turn_def_id),
    start_seq INTEGER NOT NULL,
    start_ts REAL NOT NULL,
    entry_speed REAL,
    PRIMARY KEY (run_id, turn_def_id)
);

CREATE TABLE IF NOT EXISTS turns (
    turn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    turn_def_id INTEGER NOT NULL REFERENCES track_geometry(turn_def_id),
    start_seq INTEGER NOT NULL,
    end_seq INTEGER NOT NULL,
    start_ts REAL NOT NULL,
    end_ts REAL NOT NULL,
    entry_speed REAL,
    exit_speed REAL,
    UNIQUE (run_id, turn_def_id, start_seq)
);

CREATE INDEX IF NOT EXISTS idx_turns_run ON turns(run_id, start_seq);
