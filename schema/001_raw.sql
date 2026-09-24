-- Raw signal tables. One row per sensor reading. Never mutated after insert.
-- main_snapshot ties together whatever raw tables had a value on a given tick;
-- everything downstream (classifiers, cursors) is expressed in main_snapshot.seq.

CREATE TABLE IF NOT EXISTS joulemeter (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    voltage REAL NOT NULL,
    current REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS local_planner (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    planned_path TEXT,
    target_speed REAL
);

CREATE TABLE IF NOT EXISTS gps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    speed REAL,
    heading REAL
);

CREATE TABLE IF NOT EXISTS main_snapshot (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    global_ts REAL NOT NULL,
    joulemeter_id INTEGER REFERENCES joulemeter(id),
    planner_id INTEGER REFERENCES local_planner(id),
    gps_id INTEGER REFERENCES gps(id)
);

CREATE INDEX IF NOT EXISTS idx_main_snapshot_run_seq ON main_snapshot(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_main_snapshot_run_ts ON main_snapshot(run_id, global_ts);
