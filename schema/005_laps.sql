-- start_finish_line mirrors track_geometry's circle-geofence shape (loaded
-- once per track, not derived). laps_open_state tracks whether the car is
-- currently inside the geofence (to debounce dwelling inside the small
-- circle while passing through) plus the currently-open lap's start point --
-- crossing INTO the line both closes the open lap and starts the next one.

CREATE TABLE IF NOT EXISTS start_finish_line (
    line_id INTEGER PRIMARY KEY AUTOINCREMENT,
    center_lat REAL NOT NULL,
    center_lon REAL NOT NULL,
    radius_m REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS laps_open_state (
    run_id TEXT NOT NULL,
    line_id INTEGER NOT NULL REFERENCES start_finish_line(line_id),
    lap_number INTEGER NOT NULL,
    start_seq INTEGER NOT NULL,
    start_ts REAL NOT NULL,
    currently_inside INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, line_id)
);

CREATE TABLE IF NOT EXISTS laps (
    lap_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    lap_number INTEGER NOT NULL,
    start_seq INTEGER NOT NULL,
    end_seq INTEGER NOT NULL,
    start_ts REAL NOT NULL,
    end_ts REAL NOT NULL,
    turn_count INTEGER NOT NULL,
    energy_wh REAL,
    avg_speed REAL,
    UNIQUE (run_id, lap_number)
);

CREATE INDEX IF NOT EXISTS idx_laps_run ON laps(run_id, lap_number);
