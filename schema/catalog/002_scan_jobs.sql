-- Scan results, named locations and the job queue (Phase 2).

-- A "stream" is a ROS topic (or later a Zenoh key expression): the unit that
-- makes every container fit one shape.
CREATE TABLE IF NOT EXISTS recording_streams (
    recording_id    TEXT NOT NULL REFERENCES recordings(recording_id),
    stream_name     TEXT NOT NULL,
    kind            TEXT NOT NULL,               -- 'ros_topic' | 'zenoh_key' | 'csv'
    type_or_encoding TEXT,
    msg_count       INTEGER,
    first_ts        REAL,
    last_ts         REAL,
    PRIMARY KEY (recording_id, stream_name)
);

CREATE TABLE IF NOT EXISTS named_locations (
    location_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    center_lat      REAL NOT NULL,
    center_lon      REAL NOT NULL,
    radius_m        REAL NOT NULL                -- circles first; polygons later if needed
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    recording_id    TEXT NOT NULL REFERENCES recordings(recording_id),
    kind            TEXT NOT NULL,               -- 'scan' | 'parse' | 'cache_build' | 'repair'
    lane            TEXT NOT NULL DEFAULT 'deep' CHECK (lane IN ('fast','deep')),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','done','failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 2,
    created_at      REAL NOT NULL,
    started_at      REAL,
    heartbeat_at    REAL,
    finished_at     REAL,
    progress        REAL,                        -- 0..1
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(lane, status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_recording ON jobs(recording_id);
