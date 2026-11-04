-- One row per run, for both tiers (live-next-steps-2026-11.md section C).
--   tier 'upload': written by the parser for each parsed recording (status always 'closed').
--   tier 'live':   written by the live compiler; 'recording' while the session is open.
-- A live run lives in the car's live database (evil_<car>_live.db), an upload in the
-- main one; linked_run_id points across (the live capture <-> the full upload of the same
-- drive). Times: started_at/ended_at are row times (global_ts); car_start/car_end are the
-- car's own clock (bag record time for uploads, _t_publish_ns for live), used for linking.
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    tier          TEXT NOT NULL CHECK (tier IN ('live', 'upload')),
    source        TEXT NOT NULL,                  -- 'ros2' | 'file' | ...
    schema_id     TEXT,                           -- NULL while a live run has not locked a schema
    status        TEXT NOT NULL CHECK (status IN ('recording', 'closed')),
    started_at    REAL,
    ended_at      REAL,
    car_start     REAL,
    car_end       REAL,
    end_reason    TEXT,     -- live: bag_stopped | gap | schema_change | daq_reboot | interrupted | shutdown
    session_mode  TEXT,     -- live: 'bag' (RED's record button via the bag service) | 'gap' (fallback)
    bag_id        TEXT,     -- live: the car's rosbag container id while recording
    recording_id  TEXT,     -- the catalog recording (upload, or the exported live capture)
    linked_run_id TEXT,
    stats_json    TEXT,     -- live: counters (stored/dup/rejected/loss/overflow) at close
    updated_at    REAL
);

-- Live only: every message received while a session is open, byte for byte, until the
-- session closes and is exported to the raw store as a live capture (then deleted here).
CREATE TABLE IF NOT EXISTS live_raw (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   TEXT NOT NULL,
    recv_ts  REAL NOT NULL,
    text     TEXT
);
CREATE INDEX IF NOT EXISTS idx_live_raw_run ON live_raw(run_id, id);
