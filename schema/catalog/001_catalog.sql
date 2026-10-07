-- catalog.db: unversioned index over the raw store. Lives on the NUC's local
-- disk (never NFS/SMB). See recording-catalog-design.md section 3.2. Kept in a
-- subdirectory so db.apply_schema() (which globs schema/*.sql for evil.db)
-- never applies it to the telemetry database.

CREATE TABLE IF NOT EXISTS recordings (
    recording_id    TEXT PRIMARY KEY,
    run_id          TEXT UNIQUE,                 -- 1 recording = 1 run; assigned at parse (Phase 2)
    label           TEXT,
    notes           TEXT,
    source          TEXT NOT NULL DEFAULT 'telemetry' CHECK (source IN ('telemetry','autonomy','other')),
    container       TEXT NOT NULL DEFAULT 'unknown',
    schema_id       TEXT,
    matched_stream  TEXT,
    schema_match_json TEXT,
    car             TEXT,
    category        TEXT CHECK (category IN ('competition','testing','bench','sim','other')),
    event           TEXT,
    location_id     INTEGER,
    location_method TEXT,
    gps_min_lat REAL, gps_max_lat REAL, gps_min_lon REAL, gps_max_lon REAL,
    recorded_start  REAL,
    recorded_end    REAL,
    time_source     TEXT,
    time_trust      TEXT NOT NULL DEFAULT 'ok' CHECK (time_trust IN ('ok','suspect','overridden')),
    original_name   TEXT,
    uploaded_at     REAL NOT NULL,
    uploader        TEXT,
    metadata_json   TEXT,
    storage_state   TEXT NOT NULL DEFAULT 'stored' CHECK (storage_state IN ('stored','registered')),
    total_bytes     INTEGER,
    content_sha256  TEXT UNIQUE,                 -- hash over sorted file hashes; dedup key
    parse_status    TEXT NOT NULL DEFAULT 'pending'
                    CHECK (parse_status IN ('pending','running','parsed','skipped','failed')),
    parser_version  TEXT,
    target_db       TEXT,
    rows_ingested   INTEGER,
    rows_duplicate  INTEGER,
    rows_rejected   INTEGER,
    reject_samples_json TEXT,
    parse_stats_json TEXT,                       -- extra parse counters (e.g. seq conflicts)
    parse_error     TEXT
);

CREATE TABLE IF NOT EXISTS recording_files (
    file_id         TEXT PRIMARY KEY,
    recording_id    TEXT NOT NULL REFERENCES recordings(recording_id),
    role            TEXT NOT NULL,               -- 'db3' | 'metadata' | 'csv' | 'video' | 'other'
    original_name   TEXT NOT NULL,
    storage_backend TEXT NOT NULL,               -- 'nuc-local' | 'nas'
    rel_path        TEXT NOT NULL,               -- relative to the backend's root
    size_bytes      INTEGER NOT NULL,
    sha256          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recording_files_recording ON recording_files(recording_id);
CREATE INDEX IF NOT EXISTS idx_recordings_uploaded ON recordings(uploaded_at);
