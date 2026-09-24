-- Runtime bookkeeping. Not domain data: classifier progress cursors and the
-- bridge from a derived row to the raw autonomy file it came from on NAS.

CREATE TABLE IF NOT EXISTS classifier_cursor (
    run_id TEXT NOT NULL,
    classifier_name TEXT NOT NULL,
    classifier_version INTEGER NOT NULL,
    last_processed_seq INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (run_id, classifier_name, classifier_version)
);

CREATE TABLE IF NOT EXISTS nas_index (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    path TEXT NOT NULL,
    kind TEXT NOT NULL,
    start_ts REAL NOT NULL,
    end_ts REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nas_index_run_ts ON nas_index(run_id, start_ts, end_ts);
