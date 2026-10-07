-- Runtime bookkeeping. Not domain data: classifier progress cursors. (The
-- run -> raw-file bridge that nas_index used to be lives in the recording
-- catalog now; see recording-catalog-design.md section 3.3.)

CREATE TABLE IF NOT EXISTS classifier_cursor (
    run_id TEXT NOT NULL,
    classifier_name TEXT NOT NULL,
    classifier_version INTEGER NOT NULL,
    last_processed_seq INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (run_id, classifier_name, classifier_version)
);
