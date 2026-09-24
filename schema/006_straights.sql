-- straights are the complement of turns: the interval between one turn's
-- end and the next turn's start. No open-state table needed here, unlike
-- turns/laps -- a straight is only ever finalized retroactively once BOTH
-- of its boundary turns are already known, so there's nothing "in progress"
-- to persist across ticks.

CREATE TABLE IF NOT EXISTS straights (
    straight_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    start_seq INTEGER NOT NULL,
    end_seq INTEGER NOT NULL,
    start_ts REAL NOT NULL,
    end_ts REAL NOT NULL,
    entry_speed REAL,
    exit_speed REAL,
    avg_speed REAL,
    energy_wh REAL,
    UNIQUE (run_id, start_seq, end_seq)
);

CREATE INDEX IF NOT EXISTS idx_straights_run ON straights(run_id, start_seq);
