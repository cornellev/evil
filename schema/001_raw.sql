-- Raw signal tables. One row per sensor reading (per snapshot). Never mutated
-- after insert. main_snapshot ties together whatever raw tables had a value on
-- a given tick; everything downstream (classifiers, cursors) is expressed in
-- main_snapshot.seq. Every raw row carries run_id so a recording can be
-- re-parsed idempotently (delete by run_id, then insert).
--
-- Schema v1 (evil.telemetry.v1, see recording-catalog-design.md section 4): the
-- tables hold the whole strict payload. Sensor columns are nullable: NaN (very
-- common: no GPS fix, unplugged sensors) is stored as NULL. `ts` is EVIL's row
-- time in seconds (container record time); `device_ts_us` is the group's own
-- device-clock timestamp, kept so staleness stays visible.

CREATE TABLE IF NOT EXISTS joulemeter (            -- payload "power"
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    device_ts_us INTEGER,
    voltage REAL,
    current REAL
);

CREATE TABLE IF NOT EXISTS steering (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    device_ts_us INTEGER,
    brake_pressure REAL,
    turn_angle REAL
);

CREATE TABLE IF NOT EXISTS rpm_front (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    device_ts_us INTEGER,
    rpm_left REAL,
    rpm_right REAL
);

CREATE TABLE IF NOT EXISTS rpm_back (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    device_ts_us INTEGER,
    rpm_left REAL,
    rpm_right REAL
);

CREATE TABLE IF NOT EXISTS gps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    device_ts_us INTEGER,
    lat REAL,
    lon REAL,
    speed REAL,
    heading REAL
);

CREATE TABLE IF NOT EXISTS motor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    device_ts_us INTEGER,
    rpm REAL,
    throttle REAL
);

-- Autonomy planner output. Not part of the telemetry payload (provisional
-- schema id evil.autonomy.planner.v0); kept so existing planner_id pointers work.
CREATE TABLE IF NOT EXISTS local_planner (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    ts REAL NOT NULL,
    planned_path TEXT,
    target_speed REAL
);

CREATE TABLE IF NOT EXISTS main_snapshot (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,         -- EVIL's own cursor key
    run_id TEXT NOT NULL,
    global_ts REAL NOT NULL,
    joulemeter_id INTEGER REFERENCES joulemeter(id),
    planner_id INTEGER REFERENCES local_planner(id),
    gps_id INTEGER REFERENCES gps(id),
    steering_id INTEGER REFERENCES steering(id),
    rpm_front_id INTEGER REFERENCES rpm_front(id),
    rpm_back_id INTEGER REFERENCES rpm_back(id),
    motor_id INTEGER REFERENCES motor(id),
    device_seq INTEGER,                            -- payload "seq" (DAQ seqlock counter)
    device_global_ts_us INTEGER,                   -- payload "global_ts"
    publish_ns INTEGER,                            -- payload "_t_publish_ns"
    filtered_speed REAL                            -- payload "filtered.speed"
);

CREATE INDEX IF NOT EXISTS idx_main_snapshot_run_seq ON main_snapshot(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_main_snapshot_run_ts ON main_snapshot(run_id, global_ts);
CREATE INDEX IF NOT EXISTS idx_joulemeter_run ON joulemeter(run_id);
CREATE INDEX IF NOT EXISTS idx_steering_run ON steering(run_id);
CREATE INDEX IF NOT EXISTS idx_rpm_front_run ON rpm_front(run_id);
CREATE INDEX IF NOT EXISTS idx_rpm_back_run ON rpm_back(run_id);
CREATE INDEX IF NOT EXISTS idx_gps_run ON gps(run_id);
CREATE INDEX IF NOT EXISTS idx_motor_run ON motor(run_id);
CREATE INDEX IF NOT EXISTS idx_local_planner_run ON local_planner(run_id);
