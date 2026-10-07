"""Writes a normalized RawSample into the raw tables."""

from __future__ import annotations

import sqlite3

from evil.models import RawSample


def insert_sample(conn: sqlite3.Connection, sample: RawSample) -> int:
    """Insert one tick's readings and the main_snapshot row that ties them
    together, WITHOUT committing (the caller owns the transaction, so a whole
    recording can be parsed in one). Returns the assigned seq."""
    run_id = sample.run_id

    def _row(sql: str, params: tuple) -> int:
        return conn.execute(sql, params).lastrowid

    joulemeter_id = None
    if sample.joulemeter is not None:
        j = sample.joulemeter
        joulemeter_id = _row(
            "INSERT INTO joulemeter (run_id, ts, device_ts_us, voltage, current) VALUES (?, ?, ?, ?, ?)",
            (run_id, j.ts, j.device_ts_us, j.voltage, j.current),
        )

    planner_id = None
    if sample.planner is not None:
        p = sample.planner
        planner_id = _row(
            "INSERT INTO local_planner (run_id, ts, planned_path, target_speed) VALUES (?, ?, ?, ?)",
            (run_id, p.ts, p.planned_path, p.target_speed),
        )

    gps_id = None
    if sample.gps is not None:
        g = sample.gps
        gps_id = _row(
            "INSERT INTO gps (run_id, ts, device_ts_us, lat, lon, speed, heading) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, g.ts, g.device_ts_us, g.lat, g.lon, g.speed, g.heading),
        )

    steering_id = None
    if sample.steering is not None:
        s = sample.steering
        steering_id = _row(
            "INSERT INTO steering (run_id, ts, device_ts_us, brake_pressure, turn_angle) VALUES (?, ?, ?, ?, ?)",
            (run_id, s.ts, s.device_ts_us, s.brake_pressure, s.turn_angle),
        )

    rpm_ids = {}
    for table, reading in (("rpm_front", sample.rpm_front), ("rpm_back", sample.rpm_back)):
        rpm_ids[table] = None
        if reading is not None:
            rpm_ids[table] = _row(
                f"INSERT INTO {table} (run_id, ts, device_ts_us, rpm_left, rpm_right) VALUES (?, ?, ?, ?, ?)",
                (run_id, reading.ts, reading.device_ts_us, reading.rpm_left, reading.rpm_right),
            )

    motor_id = None
    if sample.motor is not None:
        mo = sample.motor
        motor_id = _row(
            "INSERT INTO motor (run_id, ts, device_ts_us, rpm, throttle) VALUES (?, ?, ?, ?, ?)",
            (run_id, mo.ts, mo.device_ts_us, mo.rpm, mo.throttle),
        )

    return _row(
        """INSERT INTO main_snapshot
               (run_id, global_ts, joulemeter_id, planner_id, gps_id, steering_id, rpm_front_id,
                rpm_back_id, motor_id, device_seq, device_global_ts_us, publish_ns, filtered_speed)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (run_id, sample.ts, joulemeter_id, planner_id, gps_id, steering_id, rpm_ids["rpm_front"],
         rpm_ids["rpm_back"], motor_id, sample.device_seq, sample.device_global_ts_us,
         sample.publish_ns, sample.filtered_speed),
    )


def ingest_sample(conn: sqlite3.Connection, sample: RawSample) -> int:
    """One sample, one transaction (live ingestion). Raw rows are immutable
    once written, so this and insert_sample() are the only functions in the
    package that write to raw tables. Returns the assigned seq."""
    with conn:
        return insert_sample(conn, sample)
