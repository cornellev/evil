-- Static reference data. Loaded once per track by tooling, never written by the
-- compiler. Turns are modeled as a circular geofence (center + radius) for now;
-- swap for a polygon boundary if circles prove too coarse on a real track map.

CREATE TABLE IF NOT EXISTS track_geometry (
    turn_def_id INTEGER PRIMARY KEY AUTOINCREMENT,
    turn_name TEXT NOT NULL,
    center_lat REAL NOT NULL,
    center_lon REAL NOT NULL,
    radius_m REAL NOT NULL
);
