-- Static reference data. Loaded once per track by tooling (scripts/seed_reference_data), never
-- written by the compiler.
--
-- The track is an ordered ring of SEGMENTS (turns and straights) separated by GATES: short lines
-- across the track. Segment `ordinal` k runs from its entry gate to the entry gate of ordinal k+1
-- (the last wraps to 0), so segments tile the lap with no overlap and no gaps. See src/evil/track.py.
CREATE TABLE IF NOT EXISTS track_segments (
    segment_id INTEGER PRIMARY KEY,
    ordinal INTEGER NOT NULL UNIQUE,         -- position around the lap, 0-based
    kind TEXT NOT NULL CHECK (kind IN ('turn', 'straight')),
    name TEXT NOT NULL UNIQUE,               -- 'Turn 3', 'Turn 1-2', 'Straight 6-7'
    aliases TEXT NOT NULL DEFAULT '',        -- official turn numbers this covers, comma separated: '1,2'
    length_m REAL NOT NULL,
    gate_lat1 REAL NOT NULL, gate_lon1 REAL NOT NULL,   -- entry gate: one end ...
    gate_lat2 REAL NOT NULL, gate_lon2 REAL NOT NULL    -- ... and the other
);
