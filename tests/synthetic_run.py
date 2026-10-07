"""A synthetic drive around the packaged IMS track, for exercising the segments classifier end to end.

The car follows the track's centreline at a constant speed, the GPS delivers a fix once a second and the
snapshots repeat it at 50 Hz (as the real DAQ does), with a constant electrical load. Everything carries
the DAQ clock (device_global_ts_us), so boundaries, energy and distance are exact and checkable by hand.
"""

from __future__ import annotations

import math

from evil import db, track
from evil.classifiers.metrics import tangent_distance_m
from evil.ingest import insert_sample
from evil.models import GpsReading, JoulemeterReading, RawSample

HZ = 50
GPS_HZ = 1


def centerline_points(track_def: dict) -> list[tuple[float, float]]:
    pts = [tuple(p) for p in track_def["centerline"]]
    return pts + [pts[0]]                       # close the ring


def resample(points: list[tuple[float, float]], step_m: float = 1.0) -> list[tuple[float, float, float]]:
    """(distance_along_m, lat, lon) every step_m along the polyline, by the same tangent-plane metric RED uses."""
    out = [(0.0, *points[0])]
    carried = 0.0
    for (la0, lo0), (la1, lo1) in zip(points, points[1:]):
        seg = tangent_distance_m(la0, lo0, la1, lo1)
        if seg == 0:
            continue
        d = step_m - carried
        while d <= seg:
            f = d / seg
            out.append((out[-1][0] + step_m, la0 + f * (la1 - la0), lo0 + f * (lo1 - lo0)))
            d += step_m
        carried = seg - (d - step_m)
    return out


def drive(conn, run_id: str, track_def: dict, laps: float, speed_mps: float = 8.0, power_w: float = 400.0,
          start_offset_m: float = 0.0, t0: float = 1000.0, dropout: tuple[float, float] | None = None,
          bias_m: float = 0.0, voltage: float = 50.0) -> dict:
    """Insert snapshots for `laps` laps of constant-speed driving; returns the ground truth."""
    line = resample(centerline_points(track_def))
    total = line[-1][0]
    n_dist = laps * total
    step = speed_mps / HZ
    k = 0
    last_fix = None
    with conn:
        while True:
            d = start_offset_m + k * step
            if d - start_offset_m > n_dist:
                break
            t = t0 + k / HZ
            idx = int(d % total)
            _, lat, lon = line[min(idx, len(line) - 1)]
            lat += bias_m / 111_320.0
            if (k % (HZ // GPS_HZ)) == 0:
                last_fix = (lat, lon, int(t * 1e6))
            in_dropout = dropout is not None and dropout[0] <= t - t0 <= dropout[1]
            gps = None if (in_dropout or last_fix is None) else GpsReading(
                t, last_fix[0], last_fix[1], speed_mps, 0.0, device_ts_us=last_fix[2])
            insert_sample(conn, RawSample(
                run_id, t,
                joulemeter=JoulemeterReading(t, voltage, power_w / voltage, device_ts_us=int(t * 1e6)),
                gps=gps, device_seq=2 * k, device_global_ts_us=int(t * 1e6), filtered_speed=speed_mps))
            k += 1
    return {"lap_length_m": total, "samples": k, "duration_s": (k - 1) / HZ, "speed": speed_mps, "power_w": power_w}
