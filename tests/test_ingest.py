import asyncio

from evil.ingest import ingest_sample
from evil.ingestion.replay_source import ReplaySource
from evil.models import GpsReading, JoulemeterReading, RawSample


def test_ingest_sample_writes_raw_tables_and_snapshot(conn):
    sample = RawSample(
        run_id="run-1",
        ts=100.0,
        joulemeter=JoulemeterReading(ts=100.0, voltage=48.2, current=3.1),
        gps=GpsReading(ts=100.0, lat=42.44, lon=-76.48, speed=5.0),
    )

    seq = ingest_sample(conn, sample)

    row = conn.execute("SELECT * FROM main_snapshot WHERE seq = ?", (seq,)).fetchone()
    assert row["run_id"] == "run-1"
    assert row["joulemeter_id"] is not None
    assert row["gps_id"] is not None
    assert row["planner_id"] is None

    jm = conn.execute("SELECT * FROM joulemeter WHERE id = ?", (row["joulemeter_id"],)).fetchone()
    assert jm["voltage"] == 48.2
    assert jm["current"] == 3.1


def test_ingest_sample_assigns_increasing_seq(conn):
    seqs = [
        ingest_sample(conn, RawSample(run_id="run-1", ts=float(i)))
        for i in range(5)
    ]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == 5


def test_replay_source_feeds_ingest(conn):
    samples = [RawSample(run_id="run-1", ts=float(i)) for i in range(3)]
    source = ReplaySource(samples)

    async def drain():
        seqs = []
        async for sample in source.samples():
            seqs.append(ingest_sample(conn, sample))
        return seqs

    seqs = asyncio.run(drain())
    assert len(seqs) == 3
