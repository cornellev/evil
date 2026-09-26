import asyncio
from pathlib import Path

from evil.ingestion.csv_file_source import CsvFileSource


def _write_csv(tmp_path: Path, content: str) -> str:
    path = tmp_path / "recording.csv"
    path.write_text(content)
    return str(path)


def _collect(source: CsvFileSource) -> list:
    async def _drain():
        return [sample async for sample in source.samples()]

    return asyncio.run(_drain())


def test_parses_dotted_column_names(tmp_path):
    csv_path = _write_csv(
        tmp_path,
        "global_ts,gps.lat,gps.long,speed,power.voltage,power.current\n"
        "10.0,42.1,-76.2,5.0,48.0,3.0\n",
    )

    samples = _collect(CsvFileSource(run_id="run-1", path=csv_path))

    assert len(samples) == 1
    s = samples[0]
    assert s.run_id == "run-1"
    assert s.ts == 10.0
    assert s.gps.lat == 42.1
    assert s.gps.lon == -76.2
    assert s.gps.speed == 5.0
    assert s.joulemeter.voltage == 48.0
    assert s.joulemeter.current == 3.0


def test_dotted_and_flat_column_names_are_equivalent(tmp_path):
    """RaceEngineerDashboard's own replay parser treats "gps.lat" and
    "gpslat" as the same column (punctuation stripped before matching)."""
    dotted = _write_csv(tmp_path, "global_ts,gps.lat,gps.long\n1.0,10.0,20.0\n")
    flat = _write_csv(tmp_path, "GlobalTS,GPS_LAT,GPS LONG\n1.0,10.0,20.0\n")

    dotted_samples = _collect(CsvFileSource(run_id="r", path=dotted))
    flat_samples = _collect(CsvFileSource(run_id="r", path=flat))

    assert dotted_samples[0].gps.lat == flat_samples[0].gps.lat == 10.0
    assert dotted_samples[0].gps.lon == flat_samples[0].gps.lon == 20.0


def test_missing_timestamp_falls_back_to_row_index(tmp_path):
    csv_path = _write_csv(tmp_path, "gps.lat,gps.long\n1.0,2.0\n3.0,4.0\n")

    samples = _collect(CsvFileSource(run_id="run-1", path=csv_path))

    assert [s.ts for s in samples] == [0.0, 1.0]


def test_row_with_no_gps_or_power_columns_has_none_readings(tmp_path):
    csv_path = _write_csv(tmp_path, "global_ts,steering.turn_angle\n1.0,5.0\n")

    samples = _collect(CsvFileSource(run_id="run-1", path=csv_path))

    assert samples[0].gps is None
    assert samples[0].joulemeter is None
