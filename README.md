# Electric Vehicle Intelligence Layer (EVIL)

Raw + derived telemetry storage for CEV. Plain Python (stdlib `sqlite3`, no ORM)
and plain SQL. Runs on an edge device, reached over Tailscale. Exposes its
read-only tools over MCP (Streamable HTTP) for any client on the tailnet --
`tern-llm`'s own harness, a CEV member's own Claude client, or `evil-ui`'s
backend -- not just one fixed consumer.

Full design rationale (and the alternatives that were rejected) lives in
`inference-agent/4.md` in this same working directory. This README is the
"how it's built," that doc is the "why."

## Getting started

```bash
# 1. Load static reference data ONCE per track (skip if evil.db already has it)
docker compose run --rm mcp-server python -m evil.scripts.seed_reference_data turn "Turn 1" 42.0 -76.0 30
docker compose run --rm mcp-server python -m evil.scripts.seed_reference_data start-finish 42.0 -76.0 30

# 2. Run the MCP server -- what tern-llm, evil-ui, and CEV members' own
#    Claude clients all connect to. Binds 0.0.0.0:8765, reachable over
#    Tailscale automatically once the device is on the tailnet.
docker compose up --build

# 3. Separately: the live compiler against the real ROS2 topic. Builds from
#    Dockerfile.ros2 (ros:humble-ros-base), not this repo's default
#    python:3.12-slim Dockerfile -- see the note below.
docker compose --profile live up

# Or, offline / after a race: bulk-load a recorded session instead of step 3.
docker compose run --rm mcp-server python -m evil.scripts.ingest_recording <recording.csv-or-.db3> <run-id>

# Recordings: `upload` (also started by the bare `docker compose up`) stores
# every upload RAW and cataloged on :8766 and returns once it is stored; the
# `worker` service then scans and parses it off the catalog's job queue.
# evil-ui's backend proxies to it, or POST directly:
#   curl -F files=@recording.db3 -F category=testing http://<host>:8766/recordings
# (several -F files=@... for a rosbag folder: .db3 + metadata.yaml). Any file is
# accepted, even unreadable ones; see recording-catalog-design.md.
#
# Moving an existing deployment to the catalog layout (once): the parsed DB is a
# fresh file, so copy the reference data (turn geometry, start/finish) across:
docker compose run --rm mcp-server python -m evil.scripts.migrate_reference_data \
    --from /data/evil.db --to /data/parsed/uc26/evil_uc26.db
```

All of the above share the same `evil-data` volume (see `docker-compose.yml`),
so the CLI commands and the running `mcp-server`/`upload`/`worker` services see the
same data: `catalog.db` (recordings + job queue), `raw/` (originals) and
`parsed/uc26/evil_uc26.db` (what the MCP tools read).

`live-compiler` builds from `Dockerfile.ros2` (`ros:humble-ros-base`), not
this repo's default `Dockerfile` -- `mcp-server`/`upload` stay on the lighter
`python:3.12-slim` image since they never import `evil.ingestion.ros2_source`
(the only module here that needs `rclpy`). It runs with `network_mode: host`
since ROS2's default DDS discovery relies on multicast, which isn't reliable
across an isolated docker bridge network. Real `rclpy` verification used a
separate mock publisher, see `../mock-daq` -- confirmed working: `mock-daq`'s
containerized publisher genuinely publishes over rclpy, and this image
genuinely builds, starts, imports `Ros2Source`, and writes `evil.db` to the
mounted `/data` volume (not the ephemeral container filesystem -- this
surfaced and fixed a real bug where `run_live_compiler.py`'s `--db` default
never read `EVIL_DB_PATH`, so the volume mount was silently a no-op).
Publisher and compiler now have a real automated cross-container test --
`../mock-daq/tests/e2e/smoke_cross_container_round_trip.sh` builds both
images, seeds a real db, runs both containers on the same topic, and checks
for real ingested rows. It currently SKIPs (not fails) here: `mock-daq`'s
own e2e test found this dev environment's DDS discovery doesn't work at
all, even for a publisher and subscriber in the *same* container/network
namespace (confirmed via the official `ros2 topic list` CLI hanging
indefinitely there too). That's a WSL2 networking limitation, not something
specific to crossing a container boundary -- expect this to behave
differently on a real Linux host (the actual NUC).

`docker compose` (the v2 plugin) is installed and confirmed working in this
environment now -- `mcp-server`, `upload`, and `live-compiler` have each been
built and started for real via `docker compose up`, not just syntax-checked.

## Layout

```
schema/                 SQL DDL, applied in filename order, idempotent
src/evil/
  db.py                       connect() / connect_readonly() / apply_schema()
  models.py                   RawSample -- the transport-independent tick shape
  ingest.py                   writes a RawSample into raw tables + main_snapshot
  geo.py                      haversine distance helper
  registered_classifiers.py   ALL_CLASSIFIERS -- the one list ingestion and the live compiler both use
  ingestion/
    base.py                IngestionSource protocol (the ROS2/Zenoh seam)
    normalize.py           shared flatten + alias-match: every source routes through this
    replay_source.py       list-backed source, used by tests and offline replay
    ros2_source.py         today's live transport: rclpy subscriber on /spi_data
    csv_file_source.py     bulk historical ingestion from a CSV export
    rosbag_file_source.py  bulk historical ingestion from a rosbag2 .db3 file
  classifiers/
    base.py             Classifier protocol + ClassifierSpec
    registry.py         topological sort by depends_on
    runner.py           incremental tick(): per-classifier cursor + margin
    metrics.py          shared energy/avg-speed helpers used by laps.py and straights.py
    turns.py            example classifier: GPS geofence turn segmentation
    laps.py             depends_on=["turns"]: energy, avg speed, turn count per lap
    straights.py        depends_on=["turns"]: the complement of turns, no open-state needed
  tools/
    get_turn.py                 "how was I in turn X"
    compare_turn_instances.py   "what could I have done better" (this turn, across attempts)
    compare_laps.py             same, across two laps (energy, avg speed, turn count, duration)
    turn_name_matching.py       digit-normalized fallback so "3" also resolves to "Turn 3"
    list_runs.py, list_turns.py, list_laps.py, list_straights.py
                                 browsing-shaped (paginated, no lookup needed) -- for evil-ui
    catalog_tools.py            list_recordings / describe_recording / find_nas_files over catalog.db (read-only)
    read_only_sql.py            constrained fallback: SELECT/WITH-only, row cap, step budget
    registry.py                 binds tools to a connection, Ollama-shaped schemas (used by tests)
  mcp_server.py             exposes the tools above over MCP / Streamable HTTP (:8765)
  mcp_raw_server.py         separate server (:8767): query_recording_sql / catalog_sql
  rawquery.py, rawquery_child.py   the sandbox those two tools run in
  upload_server.py          write-only HTTP: /recordings (catalog), /system/status, /locations, legacy /upload
  catalog.py                recording catalog: staging, dedup, manifests, jobs, backup, status
  parser.py, scan.py        container readers + strict schema match; the cheap level-1 scan
  schemas/telemetry_v1.py   the strict evil.telemetry.v1 payload definition
  worker.py, parse_job.py   job queue worker (fast/deep lanes) and the isolated parse subprocess
  scripts/
    seed_reference_data.py  load track_geometry / start_finish_line rows
    ingest_recording.py     bulk-load a recorded CSV or rosbag .db3 as historical data
    migrate_reference_data.py  copy track geometry into the new parsed DB
    run_live_compiler.py    the production entry point: live Ros2Source + ALL_CLASSIFIERS
tests/                  pytest, one file per module above
tests/e2e/              bash scripts exercising the real running server, see below
```

## Why a cursor per classifier, not one shared one

Called a cursor here, not the industry-standard "watermark" (Spark/Flink's
term for the same concept) -- this project also has an LLM harness, and
"watermark" collides with LLM output-watermarking (SynthID and similar). Same
mechanism, different name.

Each classifier declares its own `lookback_margin_s` (how long to wait before
it's willing to call a row "final") and its own `depends_on` (raw data, or
another classifier's name, forming a DAG). `laps`/`straights` depend on
`turns` without the runner changing. A new classifier is one file plus one
line in `registered_classifiers.py` -- see `classifiers/turns.py` for the
pattern to copy. Details and the alternatives considered (CDC, broker
offsets, dirty-flag columns) are in `inference-agent/4.md` section 4.

## Ingestion, the generic normalizer, and a possible Zenoh migration

`IngestionSource` is the seam between however data arrives and how it's
stored. The actual wire format is JSON everywhere already -- `/spi_data` is
a `std_msgs/String` carrying a JSON snapshot, a CSV row is a flat JSON-shaped
object with string values, and a rosbag's recorded messages are the same
JSON strings CDR-encoded. `ingestion/normalize.py` handles all of it once:
flatten nested dicts to dotted keys, normalize casing/punctuation, alias-match
against canonical fields. `Ros2Source`, `CsvFileSource`, and
`RosbagFileSource` all route through it -- extending to a new field or a
future Zenoh payload shape means editing one alias table, not writing a new
per-format parser. If autonomy's Zenoh move stays inside ROS2 (`rmw_zenoh`),
`Ros2Source` likely doesn't change at all; if it goes native, a new
`ZenohSource` is one file that hands its JSON payload to the same
normalizer. See `inference-agent/4.md` section 6.

## MCP server: why it lives here, and the concurrency design

The tool functions live here (not in `tern-llm` or `evil-ui`) because
they're schema-coupled to these tables, and MCP colocated with EVIL on one
device makes this code EVIL's public API. Each tool call in `mcp_server.py`
opens its own short-lived `connect_readonly()` connection and runs the
blocking query via a thread offload -- never a connection shared across
calls, which wouldn't be safe for concurrent use. There is deliberately no
single-in-flight lock here (unlike `tern-llm`'s `/ask` endpoint): that lock
protects one scarce local LLM inference slot, which doesn't exist on this
side, since external MCP clients run their own model inference elsewhere.
SQLite's WAL mode (already set in `db.connect()`) lets any number of
readers proceed alongside the single ingestion writer without blocking
each other.

Writes never go through MCP -- uploads, the worker and bulk ingestion are
separate processes, same reasoning as why `read_only_sql` is generic-but-read-only:
a bad write is a categorically worse failure than a bad read.

## Running tests

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest                        # unit, in-process (no network) -- includes a
                                                   # Hypothesis fuzz test for read_only_sql
./tests/e2e/smoke_mcp_server.sh                   # real server, real MCP client, real HTTP
./tests/e2e/smoke_mcp_server_tailscale.sh         # same, over this machine's real tailnet IP
./tests/e2e/smoke_mcp_server_concurrency.sh       # 15 real concurrent MCP clients against one server
./tests/e2e/smoke_ingest_recording.sh             # real CLI subprocess, CSV
./tests/e2e/smoke_ingest_recording_rosbag.sh      # same, rosbag .db3
```

`tests/test_read_only_sql_fuzz.py` throws ~200 random/adversarial query
strings per run at `read_only_sql` (a mix of pure noise and real SQL
fragments -- `DROP TABLE`, `ATTACH DATABASE`, injection-shaped strings,
etc.), asserting the one property that actually matters: no input, however
malformed, ever changes a row. It still passes with the `_DISALLOWED`
keyword regex removed entirely (checked by hand) -- the connection's own
`mode=ro` backstops it at the SQLite driver level, genuine defense in depth,
not a gap in the test.

`smoke_mcp_server_concurrency.sh` fires 15 real, concurrent MCP client
sessions (mixed `read_only_sql`/`list_runs`) at one real running server,
proving the "each call opens its own connection, WAL mode lets readers
proceed alongside each other" design actually holds under concurrent load,
not just in isolation.

The tailscale variant is a genuine reachability proof where `tailscale` is
installed and connected (this dev machine is on CEV's real tailnet --
`cev-nuc` is visible in `tailscale status`), not a simulation. It skips
cleanly where tailscale isn't available. It proves the `0.0.0.0` bind serves
an external interface, not just loopback -- it does **not** prove `cev-nuc`
specifically can serve this, since that needs the server actually deployed
there. Not done here on purpose: deploying to or otherwise touching
`cev-nuc` is a real, shared team device, not something to do without it
being asked for.

## Not yet built

- More classifiers beyond `turns`/`laps`/`straights` -- "events" turned out
  to be a general term for whatever gets formulated next (voltage-sag /
  current-spike / power-mismatch anomaly detection ported from
  `Race-GPT/main.py`'s `precheck()` is the strongest concrete candidate,
  since that logic is already proven, just never persisted as EVIL rows) --
  deliberately left out of this pass.
- Running `LiveCompiler` (`scripts/run_live_compiler.py`) against a real
  `Ros2Source` in production. The service loop itself is built and tested
  (event-driven wake, bounded fallback interval, incremental classification
  proven via a delayed `ReplaySource` in `tests/test_run_live_compiler.py`).
  A real mock publisher exists now (`../mock-daq`, matching
  `uc26_sensor_reader`'s exact telemetry shape) to exercise the live
  `rclpy` half specifically, but that round trip hasn't been run anywhere
  yet -- it needs ROS2 Humble on a matching Ubuntu 22.04 environment,
  see `mock-daq/README.md` for why (Humble targets Jammy, not this dev
  machine's Noble).
- A watcher that uploads (or registers) `tailscale-ros-telemetry`'s rosbag
  recordings automatically when they finish -- today a human uploads them.
- `evil.telemetry.v2` (the later payload shape: `errcount`, `duty_cycle`):
  deferred, such recordings are stored and `skipped`. Autonomy planner data and
  the Zenoh recording format also wait for real samples.
- Deploying this server to `cev-nuc` and confirming a client elsewhere on
  the tailnet can reach it -- verified here over this dev machine's own
  tailnet interface only.
