"""The recording catalog: stores uploaded files raw and indexes them.

Phase 1 of recording-catalog-design.md. An upload is staged under
`incoming/`, hashed while it is written, and only when every file has fully
arrived is it atomically renamed into the raw layout and cataloged. Nothing
is parsed here. A `manifest.json` next to each recording's files mirrors the
human-entered catalog fields so the catalog can be rebuilt from the raw tree
(`reconcile`).

Layout under the data root (default: the directory holding EVIL_DB_PATH):

    catalog.db
    raw/<source>/<car>/<YYYY>/<YYYY-MM-DD>/<recording_id>/<files...> + manifest.json
    incoming/<upload_id>/...          staging, swept after 24 h
    backup/catalog-YYYYMMDD.db        nightly copies, last 7 kept
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable

from evil.db import SCHEMA_DIR

CATALOG_SCHEMA_DIR = SCHEMA_DIR / "catalog"
MANIFEST_NAME = "manifest.json"
MANIFEST_VERSION = 1
CHUNK = 1024 * 1024
STAGING_MAX_AGE_SEC = 24 * 3600
SPOOL_DIR = ".spool"
BACKUPS_KEPT = 7

SOURCES = ("telemetry", "autonomy", "other")
CATEGORIES = ("competition", "testing", "bench", "sim", "other")
EDITABLE_FIELDS = ("label", "notes", "category", "event", "car")
MAX_PATH_DEPTH = 6
SQLITE_MAGIC = b"SQLite format 3\x00"


class CatalogError(ValueError):
    """The request is invalid (maps to HTTP 400)."""


class UploadTooLarge(CatalogError):
    """Maps to HTTP 413."""


@dataclass
class DataRoot:
    path: Path

    @property
    def catalog_db(self) -> Path:
        return self.path / "catalog.db"

    @property
    def raw(self) -> Path:
        return self.path / "raw"

    @property
    def incoming(self) -> Path:
        return self.path / "incoming"

    @property
    def spool(self) -> Path:
        return self.incoming / SPOOL_DIR

    @property
    def backup(self) -> Path:
        return self.path / "backup"

    def ensure(self) -> None:
        for d in (self.path, self.raw, self.incoming, self.spool, self.backup):
            d.mkdir(parents=True, exist_ok=True)


def parsed_db_path(root: DataRoot, car: str | None) -> Path:
    """Where a car's parsed database lives: parsed/<car>/evil_<car>.db."""
    car = sanitize_relpath(car or "uc26").replace("/", "_")
    return root.path / "parsed" / car / f"evil_{car}.db"


def ensure_parsed_db(root: DataRoot, car: str | None = None) -> Path:
    """Create the car's parsed DB with the current schema if it is missing, so
    the read-only MCP server always has a database to open (it never creates one).
    Reference data (track_geometry, start_finish_line) is NOT created here: load it
    with evil.scripts.seed_reference_data / migrate_reference_data."""
    from evil import db

    path = parsed_db_path(root, car)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(str(path))
    try:
        db.apply_schema(conn)
    finally:
        conn.close()
    return path


def data_root_from_env() -> DataRoot:
    explicit = os.getenv("EVIL_DATA_ROOT")
    if explicit:
        return DataRoot(Path(explicit))
    return DataRoot(Path(os.getenv("EVIL_DB_PATH", "evil.db")).resolve().parent)


# Columns added after a catalog.db may already exist (CREATE TABLE IF NOT EXISTS
# does not add them). Idempotent.
_ADDED_COLUMNS = {
    "recordings": {"parse_stats_json": "TEXT"},
}


def _ensure_columns(conn: sqlite3.Connection) -> None:
    for table, columns in _ADDED_COLUMNS.items():
        have = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        for name, decl in columns.items():
            if name not in have:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def connect_catalog(root: DataRoot) -> sqlite3.Connection:
    root.ensure()
    conn = sqlite3.connect(str(root.catalog_db), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    for sql_file in sorted(CATALOG_SCHEMA_DIR.glob("*.sql")):
        conn.executescript(sql_file.read_text())
    _ensure_columns(conn)
    conn.commit()
    return conn


def connect_catalog_readonly(path: str | Path) -> sqlite3.Connection | None:
    """Read-only handle for the MCP server (it never creates or writes the
    catalog). None if the catalog does not exist yet."""
    if not Path(path).exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


# ---- helpers -------------------------------------------------------------

def new_id() -> str:
    """Time-ordered unique id (sorts by creation time)."""
    return f"{int(time.time() * 1000):012x}{secrets.token_hex(5)}"


_SAFE_COMPONENT = re.compile(r"[^A-Za-z0-9._ +=@()-]")


def sanitize_relpath(name: str) -> str:
    """Keep a client-supplied relative path (folder uploads) but make it safe:
    no absolute paths, no `..`, no odd characters, bounded depth."""
    parts = [p for p in re.split(r"[\\/]+", name or "") if p not in ("", ".", "..")]
    parts = [_SAFE_COMPONENT.sub("_", p).lstrip(".") or "_" for p in parts]
    if not parts:
        raise CatalogError("empty file name")
    if len(parts) > MAX_PATH_DEPTH:
        parts = parts[-MAX_PATH_DEPTH:]
    return "/".join(p[:200] for p in parts)


def detect_role(rel_path: str) -> str:
    name = rel_path.rsplit("/", 1)[-1].lower()
    if name.endswith(".db3"):
        return "db3"
    if name == "metadata.yaml":
        return "metadata"
    if name.endswith(".csv"):
        return "csv"
    if name.endswith((".mp4", ".mkv", ".avi", ".mov")):
        return "video"
    return "other"


def detect_container(files: list["StagedFile"], root_dir: Path) -> str:
    """By structure, not extension alone: a .db3 must start with the SQLite
    magic bytes to count as a rosbag2 container."""
    roles = {f.role for f in files}
    if "db3" in roles:
        for f in files:
            if f.role == "db3":
                with open(root_dir / f.rel_path, "rb") as fh:
                    if fh.read(16) == SQLITE_MAGIC:
                        return "rosbag2-sqlite3"
        return "unknown"
    if roles == {"csv"}:
        return "csv"
    return "unknown"


@dataclass
class StagedFile:
    original_name: str
    rel_path: str
    size_bytes: int
    sha256: str
    role: str


@dataclass
class UploadFields:
    source: str = "telemetry"
    category: str | None = None
    car: str | None = None
    event: str | None = None
    label: str | None = None
    notes: str | None = None
    uploader: str | None = None

    def validated(self) -> "UploadFields":
        if self.source not in SOURCES:
            raise CatalogError(f"source must be one of {', '.join(SOURCES)}")
        if self.category is not None and self.category not in CATEGORIES:
            raise CatalogError(f"category must be one of {', '.join(CATEGORIES)}")
        return self


def _clean(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


def fields_from_form(**raw: str | None) -> UploadFields:
    return UploadFields(
        source=_clean(raw.get("source")) or "telemetry",
        category=_clean(raw.get("category")),
        car=_clean(raw.get("car")),
        event=_clean(raw.get("event")),
        label=_clean(raw.get("label")),
        notes=_clean(raw.get("notes")),
        uploader=_clean(raw.get("uploader")),
    ).validated()


# ---- staging -------------------------------------------------------------

class Staging:
    """One in-progress upload. Files live only under incoming/<upload_id>/
    until `commit`, so an aborted upload is just a folder that `discard` (or
    the sweep) removes -- it can never produce a catalog row."""

    def __init__(self, root: DataRoot, max_total_bytes: int):
        self.root = root
        self.upload_id = new_id()
        self.dir = root.incoming / self.upload_id
        self.max_total_bytes = max_total_bytes
        self.files: list[StagedFile] = []
        self._total = 0
        self.dir.mkdir(parents=True)

    def add(self, original_name: str, stream: BinaryIO) -> StagedFile:
        rel = sanitize_relpath(original_name)
        # Two parts with the same name must not overwrite each other.
        taken = {f.rel_path for f in self.files}
        if rel in taken:
            stem, dot, ext = rel.rpartition(".")
            n = 2
            while True:
                candidate = f"{stem}_{n}.{ext}" if dot else f"{rel}_{n}"
                if candidate not in taken:
                    rel = candidate
                    break
                n += 1
        dest = self.dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        with open(dest, "wb") as out:
            while True:
                chunk = stream.read(CHUNK)
                if not chunk:
                    break
                size += len(chunk)
                self._total += len(chunk)
                if self._total > self.max_total_bytes:
                    raise UploadTooLarge("upload too large")
                digest.update(chunk)
                out.write(chunk)
        staged = StagedFile(original_name, rel, size, digest.hexdigest(), detect_role(rel))
        self.files.append(staged)
        return staged

    def discard(self) -> None:
        shutil.rmtree(self.dir, ignore_errors=True)


def content_hash(files: Iterable[StagedFile | dict]) -> str:
    hashes = sorted(f["sha256"] if isinstance(f, dict) else f.sha256 for f in files)
    return hashlib.sha256("\n".join(hashes).encode()).hexdigest()


# ---- manifest ------------------------------------------------------------

def _manifest_fields(row: sqlite3.Row | dict) -> dict:
    return {k: row[k] for k in ("source", "label", "notes", "category", "event", "car", "uploader")}


def write_manifest(directory: Path, recording: sqlite3.Row | dict, files: list[dict]) -> None:
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "recording_id": recording["recording_id"],
        "uploaded_at": recording["uploaded_at"],
        "original_name": recording["original_name"],
        "content_sha256": recording["content_sha256"],
        "total_bytes": recording["total_bytes"],
        "container": recording["container"],
        "fields": _manifest_fields(recording),
        "files": files,
    }
    tmp = directory / (MANIFEST_NAME + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    os.replace(tmp, directory / MANIFEST_NAME)


def _file_dicts(files: list[StagedFile]) -> list[dict]:
    return [
        {"original_name": f.original_name, "rel_path": f.rel_path, "size_bytes": f.size_bytes,
         "sha256": f.sha256, "role": f.role}
        for f in files
    ]


# ---- commit --------------------------------------------------------------

@dataclass
class CommitResult:
    recording_id: str
    deduplicated: bool


def _find_by_hash(conn: sqlite3.Connection, sha: str) -> str | None:
    row = conn.execute("SELECT recording_id FROM recordings WHERE content_sha256 = ?", (sha,)).fetchone()
    return row["recording_id"] if row else None


def _raw_dir(root: DataRoot, source: str, car: str | None, uploaded_at: float, recording_id: str) -> Path:
    when = datetime.fromtimestamp(uploaded_at, tz=timezone.utc)
    car_part = sanitize_relpath(car) if car else "unknown"
    return root.raw / source / car_part.replace("/", "_") / f"{when:%Y}" / f"{when:%Y-%m-%d}" / recording_id


def enqueue_job(conn: sqlite3.Connection, recording_id: str, kind: str, lane: str, max_attempts: int = 2) -> int:
    """Add a pending job (the caller owns the transaction)."""
    return conn.execute(
        "INSERT INTO jobs (recording_id, kind, lane, created_at, max_attempts) VALUES (?, ?, ?, ?, ?)",
        (recording_id, kind, lane, time.time(), max_attempts),
    ).lastrowid


def enqueue_ingest_jobs(conn: sqlite3.Connection, recording_id: str) -> None:
    """The cheap scan runs first on the fast lane; the parse is a deep-lane job."""
    enqueue_job(conn, recording_id, "scan", "fast")
    enqueue_job(conn, recording_id, "parse", "deep")


def _insert_recording(conn: sqlite3.Connection, rec: dict, files: list[dict], root: DataRoot, final_dir: Path) -> None:
    rel_dir = final_dir.relative_to(root.raw).as_posix()
    with conn:
        conn.execute(
            """INSERT INTO recordings (recording_id, source, container, car, category, event, label, notes,
                   original_name, uploaded_at, uploader, storage_state, total_bytes, content_sha256, parse_status)
               VALUES (:recording_id, :source, :container, :car, :category, :event, :label, :notes,
                   :original_name, :uploaded_at, :uploader, 'stored', :total_bytes, :content_sha256, 'pending')""",
            rec,
        )
        for f in files:
            conn.execute(
                """INSERT INTO recording_files (file_id, recording_id, role, original_name, storage_backend,
                       rel_path, size_bytes, sha256)
                   VALUES (?, ?, ?, ?, 'nuc-local', ?, ?, ?)""",
                (new_id(), rec["recording_id"], f["role"], f["original_name"],
                 f"{rel_dir}/{f['rel_path']}", f["size_bytes"], f["sha256"]),
            )
        enqueue_ingest_jobs(conn, rec["recording_id"])


def commit(conn: sqlite3.Connection, root: DataRoot, staging: Staging, fields: UploadFields,
           original_name: str | None = None) -> CommitResult:
    """Called only once every file has fully arrived. Dedups by content hash,
    writes the manifest, atomically moves the staging folder into the raw
    layout, then inserts the catalog rows."""
    if not staging.files:
        staging.discard()
        raise CatalogError("no files in upload")
    fields = fields.validated()
    sha = content_hash(staging.files)
    existing = _find_by_hash(conn, sha)
    if existing:
        staging.discard()
        return CommitResult(existing, True)

    recording_id = new_id()
    uploaded_at = time.time()
    rec = {
        "recording_id": recording_id,
        "source": fields.source, "car": fields.car, "category": fields.category, "event": fields.event,
        "label": fields.label, "notes": fields.notes, "uploader": fields.uploader,
        "original_name": original_name or (staging.files[0].original_name if len(staging.files) == 1 else None),
        "uploaded_at": uploaded_at,
        "container": detect_container(staging.files, staging.dir),
        "total_bytes": sum(f.size_bytes for f in staging.files),
        "content_sha256": sha,
    }
    files = _file_dicts(staging.files)
    write_manifest(staging.dir, rec, files)

    final_dir = _raw_dir(root, fields.source, fields.car, uploaded_at, recording_id)
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    os.rename(staging.dir, final_dir)  # atomic: same filesystem under the data root
    try:
        _insert_recording(conn, rec, files, root, final_dir)
    except sqlite3.IntegrityError:
        # Lost a race with an identical concurrent upload: keep theirs.
        shutil.rmtree(final_dir, ignore_errors=True)
        existing = _find_by_hash(conn, sha)
        if existing:
            return CommitResult(existing, True)
        raise
    return CommitResult(recording_id, False)


# ---- queries and edits ---------------------------------------------------

def _row(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row else None


def get_recording(conn: sqlite3.Connection, recording_id: str) -> dict | None:
    rec = _row(conn.execute("SELECT * FROM recordings WHERE recording_id = ?", (recording_id,)).fetchone())
    if rec is None:
        return None
    rec["files"] = [dict(r) for r in conn.execute(
        "SELECT * FROM recording_files WHERE recording_id = ? ORDER BY rel_path", (recording_id,))]
    return rec


def list_recordings(conn: sqlite3.Connection, *, since: float | None = None, until: float | None = None,
                    category: str | None = None, car: str | None = None, parse_status: str | None = None,
                    source: str | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
    where, args = [], []
    when = "COALESCE(recorded_start, uploaded_at)"
    if since is not None:
        where.append(f"{when} >= ?"); args.append(since)
    if until is not None:
        where.append(f"{when} < ?"); args.append(until)
    for column, value in (("category", category), ("car", car), ("parse_status", parse_status), ("source", source)):
        if value:
            where.append(f"{column} = ?"); args.append(value)
    sql = "SELECT * FROM recordings"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {when} DESC, recording_id DESC LIMIT ? OFFSET ?"
    args += [max(1, min(limit, 500)), max(0, offset)]
    return [dict(r) for r in conn.execute(sql, args)]


def raw_dir_of(conn: sqlite3.Connection, root: DataRoot, recording_id: str) -> Path | None:
    row = conn.execute("SELECT rel_path FROM recording_files WHERE recording_id = ? LIMIT 1", (recording_id,)).fetchone()
    if row is None:
        return None
    rel = row["rel_path"]
    # rel_path is "<dir relative to raw>/<file relative to recording dir>"; the manifest sits in the recording dir.
    for parent in (root.raw / rel).parents:
        if (parent / MANIFEST_NAME).exists():
            return parent
    return None


def update_recording(conn: sqlite3.Connection, root: DataRoot, recording_id: str, changes: dict) -> dict:
    """Edit the human-entered fields; rewrites the manifest so the raw tree
    stays the source for rebuilding them."""
    changes = {k: _clean(v) if isinstance(v, str) or v is None else v for k, v in changes.items() if k in EDITABLE_FIELDS}
    if not changes:
        raise CatalogError(f"nothing to update; editable fields: {', '.join(EDITABLE_FIELDS)}")
    if changes.get("category") is not None and changes["category"] not in CATEGORIES:
        raise CatalogError(f"category must be one of {', '.join(CATEGORIES)}")
    existing = get_recording(conn, recording_id)
    if existing is None:
        raise KeyError(recording_id)
    with conn:
        conn.execute(
            f"UPDATE recordings SET {', '.join(f'{k} = ?' for k in changes)} WHERE recording_id = ?",
            [*changes.values(), recording_id],
        )
    updated = get_recording(conn, recording_id)
    directory = raw_dir_of(conn, root, recording_id)
    if directory is not None:
        write_manifest(directory, updated, [
            {"original_name": f["original_name"], "rel_path": _manifest_rel(f, directory, root),
             "size_bytes": f["size_bytes"], "sha256": f["sha256"], "role": f["role"]}
            for f in updated["files"]
        ])
    return updated


def _manifest_rel(file_row: dict, directory: Path, root: DataRoot) -> str:
    return (root.raw / file_row["rel_path"]).relative_to(directory).as_posix()


# ---- maintenance: sweep, reconcile, backup -------------------------------

def sweep_incoming(root: DataRoot, max_age_sec: float = STAGING_MAX_AGE_SEC, now: float | None = None) -> int:
    """Delete staging folders untouched for max_age_sec (killed uploads whose
    request handler never got to clean up). Returns how many were removed."""
    now = time.time() if now is None else now
    removed = 0
    if not root.incoming.exists():
        return 0
    for entry in root.incoming.iterdir():
        if entry.name == SPOOL_DIR:
            # Request-body spool files live here; the directory itself persists.
            for spooled in entry.iterdir():
                try:
                    if now - spooled.stat().st_mtime > max_age_sec:
                        spooled.unlink(missing_ok=True)
                        removed += 1
                except OSError:
                    continue
            continue
        try:
            newest = max([entry.stat().st_mtime] + [p.stat().st_mtime for p in entry.rglob("*")])
        except OSError:
            continue
        if now - newest > max_age_sec:
            shutil.rmtree(entry, ignore_errors=True)
            removed += 1
    return removed


def reconcile(conn: sqlite3.Connection, root: DataRoot) -> list[str]:
    """Adopt raw folders that have a manifest but no catalog row (a crash
    between the rename and the insert, or a lost catalog.db). Returns the
    recording_ids it added. File sizes are checked; a folder whose files do
    not match its manifest is left alone and reported by the caller's log."""
    adopted: list[str] = []
    if not root.raw.exists():
        return adopted
    known = {r["recording_id"] for r in conn.execute("SELECT recording_id FROM recordings")}
    for manifest_path in root.raw.rglob(MANIFEST_NAME):
        directory = manifest_path.parent
        try:
            m = json.loads(manifest_path.read_text())
            rid = m["recording_id"]
        except (OSError, ValueError, KeyError):
            continue
        if rid in known:
            continue
        if any(not (directory / f["rel_path"]).is_file()
               or (directory / f["rel_path"]).stat().st_size != f["size_bytes"] for f in m["files"]):
            continue
        fields = m.get("fields", {})
        rec = {
            "recording_id": rid, "source": fields.get("source") or "telemetry", "car": fields.get("car"),
            "category": fields.get("category"), "event": fields.get("event"), "label": fields.get("label"),
            "notes": fields.get("notes"), "uploader": fields.get("uploader"),
            "original_name": m.get("original_name"), "uploaded_at": m["uploaded_at"],
            "container": m.get("container", "unknown"), "total_bytes": m["total_bytes"],
            "content_sha256": m["content_sha256"],
        }
        try:
            _insert_recording(conn, rec, m["files"], root, directory)
        except sqlite3.IntegrityError:
            continue
        adopted.append(rid)
        known.add(rid)
    return adopted


def backup_catalog(conn: sqlite3.Connection, root: DataRoot, keep: int = BACKUPS_KEPT,
                   today: datetime | None = None) -> Path | None:
    """One copy per UTC day via SQLite's online-backup API; prunes to `keep`.
    Returns the new file, or None if today's copy already exists."""
    root.backup.mkdir(parents=True, exist_ok=True)
    stamp = (today or datetime.now(timezone.utc)).strftime("%Y%m%d")
    dest = root.backup / f"catalog-{stamp}.db"
    created = None
    if not dest.exists():
        tmp = dest.with_suffix(".db.tmp")
        out = sqlite3.connect(str(tmp))
        try:
            conn.backup(out)
        finally:
            out.close()
        os.replace(tmp, dest)
        created = dest
    for old in sorted(root.backup.glob("catalog-*.db"))[:-keep]:
        old.unlink(missing_ok=True)
    return created


# ---- reparse, locations, status ------------------------------------------

def request_reparse(conn: sqlite3.Connection, recording_id: str, rescan: bool = False) -> int | None:
    """Queue a parse (and optionally a fresh scan) for an already-stored recording.
    Returns the parse job id, or None if the recording does not exist. A parse
    already waiting or running is reused instead of queued twice."""
    if conn.execute("SELECT 1 FROM recordings WHERE recording_id = ?", (recording_id,)).fetchone() is None:
        return None
    with conn:
        if rescan:
            enqueue_job(conn, recording_id, "scan", "fast")
        existing = conn.execute(
            "SELECT job_id FROM jobs WHERE recording_id = ? AND kind = 'parse' AND status IN ('pending','running')",
            (recording_id,)).fetchone()
        if existing:
            return existing["job_id"]
        conn.execute("UPDATE recordings SET parse_status = 'pending', parse_error = NULL WHERE recording_id = ?",
                     (recording_id,))
        return enqueue_job(conn, recording_id, "parse", "deep")


def add_location(conn: sqlite3.Connection, name: str, lat: float, lon: float, radius_m: float) -> int:
    """Create or update a named location (by name), then relabel recordings."""
    name = (name or "").strip()
    if not name:
        raise CatalogError("name is required")
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise CatalogError("lat/lon out of range")
    if radius_m <= 0:
        raise CatalogError("radius_m must be positive")
    with conn:
        conn.execute(
            """INSERT INTO named_locations (name, center_lat, center_lon, radius_m) VALUES (?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET center_lat = excluded.center_lat,
                   center_lon = excluded.center_lon, radius_m = excluded.radius_m""",
            (name, lat, lon, radius_m))
    relabel_locations(conn)
    return conn.execute("SELECT location_id FROM named_locations WHERE name = ?", (name,)).fetchone()["location_id"]


def relabel_locations(conn: sqlite3.Connection) -> int:
    """Re-match every recording that has a GPS box and no manual label against the
    current named locations (cheap: no file is opened). Returns how many changed."""
    from evil.geo import haversine_m

    locations = conn.execute("SELECT * FROM named_locations").fetchall()
    changed = 0
    with conn:
        for rec in conn.execute(
            """SELECT recording_id, location_id, gps_min_lat, gps_max_lat, gps_min_lon, gps_max_lon FROM recordings
               WHERE gps_min_lat IS NOT NULL AND COALESCE(location_method, '') != 'manual'""").fetchall():
            lat = (rec["gps_min_lat"] + rec["gps_max_lat"]) / 2
            lon = (rec["gps_min_lon"] + rec["gps_max_lon"]) / 2
            best, best_d = None, float("inf")
            for loc in locations:
                d = haversine_m(lat, lon, loc["center_lat"], loc["center_lon"])
                if d <= loc["radius_m"] and d < best_d:
                    best, best_d = loc["location_id"], d
            if best != rec["location_id"]:
                conn.execute("UPDATE recordings SET location_id = ?, location_method = ? WHERE recording_id = ?",
                             (best, "gps-match" if best is not None else None, rec["recording_id"]))
                changed += 1
    return changed


def list_locations(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute("SELECT * FROM named_locations ORDER BY name")]


def _meminfo() -> dict[str, int]:
    info: dict[str, int] = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                key, _, rest = line.partition(":")
                info[key] = int(rest.split()[0]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return info


def system_status(conn: sqlite3.Connection, root: DataRoot, recent: int = 25) -> dict:
    """What the status panel shows: the job queue plus CPU/memory/disk."""
    counts = {r["status"]: r["n"] for r in conn.execute("SELECT status, COUNT(*) n FROM jobs GROUP BY status")}
    queue = [dict(r) for r in conn.execute(
        """SELECT j.job_id, j.recording_id, j.kind, j.lane, j.status, j.attempts, j.max_attempts, j.progress,
                  j.error, j.created_at, j.started_at, j.finished_at,
                  COALESCE(r.label, r.original_name, r.recording_id) AS name, r.total_bytes AS size_bytes
           FROM jobs j JOIN recordings r ON r.recording_id = j.recording_id
           ORDER BY CASE j.status WHEN 'running' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END,
                    COALESCE(j.finished_at, j.created_at) DESC, j.job_id DESC
           LIMIT ?""", (recent,))]
    oldest = conn.execute("SELECT MIN(created_at) m FROM jobs WHERE status = 'pending'").fetchone()["m"]
    mem = _meminfo()
    try:
        disk = shutil.disk_usage(root.path)
        disk_info = {"total_bytes": disk.total, "free_bytes": disk.free}
    except OSError:
        disk_info = {"total_bytes": None, "free_bytes": None}
    try:
        load = os.getloadavg()
    except OSError:
        load = (None, None, None)
    raw_bytes = conn.execute("SELECT COALESCE(SUM(total_bytes), 0) b FROM recordings").fetchone()["b"]
    return {
        "jobs": {"counts": {k: counts.get(k, 0) for k in ("pending", "running", "done", "failed")},
                 "oldest_pending_age_sec": (time.time() - oldest) if oldest else None, "queue": queue},
        "system": {"cpu_count": os.cpu_count(), "load_avg": list(load),
                   "mem_total_bytes": mem.get("MemTotal"), "mem_available_bytes": mem.get("MemAvailable"),
                   "disk": disk_info, "recordings_bytes": raw_bytes},
        "cache": cache_summary(conn),
        "time": time.time(),
    }


# ---- CAT cache (Phase 4) -------------------------------------------------

CACHE_CONTAINERS = {"rosbag2-sqlite3": "bag", "csv": "csv"}
CACHE_MAX_AGE_SEC = float(os.getenv("EVIL_CACHE_MAX_AGE_SEC", str(7 * 24 * 3600)))
CACHE_MAX_BYTES = int(os.getenv("EVIL_CACHE_MAX_BYTES", str(20 * 1024**3)))


def cache_kind(container: str) -> str | None:
    return CACHE_CONTAINERS.get(container)


def cache_state(conn: sqlite3.Connection, recording_id: str) -> dict | None:
    """absent | queued | building | ready | failed for one recording (None if it does not exist).
    Derived from cache_entries (ready) and the latest cache_build job."""
    rec = conn.execute("SELECT recording_id, container, parse_error FROM recordings WHERE recording_id = ?",
                       (recording_id,)).fetchone()
    if rec is None:
        return None
    entry = conn.execute("SELECT * FROM cache_entries WHERE recording_id = ?", (recording_id,)).fetchone()
    job = conn.execute("SELECT * FROM jobs WHERE recording_id = ? AND kind = 'cache_build' ORDER BY job_id DESC LIMIT 1",
                       (recording_id,)).fetchone()
    out = {"recording_id": recording_id, "state": "absent", "progress": None, "error": None,
           "built_at": None, "last_access_at": None, "size_bytes": None, "messages": None, "detail": None,
           "cacheable": cache_kind(rec["container"]) is not None
                        and not (rec["parse_error"] or "").startswith("container unreadable")}
    if entry is not None:
        out.update(state="ready", built_at=entry["built_at"], last_access_at=entry["last_access_at"],
                   size_bytes=entry["size_bytes"], messages=entry["messages"],
                   detail=json.loads(entry["detail_json"]) if entry["detail_json"] else None)
    elif job is not None and job["status"] == "pending" and job["attempts"] == 0:
        out["state"] = "queued"
    elif job is not None and job["status"] in ("pending", "running"):
        out.update(state="building", progress=job["progress"])
        if job["error"]:
            out["error"] = f"retrying after: {job['error']}"
    elif job is not None and job["status"] == "failed":
        out.update(state="failed", error=job["error"])
    return out


def request_cache(conn: sqlite3.Connection, recording_id: str) -> dict:
    """Ask for a recording's CAT cache to exist: queue a build unless it is ready or already
    queued/running. A ready entry is touched. Raises CatalogError if it cannot be cached."""
    st = cache_state(conn, recording_id)
    if st is None:
        raise KeyError(recording_id)
    if not st["cacheable"]:
        raise CatalogError("this recording cannot be opened in CAT (not a readable rosbag2 or CSV)")
    if st["state"] == "ready":
        touch_cache(conn, recording_id)
    elif st["state"] in ("absent", "failed"):
        with conn:
            enqueue_job(conn, recording_id, "cache_build", "cache")
    return cache_state(conn, recording_id)


def touch_cache(conn: sqlite3.Connection, recording_id: str, now: float | None = None) -> bool:
    with conn:
        cur = conn.execute("UPDATE cache_entries SET last_access_at = ? WHERE recording_id = ?",
                           (time.time() if now is None else now, recording_id))
    return cur.rowcount > 0


def record_cache_built(conn: sqlite3.Connection, recording_id: str, size_bytes: int, messages: int | None,
                       detail: dict | None, now: float | None = None) -> None:
    now = time.time() if now is None else now
    with conn:
        conn.execute(
            """INSERT INTO cache_entries (recording_id, built_at, last_access_at, size_bytes, messages, detail_json)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(recording_id) DO UPDATE SET built_at = excluded.built_at,
                   last_access_at = excluded.last_access_at, size_bytes = excluded.size_bytes,
                   messages = excluded.messages, detail_json = excluded.detail_json""",
            (recording_id, now, now, int(size_bytes or 0), messages, json.dumps(detail) if detail else None))


def plan_eviction(conn: sqlite3.Connection, now: float | None = None, max_age: float = CACHE_MAX_AGE_SEC,
                  max_bytes: int = CACHE_MAX_BYTES) -> list[str]:
    """Which cache entries to drop: everything idle longer than max_age, then least-recently-used
    entries while what remains exceeds max_bytes."""
    now = time.time() if now is None else now
    rows = conn.execute("SELECT recording_id, size_bytes, last_access_at FROM cache_entries "
                        "ORDER BY last_access_at ASC, recording_id").fetchall()
    evict = [r["recording_id"] for r in rows if now - r["last_access_at"] > max_age]
    keep = [r for r in rows if r["recording_id"] not in set(evict)]
    total = sum(r["size_bytes"] for r in keep)
    for r in keep:                      # oldest access first
        if total <= max_bytes:
            break
        evict.append(r["recording_id"])
        total -= r["size_bytes"]
    return evict


def drop_cache_entry(conn: sqlite3.Connection, recording_id: str) -> None:
    with conn:
        conn.execute("DELETE FROM cache_entries WHERE recording_id = ?", (recording_id,))


def cat_recordings(conn: sqlite3.Connection, kind: str = "all", limit: int = 500, offset: int = 0) -> list[dict]:
    """What CAT lists: every recording it could open (rosbag2 and CSV), newest first, with a
    display name and its cache state."""
    containers = [c for c, k in CACHE_CONTAINERS.items() if kind in ("all", k)]
    if not containers:
        return []
    marks = ",".join("?" * len(containers))
    rows = conn.execute(
        f"""SELECT recording_id FROM recordings WHERE container IN ({marks})
              AND COALESCE(parse_error, '') NOT LIKE 'container unreadable%'
            ORDER BY COALESCE(recorded_start, uploaded_at) DESC, recording_id DESC LIMIT ? OFFSET ?""",
        (*containers, max(1, min(limit, 2000)), max(0, offset))).fetchall()
    out = []
    for r in rows:
        rec = conn.execute("SELECT * FROM recordings WHERE recording_id = ?", (r["recording_id"],)).fetchone()
        st = cache_state(conn, r["recording_id"])
        out.append({
            "recording_id": rec["recording_id"],
            "name": rec["label"] or rec["original_name"] or rec["recording_id"],
            "kind": CACHE_CONTAINERS[rec["container"]], "container": rec["container"],
            "category": rec["category"], "car": rec["car"], "event": rec["event"],
            "recorded_start": rec["recorded_start"], "uploaded_at": rec["uploaded_at"],
            "total_bytes": rec["total_bytes"], "parse_status": rec["parse_status"],
            "cache_state": st["state"], "cache_progress": st["progress"], "cache_error": st["error"],
        })
    return out


def cache_summary(conn: sqlite3.Connection) -> dict:
    row = conn.execute("SELECT COUNT(*) n, COALESCE(SUM(size_bytes), 0) b FROM cache_entries").fetchone()
    return {"entries": row["n"], "bytes": row["b"], "max_bytes": CACHE_MAX_BYTES, "max_age_sec": CACHE_MAX_AGE_SEC}
