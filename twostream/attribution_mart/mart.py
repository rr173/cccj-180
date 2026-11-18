"""Device-attribution statistics mart over a twostream SQLite warehouse.

The mart is a second, independent SQLite file.  It reads the warehouse
strictly read-only (``mode=ro``) and never re-runs the interval join: the
job stores, batch intake and delivery protocol keep working unchanged.

Grain: ``(window_start, owner)``.  A MATCH is windowed by its left event
time, a LEFT PAD by its left time and a RIGHT PAD by its right time.  The
window start is ``O + floor((t-O)/W)*W`` (half-open, floors toward
-infinity, so negative times work).  The same anchor time selects the
device's owner from a piecewise ``[start, end)`` history.  "No covering
segment" and "null device key" form one dedicated unknown group.  It is
stored under the empty owner key; a real owner is never empty text, so the
unknown group cannot collide with any owner -- including the literal
``"unknown"``.

Correctness techniques (full argument in the README):

* **One warehouse snapshot per sync.**  A single ``BEGIN`` read
  transaction captures every configured job's upper ``offset`` and reads
  exactly ``(processed, upper]``.  Rows committed to the warehouse after
  that point are invisible to this sync and belong to the next one.
* **Atomic published versions.**  Positions, facts, request rows,
  latency frequencies, recomputed group rows and the incremented mart
  revision are written in one SQLite transaction (WAL mode).  Readers see
  the previous complete version or the next complete version, never a
  mixture of jobs or half-updated statistics.
* **Deduplicated requests.**  ``reqs`` is keyed by ``(job_id,
  left_partition, left_seq)``, so a request that matched several
  responses is counted once; equal partition/seq in different jobs stay
  separate.  Every match's latency still reaches the quantile input via
  ``lat_freq`` (multiplicity per latency value).
* **Exact order statistics in constant memory.**  Latencies are stored
  as order-preserving decimal text in ``lat_freq``.  min/max/p50/p95 are
  obtained by one ascending cursor per recomputed group: the nearest
  ranks are ``ceil(p*n)`` (p50 = ``(n+1)//2``, p95 = ``(19n+19)//20``),
  so a single streaming pass with a running count is exact -- no
  interpolation, no latency list in Python, no float.
* **Corrections touch only changed ranges.**  A replacement history is
  validated as a whole, then :func:`history.changed_intervals` splits the
  integer line where the effective owner actually changes.  Only facts
  whose anchor lies in those ranges are read (keyset-paged through the
  ``(device, anchor)`` index); only the groups they leave or join are
  recomputed.  Untouched devices, ranges and groups keep their rows.
* **Traceable facts.**  Every stored fact keeps ``(job_id, offset)``,
  both event identities and times, anchor, owner and the covering
  attribution segment (or the unknown reason).  Expansion pages re-read
  the warehouse read-only and show the original row, so the numbers can
  be reproduced from the detail alone.

Arbitrary precision: times, bounds, origin, width, latency and sums are
Python integers end to end.  SQLite stores them as decimal text (with
``enc_int`` order keys); no 64-bit integer arithmetic and no float ever
touches them.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time as _time
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from interval_join.encoding import enc_int

from .errors import (AttributionInputError, MartBusyError, MartError,
                     MartFormatError, MartIdentityError, MartInputError,
                     MartLockRequiredError, StaleRevisionError)
from .history import Segment, TimeMap, changed_intervals, parse_segments
from .source import SourceWarehouse, parse_dec
from .stats import window_start

MART_FORMAT_VERSION = 1
UNKNOWN_KEY = ""          # owner storage key of the unknown group
SNAPSHOT_CHUNK = 2000     # fact rows per keyset page while correcting


class StaleVersionError(MartError):
    """The requested version is not the currently committed one; numbers
    from a newer version may never explain an older view."""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def owner_key(owner: Optional[str]) -> str:
    """Storage key for an owner; None (unknown group) -> empty string."""
    return UNKNOWN_KEY if owner is None else owner


def owner_value(key: str) -> Optional[str]:
    """Inverse of :func:`owner_key`; None means the unknown group."""
    return None if key == "" else key


# --------------------------------------------------------------- schema
_SCHEMA = """
CREATE TABLE IF NOT EXISTS mart_meta(
  id INTEGER PRIMARY KEY CHECK(id=1),
  format_version INTEGER NOT NULL,
  warehouse_id TEXT NOT NULL,
  warehouse_path TEXT NOT NULL,
  job_ids_json TEXT NOT NULL,
  window_width TEXT NOT NULL,
  window_origin TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mart_state(
  id INTEGER PRIMARY KEY CHECK(id=1),
  revision INTEGER NOT NULL,
  dim_revision INTEGER NOT NULL,
  view_token TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_progress(
  job_id TEXT PRIMARY KEY,
  processed_offset INTEGER NOT NULL
);
-- Attribution dimension: one row per (sorted) segment of a device.
CREATE TABLE IF NOT EXISTS attr_segments(
  device TEXT NOT NULL,
  idx INTEGER NOT NULL,
  start_txt TEXT,             -- NULL = -infinity
  end_txt TEXT,               -- NULL = +infinity
  owner TEXT NOT NULL,
  PRIMARY KEY(device, idx)
);
-- One row per warehouse result (fact).  (job_id, offset) is the traceable
-- identity in the source warehouse.
CREATE TABLE IF NOT EXISTS facts(
  id INTEGER PRIMARY KEY,
  job_id TEXT NOT NULL,
  offset INTEGER NOT NULL,
  kind TEXT NOT NULL,          -- match | lpad | rpad
  lp TEXT, ls TEXT, rp TEXT, rs TEXT,
  k TEXT,
  ltime TEXT, rtime TEXT,
  anchor_txt TEXT NOT NULL,
  anchor_enc TEXT NOT NULL,
  ws_txt TEXT NOT NULL,
  ws_enc TEXT NOT NULL,
  owner_k TEXT NOT NULL,
  status TEXT NOT NULL,        -- owner | unknown_gap | unknown_null_key
  UNIQUE(job_id, offset)
);
CREATE INDEX IF NOT EXISTS idx_facts_group ON facts(ws_enc, owner_k);
CREATE INDEX IF NOT EXISTS idx_facts_device ON facts(k, anchor_enc);
-- One row per matched request: (job, left partition, left seq).
CREATE TABLE IF NOT EXISTS reqs(
  job_id TEXT NOT NULL,
  lp TEXT NOT NULL,
  ls TEXT NOT NULL,
  ws_enc TEXT NOT NULL,
  ws_txt TEXT NOT NULL,
  owner_k TEXT NOT NULL,
  k TEXT,
  anchor_txt TEXT NOT NULL,
  ltime TEXT NOT NULL,
  rtime TEXT NOT NULL,
  PRIMARY KEY(job_id, lp, ls)
);
CREATE INDEX IF NOT EXISTS idx_reqs_group ON reqs(ws_enc, owner_k);
-- Match latency multiplicity per group: every match pair counts here.
CREATE TABLE IF NOT EXISTS lat_freq(
  ws_enc TEXT NOT NULL,
  owner_k TEXT NOT NULL,
  lat_enc TEXT NOT NULL,
  lat_txt TEXT NOT NULL,
  cnt INTEGER NOT NULL,
  PRIMARY KEY(ws_enc, owner_k, lat_enc)
);
-- Groups whose statistics must be recomputed in the current operation.
CREATE TABLE IF NOT EXISTS dirty_groups(
  ws_enc TEXT NOT NULL,
  ws_txt TEXT NOT NULL,
  owner_k TEXT NOT NULL,
  PRIMARY KEY(ws_enc, owner_k)
);
CREATE TABLE IF NOT EXISTS group_stats(
  ws_enc TEXT NOT NULL,
  owner_k TEXT NOT NULL,
  ws_txt TEXT NOT NULL,
  n_match INTEGER NOT NULL,
  n_req INTEGER NOT NULL,
  n_lpad INTEGER NOT NULL,
  n_rpad INTEGER NOT NULL,
  lat_sum TEXT NOT NULL,
  lat_min TEXT, lat_max TEXT, p50 TEXT, p95 TEXT,
  PRIMARY KEY(ws_enc, owner_k)
);
"""

_REQUIRED_TABLES = {
    "mart_meta", "mart_state", "job_progress", "attr_segments", "facts",
    "reqs", "lat_freq", "dirty_groups", "group_stats",
}


def _validate_mart(conn, path: str) -> None:
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "mart_meta" not in tables:
            raise MartFormatError(
                f"{path!r} is not an attribution mart "
                f"(found tables: {sorted(tables)})")
        row = conn.execute(
            "SELECT format_version FROM mart_meta WHERE id=1").fetchone()
        if row is None:
            raise MartFormatError(f"{path!r}: mart metadata row is missing")
        if row[0] != MART_FORMAT_VERSION:
            raise MartFormatError(
                f"{path!r}: unsupported mart format version {row[0]!r} "
                f"(this build understands {MART_FORMAT_VERSION}); refusing "
                f"to interpret an unknown state")
        missing = _REQUIRED_TABLES - tables
        if missing:
            raise MartFormatError(
                f"{path!r}: mart is incomplete, missing tables: "
                f"{sorted(missing)}")
        if conn.execute("SELECT COUNT(*) FROM mart_state WHERE id=1"
                        ).fetchone()[0] != 1:
            raise MartFormatError(f"{path!r}: mart state row is missing")
    except sqlite3.DatabaseError as e:
        raise MartFormatError(
            f"{path!r} is corrupt or not a SQLite database: {e}") from e


class HistoryIndex:
    """Lazy, bounded lookup of device histories.

    The attribution dimension can hold hundreds of thousands of devices;
    loading all of it into Python for a sync would be neither necessary nor
    desirable.  This starts nothing: each first lookup of a device reads
    its segments with one indexed query and caches the resulting
    :class:`TimeMap` in a small LRU.  Memory therefore depends on the
    working set (and the capacity), never on the total number of devices
    or facts.
    """

    def __init__(self, conn: sqlite3.Connection, capacity: int = 4096):
        self._conn = conn
        self._capacity = capacity
        self._cache: "OrderedDict[str, Optional[TimeMap]]" = OrderedDict()
        self.queries = 0

    def get(self, device: Optional[str]) -> Optional[TimeMap]:
        if device is None:
            return None
        if device in self._cache:
            self._cache.move_to_end(device)
            return self._cache[device]
        segs: List[Segment] = []
        for s0, e0, owner in self._conn.execute(
                "SELECT start_txt, end_txt, owner FROM attr_segments"
                " WHERE device=? ORDER BY idx", (device,)):
            segs.append(Segment(None if s0 is None else int(s0),
                                None if e0 is None else int(e0), owner))
        self.queries += 1
        tm: Optional[TimeMap] = TimeMap(segs) if segs else None
        self._cache[device] = tm
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)
        return tm

    def put(self, device: str, tm: TimeMap) -> None:
        self._cache[device] = tm
        self._cache.move_to_end(device)
        if len(self._cache) > self._capacity:
            self._cache.popitem(last=False)


class AttributionMart:
    """A durable, versioned statistics mart over one read-only warehouse.

    Create with :meth:`create`; later opens use :meth:`open`.  Creation
    binds the warehouse's own persistent ``warehouse_id``, the exact set
    of job ids to accumulate and the fixed positive window ``width`` /
    ``origin``.  The same warehouse may move to another path (identity,
    not path, is bound); a different warehouse is rejected with
    :class:`MartIdentityError`.

    Mutating work (sync and attribution correction) requires the
    single-writer lock: ``with mart.modify(): ...``.  A second modifier
    gets :class:`MartBusyError`.  The lock is an flock on a sidecar file,
    so a SIGKILLed/killed writer releases it automatically and no manual
    lock cleanup is needed.  Readers use :meth:`query` / :meth:`expand`
    without the lock and always see the last complete committed version.
    """

    # ----------------------------------------------------------- create
    @classmethod
    def create(cls, path: str, *, warehouse: str, job_ids: Iterable[str],
               width: int, origin: int) -> "AttributionMart":
        if isinstance(width, bool) or not isinstance(width, int) or width <= 0:
            raise MartInputError(
                f"window width W must be a fixed positive integer, got {width!r}")
        if isinstance(origin, bool) or not isinstance(origin, int):
            raise MartInputError(
                f"window origin O must be an integer, got {origin!r}")
        job_ids = list(job_ids)
        if not job_ids:
            raise MartInputError("at least one job_id must be configured")
        for j in job_ids:
            if not isinstance(j, str) or not j:
                raise MartInputError(
                    f"job ids must be non-empty text, got {j!r}")
        if len(set(job_ids)) != len(job_ids):
            raise MartInputError("duplicate job_id in the configured set")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            raise MartFormatError(
                f"{path!r} already exists and is not empty; refusing to "
                f"replace it")
        src = SourceWarehouse(warehouse)
        try:
            wh_id = src.warehouse_id
            uri = "file:" + os.path.abspath(path) + "?mode=rwc"
            conn = sqlite3.connect(uri, uri=True, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript(_SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO mart_meta(id, format_version, warehouse_id,"
                    " warehouse_path, job_ids_json, window_width,"
                    " window_origin, created_at) VALUES (1,?,?,?,?,?,?,?)",
                    (MART_FORMAT_VERSION, wh_id, os.path.abspath(warehouse),
                     json.dumps(job_ids), str(width), str(origin), _utcnow()))
                conn.execute(
                    "INSERT INTO mart_state(id, revision, dim_revision,"
                    " view_token, updated_at) VALUES (1, 0, 0, ?, ?)",
                    (uuid.uuid4().hex, _utcnow()))
                conn.executemany(
                    "INSERT INTO job_progress(job_id, processed_offset)"
                    " VALUES (?, 0)", [(j,) for j in job_ids])
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                conn.close()
                raise
        finally:
            src.close()
        return cls(path, conn, os.path.abspath(warehouse))

    # ------------------------------------------------------------- open
    @classmethod
    def open(cls, path: str, *, warehouse: Optional[str] = None
             ) -> "AttributionMart":
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise MartFormatError(
                f"no mart at {path!r} (missing or empty file); create one "
                f"with AttributionMart.create()")
        try:
            conn = sqlite3.connect(path, isolation_level=None)
            conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.Error as e:
            raise MartFormatError(f"cannot open mart {path!r}: {e}") from e
        try:
            _validate_mart(conn, path)
        except Exception:
            conn.close()
            raise
        wh = conn.execute(
            "SELECT warehouse_path FROM mart_meta WHERE id=1").fetchone()[0]
        mart = cls(path, conn, wh)
        if warehouse is not None:
            with mart.modify():
                mart.rebind_warehouse(warehouse)
        return mart

    def __init__(self, path: str, conn: sqlite3.Connection,
                 warehouse_path: str):
        self.path = path
        self._conn = conn
        self._warehouse_path = warehouse_path
        self._lock_fd: Optional[int] = None
        row = conn.execute(
            "SELECT job_ids_json, window_width, window_origin FROM mart_meta"
            " WHERE id=1").fetchone()
        self._job_ids = list(json.loads(row[0]))
        self._width = parse_dec(row[1])
        self._origin = parse_dec(row[2])

    def close(self) -> None:
        self.release()
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -------------------------------------------------------- warehouse
    def rebind_warehouse(self, path: str) -> Dict[str, Any]:
        """Point the mart at the same warehouse at a new filesystem path.

        The warehouse identity (its ``warehouse_id``) must match the bound
        one; another warehouse is rejected with :class:`MartIdentityError`
        and nothing is written.  Rebinding does not touch the statistics."""
        src = SourceWarehouse(path)
        try:
            if src.warehouse_id != self.warehouse_id:
                raise MartIdentityError(
                    f"warehouse at {path!r} is {src.warehouse_id}, but this "
                    f"mart is bound to {self.warehouse_id}; refusing to "
                    f"rebind to a different warehouse")
        finally:
            src.close()
        self.require_lock()
        self._conn.execute(
            "UPDATE mart_meta SET warehouse_path=? WHERE id=1",
            (os.path.abspath(path),))
        self._warehouse_path = os.path.abspath(path)
        return {"warehouse_id": self.warehouse_id,
                "warehouse_path": self._warehouse_path}

    # ------------------------------------------------------------- lock
    def acquire(self) -> "AttributionMart":
        if self._lock_fd is not None:
            return self
        fd = os.open(self.path + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise MartBusyError(
                f"mart {self.path!r} is locked by another process; only one "
                f"modifier (sync or correction) is allowed") from None
        self._lock_fd = fd
        return self

    def release(self) -> None:
        fd, self._lock_fd = self._lock_fd, None
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    @contextmanager
    def modify(self):
        if self._lock_fd is None:
            self.acquire()
            try:
                yield self
            finally:
                self.release()
        else:
            yield self

    def require_lock(self) -> None:
        if self._lock_fd is None:
            raise MartLockRequiredError(
                "this operation modifies the mart; use mart.modify()")

    def _locked_probe(self) -> bool:
        if self._lock_fd is not None:
            return True
        fd = os.open(self.path + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        finally:
            os.close(fd)

    # -------------------------------------------------------- metadata
    @property
    def warehouse_id(self) -> str:
        return self._conn.execute(
            "SELECT warehouse_id FROM mart_meta WHERE id=1").fetchone()[0]

    @property
    def warehouse_path(self) -> str:
        return self._warehouse_path

    @property
    def job_ids(self) -> List[str]:
        return list(self._job_ids)

    @property
    def width(self) -> int:
        return self._width

    @property
    def origin(self) -> int:
        return self._origin

    def _window(self, anchor: int) -> int:
        return window_start(anchor, self._width, self._origin)

    def _positions(self) -> Dict[str, int]:
        return {r[0]: int(r[1]) for r in self._conn.execute(
            "SELECT job_id, processed_offset FROM job_progress")}

    def _state(self) -> Dict[str, Any]:
        row = self._conn.execute(
            "SELECT revision, dim_revision, view_token FROM mart_state"
            " WHERE id=1").fetchone()
        return {"mart_revision": int(row[0]), "dim_revision": int(row[1]),
                "view_token": row[2], "positions": self._positions()}

    @staticmethod
    def _segments_on(conn, devices) -> Dict[str, TimeMap]:
        maps: Dict[str, TimeMap] = {}
        if not devices:
            return maps
        marks = ",".join("?" * len(devices))
        for dev, s0, e0, owner in conn.execute(
                f"SELECT device, start_txt, end_txt, owner FROM attr_segments"
                f" WHERE device IN ({marks}) ORDER BY device, idx",
                tuple(sorted(devices))):
            maps.setdefault(dev, []).append(Segment(
                None if s0 is None else int(s0),
                None if e0 is None else int(e0), owner))
        return {d: TimeMap(segs) for d, segs in maps.items()}

    @staticmethod
    def _segment_json(tm: Optional[TimeMap], device, anchor) -> Optional[dict]:
        if tm is None or device is None:
            return None
        run = tm.run_at(anchor)
        if run is None:
            return None
        return {"start": run.start, "end": run.end, "owner": run.owner}

    def _attribute(self, tmap, k: Optional[str],
                   anchor: int) -> Tuple[str, str, Optional[Segment]]:
        if k is None:
            return UNKNOWN_KEY, "unknown_null_key", None
        tm = tmap.get(k)
        if tm is None:
            return UNKNOWN_KEY, "unknown_gap", None
        seg = tm.run_at(anchor)
        if seg is None:
            return UNKNOWN_KEY, "unknown_gap", None
        return seg.owner, "owner", seg

    # ------------------------------------------------------------ query
    @contextmanager
    def _read_snapshot(self):
        """A read-only connection with one transaction: every SELECT in
        the block sees the same committed mart version."""
        uri = "file:" + os.path.abspath(self.path) + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("BEGIN")
            try:
                yield conn
            finally:
                conn.execute("COMMIT")
        finally:
            conn.close()

    def query(self, view_token: Optional[str] = None,
              after: Optional[Tuple[int, Optional[str]]] = None,
              limit: Optional[int] = None) -> Dict[str, Any]:
        """Statistics of the current committed mart version.

        ``view_token`` pins the caller's expectation: a mismatch raises
        :class:`StaleVersionError` rather than silently answering a newer
        version.  ``after=(window_start, owner)`` resumes a paginated
        scan in ``(window_start, owner)`` order."""
        if limit is not None and (isinstance(limit, bool)
                                  or not isinstance(limit, int) or limit < 1):
            raise MartInputError(f"limit must be a positive integer, got {limit!r}")
        with self._read_snapshot() as c:
            state = self._state_on(c)
            if view_token is not None and view_token != state["view_token"]:
                raise StaleVersionError(
                    f"view {view_token!r} is no longer the committed version "
                    f"(now {state['view_token']!r}); re-query and retry")
            sql = ("SELECT ws_enc, owner_k, ws_txt, n_match, n_req, n_lpad,"
                   " n_rpad, lat_sum, lat_min, lat_max, p50, p95"
                   " FROM group_stats")
            args: List[Any] = []
            if after is not None:
                ws, owner = after
                if isinstance(ws, bool) or not isinstance(ws, int):
                    raise MartInputError(
                        f"cursor window_start must be an integer, got {ws!r}")
                enc = enc_int(ws)
                ok = owner_key(owner)
                sql += " WHERE ws_enc>? OR (ws_enc=? AND owner_k>?)"
                args = [enc, enc, ok]
            sql += " ORDER BY ws_enc, owner_k"
            if limit is not None:
                sql += " LIMIT ?"
                args.append(limit + 1)
            rows = c.execute(sql, args).fetchall()
            more = limit is not None and len(rows) > limit
            rows = rows[:limit] if limit is not None else rows
            out = [self._group_out(r) for r in rows]
            next_cursor = None if not rows else [parse_dec(rows[-1][2]),
                                                 owner_value(rows[-1][1])]
        return {"view_token": state["view_token"],
                "mart_revision": state["mart_revision"],
                "dim_revision": state["dim_revision"],
                "positions": state["positions"],
                "warehouse_id": self.warehouse_id,
                "window": {"width": self._width, "origin": self._origin},
                "jobs": self.job_ids,
                "groups": out,
                "has_more": more,
                "next_cursor": next_cursor}

    @staticmethod
    def _group_out(r) -> Dict[str, Any]:
        return {
            "window_start": parse_dec(r[2]),
            "owner": owner_value(r[1]),
            "match_pairs": int(r[3]),
            "matched_requests": int(r[4]),
            "unmatched_requests": int(r[5]),
            "unmatched_responses": int(r[6]),
            "latency_sum": parse_dec(r[7]),
            "latency_min": None if r[8] is None else parse_dec(r[8]),
            "latency_max": None if r[9] is None else parse_dec(r[9]),
            "p50": None if r[10] is None else parse_dec(r[10]),
            "p95": None if r[11] is None else parse_dec(r[11]),
        }

    @staticmethod
    def _state_on(conn) -> Dict[str, Any]:
        row = conn.execute(
            "SELECT revision, dim_revision, view_token FROM mart_state"
            " WHERE id=1").fetchone()
        pos = {r[0]: int(r[1]) for r in conn.execute(
            "SELECT job_id, processed_offset FROM job_progress")}
        return {"mart_revision": int(row[0]), "dim_revision": int(row[1]),
                "view_token": row[2], "positions": pos}

    # ---------------------------------------------------------- expand
    def expand(self, view_token: str, window_start_value: int,
               owner: Optional[str], *, kind: Optional[str] = None,
               limit: int = 100, after: Optional[int] = None
               ) -> Dict[str, Any]:
        """Page the stored facts behind one group of one specific version.

        A stale ``view_token`` is refused outright, so a new version's
        detail can never be used to explain an old number.  ``after`` is
        the last fact id of the previous page (stable within a version).
        Each entry carries the warehouse ``(job_id, offset)``, both event
        identities and times, the anchor time, the matched attribution
        segment (or the unknown reason) and the match latency; the
        original warehouse row is re-read read-only for independent
        checking.
        """
        if isinstance(limit, bool) or not isinstance(limit, int) \
                or not 1 <= limit <= 1000:
            raise MartInputError(f"limit must be an integer in 1..1000, got {limit!r}")
        if kind is not None and kind not in ("match", "lpad", "rpad"):
            raise MartInputError(f"kind must be match/lpad/rpad, got {kind!r}")
        ws = window_start_value
        if isinstance(ws, str):
            ws = parse_dec(ws)
        if isinstance(ws, bool) or not isinstance(ws, int):
            raise MartInputError(
                f"window_start must be an integer, got {window_start_value!r}")
        ws_enc, owner_k = enc_int(ws), owner_key(owner)
        with self._read_snapshot() as c:
            state = self._state_on(c)
            if view_token != state["view_token"]:
                raise StaleVersionError(
                    f"view {view_token!r} is no longer committed "
                    f"(now {state['view_token']!r}); refusing to explain old "
                    f"numbers with newer detail")
            if c.execute(
                    "SELECT 1 FROM group_stats WHERE ws_enc=? AND owner_k=?",
                    (ws_enc, owner_k)).fetchone() is None:
                raise MartInputError(
                    f"group (window {ws}, owner {owner!r}) does not exist in "
                    f"this version (empty groups are not kept); nothing to "
                    f"expand")
            where = "ws_enc=? AND owner_k=?"
            args: List[Any] = [ws_enc, owner_k]
            if kind is not None:
                where += " AND kind=?"
                args.append(kind)
            if after is not None:
                if isinstance(after, bool) or not isinstance(after, int):
                    raise MartInputError(f"after must be a fact id, got {after!r}")
                where += " AND id>?"
                args.append(after)
            rows = c.execute(
                "SELECT id, job_id, offset, kind, lp, ls, rp, rs, k, ltime,"
                " rtime, anchor_txt, owner_k, status"
                f" FROM facts WHERE {where} ORDER BY id LIMIT ?",
                args + [limit + 1]).fetchall()
            # The covering interval is derived from this snapshot's
            # histories for exactly the devices on this page, so expansion
            # reports the interval that explains the number and corrections
            # never need to rewrite unrelated facts.
            devices = {r[8] for r in rows if r[8] is not None}
            seg_maps = self._segments_on(c, devices)
            more = len(rows) > limit
            rows = rows[:limit]
            total = int(c.execute(
                f"SELECT COUNT(*) FROM facts WHERE ws_enc=? AND owner_k=?",
                (ws_enc, owner_k)).fetchone()[0])
        identities, keys = [], []
        for r in rows:
            identities.append({
                "fact_id": r[0], "job_id": r[1], "offset": r[2],
                "kind": r[3],
                "left": (None if r[4] is None else
                         {"partition": r[4], "seq": parse_dec(r[5])}),
                "right": (None if r[6] is None else
                          {"partition": r[6], "seq": parse_dec(r[7])}),
                "device_key": r[8],
                "left_time": None if r[9] is None else parse_dec(r[9]),
                "right_time": None if r[10] is None else parse_dec(r[10]),
                "anchor_time": parse_dec(r[11]),
                "window_start": ws,
                "owner": owner_value(r[12]),
                "attribution": {
                    "status": r[13],
                    "segment": self._segment_json(
                        seg_maps.get(r[8]), r[8], parse_dec(r[11])),
                    "unknown_reason": (
                        None if r[13] == "owner" else
                        ("null device key" if r[13] == "unknown_null_key"
                         else "no covering attribution segment")),
                },
                "latency": (None if r[3] != "match" else
                            parse_dec(r[10]) - parse_dec(r[9])),
            })
            keys.append((r[1], r[2]))
        result = {"view_token": view_token,
                  "mart_revision": state["mart_revision"],
                  "dim_revision": state["dim_revision"],
                  "window_start": ws, "owner": owner,
                  "group_fact_count": total,
                  "facts": identities, "has_more": more,
                  "next_after": None if not rows else rows[-1][0]}
        if keys:
            result["warehouse_rows"] = self._warehouse_rows(keys)
        return result

    def _warehouse_rows(self, keys) -> List[Optional[dict]]:
        src = SourceWarehouse(self._warehouse_path)
        try:
            if src.warehouse_id != self.warehouse_id:
                raise MartIdentityError(
                    f"warehouse at {self._warehouse_path!r} is "
                    f"{src.warehouse_id}, but this mart is bound to "
                    f"{self.warehouse_id}")
            return [src.fetch_result(j, o) for j, o in keys]
        finally:
            src.close()

    # ------------------------------------------------------------ sync
    def sync(self) -> Dict[str, Any]:
        """Capture and process the new warehouse range of every configured
        job in one snapshot, publishing one complete new mart version.

        Returns per job the captured upper offset, the processed position
        and the number of newly stored facts and requests.  Having no new
        results says nothing about whether the source job ended or was
        fully delivered: the mart only reports what the warehouse holds.
        """
        self.require_lock()
        src = SourceWarehouse(self._warehouse_path)
        try:
            if src.warehouse_id != self.warehouse_id:
                raise MartIdentityError(
                    f"warehouse at {self._warehouse_path!r} is "
                    f"{src.warehouse_id}, but this mart is bound to "
                    f"{self.warehouse_id}; refusing to read the wrong "
                    f"warehouse")
            positions = self._positions()
            summary = {}
            src.begin_snapshot()
            try:
                bounds = {j: src.max_offset(j) for j in self._job_ids}
                for j in self._job_ids:
                    if bounds[j] < positions[j]:
                        raise MartInputError(
                            f"job {j!r}: warehouse offset went backwards "
                            f"({positions[j]} -> {bounds[j]}); the source is "
                            f"not append-only")
                    src.verify_contiguous(j, positions[j], bounds[j])
                _test_hook("sync.snapshot_taken")
                tmap = HistoryIndex(self._conn)
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    scanned = 0
                    for j in self._job_ids:
                        summary[j] = {
                            "captured_upper": bounds[j],
                            "processed_before": positions[j],
                            "processed_after": bounds[j],
                            "facts_added": 0, "requests_added": 0}
                        for row in src.iter_results(j, positions[j],
                                                    bounds[j]):
                            scanned += 1
                            added = self._add_fact(j, row, tmap)
                            if added is None:
                                continue        # already stored before
                            summary[j]["facts_added"] += 1
                            if added:
                                summary[j]["requests_added"] += 1
                    recomputed = self._recompute_dirty()
                    for j, b in bounds.items():
                        self._conn.execute(
                            "UPDATE job_progress SET processed_offset=?"
                            " WHERE job_id=?", (b, j))
                    _test_hook("sync.before_commit")
                    self._commit(op="sync")
                    self._conn.execute("COMMIT")
                    _test_hook("sync.after_commit")
                except BaseException:
                    self._conn.execute("ROLLBACK")
                    raise
            finally:
                src.end_snapshot()
            return {"warehouse_id": self.warehouse_id,
                    "jobs": summary,
                    "facts_scanned": scanned,
                    "groups_recomputed": recomputed,
                    **self._state()}
        finally:
            src.close()

    def _add_fact(self, job_id: str, row, tmap) -> Optional[bool]:
        """Store one warehouse row.  Returns None when the (job_id, offset)
        fact is already stored (so a repeated sync cannot count it twice),
        False for a new non-match fact and True when the row also created a
        new request identity."""

        offset, kind, lp, ls, rp, rs, k, ltime, rtime = row
        if kind == "match":
            if lp is None or ls is None or rp is None or rs is None \
                    or ltime is None or rtime is None:
                raise MartInputError(
                    f"job {job_id!r} offset {offset}: uninterpretable match "
                    f"row (missing identity or time)")
            anchor = parse_dec(ltime)
            lat = parse_dec(rtime) - anchor
        elif kind == "lpad":
            if lp is None or ls is None or ltime is None:
                raise MartInputError(
                    f"job {job_id!r} offset {offset}: uninterpretable lpad row")
            anchor, lat = parse_dec(ltime), None
        elif kind == "rpad":
            if rp is None or rs is None or rtime is None:
                raise MartInputError(
                    f"job {job_id!r} offset {offset}: uninterpretable rpad row")
            anchor, lat = parse_dec(rtime), None
        else:
            raise MartInputError(
                f"job {job_id!r} offset {offset}: unknown result kind "
                f"{kind!r}; refusing to interpret the warehouse")
        ws = self._window(anchor)
        owner_k, status, seg = self._attribute(tmap, k, anchor)
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO facts(job_id, offset, kind, lp, ls, rp,"
            " rs, k, ltime, rtime, anchor_txt, anchor_enc, ws_txt, ws_enc,"
            " owner_k, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (job_id, offset, kind, lp, ls, rp, rs, k, ltime, rtime,
             str(anchor), enc_int(anchor), str(ws), enc_int(ws), owner_k,
             status))
        if cur.rowcount != 1:
            # (job_id, results, offset) is already stored: a repeated sync
            # must not count the same fact twice.
            return None
        self._dirty(ws, owner_k)
        if kind != "match":
            return False
        req = self._conn.execute(
            "INSERT OR IGNORE INTO reqs(job_id, lp, ls, ws_enc, ws_txt,"
            " owner_k, k, anchor_txt, ltime, rtime)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (job_id, lp, ls, enc_int(ws), str(ws), owner_k, k, str(anchor),
             ltime, rtime))
        self._conn.execute(
            "INSERT INTO lat_freq(ws_enc, owner_k, lat_enc, lat_txt, cnt)"
            " VALUES (?,?,?,?,1) ON CONFLICT(ws_enc, owner_k, lat_enc)"
            " DO UPDATE SET cnt=cnt+1",
            (enc_int(ws), owner_k, enc_int(lat), str(lat)))
        return req.rowcount == 1

    def _dirty(self, ws: int, owner_k: str) -> None:
        self._dirty_enc(enc_int(ws), str(ws), owner_k)

    def _dirty_enc(self, ws_enc: str, ws_txt: str, owner_k: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO dirty_groups(ws_enc, ws_txt, owner_k)"
            " VALUES (?,?,?)", (ws_enc, ws_txt, owner_k))

    def _recompute_dirty(self) -> int:
        rows = self._conn.execute(
            "SELECT ws_enc, ws_txt, owner_k FROM dirty_groups").fetchall()
        keys = [(r[0], r[1], r[2]) for r in rows]
        self._conn.execute("DELETE FROM dirty_groups")
        for ws_enc, ws_txt, owner_k in keys:
            self._recompute_group(ws_enc, ws_txt, owner_k)
        return len(keys)

    def _recompute_group(self, ws_enc: str, ws_txt: str, owner_k: str) -> None:
        c = self._conn
        n_match, n_lpad, n_rpad = c.execute(
            "SELECT"
            " COALESCE(SUM(CASE WHEN kind='match' THEN 1 ELSE 0 END),0),"
            " COALESCE(SUM(CASE WHEN kind='lpad' THEN 1 ELSE 0 END),0),"
            " COALESCE(SUM(CASE WHEN kind='rpad' THEN 1 ELSE 0 END),0)"
            " FROM facts WHERE ws_enc=? AND owner_k=?",
            (ws_enc, owner_k)).fetchone()
        n_match, n_lpad, n_rpad = int(n_match), int(n_lpad), int(n_rpad)
        n_req = int(c.execute(
            "SELECT COUNT(*) FROM reqs WHERE ws_enc=? AND owner_k=?",
            (ws_enc, owner_k)).fetchone()[0])
        if not (n_match or n_lpad or n_rpad or n_req):
            c.execute("DELETE FROM group_stats WHERE ws_enc=? AND owner_k=?",
                      (ws_enc, owner_k))
            c.execute("DELETE FROM lat_freq WHERE ws_enc=? AND owner_k=?"
                      " AND cnt<=0", (ws_enc, owner_k))
            return
        # One ascending pass over the group's latency frequencies:
        # sum/min/max and the total count for the exact nearest ranks
        # ceil(p*n) (p50 = (n+1)//2, p95 = (19n+19)//20).
        lat_sum = 0
        lat_min = lat_max = None
        n = 0
        for lat_txt, cnt in c.execute(
                "SELECT lat_txt, cnt FROM lat_freq WHERE ws_enc=? AND"
                " owner_k=? ORDER BY lat_enc", (ws_enc, owner_k)):
            if cnt <= 0:
                continue
            val = int(lat_txt, 10)
            if lat_min is None:
                lat_min = val
            lat_max = val
            n += cnt
            lat_sum += val * cnt
        p50 = p95 = None
        if n:
            p50 = self._rank_lat(ws_enc, owner_k, (n + 1) // 2)
            p95 = self._rank_lat(ws_enc, owner_k, (19 * n + 19) // 20)
        c.execute(
            "INSERT OR REPLACE INTO group_stats(ws_enc, owner_k, ws_txt,"
            " n_match, n_req, n_lpad, n_rpad, lat_sum, lat_min, lat_max,"
            " p50, p95) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (ws_enc, owner_k, ws_txt, n_match, n_req, n_lpad, n_rpad,
             str(lat_sum),
             None if lat_min is None else str(lat_min),
             None if lat_max is None else str(lat_max),
             None if p50 is None else str(p50),
             None if p95 is None else str(p95)))

    def _rank_lat(self, ws_enc: str, owner_k: str, rank: int):
        """The `rank`-th smallest latency (1-based) of one group, by a
        single streaming pass over its ordered latency frequencies."""
        cum = 0
        for lat_txt, cnt in self._conn.execute(
                "SELECT lat_txt, cnt FROM lat_freq WHERE ws_enc=? AND"
                " owner_k=? ORDER BY lat_enc", (ws_enc, owner_k)):
            if cnt <= 0:
                continue
            cum += cnt
            if cum >= rank:
                return int(lat_txt, 10)
        return None

    def _commit(self, op: str, dim_bump: bool = False) -> None:
        row = self._conn.execute(
            "SELECT revision, dim_revision FROM mart_state WHERE id=1"
        ).fetchone()
        rev = int(row[0]) + 1
        dim = int(row[1]) + (1 if dim_bump else 0)
        self._conn.execute(
            "UPDATE mart_state SET revision=?, dim_revision=?, view_token=?,"
            " updated_at=?", (rev, dim, uuid.uuid4().hex, _utcnow()))

    # ----------------------------------------------------- correction
    def replace_attribution(self, expected_revision: int,
                            devices: Dict[str, Optional[Sequence]]
                            ) -> Dict[str, Any]:
        """Atomically replace complete ownership histories of devices.

        ``devices`` maps device -> full history (list of
        ``{"start", "end", "owner"}``); an empty list clears the device.
        ``expected_revision`` must equal the current mart revision.
        Everything is validated before any write, and the whole call is
        one transaction: stale revisions, duplicate device keys and any
        illegal interval reject the whole correction, leaving other
        devices and statistics untouched.  One successful call bumps the
        dimension revision by exactly one, even if the effective ownership
        did not change.
        """
        self.require_lock()
        if isinstance(expected_revision, bool) or not isinstance(
                expected_revision, int):
            raise AttributionInputError(
                f"expected_revision must be an integer, got "
                f"{expected_revision!r}")
        cur = self._state()["mart_revision"]
        if expected_revision != cur:
            raise StaleRevisionError(
                f"stale expected_revision {expected_revision}: the mart is "
                f"at revision {cur}; nothing was written")
        if isinstance(devices, dict):
            items = list(devices.items())
        elif isinstance(devices, (list, tuple)):
            items = []
            for i, pair in enumerate(devices):
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    raise AttributionInputError(
                        f"device entry #{i} must be a [device, history] pair")
                items.append((pair[0], pair[1]))
        else:
            raise AttributionInputError(
                "devices must be an object mapping device -> full history,"
                " or a list of [device, history] pairs")
        if not items:
            raise AttributionInputError("no devices given")
        if len({d for d, _ in items}) != len(items):
            raise AttributionInputError(
                "duplicate device key in the replacement; each device may"
                " appear only once")
        parsed: Dict[str, TimeMap] = {}
        for dev, hist in items:
            if not isinstance(dev, str) or not dev:
                raise AttributionInputError(
                    f"device keys must be non-empty text, got {dev!r}")
            if hist is None:
                hist = []
            if not isinstance(hist, (list, tuple)):
                raise AttributionInputError(
                    f"device {dev!r}: history must be a list of segments "
                    f"(an empty list clears the device)")
            try:
                parsed[dev] = TimeMap(parse_segments(hist))
            except AttributionInputError as e:
                raise AttributionInputError(f"device {dev!r}: {e}") from e

        index = HistoryIndex(self._conn)
        current: Dict[str, TimeMap] = {}
        changed: Dict[str, List[Tuple[Optional[int], Optional[int]]]] = {}
        for dev, new_tm in parsed.items():
            old_tm = index.get(dev)
            current[dev] = old_tm if old_tm is not None else TimeMap(())
            ranges = changed_intervals(current[dev], new_tm)
            if ranges:
                changed[dev] = ranges
            index.put(dev, new_tm)

        scanned = 0
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            for dev, ranges in changed.items():
                for lo, hi in ranges:
                    scanned += self._correct_range(index, dev, parsed[dev],
                                                   lo, hi)
            # publish the new histories (even for unchanged effective maps)
            for dev, new_tm in parsed.items():
                self._conn.execute(
                    "DELETE FROM attr_segments WHERE device=?", (dev,))
                self._conn.executemany(
                    "INSERT INTO attr_segments(device, idx, start_txt,"
                    " end_txt, owner) VALUES (?,?,?,?,?)",
                    [(dev, i,
                      None if s.start is None else str(s.start),
                      None if s.end is None else str(s.end), s.owner)
                     for i, s in enumerate(new_tm.segments)])
            _test_hook("correction.before_commit")
            recomputed = self._recompute_dirty()
            self._commit(op="correction", dim_bump=True)
            self._conn.execute("COMMIT")
            _test_hook("correction.after_commit")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        return {"corrected_devices": sorted(parsed),
                "changed_ranges": {d: [[a, b] for a, b in rs]
                                   for d, rs in changed.items()},
                "facts_scanned": scanned,
                "groups_recomputed": recomputed,
                **self._state()}

    def _correct_range(self, index: "HistoryIndex", device: str,
                       new_tm: TimeMap, lo: Optional[int],
                       hi: Optional[int]) -> int:
        """Re-attribute the facts of `device` whose anchor lies in
        ``[lo, hi)`` and return how many facts were read.

        The scan is keyset-paged through the ``(device, anchor_enc)``
        index, so it never materialises the device's history and never
        reads facts outside the changed range.  For every affected fact
        its old group loses the contribution and its new group gains it;
        both groups are marked dirty for recomputation."""
        c = self._conn
        last_enc: Optional[str] = None
        last_id = 0
        scanned = 0
        while True:
            clauses = ["k=?"]
            args: List[Any] = [device]
            if lo is not None:
                clauses.append("anchor_enc>=?")
                args.append(enc_int(lo))
            if hi is not None:
                clauses.append("anchor_enc<?")
                args.append(enc_int(hi))
            if last_enc is not None:
                clauses.append("(anchor_enc>? OR (anchor_enc=? AND id>?))")
                args.extend([last_enc, last_enc, last_id])
            args.append(SNAPSHOT_CHUNK)
            rows = c.execute(
                "SELECT id, job_id, offset, kind, lp, ls, rp, rs, ltime,"
                " rtime, anchor_txt, anchor_enc, ws_txt, ws_enc, owner_k,"
                " status"
                f" FROM facts WHERE {' AND '.join(clauses)}"
                " ORDER BY anchor_enc, id LIMIT ?", args).fetchall()
            if not rows:
                break
            for r in rows:
                (fid, job_id, offset, kind, lp, ls, rp, rs, ltime, rtime,
                 anchor_txt, anchor_enc, ws_txt, ws_enc, old_owner_k,
                 old_status) = r
                scanned += 1
                anchor = int(anchor_txt)
                owner_k, status, _seg = self._attribute(index, device, anchor)
                if owner_k == old_owner_k and status == old_status:
                    continue
                c.execute(
                    "UPDATE facts SET owner_k=?, status=? WHERE id=?",
                    (owner_k, status, fid))
                self._dirty_enc(ws_enc, ws_txt, old_owner_k)
                self._dirty_enc(ws_enc, ws_txt, owner_k)
                if kind == "match":
                    c.execute(
                        "UPDATE reqs SET owner_k=? WHERE job_id=? AND lp=?"
                        " AND ls=?", (owner_k, job_id, lp, ls))
                    lat = parse_dec(rtime) - anchor
                    le = enc_int(lat)
                    c.execute(
                        "UPDATE lat_freq SET cnt=cnt-1 WHERE ws_enc=?"
                        " AND owner_k=? AND lat_enc=?",
                        (ws_enc, old_owner_k, le))
                    c.execute(
                        "DELETE FROM lat_freq WHERE ws_enc=? AND owner_k=?"
                        " AND lat_enc=? AND cnt<=0",
                        (ws_enc, old_owner_k, le))
                    c.execute(
                        "INSERT INTO lat_freq(ws_enc, owner_k, lat_enc,"
                        " lat_txt, cnt) VALUES (?,?,?,?,1)"
                        " ON CONFLICT(ws_enc, owner_k, lat_enc)"
                        " DO UPDATE SET cnt=cnt+1",
                        (ws_enc, owner_k, le, str(lat)))
            last_enc, last_id = rows[-1][11], rows[-1][0]
            if len(rows) < SNAPSHOT_CHUNK:
                break
        return scanned

    # ----------------------------------------------------------- status
    def status(self) -> Dict[str, Any]:
        state = self._state()
        counts = {}
        for table in ("facts", "reqs", "lat_freq", "group_stats",
                      "attr_segments"):
            counts[table] = int(self._conn.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return {"path": self.path, "warehouse_id": self.warehouse_id,
                "warehouse_path": self._warehouse_path,
                "job_ids": self.job_ids,
                "window": {"width": self._width, "origin": self._origin},
                **state,
                "counts": counts,
                "locked": self._locked_probe()}


# --------------------------------------------------------------- hooks
_HOOK_COUNTS: Dict[str, int] = {}


def _test_hook(name: str) -> None:
    """Inert test hook for crash/barrier acceptance drills.

    ``MART_CRASH_AT`` is a comma list of ``point`` or ``point@count``
    entries; a hit calls ``os._exit(117)`` (simulated abrupt kill).
    ``MART_BARRIER_DIR`` makes the named points create
    ``<dir>/<point>.reached`` and wait for ``<dir>/<point>.release`` so a
    parent process can interleave work exactly between two steps.
    Neither variable affects a normal run.
    """
    spec = os.environ.get("MART_CRASH_AT", "")
    if spec:
        for item in spec.split(","):
            item = item.strip()
            if not item:
                continue
            if "@" in item:
                point, _, n = item.rpartition("@")
                count = int(n)
            else:
                point, count = item, 1
            if point == name:
                hit = _HOOK_COUNTS.get(name, 0) + 1
                _HOOK_COUNTS[name] = hit
                if hit >= count:
                    os._exit(117)
    bdir = os.environ.get("MART_BARRIER_DIR")
    if bdir:
        # Only *requested* barrier points block: the parent creates
        # <point>.want.  A plain MART_BARRIER_DIR never stalls other steps.
        if not os.path.exists(os.path.join(bdir, name + ".want")):
            return
        reached = os.path.join(bdir, name + ".reached")
        release = os.path.join(bdir, name + ".release")
        with open(reached, "w", encoding="utf-8") as f:
            f.write("reached\n")
            f.flush()
            os.fsync(f.fileno())
        deadline = _time.monotonic() + 120
        while not os.path.exists(release):
            if _time.monotonic() > deadline:
                raise MartError(f"barrier {name!r} timed out")
            _time.sleep(0.005)
