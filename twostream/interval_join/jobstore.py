"""Persistent join jobs: recoverable input batches and acknowledged delivery.

A *job store* is a single SQLite file holding everything needed to resume
the two-stream interval join after a process crash and to hand results to
a warehouse with explicit, cumulative acknowledgements:

  * job_meta        -- job_id (generated at creation, never changes), the
                       format version, the frozen join configuration and a
                       per-job secret used to tag ack credentials
  * batch_ledger    -- one row per committed input batch: batch_no, the
                       canonical batch content and the receipt that was
                       returned (the input acknowledgement position)
  * engine tables   -- left/right state, match ledger, outbox, latebox
                       (JoinEngine in durable mode)
  * seq/wm/end      -- durable per-partition ledgers (decimal text, so
                       arbitrary-precision times never degrade to floats)
  * delivery_state  -- per-channel acknowledged delivery offset and the
                       number of source rows reclaimed after ack
  * target_binding  -- the persistent identity of the delivery warehouse

Batch intake
------------
The collector advances the job with ordered envelopes
{"batch_no": n, "messages": [...]} where batch_no starts at 1 and is
strictly consecutive and each message is a data/watermark/end object in
the existing event-file format.  A batch is all-or-nothing: messages are
applied inside one SQLite transaction together with the ledger row, and
the receipt is returned only after the commit.  Resending an already
committed batch with identical content returns the original receipt
without re-executing anything; the same number with different content,
gaps, and illegal in-batch messages are rejected and leave the job at the
last committed point (a later legal batch is still accepted).  Content is
compared as decoded JSON values: object key order and whitespace do not
matter, array order and the string/number distinction do; NaN, Infinity
and non-JSON objects are not accepted.  Once every partition ended, new
batches are rejected but old ones can still be resent for their receipt.

Acknowledged delivery
---------------------
Business results (channel "results") and late records (channel "late")
each carry an independent, strictly increasing, never reused
delivery_offset; a delivery is identified by (job_id, channel, offset).
read_page() returns the next page after the acknowledged position without
deleting anything, plus a credential binding (job_id, channel, page
start/end, limit, content hash).  ack_page() advances the acknowledged
offset only for a whole, contiguous, unforged page; re-acking the same
credential is a successful no-op.  Acked rows may be reclaimed, but
offsets never restart at 1.  Processing completion (all partitions ended)
and delivery completion (nothing unacknowledged) are reported separately.

Modification lock
-----------------
Input advance, acks, delivery and target binding all require the job's
modification lock (JobStore.acquire()/modify()), an flock on a sidecar
file: a second modifier gets BusyError, while a process killed with
SIGKILL/os._exit releases it automatically (the kernel closes the fd), so
a successor can take over without manual lock removal.  Read-only status
queries run without the lock and always see the last committed state.

Test hook: when the environment variable TWOSTREAM_CRASH_AT is set to a
comma-separated list of point[@count] entries, the named points call
os._exit(117) on the count-th hit, simulating an abrupt kill between
handoff steps ("batch.pre_commit", "batch.post_commit",
"deliver.pre_target_commit", "deliver.post_target_commit").  The normal
path never depends on it.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .engine import JoinEngine, _validate_config
from .errors import (AckError, BatchConflictError, BatchGapError, BusyError,
                     DeliveryConflictError, InputError, JobExistsError,
                     JobFinishedError, LockRequiredError, ResourceError,
                     StoreFormatError)
from .messages import parse_message
from .results import EventId, Late

FORMAT_VERSION = 1

CHANNELS = ("results", "late")
_CHANNEL_TABLE = {"results": "outbox", "late": "latebox"}

_META_SCHEMA = """
CREATE TABLE IF NOT EXISTS job_meta(
  id INTEGER PRIMARY KEY CHECK(id=1),
  job_id TEXT NOT NULL,
  format_version INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  config_json TEXT NOT NULL,
  secret TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_ledger(
  batch_no INTEGER PRIMARY KEY,
  content_sha256 TEXT NOT NULL,
  content_json TEXT NOT NULL,
  receipt_json TEXT NOT NULL,
  committed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_state(
  channel TEXT PRIMARY KEY,
  acked_offset INTEGER NOT NULL,
  reclaimed INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS target_binding(
  id INTEGER PRIMARY KEY CHECK(id=1),
  warehouse_id TEXT NOT NULL,
  bound_at TEXT NOT NULL
);
"""

_REQUIRED_TABLES = {
    "job_meta", "batch_ledger", "delivery_state", "target_binding",
    "left_state", "right_state", "matched", "outbox", "latebox",
    "seq_ledger", "wm_ledger", "end_ledger",
}

_CONFIG_KEYS = {"lo", "hi", "mode", "left_partitions", "right_partitions"}

_INT64_MAX = 2 ** 63 - 1


# --------------------------------------------------------------- test hook
_CRASH_COUNTS: Dict[str, int] = {}


def _crashpoint(name: str) -> None:
    """Abrupt-exit test hook (see module docstring).  Inert unless
    TWOSTREAM_CRASH_AT names this point."""
    spec = os.environ.get("TWOSTREAM_CRASH_AT", "")
    if not spec:
        return
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
            hit = _CRASH_COUNTS.get(name, 0) + 1
            _CRASH_COUNTS[name] = hit
            if hit >= count:
                os._exit(117)  # distinctive code: simulated kill


# --------------------------------------------------------- canonical JSON
def _reject_constant(x: str):
    raise InputError(f"non-standard JSON value {x!r} is not allowed in a batch")


def _validate_json_value(v: Any, path: str = "$") -> None:
    if v is None or isinstance(v, bool) or isinstance(v, int) \
            or isinstance(v, str):
        return
    if isinstance(v, float):
        if math.isfinite(v):
            return
        raise InputError(f"{path}: non-finite numbers are not standard JSON")
    if isinstance(v, list):
        for i, x in enumerate(v):
            _validate_json_value(x, f"{path}[{i}]")
        return
    if isinstance(v, dict):
        for k, x in v.items():
            if not isinstance(k, str):
                raise InputError(f"{path}: object keys must be strings, got {k!r}")
            _validate_json_value(x, f"{path}.{k}")
        return
    raise InputError(
        f"{path}: value of type {type(v).__name__} is not JSON-serializable")


def _normalize(v: Any) -> Any:
    if v is None or isinstance(v, bool) or isinstance(v, int) \
            or isinstance(v, str):
        return v
    if isinstance(v, float):
        # JSON has a single number type: 1 and 1.0 are the same value.
        return int(v) if v.is_integer() else v
    if isinstance(v, list):
        return [_normalize(x) for x in v]
    return {k: _normalize(v[k]) for k in sorted(v)}


def _canon(value: Any) -> str:
    """Canonical JSON text: sorted object keys, no whitespace, numbers
    normalised (1 == 1.0).  Array order and string/number types matter."""
    return json.dumps(_normalize(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------ page records
@dataclass(frozen=True)
class Delivery:
    """One deliverable row: its delivery offset and full content.

    `record` is the typed business object (Match/LeftPad/RightPad/Late);
    `row` is the complete JSON-safe dict used for hashing and warehousing
    (times/seqs as decimal text, so arbitrary precision is preserved)."""
    offset: int
    record: Any
    row: Dict[str, Any]


@dataclass(frozen=True)
class Page:
    """The next unacknowledged page of a channel.  Reading a page never
    deletes anything; repeated reads before ack return identical content."""
    channel: str
    rows: tuple                 # tuple of Delivery
    start: Optional[int]        # first delivery offset (None when empty)
    end: Optional[int]          # last delivery offset (None when empty)
    rows_hash: Optional[str]
    credential: Optional[dict]  # opaque ack credential for ack_page()

    @property
    def empty(self) -> bool:
        return not self.rows


def _results_row_dict(r) -> Dict[str, Any]:
    return {"kind": r[1], "lp": r[2], "ls": r[3], "rp": r[4], "rs": r[5],
            "k": r[6], "ltime": r[7], "rtime": r[8]}


def _late_row_dict(r) -> Dict[str, Any]:
    return {"side": r[1], "partition": r[2], "seq": r[3], "k": r[4],
            "time": r[5], "wm": r[6], "payload": r[7]}


def _late_record(r) -> Late:
    return Late(EventId(r[1], r[2], int(r[3])), r[4], int(r[5]), int(r[6]))


def hash_page_rows(channel: str, rows) -> str:
    """Content hash of a page's (offset, row-dict) sequence; shared by the
    source credential and the warehouse receipt."""
    h = hashlib.sha256()
    for offset, rd in rows:
        h.update(_canon([channel, offset, rd]).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _offset_span(before: int, after: int) -> Dict[str, Any]:
    return {"count": after - before,
            "first_offset": before + 1 if after > before else None,
            "last_offset": after if after > before else None}


def _validate_envelope(envelope):
    """Validate a batch envelope (dict or JSON text).  Returns
    (batch_no, messages, canonical_content_json).  Raises InputError."""
    if isinstance(envelope, str):
        try:
            envelope = json.loads(envelope, parse_constant=_reject_constant)
        except json.JSONDecodeError as e:
            raise InputError(f"batch envelope is not valid JSON: {e}") from e
    if not isinstance(envelope, dict):
        raise InputError("batch envelope must be a JSON object "
                         '{"batch_no": n, "messages": [...]}')
    extra = set(envelope) - {"batch_no", "messages"}
    if extra:
        raise InputError(f"unknown batch envelope fields: {sorted(extra)}")
    bn = envelope.get("batch_no")
    if not isinstance(bn, int) or isinstance(bn, bool) or bn < 1:
        raise InputError(f"batch_no must be a positive integer, got {bn!r}")
    if bn > _INT64_MAX:
        raise InputError(f"batch_no too large: {bn!r}")
    messages = envelope.get("messages")
    if not isinstance(messages, list):
        raise InputError(f"messages must be an array, got {messages!r}")
    _validate_json_value(envelope)
    return bn, messages, _canon(messages)


class JobStore:
    """A persistent, recoverable join job.  Create with JobStore.create(),
    reopen with JobStore.open(); see the module docstring."""

    # ----------------------------------------------------------- creation
    @classmethod
    def create(cls, path: str, *, lo: int, hi: int, mode: str,
               left_partitions, right_partitions) -> "JobStore":
        """Create a new job store at `path`.  Refuses to touch a non-empty
        existing file (never silently replaces or reconfigures a job)."""
        lp, rp = _validate_config(lo, hi, mode, left_partitions,
                                  right_partitions)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            raise JobExistsError(
                f"refusing to create a new job at {path!r}: a non-empty file "
                f"already exists there (open it with JobStore.open() instead)")
        try:
            lock_fd = os.open(path + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        except OSError as e:
            raise StoreFormatError(
                f"cannot create job store at {path!r}: {e}") from e
        try:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as e:
                raise BusyError(f"cannot create job at {path!r}: "
                                f"another process is working on it") from e
            try:
                conn = sqlite3.connect(path)
            except sqlite3.Error as e:
                raise StoreFormatError(f"cannot create job store {path!r}: {e}") from e
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=5000")
                conn.executescript(_META_SCHEMA)
                job_id = uuid.uuid4().hex
                secret = uuid.uuid4().hex
                config = {"lo": lo, "hi": hi, "mode": mode,
                          "left_partitions": lp, "right_partitions": rp}
                conn.execute(
                    "INSERT INTO job_meta(id, job_id, format_version,"
                    " created_at, config_json, secret) VALUES (1,?,?,?,?,?)",
                    (job_id, FORMAT_VERSION, _utcnow(),
                     _canon(config), secret))
                conn.executemany(
                    "INSERT INTO delivery_state(channel, acked_offset,"
                    " reclaimed) VALUES (?,0,0)", [(c,) for c in CHANNELS])
                conn.commit()  # PRAGMAs in the engine ctor need a clean txn
                engine = JoinEngine(path, lo, hi, mode, lp, rp,
                                    _conn=conn, _durable=True)
                conn.commit()
            except Exception:
                conn.close()
                raise
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
        return cls._wrap(conn, engine, path, job_id, secret)

    @classmethod
    def open(cls, path: str) -> "JobStore":
        """Reopen the job store at `path`, restoring the frozen
        configuration and every committed ledger.  Fails explicitly —
        without writing anything — on missing/empty files, old
        event-table-only databases, unknown format versions and corrupt
        metadata."""
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise StoreFormatError(
                f"no job store at {path!r} (missing or empty file); "
                f"create one with JobStore.create()")
        try:
            conn = sqlite3.connect(path)
            conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.Error as e:
            raise StoreFormatError(f"cannot open job store {path!r}: {e}") from e
        try:
            job_id, secret, config = cls._read_meta(conn, path)
            engine = JoinEngine(path, config["lo"], config["hi"],
                                config["mode"], config["left_partitions"],
                                config["right_partitions"],
                                _conn=conn, _durable=True)
        except Exception:
            conn.close()
            raise
        return cls._wrap(conn, engine, path, job_id, secret)

    @staticmethod
    def _read_meta(conn, path):
        """Read and validate job metadata.  Raises StoreFormatError before
        any write when the file cannot be interpreted as a job store."""
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if "job_meta" not in tables:
                others = tables - {"sqlite_sequence"}
                if others:
                    raise StoreFormatError(
                        f"{path!r} is not a recoverable job store: it has "
                        f"tables {sorted(others)} but no job metadata.  An "
                        f"old event-table-only database cannot be resumed; "
                        f"start a new job instead")
                raise StoreFormatError(
                    f"{path!r} holds an empty database, not a job store")
            row = conn.execute(
                "SELECT job_id, format_version, config_json, secret"
                " FROM job_meta WHERE id=1").fetchone()
            if row is None:
                raise StoreFormatError(f"{path!r}: job metadata row is missing")
            job_id, version, config_json, secret = row
            if version != FORMAT_VERSION:
                raise StoreFormatError(
                    f"{path!r}: unsupported job store format version "
                    f"{version!r} (this build understands {FORMAT_VERSION}); "
                    f"refusing to touch it")
            if not isinstance(job_id, str) or not job_id \
                    or not isinstance(secret, str) or not secret:
                raise StoreFormatError(f"{path!r}: corrupt job identity metadata")
            try:
                config = json.loads(config_json)
            except (TypeError, json.JSONDecodeError) as e:
                raise StoreFormatError(
                    f"{path!r}: uninterpretable job configuration: {e}") from e
            if not isinstance(config, dict) or set(config) != _CONFIG_KEYS:
                raise StoreFormatError(
                    f"{path!r}: job configuration has unknown or missing "
                    f"fields: {sorted(config) if isinstance(config, dict) else config!r}")
            try:
                _validate_config(config["lo"], config["hi"], config["mode"],
                                 config["left_partitions"],
                                 config["right_partitions"])
            except ValueError as e:
                raise StoreFormatError(
                    f"{path!r}: stored job configuration is invalid: {e}") from e
            missing = _REQUIRED_TABLES - tables
            if missing:
                raise StoreFormatError(
                    f"{path!r}: job store is incomplete, missing tables: "
                    f"{sorted(missing)}")
            have_channels = {r[0] for r in conn.execute(
                "SELECT channel FROM delivery_state")}
            if have_channels != set(CHANNELS):
                raise StoreFormatError(
                    f"{path!r}: delivery state is incomplete "
                    f"(channels {sorted(have_channels)})")
            return job_id, secret, config
        except sqlite3.DatabaseError as e:
            raise StoreFormatError(
                f"{path!r} is corrupt or not a SQLite database: {e}") from e

    @classmethod
    def _wrap(cls, conn, engine, path, job_id, secret) -> "JobStore":
        self = cls.__new__(cls)
        self._conn = conn
        self._engine = engine
        self._path = path
        self._job_id = job_id
        self._secret = secret
        self._lock_fd = None
        return self

    # ------------------------------------------------------------- basics
    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def path(self) -> str:
        return self._path

    @property
    def lo(self) -> int:
        return self._engine.lo

    @property
    def hi(self) -> int:
        return self._engine.hi

    @property
    def mode(self) -> str:
        return self._engine.mode

    @property
    def left_partitions(self) -> List[str]:
        return list(self._engine.left_partitions)

    @property
    def right_partitions(self) -> List[str]:
        return list(self._engine.right_partitions)

    def close(self) -> None:
        try:
            self.release()
        finally:
            self._conn.close()

    def __enter__(self) -> "JobStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------- modification lock
    def acquire(self) -> "JobStore":
        """Take the modification lock (non-blocking).  Raises BusyError if
        another process holds it.  The lock is an flock on a sidecar file,
        so a holder killed by SIGKILL/os._exit releases it automatically.
        Refreshes the durable ledgers after acquisition so the new holder
        resumes from the last committed state."""
        if self._lock_fd is not None:
            return self
        fd = os.open(self._path + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            os.close(fd)
            raise BusyError(
                f"job store {self._path!r} is locked by another process") from e
        self._lock_fd = fd
        self._engine.reload_ledgers()
        return self

    def release(self) -> None:
        fd, self._lock_fd = self._lock_fd, None
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def modify(self):
        """Context manager: acquire() on entry, release() on exit."""
        job = self

        class _Modify:
            def __enter__(self):
                job.acquire()
                return job

            def __exit__(self, *exc):
                job.release()
                return False

        return _Modify()

    def require_lock(self) -> None:
        if self._lock_fd is None:
            raise LockRequiredError(
                "this operation modifies the job; call acquire() or use "
                "the modify() context manager first")

    def _locked_probe(self) -> bool:
        if self._lock_fd is not None:
            return True
        fd = os.open(self._path + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        finally:
            os.close(fd)

    # ------------------------------------------------------------ batches
    def submit_batch(self, envelope) -> Dict[str, Any]:
        """Submit one input batch envelope {"batch_no", "messages"} (dict
        or JSON text).  Returns the receipt after the durable commit.

        Resending a committed batch with identical content returns the
        original receipt without re-executing; conflicts, gaps and illegal
        messages are rejected and rolled back to the last committed point.
        """
        self.require_lock()
        batch_no, messages, content_json = _validate_envelope(envelope)
        last = self._last_batch_no()
        if batch_no <= last:
            row = self._conn.execute(
                "SELECT content_json, receipt_json FROM batch_ledger"
                " WHERE batch_no=?", (batch_no,)).fetchone()
            if row is not None and row[0] == content_json:
                return json.loads(row[1])
            raise BatchConflictError(
                f"batch {batch_no} was already committed with different "
                f"content; the job remains at batch {last}")
        if batch_no != last + 1:
            raise BatchGapError(
                f"expected batch_no {last + 1}, got {batch_no}")
        if self._all_ended():
            raise JobFinishedError(
                "all partitions have ended; new batches are rejected "
                "(already-committed batches may still be resent for their "
                "receipt)")
        snap = self._engine.snapshot_ledgers()
        out_before = self._max_offset("results")
        late_before = self._max_offset("late")
        now = _utcnow()
        try:
            for m in messages:
                self._engine.apply_message(parse_message(m))
            out_after = self._max_offset("results")
            late_after = self._max_offset("late")
            receipt = {
                "job_id": self._job_id,
                "batch_no": batch_no,
                "messages": len(messages),
                "results": _offset_span(out_before, out_after),
                "late": _offset_span(late_before, late_after),
                "committed_at": now,
            }
            self._conn.execute(
                "INSERT INTO batch_ledger(batch_no, content_sha256,"
                " content_json, receipt_json, committed_at)"
                " VALUES (?,?,?,?,?)",
                (batch_no,
                 hashlib.sha256(content_json.encode("utf-8")).hexdigest(),
                 content_json, json.dumps(receipt, ensure_ascii=False), now))
            _crashpoint("batch.pre_commit")
            self._conn.commit()
        except BaseException as e:
            # Roll back both the disk writes (engine state, durable
            # ledgers, ledger row) and the in-memory ledger cache, so the
            # job is exactly at the last committed batch.
            self._conn.rollback()
            self._engine.restore_ledgers(snap)
            if isinstance(e, sqlite3.Error):
                raise ResourceError(str(e)) from e
            raise
        _crashpoint("batch.post_commit")
        return receipt

    def _last_batch_no(self) -> int:
        return self._conn.execute(
            "SELECT COALESCE(MAX(batch_no), 0) FROM batch_ledger"
        ).fetchone()[0]

    def _all_ended(self) -> bool:
        n = self._conn.execute("SELECT COUNT(*) FROM end_ledger").fetchone()[0]
        return n == len(self._engine.left_partitions) + \
            len(self._engine.right_partitions)

    # ------------------------------------------------------------- pages
    def read_page(self, channel: str, limit: int) -> Page:
        """Read the next unacknowledged page of `channel` ("results" or
        "late").  Never deletes; repeated reads before ack are identical."""
        self._check_channel(channel)
        if not isinstance(limit, int) or isinstance(limit, bool) \
                or not 1 <= limit <= 1_000_000:
            raise InputError(f"page limit must be an integer in 1..1000000,"
                             f" got {limit!r}")
        acked = self.acked_offset(channel)
        if channel == "results":
            rows = self._conn.execute(
                "SELECT id,kind,lp,ls,rp,rs,k,ltime,rtime FROM outbox"
                " WHERE id>? ORDER BY id LIMIT ?", (acked, limit)).fetchall()
            deliveries = tuple(
                Delivery(r[0], JoinEngine._mk_result(r), _results_row_dict(r))
                for r in rows)
        else:
            rows = self._conn.execute(
                "SELECT id,side,partition,seq_txt,k,time_txt,wm_txt,payload"
                " FROM latebox WHERE id>? ORDER BY id LIMIT ?",
                (acked, limit)).fetchall()
            deliveries = tuple(
                Delivery(r[0], _late_record(r), _late_row_dict(r))
                for r in rows)
        if not rows:
            return Page(channel, (), None, None, None, None)
        start, end = rows[0][0], rows[-1][0]
        rows_hash = hash_page_rows(
            channel, [(d.offset, d.row) for d in deliveries])
        credential = self._make_credential(channel, start, end, limit,
                                           rows_hash)
        return Page(channel, deliveries, start, end, rows_hash, credential)

    def ack_page(self, credential: Dict[str, Any]) -> Dict[str, Any]:
        """Cumulatively acknowledge one whole page by its credential.

        Only a credential issued by read_page() for this job and channel,
        starting exactly at the first unacknowledged offset and matching
        the committed page content, advances the acknowledged position.
        Re-acking the same credential is a successful no-op.  Acked rows
        are reclaimed, but delivery offsets never restart at 1.
        """
        self.require_lock()
        c_job, c_chan, c_start, c_end, c_limit, c_hash, c_tag = \
            self._check_credential_shape(credential)
        if c_job != self._job_id:
            raise AckError("credential belongs to a different job")
        if c_chan not in CHANNELS:
            raise AckError(f"unknown delivery channel {c_chan!r}")
        if c_tag != self._tag(c_chan, c_start, c_end, c_limit, c_hash):
            raise AckError("credential tag is invalid (forged, corrupted, "
                           "or from another channel/job)")
        acked = self.acked_offset(c_chan)
        if c_end <= acked:
            return {"channel": c_chan, "acked_offset": acked,
                    "already_acked": True, "reclaimed": 0}
        if c_start != acked + 1:
            raise AckError(
                f"credential starts at offset {c_start}, but the first "
                f"unacknowledged offset of channel {c_chan!r} is "
                f"{acked + 1}; pages must be acknowledged contiguously")
        page = self.read_page(c_chan, c_limit)
        if page.end != c_end or page.rows_hash != c_hash:
            raise AckError(
                "credential does not match the committed page content "
                "(forged end or changed rows)")
        table = _CHANNEL_TABLE[c_chan]
        try:
            cur = self._conn.execute(f"DELETE FROM {table} WHERE id<=?",
                                     (c_end,))
            deleted = cur.rowcount
            self._conn.execute(
                "UPDATE delivery_state SET acked_offset=?,"
                " reclaimed=reclaimed+? WHERE channel=?",
                (c_end, deleted, c_chan))
            self._conn.commit()
        except sqlite3.Error as e:
            self._conn.rollback()
            raise ResourceError(str(e)) from e
        return {"channel": c_chan, "acked_offset": c_end,
                "already_acked": False, "reclaimed": deleted}

    @staticmethod
    def _check_credential_shape(credential):
        if not isinstance(credential, dict):
            raise AckError("credential must be a JSON object")
        try:
            values = [credential[k] for k in
                      ("job_id", "channel", "start", "end", "limit",
                       "rows_sha256", "tag")]
        except KeyError as e:
            raise AckError(f"credential is missing field {e}") from e
        c_job, c_chan, c_start, c_end, c_limit, c_hash, c_tag = values
        for name, v in (("start", c_start), ("end", c_end),
                        ("limit", c_limit)):
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                raise AckError(f"credential field {name!r} must be a "
                               f"positive integer, got {v!r}")
        if c_end < c_start:
            raise AckError(f"credential end {c_end} < start {c_start}")
        for name, v in (("job_id", c_job), ("channel", c_chan),
                        ("rows_sha256", c_hash), ("tag", c_tag)):
            if not isinstance(v, str) or not v:
                raise AckError(f"credential field {name!r} must be text")
        return c_job, c_chan, c_start, c_end, c_limit, c_hash, c_tag

    def _tag(self, channel, start, end, limit, rows_hash) -> str:
        payload = _canon([self._job_id, channel, start, end, limit, rows_hash])
        return hashlib.sha256(
            (self._secret + ":" + payload).encode("utf-8")).hexdigest()

    def _make_credential(self, channel, start, end, limit, rows_hash) -> dict:
        return {"job_id": self._job_id, "channel": channel, "start": start,
                "end": end, "limit": limit, "rows_sha256": rows_hash,
                "tag": self._tag(channel, start, end, limit, rows_hash)}

    # ------------------------------------------------------------- state
    @staticmethod
    def _check_channel(channel) -> None:
        if channel not in CHANNELS:
            raise InputError(f"channel must be one of {CHANNELS},"
                             f" got {channel!r}")

    def acked_offset(self, channel: str) -> int:
        self._check_channel(channel)
        return self._conn.execute(
            "SELECT acked_offset FROM delivery_state WHERE channel=?",
            (channel,)).fetchone()[0]

    def _max_offset(self, channel: str) -> int:
        # High-water mark of ever-emitted offsets: the AUTOINCREMENT
        # sequence survives reclamation deletes, so offsets never restart.
        return self._conn.execute(
            "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name=?),"
            " 0)", (_CHANNEL_TABLE[channel],)).fetchone()[0]

    def target_binding(self) -> Optional[str]:
        row = self._conn.execute(
            "SELECT warehouse_id FROM target_binding WHERE id=1").fetchone()
        return row[0] if row else None

    def bind_target(self, warehouse_id: str) -> None:
        """Bind this job to a warehouse identity (first delivery).  A
        different identity is rejected; the binding survives restarts."""
        self.require_lock()
        row = self._conn.execute(
            "SELECT warehouse_id FROM target_binding WHERE id=1").fetchone()
        if row is not None:
            if row[0] != warehouse_id:
                raise DeliveryConflictError(
                    f"job is already bound to warehouse {row[0]},"
                    f" refusing to rebind to {warehouse_id}")
            return
        try:
            self._conn.execute(
                "INSERT INTO target_binding(id, warehouse_id, bound_at)"
                " VALUES (1,?,?)", (warehouse_id, _utcnow()))
            self._conn.commit()
        except sqlite3.Error as e:
            self._conn.rollback()
            raise ResourceError(str(e)) from e

    # ------------------------------------------------------------- status
    def status(self) -> Dict[str, Any]:
        """Committed, read-only status.  Safe to call while another process
        holds the modification lock: it always shows the last committed
        state, never a half-applied batch."""
        ended = {(s, p) for s, p in self._conn.execute(
            "SELECT side, partition FROM end_ledger")}
        wms = {(s, p): int(w) for s, p, w in self._conn.execute(
            "SELECT side, partition, wm_txt FROM wm_ledger")}

        def view(side, parts):
            return {p: {"watermark": wms.get((side, p)),
                        "ended": (side, p) in ended} for p in parts}

        def side_min(side, parts):
            acc = None
            for p in parts:
                if (side, p) in ended:
                    continue
                w = wms.get((side, p))
                if w is None:
                    return None  # silent partition pins the side to -inf
                acc = w if acc is None else min(acc, w)
            return "+inf" if acc is None else acc

        lp = self._engine.left_partitions
        rp = self._engine.right_partitions
        all_ended = len(ended) == len(lp) + len(rp)
        last_batch, n_batches = self._conn.execute(
            "SELECT COALESCE(MAX(batch_no),0), COUNT(*) FROM batch_ledger"
        ).fetchone()
        one = lambda q: self._conn.execute(q).fetchone()[0]
        delivery = {}
        fully_delivered = True
        for channel in CHANNELS:
            acked, reclaimed = self._conn.execute(
                "SELECT acked_offset, reclaimed FROM delivery_state"
                " WHERE channel=?", (channel,)).fetchone()
            mx = self._max_offset(channel)
            fully_delivered = fully_delivered and acked == mx
            delivery[channel] = {"acked_offset": acked, "max_offset": mx,
                                 "pending": mx - acked, "reclaimed": reclaimed}
        return {
            "job_id": self._job_id,
            "format_version": FORMAT_VERSION,
            "config": {"lo": self.lo, "hi": self.hi, "mode": self.mode,
                       "left_partitions": lp, "right_partitions": rp},
            "batches": {"last": last_batch, "count": n_batches},
            "processing": {
                "status": "finished" if all_ended else "running",
                "ended_partitions": len(ended),
                "declared_partitions": len(lp) + len(rp),
                "left": view("L", lp),
                "right": view("R", rp),
                "left_min": side_min("L", lp),    # null=-inf, "+inf", or int
                "right_min": side_min("R", rp),
            },
            "state": {
                "left_state": one("SELECT COUNT(*) FROM left_state"),
                "right_state": one("SELECT COUNT(*) FROM right_state"),
                "matched_marks": one("SELECT COUNT(*) FROM matched"),
                "outbox": one("SELECT COUNT(*) FROM outbox"),
                "latebox": one("SELECT COUNT(*) FROM latebox"),
            },
            "delivery": dict(delivery, fully_delivered=fully_delivered),
            "warehouse_complete": all_ended and fully_delivered,
            "target": {"warehouse_id": self.target_binding()},
            "locked": self._locked_probe(),
        }
