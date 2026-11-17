"""Delivery of acknowledged result pages to an independent SQLite warehouse.

Protocol per page, per channel ("results" then "late"):

  1. read the next unacknowledged page from the job store (no deletion);
  2. if the warehouse already holds a receipt for exactly this page, the
     page was committed before a crash: skip the write.  Otherwise write
     all rows plus the receipt in ONE warehouse transaction.  Rows are
     deduplicated by (job_id, channel, offset): identical content is a
     replay, different content is a conflict and aborts the page;
  3. only after the warehouse commit, acknowledge the page at the source
     (which may reclaim the delivered rows).

The two databases are independent files on separate connections: no
ATTACH, no shared transaction.  A crash between steps 2 and 3 is healed
on the next run by the receipt written in step 2, so the target never
gets duplicates and the source acknowledgement position never advances
past a page the target has not committed.

The first delivery binds the job to the warehouse's own persistent
identity (stored in the job store).  Later deliveries to the same
warehouse continue even if the file moved to a different path; a
*different* warehouse is rejected, so already-acknowledged results are
never silently dropped.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from typing import Any, Dict, Optional

from .errors import (DeliveryConflictError, JobError, ResourceError,
                     StoreFormatError)
from .jobstore import CHANNELS, JobStore, _crashpoint, _utcnow, hash_page_rows

WH_FORMAT_VERSION = 1

_WH_SCHEMA = """
CREATE TABLE IF NOT EXISTS wh_meta(
  id INTEGER PRIMARY KEY CHECK(id=1),
  warehouse_id TEXT NOT NULL,
  format_version INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wh_results(
  job_id TEXT NOT NULL,
  offset INTEGER NOT NULL,
  kind TEXT NOT NULL,
  lp TEXT, ls TEXT, rp TEXT, rs TEXT,
  k TEXT, ltime TEXT, rtime TEXT,
  PRIMARY KEY(job_id, offset)
);
CREATE TABLE IF NOT EXISTS wh_late(
  job_id TEXT NOT NULL,
  offset INTEGER NOT NULL,
  side TEXT NOT NULL, partition TEXT NOT NULL, seq_txt TEXT NOT NULL,
  k TEXT, time_txt TEXT NOT NULL, wm_txt TEXT NOT NULL, payload TEXT,
  PRIMARY KEY(job_id, offset)
);
CREATE TABLE IF NOT EXISTS wh_receipt(
  job_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  page_start INTEGER NOT NULL,
  page_end INTEGER NOT NULL,
  rows_hash TEXT NOT NULL,
  received_at TEXT NOT NULL,
  PRIMARY KEY(job_id, channel, page_start, page_end)
);
"""

_WH_TABLES = {"wh_meta", "wh_results", "wh_late", "wh_receipt"}


class SQLiteWarehouse:
    """An independent SQLite delivery target with a persistent identity."""

    def __init__(self, path: str, create: bool = False):
        self._path = path
        nonempty = os.path.exists(path) and os.path.getsize(path) > 0
        if not nonempty and not create:
            raise StoreFormatError(
                f"no warehouse at {path!r} (missing or empty file)")
        try:
            self._conn = sqlite3.connect(path)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.Error as e:
            raise StoreFormatError(
                f"cannot open warehouse {path!r}: {e}") from e
        if not nonempty:
            self._init_new()
        else:
            self._load_and_validate(path)

    def _init_new(self) -> None:
        warehouse_id = uuid.uuid4().hex
        try:
            self._conn.executescript(_WH_SCHEMA)
            self._conn.execute(
                "INSERT INTO wh_meta(id, warehouse_id, format_version,"
                " created_at) VALUES (1,?,?,?)",
                (warehouse_id, WH_FORMAT_VERSION, _utcnow()))
            self._conn.commit()
        except sqlite3.Error as e:
            self._conn.rollback()
            raise ResourceError(str(e)) from e
        self.warehouse_id = warehouse_id

    def _load_and_validate(self, path: str) -> None:
        try:
            tables = {r[0] for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if "wh_meta" not in tables:
                if tables - {"sqlite_sequence"}:
                    raise StoreFormatError(
                        f"{path!r} is not a twostream warehouse "
                        f"(found tables {sorted(tables - {'sqlite_sequence'})})")
                self._init_new()  # >0 bytes but no tables: treat as empty
                return
            row = self._conn.execute(
                "SELECT warehouse_id, format_version FROM wh_meta"
                " WHERE id=1").fetchone()
            if row is None:
                raise StoreFormatError(f"{path!r}: warehouse metadata missing")
            if row[1] != WH_FORMAT_VERSION:
                raise StoreFormatError(
                    f"{path!r}: unsupported warehouse format version "
                    f"{row[1]!r} (this build understands {WH_FORMAT_VERSION})")
            missing = _WH_TABLES - tables
            if missing:
                raise StoreFormatError(
                    f"{path!r}: warehouse is incomplete, missing tables: "
                    f"{sorted(missing)}")
            self.warehouse_id = row[0]
        except sqlite3.DatabaseError as e:
            raise StoreFormatError(
                f"{path!r} is corrupt or not a SQLite database: {e}") from e

    @property
    def path(self) -> str:
        return self._path

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "SQLiteWarehouse":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------ pages
    def get_receipt(self, job_id: str, channel: str, start: int,
                    end: int) -> Optional[str]:
        """The stored content hash for an exactly matching page receipt,
        or None when the page was never committed to this warehouse."""
        row = self._conn.execute(
            "SELECT rows_hash FROM wh_receipt"
            " WHERE job_id=? AND channel=? AND page_start=? AND page_end=?",
            (job_id, channel, start, end)).fetchone()
        return row[0] if row else None

    def write_page(self, job_id: str, channel: str, start: int, end: int,
                   rows) -> None:
        """Write one page's rows plus its receipt in a single warehouse
        transaction.  Rows already present with identical content are
        replays; the same (job_id, channel, offset) identity with
        different content is a conflict and aborts the whole page."""
        table = "wh_results" if channel == "results" else "wh_late"
        rows_hash = hash_page_rows(channel, rows)
        try:
            for offset, rd in rows:
                if channel == "results":
                    vals = (rd["kind"], rd["lp"], rd["ls"], rd["rp"],
                            rd["rs"], rd["k"], rd["ltime"], rd["rtime"])
                    cols = "kind,lp,ls,rp,rs,k,ltime,rtime"
                else:
                    vals = (rd["side"], rd["partition"], rd["seq"], rd["k"],
                            rd["time"], rd["wm"], rd["payload"])
                    cols = "side,partition,seq_txt,k,time_txt,wm_txt,payload"
                existing = self._conn.execute(
                    f"SELECT {cols} FROM {table}"
                    " WHERE job_id=? AND offset=?",
                    (job_id, offset)).fetchone()
                if existing is not None:
                    if tuple(existing) != vals:
                        raise DeliveryConflictError(
                            f"warehouse row ({job_id}, {channel}, {offset}) "
                            f"already exists with different content; "
                            f"refusing to overwrite")
                else:
                    placeholders = ",".join(["?"] * (2 + len(vals)))
                    self._conn.execute(
                        f"INSERT INTO {table}(job_id,offset,{cols})"
                        f" VALUES ({placeholders})",
                        (job_id, offset) + vals)
            rec = self._conn.execute(
                "SELECT rows_hash FROM wh_receipt"
                " WHERE job_id=? AND channel=? AND page_start=? AND page_end=?",
                (job_id, channel, start, end)).fetchone()
            if rec is not None:
                if rec[0] != rows_hash:
                    raise DeliveryConflictError(
                        f"warehouse receipt for ({job_id}, {channel}, "
                        f"{start}..{end}) has different content hash")
            else:
                self._conn.execute(
                    "INSERT INTO wh_receipt(job_id, channel, page_start,"
                    " page_end, rows_hash, received_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (job_id, channel, start, end, rows_hash, _utcnow()))
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    # -------------------------------------------------------- inspection
    def count(self, job_id: str, channel: str) -> int:
        table = "wh_results" if channel == "results" else "wh_late"
        return self._conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE job_id=?",
            (job_id,)).fetchone()[0]

    def iter_rows(self, job_id: str, channel: str):
        """Yield (offset, row-dict) in offset order (verification aid)."""
        if channel == "results":
            cur = self._conn.execute(
                "SELECT offset,kind,lp,ls,rp,rs,k,ltime,rtime"
                " FROM wh_results WHERE job_id=? ORDER BY offset", (job_id,))
            for r in cur:
                yield r[0], {"kind": r[1], "lp": r[2], "ls": r[3],
                             "rp": r[4], "rs": r[5], "k": r[6],
                             "ltime": r[7], "rtime": r[8]}
        else:
            cur = self._conn.execute(
                "SELECT offset,side,partition,seq_txt,k,time_txt,wm_txt,payload"
                " FROM wh_late WHERE job_id=? ORDER BY offset", (job_id,))
            for r in cur:
                yield r[0], {"side": r[1], "partition": r[2], "seq": r[3],
                             "k": r[4], "time": r[5], "wm": r[6],
                             "payload": r[7]}


def deliver_to_warehouse(job: JobStore, warehouse_path: str,
                         page_size: int = 500) -> Dict[str, Any]:
    """Deliver every pending page of both channels to the warehouse at
    `warehouse_path`, acknowledging each page only after the warehouse
    commit.  Safe to re-run after a crash and with no new input; memory
    use is bounded by page_size, not by the result history."""
    job.require_lock()
    if os.path.abspath(warehouse_path) == os.path.abspath(job.path) or (
            os.path.exists(warehouse_path)
            and os.path.samefile(warehouse_path, job.path)):
        raise JobError(
            "the warehouse must be an independent SQLite file, not the "
            "job store itself")
    bound = job.target_binding()
    if bound is not None and not (os.path.exists(warehouse_path)
                                  and os.path.getsize(warehouse_path) > 0):
        raise DeliveryConflictError(
            f"this job is bound to warehouse {bound}, but no warehouse "
            f"exists at {warehouse_path!r}; refusing to silently start a "
            f"new one (already-acknowledged results would be lost)")
    wh = SQLiteWarehouse(warehouse_path, create=(bound is None))
    try:
        if bound is None:
            job.bind_target(wh.warehouse_id)
        elif wh.warehouse_id != bound:
            raise DeliveryConflictError(
                f"this job is bound to warehouse {bound}; the file at "
                f"{warehouse_path!r} is warehouse {wh.warehouse_id}")
        summary: Dict[str, Any] = {
            "job_id": job.job_id, "warehouse_id": wh.warehouse_id,
            "page_size": page_size, "channels": {}}
        for channel in CHANNELS:
            pages = rows = reused = 0
            while True:
                page = job.read_page(channel, page_size)
                if page.empty:
                    break
                existing = wh.get_receipt(job.job_id, channel,
                                          page.start, page.end)
                if existing is not None:
                    if existing != page.rows_hash:
                        raise DeliveryConflictError(
                            f"warehouse receipt for page {page.start}.."
                            f"{page.end} of channel {channel!r} does not "
                            f"match the source content")
                    reused += 1  # target committed before a crash: heal ack
                else:
                    _crashpoint("deliver.pre_target_commit")
                    wh.write_page(job.job_id, channel, page.start, page.end,
                                  [(d.offset, d.row) for d in page.rows])
                    _crashpoint("deliver.post_target_commit")
                job.ack_page(page.credential)
                pages += 1
                rows += len(page.rows)
            summary["channels"][channel] = {
                "pages": pages, "rows": rows, "reused_receipts": reused,
                "acked_offset": job.acked_offset(channel)}
        return summary
    finally:
        wh.close()
