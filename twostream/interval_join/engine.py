"""Two-stream interval join engine with SQLite-backed state.

Semantics
---------
Config: integers lo <= hi, mode in {inner, left_outer, full_outer}.
Left event l matches right event r iff both keys are non-null and equal
and  l.time + lo <= r.time <= l.time + hi  (both bounds inclusive).
Every matching identity pair is emitted exactly once, as soon as the
second of the two events arrives.  There is no nearest-neighbour
pairing: equal values under different seq are different events.

Watermarks / finality
---------------------
Each side's proof bound is the minimum effective watermark over its
declared partitions (unset partition = -inf, ended partition = +inf).

  * left event l expires when  right_min_wm > l.time + hi  — after that
    no legal right event can still match it.  On expiry the engine emits
    (l, null) once if l never matched and the mode pads left rows, then
    deletes l regardless of whether it matched.
  * right event r expires symmetrically when  left_min_wm > r.time - lo
    (its left window [r.time-hi, r.time-lo] is closed), padding with
    (null, r) only in full_outer mode.

A side's own watermark never substitutes for the opposite side's proof.
A matched event is NOT reclaimed early: it may still pair with later
legal events, so it stays until the opposite watermark proves expiry.
Null-key events never match; they are only kept where an outer mode
owes them a final null-padded verdict.

State lives in SQLite (time index over an order-preserving encoding of
arbitrary-precision integers).  Emitted results go to a SQLite outbox
and stream out through pull_results(); nothing assembles all matches or
a hot key's candidates in memory.

Durability: by default the seq/watermark/end ledgers are in-memory only
(single-run engine, no crash recovery).  With _durable=True they are
written through to ledger tables inside the ambient transaction and
reloaded from disk on open; the persistent job store
(interval_join.jobstore) builds recoverable jobs on that mode.
"""

from __future__ import annotations

import json
import math
import sqlite3
from typing import Any, Dict, Iterator, List, Optional

from .encoding import enc_int
from .errors import InputError, ResourceError
from .messages import Data, End, Watermark, validate_message
from .results import EventId, Late, LeftPad, Match, RightPad

INF = math.inf  # +infinity watermark of an ended partition (sentinel only)

_MODES = ("inner", "left_outer", "full_outer")

_BATCH = 100  # rows per executemany batch (keeps SQLite var count < 999)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS left_state(
  partition TEXT NOT NULL,
  seq_txt   TEXT NOT NULL,
  k         TEXT,
  time_txt  TEXT NOT NULL,
  tkey      TEXT NOT NULL,
  payload   TEXT,
  PRIMARY KEY(partition, seq_txt)
);
CREATE INDEX IF NOT EXISTS idx_left_k_tkey ON left_state(k, tkey);
CREATE INDEX IF NOT EXISTS idx_left_tkey   ON left_state(tkey);
CREATE TABLE IF NOT EXISTS right_state(
  partition TEXT NOT NULL,
  seq_txt   TEXT NOT NULL,
  k         TEXT,
  time_txt  TEXT NOT NULL,
  tkey      TEXT NOT NULL,
  payload   TEXT,
  PRIMARY KEY(partition, seq_txt)
);
CREATE INDEX IF NOT EXISTS idx_right_k_tkey ON right_state(k, tkey);
CREATE INDEX IF NOT EXISTS idx_right_tkey   ON right_state(tkey);
-- Append-only match ledger: which stored events ever matched.  Event
-- rows themselves are never updated in place.  tkey is duplicated here
-- so the ledger is reclaimed by the same time proof as the state.
CREATE TABLE IF NOT EXISTS matched(
  side      TEXT NOT NULL,
  partition TEXT NOT NULL,
  seq_txt   TEXT NOT NULL,
  tkey      TEXT NOT NULL,
  PRIMARY KEY(side, partition, seq_txt)
);
CREATE INDEX IF NOT EXISTS idx_matched_side_tkey ON matched(side, tkey);
CREATE TABLE IF NOT EXISTS outbox(
  id    INTEGER PRIMARY KEY AUTOINCREMENT,
  kind  TEXT NOT NULL,            -- 'match' | 'lpad' | 'rpad'
  lp    TEXT, ls TEXT, rp TEXT, rs TEXT,
  k     TEXT, ltime TEXT, rtime TEXT
);
CREATE TABLE IF NOT EXISTS latebox(
  id    INTEGER PRIMARY KEY AUTOINCREMENT,
  side  TEXT NOT NULL, partition TEXT NOT NULL, seq_txt TEXT NOT NULL,
  k     TEXT, time_txt TEXT NOT NULL, wm_txt TEXT NOT NULL, payload TEXT
);
"""

# Durable per-partition ledgers (created only when the engine runs with
# _durable=True).  Values are decimal text so arbitrary-precision seqs and
# watermarks survive persistence exactly (never floats).
_LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS seq_ledger(
  side TEXT NOT NULL, partition TEXT NOT NULL, last_seq_txt TEXT NOT NULL,
  PRIMARY KEY(side, partition)
);
CREATE TABLE IF NOT EXISTS wm_ledger(
  side TEXT NOT NULL, partition TEXT NOT NULL, wm_txt TEXT NOT NULL,
  PRIMARY KEY(side, partition)
);
CREATE TABLE IF NOT EXISTS end_ledger(
  side TEXT NOT NULL, partition TEXT NOT NULL,
  PRIMARY KEY(side, partition)
);
"""


def _validate_config(lo, hi, mode, left_partitions, right_partitions):
    """Validate a join configuration.  Returns (lp, rp) as lists.
    Raises ValueError on any violation."""
    if isinstance(lo, bool) or isinstance(hi, bool) \
            or not isinstance(lo, int) or not isinstance(hi, int):
        raise ValueError("lo and hi must be integers")
    if lo > hi:
        raise ValueError(f"lo ({lo}) must be <= hi ({hi})")
    if mode not in _MODES:
        raise ValueError(f"mode must be one of {_MODES}, got {mode!r}")
    lp = list(left_partitions)
    rp = list(right_partitions)
    if not lp or not rp:
        raise ValueError("each side must declare at least one partition")
    for p in lp + rp:
        if not isinstance(p, str) or not p:
            raise ValueError(f"partition names must be non-empty strings, got {p!r}")
    if len(set(lp)) != len(lp) or len(set(rp)) != len(rp):
        raise ValueError("duplicate partition names within a side")
    return lp, rp


def _dump_payload(p: Any) -> Optional[str]:
    if p is None:
        return None
    if isinstance(p, str):
        return p
    try:
        return json.dumps(p, ensure_ascii=False)
    except (TypeError, ValueError):
        return repr(p)


class JoinEngine:
    """Streaming two-stream interval join.  See module docstring."""

    def __init__(self, db_path: str, lo: int, hi: int, mode: str,
                 left_partitions, right_partitions, *,
                 _conn: Optional[sqlite3.Connection] = None,
                 _durable: bool = False):
        lp, rp = _validate_config(lo, hi, mode, left_partitions,
                                  right_partitions)
        self.lo, self.hi, self.mode = lo, hi, mode
        self.left_partitions = lp
        self.right_partitions = rp
        self._owns_conn = _conn is None
        self._durable = _durable
        try:
            self._conn = _conn if _conn is not None else sqlite3.connect(db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA)
            if _durable:
                self._conn.executescript(_LEDGER_SCHEMA)
            self._conn.commit()
        except sqlite3.Error as e:
            raise ResourceError(f"cannot open state database {db_path!r}: {e}") from e
        # In-memory bookkeeping (event state itself is on disk).  In the
        # default single-run mode the seq/watermark/end ledgers live only
        # here; in durable mode they are written through to the ledger
        # tables inside the ambient transaction and reloaded from disk by
        # reload_ledgers() (open time / lock acquisition / rollback).
        self._wm: Dict[tuple, Optional[int]] = {}     # (side,part) -> None(-inf) | int
        self._ended: Dict[tuple, bool] = {}
        self._last_seq: Dict[tuple, int] = {}
        self._failed = False
        self.reload_ledgers()

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        if self._owns_conn:
            self._conn.close()

    # ---------------------------------------------------- durable ledgers
    def reload_ledgers(self) -> None:
        """(Re)load the seq/watermark/end ledgers from the database,
        discarding the in-memory copies.  No-op unless durable."""
        self._wm = {}
        self._ended = {}
        self._last_seq = {}
        if not self._durable:
            return
        for side, part, txt in self._conn.execute(
                "SELECT side, partition, last_seq_txt FROM seq_ledger"):
            self._last_seq[(side, part)] = int(txt)
        for side, part, txt in self._conn.execute(
                "SELECT side, partition, wm_txt FROM wm_ledger"):
            self._wm[(side, part)] = int(txt)
        for side, part in self._conn.execute(
                "SELECT side, partition FROM end_ledger"):
            self._ended[(side, part)] = True

    def snapshot_ledgers(self):
        """In-memory snapshot of the seq/watermark/end ledgers, for
        embedders that roll a failed multi-message transaction back."""
        return (dict(self._wm), dict(self._ended), dict(self._last_seq))

    def restore_ledgers(self, snap) -> None:
        """Undo a snapshot_ledgers() snapshot (after a transaction
        rollback discarded the corresponding durable writes)."""
        wm, ended, last_seq = snap
        self._wm = dict(wm)
        self._ended = dict(ended)
        self._last_seq = dict(last_seq)

    def apply_message(self, msg) -> None:
        """Dispatch one message inside the *caller's* transaction.

        Unlike feed()/feed_batch() this neither commits nor latches the
        engine into the failed state on InputError; embedders managing
        their own transactions (the persistent job store) combine it with
        snapshot_ledgers()/restore_ledgers() to roll a failed batch back.
        """
        self._dispatch(msg)

    def __enter__(self) -> "JoinEngine":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -------------------------------------------------------------- feed
    def feed(self, msg) -> None:
        """Process one message.  Raises InputError / ResourceError; either
        one fails the engine (status 'failed', further feeds rejected)."""
        if self._failed:
            raise InputError("engine is in failed state; refusing further input")
        try:
            with self._conn:  # one transaction per message
                self._dispatch(msg)
        except InputError:
            self._failed = True
            raise
        except sqlite3.Error as e:
            self._failed = True
            raise ResourceError(str(e)) from e

    def feed_batch(self, messages) -> None:
        """All-or-nothing batch feed inside a single transaction."""
        if self._failed:
            raise InputError("engine is in failed state; refusing further input")
        try:
            with self._conn:
                for m in messages:
                    self._dispatch(m)
        except InputError:
            self._failed = True
            raise
        except sqlite3.Error as e:
            self._failed = True
            raise ResourceError(str(e)) from e

    # Convenience feeders -------------------------------------------------
    def feed_data(self, side, partition, seq, key, time, payload=None) -> None:
        self.feed(Data(side, partition, seq, key, time, payload))

    def feed_watermark(self, side, partition, w) -> None:
        self.feed(Watermark(side, partition, w))

    def feed_end(self, side, partition) -> None:
        self.feed(End(side, partition))

    # ----------------------------------------------------------- dispatch
    def _dispatch(self, msg) -> None:
        validate_message(msg)
        if isinstance(msg, Data):
            self._on_data(msg)
        elif isinstance(msg, Watermark):
            self._on_watermark(msg)
        elif isinstance(msg, End):
            self._on_end(msg)

    def _check_partition(self, side: str, partition: str) -> None:
        parts = self.left_partitions if side == "L" else self.right_partitions
        if partition not in parts:
            raise InputError(f"undeclared partition {side}/{partition}")
        if self._ended.get((side, partition)):
            raise InputError(f"partition {side}/{partition} already ended")

    def _on_data(self, m: Data) -> None:
        self._check_partition(m.side, m.partition)
        pk = (m.side, m.partition)
        last = self._last_seq.get(pk)
        if last is not None and m.seq <= last:
            raise InputError(
                f"{m.side}/{m.partition}: seq {m.seq} not strictly increasing "
                f"(already saw {last})")
        self._last_seq[pk] = m.seq
        if self._durable:
            self._conn.execute(
                "INSERT OR REPLACE INTO seq_ledger(side,partition,last_seq_txt)"
                " VALUES (?,?,?)", (m.side, m.partition, str(m.seq)))
        wm = self._wm.get(pk)
        if wm is not None and m.time < wm:
            # late bypass: recorded, never joined, never retracts anything
            self._conn.execute(
                "INSERT INTO latebox(side,partition,seq_txt,k,time_txt,wm_txt,payload)"
                " VALUES (?,?,?,?,?,?,?)",
                (m.side, m.partition, str(m.seq), m.key, str(m.time), str(wm),
                 _dump_payload(m.payload)))
            return
        self._arrive(m)

    def _on_watermark(self, m: Watermark) -> None:
        self._check_partition(m.side, m.partition)
        pk = (m.side, m.partition)
        cur = self._wm.get(pk)
        if cur is not None and m.w < cur:
            raise InputError(
                f"{m.side}/{m.partition}: watermark regression {m.w} < {cur}")
        self._wm[pk] = m.w
        if self._durable:
            self._conn.execute(
                "INSERT OR REPLACE INTO wm_ledger(side,partition,wm_txt)"
                " VALUES (?,?,?)", (m.side, m.partition, str(m.w)))
        self._expire_opposite(m.side)

    def _on_end(self, m: End) -> None:
        self._check_partition(m.side, m.partition)
        self._ended[(m.side, m.partition)] = True  # effective watermark = +inf
        if self._durable:
            self._conn.execute(
                "INSERT OR IGNORE INTO end_ledger(side,partition) VALUES (?,?)",
                (m.side, m.partition))
        self._expire_opposite(m.side)

    # -------------------------------------------------------------- join
    def _arrive(self, m: Data) -> None:
        side = m.side
        if side == "L":
            opp_table, opp_side = "right_state", "R"
            win_lo, win_hi = m.time + self.lo, m.time + self.hi
            own_table = "left_state"
            pad_allowed = self.mode in ("left_outer", "full_outer")
            opp_min = self._side_min("R")
            expired = opp_min is not None and (opp_min == INF
                                               or m.time + self.hi < opp_min)
        else:
            opp_table, opp_side = "left_state", "L"
            win_lo, win_hi = m.time - self.hi, m.time - self.lo
            own_table = "right_state"
            pad_allowed = self.mode == "full_outer"
            opp_min = self._side_min("L")
            expired = opp_min is not None and (opp_min == INF
                                               or m.time - self.lo < opp_min)

        out_batch: List[tuple] = []
        mark_batch: List[tuple] = []
        nmatch = 0
        if m.key is not None:
            cur = self._conn.execute(
                f"SELECT partition, seq_txt, time_txt, tkey FROM {opp_table}"
                f" WHERE k=? AND tkey BETWEEN ? AND ?",
                (m.key, enc_int(win_lo), enc_int(win_hi)))
            for op, oseq, ott, otkey in cur:
                if side == "L":
                    out_batch.append(("match", m.partition, str(m.seq), op, oseq,
                                      m.key, str(m.time), ott))
                else:
                    out_batch.append(("match", op, oseq, m.partition, str(m.seq),
                                      m.key, ott, str(m.time)))
                if self.mode != "inner":
                    mark_batch.append((opp_side, op, oseq, otkey))
                nmatch += 1
                if len(out_batch) >= _BATCH:
                    self._flush_matches(out_batch, mark_batch)
            if out_batch:
                self._flush_matches(out_batch, mark_batch)

        # Keep null-key events only where an outer mode owes them a verdict.
        keep_null = (side == "L" and self.mode in ("left_outer", "full_outer")) \
            or (side == "R" and self.mode == "full_outer")
        if not expired and (m.key is not None or keep_null):
            self._conn.execute(
                f"INSERT INTO {own_table}(partition,seq_txt,k,time_txt,tkey,payload)"
                f" VALUES (?,?,?,?,?,?)",
                (m.partition, str(m.seq), m.key, str(m.time), enc_int(m.time),
                 _dump_payload(m.payload)))
            if nmatch and self.mode != "inner":
                self._conn.execute(
                    "INSERT OR IGNORE INTO matched(side,partition,seq_txt,tkey)"
                    " VALUES (?,?,?,?)",
                    (side, m.partition, str(m.seq), enc_int(m.time)))
        elif expired and nmatch == 0 and pad_allowed:
            # Already proven unmatchable on arrival (e.g. opposite side
            # ended): emit the final verdict immediately, store nothing.
            self._emit_pad(side, m)

    def _flush_matches(self, out_batch: List[tuple], mark_batch: List[tuple]) -> None:
        self._conn.executemany(
            "INSERT INTO outbox(kind,lp,ls,rp,rs,k,ltime,rtime)"
            " VALUES (?,?,?,?,?,?,?,?)", out_batch)
        if mark_batch:
            self._conn.executemany(
                "INSERT OR IGNORE INTO matched(side,partition,seq_txt,tkey)"
                " VALUES (?,?,?,?)", mark_batch)
        out_batch.clear()
        mark_batch.clear()

    def _emit_pad(self, side: str, m: Data) -> None:
        if side == "L":
            row = ("lpad", m.partition, str(m.seq), None, None,
                   m.key, str(m.time), None)
        else:
            row = ("rpad", None, None, m.partition, str(m.seq),
                   m.key, None, str(m.time))
        self._conn.execute(
            "INSERT INTO outbox(kind,lp,ls,rp,rs,k,ltime,rtime)"
            " VALUES (?,?,?,?,?,?,?,?)", row)

    # ---------------------------------------------------------- expiry/GC
    def _side_min(self, side: str):
        """Min effective watermark of a side: None = -inf, INF = +inf."""
        parts = self.left_partitions if side == "L" else self.right_partitions
        mn = INF
        for p in parts:
            pk = (side, p)
            if self._ended.get(pk):
                continue  # +inf, never the minimum
            w = self._wm.get(pk)
            if w is None:
                return None  # a silent partition pins the side to -inf
            if w < mn:
                mn = w
        return mn

    def _expire_opposite(self, advanced_side: str) -> None:
        # A side's watermark proof expires the *opposite* side's state.
        self._expire_side("L" if advanced_side == "R" else "R")

    def _expire_side(self, side: str) -> None:
        opp_min = self._side_min("R" if side == "L" else "L")
        if opp_min is None:
            return
        if opp_min == INF:
            threshold = None  # expire everything
        elif side == "L":
            threshold = enc_int(opp_min - self.hi)   # l.time + hi < opp_min
        else:
            threshold = enc_int(opp_min + self.lo)   # r.time - lo < opp_min
        self._expire(side, threshold)

    def _expire(self, side: str, tkey_threshold: Optional[str]) -> None:
        table = "left_state" if side == "L" else "right_state"
        want_pad = (side == "L" and self.mode in ("left_outer", "full_outer")) \
            or (side == "R" and self.mode == "full_outer")
        cond = "tkey < ?" if tkey_threshold is not None else "1=1"
        params = (tkey_threshold,) if tkey_threshold is not None else ()
        if want_pad:
            cur = self._conn.execute(
                f"SELECT partition, seq_txt, k, time_txt FROM {table}"
                f" WHERE {cond} AND NOT EXISTS("
                f"   SELECT 1 FROM matched m WHERE m.side=?"
                f"   AND m.partition={table}.partition AND m.seq_txt={table}.seq_txt)",
                params + (side,))
            batch = []
            for p, s, k, tt in cur:
                if side == "L":
                    batch.append(("lpad", p, s, None, None, k, tt, None))
                else:
                    batch.append(("rpad", None, None, p, s, k, None, tt))
                if len(batch) >= _BATCH:
                    self._conn.executemany(
                        "INSERT INTO outbox(kind,lp,ls,rp,rs,k,ltime,rtime)"
                        " VALUES (?,?,?,?,?,?,?,?)", batch)
                    batch.clear()
            if batch:
                self._conn.executemany(
                    "INSERT INTO outbox(kind,lp,ls,rp,rs,k,ltime,rtime)"
                    " VALUES (?,?,?,?,?,?,?,?)", batch)
        self._conn.execute(f"DELETE FROM {table} WHERE {cond}", params)
        self._conn.execute(f"DELETE FROM matched WHERE side=? AND {cond}",
                           (side,) + params)

    # --------------------------------------------------------------- pull
    def pull_results(self, limit: Optional[int] = None,
                     chunk_size: int = 1000) -> Iterator:
        """Drain business results (Match / LeftPad / RightPad) in emission
        order.  Rows are deleted once yielded; if the iterator is closed
        early, only the consumed prefix is deleted (exactly-once per row).
        """
        last = -1
        n = 0
        try:
            while True:
                rows = self._conn.execute(
                    "SELECT id,kind,lp,ls,rp,rs,k,ltime,rtime FROM outbox"
                    " WHERE id>? ORDER BY id LIMIT ?", (last, chunk_size)
                ).fetchall()
                if not rows:
                    return
                for r in rows:
                    last = r[0]
                    n += 1
                    yield self._mk_result(r)
                    if limit is not None and n >= limit:
                        return
        finally:
            if last >= 0:
                with self._conn:
                    self._conn.execute("DELETE FROM outbox WHERE id<=?", (last,))

    def pull_late(self, limit: Optional[int] = None,
                  chunk_size: int = 1000) -> Iterator[Late]:
        """Drain late-bypass records (same drain semantics as pull_results)."""
        last = -1
        n = 0
        try:
            while True:
                rows = self._conn.execute(
                    "SELECT id,side,partition,seq_txt,k,time_txt,wm_txt"
                    " FROM latebox WHERE id>? ORDER BY id LIMIT ?",
                    (last, chunk_size)).fetchall()
                if not rows:
                    return
                for r in rows:
                    last = r[0]
                    n += 1
                    yield Late(EventId(r[1], r[2], int(r[3])), r[4],
                               int(r[5]), int(r[6]))
                    if limit is not None and n >= limit:
                        return
        finally:
            if last >= 0:
                with self._conn:
                    self._conn.execute("DELETE FROM latebox WHERE id<=?", (last,))

    @staticmethod
    def _mk_result(row):
        _id, kind, lp, ls, rp, rs, k, lt, rt = row
        if kind == "match":
            return Match(EventId("L", lp, int(ls)), EventId("R", rp, int(rs)),
                         k, int(lt), int(rt))
        if kind == "lpad":
            return LeftPad(EventId("L", lp, int(ls)), k, int(lt))
        return RightPad(EventId("R", rp, int(rs)), k, int(rt))

    # ------------------------------------------------------------- status
    def watermarks(self) -> Dict[str, Any]:
        def view(side, parts):
            return {p: {"watermark": self._wm.get((side, p)),
                        "ended": bool(self._ended.get((side, p)))}
                    for p in parts}

        return {
            "left": view("L", self.left_partitions),
            "right": view("R", self.right_partitions),
            "left_min": self._side_min("L"),    # None=-inf, math.inf=+inf
            "right_min": self._side_min("R"),
        }

    def pending(self) -> Dict[str, int]:
        c = self._conn
        one = lambda q: c.execute(q).fetchone()[0]
        return {
            "left_state": one("SELECT COUNT(*) FROM left_state"),
            "right_state": one("SELECT COUNT(*) FROM right_state"),
            "matched_marks": one("SELECT COUNT(*) FROM matched"),
            "outbox": one("SELECT COUNT(*) FROM outbox"),
            "late": one("SELECT COUNT(*) FROM latebox"),
        }

    @property
    def status(self) -> str:
        """'running' | 'finished' | 'failed'.

        'finished' requires every declared partition on BOTH sides ended
        (which by the expiry invariant drains all state).  Temporarily
        having no pending rows is NOT completion.
        """
        if self._failed:
            return "failed"
        all_ended = all(self._ended.get(("L", p)) for p in self.left_partitions) \
            and all(self._ended.get(("R", p)) for p in self.right_partitions)
        return "finished" if all_ended else "running"
