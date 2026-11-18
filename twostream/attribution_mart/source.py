"""Read-only access to the source warehouse (``wh_meta``/``wh_results``).

The SQLite connection is opened with ``?mode=ro``: the mart cannot write
the warehouse's rows, receipts, or the source jobs' acknowledged
positions.  That is enforced by SQLite, not just convention.

Times are canonical decimal text in the warehouse and are parsed straight
to Python integers here, never through a float or a SQLite 64-bit value.
"""

from __future__ import annotations

import os
import sqlite3
from urllib.parse import quote

from .errors import MartFormatError, MartInputError

WH_FORMAT_VERSION = 1
_WH_TABLES = {"wh_meta", "wh_results", "wh_late", "wh_receipt"}


def parse_dec(text) -> int:
    """Parse an arbitrary-precision decimal integer stored as text."""
    if not isinstance(text, str) or not text:
        raise MartInputError(f"expected decimal integer text, got {text!r}")
    try:
        return int(text, 10)
    except (TypeError, ValueError) as e:
        raise MartInputError(f"not a decimal integer: {text!r}") from e


class SourceWarehouse:
    """A read-only handle on a warehouse written by SQLiteWarehouse."""

    def __init__(self, path: str):
        self.path = path
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise MartFormatError(
                f"no warehouse at {path!r} (missing or empty file)")
        uri = "file:" + quote(os.path.abspath(path)) + "?mode=ro"
        try:
            self._conn = sqlite3.connect(uri, uri=True, isolation_level=None)
            self._conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.Error as e:
            raise MartFormatError(f"cannot open warehouse {path!r}: {e}") from e
        self._in_snapshot = False
        self._validate()

    def _validate(self) -> None:
        try:
            tables = {r[0] for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if "wh_meta" not in tables:
                raise MartFormatError(
                    f"{self.path!r} is not a twostream warehouse "
                    f"(tables: {sorted(tables)})")
            row = self._conn.execute(
                "SELECT warehouse_id, format_version FROM wh_meta"
                " WHERE id=1").fetchone()
            if row is None:
                raise MartFormatError(f"{self.path!r}: warehouse metadata missing")
            if row[1] != WH_FORMAT_VERSION:
                raise MartFormatError(
                    f"{self.path!r}: unsupported warehouse format version "
                    f"{row[1]!r} (this build understands {WH_FORMAT_VERSION})")
            missing = _WH_TABLES - tables
            if missing:
                raise MartFormatError(
                    f"{self.path!r}: warehouse is incomplete, missing "
                    f"tables: {sorted(missing)}")
            self.warehouse_id = row[0]
            if not isinstance(self.warehouse_id, str) or not self.warehouse_id:
                raise MartFormatError(f"{self.path!r}: corrupt warehouse identity")
        except sqlite3.DatabaseError as e:
            raise MartFormatError(
                f"{self.path!r} is corrupt or not a SQLite database: {e}") from e

    def close(self) -> None:
        if self._in_snapshot:
            try:
                self._conn.execute("COMMIT")
            except sqlite3.Error:
                pass
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --------------------------------------------------------- snapshot
    def begin_snapshot(self) -> None:
        """Open one deferred read transaction: every later SELECT sees the
        same committed warehouse state, even while the collector commits
        new results.  The bounds captured here and the rows read for them
        therefore always agree."""
        self._conn.execute("BEGIN")
        self._in_snapshot = True
        # Force the snapshot to be established now (not at first read).
        self._conn.execute("SELECT 1 FROM wh_meta LIMIT 1").fetchall()

    def end_snapshot(self) -> None:
        if self._in_snapshot:
            self._conn.execute("COMMIT")
            self._in_snapshot = False

    def max_offset(self, job_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(offset), 0) FROM wh_results WHERE job_id=?",
            (job_id,)).fetchone()
        return int(row[0])

    def job_ids(self) -> set:
        return {r[0] for r in self._conn.execute(
            "SELECT DISTINCT job_id FROM wh_results")}

    def iter_results(self, job_id: str, after: int, upto: int):
        """Yield ``(offset, kind, lp, ls, rp, rs, k, ltime, rtime)`` for
        ``after < offset <= upto`` in offset order; a streaming cursor."""
        cur = self._conn.execute(
            "SELECT offset, kind, lp, ls, rp, rs, k, ltime, rtime"
            " FROM wh_results WHERE job_id=? AND offset>? AND offset<=?"
            " ORDER BY offset", (job_id, after, upto))
        for r in cur:
            yield tuple(r)

    def verify_contiguous(self, job_id: str, after: int, upto: int) -> None:
        """Refuse an uninterpretable range: the offsets ``(after, upto]``
        must all exist.  A hole means the new data cannot be explained, so
        the sync fails and the previous mart version stays intact instead
        of skipping the gap."""
        if upto <= after:
            return
        n = self._conn.execute(
            "SELECT COUNT(*) FROM wh_results WHERE job_id=? AND offset>?"
            " AND offset<=?", (job_id, after, upto)).fetchone()[0]
        expected = upto - after
        if n != expected:
            raise MartInputError(
                f"job {job_id!r}: result offsets ({after}, {upto}] contain "
                f"a gap ({n} rows, expected {expected}); refusing to skip "
                f"uninterpretable input")

    def fetch_result(self, job_id: str, offset: int) -> dict | None:
        row = self._conn.execute(
            "SELECT offset, kind, lp, ls, rp, rs, k, ltime, rtime"
            " FROM wh_results WHERE job_id=? AND offset=?",
            (job_id, offset)).fetchone()
        if row is None:
            return None
        return {"offset": row[0], "kind": row[1], "lp": row[2], "ls": row[3],
                "rp": row[4], "rs": row[5], "k": row[6], "ltime": row[7],
                "rtime": row[8]}
