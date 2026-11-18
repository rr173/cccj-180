"""The only way the investigation touches the source warehouse.

Opened ``mode=ro``: the tool cannot write the warehouse rows, receipts or
the source jobs' acknowledged positions; SQLite enforces that.  Reads
always happen inside one deferred read transaction, so the upper bounds
verified for ``P``/``Q`` and the rows actually used agree even while a
collector keeps delivering new results.
"""

from __future__ import annotations

import os
import sqlite3
from urllib.parse import quote

from .errors import InvestigationFormatError, InvestigationInputError

WH_FORMAT_VERSION = 1
_WH_TABLES = {"wh_meta", "wh_results", "wh_late", "wh_receipt"}


class SourceWarehouse:
    """Read-only handle on a warehouse written by ``SQLiteWarehouse``."""

    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise InvestigationFormatError(
                f"no warehouse at {path!r} (missing or empty file)")
        uri = "file:" + quote(self.path) + "?mode=ro"
        try:
            self._conn = sqlite3.connect(uri, uri=True, isolation_level=None)
            self._conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.Error as e:
            raise InvestigationFormatError(
                f"cannot open warehouse {path!r}: {e}") from e
        self._in_snapshot = False
        self._validate()

    def _validate(self) -> None:
        try:
            tables = {r[0] for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if "wh_meta" not in tables:
                raise InvestigationFormatError(
                    f"{self.path!r} is not a twostream warehouse "
                    f"(tables: {sorted(tables)})")
            row = self._conn.execute(
                "SELECT warehouse_id, format_version FROM wh_meta"
                " WHERE id=1").fetchone()
            if row is None:
                raise InvestigationFormatError(
                    f"{self.path!r}: warehouse metadata missing")
            if row[1] != WH_FORMAT_VERSION:
                raise InvestigationFormatError(
                    f"{self.path!r}: unsupported warehouse format version "
                    f"{row[1]!r} (this build understands {WH_FORMAT_VERSION})")
            missing = _WH_TABLES - tables
            if missing:
                raise InvestigationFormatError(
                    f"{self.path!r}: warehouse is incomplete, missing "
                    f"tables: {sorted(missing)}")
            if not isinstance(row[0], str) or not row[0]:
                raise InvestigationFormatError(
                    f"{self.path!r}: corrupt warehouse identity")
            self.warehouse_id = row[0]
        except sqlite3.DatabaseError as e:
            raise InvestigationFormatError(
                f"{self.path!r} is corrupt or not a SQLite database: {e}") from e

    def close(self) -> None:
        if self._in_snapshot:
            try:
                self._conn.execute("COMMIT")
            except sqlite3.Error:
                pass
            self._in_snapshot = False
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --------------------------------------------------------- snapshot
    def begin_snapshot(self) -> None:
        """One deferred read transaction: every later SELECT in this
        handle sees the same committed warehouse state."""
        self._conn.execute("BEGIN")
        self._in_snapshot = True
        self._conn.execute("SELECT 1 FROM wh_meta LIMIT 1").fetchall()

    def end_snapshot(self) -> None:
        if self._in_snapshot:
            self._conn.execute("COMMIT")
            self._in_snapshot = False

    # ------------------------------------------------------------ reads
    def max_offset(self, job_id: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(offset), 0) FROM wh_results WHERE job_id=?",
            (job_id,)).fetchone()
        return int(row[0])

    def count_results(self, job_id: str, upto: int) -> int:
        return int(self._conn.execute(
            "SELECT COUNT(*) FROM wh_results WHERE job_id=? AND offset<=?",
            (job_id, upto)).fetchone()[0])

    def fetch_result(self, job_id: str, offset: int):
        return self._conn.execute(
            "SELECT job_id, offset, kind, lp, ls, rp, rs, k, ltime, rtime"
            " FROM wh_results WHERE job_id=? AND offset=?",
            (job_id, offset)).fetchone()

    def iter_results(self, job_id: str, upto: int):
        """Stream ``(job, offset, kind, lp, ls, rp, rs, k, ltime, rtime)``
        for ``1 <= offset <= upto`` in offset order."""
        cur = self._conn.execute(
            "SELECT job_id, offset, kind, lp, ls, rp, rs, k, ltime, rtime"
            " FROM wh_results WHERE job_id=? AND offset>=1 AND offset<=?"
            " ORDER BY offset", (job_id, upto))
        for row in cur:
            yield tuple(row)

    def verify_contiguous(self, job_id: str, upto: int) -> None:
        """The positions ``1..upto`` must all exist.

        ``(job_id, offset)`` is the warehouse primary key, so a row count
        equality plus ``MAX(offset) == upto`` plus ``MIN(offset) == 1``
        proves there is no hole and no out-of-range position.  The message
        points at the job and the first gap it can name.
        """
        if upto <= 0:
            return
        row = self._conn.execute(
            "SELECT COUNT(*), MIN(offset), MAX(offset) FROM wh_results"
            " WHERE job_id=? AND offset BETWEEN 1 AND ?",
            (job_id, upto)).fetchone()
        n, lo, hi = int(row[0]), row[1], row[2]
        if n == upto and lo == 1 and hi == upto:
            return
        if lo is None:
            raise InvestigationInputError(
                f"job {job_id!r}: requested results up to offset {upto}, but "
                f"the warehouse holds none in 1..{upto} (gap or out of range)")
        if lo != 1:
            raise InvestigationInputError(
                f"job {job_id!r}: results start at offset {lo}, expected 1 "
                f"(gap at or before offset {lo})")
        if n < upto or hi > upto:
            first = self._first_gap(job_id, upto)
            if first is not None:
                raise InvestigationInputError(
                    f"job {job_id!r}: result offsets 1..{upto} contain a gap "
                    f"(first missing offset {first})")
            raise InvestigationInputError(
                f"job {job_id!r}: result offsets 1..{upto} are not contiguous "
                f"(rows {n}, min {lo}, max {hi})")
        raise InvestigationInputError(
            f"job {job_id!r}: result offsets 1..{upto} are not contiguous "
            f"(rows {n}, min {lo}, max {hi})")

    def _first_gap(self, job_id: str, upto: int):
        """First missing offset in 1..upto, found with an index-bounded
        walk that stops as soon as the gap is seen (no full scan)."""
        expected = 1
        cur = self._conn.execute(
            "SELECT offset FROM wh_results WHERE job_id=? AND offset>=1"
            " AND offset<=? ORDER BY offset", (job_id, upto))
        for (offset,) in cur:
            if offset != expected:
                return expected
            expected += 1
        return None if expected <= upto else None
