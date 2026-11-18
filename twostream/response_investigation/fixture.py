"""Hand-computable fixtures shared by the drills and the tests.

The small fixture is placed through the warehouse's *write-page* interface
(``SQLiteWarehouse.write_page``), exactly the path a real delivery uses for
one page.  The larger fixtures go through ``JobStore`` batches and
``deliver_to_warehouse``.
"""

from __future__ import annotations

import os
import sqlite3

from interval_join import SQLiteWarehouse

KEY = "dev-shared"


def _clean(path):
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


def _row(kind, lp=None, ls=None, rp=None, rs=None, k=KEY,
         ltime=None, rtime=None):
    return {"kind": kind, "lp": lp, "ls": ls, "rp": rp, "rs": rs, "k": k,
            "ltime": ltime, "rtime": rtime}


# The brief's fixture (one job, nine results):
#   1 A-X   2 B-X   3 C-Y   4 D-Y   5 E-Y
#   6 B-Y   7 A-Y   8 F-Z   9 lpad(G)
_HAND = [
    (1, _row("match", "lp", "1", "rp", "1", ltime="100", rtime="101")),
    (2, _row("match", "lp", "2", "rp", "1", ltime="100", rtime="101")),
    (3, _row("match", "lp", "3", "rp", "2", ltime="100", rtime="101")),
    (4, _row("match", "lp", "4", "rp", "2", ltime="100", rtime="101")),
    (5, _row("match", "lp", "5", "rp", "2", ltime="100", rtime="101")),
    (6, _row("match", "lp", "2", "rp", "2", ltime="100", rtime="101")),
    (7, _row("match", "lp", "1", "rp", "2", ltime="100", rtime="101")),
    (8, _row("match", "lp", "6", "rp", "3", ltime="100", rtime="101")),
    (9, _row("lpad", "lp", "7", None, None, ltime="100")),
]


def build_hand(base, job_id="hand-job"):
    """Place the nine rows through the warehouse write-page interface."""
    os.makedirs(base, exist_ok=True)
    wh_path = os.path.join(base, "warehouse.db")
    _clean(wh_path)
    wh = SQLiteWarehouse(wh_path, create=True)
    try:
        wh.write_page(job_id, "results", 1, 9, _HAND)
    finally:
        wh.close()
    return {"warehouse": wh_path, "job_id": job_id}


def warehouse_rows(wh_path, job_id):
    conn = sqlite3.connect("file:" + wh_path + "?mode=ro", uri=True)
    try:
        return conn.execute(
            "SELECT offset, kind, lp, ls, rp, rs, k, ltime, rtime"
            " FROM wh_results WHERE job_id=? ORDER BY offset",
            (job_id,)).fetchall()
    finally:
        conn.close()


def hand_identities(job_id):
    """A..G as request identities (all left partition ``lp``)."""
    return {name: {"job_id": job_id, "partition": "lp", "seq": i + 1}
            for i, name in enumerate("ABCDEFG")}
