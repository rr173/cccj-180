"""Shared fixtures for the shared-response investigation drills.

Everything is generated at run time under
``drills/out/response/<name>/`` and is never committed.  The small
fixtures go through the warehouse's write-page interface; the larger ones
use the real ``JobStore`` + ``deliver_to_warehouse`` path.
"""

from __future__ import annotations

import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interval_join import SQLiteWarehouse  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out",
                   "response")


def fresh_dir(name: str) -> str:
    path = os.path.join(OUT, name)
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def make_warehouse(path: str, jobs) -> str:
    """``jobs`` maps job_id -> list of (offset, row-dict) as produced by
    ``SQLiteWarehouse.write_page``; each job is written as one page."""
    wh = SQLiteWarehouse(path, create=True)
    try:
        for job_id, rows in jobs.items():
            if not rows:
                continue
            wh.write_page(job_id, "results", rows[0][0], rows[-1][0], rows)
    finally:
        wh.close()
    return path


def db_bytes(path: str) -> int:
    total = 0
    for suffix in ("", "-wal"):
        p = path + suffix
        if os.path.exists(p):
            total += os.path.getsize(p)
    return total


def rss_kb() -> int:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmHWM"):
                    return int(line.split()[1])
    except OSError:
        pass
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def check(cond, message):
    if not cond:
        raise AssertionError(message)
    print(f"   ok: {message}")
