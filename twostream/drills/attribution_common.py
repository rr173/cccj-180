"""Shared fixtures for the attribution-mart drills.

Everything here is generated at run time under ``drills/out/attribution/``
and is never committed.  The helpers build real ``JobStore`` jobs, run the
real join engine, deliver through the real ``deliver_to_warehouse``
protocol and then exercise the mart -- no shortcuts that bypass the
production path.
"""

from __future__ import annotations

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interval_join import JobStore, deliver_to_warehouse  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out",
                   "attribution")


def fresh_dir(name: str) -> str:
    """A clean directory for one drill (removes any previous run)."""
    path = os.path.join(OUT, name)
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


def d(side, part, seq, key, time):
    return {"type": "data", "side": side, "partition": part, "seq": seq,
            "key": key, "time": time}


def w(side, part, wm):
    return {"type": "watermark", "side": side, "partition": part, "w": wm}


def e(side, part):
    return {"type": "end", "side": side, "partition": part}


def make_job(path, lo, hi, mode="full_outer",
             left=("lp0",), right=("rp0",)):
    return JobStore.create(path, lo=lo, hi=hi, mode=mode,
                           left_partitions=list(left),
                           right_partitions=list(right))


def feed(job, batches, start_no=1):
    """Submit already-constructed batches (each a message list)."""
    receipts = []
    with job.modify():
        for i, msgs in enumerate(batches, start_no):
            receipts.append(job.submit_batch(
                {"batch_no": i, "messages": msgs}))
    return receipts


def deliver(job, warehouse, page_size=200):
    with job.modify():
        return deliver_to_warehouse(job, warehouse, page_size=page_size)


def append_more(job, batches):
    """Append further batches after earlier ones were delivered."""
    last = job.status()["batches"]["last"]
    return feed(job, batches, start_no=last + 1)


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


def db_bytes(path: str) -> int:
    total = 0
    for suffix in ("", "-wal"):
        p = path + suffix
        if os.path.exists(p):
            total += os.path.getsize(p)
    return total


def show(title, obj):
    print(f"-- {title}")
    print(json.dumps(obj, indent=2, ensure_ascii=False, default=str))


def check(cond, message):
    if not cond:
        raise AssertionError(message)
    print(f"   ok: {message}")
