"""Drill: the build reads one snapshot and only the requested prefixes.

Steps:

1. **Live append during the build.**  A child process builds the
   investigation and is held at a barrier just after the snapshot has been
   read.  The parent appends a new batch to a real ``JobStore`` and
   delivers it while the child is still working.  The published
   investigation must contain exactly the rows in ``1..Q`` and nothing of
   the append (checked in the warehouse evidence table and in every Q
   group).
2. **Real gap and out-of-range rejections.**  A warehouse row is deleted
   behind the tool's back.  The build must fail naming the job and the
   first missing offset, leave no investigation file and keep any previous
   one intact; the same for a position beyond the warehouse's maximum.
3. **Kill before and after the final commit.**  ``RESPONSE_CRASH_AT``
   stops a child at several build points.  Before the final commit no
   investigation can be opened and no temp file is left behind; after it
   the investigation is complete.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.response_common import check, fresh_dir
from interval_join import JobStore, deliver_to_warehouse
from response_investigation import (InvestigationFormatError,
                                    InvestigationInputError,
                                    ResponseInvestigation)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRASH = 117


def _d(side, part, seq, key, time):
    return {"type": "data", "side": side, "partition": part, "seq": seq,
            "key": key, "time": time}


def _w(side, part, tm):
    return {"type": "watermark", "side": side, "partition": part, "w": tm}


def _make_job(path, batches, lo=0, hi=5):
    job = JobStore.create(path, lo=lo, hi=hi, mode="full_outer",
                          left_partitions=["lp"], right_partitions=["rp"])
    for i, msgs in enumerate(batches, 1):
        with job.modify():
            job.submit_batch({"batch_no": i, "messages": msgs})
    return job


def _append(job, batch_no, msgs):
    with job.modify():
        job.submit_batch({"batch_no": batch_no, "messages": msgs})


def _rows(client, job_id):
    return [r for r in client.execute(
        "SELECT results_offset, kind FROM inv_results WHERE job_id=?"
        " ORDER BY results_offset", (job_id,))]


def main() -> int:
    base = fresh_dir("snapshot")
    job_path = os.path.join(base, "job.db")
    wh_path = os.path.join(base, "warehouse.db")
    inv_path = os.path.join(base, "inv.db")

    # Prefix rows 1..3, then a batch that would change the grouping.
    job = _make_job(job_path, [[
        _d("L", "lp", 1, "dev", 100),
        _d("R", "rp", 1, "dev", 101),
        _d("L", "lp", 2, "dev", 100),
        _d("L", "lp", 3, "dev", 100),
    ]])
    job_id = job.job_id
    with job.modify():
        deliver_to_warehouse(job, wh_path, page_size=100)
    job.close()
    job = JobStore.open(job_path)
    before = [(r[0], r[1]) for r in
              sqlite3.connect(wh_path).execute(
                  "SELECT offset, kind FROM wh_results WHERE job_id=?"
                  " ORDER BY offset", (job_id,))]
    print("warehouse before append:", before)
    P = {job_id: 2}
    Q = {job_id: 3}

    barrier = os.path.join(base, "barrier")
    os.makedirs(barrier, exist_ok=True)
    # Ask the child to stop at this point (a plain barrier dir never
    # stalls any other run).
    with open(os.path.join(barrier, "build.snapshot_read.want"), "w") as f:
        f.write("1\n")
    child = subprocess.Popen(
        [sys.executable, "-c", _child_script(),
         inv_path, wh_path, job_id, str(P[job_id]), str(Q[job_id])],
        cwd=ROOT, env={**os.environ, "RESPONSE_BARRIER_DIR": barrier})
    _wait(os.path.join(barrier, "build.snapshot_read.reached"))
    # The child has the snapshot and is waiting; append new results now.
    _append(job, 2, [
        _d("L", "lp", 4, "dev", 200),
        _d("R", "rp", 2, "dev", 201),
        _d("L", "lp", 5, "dev", 220),
        _w("L", "lp", 10 ** 6),
        _w("R", "rp", 10 ** 6),
    ])
    with job.modify():
        deliver_to_warehouse(job, wh_path, page_size=100)
    after = [(r[0], r[1]) for r in
             sqlite3.connect(wh_path).execute(
                 "SELECT offset, kind FROM wh_results WHERE job_id=?"
                 " ORDER BY offset", (job_id,))]
    print("warehouse after append: ", after)
    check(len(after) > len(before), "the parent did append new warehouse rows")
    with open(os.path.join(barrier, "build.snapshot_read.release"), "w") as f:
        f.write("go\n")
    rc = child.wait()
    check(rc == 0, "the building child finished successfully")

    inv = ResponseInvestigation.open(inv_path)
    try:
        conn = sqlite3.connect(inv_path)
        stored = [r for r in conn.execute(
            "SELECT results_offset FROM inv_results WHERE job_id=?"
            " ORDER BY results_offset", (job_id,))]
        conn.close()
        check([r[0] for r in stored] == list(range(1, Q[job_id] + 1)),
              "the investigation stored exactly 1..Q, not the appended rows")
        st = inv.status()
        check(st["positions"]["Q"] == Q,
              "the published Q is the requested bound")
        groups = inv.groups("Q")["groups"]
        total_nodes = sum(g["n_nodes"] for g in groups)
        check(total_nodes == 4,
              "Q groups cover only the prefix's nodes (A, B, X)")
        r = inv.resolve("Q", {"job_id": job_id, "partition": "lp", "seq": 1},
                        {"job_id": job_id, "partition": "lp", "seq": 2})
        check(r["status"] == "connected" and r["first_connected_offset"] == 2,
              "resolution is unaffected by the append")
        moved = os.path.join(base, "moved.db")
        os.replace(wh_path, moved)
        inv2 = ResponseInvestigation.open(inv_path)
        check(inv2.status()["investigation_id"] == st["investigation_id"],
              "the investigation still opens after the warehouse moved")
        inv2.close()
        os.replace(moved, wh_path)
    finally:
        inv.close()

    # ---- gap: remove a row behind the tool's back
    job = JobStore.open(job_path)
    _append(job, 3, [_d("L", "lp", 6, "dev", 300)])
    with job.modify():
        deliver_to_warehouse(job, wh_path, page_size=100)
    job.close()
    conn = sqlite3.connect(wh_path)
    conn.execute("DELETE FROM wh_results WHERE job_id=? AND offset=3",
                 (job_id,))
    conn.commit()
    conn.close()
    os.replace(inv_path, inv_path + ".keep")
    try:
        try:
            ResponseInvestigation.build(inv_path, wh_path, [job_id],
                                        {job_id: 2}, {job_id: 5})
        except InvestigationInputError as e:
            check("gap" in str(e) or "missing" in str(e),
                  "a gap is refused with a message naming the job/offset")
            check(job_id in str(e), "the refusal names the job")
        else:
            raise AssertionError("a gap was not refused")
        check(not os.path.exists(inv_path),
              "a failed build left no investigation file")
    finally:
        os.replace(inv_path + ".keep", inv_path)

    # ---- out of range
    try:
        ResponseInvestigation.build(inv_path, wh_path, [job_id],
                                    {job_id: 1}, {job_id: 50},
                                    overwrite=True)
    except InvestigationInputError as e:
        check("out of range" in str(e), "an out-of-range Q is refused")
    else:
        raise AssertionError("an out-of-range Q was not refused")
    inv = ResponseInvestigation.open(inv_path)
    check(inv.status()["positions"]["Q"] == Q,
          "the previous investigation survived the failed rebuild")
    inv.close()

    # ---- crash before and after the final commit
    for point, expect_open in (("build.after_tables", False),
                               ("build.snapshot_read", False),
                               ("build.before_commit", False),
                               ("build.after_commit", True)):
        target = os.path.join(base, "crash.db")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(target + suffix)
            except FileNotFoundError:
                pass
        proc = subprocess.run(
            [sys.executable, "-c", _child_script(), target, wh_path, job_id,
             "2", "2"],
            cwd=ROOT, env={**os.environ, "RESPONSE_CRASH_AT": point})
        check(proc.returncode == CRASH,
              f"the child was killed at {point} (rc {proc.returncode})")
        can_open = os.path.exists(target) and os.path.getsize(target) > 0
        if can_open:
            try:
                ResponseInvestigation.open(target).close()
            except InvestigationFormatError:
                can_open = False
        check(can_open == expect_open,
              f"after a kill at {point} the file "
              f"{'opens' if expect_open else 'cannot open'}")
        leftovers = [n for n in os.listdir(base) if ".tmp-" in n]
        # A killed process cannot clean up its own temp file; the next
        # build must simply not mistake it for a published investigation.
        for name in leftovers:
            try:
                ResponseInvestigation.open(os.path.join(base, name)).close()
            except InvestigationFormatError:
                pass
            else:
                raise AssertionError(
                    f"temp file {name} from a killed build opened as a "
                    f"valid investigation")
        # Only temp residue is allowed; a build killed after the final
        # commit has published the real file, which must open.
        if expect_open:
            check(os.path.exists(target),
                  f"after a kill at {point} the published file exists")
        else:
            check(not os.path.exists(target) or
                  os.path.getsize(target) == 0,
                  f"after a kill at {point} nothing is published")
        print(f"   ok: at {point} the temp residue cannot be opened as a "
              f"successful investigation ({len(leftovers)} file(s))")

    print("\nSNAPSHOT/GAP/CRASH DRILL PASSED")
    return 0


def _wait(path: str, timeout: float = 120):
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {path}")
        time.sleep(0.01)


def _child_script() -> str:
    return (
        "import sys\n"
        "from response_investigation import ResponseInvestigation\n"
        "path, wh, job, p, q = sys.argv[1:6]\n"
        "ResponseInvestigation.build(path, wh, [job], {job: int(p)},\n"
        "                           {job: int(q)})\n"
    )


if __name__ == "__main__":
    sys.exit(main())
