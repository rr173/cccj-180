"""Drill: independent acceptance for the attribution mart.

What this drill proves, and how:

1. **Reference equivalence under random interleaving.**
   A deterministic pseudo-random schedule mixes small jobs (different left
   partitions, repeated partition/seq names across jobs, null keys,
   unmatched rows, >64-bit times) and random complete history replacements.
   After every sync and every replacement the mart's groups, the exact
   latency statistics and the expansion identities are compared with
   ``drills.attribution_reference`` -- an independent implementation that
   reads the warehouse row by row and keeps dense, sorted latency lists.
   It never calls the mart's attribution lookup, retraction or quantile
   code.

2. **Snapshot barrier.**  A child process syncs, reaching a barrier just
   after the warehouse snapshot is taken.  The parent then appends new
   results (a real ``JobStore`` batch + real delivery) while the child is
   still working.  The child's reported ``captured_upper`` must not include
   the appended rows, and the *next* sync must pick exactly those rows up.

3. **Real processes killed before and after commit.**  Child processes are
   killed with ``os._exit(117)`` via the ``MART_CRASH_AT`` test hook, once
   before the sync/correction commit and once just after it.  Before commit
   the previous version must be intact; after commit the whole new version
   must be intact.  No mixed versions, no manual lock cleanup on restart.

4. **Concurrent readers and writer exclusion.**  While a slow sync (held at
   a barrier) is uncommitted, readers must keep seeing the previous
   complete version.  A second modifier must get ``MartBusyError`` (and the
   CLI must exit 3); a correction with a stale ``expected_revision`` must be
   rejected without writing anything.

5. **Gap and overlap recovery.**  A warehouse row is removed behind the
   mart's back to simulate a gap in the new range: sync must fail, the old
   version must stay readable and a later legal sync must succeed.  An
   overlapping history is rejected and a corrected history is accepted.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from attribution_mart import (AttributionInputError, AttributionMart,
                              MartBusyError, MartIdentityError,
                              MartInputError, StaleRevisionError,
                              StaleVersionError)
from drills.attribution_common import (OUT, check, d, deliver, feed,
                                       fresh_dir, make_job, w)
from drills.attribution_reference import reference_groups
from interval_join import JobStore

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
W = 10
O = 0
LO = -3
HI = 4
CRASH = 117
BIG = 2 ** 80 + 7


def _open(p):
    return JobStore.open(p)


def _groups(mart):
    return {(g["window_start"], g["owner"]): g for g in mart.query()["groups"]}


def _compare(mart, wh, jids, histories, label):
    ref, facts = reference_groups(wh, jids, histories, W, O)
    got = _groups(mart)
    check(set(got) == set(ref), f"{label}: group set == reference")
    for key, expected in ref.items():
        for name, value in expected.items():
            if got[key][name] != value:
                raise AssertionError(
                    f"{label} {key} {name}: mart {got[key][name]!r} != "
                    f"reference {value!r}")
    for key in ref:
        got_ids = _expand_all(mart, mart.query()["view_token"], key)
        ref_ids = {(f["job_id"], f["offset"])
                   for f in facts if (f["window_start"], f["owner"]) == key}
        check(got_ids == ref_ids, f"{label} {key}: expansion identity set")


def _expand_all(mart, token, key, page=200):
    """Walk an expansion to the end; returns every (job, offset) identity."""
    ids, after = set(), None
    while True:
        res = mart.expand(token, key[0], key[1], limit=page, after=after)
        ids |= {(f["job_id"], f["offset"]) for f in res["facts"]}
        if not res["has_more"]:
            return ids
        after = res["next_after"]


def _random_history(rng, device_seed):
    """Random but legal history: random gaps, touching and unbounded
    segments, random owner texts (including a literal 'unknown')."""
    cuts = sorted(rng.sample(range(-60, 61, 5), rng.randint(1, 5)))
    segments = []
    pos = None if rng.random() < 0.4 else cuts[0] - 5
    for i, cut in enumerate(cuts):
        end = cut if rng.random() < 0.8 else None
        if end is not None and pos is not None and end <= pos:
            continue
        if pos is not None and end is not None and end - pos < 1:
            continue
        segments.append({"start": pos, "end": end,
                         "owner": rng.choice(["A", "B", "C", "unknown",
                                              f"team-{device_seed}"])})
        if end is None:
            break
        pos = end + rng.choice([0, 0, 1, 2])   # touching or a small gap
        if pos > 60:
            break
    return segments


def part_random(base):
    """Part 1: random interleaving of sync and complete history replacement
    against the independent reference."""
    rng = random.Random(20261118)
    wh = os.path.join(base, "wh.db")
    jobs = []
    counters = {}
    for i in range(3):
        path = os.path.join(base, f"rnd_job{i}.db")
        counters[path] = {}
        feed(make_job(path, LO, HI), [_random_batch(rng, 0, counters[path])], 1)
        deliver(_open(path), wh)
        jobs.append(path)
    jids = [_open(p).job_id for p in jobs]
    mart = AttributionMart.create(os.path.join(base, "rnd_mart.db"),
                                  warehouse=wh, job_ids=jids,
                                  width=W, origin=O)
    histories = {"dev0": _random_history(rng, 0),
                 "dev1": _random_history(rng, 1),
                 "dev2": _random_history(rng, 2),
                 "dev3": [{"start": None, "end": None, "owner": "C"}]}
    with mart.modify():
        mart.replace_attribution(0, histories)
    batch_no = {p: 1 for p in jobs}
    for step in range(14):
        # half the steps: append results to a random job, then sync all
        if step % 2 == 0:
            path = rng.choice(jobs)
            batch_no[path] += 1
            append = _random_batch(rng, step, counters[path])
            feed(_open(path), [append], batch_no[path])
            deliver(_open(path), wh)
            with mart.modify():
                mart.sync()
            _compare(mart, wh, jids, histories, f"step {step} sync")
        else:
            dev = rng.choice(list(histories))
            histories[dev] = _random_history(rng, step)
            rev = mart.status()["mart_revision"]
            with mart.modify():
                res = mart.replace_attribution(rev, {dev: histories[dev]})
            _compare(mart, wh, jids, histories, f"step {step} correction")
    # a no-op replacement still adds exactly one dimension revision
    rev, dim = mart.status()["mart_revision"], mart.status()["dim_revision"]
    with mart.modify():
        mart.replace_attribution(rev, histories)
    st = mart.status()
    check(st["mart_revision"] == rev + 1 and st["dim_revision"] == dim + 1,
          "a no-op replacement still publishes one new dimension revision")
    # paginated query and expand must see the same committed snapshot
    full = mart.query()
    paged, cursor = [], None
    while True:
        page = mart.query(view_token=full["view_token"], after=cursor, limit=2)
        paged.extend(page["groups"])
        if not page["has_more"]:
            break
        cursor = tuple(page["next_cursor"])
    check(paged == full["groups"], "paginated query equals the full query")
    tok = full["view_token"]
    for key in [(g["window_start"], g["owner"]) for g in full["groups"]][:4]:
        first = mart.expand(tok, key[0], key[1], limit=2)
        ids = {(f["job_id"], f["offset"]) for f in first["facts"]}
        while first["has_more"]:
            first = mart.expand(tok, key[0], key[1], limit=2,
                                after=first["next_after"])
            ids |= {(f["job_id"], f["offset"]) for f in first["facts"]}
        check(len(ids) == mart.expand(tok, key[0], key[1], limit=1)
              ["group_fact_count"],
              f"{key}: paged expansion covers every fact exactly once")
    mart.close()


def _random_batch(rng, step, seq):
    """Random open-job batch; `seq` carries the per-job seq counters so
    the batches are strictly increasing across appends."""
    msgs = []

    def nxt(side, part):
        k = (side, part)
        seq[k] = seq.get(k, 0) + 1
        return seq[k]

    for _ in range(rng.randint(6, 14)):
        side = rng.choice(["L", "R"])
        part = "lp0" if side == "L" else "rp0"
        key = rng.choice(["dev0", "dev1", "dev2", "dev3", None])
        t = rng.choice([rng.randint(-50, 50), BIG, -BIG]) \
            if rng.random() < 0.15 else rng.randint(-50, 50)
        msgs.append(d(side, part, nxt(side, part), key, t))
    # no watermark and no end here: jobs stay open for later appends
    return msgs


# --------------------------------------------------------------- barrier
CHILD = os.path.join(OUT, "_child.py")


def _write_child():
    os.makedirs(OUT, exist_ok=True)
    with open(CHILD, "w", encoding="utf-8") as f:
        f.write(CHILD_SOURCE)


CHILD_SOURCE = r'''
import json, os, sys
sys.path.insert(0, os.environ["TWOSTREAM_ROOT"])
from attribution_mart import AttributionMart

def main():
    cmd = sys.argv[1]
    path = sys.argv[2]
    mart = AttributionMart.open(path)
    if cmd == "sync":
        with mart.modify():
            print(json.dumps(mart.sync()))
    elif cmd == "correct":
        rev = int(sys.argv[3])
        hist = json.loads(sys.argv[4])
        with mart.modify():
            print(json.dumps(mart.replace_attribution(rev, hist)))
    elif cmd == "slow_sync":
        with mart.modify():
            mart.sync()
        print("{}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
'''


def _run_child(args, barrier_dir=None, crash=None, timeout=120):
    env = dict(os.environ)
    env["TWOSTREAM_ROOT"] = ROOT
    env["PYTHONPATH"] = ROOT
    if barrier_dir:
        env["MART_BARRIER_DIR"] = barrier_dir
    if crash:
        env["MART_CRASH_AT"] = crash
    return subprocess.Popen([sys.executable, "-I", CHILD] + args, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True)


def _wait_file(path, timeout=60):
    deadline = time.time() + timeout
    while not os.path.exists(path):
        if time.time() > deadline:
            raise AssertionError(f"barrier not reached: {path}")
        time.sleep(0.005)
    time.sleep(0.02)  # let the file content settle


def _release(barrier_dir, point):
    with open(os.path.join(barrier_dir, point + ".release"), "w") as f:
        f.write("go\n")


def part_barrier(base):
    """Part 2: source appends after the sync snapshot is formed -- the
    captured bound must not drift, the next sync must include the rows."""
    _write_child()
    wh = os.path.join(base, "bar_wh.db")
    job_path = os.path.join(base, "bar_job.db")
    feed(make_job(job_path, LO, HI), [[
        d("L", "lp0", 1, "dev0", 5), d("R", "rp0", 1, "dev0", 6),
        d("L", "lp0", 2, "dev0", 15),
    ]], 1)
    deliver(_open(job_path), wh)
    jid = _open(job_path).job_id
    import sqlite3 as _sq
    with _sq.connect(wh) as _c:
        initial_rows = _c.execute(
            "SELECT COUNT(*) FROM wh_results WHERE job_id=?", (jid,)
        ).fetchone()[0]
    mart_path = os.path.join(base, "bar_mart.db")
    mart = AttributionMart.create(mart_path, warehouse=wh, job_ids=[jid],
                                  width=W, origin=O)
    with mart.modify():
        mart.replace_attribution(0, {"dev0": [
            {"start": None, "end": None, "owner": "A"}]})
    mart.close()

    bdir = os.path.join(base, "barrier")
    os.makedirs(bdir, exist_ok=True)
    open(os.path.join(bdir, "sync.snapshot_taken.want"), "w").close()
    proc = _run_child(["sync", mart_path], barrier_dir=bdir)
    _wait_file(os.path.join(bdir, "sync.snapshot_taken.reached"))

    # The collector appends and delivers while the child is between the
    # snapshot and its commit.
    job = _open(job_path)
    feed(job, [[d("L", "lp0", 3, "dev0", 25),
                d("R", "rp0", 2, "dev0", 26)]], 2)
    deliver(job, wh)
    job.close()
    import sqlite3
    with sqlite3.connect(wh) as c:
        latest = c.execute("SELECT COUNT(*) FROM wh_results WHERE job_id=?",
                           (jid,)).fetchone()[0]
    print("   warehouse rows after append:", latest)
    check(latest == initial_rows + 1,
          f"collector appended one match while the sync hung "
          f"({latest} rows, was {initial_rows})")

    _release(bdir, "sync.snapshot_taken")
    out, err = proc.communicate(timeout=120)
    check(proc.returncode == 0, f"barrier sync exited 0 ({err[-300:]})")
    summary = json.loads(out)
    captured = summary["jobs"][jid]["captured_upper"]
    check(captured == initial_rows,
          f"captured upper stayed at the snapshot bound "
          f"({initial_rows}), got {captured}")
    check(summary["jobs"][jid]["processed_after"] == initial_rows,
          "the sync processed exactly the captured range")
    mart = AttributionMart.open(mart_path)
    check(mart.query()["positions"][jid] == initial_rows,
          "the published position is the snapshot bound, not the later rows")
    with mart.modify():
        nxt = mart.sync()
    check(nxt["jobs"][jid]["captured_upper"] == latest and
          nxt["jobs"][jid]["facts_added"] == 1,
          "the next sync captured exactly the row appended during the hang")
    groups = _groups(mart)
    check(groups[(20, "A")]["match_pairs"] == 1,
          "the appended match landed in its own window")
    mart.close()


def part_crash(base):
    """Part 3: real processes killed before/after commit."""
    _write_child()
    wh = os.path.join(base, "crash_wh.db")
    job_path = os.path.join(base, "crash_job.db")
    feed(make_job(job_path, LO, HI), [[
        d("L", "lp0", 1, "dev0", 3), d("R", "rp0", 1, "dev0", 4),
        d("L", "lp0", 2, "dev0", 13), d("R", "rp0", 2, "dev0", 14),
    ]], 1)
    deliver(_open(job_path), wh)
    jid = _open(job_path).job_id
    mart_path = os.path.join(base, "crash_mart.db")
    mart = AttributionMart.create(mart_path, warehouse=wh, job_ids=[jid],
                                  width=W, origin=O)
    hist = {"dev0": [{"start": None, "end": 10, "owner": "A"},
                     {"start": 10, "end": None, "owner": "B"}]}
    with mart.modify():
        mart.replace_attribution(0, hist)
        mart.sync()
    rev_before = mart.status()
    check(rev_before["mart_revision"] == 2, "baseline version committed")
    mart.close()

    # (a) killed before the sync commit -> old version survives
    job = _open(job_path)
    feed(job, [[d("L", "lp0", 3, "dev0", 23),
                d("R", "rp0", 3, "dev0", 24)]], 2)
    deliver(job, wh)
    job.close()
    proc = _run_child(["sync", mart_path], crash="sync.before_commit")
    proc.communicate(timeout=120)
    check(proc.returncode == CRASH,
          f"sync child was killed before commit (exit {proc.returncode})")
    mart = AttributionMart.open(mart_path)
    st = mart.status()
    check(st["mart_revision"] == rev_before["mart_revision"] and
          st["view_token"] == rev_before["view_token"],
          "kill before commit: previous version fully intact")
    check(st["positions"][jid] == 2, "kill before commit: position unchanged")
    check(not st["locked"], "the killed writer's flock is gone (no cleanup)")
    # a successor can immediately finish the same sync
    with mart.modify():
        summary = mart.sync()
    check(summary["jobs"][jid]["captured_upper"] == 3 and
          summary["jobs"][jid]["facts_added"] == 1,
          "successor sync picks up the same range after the kill")
    tok_after = mart.query()["view_token"]
    check(tok_after != rev_before["view_token"], "a new version was published")

    # (b) killed after the sync commit -> new version survives completely
    job = _open(job_path)
    feed(job, [[d("L", "lp0", 4, "dev0", 33),
                d("R", "rp0", 4, "dev0", 34)]], 3)
    deliver(job, wh)
    job.close()
    proc = _run_child(["sync", mart_path], crash="sync.after_commit")
    proc.communicate(timeout=120)
    check(proc.returncode == CRASH, "sync child was killed after commit")
    mart = AttributionMart.open(mart_path)
    st = mart.status()
    check(st["positions"][jid] == 4,
          "kill after commit: the new position is durable")
    check(st["mart_revision"] == 4,
          "kill after commit: exactly one new version was published")
    _compare(mart, wh, [jid], hist, "after kill-after-sync-commit")
    mart.close()

    # (c) killed before a correction commit -> old histories survive
    rev = st["mart_revision"]
    dim_before = st["dim_revision"]
    new_hist = {"dev0": [{"start": None, "end": None, "owner": "C"}]}
    proc = _run_child(["correct", mart_path, str(rev),
                       json.dumps(new_hist)],
                      crash="correction.before_commit")
    proc.communicate(timeout=120)
    check(proc.returncode == CRASH, "correction child was killed before commit")
    mart = AttributionMart.open(mart_path)
    st2 = mart.status()
    check(st2["mart_revision"] == rev and st2["dim_revision"] == dim_before,
          "kill before correction commit: revision and histories unchanged")
    _compare(mart, wh, [jid], hist, "after kill-before-correction")
    check(not st2["locked"], "no manual lock cleanup needed after a kill")
    # (d) killed after a correction commit -> new histories survive
    proc = _run_child(["correct", mart_path, str(rev),
                       json.dumps(new_hist)],
                      crash="correction.after_commit")
    proc.communicate(timeout=120)
    check(proc.returncode == CRASH,
          "correction child was killed after commit")
    mart.close()
    mart = AttributionMart.open(mart_path)
    st3 = mart.status()
    check(st3["mart_revision"] == rev + 1 and
          st3["dim_revision"] == dim_before + 1,
          "kill after correction commit: exactly one new version + revision")
    _compare(mart, wh, [jid], new_hist,
             "after kill-after-correction-commit")
    mart.close()


def part_concurrency(base):
    """Part 4: readers during an uncommitted sync; writer exclusion."""
    _write_child()
    wh = os.path.join(base, "cc_wh.db")
    job_path = os.path.join(base, "cc_job.db")
    feed(make_job(job_path, LO, HI), [[
        d("L", "lp0", 1, "dev0", 1), d("R", "rp0", 1, "dev0", 2)]], 1)
    deliver(_open(job_path), wh)
    jid = _open(job_path).job_id
    mart_path = os.path.join(base, "cc_mart.db")
    mart = AttributionMart.create(mart_path, warehouse=wh, job_ids=[jid],
                                  width=W, origin=O)
    with mart.modify():
        mart.replace_attribution(0, {"dev0": [
            {"start": None, "end": None, "owner": "A"}]})
        mart.sync()
    before = mart.query()

    job = _open(job_path)
    feed(job, [[d("L", "lp0", 2, "dev0", 11),
                d("R", "rp0", 2, "dev0", 12)]], 2)
    deliver(job, wh)
    job.close()

    bdir = os.path.join(base, "cc_barrier")
    os.makedirs(bdir, exist_ok=True)
    open(os.path.join(bdir, "sync.snapshot_taken.want"), "w").close()
    proc = _run_child(["sync", mart_path], barrier_dir=bdir)
    _wait_file(os.path.join(bdir, "sync.snapshot_taken.reached"))
    # reader while the writer's transaction is open
    reader = AttributionMart.open(mart_path)
    during = reader.query()
    check(during["view_token"] == before["view_token"],
          "a reader sees the previous complete version during a long sync")
    check((10, "A") not in _groups(reader),
          "the reader does not see half-applied new data")
    # a second modifier is excluded (API and CLI)
    try:
        reader.acquire()
    except MartBusyError:
        check(True, "a second modifier gets MartBusyError")
    else:
        raise AssertionError("a second modifier acquired the writer lock")
    cli = subprocess.run(
        [sys.executable, "-m", "attribution_mart.cli", "sync",
         "--mart", mart_path],
        cwd=ROOT, capture_output=True, text=True)
    check(cli.returncode == 3, f"the CLI reports busy with exit 3 "
                               f"(got {cli.returncode})")
    # a stale correction while the sync is pending is rejected
    try:
        with reader.modify():
            reader.replace_attribution(before["mart_revision"], {"dev0": []})
    except MartBusyError:
        pass   # (the writer lock is still held; also correct behaviour)
    _release(bdir, "sync.snapshot_taken")
    out, _ = proc.communicate(timeout=120)
    check(proc.returncode == 0, "the slow sync committed successfully")
    after = reader.query()
    check((10, "A") in _groups(reader),
          "after the commit readers see the complete new version")
    check(after["mart_revision"] == before["mart_revision"] + 1,
          "exactly one version was published")
    # stale revision after the commit is rejected and writes nothing
    try:
        with reader.modify():
            reader.replace_attribution(before["mart_revision"], {"dev0": []})
    except StaleRevisionError:
        check(True, "a correction with a stale expected_revision is rejected")
    else:
        raise AssertionError("stale correction was accepted")
    check(reader.query()["view_token"] == after["view_token"],
          "the rejected correction wrote nothing")
    # a pinned old view token is refused by expand and query
    try:
        reader.expand(before["view_token"], 0, "A")
    except StaleVersionError:
        check(True, "expand refuses a stale view token")
    else:
        raise AssertionError("expand accepted a stale view")
    reader.close()


def part_gap_and_overlap(base):
    """Part 5: gap in new input -> explicit failure, old version kept;
    overlapping history rejected, corrected one accepted."""
    wh = os.path.join(base, "gap_wh.db")
    job_path = os.path.join(base, "gap_job.db")
    feed(make_job(job_path, LO, HI), [[
        d("L", "lp0", 1, "dev0", 1), d("R", "rp0", 1, "dev0", 2),
    ]], 1)
    deliver(_open(job_path), wh)
    jid = _open(job_path).job_id
    mart_path = os.path.join(base, "gap_mart.db")
    mart = AttributionMart.create(mart_path, warehouse=wh, job_ids=[jid],
                                  width=W, origin=O)
    with mart.modify():
        mart.replace_attribution(0, {"dev0": [
            {"start": None, "end": None, "owner": "A"}]})
        mart.sync()
    before = mart.query()
    check(before["positions"][jid] == 1, "baseline position is 1")

    # The collector appends three matches (warehouse offsets 2, 3, 4).
    job = _open(job_path)
    feed(job, [[d("L", "lp0", 2, "dev0", 11), d("R", "rp0", 2, "dev0", 12)],
               [d("L", "lp0", 3, "dev0", 21), d("R", "rp0", 3, "dev0", 22)],
               [d("L", "lp0", 4, "dev0", 31), d("R", "rp0", 4, "dev0", 32)]],
         2)
    deliver(job, wh)
    job.close()

    # Simulate an uninterpretable source: copy the warehouse (same
    # identity), then delete the middle row of the new range (offset 3),
    # leaving offsets (1, 4] with a hole.
    victim = os.path.join(base, "gap_wh_copy.db")
    _copy_sqlite(wh, victim)
    import sqlite3
    c = sqlite3.connect(victim)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("DELETE FROM wh_results WHERE job_id=? AND offset=3", (jid,))
    c.commit()
    c.close()
    mart.close()
    mart = AttributionMart.open(mart_path)
    with mart.modify():
        mart.rebind_warehouse(victim)     # same identity, new path
    try:
        with mart.modify():
            mart.sync()
    except MartInputError as e:
        check(True, "a gap in the new range fails explicitly: "
                    + str(e)[:70])
    else:
        raise AssertionError("a gap in the new range was skipped")
    after = mart.query()
    check(after["view_token"] == before["view_token"] and
          after["positions"][jid] == 1,
          "the failed sync kept the previous complete version")
    # repair by rebinding to the complete warehouse (same identity)
    with mart.modify():
        mart.rebind_warehouse(wh)
        summary = mart.sync()
    check(summary["jobs"][jid]["captured_upper"] == 4 and
          summary["jobs"][jid]["facts_added"] == 3,
          "after the gap is repaired the same sync completes")
    _compare(mart, wh, [jid], {"dev0": [
        {"start": None, "end": None, "owner": "A"}]}, "after gap repair")
    # a different warehouse is rejected by path rebinding and by open()
    other = os.path.join(base, "other_wh.db")
    feed(make_job(os.path.join(base, "other_job.db"), LO, HI), [[
        d("L", "lp0", 1, "dev0", 1), d("R", "rp0", 1, "dev0", 2)]], 1)
    deliver(_open(os.path.join(base, "other_job.db")), other)
    try:
        with mart.modify():
            mart.rebind_warehouse(other)
    except MartIdentityError:
        check(True, "binding to a different warehouse is rejected")
    else:
        raise AssertionError("a different warehouse was accepted")
    try:
        AttributionMart.open(mart_path, warehouse=other)
    except MartIdentityError:
        check(True, "opening against a different warehouse is rejected")
    else:
        raise AssertionError("open() accepted a different warehouse")
    # overlapping history rejected whole; corrected history accepted
    rev = mart.status()["mart_revision"]
    try:
        with mart.modify():
            mart.replace_attribution(rev, {
                "dev0": [{"start": 0, "end": 10, "owner": "A"},
                         {"start": 5, "end": 20, "owner": "B"}],
                "dev1": [{"start": None, "end": None, "owner": "Z"}]})
    except AttributionInputError:
        check(True, "an overlapping history rejects the whole correction")
    else:
        raise AssertionError("overlapping history accepted")
    check((0, "Z") not in _groups(mart),
          "the other device in the rejected correction did not change")
    with mart.modify():
        mart.replace_attribution(rev, {
            "dev0": [{"start": 0, "end": 10, "owner": "A"},
                     {"start": 10, "end": 20, "owner": "B"}]})
    check((10, "B") in _groups(mart), "the corrected history is applied")
    mart.close()


def _copy_sqlite(src, dst):
    import sqlite3
    s = sqlite3.connect(src)
    d = sqlite3.connect(dst)
    with d:
        s.backup(d)
    d.close()
    s.close()


def main() -> int:
    base = fresh_dir("acceptance")
    for name, fn in [("1 random interleaving vs reference", part_random),
                     ("2 snapshot barrier", part_barrier),
                     ("3 process kills before/after commit", part_crash),
                     ("4 concurrency and writer exclusion", part_concurrency),
                     ("5 gap and overlap recovery", part_gap_and_overlap)]:
        print(f"===== {name} =====", flush=True)
        fn(base)
        print(f"----- {name}: OK\n", flush=True)
    print("ACCEPTANCE DRILL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
