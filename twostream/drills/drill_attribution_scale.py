"""Drill: scale and locality of a narrow history correction.

Everything is generated at run time under ``drills/out/attribution/scale``
and is never committed.

Data set (produced with the real JobStore + join engine + delivery):

  * three jobs sharing partition names, with a statistics grid of W=100;
  * 200,000 facts about *unrelated* devices spread over a long history
    (400,000 messages -> 200,000 matched-pair results);
  * a hot device with 20,000 matched pairs (25 requests x 8 responses,
    per window) across 100 windows;
  * one narrow device with facts spread over the whole time axis.

Then a single narrow range of the hot device's history is corrected.  The
drill reports, as concrete evidence:

  * rows visited by the correction (``facts_scanned``) and groups
    recomputed, against the totals in the mart;
  * `EXPLAIN QUERY PLAN` of the range lookup, proving the ``(device,
    anchor)`` index is used rather than a full scan;
  * peak RSS of the process;
  * group numbers before/after, showing only the affected windows changed;
  * a paginated expansion of one affected group.

The numbers are also checked against incremental invariants (totals over
all groups, and the unaffected groups' exact rows) rather than by loading
all facts.
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from attribution_mart import AttributionMart
from drills.attribution_common import (check, d, db_bytes, deliver, feed,
                                       fresh_dir, make_job, rss_kb, show, w)
from interval_join import JobStore

W = 100
O = 0
LO, HI = 0, 2
UNRELATED = 400_000          # messages -> 200,000 unrelated facts
HOT_WINDOWS = 100            # the hot device has facts in windows 0..9900
HOT_PER_WINDOW = 25          # requests per window
HOT_RESPONSES = 8            # responses per request -> 20000 hot pairs
SPREAD = 20_000


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _submit(job, msgs, start_no=1, chunk=20000):
    """Submit messages in several batches (a real collector advances the
    job incrementally rather than in one giant transaction)."""
    batches = list(_chunks(msgs, chunk))
    start = start_no
    feed(job, batches, start)
    return start + len(batches)


def _build(base):
    wh = os.path.join(base, "warehouse.db")
    jobs = []
    # job A: 200k unrelated facts, the hot device and the spread device.
    ja = os.path.join(base, "job_a.db")
    msgs, ls, rs = [], 0, 0

    def push_l(key, t):
        nonlocal ls
        ls += 1
        msgs.append(d("L", "lp0", ls, key, t))

    def push_r(key, t):
        nonlocal rs
        rs += 1
        msgs.append(d("R", "rp0", rs, key, t))

    # 100k unrelated devices, one matched pair each, over a long history
    for i in range(UNRELATED // 2):
        t = -(UNRELATED // 2) + i * 3
        push_l(f"dev-{i}", t)
        push_r(f"dev-{i}", t + 1)
    # hot device: 25 requests per window (spacing 4), each matched by 8
    # responses at t+1 / t+2, so pairs do not leak to neighbouring requests
    # (spacing 4 > hi = 2).  100 windows x 25 x 8 = 20,000 pairs.
    for wi in range(HOT_WINDOWS):
        for i in range(HOT_PER_WINDOW):
            t = wi * W + i * 4
            push_l("hot", t)
            for k in range(HOT_RESPONSES):
                push_r("hot", t + 1 + (k % 2))
    # spread device: one matched pair per window over the whole history
    for i in range(SPREAD):
        t = -SPREAD * 10 + i * 10
        push_l("spread", t)
        push_r("spread", t + 1)
    msgs.append(w("L", "lp0", 10 ** 12))
    msgs.append(w("R", "rp0", 10 ** 12))
    _submit(make_job(ja, LO, HI), msgs)
    deliver(JobStore.open(ja), wh, page_size=4000)
    jobs.append(JobStore.open(ja).job_id)

    # job B/C: same partition names, and every 50th row uses the same
    # device key "hot" -- equal (partition, seq) across jobs must not merge.
    for name, n in (("job_b", 6000), ("job_c", 6000)):
        jp = os.path.join(base, name + ".db")
        msgs = []
        for i in range(n):
            t = -n + i * 2
            key = "hot" if i % 50 == 0 else f"x{i}"
            msgs.append(d("L", "lp0", i + 1, key, t))
            msgs.append(d("R", "rp0", i + 1, key, t + 1))
        msgs.append(w("L", "lp0", 10 ** 12))
        msgs.append(w("R", "rp0", 10 ** 12))
        _submit(make_job(jp, LO, HI), msgs)
        deliver(JobStore.open(jp), wh, page_size=4000)
        jobs.append(JobStore.open(jp).job_id)
    return wh, jobs


def main() -> int:
    base = fresh_dir("scale")
    print("building the data set with the real job/engine/delivery path...")
    t0 = time.time()
    wh, jids = _build(base)
    print(f"  built in {time.time() - t0:.1f}s, warehouse {db_bytes(wh)/1e6:.1f} MB")
    import sqlite3
    with sqlite3.connect(wh) as c:
        total_rows = c.execute("SELECT COUNT(*) FROM wh_results").fetchone()[0]
        per_job = dict(c.execute(
            "SELECT job_id, COUNT(*) FROM wh_results GROUP BY job_id"))
    print("  warehouse result rows:", total_rows, per_job)

    histories = {
        # only [120, 220) will change; everything else keeps its owner
        "hot": [{"start": None, "end": 120, "owner": "A"},
                {"start": 120, "end": None, "owner": "B"}],
        "spread": [{"start": None, "end": None, "owner": "S"}],
    }
    mart_path = os.path.join(base, "mart.db")
    mart = AttributionMart.create(mart_path, warehouse=wh, job_ids=jids,
                                  width=W, origin=O)
    with mart.modify():
        mart.replace_attribution(0, histories)
        t0 = time.time()
        summary = mart.sync()
    print(f"  initial sync: {time.time() - t0:.1f}s, "
          f"facts {sum(j['facts_added'] for j in summary['jobs'].values())}")
    st = mart.status()
    print("  mart facts:", st["counts"]["facts"],
          "requests:", st["counts"]["reqs"],
          "groups:", st["counts"]["group_stats"])
    check(st["counts"]["facts"] == total_rows,
          "every warehouse row became exactly one mart fact")

    before = {(g["window_start"], g["owner"]): g for g in mart.query()["groups"]}
    hot_before = {(k[0], k[1]): v for k, v in before.items() if k[1] in ("A", "B")}
    windows_before = sorted({k[0] for k in hot_before})
    print("  hot device windows before:", windows_before[:3], "...",
          f"({len(windows_before)} windows)")
    totals_before = {name: sum(g[name] for g in before.values())
                     for name in ("match_pairs", "matched_requests",
                                  "unmatched_requests", "unmatched_responses")}
    print("  global totals before:", totals_before)

    # Correct exactly one narrow range of `hot`: [120, 220) becomes C.
    rev = mart.status()["mart_revision"]
    t0 = time.time()
    with mart.modify():
        res = mart.replace_attribution(rev, {"hot": [
            {"start": None, "end": 120, "owner": "A"},
            {"start": 120, "end": 220, "owner": "C"},
            {"start": 220, "end": None, "owner": "B"},
        ]})
    elapsed = time.time() - t0
    print(f"\nnarrow correction: {elapsed:.3f}s")
    print("  changed ranges:", res["changed_ranges"])
    print("  facts visited:", res["facts_scanned"],
          f"of {st['counts']['facts']} stored facts")
    print("  groups recomputed:", res["groups_recomputed"])

    # The plan evidence: the correction's lookup is an index range scan.
    with sqlite3.connect(mart_path) as c:
        plan = c.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM facts WHERE k=? AND"
            " anchor_enc>=? AND anchor_enc<?", ("hot", "x", "y")).fetchall()
    print("  query plan for the changed-range lookup:", plan)
    check(any("idx_facts_device" in str(row) for row in plan),
          "the correction uses the (device, anchor) index, not a full scan")
    check(res["facts_scanned"] < st["counts"]["facts"] * 0.05,
          f"the correction visited {res['facts_scanned']} facts, far fewer "
          f"than the {st['counts']['facts']} stored")
    assert res["groups_recomputed"] <= len(before)

    after = {(g["window_start"], g["owner"]): g for g in mart.query()["groups"]}
    totals_after = {name: sum(g[name] for g in after.values())
                    for name in ("match_pairs", "matched_requests",
                                 "unmatched_requests", "unmatched_responses")}
    print("  global totals after: ", totals_after)
    check(totals_after == totals_before,
          "the correction only moved numbers between groups")
    changed_keys = {(w, o) for (w, o) in set(before) | set(after)
                    if before.get((w, o)) != after.get((w, o))}
    print("  groups whose published row changed:", len(changed_keys),
          sorted(changed_keys))
    untouched = {k: v for k, v in after.items()
                 if k not in changed_keys and ("A" in (k[1],) or "B" in (k[1],))
                 and k[0] in windows_before}
    check(all(before[k] == v for k, v in untouched.items()),
          "unaffected windows keep exactly their previous rows")
    # Only window 100 can be affected: the changed range [120,220) is in
    # that window (and a window is the grain).
    # The change is only [120, 220); every other window that changed must
    # still lie inside one of the windows the range overlaps (100, 200).
    check(res["changed_ranges"] == {"hot": [[120, 220]]},
          "the correction's changed range is exactly [120, 220)")
    check(changed_keys and {wm for (wm, _) in changed_keys} <= {100, 200},
          f"only windows 100/200 changed, got {sorted({wm for wm, _ in changed_keys})}")
    cg = after[(100, "C")]
    print("  new C group in window 100:", cg)
    from interval_join.encoding import enc_int
    with sqlite3.connect(mart_path) as c:
        counts = dict(c.execute(
            "SELECT owner_k, COUNT(*) FROM facts WHERE ws_enc=?"
            " AND kind='match' GROUP BY owner_k", (enc_int(100),)).fetchall())
    print("  window-100 facts by owner in the mart:", counts)
    check(cg["match_pairs"] == counts.get("C", 0) and
          cg["match_pairs"] > 0,
          "the published match count equals the stored facts of the group")

    # Paginated expansion of the affected group.
    tok = mart.query()["view_token"]
    page = mart.expand(tok, 100, "C", kind="match", limit=7)
    print("  expansion page: facts", len(page["facts"]),
          "group total", page["group_fact_count"],
          "has_more", page["has_more"])
    check(page["has_more"] and len(page["facts"]) == 7,
          "expansion is genuinely paginated")
    seen = 0
    while True:
        page = mart.expand(tok, 100, "C", kind="match", limit=1000,
                           after=None if seen == 0 else page["next_after"])
        seen += len(page["facts"])
        print("    page:", len(page["facts"]), "facts, has_more", page["has_more"])
        if not page["has_more"]:
            break
    check(seen == cg["match_pairs"],
          "the paged expansion totals exactly the group's match pairs")
    check({f["attribution"]["segment"]["owner"] for f in
           mart.expand(tok, 100, "C", kind="match", limit=1000)["facts"]}
          == {"C"},
          "every expanded fact carries its matched segment")

    # A correction to a device whose facts have no change is cheap.
    rev = mart.status()["mart_revision"]
    t0 = time.time()
    with mart.modify():
        res2 = mart.replace_attribution(rev, {"spread": [
            {"start": None, "end": None, "owner": "S"}]})
    print(f"\nno-op history replacement on a spread device: "
          f"{time.time() - t0:.3f}s, facts visited {res2['facts_scanned']}")
    check(res2["facts_scanned"] == 0 and res2["groups_recomputed"] == 0,
          "an identical replacement visits no facts and recomputes nothing")
    check(res2["dim_revision"] == res["dim_revision"] + 1,
          "but it still publishes one dimension revision")

    print(f"\npeak RSS: {rss_kb()} kB  "
          f"(mart {db_bytes(mart_path)/1e6:.1f} MB, "
          f"warehouse {db_bytes(wh)/1e6:.1f} MB)")
    check(rss_kb() < 400_000,
          "peak RSS stays far below the size of a full in-memory copy of "
          "the facts or the hot device's latency list")
    mart.close()
    print("SCALE DRILL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
