"""Drill: hand-computable walk-through of "why a correction must deduct".

Fixed scenario from the acceptance brief (lo=1, hi=4, W=10, O=0):

  job1  L1@9  matched by R@11 and R@13   (latencies 2 and 4)
        L2@19 matched by R@20            (latency 1)
        lpad@20 (unmatched request), rpad@30 (unmatched response)
  job2  same left partition name and left seq=1, anchored @9, matched by
        R@10 -- an independent request, because request identity is
        (job_id, left partition, left seq).

  Device d: A on [0,20), B on [20,40).

Window 0 therefore joins job1's L1@9 (two pairs) and job2's L1@9 (one
pair): 3 pairs, 2 requests, latencies 1,2,4.  Window 10 holds L2@19:
1 pair, 1 request, latency 1.  lpad@20 is a B-group window-20 row and
rpad@30 a B-group window-30 row.

Steps:
  1. sync                         -- baseline table
  2. correct [8,20) to C          -- window 0 -> C: 3 pairs / 2 requests,
                                     lat 1,2,4, p50=2, p95=4; window 10 ->
                                     C: 1 pair / 1 request, lat 1.  A's
                                     groups vanish, B's pads do not move.
  3. punch [9,10) into a gap      -- exactly those 3 pairs and 2 requests
                                     move into the unknown group.
  4. a new match for d anchored 9 arrives and is synced -- it joins the
     unknown group by the *new* history.
  5. boundaries, negative windows, null keys, the literal owner "unknown",
     >64-bit times and a negative latency.

Every step is also compared against the independent reference
(``drills.attribution_reference``: its own dense arrays, window arithmetic
and nearest-rank code), including expansion identities.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from attribution_mart import AttributionMart, StaleRevisionError
from attribution_mart.stats import window_start
from drills.attribution_common import (check, d, deliver, feed, fresh_dir,
                                       make_job, rss_kb, show, w)
from drills.attribution_reference import reference_groups
from interval_join import JobStore, SQLiteWarehouse

LO, HI, W, O = 1, 4, 10, 0
BIG = 2 ** 70 + 9        # a time far beyond 64 bits


def _open(path):
    return JobStore.open(path)


def _end(side, part):
    return {"type": "end", "side": side, "partition": part}


def build_warehouse(base):
    j1 = os.path.join(base, "job1.db")
    j2 = os.path.join(base, "job2.db")
    wh = os.path.join(base, "warehouse.db")
    # job1 emits pads, so the watermarks must prove them.  R wm 25 proves
    # the lpad of L@20 (right_min > 20+hi = 24); L wm 30 proves the rpad of
    # R@30 (left_min > 30-lo = 29).
    feed(make_job(j1, LO, HI), [[
        d("L", "lp0", 1, "d", 9),        # L1 -> 11, 13
        d("R", "rp0", 1, "d", 11),
        d("R", "rp0", 2, "d", 13),
        d("L", "lp0", 2, "d", 19),       # L2 -> 20
        d("R", "rp0", 3, "d", 20),
        d("L", "lp0", 3, "d", 20),       # unmatched request (lpad)
        d("R", "rp0", 4, "d", 30),       # unmatched response (rpad)
        w("R", "rp0", 25), w("L", "lp0", 30),
    ]])
    deliver(_open(j1), wh)
    feed(make_job(j2, LO, HI), [[
        d("L", "lp0", 1, "d", 9),
        d("R", "rp0", 1, "d", 10),
        w("L", "lp0", 100), w("R", "rp0", 100),
        _end("L", "lp0"), _end("R", "rp0"),
    ]])
    deliver(_open(j2), wh)
    return j1, j2, wh


def _groups(mart):
    return {(g["window_start"], g["owner"]): g for g in mart.query()["groups"]}


def _expect(mart, expected, label):
    got = _groups(mart)
    check(set(got) == set(expected), f"{label}: exact group set")
    for key, fields in expected.items():
        g = got[key]
        for name, value in fields.items():
            check(g[name] == value,
                  f"{label} {key} {name} = {value!r} (got {g[name]!r})")
    return got


def _compare_reference(mart, wh, jids, histories, label):
    ref, facts = reference_groups(wh, jids, histories, W, O)
    got = _groups(mart)
    check(set(got) == set(ref), f"{label}: group set agrees with reference")
    for key in ref:
        for name, value in ref[key].items():
            check(got[key][name] == value,
                  f"{label} {key} {name} matches the independent reference")
    tok = mart.query()["view_token"]
    for key in ref:
        page = mart.expand(tok, key[0], key[1], limit=1000)
        got_ids = {(f["job_id"], f["offset"]) for f in page["facts"]}
        ref_ids = {(f["job_id"], f["offset"])
                   for f in facts if (f["window_start"], f["owner"]) == key}
        check(got_ids == ref_ids, f"{label} {key}: expansion identities")
    return ref


def main() -> int:
    base = fresh_dir("hand")
    j1, j2, wh = build_warehouse(base)
    jids = [_open(j1).job_id, _open(j2).job_id]
    print(f"job1={jids[0][:8]}..  job2={jids[1][:8]}..  warehouse={wh}")
    with SQLiteWarehouse(wh) as w_:
        print("warehouse rows:", {j[:8]: w_.count(j, "results") for j in jids})

    mart = AttributionMart.create(os.path.join(base, "mart.db"),
                                  warehouse=wh, job_ids=jids,
                                  width=W, origin=O)
    hist_a = {"d": [{"start": 0, "end": 20, "owner": "A"},
                    {"start": 20, "end": 40, "owner": "B"}]}
    with mart.modify():
        mart.replace_attribution(0, hist_a)
        show("sync", mart.sync())
    _expect(mart, {
        (0, "A"): {"match_pairs": 3, "matched_requests": 2,
                   "unmatched_requests": 0, "unmatched_responses": 0,
                   "latency_sum": 1 + 2 + 4, "latency_min": 1,
                   "latency_max": 4, "p50": 2, "p95": 4},
        (10, "A"): {"match_pairs": 1, "matched_requests": 1,
                    "unmatched_requests": 0, "unmatched_responses": 0,
                    "latency_sum": 1, "latency_min": 1, "latency_max": 1,
                    "p50": 1, "p95": 1},
        (20, "B"): {"match_pairs": 0, "matched_requests": 0,
                    "unmatched_requests": 1, "unmatched_responses": 0,
                    "latency_sum": 0, "latency_min": None, "p50": None},
        (30, "B"): {"match_pairs": 0, "matched_requests": 0,
                    "unmatched_requests": 0, "unmatched_responses": 1,
                    "latency_sum": 0, "latency_min": None, "p50": None},
    }, "baseline")
    _compare_reference(mart, wh, jids, hist_a, "baseline")
    check(mart.query()["positions"] ==
          {jids[0]: 5, jids[1]: 1},
          "both jobs' processed positions are published together")

    # ---- step 2: only [8,20) changes owner (A -> C)
    rev = mart.status()["mart_revision"]
    hist_c = {"d": [{"start": 0, "end": 8, "owner": "A"},
                    {"start": 8, "end": 20, "owner": "C"},
                    {"start": 20, "end": 40, "owner": "B"}]}
    with mart.modify():
        corr = mart.replace_attribution(rev, hist_c)
    print("correction changed ranges:", corr["changed_ranges"],
          "facts scanned:", corr["facts_scanned"],
          "groups recomputed:", corr["groups_recomputed"])
    _expect(mart, {
        (0, "C"): {"match_pairs": 3, "matched_requests": 2,
                   "latency_sum": 7, "latency_min": 1, "latency_max": 4,
                   "p50": 2, "p95": 4},
        (10, "C"): {"match_pairs": 1, "matched_requests": 1,
                    "latency_sum": 1, "p50": 1, "p95": 1},
        (20, "B"): {"unmatched_requests": 1, "match_pairs": 0},
        (30, "B"): {"unmatched_responses": 1, "match_pairs": 0},
    }, "after [8,20)->C")
    check((0, "A") not in _groups(mart), "A's emptied window-0 group is gone")
    check((10, "A") not in _groups(mart), "A's emptied window-10 group is gone")
    check(mart.status()["dim_revision"] == 2,
          "one successful correction = exactly one dimension revision")
    _compare_reference(mart, wh, jids, hist_c, "after correction")

    # ---- step 3: punch an uncovered gap [9,10)
    rev = mart.status()["mart_revision"]
    hist_gap = {"d": [{"start": 0, "end": 8, "owner": "A"},
                      {"start": 8, "end": 9, "owner": "C"},
                      {"start": 10, "end": 20, "owner": "C"},
                      {"start": 20, "end": 40, "owner": "B"}]}
    with mart.modify():
        corr = mart.replace_attribution(rev, hist_gap)
    print("gap correction:", corr["changed_ranges"],
          "facts scanned:", corr["facts_scanned"])
    _expect(mart, {
        (0, None): {"match_pairs": 3, "matched_requests": 2,
                    "latency_sum": 7, "latency_min": 1, "latency_max": 4,
                    "p50": 2, "p95": 4},
        (10, "C"): {"match_pairs": 1, "matched_requests": 1},
        (20, "B"): {"unmatched_requests": 1},
        (30, "B"): {"unmatched_responses": 1},
    }, "after [9,10) gap")
    check((0, "C") not in _groups(mart),
          "C's emptied window-0 group disappeared (no zero-only groups)")
    _compare_reference(mart, wh, jids, hist_gap, "after gap")
    tok = mart.query()["view_token"]
    page = mart.expand(tok, 0, None, limit=1000)
    check(page["group_fact_count"] == 3, "unknown group has exactly 3 facts")
    check({f["attribution"]["unknown_reason"] for f in page["facts"]}
          == {"no covering attribution segment"},
          "each unknown fact carries the covering-gap reason")
    check({f["job_id"] for f in page["facts"]} == set(jids),
          "the unknown group explains facts from both jobs")

    # ---- step 4: a new match for d anchored 9 arrives and is synced
    j3 = os.path.join(base, "job3.db")
    feed(make_job(j3, LO, HI), [[
        d("L", "lp0", 1, "d", 9),
        d("R", "rp0", 1, "d", 10),
        w("L", "lp0", 100), w("R", "rp0", 100),
        _end("L", "lp0"), _end("R", "rp0"),
    ]])
    deliver(_open(j3), wh)
    jid3 = _open(j3).job_id
    mart.close()
    mart = AttributionMart.open(os.path.join(base, "mart.db"))
    check(mart.job_ids == jids,
          "reopening keeps the configured job set (job3 is not included)")
    mart.close()
    mart = AttributionMart.create(os.path.join(base, "mart_ext.db"),
                                  warehouse=wh, job_ids=jids + [jid3],
                                  width=W, origin=O)
    with mart.modify():
        mart.replace_attribution(0, hist_gap)
        mart.sync()
    got = _groups(mart)
    check(got[(0, None)]["match_pairs"] == 4 and
          got[(0, None)]["matched_requests"] == 3,
          "the new anchored-9 match joins the unknown group by the new "
          "history (4 pairs / 3 requests)")
    check(got[(0, None)]["latency_sum"] == 8 and
          got[(0, None)]["latency_max"] == 4,
          "the new latency 1 is folded into the unknown group's exact set")
    check((0, "A") not in got and (0, "C") not in got,
          "no stale owner group received the new fact")
    _compare_reference(mart, wh, jids + [jid3], hist_gap, "after new fact")
    mart.close()

    # ---- step 5: boundaries, negative windows, null keys, literal
    #             "unknown", >64-bit times, negative latency
    j4 = os.path.join(base, "job4.db")
    feed(make_job(j4, -1000, 1000), [[
        d("L", "lp0", 1, "d", 20),        # exactly on a segment boundary
        d("R", "rp0", 1, "d", 21),
        d("L", "lp0", 2, None, 15),       # null device key
        d("R", "rp0", 2, None, 16),
        d("L", "lp0", 3, "lit", 5),       # owner text literally "unknown"
        d("R", "rp0", 3, "lit", 10),
        d("L", "lp0", 4, "big", -BIG),    # >64-bit negative time
        d("R", "rp0", 4, "big", -BIG + 3),
        d("L", "lp0", 5, "neg", 100),     # negative latency (lo very low)
        d("R", "rp0", 5, "neg", 40),
        w("L", "lp0", 10 ** 30), w("R", "rp0", 10 ** 30),
        _end("L", "lp0"), _end("R", "rp0"),
    ]])
    deliver(_open(j4), wh)
    jid4 = _open(j4).job_id
    m2 = AttributionMart.create(os.path.join(base, "mart_edge.db"),
                                warehouse=wh, job_ids=[jid4],
                                width=W, origin=O)
    hist2 = {"d": [{"start": 0, "end": 20, "owner": "A"},
                   {"start": 20, "end": 40, "owner": "B"}],
             "lit": [{"start": None, "end": None, "owner": "unknown"}],
             "neg": [{"start": None, "end": None, "owner": "N"}],
             "big": [{"start": None, "end": -2 ** 69, "owner": "G"}]}
    with m2.modify():
        m2.replace_attribution(0, hist2)
        m2.sync()
    got = _groups(m2)
    show("edge groups", {f"{k[0]}/{k[1]}": v for k, v in got.items()})
    check(window_start(-BIG, W, O) == O + ((-BIG - O) // W) * W,
          "a negative >64-bit time floors toward -infinity")
    check(got[(20, "B")]["match_pairs"] == 1,
          "an anchor exactly at a segment start belongs to the new segment")
    check((20, "A") not in got, "the half-open end of [0,20) is excluded")
    # The join never matches null keys; they are emitted as pads.  Both
    # pads land in the unknown group at their anchor's window.
    check(got[(10, None)]["match_pairs"] == 0 and
          got[(10, None)]["unmatched_requests"] == 1 and
          got[(10, None)]["unmatched_responses"] == 1,
          "null-key rows go to the unknown group (as unmatched rows)")
    check(got[(0, "unknown")]["match_pairs"] == 1,
          'a real owner text "unknown" is distinct from the unknown group')
    check(got[(window_start(-BIG, W, O), "G")]["match_pairs"] == 1,
          "the >64-bit fact lands in its precise negative window with G")
    check(window_start(-BIG, W, O) < -(2 ** 63),
          "the window start itself is beyond 64 bits (no wrapping)")
    check(got[(100, "N")]["p50"] == -60 and
          got[(100, "N")]["latency_min"] == -60 and
          got[(100, "N")]["latency_sum"] == -60,
          "a negative latency keeps its exact value in min/sum/p50")
    _compare_reference(m2, wh, [jid4], hist2, "edge cases")

    # ---- illegal corrections are rejected atomically
    rev = m2.status()["mart_revision"]
    before = m2.query()
    token_before = before["view_token"]
    bad = [
        {"d": [{"start": 0, "end": 10, "owner": "X"},
               {"start": 5, "end": 20, "owner": "Y"}]},          # overlap
        {"d": [{"start": 5, "end": 5, "owner": "X"}]},           # empty
        {"d": [{"start": 0, "end": 10, "owner": ""}]},           # empty owner
        {"d": [{"start": 20, "end": 10, "owner": "X"}]},         # inverted
    ]
    for hist in bad:
        try:
            with m2.modify():
                m2.replace_attribution(rev, hist)
        except Exception as e:
            print("rejected as expected:", type(e).__name__, str(e)[:70])
        else:
            raise AssertionError(f"illegal history accepted: {hist}")
    check(m2.query()["view_token"] == token_before,
          "rejected corrections leave the committed version untouched")
    try:
        with m2.modify():
            m2.replace_attribution(rev - 1, {"d": []})
    except StaleRevisionError as e:
        print("stale revision rejected:", str(e)[:70])
    else:
        raise AssertionError("stale expected_revision accepted")

    print("peak RSS (kb):", rss_kb())
    m2.close()
    print("HAND DRILL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
