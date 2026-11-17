"""Drill 5: independent small sample, brute force vs. legal interleavings.

A fixed small event set (null keys, duplicate key+time under different
seq, negative times, a >64-bit time, all-empty verdicts included) is
first evaluated by an independent offline enumeration of all matching
identity pairs and orphan rows.  The same set is then streamed through
the engine in several legal interleavings (per-partition seq order
preserved) and watermark schedules:

  * "bare"   : data only, then end() everywhere;
  * "safe-wm": after every data message, each partition's watermark is
               advanced to the minimum time of its not-yet-sent events
               (always legal, so nothing becomes late), then ends.

Every run must reproduce the reference multiset exactly (matches, left
pads, right pads) with an empty late set, in all three join modes.
"""

import os
import random
import sys
from collections import Counter, defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import out_dir, write_event_file
from interval_join import (Data, End, EventId, JoinEngine, LeftPad, Match,
                           RightPad, Watermark)

LO, HI = -2, 5
LEFT_PARTS = ["la", "lb"]
RIGHT_PARTS = ["ra", "rb"]
BIG = 2 ** 65 + 3  # beyond int64

LEFTS = [
    Data("L", "la", 1, "a", 5),
    Data("L", "la", 2, "b", 7),
    Data("L", "la", 3, None, 9),          # null key: outer verdict only
    Data("L", "la", 4, "a", -3),          # negative time
    Data("L", "lb", 1, "c", 12),
    Data("L", "lb", 2, "a", 5),           # same key+time as la#1, other identity
    Data("L", "lb", 3, "b", BIG),         # >64-bit time
    Data("L", "lb", 4, "a", 100),
]
RIGHTS = [
    Data("R", "ra", 1, "a", 6),
    Data("R", "ra", 2, "a", 4),
    Data("R", "ra", 3, None, 5),          # null key
    Data("R", "ra", 4, "b", BIG + 2),     # matches lb#3 (BIG+2 <= BIG+5)
    Data("R", "rb", 1, "c", 10),
    Data("R", "rb", 2, "a", 5),
    Data("R", "rb", 3, "b", -4),
    Data("R", "rb", 4, "x", 50),
]
EVENTS = LEFTS + RIGHTS


def reference(events, lo, hi, mode):
    """Offline enumeration of all pairs and orphans (the ground truth)."""
    lefts = [e for e in events if e.side == "L"]
    rights = [e for e in events if e.side == "R"]
    out = []
    l_hit, r_hit = set(), set()
    for l in lefts:
        if l.key is None:
            continue
        for r in rights:
            if r.key is None:
                continue
            if l.key == r.key and l.time + lo <= r.time <= l.time + hi:
                out.append(Match(EventId("L", l.partition, l.seq),
                                 EventId("R", r.partition, r.seq),
                                 l.key, l.time, r.time))
                l_hit.add((l.partition, l.seq))
                r_hit.add((r.partition, r.seq))
    if mode in ("left_outer", "full_outer"):
        for l in lefts:
            if (l.partition, l.seq) not in l_hit:
                out.append(LeftPad(EventId("L", l.partition, l.seq), l.key, l.time))
    if mode == "full_outer":
        for r in rights:
            if (r.partition, r.seq) not in r_hit:
                out.append(RightPad(EventId("R", r.partition, r.seq), r.key, r.time))
    return Counter(out)


def interleave(seed):
    """Shuffle events into a legal order: per-partition seq order kept."""
    queues = defaultdict(deque)
    for e in EVENTS:
        queues[(e.side, e.partition)].append(e)
    rng = random.Random(seed)
    order = []
    while queues:
        key = rng.choice(sorted(queues))
        order.append(queues[key].popleft())
        if not queues[key]:
            del queues[key]
    return order


def with_safe_watermarks(order):
    """Insert watermarks that can never make remaining data late."""
    remaining = defaultdict(list)
    for e in order:
        remaining[(e.side, e.partition)].append(e.time)
    current = {}
    msgs = []
    for e in order:
        msgs.append(e)
        lst = remaining[(e.side, e.partition)]
        lst.remove(e.time)
        if lst:
            w = min(lst)  # all future data of this partition has time >= w
            if current.get((e.side, e.partition)) != w:
                current[(e.side, e.partition)] = w
                msgs.append(Watermark(e.side, e.partition, w))
    return msgs


def ends():
    return ([End("L", p) for p in LEFT_PARTS]
            + [End("R", p) for p in RIGHT_PARTS])


def run_stream(msgs, mode, tag):
    """Run one interleaving through the engine via an event file."""
    records = [{"type": "config", "lo": LO, "hi": HI, "mode": mode,
                "left_partitions": LEFT_PARTS, "right_partitions": RIGHT_PARTS}]
    for m in msgs:
        if isinstance(m, Data):
            records.append({"type": "data", "side": m.side,
                            "partition": m.partition, "seq": m.seq,
                            "key": m.key, "time": m.time})
        elif isinstance(m, Watermark):
            records.append({"type": "watermark", "side": m.side,
                            "partition": m.partition, "w": m.w})
        else:
            records.append({"type": "end", "side": m.side,
                            "partition": m.partition})
    path = write_event_file(f"interleave_{mode}_{tag}", records)
    from interval_join import engine_from_config, run_event_file
    engine = engine_from_config(records[0], ":memory:")
    run_event_file(engine, path)
    results = list(engine.pull_results())
    late = list(engine.pull_late())
    status = engine.status
    pend = engine.pending()
    engine.close()
    assert status == "finished", (tag, status)
    assert pend["left_state"] == 0 and pend["right_state"] == 0, (tag, pend)
    assert late == [], f"{tag}: no event may be late in a legal schedule: {late}"
    return Counter(results)


def main() -> int:
    runs = 0
    for mode in ("inner", "left_outer", "full_outer"):
        want = reference(EVENTS, LO, HI, mode)
        schedules = [("bare0", [m for m in EVENTS])]  # declaration order
        for seed in (1, 2, 3):
            schedules.append((f"bare{seed}", interleave(seed)))
            schedules.append((f"safe-wm{seed}", with_safe_watermarks(interleave(seed))))
        for tag, order in schedules:
            got = run_stream(list(order) + ends(), mode, f"{tag}")
            assert got == want, (
                f"mode={mode} schedule={tag}\n"
                f"  missing: {list((want - got).elements())[:3]}\n"
                f"  extra  : {list((got - want).elements())[:3]}")
            runs += 1
        n_match = sum(1 for r in want if isinstance(r, Match))
        n_lpad = sum(1 for r in want if isinstance(r, LeftPad))
        n_rpad = sum(1 for r in want if isinstance(r, RightPad))
        print(f"  mode={mode:<10} reference: {n_match} matches, "
              f"{n_lpad} left pads, {n_rpad} right pads "
              f"({len(schedules)} schedules verified)")

    print(f"event files: {os.path.join(out_dir(), 'interleave_*.jsonl')}")
    print(f"PASS drill_interleave ({runs} runs x 3 modes, "
          f"multisets identical to offline enumeration)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
