"""Drill 2: matched does NOT mean reclaimable.

Same window (lo=-2, hi=5).  Before the right watermark rises past 15,
R2@12 arrives first and matches L@10; L stays in state (a matched event
can still pair with later legal events).  R@15 then arrives and matches
L as well -> two matches.  Only the watermark proof (rp0 -> 16) reclaims
L, and because L matched, no null pad is emitted.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import counts, db_size, run_drill
from interval_join import EventId, Match

RECORDS = [
    {"type": "config", "lo": -2, "hi": 5, "mode": "full_outer",
     "left_partitions": ["lp0"], "right_partitions": ["rp0", "rp1"]},
    {"type": "watermark", "side": "R", "partition": "rp0", "w": 0},
    {"type": "watermark", "side": "R", "partition": "rp1", "w": 30},
    {"type": "data", "side": "L", "partition": "lp0", "seq": 1,
     "key": "dev-1", "time": 10},
    {"type": "data", "side": "R", "partition": "rp0", "seq": 1,
     "key": "dev-1", "time": 12},
    {"type": "barrier", "name": "after-R2"},
    {"type": "data", "side": "R", "partition": "rp0", "seq": 2,
     "key": "dev-1", "time": 15},
    {"type": "barrier", "name": "after-R"},
    {"type": "watermark", "side": "R", "partition": "rp0", "w": 16},
    {"type": "barrier", "name": "sealed"},
    {"type": "end", "side": "L", "partition": "lp0"},
    {"type": "end", "side": "R", "partition": "rp0"},
    {"type": "end", "side": "R", "partition": "rp1"},
]

L = EventId("L", "lp0", 1)
R2 = EventId("R", "rp0", 1)
R = EventId("R", "rp0", 2)


def main() -> int:
    checks = []

    def after_r2(engine, results, late):
        assert results == [Match(L, R2, "dev-1", 10, 12)], results
        assert engine.pending()["left_state"] == 1, \
            "matched L must remain in state (not reclaimable yet)"
        checks.append("(L,R2@12) emitted; matched L still pending")

    def after_r(engine, results, late):
        assert results[-1] == Match(L, R, "dev-1", 10, 15), results
        assert engine.pending()["left_state"] == 1
        checks.append("(L,R@15) emitted: second match for the same L")

    def sealed(engine, results, late):
        assert engine.pending()["left_state"] == 0
        assert len(results) == 2, f"matched L must not be padded: {results}"
        checks.append("watermark 16 reclaims L with no pad")

    engine, results, late, path = run_drill(
        "two_matches", RECORDS,
        {"after-R2": after_r2, "after-R": after_r, "sealed": sealed},
        db_name="two_matches.db")

    assert engine.status == "finished", engine.status
    assert results == [Match(L, R2, "dev-1", 10, 12),
                       Match(L, R, "dev-1", 10, 15)], results
    assert late == [], late
    size = db_size(engine)
    engine.close()

    print(f"event file : {path}")
    for c in checks:
        print(f"  ok       : {c}")
    print(f"totals     : {counts(results)}, late={len(late)}, "
          f"status=finished, sqlite={size}B")
    print("PASS drill_two_matches")
    return 0


if __name__ == "__main__":
    sys.exit(main())
