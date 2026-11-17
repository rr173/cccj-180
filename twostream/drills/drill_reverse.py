"""Drill 3 (reverse example): right R@20 waits for the left bound 22.

lo=-2, hi=5, full_outer.  A right row r can only be null-padded once the
left side proves its window [r.time-hi, r.time-lo] closed, i.e.
left_min_wm > r.time - lo = 20 - (-2) = 22.  So left_min = 22 does NOT
seal R@20 (strictly greater is required); 23 does.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import counts, db_size, run_drill
from interval_join import EventId, RightPad

RECORDS = [
    {"type": "config", "lo": -2, "hi": 5, "mode": "full_outer",
     "left_partitions": ["lp0", "lp1"], "right_partitions": ["rp0"]},
    {"type": "watermark", "side": "L", "partition": "lp0", "w": 0},
    {"type": "watermark", "side": "L", "partition": "lp1", "w": 0},
    {"type": "data", "side": "R", "partition": "rp0", "seq": 1,
     "key": "dev-2", "time": 20},
    {"type": "barrier", "name": "R-waiting"},
    {"type": "watermark", "side": "L", "partition": "lp0", "w": 22},
    {"type": "watermark", "side": "L", "partition": "lp1", "w": 22},
    {"type": "barrier", "name": "at-22"},
    {"type": "watermark", "side": "L", "partition": "lp0", "w": 23},
    {"type": "watermark", "side": "L", "partition": "lp1", "w": 23},
    {"type": "barrier", "name": "sealed"},
    {"type": "end", "side": "L", "partition": "lp0"},
    {"type": "end", "side": "L", "partition": "lp1"},
    {"type": "end", "side": "R", "partition": "rp0"},
]

R = EventId("R", "rp0", 1)


def main() -> int:
    checks = []

    def waiting(engine, results, late):
        assert results == [], results
        assert engine.pending()["right_state"] == 1
        checks.append("R@20 pending while left_min=0")

    def at_22(engine, results, late):
        # left_min = 22 is not strictly greater than 20-(-2) = 22.
        assert results == [], f"22 must not seal R@20: {results}"
        assert engine.pending()["right_state"] == 1
        checks.append("left_min=22 still not enough (need > 22)")

    def sealed(engine, results, late):
        assert results == [RightPad(R, "dev-2", 20)], results
        assert engine.pending()["right_state"] == 0
        checks.append("left_min=23 seals R@20 -> (null, R@20)")

    engine, results, late, path = run_drill(
        "reverse", RECORDS,
        {"R-waiting": waiting, "at-22": at_22, "sealed": sealed},
        db_name="reverse.db")

    assert engine.status == "finished", engine.status
    assert results == [RightPad(R, "dev-2", 20)], results
    assert late == [], late
    size = db_size(engine)
    engine.close()

    print(f"event file : {path}")
    for c in checks:
        print(f"  ok       : {c}")
    print(f"totals     : {counts(results)}, late={len(late)}, "
          f"status=finished, sqlite={size}B")
    print("PASS drill_reverse")
    return 0


if __name__ == "__main__":
    sys.exit(main())
