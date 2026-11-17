"""Drill 1 (main example): lo=-2, hi=5, full_outer.

Left L@10 arrives first.  Right partitions rp0/rp1 hold watermarks
15 and 30, so right_min = 15 is NOT strictly greater than L.time+hi=15:
L must not be null-padded.  R@15 then matches L (inclusive endpoint).
Only when rp0 advances to 16 (right_min = 16 > 15) is L sealed; since L
matched, no pad is emitted and L is reclaimed.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import counts, db_size, fmt_wm, run_drill
from interval_join import EventId, Match

RECORDS = [
    {"type": "config", "lo": -2, "hi": 5, "mode": "full_outer",
     "left_partitions": ["lp0"], "right_partitions": ["rp0", "rp1"]},
    {"type": "watermark", "side": "R", "partition": "rp0", "w": 15},
    {"type": "watermark", "side": "R", "partition": "rp1", "w": 30},
    {"type": "barrier", "name": "wm-set"},
    {"type": "data", "side": "L", "partition": "lp0", "seq": 1,
     "key": "dev-1", "time": 10, "payload": {"kind": "request"}},
    {"type": "barrier", "name": "L-arrived"},
    {"type": "data", "side": "R", "partition": "rp0", "seq": 1,
     "key": "dev-1", "time": 15, "payload": {"kind": "response"}},
    {"type": "barrier", "name": "R-arrived"},
    {"type": "watermark", "side": "R", "partition": "rp0", "w": 16},
    {"type": "barrier", "name": "L-sealed"},
    {"type": "end", "side": "L", "partition": "lp0"},
    {"type": "end", "side": "R", "partition": "rp0"},
    {"type": "end", "side": "R", "partition": "rp1"},
]

L = EventId("L", "lp0", 1)
R = EventId("R", "rp0", 1)


def main() -> int:
    checks = []

    def at_wm_set(engine, results, late):
        wm = engine.watermarks()
        assert fmt_wm(wm["right_min"]) == "15", wm
        checks.append("right_min watermark is min(15,30)=15")

    def at_L_arrived(engine, results, late):
        # right_min=15 is not strictly greater than 10+5=15: no pad.
        assert results == [], f"L must not be null-padded yet: {results}"
        assert engine.pending()["left_state"] == 1
        checks.append("L@10 pending, no (L,null) while right_min=15")

    def at_R_arrived(engine, results, late):
        assert results == [Match(L, R, "dev-1", 10, 15)], results
        assert engine.pending()["left_state"] == 1  # matched but not reclaimed
        checks.append("(L@10, R@15) emitted on R's arrival (endpoint hi)")

    def at_L_sealed(engine, results, late):
        # right_min=16 > 15: L sealed.  It matched, so no pad; it is reclaimed.
        assert len(results) == 1, f"no pad expected for matched L: {results}"
        assert engine.pending()["left_state"] == 0
        checks.append("watermark 16 seals L: reclaimed, still no pad")

    engine, results, late, path = run_drill(
        "main", RECORDS,
        {"wm-set": at_wm_set, "L-arrived": at_L_arrived,
         "R-arrived": at_R_arrived, "L-sealed": at_L_sealed},
        db_name="main.db")

    assert engine.status == "finished", engine.status
    assert results == [Match(L, R, "dev-1", 10, 15)], results
    assert late == [], late
    assert engine.pending()["left_state"] == 0
    assert engine.pending()["right_state"] == 0
    size = db_size(engine)
    engine.close()

    print(f"event file : {path}")
    for c in checks:
        print(f"  ok       : {c}")
    print(f"totals     : {counts(results)}, late={len(late)}, "
          f"status=finished, sqlite={size}B")
    print("PASS drill_main")
    return 0


if __name__ == "__main__":
    sys.exit(main())
