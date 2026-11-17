"""Drill 4 (barrier): a silent partition blocks sealing.

left_outer, lo=-2, hi=5.  Right partition rp0 pushes a large watermark
(1000, then 2000) but rp1 stays silent, so right_min = -inf and L@10
cannot be null-padded: one partition's big watermark never substitutes
for another partition's proof.  Only when rp1 reports (w=100) does
right_min become 100 > 15 and L is sealed with (L, null).

Barriers in the event file drive the checkpoints below.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import counts, db_size, fmt_wm, run_drill
from interval_join import EventId, LeftPad

RECORDS = [
    {"type": "config", "lo": -2, "hi": 5, "mode": "left_outer",
     "left_partitions": ["lp0"], "right_partitions": ["rp0", "rp1"]},
    {"type": "watermark", "side": "R", "partition": "rp0", "w": 1000},
    {"type": "data", "side": "L", "partition": "lp0", "seq": 1,
     "key": "dev-9", "time": 10},
    {"type": "barrier", "name": "silent-rp1"},
    {"type": "watermark", "side": "R", "partition": "rp0", "w": 2000},
    {"type": "barrier", "name": "big-wm-not-enough"},
    {"type": "watermark", "side": "R", "partition": "rp1", "w": 100},
    {"type": "barrier", "name": "now-sealed"},
    {"type": "end", "side": "L", "partition": "lp0"},
    {"type": "end", "side": "R", "partition": "rp0"},
    {"type": "end", "side": "R", "partition": "rp1"},
]

L = EventId("L", "lp0", 1)


def main() -> int:
    checks = []

    def silent(engine, results, late):
        wm = engine.watermarks()
        assert fmt_wm(wm["right_min"]) == "-inf", wm
        assert results == [], f"silent rp1 must block sealing: {results}"
        assert engine.pending()["left_state"] == 1
        checks.append("rp1 silent: right_min=-inf, L@10 waits despite rp0=1000")

    def big_wm(engine, results, late):
        assert results == [], f"rp0=2000 still cannot seal: {results}"
        checks.append("rp0=2000 alone still cannot seal L")

    def sealed(engine, results, late):
        assert results == [LeftPad(L, "dev-9", 10)], results
        assert engine.pending()["left_state"] == 0
        checks.append("rp1 reports 100: right_min=100 > 15 -> (L, null)")

    engine, results, late, path = run_drill(
        "barrier", RECORDS,
        {"silent-rp1": silent, "big-wm-not-enough": big_wm,
         "now-sealed": sealed},
        db_name="barrier.db")

    assert engine.status == "finished", engine.status
    assert results == [LeftPad(L, "dev-9", 10)], results
    assert late == [], late
    size = db_size(engine)
    engine.close()

    print(f"event file : {path}")
    for c in checks:
        print(f"  ok       : {c}")
    print(f"totals     : {counts(results)}, late={len(late)}, "
          f"status=finished, sqlite={size}B")
    print("PASS drill_barrier")
    return 0


if __name__ == "__main__":
    sys.exit(main())
