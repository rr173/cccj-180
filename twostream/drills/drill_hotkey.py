"""Drill 6: hot key on disk, large input, real RSS and reclamation counts.

One key ("hot") with far more candidates than memory-friendly buffering
would allow.  State lives in a SQLite file; results stream out of the
outbox and are counted without ever materialising the match list.

Defaults: 30_000 left events (times cycling over 60 distinct values,
500 per value) and 200 right events (times 10..29).  With lo=-2, hi=5
each right matches the 8 left time buckets [t-5, t+2], i.e.
200 * 8 * 500 = 800_000 matches.  The drill prints the match count,
sampled identities, real RSS (VmRSS), and state row counts before and
after the watermark proof reclaims everything.
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import out_dir
from interval_join import Data, JoinEngine, Match

LO, HI = -2, 5
SPAN = 60           # distinct left time values
RIGHT_BASE = 10     # right times: RIGHT_BASE .. RIGHT_BASE+RIGHT_SPAN-1
RIGHT_SPAN = 20


def rss_kb() -> int:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1])
    except OSError:
        pass
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", type=int, default=30_000)
    ap.add_argument("--right", type=int, default=200)
    args = ap.parse_args()
    n_left, n_right = args.left, args.right
    assert n_left % SPAN == 0
    per_bucket = n_left // SPAN
    expected = n_right * (HI - LO + 1) * per_bucket

    db_path = os.path.join(out_dir(), "hotkey.db")
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(db_path + suffix):
            os.remove(db_path + suffix)

    t0 = time.time()
    total = 0
    samples = []
    outbox_highwater = 0
    with JoinEngine(db_path, LO, HI, "inner", ["lp0"], ["rp0"]) as engine:
        # ---- load the left stream (batched transactions)
        for start in range(0, n_left, 2000):
            engine.feed_batch(
                Data("L", "lp0", i + 1, "hot", i % SPAN, {"i": i})
                for i in range(start, min(start + 2000, n_left)))

        # ---- stream the right stream; drain the outbox between arrivals
        def drain():
            nonlocal total
            for r in engine.pull_results():
                if len(samples) < 5:
                    samples.append(r)
                total += 1

        for j in range(n_right):
            engine.feed_data("R", "rp0", j + 1, "hot",
                             RIGHT_BASE + j % RIGHT_SPAN, {"j": j})
            if j % 25 == 24:
                outbox_highwater = max(outbox_highwater, engine.pending()["outbox"])
                drain()
        drain()

        pend_before = engine.pending()
        rss_before = rss_kb()

        # ---- watermark proof: end() everywhere -> all state reclaimed
        engine.feed_end("L", "lp0")
        engine.feed_end("R", "rp0")
        drain()

        pend_after = engine.pending()
        rss_after = rss_kb()
        status = engine.status
        late = list(engine.pull_late())

    elapsed = time.time() - t0
    db_bytes = os.path.getsize(db_path)

    assert total == expected, (total, expected)
    assert status == "finished", status
    assert late == [], late
    assert pend_after["left_state"] == 0 and pend_after["right_state"] == 0
    assert pend_after["matched_marks"] == 0
    for s in samples:
        assert isinstance(s, Match) and s.key == "hot"
        assert s.left_time + LO <= s.right_time <= s.left_time + HI

    print(f"db file        : {db_path} ({db_bytes/1e6:.1f} MB)")
    print(f"input          : {n_left} left + {n_right} right events, key='hot'")
    print(f"matches        : {total} (expected {expected})")
    print(f"sample ids     : " + "; ".join(
        f"({s.left},{s.right})@{s.left_time}->{s.right_time}" for s in samples))
    print(f"outbox backlog : <= {outbox_highwater} rows between drains")
    print(f"state before GC: left={pend_before['left_state']} "
          f"right={pend_before['right_state']}  RSS={rss_before} kB")
    print(f"state after GC : left={pend_after['left_state']} "
          f"right={pend_after['right_state']} marks={pend_after['matched_marks']}"
          f"  RSS={rss_after} kB")
    print(f"elapsed        : {elapsed:.1f}s")
    print("PASS drill_hotkey")
    return 0


if __name__ == "__main__":
    sys.exit(main())
