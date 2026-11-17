"""Drill 8: hot-key delivery at scale with a real mid-delivery kill.

A runtime-generated stream with one hot key produces far more matches
than one delivery page (192_000 matches + 50 lpads + 30 rpads + 2_000
late records with the defaults below).  Input is submitted through the
Python API; delivery runs as separate CLI processes.  The first delivery
process is killed (simulated, exit 117) after the 37th page's target
commit but before its source ack; a second process resumes delivery with
no new input.

The drill reports and verifies:
  * independent expected counts and an order-independent content hash
    (computed by enumeration here, never by the engine);
  * sampled delivery identities (channel, offset -> record);
  * real peak RSS of the delivery process (memory follows the page size,
    not the result history);
  * both channels' acknowledged positions before the crash, after the
    crash and after the resume, plus the source-side reclamation counts.

Generated databases stay under drills/out/ (git-ignored); nothing large
is written into the repository.
"""

import hashlib
import json
import os
import sqlite3
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import out_dir
from interval_join import JobStore

LO, HI = -2, 5
SPAN = 60                 # distinct left time buckets
LEFT = 12_000             # hot left events (200 per bucket)
RIGHT = 120               # hot right events, times 10..29
LATE = 2_000              # late left events
COLD_L, COLD_R = 50, 30   # unmatched keys on each side (pads)
PAGE = 1_000
CRASH = 117
CRASH_AT_PAGE = 37

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(out_dir(), "deliver")
JOB_DB = os.path.join(OUT, "hotjob.db")
WH_DB = os.path.join(OUT, "hotwh.db")

EXPECTED_MATCHES = RIGHT * (HI - LO + 1) * (LEFT // SPAN)   # 192_000
# Left buckets whose window [b+LO, b+HI] misses every right time (10..29)
# never match: full_outer pads them.  Derived here from first principles.
_RIGHT_TIMES = {10 + (j % 20) for j in range(1, RIGHT + 1)}
_UNMATCHED_BUCKETS = [b for b in range(SPAN)
                      if not any(b + LO <= t <= b + HI for t in _RIGHT_TIMES)]
EXPECTED_LPADS = len(_UNMATCHED_BUCKETS) * (LEFT // SPAN) + COLD_L
EXPECTED_RESULTS = EXPECTED_MATCHES + EXPECTED_LPADS + COLD_R
EXPECTED_LATE = LATE


def canon_hash(records):
    """Order-independent content hash: sum of per-record sha256 ints."""
    total = 0
    for rec in records:
        total += int.from_bytes(
            hashlib.sha256(json.dumps(rec).encode()).digest(), "big")
    return total


def expected_records():
    """Independent enumeration of every expected record (matches by
    closed-form bucket arithmetic, pads and late by construction)."""
    for j in range(1, RIGHT + 1):
        t = 10 + (j % 20)
        # lefts matching right time t: l.time in [t-hi, t-lo]
        for b in range(t - HI, t - LO + 1):
            start = b if b else SPAN
            for i in range(start, LEFT + 1, SPAN):
                yield ("match", "lp0", i, "rp0", j, "hot", b, t)
    for b in _UNMATCHED_BUCKETS:      # hot lefts no right can reach
        start = b if b else SPAN
        for i in range(start, LEFT + 1, SPAN):
            yield ("lpad", "lp0", i, "hot", b)
    for i in range(COLD_L):
        yield ("lpad", "lp0", LEFT + 1 + i, f"coldL{i}", 10)
    for i in range(COLD_R):
        yield ("rpad", "rp0", RIGHT + 1 + i, f"coldR{i}", 10)
    for i in range(LATE):
        yield ("late", "L", "lp0", LEFT + COLD_L + 1 + i, "late", 1, 100)


def wh_records(path, job_id):
    """Canonical records actually delivered, plus per-kind counts."""
    conn = sqlite3.connect(path)
    kinds = {}
    recs = []
    offsets = {"results": [], "late": []}
    for row in conn.execute(
            "SELECT offset,kind,lp,ls,rp,rs,k,ltime,rtime FROM wh_results"
            " WHERE job_id=? ORDER BY offset", (job_id,)):
        off, kind, lp, ls, rp, rs, k, lt, rt = row
        kinds[kind] = kinds.get(kind, 0) + 1
        offsets["results"].append(off)
        if kind == "match":
            recs.append(("match", lp, int(ls), rp, int(rs), k,
                         int(lt), int(rt)))
        elif kind == "lpad":
            recs.append(("lpad", lp, int(ls), k, int(lt)))
        else:
            recs.append(("rpad", rp, int(rs), k, int(rt)))
    for row in conn.execute(
            "SELECT offset,side,partition,seq_txt,k,time_txt,wm_txt"
            " FROM wh_late WHERE job_id=? ORDER BY offset", (job_id,)):
        off, side, part, seq, k, t, wm_ = row
        kinds["late"] = kinds.get("late", 0) + 1
        offsets["late"].append(off)
        recs.append(("late", side, part, int(seq), k, int(t), int(wm_)))
    conn.close()
    return recs, kinds, offsets


def run_cli(args, env_extra=None, expect=0):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    p = subprocess.run([sys.executable, "-m", "interval_join.jobcli"] + args,
                       cwd=ROOT, env=env, capture_output=True, text=True)
    if p.returncode != expect:
        raise AssertionError(
            f"cli {args}\n  expected exit {expect}, got {p.returncode}\n"
            f"  stdout: {p.stdout[-2000:]}\n  stderr: {p.stderr[-2000:]}")
    return p


def cli_json(args, env_extra=None, expect=0):
    return json.loads(run_cli(args, env_extra, expect).stdout)


def status():
    return cli_json(["status", "--db", JOB_DB])


def fresh(path):
    for suffix in ("", "-wal", "-shm", ".lock"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)


def submit_input(job):
    """The whole input as consecutive batches through the Python API."""
    def data(side, part, seq, key, time):
        return {"type": "data", "side": side, "partition": part,
                "seq": seq, "key": key, "time": time}

    batches = []
    for start in range(0, LEFT, 1000):
        batches.append([data("L", "lp0", i + 1, "hot", (i + 1) % SPAN)
                        for i in range(start, min(start + 1000, LEFT))])
    batches.append([data("L", "lp0", LEFT + 1 + i, f"coldL{i}", 10)
                    for i in range(COLD_L)]
                   + [{"type": "watermark", "side": "L", "partition": "lp0",
                       "w": 100}])
    for start in range(0, LATE, 1000):
        batches.append([data("L", "lp0", LEFT + COLD_L + 1 + i, "late", 1)
                        for i in range(start, min(start + 1000, LATE))])
    batches.append([data("R", "rp0", j + 1, "hot", 10 + ((j + 1) % 20))
                    for j in range(RIGHT)])
    batches.append([data("R", "rp0", RIGHT + 1 + i, f"coldR{i}", 10)
                    for i in range(COLD_R)])
    batches.append([{"type": "end", "side": "L", "partition": "lp0"},
                    {"type": "end", "side": "R", "partition": "rp0"}])
    with job.modify():
        for no, msgs in enumerate(batches, 1):
            job.submit_batch({"batch_no": no, "messages": msgs})
    return len(batches)


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    fresh(JOB_DB)
    fresh(WH_DB)

    print(f"input: {LEFT} hot left + {RIGHT} hot right + {LATE} late + "
          f"{COLD_L}/{COLD_R} cold keys (lo={LO}, hi={HI}, full_outer)")
    print(f"independent expectation: {EXPECTED_MATCHES} matches, "
          f"{EXPECTED_LPADS} lpads ({len(_UNMATCHED_BUCKETS)} unreachable "
          f"hot buckets + {COLD_L} cold), {COLD_R} rpads, "
          f"{EXPECTED_LATE} late")

    job = JobStore.create(JOB_DB, lo=LO, hi=HI, mode="full_outer",
                          left_partitions=["lp0"], right_partitions=["rp0"])
    job_id = job.job_id
    n_batches = submit_input(job)
    job.close()
    st = status()
    assert st["processing"]["status"] == "finished"
    assert st["delivery"]["results"]["pending"] == EXPECTED_RESULTS
    assert st["delivery"]["late"]["pending"] == EXPECTED_LATE
    print(f"input committed: {n_batches} batches, "
          f"pending results={st['delivery']['results']['pending']} "
          f"late={st['delivery']['late']['pending']}")

    # ---- first delivery process: killed mid-delivery (after page 37's
    #      target commit, before its source ack)
    run_cli(["deliver", "--db", JOB_DB, "--warehouse", WH_DB,
             "--page-size", str(PAGE)],
            {"TWOSTREAM_CRASH_AT":
             f"deliver.post_target_commit@{CRASH_AT_PAGE}"}, expect=CRASH)
    st = status()
    ack_mid = (st["delivery"]["results"]["acked_offset"],
               st["delivery"]["late"]["acked_offset"])
    assert ack_mid == ((CRASH_AT_PAGE - 1) * PAGE, 0), ack_mid
    print(f"killed after page {CRASH_AT_PAGE}'s target commit: "
          f"acked results={ack_mid[0]} late={ack_mid[1]}")

    # ---- resume delivery with no new input
    summary = cli_json(["deliver", "--db", JOB_DB, "--warehouse", WH_DB,
                        "--page-size", str(PAGE)])
    st = status()
    ack_end = (st["delivery"]["results"]["acked_offset"],
               st["delivery"]["late"]["acked_offset"])
    reclaimed = (st["delivery"]["results"]["reclaimed"],
                 st["delivery"]["late"]["reclaimed"])
    assert ack_end == (EXPECTED_RESULTS, EXPECTED_LATE), ack_end
    assert reclaimed == ack_end, reclaimed
    assert st["warehouse_complete"] is True
    ch = summary["channels"]
    assert ch["results"]["reused_receipts"] == 1, ch
    rss = summary["peak_rss_kb"]
    print(f"resumed with no new input: pages={ch['results']['pages']}+"
          f"{ch['late']['pages']} rows={ch['results']['rows']}+"
          f"{ch['late']['rows']} (1 orphan receipt healed)")
    print(f"acked before/after resume: {ack_mid} -> {ack_end}; "
          f"source reclaimed: results={reclaimed[0]} late={reclaimed[1]}")
    print(f"peak RSS of the delivery process: {rss} kB "
          f"(page size {PAGE}, independent of the {EXPECTED_RESULTS}-row "
          f"history)")
    assert rss < 400_000, f"delivery memory looks unbounded: {rss} kB"

    # ---- verify warehouse content against the independent enumeration
    recs, kinds, offsets = wh_records(WH_DB, job_id)
    assert kinds.get("match", 0) == EXPECTED_MATCHES, kinds
    assert kinds.get("lpad", 0) == EXPECTED_LPADS, kinds
    assert kinds.get("rpad", 0) == COLD_R, kinds
    assert kinds.get("late", 0) == EXPECTED_LATE, kinds
    assert offsets["results"] == list(range(1, EXPECTED_RESULTS + 1))
    assert offsets["late"] == list(range(1, EXPECTED_LATE + 1))
    got_hash = canon_hash(recs)
    want_hash = canon_hash(expected_records())
    assert got_hash == want_hash, "warehouse content hash != enumeration"
    print(f"warehouse verified: {sum(kinds.values())} rows, kinds={kinds}, "
          f"offsets contiguous 1..N, content hash matches the independent "
          f"enumeration")

    # ---- sampled delivery identities
    conn = sqlite3.connect(WH_DB)
    samples = []
    for off in (1, ack_mid[0], EXPECTED_RESULTS // 2, EXPECTED_RESULTS):
        row = conn.execute(
            "SELECT kind,lp,ls,rp,rs,k,ltime,rtime FROM wh_results"
            " WHERE job_id=? AND offset=?", (job_id, off)).fetchone()
        kind, lp, ls, rp, rs, k, lt, rt = row
        samples.append(f"results#{off}={kind}({lp}#{ls},{rp}#{rs},{k},"
                       f"{lt}->{rt})")
        if kind == "match":
            assert k == "hot"
            assert int(lt) + LO <= int(rt) <= int(lt) + HI
    row = conn.execute(
        "SELECT side,partition,seq_txt,k,time_txt,wm_txt FROM wh_late"
        " WHERE job_id=? AND offset=?", (job_id, EXPECTED_LATE)).fetchone()
    samples.append(f"late#{EXPECTED_LATE}={row[0]}/{row[1]}#{row[2]}"
                   f"({row[3]}@{row[4]}, wm={row[5]})")
    conn.close()
    print("sampled identities:")
    for s in samples:
        print(f"  {s}")

    print(f"\nPASS drill_deliver ({EXPECTED_RESULTS} results + "
          f"{EXPECTED_LATE} late delivered through a real mid-delivery "
          f"kill; db files under {OUT} are git-ignored)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
