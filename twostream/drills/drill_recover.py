"""Drill 7: recoverable job + acknowledged delivery across real process crashes.

Business acceptance for the persistent job store.  Two jobs run the SAME
deterministic batch schedule (full_outer, null keys, a >64-bit time, one
late event, cold keys on both sides):

  * job A ("clean")   -- no failures: create, input, deliver;
  * job B ("crashed") -- real subprocess exits (simulated kills, exit 117)
                         at every handoff point: before a batch commit,
                         after the commit but before the receipt returns,
                         after a page's target commit but before the source
                         ack, plus a target-write failure before commit.

The expected results (matches, pads, late) are computed by an independent
offline enumeration over the message schedule -- not by the engine.  The
final warehouse contents of both jobs must equal that reference exactly,
and job B's delivery identities (channel, offset -> record) must be
identical to job A's.

Also demonstrated, each at least once:
  * an illegal message late in a batch rolls the whole batch back
    (in-memory ledgers and disk), and the next legal batch is accepted;
  * resending a committed batch with different content is a conflict;
  * a credential from another channel / another job, or with a forged
    end, is rejected;
  * two modifiers -> busy (exit 3); after SIGKILL the lock is taken over
    with no manual cleanup;
  * switching the warehouse identity and a conflicting warehouse row both
    refuse to advance the source acknowledgement position.

Failure points are driven by the TWOSTREAM_CRASH_AT test hook; the normal
path never depends on it.
"""

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.common import out_dir
from interval_join import SQLiteWarehouse

LO, HI = -2, 5
LEFT_PARTS = ["lp0", "lp1"]
RIGHT_PARTS = ["rp0", "rp1"]
BIG = 2 ** 70 + 10
CRASH = 117          # exit code of the simulated-kill test hook
PAGE_SIZE = 3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(out_dir(), "recover")


def d(side, part, seq, key, time):
    return {"type": "data", "side": side, "partition": part, "seq": seq,
            "key": key, "time": time}


def w(side, part, wm):
    return {"type": "watermark", "side": side, "partition": part, "w": wm}


def e(side, part):
    return {"type": "end", "side": side, "partition": part}


# The deterministic batch schedule.  B6_BAD carries an illegal third
# message (seq 4 after seq 7 on lp0); B6 is its legal replacement.
B1 = [d("L", "lp0", 1, "dev-1", 10), d("L", "lp0", 2, "dev-2", 20),
      d("L", "lp1", 1, "dev-1", 11), d("L", "lp1", 2, "dev-big", BIG),
      d("L", "lp0", 3, None, 15), w("L", "lp0", 5)]
B2 = [d("R", "rp0", 1, "dev-1", 12), d("R", "rp0", 2, "dev-9", 50),
      d("R", "rp0", 3, "dev-big", BIG + 3), d("R", "rp1", 1, None, 8),
      w("R", "rp0", 12)]
B3 = [d("L", "lp0", 4, "dev-1", 9), d("L", "lp0", 5, "dev-3", 4),
      d("R", "rp1", 2, "dev-2", 21), w("L", "lp1", 100)]
B4 = [d("R", "rp0", 4, "dev-1", 13), d("L", "lp1", 3, "dev-4", 200),
      w("R", "rp1", 25)]
B5 = [d("R", "rp1", 3, "dev-2", 22), d("L", "lp0", 6, "dev-5", 6),
      w("R", "rp0", 30)]
B6_BAD = [d("L", "lp0", 7, "dev-1", 8), w("L", "lp0", 6),
          d("L", "lp0", 4, "dev-x", 1)]
B6 = [d("L", "lp0", 7, "dev-1", 8), w("L", "lp0", 6)]
B7 = [e("L", "lp0"), e("L", "lp1"), e("R", "rp0"), e("R", "rp1")]
LEGAL_BATCHES = [B1, B2, B3, B4, B5, B6, B7]


def reference(batches):
    """Independent offline enumeration: matches, pads and late records
    implied by the message schedule (never touches the engine)."""
    wms = {}
    ontime, late = [], []
    for msgs in batches:
        for m in msgs:
            if m["type"] == "data":
                cur = wms.get((m["side"], m["partition"]))
                if cur is not None and m["time"] < cur:
                    late.append((m, cur))
                else:
                    ontime.append(m)
            elif m["type"] == "watermark":
                key = (m["side"], m["partition"])
                wms[key] = m["w"] if key not in wms else max(wms[key], m["w"])
    results = []
    l_hit, r_hit = set(), set()
    lefts = [m for m in ontime if m["side"] == "L"]
    rights = [m for m in ontime if m["side"] == "R"]
    for l in lefts:
        if l["key"] is None:
            continue
        for r in rights:
            if r["key"] is None:
                continue
            if l["key"] == r["key"] \
                    and l["time"] + LO <= r["time"] <= l["time"] + HI:
                results.append(("match", l["partition"], l["seq"],
                                r["partition"], r["seq"],
                                l["key"], l["time"], r["time"]))
                l_hit.add((l["partition"], l["seq"]))
                r_hit.add((r["partition"], r["seq"]))
    for l in lefts:  # full_outer: pad every unmatched row on both sides
        if (l["partition"], l["seq"]) not in l_hit:
            results.append(("lpad", l["partition"], l["seq"],
                            l["key"], l["time"]))
    for r in rights:
        if (r["partition"], r["seq"]) not in r_hit:
            results.append(("rpad", r["partition"], r["seq"],
                            r["key"], r["time"]))
    late_recs = [("late", m["side"], m["partition"], m["seq"], m["key"],
                  m["time"], cur) for m, cur in late]
    return results, late_recs


# ------------------------------------------------------------------ helpers
def run_cli(args, env_extra=None, expect=0):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    p = subprocess.run([sys.executable, "-m", "interval_join.jobcli"] + args,
                       cwd=ROOT, env=env, capture_output=True, text=True)
    if p.returncode != expect:
        raise AssertionError(
            f"cli {args}\n  expected exit {expect}, got {p.returncode}\n"
            f"  stdout: {p.stdout}\n  stderr: {p.stderr}")
    return p


def cli_json(args, env_extra=None, expect=0):
    return json.loads(run_cli(args, env_extra, expect).stdout)


def write_batch(name, batch_no, messages):
    path = os.path.join(OUT, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"batch_no": batch_no, "messages": messages}, f)
    return path


def status(db):
    return cli_json(["status", "--db", db])


def ledger_receipt(db, batch_no):
    conn = sqlite3.connect(db)
    row = conn.execute("SELECT receipt_json FROM batch_ledger"
                       " WHERE batch_no=?", (batch_no,)).fetchone()
    conn.close()
    return json.loads(row[0]) if row else None


def dump_warehouse(path, job_id):
    """offset -> canonical record, for both channels."""
    conn = sqlite3.connect(path)
    results = {}
    for row in conn.execute(
            "SELECT offset,kind,lp,ls,rp,rs,k,ltime,rtime FROM wh_results"
            " WHERE job_id=? ORDER BY offset", (job_id,)):
        off, kind, lp, ls, rp, rs, k, lt, rt = row
        if kind == "match":
            results[off] = ("match", lp, int(ls), rp, int(rs), k,
                            int(lt), int(rt))
        elif kind == "lpad":
            results[off] = ("lpad", lp, int(ls), k, int(lt))
        else:
            results[off] = ("rpad", rp, int(rs), k, int(rt))
    late = {}
    for row in conn.execute(
            "SELECT offset,side,partition,seq_txt,k,time_txt,wm_txt"
            " FROM wh_late WHERE job_id=? ORDER BY offset", (job_id,)):
        off, side, part, seq, k, t, wm_ = row
        late[off] = ("late", side, part, int(seq), k, int(t), int(wm_))
    conn.close()
    return results, late


CHECKS = []


def ok(msg):
    CHECKS.append(msg)
    print(f"  ok       : {msg}")


HOLDER_SRC = r"""
import sys, time
sys.path.insert(0, ".")
from interval_join import JobStore
job = JobStore.open(sys.argv[1])
job.acquire()
print("LOCK-HELD", flush=True)
time.sleep(120)
"""


def fresh(path):
    for suffix in ("", "-wal", "-shm", ".lock"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)


def create_job(db):
    return cli_json(["create", "--db", db, "--lo", str(LO), "--hi", str(HI),
                     "--mode", "full_outer",
                     "--left-partitions"] + LEFT_PARTS +
                    ["--right-partitions"] + RIGHT_PARTS)


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    batch_files = [write_batch(f"b{i}.json", i, b)
                   for i, b in enumerate(LEGAL_BATCHES, 1)]
    bad_file = write_batch("b6_bad.json", 6, B6_BAD)

    ref_results, ref_late = reference(LEGAL_BATCHES)
    n_match = sum(1 for r in ref_results if r[0] == "match")
    print(f"reference (independent enumeration): {n_match} matches, "
          f"{sum(1 for r in ref_results if r[0] == 'lpad')} lpads, "
          f"{sum(1 for r in ref_results if r[0] == 'rpad')} rpads, "
          f"{len(ref_late)} late")

    # ------------------------------------------------ Phase A: clean run
    print("phase A: uninterrupted run")
    db_a = os.path.join(OUT, "jobA.db")
    wh_a = os.path.join(OUT, "whA.db")
    fresh(db_a)
    fresh(wh_a)
    job_a = create_job(db_a)["job_id"]
    for bf in batch_files:
        cli_json(["input", "--db", db_a, "--batch", bf])
    # keep a credential of job A for the cross-job rejection check
    cred_a = cli_json(["pages", "--db", db_a, "--channel", "results",
                       "--limit", str(PAGE_SIZE)])["credential"]
    cred_a_path = os.path.join(OUT, "credA.json")
    with open(cred_a_path, "w", encoding="utf-8") as f:
        json.dump(cred_a, f)
    cli_json(["deliver", "--db", db_a, "--warehouse", wh_a,
              "--page-size", str(PAGE_SIZE)])
    st_a = status(db_a)
    assert st_a["warehouse_complete"] is True, st_a
    res_a, late_a = dump_warehouse(wh_a, job_a)
    assert sorted(res_a) == list(range(1, len(res_a) + 1)), "offsets 1..N"
    assert sorted(late_a) == list(range(1, len(late_a) + 1))
    assert Counter(res_a.values()) == Counter(ref_results), (
        "clean run content != reference",
        list((Counter(ref_results) - Counter(res_a.values())).elements())[:3])
    assert Counter(late_a.values()) == Counter(ref_late)
    ok(f"clean run: warehouse == reference "
       f"({len(res_a)} results, {len(late_a)} late), offsets contiguous")
    big_rows = [r for r in res_a.values() if r[0] == "match"
                and r[6] > 2 ** 63]
    assert big_rows and big_rows[0][6] == BIG and big_rows[0][7] == BIG + 3, \
        big_rows
    ok(f">64-bit time survived persistence exactly: "
       f"ltime={BIG}, rtime={BIG + 3}")

    # --------------------------------------- Phase B: crashed & resumed run
    print("phase B: real subprocess crashes at every handoff point")
    db_b = os.path.join(OUT, "jobB.db")
    wh_b = os.path.join(OUT, "whB.db")
    fresh(db_b)
    fresh(wh_b)
    job_b = create_job(db_b)["job_id"]

    cli_json(["input", "--db", db_b, "--batch", batch_files[0]])

    # (1) killed before the batch commit: nothing of batch 2 may survive
    run_cli(["input", "--db", db_b, "--batch", batch_files[1]],
            {"TWOSTREAM_CRASH_AT": "batch.pre_commit"}, expect=CRASH)
    st = status(db_b)
    assert st["batches"]["last"] == 1, st["batches"]
    r2 = cli_json(["input", "--db", db_b, "--batch", batch_files[1]])
    assert r2["batch_no"] == 2
    ok("crash before batch commit: batch absent, resend accepted "
       "(seq/watermark ledgers rolled back)")

    cli_json(["input", "--db", db_b, "--batch", batch_files[2]])

    # (2) killed after the commit but before the receipt returned
    run_cli(["input", "--db", db_b, "--batch", batch_files[3]],
            {"TWOSTREAM_CRASH_AT": "batch.post_commit"}, expect=CRASH)
    st = status(db_b)
    assert st["batches"]["last"] == 4, st["batches"]
    stored = ledger_receipt(db_b, 4)
    max_before = st["delivery"]["results"]["max_offset"]
    r4 = cli_json(["input", "--db", db_b, "--batch", batch_files[3]])
    assert r4 == stored, "resend must return the original committed receipt"
    st = status(db_b)
    assert st["delivery"]["results"]["max_offset"] == max_before
    ok("crash after commit before receipt: resend returns the stored "
       "receipt without re-pairing (no new offsets)")

    cli_json(["input", "--db", db_b, "--batch", batch_files[4]])

    # (3) illegal message late in a batch: whole batch rolls back
    max_before = status(db_b)["delivery"]["results"]["max_offset"]
    run_cli(["input", "--db", db_b, "--batch", bad_file], expect=1)
    st = status(db_b)
    assert st["batches"]["last"] == 5, st["batches"]
    assert st["delivery"]["results"]["max_offset"] == max_before
    assert st["processing"]["left"]["lp0"]["watermark"] == 5  # not 6
    r6 = cli_json(["input", "--db", db_b, "--batch", batch_files[5]])
    assert r6["batch_no"] == 6
    ok("in-batch illegal message after legal data/watermark: batch fully "
       "rolled back (no seq/wm latch, no match), legal batch 6 accepted")

    cli_json(["input", "--db", db_b, "--batch", batch_files[6]])
    st = status(db_b)
    assert st["processing"]["status"] == "finished"

    # finished job: new batches rejected, old ones still answer
    run_cli(["input", "--db", db_b, "--batch",
             write_batch("b8.json", 8, [d("L", "lp0", 8, "x", 1)])], expect=1)
    r1 = cli_json(["input", "--db", db_b, "--batch", batch_files[0]])
    assert r1 == ledger_receipt(db_b, 1)
    ok("after all ends: new batch rejected, historical batch resend "
       "returns the original receipt")

    # (4) same batch number, different content -> conflict
    tampered = write_batch("b5_tampered.json", 5,
                           [d("R", "rp1", 3, "dev-2", 22),
                            d("L", "lp0", 6, "dev-5", 6),
                            w("R", "rp0", 31)])   # 31 instead of 30
    run_cli(["input", "--db", db_b, "--batch", tampered], expect=1)
    assert status(db_b)["batches"]["last"] == 7
    r5 = cli_json(["input", "--db", db_b, "--batch", batch_files[4]])
    assert r5 == ledger_receipt(db_b, 5)
    ok("same batch_no with different content rejected; identical resend "
       "still returns the original receipt")

    # (5) two modifiers -> busy; SIGKILL -> takeover without cleanup
    holder = subprocess.Popen([sys.executable, "-c", HOLDER_SRC, db_b],
                              cwd=ROOT, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "LOCK-HELD"
        run_cli(["input", "--db", db_b, "--batch", batch_files[0]], expect=3)
        run_cli(["deliver", "--db", db_b, "--warehouse", wh_b], expect=3)
        st = status(db_b)   # read-only status works while locked
        assert st["locked"] is True and st["batches"]["last"] == 7
        ok("second modifier gets busy (exit 3); read-only status still "
           "shows the committed state")
    finally:
        holder.send_signal(signal.SIGKILL)
        holder.wait()
    r1 = cli_json(["input", "--db", db_b, "--batch", batch_files[0]])
    assert r1["batch_no"] == 1
    ok("holder killed with SIGKILL: lock released by the kernel, "
       "successor took over without manual lock removal")

    # (6) delivery crashes: before target commit, then after target
    #     commit but before the source ack
    page_before = cli_json(["pages", "--db", db_b, "--channel", "results",
                            "--limit", str(PAGE_SIZE)])
    run_cli(["deliver", "--db", db_b, "--warehouse", wh_b,
             "--page-size", str(PAGE_SIZE)],
            {"TWOSTREAM_CRASH_AT": "deliver.pre_target_commit"}, expect=CRASH)
    st = status(db_b)
    assert st["delivery"]["results"]["acked_offset"] == 0
    page_after = cli_json(["pages", "--db", db_b, "--channel", "results",
                           "--limit", str(PAGE_SIZE)])
    assert page_before == page_after
    ok("crash after page read before ack: source ack position did not "
       "move and the page re-reads identically")

    run_cli(["deliver", "--db", db_b, "--warehouse", wh_b,
             "--page-size", str(PAGE_SIZE)],
            {"TWOSTREAM_CRASH_AT": "deliver.post_target_commit@2"},
            expect=CRASH)
    st = status(db_b)
    assert st["delivery"]["results"]["acked_offset"] == PAGE_SIZE, st["delivery"]
    ok(f"crash after target commit before source ack: only page 1 "
       f"({PAGE_SIZE} rows) acknowledged")

    summary = cli_json(["deliver", "--db", db_b, "--warehouse", wh_b,
                        "--page-size", str(PAGE_SIZE)])
    ch = summary["channels"]
    assert ch["results"]["reused_receipts"] == 1, ch
    assert ch["results"]["rows"] + ch["late"]["rows"] == \
        len(ref_results) + len(ref_late) - PAGE_SIZE
    st = status(db_b)
    assert st["warehouse_complete"] is True, st["delivery"]
    ok("resumed delivery recognised the orphan receipt, completed the ack "
       "and finished both channels without duplicates")

    res_b, late_b = dump_warehouse(wh_b, job_b)
    assert Counter(res_b.values()) == Counter(ref_results)
    assert Counter(late_b.values()) == Counter(ref_late)
    assert sorted(res_b) == list(range(1, len(res_b) + 1))
    assert res_b == res_a and late_b == late_a, \
        "crashed run delivery identities differ from the clean run"
    ok("crashed+resumed run: warehouse == reference AND delivery "
       "identities (channel, offset -> record) identical to the clean run")

    # --------------------------------------- Phase C: warehouse handoff rules
    print("phase C: warehouse identity and conflict discipline")
    again = cli_json(["deliver", "--db", db_b, "--warehouse", wh_b,
                      "--page-size", str(PAGE_SIZE)])
    assert all(c["rows"] == 0 for c in again["channels"].values())
    ok("re-delivery to the same warehouse is a no-op (no duplicates)")

    wh_b2 = os.path.join(OUT, "whB_moved.db")
    fresh(wh_b2)
    shutil.copyfile(wh_b, wh_b2)
    moved = cli_json(["deliver", "--db", db_b, "--warehouse", wh_b2])
    assert moved["warehouse_id"] == summary["warehouse_id"]
    ok("same warehouse at a new path: identity matches, delivery continues")

    wh_other = os.path.join(OUT, "whOther.db")
    fresh(wh_other)
    run_cli(["deliver", "--db", db_b, "--warehouse", wh_other], expect=1)
    assert not os.path.exists(wh_other) or os.path.getsize(wh_other) == 0
    st = status(db_b)
    assert st["delivery"]["results"]["acked_offset"] == len(res_b)
    ok("a different warehouse is rejected; acknowledged position untouched "
       "(no silent loss of already-acked results)")

    # job D: a conflicting warehouse row must abort delivery and must not
    # advance the source ack position
    db_d = os.path.join(OUT, "jobD.db")
    wh_d = os.path.join(OUT, "whD.db")
    fresh(db_d)
    fresh(wh_d)
    job_d = cli_json(["create", "--db", db_d, "--lo", str(LO), "--hi",
                      str(HI), "--mode", "full_outer",
                      "--left-partitions", "lp0",
                      "--right-partitions", "rp0"])["job_id"]
    cli_json(["input", "--db", db_d, "--batch",
              write_batch("d1.json", 1, [d("L", "lp0", 1, "x", 1),
                                         d("R", "rp0", 1, "x", 1)])])
    wh = SQLiteWarehouse(wh_d, create=True)
    wh._conn.execute(
        "INSERT INTO wh_results(job_id,offset,kind,lp,ls,rp,rs,k,ltime,rtime)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (job_d, 1, "match", "lp0", "1", "rp0", "1", "EVIL", "1", "1"))
    wh._conn.commit()
    wh.close()
    run_cli(["deliver", "--db", db_d, "--warehouse", wh_d], expect=1)
    st = status(db_d)
    assert st["delivery"]["results"]["acked_offset"] == 0
    conn = sqlite3.connect(wh_d)
    k = conn.execute("SELECT k FROM wh_results WHERE job_id=? AND offset=1",
                     (job_d,)).fetchone()[0]
    conn.close()
    assert k == "EVIL"
    ok("conflicting warehouse row: delivery aborted, row not overwritten, "
       "source ack position did not advance")

    # --------------------------------------- Phase D: credential discipline
    print("phase D: page reads and ack credentials")
    db_e = os.path.join(OUT, "jobE.db")
    wh_e = os.path.join(OUT, "whE.db")
    fresh(db_e)
    fresh(wh_e)
    create_job(db_e)  # same config as A/B
    cli_json(["input", "--db", db_e, "--batch",
              write_batch("e1.json", 1, [d("L", "lp0", 1, "k", 5),
                                         d("R", "rp0", 1, "k", 5),
                                         w("L", "lp0", 10),
                                         d("L", "lp0", 2, "k", 1)])])
    cli_json(["input", "--db", db_e, "--batch",
              write_batch("e2.json", 2, [e("L", "lp0"), e("L", "lp1"),
                                         e("R", "rp0"), e("R", "rp1")])])
    st = status(db_e)
    assert st["processing"]["status"] == "finished"
    assert st["warehouse_complete"] is False
    ok("processing finished but results unacknowledged: status does not "
       "claim warehouse completion")

    page_r = cli_json(["pages", "--db", db_e, "--channel", "results",
                       "--limit", "5"])
    page_r2 = cli_json(["pages", "--db", db_e, "--channel", "results",
                        "--limit", "5"])
    assert page_r == page_r2
    ok("page reads across processes are identical and never delete")

    cred_path = os.path.join(OUT, "credE.json")
    with open(cred_path, "w", encoding="utf-8") as f:
        json.dump(page_r["credential"], f)
    ack1 = cli_json(["ack", "--db", db_e, "--credential", cred_path])
    ack2 = cli_json(["ack", "--db", db_e, "--credential", cred_path])
    assert ack1["already_acked"] is False and ack2["already_acked"] is True
    ok("cumulative ack advances; re-acking the same credential is a "
       "successful no-op")

    page_l = cli_json(["pages", "--db", db_e, "--channel", "late",
                       "--limit", "5"])
    flipped = dict(page_l["credential"], channel="results")
    with open(cred_path, "w", encoding="utf-8") as f:
        json.dump(flipped, f)
    run_cli(["ack", "--db", db_e, "--credential", cred_path], expect=1)
    forged = dict(page_l["credential"], end=page_l["end"] + 1)
    with open(cred_path, "w", encoding="utf-8") as f:
        json.dump(forged, f)
    run_cli(["ack", "--db", db_e, "--credential", cred_path], expect=1)
    run_cli(["ack", "--db", db_e, "--credential", cred_a_path], expect=1)
    st = status(db_e)
    assert st["delivery"]["late"]["acked_offset"] == 0
    ok("cross-channel credential, forged end, and another job's credential "
       "all rejected; ack position unchanged")

    cli_json(["deliver", "--db", db_e, "--warehouse", wh_e])
    st = status(db_e)
    assert st["warehouse_complete"] is True
    ok("delivery with no new input completes the job "
       "(warehouse_complete=true)")

    print(f"\nPASS drill_recover ({len(CHECKS)} checks, "
          f"{len(res_b)} results + {len(late_b)} late verified against "
          f"independent enumeration)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
