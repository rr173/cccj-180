"""Persistent job store tests: creation/opening discipline, batch intake,
recovery, delivery pages and cumulative acks, modification lock."""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from interval_join import (AckError, BatchConflictError, BatchGapError,
                           BusyError, Data, End, EventId, InputError,
                           JobExistsError, JobFinishedError, JobStore,
                           JoinEngine, Late, LeftPad, LockRequiredError,
                           Match, RightPad, StoreFormatError, Watermark)

CFG = dict(lo=-2, hi=5, mode="full_outer",
           left_partitions=["lp0"], right_partitions=["rp0"])


def data(side, part, seq, key, time, payload=None):
    m = {"type": "data", "side": side, "partition": part, "seq": seq,
         "key": key, "time": time}
    if payload is not None:
        m["payload"] = payload
    return m


def wm(side, part, w):
    return {"type": "watermark", "side": side, "partition": part, "w": w}


def end(side, part):
    return {"type": "end", "side": side, "partition": part}


class JobCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "job.db")

    def tearDown(self):
        self.dir.cleanup()

    def create(self, **cfg):
        return JobStore.create(self.path, **(cfg or CFG))

    def open(self):
        return JobStore.open(self.path)


class TestCreateOpen(JobCase):
    def test_create_and_reopen_restores_identity_and_config(self):
        job = self.create()
        jid = job.job_id
        job.close()
        job = self.open()
        self.assertEqual(job.job_id, jid)
        self.assertEqual((job.lo, job.hi, job.mode), (-2, 5, "full_outer"))
        self.assertEqual(job.left_partitions, ["lp0"])
        self.assertEqual(job.right_partitions, ["rp0"])
        st = job.status()
        self.assertEqual(st["config"]["mode"], "full_outer")
        self.assertEqual(st["format_version"], 1)
        job.close()

    def test_create_refuses_existing_job(self):
        self.create().close()
        with self.assertRaises(JobExistsError):
            self.create()
        with self.assertRaises(JobExistsError):
            JobStore.create(self.path, lo=0, hi=1, mode="inner",
                            left_partitions=["x"], right_partitions=["y"])
        # the original job is untouched
        job = self.open()
        self.assertEqual(job.mode, "full_outer")
        job.close()

    def test_create_on_empty_file_is_allowed(self):
        open(self.path, "wb").close()
        job = self.create()
        job.close()
        self.assertEqual(self.open().status()["batches"]["last"], 0)

    def test_open_missing_and_empty(self):
        with self.assertRaises(StoreFormatError):
            self.open()
        open(self.path, "wb").close()
        with self.assertRaises(StoreFormatError):
            self.open()

    def test_open_old_engine_db_rejected_without_writing(self):
        # a v1 JoinEngine database has event tables but no recovery metadata
        eng = JoinEngine(self.path, -2, 5, "inner", ["lp0"], ["rp0"])
        eng.feed_data("L", "lp0", 1, "k", 1)
        eng.close()
        with self.assertRaises(StoreFormatError) as ctx:
            self.open()
        self.assertIn("no job metadata", str(ctx.exception))
        # nothing was added to the old database
        conn = sqlite3.connect(self.path)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertNotIn("job_meta", tables)
        self.assertNotIn("batch_ledger", tables)

    def test_open_foreign_and_corrupt_rejected(self):
        conn = sqlite3.connect(self.path)
        conn.execute("CREATE TABLE something(x)")
        conn.commit()
        conn.close()
        with self.assertRaises(StoreFormatError):
            self.open()
        with open(self.path, "wb") as f:
            f.write(os.urandom(2048))
        with self.assertRaises(StoreFormatError):
            self.open()

    def test_open_unknown_version_rejected_without_writing(self):
        self.create().close()
        conn = sqlite3.connect(self.path)
        conn.execute("UPDATE job_meta SET format_version=999")
        conn.commit()
        conn.close()
        with self.assertRaises(StoreFormatError) as ctx:
            self.open()
        self.assertIn("version", str(ctx.exception))
        conn = sqlite3.connect(self.path)
        v = conn.execute("SELECT format_version FROM job_meta").fetchone()[0]
        conn.close()
        self.assertEqual(v, 999)  # untouched

    def test_open_incomplete_store_rejected(self):
        self.create().close()
        conn = sqlite3.connect(self.path)
        conn.execute("DROP TABLE batch_ledger")
        conn.commit()
        conn.close()
        with self.assertRaises(StoreFormatError):
            self.open()


class TestBatches(JobCase):
    def test_submit_and_resend_returns_original_receipt(self):
        job = self.create()
        with job.modify():
            r1 = job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10), data("R", "rp0", 1, "a", 12)]})
            self.assertEqual(r1["batch_no"], 1)
            self.assertEqual(r1["results"],
                             {"count": 1, "first_offset": 1, "last_offset": 1})
            # same content, different key order / whitespace / number form
            text = ('{ "messages" : [ {"time":10,"key":"a","seq":1,'
                    '"partition":"lp0","side":"L","type":"data"},'
                    '{"type":"data","side":"R","partition":"rp0","seq":1.0,'
                    '"key":"a","time":12} ], "batch_no" : 1 }')
            r1b = job.submit_batch(text)
            self.assertEqual(r1b, r1)
            # no re-execution: still exactly one result row
            self.assertEqual(job.status()["delivery"]["results"]["max_offset"], 1)
        job.close()

    def test_conflicting_resend_rejected_state_kept(self):
        job = self.create()
        with job.modify():
            job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10)]})
            with self.assertRaises(BatchConflictError):
                job.submit_batch({"batch_no": 1, "messages": [
                    data("L", "lp0", 1, "a", 11)]})
            with self.assertRaises(BatchConflictError):
                job.submit_batch({"batch_no": 1, "messages": []})
            # state is still at batch 1 and the next legal batch is accepted
            r2 = job.submit_batch({"batch_no": 2, "messages": [
                data("R", "rp0", 1, "a", 12)]})
            self.assertEqual(r2["results"]["count"], 1)
        job.close()

    def test_gap_rejected(self):
        job = self.create()
        with job.modify():
            with self.assertRaises(BatchGapError):
                job.submit_batch({"batch_no": 2, "messages": []})
            job.submit_batch({"batch_no": 1, "messages": []})
            with self.assertRaises(BatchGapError):
                job.submit_batch({"batch_no": 3, "messages": []})
            job.submit_batch({"batch_no": 2, "messages": []})
            self.assertEqual(job.status()["batches"]["last"], 2)
        job.close()

    def test_illegal_message_rolls_back_memory_and_disk(self):
        job = self.create()
        with job.modify():
            job.submit_batch({"batch_no": 1, "messages": [
                data("R", "rp0", 1, "k", 5)]})
            before = job.status()
            # legal data, legal watermark, a match, then an illegal message
            with self.assertRaises(InputError):
                job.submit_batch({"batch_no": 2, "messages": [
                    data("L", "lp0", 1, "k", 5),          # would match R#1
                    wm("L", "lp0", 3),                     # legal wm update
                    data("L", "lp0", 1, "x", 9),           # duplicate seq
                ]})
            after = job.status()
            # nothing of the batch survived: no seq/wm ledgers, no state,
            # no match in the outbox, batch position unchanged
            self.assertEqual(after["batches"]["last"], 1)
            self.assertEqual(after["state"]["left_state"], 0)
            self.assertEqual(after["delivery"]["results"]["max_offset"],
                             before["delivery"]["results"]["max_offset"])
            self.assertIsNone(after["processing"]["left"]["lp0"]["watermark"])
            # the same messages are still legal: seq 1 and wm 3 were not
            # latched by the rolled-back batch
            job.submit_batch({"batch_no": 2, "messages": [
                data("L", "lp0", 1, "k", 5), wm("L", "lp0", 3)]})
            self.assertEqual(job.status()["delivery"]["results"]
                             ["max_offset"], 1)
        job.close()

    def test_illegal_message_variants_rejected(self):
        job = self.create()
        with job.modify():
            for bad in (
                    {"type": "data", "side": "L"},            # missing fields
                    {"type": "mystery", "side": "L"},
                    data("L", "nope", 1, "k", 1),             # undeclared
                    data("X", "lp0", 1, "k", 1),              # bad side
                    data("L", "lp0", 1, "k", 1.5),            # non-integer time
                    wm("L", "lp0", "5x"),                     # bad wm value
            ):
                with self.assertRaises(InputError, msg=bad):
                    job.submit_batch({"batch_no": 1, "messages": [bad]})
            self.assertEqual(job.status()["batches"]["last"], 0)
        job.close()

    def test_non_standard_json_rejected(self):
        job = self.create()
        with job.modify():
            # NaN / Infinity as decoded values
            with self.assertRaises(InputError):
                job.submit_batch({"batch_no": 1, "messages": [
                    data("L", "lp0", 1, "k", 1, payload=float("nan"))]})
            with self.assertRaises(InputError):
                job.submit_batch({"batch_no": 1, "messages": [
                    data("L", "lp0", 1, "k", 1,
                         payload={"x": float("inf")})]})
            # NaN literal inside a JSON text envelope
            with self.assertRaises(InputError):
                job.submit_batch('{"batch_no":1,"messages":[{"type":"data",'
                                 '"side":"L","partition":"lp0","seq":1,'
                                 '"key":"k","time":1,"payload":NaN}]}')
            # non-serializable objects
            with self.assertRaises(InputError):
                job.submit_batch({"batch_no": 1, "messages": [
                    data("L", "lp0", 1, "k", 1, payload=object())]})
            with self.assertRaises(InputError):
                job.submit_batch({"batch_no": 1, "messages": [
                    data("L", "lp0", 1, "k", 1, payload={1: "x"})]})
            self.assertEqual(job.status()["batches"]["last"], 0)
        job.close()

    def test_envelope_shape_rejected(self):
        job = self.create()
        with job.modify():
            for bad in ({"messages": []},
                        {"batch_no": 1},
                        {"batch_no": 0, "messages": []},
                        {"batch_no": 1.5, "messages": []},
                        {"batch_no": True, "messages": []},
                        {"batch_no": 1, "messages": [], "extra": 1},
                        {"batch_no": 1, "messages": {}},
                        [1, 2], "not json", 42):
                with self.assertRaises(InputError, msg=repr(bad)):
                    job.submit_batch(bad)
        job.close()

    def test_new_batch_rejected_after_finish_but_resend_ok(self):
        job = self.create()
        with job.modify():
            r1 = job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10)]})
            job.submit_batch({"batch_no": 2, "messages": [
                end("L", "lp0"), end("R", "rp0")]})
            self.assertEqual(job.status()["processing"]["status"], "finished")
            with self.assertRaises(JobFinishedError):
                job.submit_batch({"batch_no": 3, "messages": [
                    data("L", "lp0", 2, "b", 20)]})
            # historical batches can still be resent for their receipt
            self.assertEqual(job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10)]}), r1)
        job.close()

    def test_big_times_and_seqs_stay_exact(self):
        big = 2 ** 70 + 12345
        job = self.create()
        with job.modify():
            job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "k", big),
                data("R", "rp0", 1, "k", str(big + 5)),   # decimal string
                wm("R", "rp0", big + 6)]})
        job.close()
        job = self.open()
        page = job.read_page("results", 10)
        self.assertEqual(page.rows[0].record.left_time, big)
        self.assertEqual(page.rows[0].row["ltime"], str(big))
        self.assertEqual(job.status()["processing"]["right"]["rp0"]
                         ["watermark"], big + 6)
        job.close()


class TestRecovery(JobCase):
    def test_full_state_survives_reopen(self):
        job = self.create()
        with job.modify():
            job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10), wm("L", "lp0", 5),
                data("L", "lp0", 2, "b", 3),              # late (3 < 5)
                data("R", "rp0", 1, "a", 12)]})           # matches L#1
        job.close()
        job = self.open()
        st = job.status()
        self.assertEqual(st["batches"]["last"], 1)
        self.assertEqual(st["processing"]["left"]["lp0"]["watermark"], 5)
        self.assertEqual(st["state"]["left_state"], 1)    # matched L#1 kept
        self.assertEqual(st["delivery"]["results"]["max_offset"], 1)
        self.assertEqual(st["delivery"]["late"]["max_offset"], 1)
        with job.modify():
            # seq ledger restored: duplicate seq still an error
            with self.assertRaises(InputError):
                job.submit_batch({"batch_no": 2, "messages": [
                    data("L", "lp0", 2, "c", 9)]})
            # and the join continues where it stopped: the stored L#1@10
            # pairs with the newly arrived R#2@13 (window [8,15])
            r = job.submit_batch({"batch_no": 2, "messages": [
                data("R", "rp0", 2, "a", 13)]})
            self.assertEqual(r["results"]["count"], 1)
            self.assertEqual(r["results"]["first_offset"], 2)
        job.close()

    def test_resend_after_reopen_does_not_reexecute(self):
        msgs = [data("L", "lp0", 1, "a", 10), data("R", "rp0", 1, "a", 12)]
        job = self.create()
        with job.modify():
            r1 = job.submit_batch({"batch_no": 1, "messages": msgs})
        job.close()
        job = self.open()
        with job.modify():
            r1b = job.submit_batch({"batch_no": 1, "messages": msgs})
            self.assertEqual(r1b, r1)
            self.assertEqual(job.status()["delivery"]["results"]
                             ["max_offset"], 1)
        job.close()

    def test_join_semantics_match_single_run_engine(self):
        # the same message stream through batches and through the
        # single-run JoinEngine must produce the same result multiset
        msgs = [
            Data("L", "lp0", 1, "a", 5), Data("L", "lp0", 2, "b", 7),
            Data("L", "lp0", 3, None, 9), Watermark("L", "lp0", 4),
            Data("R", "rp0", 1, "a", 6), Data("R", "rp0", 2, "b", 100),
            Watermark("R", "rp0", 20), Data("L", "lp0", 4, "c", 1),
            End("L", "lp0"), End("R", "rp0"),
        ]
        eng = JoinEngine(":memory:", -2, 5, "full_outer", ["lp0"], ["rp0"])
        for m in msgs:
            eng.feed(m)
        want = sorted(map(repr, eng.pull_results()))
        want_late = sorted(map(repr, eng.pull_late()))
        eng.close()

        job = self.create()
        envs, batch = [], []
        from interval_join.messages import Data as D, Watermark as W, End as E
        for i, m in enumerate(msgs):
            if isinstance(m, D):
                batch.append(data(m.side, m.partition, m.seq, m.key, m.time))
            elif isinstance(m, W):
                batch.append(wm(m.side, m.partition, m.w))
            else:
                batch.append(end(m.side, m.partition))
            if len(batch) == 3:
                envs.append(batch)
                batch = []
        if batch:
            envs.append(batch)
        got, got_late = [], []
        with job.modify():
            for i, b in enumerate(envs, 1):
                job.submit_batch({"batch_no": i, "messages": b})
            while True:
                page = job.read_page("results", 2)
                if page.empty:
                    break
                got.extend(repr(d.record) for d in page.rows)
                job.ack_page(page.credential)
            while True:
                page = job.read_page("late", 2)
                if page.empty:
                    break
                got_late.extend(repr(d.record) for d in page.rows)
                job.ack_page(page.credential)
        self.assertEqual(sorted(got), want)
        self.assertEqual(sorted(got_late), want_late)
        job.close()


class TestPagesAndAck(JobCase):
    def _job_with_results(self):
        job = self.create()
        with job.modify():
            job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10), data("L", "lp0", 2, "b", 10),
                data("L", "lp0", 3, "c", 10),
                data("R", "rp0", 1, "a", 11), data("R", "rp0", 2, "b", 11),
                data("R", "rp0", 3, "c", 11),
                wm("L", "lp0", 100), data("L", "lp0", 4, "d", 1)]})  # late
        return job

    def test_pages_stable_until_ack(self):
        job = self._job_with_results()
        p1 = job.read_page("results", 2)
        p2 = job.read_page("results", 2)
        self.assertEqual(p1, p2)                    # reads never delete
        self.assertEqual((p1.start, p1.end), (1, 2))
        self.assertEqual(job.status()["delivery"]["results"]["pending"], 3)
        with job.modify():
            r = job.ack_page(p1.credential)
            self.assertEqual((r["acked_offset"], r["reclaimed"]), (2, 2))
        p3 = job.read_page("results", 2)
        self.assertEqual((p3.start, p3.end), (3, 3))
        job.close()

    def test_ack_idempotent(self):
        job = self._job_with_results()
        page = job.read_page("results", 2)
        with job.modify():
            r1 = job.ack_page(page.credential)
            r2 = job.ack_page(page.credential)
        self.assertFalse(r1["already_acked"])
        self.assertTrue(r2["already_acked"])
        self.assertEqual(r2["acked_offset"], 2)
        job.close()

    def test_cross_channel_credential_rejected(self):
        job = self._job_with_results()
        cred = dict(job.read_page("results", 2).credential)
        cred["channel"] = "late"
        with job.modify():
            with self.assertRaises(AckError):
                job.ack_page(cred)
        job.close()

    def test_forged_end_and_skip_rejected(self):
        job = self._job_with_results()
        page = job.read_page("results", 2)
        forged = dict(page.credential, end=page.end + 1)
        skipped = dict(page.credential, start=page.start + 1,
                       end=page.end + 1)
        with job.modify():
            with self.assertRaises(AckError):
                job.ack_page(forged)
            with self.assertRaises(AckError):
                job.ack_page(skipped)
            self.assertEqual(job.acked_offset("results"), 0)
        job.close()

    def test_other_jobs_credential_rejected(self):
        job = self._job_with_results()
        other_path = os.path.join(self.dir.name, "other.db")
        other = JobStore.create(other_path, **CFG)
        with other.modify():
            other.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10), data("R", "rp0", 1, "a", 11)]})
        foreign = other.read_page("results", 1).credential
        with job.modify():
            with self.assertRaises(AckError):
                job.ack_page(foreign)
        job.close()
        other.close()

    def test_ack_requires_lock(self):
        job = self._job_with_results()
        page = job.read_page("results", 2)
        with self.assertRaises(LockRequiredError):
            job.ack_page(page.credential)
        job.close()

    def test_offsets_never_restart_after_reclaim(self):
        job = self._job_with_results()
        with job.modify():
            job.ack_page(job.read_page("results", 3).credential)
            job.ack_page(job.read_page("late", 1).credential)
            self.assertEqual(job.status()["state"]["outbox"], 0)
            r = job.submit_batch({"batch_no": 2, "messages": [
                data("L", "lp0", 5, "z", 50), data("R", "rp0", 4, "z", 51)]})
            self.assertEqual(r["results"]["first_offset"], 4)  # not 1
            self.assertEqual(job.status()["delivery"]["results"]
                             ["reclaimed"], 3)
        job.close()

    def test_late_channel_independent_offsets(self):
        job = self._job_with_results()
        lp = job.read_page("late", 10)
        rp = job.read_page("results", 10)
        self.assertEqual((lp.start, lp.end), (1, 1))
        self.assertEqual((rp.start, rp.end), (1, 3))
        self.assertIsInstance(lp.rows[0].record, Late)
        with job.modify():
            job.ack_page(lp.credential)
            self.assertEqual(job.acked_offset("results"), 0)
            self.assertEqual(job.acked_offset("late"), 1)
        job.close()


class TestLock(JobCase):
    def test_second_modifier_gets_busy(self):
        j1 = self.create()
        j1.acquire()
        j2 = self.open()
        with self.assertRaises(BusyError):
            j2.acquire()
        j1.release()
        j2.acquire()          # takeover after release
        j2.release()
        j1.close()
        j2.close()

    def test_mutations_require_lock(self):
        job = self.create()
        with self.assertRaises(LockRequiredError):
            job.submit_batch({"batch_no": 1, "messages": []})
        job.close()

    def test_status_while_locked_shows_committed_state(self):
        j1 = self.create()
        with j1.modify():
            j1.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10)]})
        j1.acquire()
        j2 = self.open()
        st = j2.status()       # read-only while j1 holds the lock
        self.assertEqual(st["batches"]["last"], 1)
        self.assertTrue(st["locked"])
        j1.release()
        j1.close()
        j2.close()


class TestStatus(JobCase):
    def test_processing_and_delivery_completion_are_separate(self):
        job = self.create()
        with job.modify():
            job.submit_batch({"batch_no": 1, "messages": [
                data("L", "lp0", 1, "a", 10), data("R", "rp0", 1, "a", 12),
                end("L", "lp0"), end("R", "rp0")]})
        st = job.status()
        self.assertEqual(st["processing"]["status"], "finished")
        self.assertFalse(st["delivery"]["fully_delivered"])
        self.assertFalse(st["warehouse_complete"])   # unacked result remains
        with job.modify():
            job.ack_page(job.read_page("results", 10).credential)
        st = job.status()
        self.assertTrue(st["delivery"]["fully_delivered"])
        self.assertTrue(st["warehouse_complete"])
        job.close()


if __name__ == "__main__":
    unittest.main()
