"""Warehouse delivery tests: page handoff, dedup/conflict, identity
binding, crash-window recovery between target commit and source ack."""

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from interval_join import (DeliveryConflictError, JobError, JobStore,
                           SQLiteWarehouse, StoreFormatError,
                           deliver_to_warehouse)

CFG = dict(lo=-2, hi=5, mode="full_outer",
           left_partitions=["lp0"], right_partitions=["rp0"])


def data(side, part, seq, key, time):
    return {"type": "data", "side": side, "partition": part, "seq": seq,
            "key": key, "time": time}


def wm(side, part, w):
    return {"type": "watermark", "side": side, "partition": part, "w": w}


def end(side, part):
    return {"type": "end", "side": side, "partition": part}


class DeliverCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.job_path = os.path.join(self.dir.name, "job.db")
        self.wh_path = os.path.join(self.dir.name, "wh.db")

    def tearDown(self):
        self.dir.cleanup()

    def make_job(self, messages_by_batch):
        job = JobStore.create(self.job_path, **CFG)
        with job.modify():
            for i, msgs in enumerate(messages_by_batch, 1):
                job.submit_batch({"batch_no": i, "messages": msgs})
        return job

    def simple_batches(self):
        # 3 matches, 1 lpad, 1 rpad, 1 late
        return [
            [data("L", "lp0", 1, "a", 10), data("L", "lp0", 2, "b", 10),
             data("L", "lp0", 3, "c", 10), data("L", "lp0", 4, "d", 10),
             data("R", "rp0", 1, "a", 11), data("R", "rp0", 2, "b", 11),
             data("R", "rp0", 3, "c", 11), data("R", "rp0", 4, "e", 10),
             wm("L", "lp0", 100), data("L", "lp0", 5, "late", 1)],
            [end("L", "lp0"), end("R", "rp0")],
        ]


class TestDelivery(DeliverCase):
    def test_deliver_both_channels_content_and_receipts(self):
        job = self.make_job(self.simple_batches())
        with job.modify():
            summary = deliver_to_warehouse(job, self.wh_path, page_size=2)
        jid = job.job_id
        self.assertEqual(summary["channels"]["results"]["rows"], 5)
        self.assertEqual(summary["channels"]["results"]["pages"], 3)
        self.assertEqual(summary["channels"]["late"]["rows"], 1)
        self.assertTrue(job.status()["warehouse_complete"])
        job.close()
        wh = SQLiteWarehouse(self.wh_path)
        results = list(wh.iter_rows(jid, "results"))
        late = list(wh.iter_rows(jid, "late"))
        self.assertEqual([o for o, _ in results], [1, 2, 3, 4, 5])
        self.assertEqual([o for o, _ in late], [1])
        kinds = sorted(r["kind"] for _, r in results)
        self.assertEqual(kinds, ["lpad", "match", "match", "match", "rpad"])
        self.assertEqual(late[0][1]["k"], "late")
        self.assertEqual(late[0][1]["wm"], "100")
        # every page has a receipt
        conn = wh._conn
        n_receipts = conn.execute(
            "SELECT COUNT(*) FROM wh_receipt WHERE job_id=?", (jid,)).fetchone()[0]
        self.assertEqual(n_receipts, 4)  # 3 result pages + 1 late page
        wh.close()

    def test_redeliver_is_a_noop(self):
        job = self.make_job(self.simple_batches())
        with job.modify():
            deliver_to_warehouse(job, self.wh_path, page_size=2)
            again = deliver_to_warehouse(job, self.wh_path, page_size=2)
        self.assertEqual(again["channels"]["results"]["rows"], 0)
        self.assertEqual(again["channels"]["late"]["rows"], 0)
        job.close()
        wh = SQLiteWarehouse(self.wh_path)
        self.assertEqual(wh.count(job.job_id, "results"), 5)
        wh.close()

    def test_crash_after_target_commit_healed_by_receipt(self):
        job = self.make_job(self.simple_batches())
        jid = job.job_id
        # simulate: page committed to the warehouse, process killed before
        # the source ack (what deliver.post_target_commit exercises end to
        # end in the drills)
        with job.modify():
            page = job.read_page("results", 2)
            wh = SQLiteWarehouse(self.wh_path, create=True)
            job.bind_target(wh.warehouse_id)
            wh.write_page(jid, "results", page.start, page.end,
                          [(d.offset, d.row) for d in page.rows])
            wh.close()
        self.assertEqual(job.acked_offset("results"), 0)  # ack never happened
        with job.modify():
            summary = deliver_to_warehouse(job, self.wh_path, page_size=2)
        # the pre-committed page was recognised via its receipt, not rewritten
        self.assertEqual(summary["channels"]["results"]["reused_receipts"], 1)
        self.assertEqual(job.acked_offset("results"), 5)
        job.close()
        wh = SQLiteWarehouse(self.wh_path)
        self.assertEqual(wh.count(jid, "results"), 5)  # no duplicates
        offsets = [o for o, _ in wh.iter_rows(jid, "results")]
        self.assertEqual(offsets, [1, 2, 3, 4, 5])
        wh.close()

    def test_resume_with_different_page_size(self):
        job = self.make_job(self.simple_batches())
        jid = job.job_id
        with job.modify():
            page = job.read_page("results", 2)
            wh = SQLiteWarehouse(self.wh_path, create=True)
            job.bind_target(wh.warehouse_id)
            wh.write_page(jid, "results", page.start, page.end,
                          [(d.offset, d.row) for d in page.rows])
            wh.close()
        with job.modify():
            # resume with a larger page: rows 1..2 dedup per-row
            summary = deliver_to_warehouse(job, self.wh_path, page_size=4)
        self.assertEqual(job.acked_offset("results"), 5)
        job.close()
        wh = SQLiteWarehouse(self.wh_path)
        self.assertEqual(wh.count(jid, "results"), 5)
        wh.close()

    def test_conflicting_row_aborts_without_source_ack(self):
        job = self.make_job(self.simple_batches())
        jid = job.job_id
        wh = SQLiteWarehouse(self.wh_path, create=True)
        # a foreign row under the same (job_id, offset) identity
        wh._conn.execute(
            "INSERT INTO wh_results(job_id,offset,kind,lp,ls,rp,rs,k,"
            "ltime,rtime) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (jid, 1, "match", "lp0", "1", "rp0", "1", "EVIL", "10", "11"))
        wh._conn.commit()
        wh_id = wh.warehouse_id
        wh.close()
        with job.modify():
            with self.assertRaises(DeliveryConflictError):
                deliver_to_warehouse(job, self.wh_path, page_size=2)
            self.assertEqual(job.acked_offset("results"), 0)  # not advanced
        job.close()
        # the conflicting row was not overwritten
        wh = SQLiteWarehouse(self.wh_path)
        row = list(wh.iter_rows(jid, "results"))
        self.assertEqual(row[0][1]["k"], "EVIL")
        self.assertEqual(wh.warehouse_id, wh_id)
        wh.close()

    def test_warehouse_identity_binding(self):
        job = self.make_job(self.simple_batches())
        with job.modify():
            s = deliver_to_warehouse(job, self.wh_path)
            bound_id = s["warehouse_id"]
            self.assertEqual(job.target_binding(), bound_id)
            # a different (new, empty) warehouse path is rejected
            other = os.path.join(self.dir.name, "other.db")
            with self.assertRaises(DeliveryConflictError):
                deliver_to_warehouse(job, other)
            self.assertFalse(os.path.exists(other) and
                             os.path.getsize(other) > 0)
            # the same warehouse at a new path continues
            moved = os.path.join(self.dir.name, "moved.db")
            shutil.copyfile(self.wh_path, moved)
            again = deliver_to_warehouse(job, moved)
            self.assertEqual(again["warehouse_id"], bound_id)
        job.close()

    def test_binding_survives_reopen(self):
        job = self.make_job(self.simple_batches())
        with job.modify():
            deliver_to_warehouse(job, self.wh_path)
        jid = job.job_id
        job.close()
        job = JobStore.open(self.job_path)
        with job.modify():
            other = os.path.join(self.dir.name, "other.db")
            with self.assertRaises(DeliveryConflictError):
                deliver_to_warehouse(job, other)
            s = deliver_to_warehouse(job, self.wh_path)
            self.assertEqual(s["channels"]["results"]["rows"], 0)
        job.close()

    def test_warehouse_must_be_independent_file(self):
        job = self.make_job(self.simple_batches())
        with job.modify():
            with self.assertRaises(JobError):
                deliver_to_warehouse(job, self.job_path)
        job.close()

    def test_open_missing_warehouse_when_bound_is_rejected(self):
        job = self.make_job(self.simple_batches())
        with job.modify():
            deliver_to_warehouse(job, self.wh_path)
        job.close()
        os.remove(self.wh_path)
        job = JobStore.open(self.job_path)
        with job.modify():
            with self.assertRaises(DeliveryConflictError):
                deliver_to_warehouse(job, self.wh_path)
        job.close()

    def test_foreign_file_as_warehouse_rejected(self):
        import sqlite3
        conn = sqlite3.connect(self.wh_path)
        conn.execute("CREATE TABLE whatever(x)")
        conn.commit()
        conn.close()
        job = self.make_job(self.simple_batches())
        with job.modify():
            with self.assertRaises(StoreFormatError):
                deliver_to_warehouse(job, self.wh_path)
        job.close()


if __name__ == "__main__":
    unittest.main()
