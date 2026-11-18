"""Unit tests for the attribution mart: history validation, window
arithmetic, exact quantiles, sync idempotency, request deduplication,
corrections and version/expansion semantics.

System-level acceptance (random interleaving against an independent
reference, real process kills, barriers, 200k-fact locality) lives in
drills/drill_attribution_*.py.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from attribution_mart import (AttributionInputError, AttributionMart,
                              MartBusyError, MartFormatError,
                              MartIdentityError, MartInputError,
                              MartLockRequiredError, StaleRevisionError,
                              StaleVersionError, TimeMap,
                              changed_intervals, nearest_rank, parse_segments,
                              window_start)
from attribution_mart.stats import window_start as ws2
from interval_join import JobStore, deliver_to_warehouse


def d(side, part, seq, key, time):
    return {"type": "data", "side": side, "partition": part, "seq": seq,
            "key": key, "time": time}


def w(side, part, wm):
    return {"type": "watermark", "side": side, "partition": part, "w": wm}


def end(side, part):
    return {"type": "end", "side": side, "partition": part}


class Case(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.base = self.dir.name
        self.wh = os.path.join(self.base, "wh.db")
        self.mart_path = os.path.join(self.base, "mart.db")

    def tearDown(self):
        self.dir.cleanup()

    def job(self, name, batches, lo=1, hi=4, deliver=True):
        path = os.path.join(self.base, name + ".db")
        job = JobStore.create(path, lo=lo, hi=hi, mode="full_outer",
                              left_partitions=["lp0"],
                              right_partitions=["rp0"])
        with job.modify():
            for i, msgs in enumerate(batches, 1):
                job.submit_batch({"batch_no": i, "messages": msgs})
        if deliver:
            with job.modify():
                deliver_to_warehouse(job, self.wh, page_size=100)
        jid = job.job_id
        job.close()
        return path, jid

    def mart(self, jids, width=10, origin=0):
        return AttributionMart.create(self.mart_path, warehouse=self.wh,
                                      job_ids=jids, width=width, origin=origin)

    def groups(self, mart):
        return {(g["window_start"], g["owner"]): g
                for g in mart.query()["groups"]}


class TestHistory(unittest.TestCase):
    def test_unknown_is_not_owner_text(self):
        tm = TimeMap.from_raw([{"start": 0, "end": 10, "owner": "unknown"}])
        self.assertEqual(tm.owner_at(5), "unknown")
        self.assertIsNone(tm.owner_at(10))
        tm2 = TimeMap.from_raw([])
        self.assertIsNone(tm2.owner_at(0))

    def test_infinities_and_touching(self):
        tm = TimeMap.from_raw([
            {"start": 10, "end": None, "owner": "B"},
            {"start": None, "end": 10, "owner": "A"},
        ])
        self.assertEqual(tm.owner_at(-10 ** 100), "A")
        self.assertEqual(tm.owner_at(9), "A")
        self.assertEqual(tm.owner_at(10), "B")
        self.assertIsNone(TimeMap.from_raw([]).owner_at(0))

    def test_illegal_histories(self):
        bad = [
            [{"start": 0, "end": 10, "owner": "A"},
             {"start": 5, "end": 20, "owner": "B"}],           # overlap
            [{"start": None, "end": 10, "owner": "A"},
             {"start": 10, "end": None, "owner": "B"}],        # touching is ok
            [{"start": 5, "end": 5, "owner": "A"}],            # empty
            [{"start": 10, "end": 5, "owner": "A"}],           # inverted
            [{"start": 0, "end": 10, "owner": ""}],            # empty owner
            [{"start": 0, "end": 10, "owner": None}],          # non-text
            [{"start": 0, "end": 10, "owner": "A", "x": 1}],   # unknown field
            [{"start": 0.5, "end": 10, "owner": "A"}],         # non-integer
            [{"start": None, "end": None, "owner": "A"},
             {"start": 0, "end": 1, "owner": "B"}],            # covered twice
        ]
        for i, hist in enumerate(bad):
            with self.subTest(i=i):
                if i == 1:      # touching is legal
                    parse_segments(hist)
                    continue
                with self.assertRaises(AttributionInputError):
                    parse_segments(hist)

    def test_input_order_does_not_matter(self):
        a = parse_segments([{"start": 20, "end": None, "owner": "B"},
                            {"start": None, "end": 20, "owner": "A"}])
        b = parse_segments([{"start": None, "end": 20, "owner": "A"},
                            {"start": 20, "end": None, "owner": "B"}])
        self.assertEqual([s.val for s in a], [s.val for s in b])

    def test_changed_intervals_are_exact(self):
        old = TimeMap.from_raw([{"start": 0, "end": 20, "owner": "A"},
                                {"start": 20, "end": 40, "owner": "B"}])
        new = TimeMap.from_raw([{"start": 0, "end": 8, "owner": "A"},
                                {"start": 8, "end": 20, "owner": "C"},
                                {"start": 20, "end": 40, "owner": "B"}])
        self.assertEqual(changed_intervals(old, new), [(8, 20)])
        # a boundary move without an owner change is not a group change
        moved = TimeMap.from_raw([{"start": 0, "end": 5, "owner": "A"},
                                  {"start": 5, "end": 20, "owner": "A"},
                                  {"start": 20, "end": 40, "owner": "B"}])
        self.assertEqual(changed_intervals(old, moved), [])
        # unbounded changes
        empty = TimeMap(())
        self.assertEqual(changed_intervals(empty, old), [(0, 40)])
        self.assertEqual(changed_intervals(old, empty), [(0, 40)])


class TestWindows(unittest.TestCase):
    def test_floor_and_negative(self):
        self.assertEqual(window_start(-1, 10, 0), -10)
        self.assertEqual(window_start(-10, 10, 0), -10)
        self.assertEqual(window_start(-11, 10, 0), -20)
        self.assertEqual(window_start(9, 10, 0), 0)
        self.assertEqual(window_start(10, 10, 0), 10)
        self.assertEqual(window_start(13, 10, 3), 13)
        self.assertEqual(window_start(12, 10, 3), 3)
        big = 2 ** 200 + 5
        self.assertEqual(ws2(big, 7, 4), 4 + ((big - 4) // 7) * 7)

    def test_quantiles(self):
        vals = [1, 2, 3, 4]
        self.assertEqual(nearest_rank(vals, 1, 2), 2)
        self.assertEqual(nearest_rank(vals, 19, 20), 4)
        self.assertEqual(nearest_rank([5], 1, 2), 5)
        self.assertIsNone(nearest_rank([], 1, 2))
        n = 20
        vals = list(range(1, n + 1))
        self.assertEqual(nearest_rank(vals, 19, 20), 19)


class TestSync(Case):
    def test_metrics_dedup_and_unknown(self):
        _, jid = self.job("j", [[
            d("L", "lp0", 1, "a", 9), d("R", "rp0", 1, "a", 11),
            d("R", "rp0", 2, "a", 13),
            d("L", "lp0", 2, "b", 12), d("R", "rp0", 3, "b", 13),
            d("L", "lp0", 3, None, 15), d("R", "rp0", 4, "c", 16),
            d("L", "lp0", 4, "a", 22),
            w("R", "rp0", 40), w("L", "lp0", 40),
        ]])
        mart = self.mart([jid])
        with mart.modify():
            mart.replace_attribution(0, {"a": [
                {"start": 0, "end": 20, "owner": "A"}]})
            s = mart.sync()
        self.assertEqual(s["jobs"][jid]["facts_added"], 6)
        g = self.groups(mart)
        # window 0, A: request a@9 matched twice -> 2 pairs but 1 request
        self.assertEqual(g[(0, "A")]["match_pairs"], 2)
        self.assertEqual(g[(0, "A")]["matched_requests"], 1)
        self.assertEqual(g[(0, "A")]["latency_sum"], 2 + 4)
        self.assertEqual(g[(0, "A")]["p50"], 2)
        self.assertEqual(g[(0, "A")]["p95"], 4)
        # window 10 unknown: b@12 matched @13 (latency 1), the null-key
        # request stays unmatched, and the unmatched response "c" is here
        self.assertEqual(g[(10, None)]["match_pairs"], 1)
        self.assertEqual(g[(10, None)]["matched_requests"], 1)
        self.assertEqual(g[(10, None)]["unmatched_requests"], 1)
        self.assertEqual(g[(10, None)]["unmatched_responses"], 1)
        self.assertEqual(g[(10, None)]["latency_sum"], 1)
        # a@22 matched nothing and has no attribution: window 20 unknown
        self.assertEqual(g[(20, None)]["unmatched_requests"], 1)
        # repeat sync stores nothing twice and reports nothing new
        with mart.modify():
            again = mart.sync()
        self.assertEqual(again["jobs"][jid]["facts_added"], 0)
        self.assertEqual(again["jobs"][jid]["requests_added"], 0)
        self.assertEqual(again["jobs"][jid]["captured_upper"], 6)
        self.assertEqual(again["jobs"][jid]["processed_after"], 6)
        self.assertEqual(self.groups(mart), g)

    def test_jobs_do_not_merge_requests(self):
        _, j1 = self.job("j1", [[
            d("L", "lp0", 1, "a", 1), d("R", "rp0", 1, "a", 2)]])
        _, j2 = self.job("j2", [[
            d("L", "lp0", 1, "a", 1), d("R", "rp0", 1, "a", 3)]])
        mart = self.mart([j1, j2])
        with mart.modify():
            mart.replace_attribution(0, {"a": [
                {"start": None, "end": None, "owner": "A"}]})
            mart.sync()
        g = self.groups(mart)
        self.assertEqual(g[(0, "A")]["match_pairs"], 2)
        self.assertEqual(g[(0, "A")]["matched_requests"], 2)
        self.assertEqual(g[(0, "A")]["latency_sum"], 3)

    def test_positions_and_meta(self):
        _, jid = self.job("j", [[d("L", "lp0", 1, "a", 1),
                                 d("R", "rp0", 1, "a", 2)]])
        mart = self.mart([jid])
        with mart.modify():
            s = mart.sync()
        q = mart.query()
        self.assertEqual(q["positions"][jid], 1)
        self.assertEqual(s["jobs"][jid]["captured_upper"], 1)
        self.assertEqual(q["jobs"], [jid])
        self.assertEqual(q["window"], {"width": 10, "origin": 0})
        # view token is stable until the next committed change
        self.assertEqual(mart.query()["view_token"], q["view_token"])


class TestVersions(Case):
    def setUp(self):
        super().setUp()
        _, self.jid = self.job("j", [[
            d("L", "lp0", 1, "a", 1), d("R", "rp0", 1, "a", 2),
            d("L", "lp0", 2, "a", 11), d("R", "rp0", 2, "a", 13),
            w("L", "lp0", 40), w("R", "rp0", 40)]])
        self.m = self.mart([self.jid])
        with self.m.modify():
            self.m.replace_attribution(0, {"a": [
                {"start": None, "end": 10, "owner": "A"},
                {"start": 10, "end": None, "owner": "B"}]})
            self.m.sync()
        self.q = self.m.query()

    def test_expand_and_stale(self):
        tok = self.q["view_token"]
        page = self.m.expand(tok, 0, "A")
        self.assertEqual(len(page["facts"]), 1)
        self.assertEqual(page["facts"][0]["job_id"], self.jid)
        self.assertEqual(page["facts"][0]["offset"], 1)
        self.assertEqual(page["facts"][0]["attribution"]["segment"],
                         {"start": None, "end": 10, "owner": "A"})
        self.assertEqual(page["facts"][0]["latency"], 1)
        self.assertEqual(page["warehouse_rows"][0]["k"], "a")
        # a stale token is refused
        with self.m.modify():
            self.m.replace_attribution(self.m.status()["mart_revision"],
                                       {"a": [{"start": None, "end": None,
                                               "owner": "Z"}]})
        with self.assertRaises(StaleVersionError):
            self.m.expand(tok, 0, "A")
        with self.assertRaises(StaleVersionError):
            self.m.query(view_token=tok)

    def test_correction_moves_groups(self):
        rev = self.m.status()["mart_revision"]
        with self.m.modify():
            res = self.m.replace_attribution(rev, {"a": [
                {"start": None, "end": 10, "owner": "A"},
                {"start": 10, "end": 12, "owner": "C"},
                {"start": 12, "end": None, "owner": "B"}]})
        self.assertEqual(res["changed_ranges"], {"a": [[10, 12]]})
        g = self.groups(self.m)
        self.assertEqual(g[(10, "C")]["match_pairs"], 1)
        self.assertEqual(g[(10, "C")]["matched_requests"], 1)
        self.assertNotIn((10, "B"), g)
        self.assertEqual(res["groups_recomputed"], 2)

    def test_clear_device_goes_unknown(self):
        rev = self.m.status()["mart_revision"]
        with self.m.modify():
            self.m.replace_attribution(rev, {"a": []})
        g = self.groups(self.m)
        self.assertEqual(g[(0, None)]["match_pairs"], 1)
        self.assertEqual(g[(10, None)]["match_pairs"], 1)
        self.assertNotIn((0, "A"), g)
        self.assertNotIn((10, "B"), g)

    def test_stale_revision_and_no_partial_effect(self):
        rev = self.m.status()["mart_revision"]
        before = self.m.query()
        with self.assertRaises(StaleRevisionError):
            with self.m.modify():
                self.m.replace_attribution(rev - 1, {"a": []})
        self.assertEqual(self.m.query()["view_token"], before["view_token"])
        with self.assertRaises(AttributionInputError):
            with self.m.modify():
                self.m.replace_attribution(rev, {
                    "a": [{"start": 0, "end": 5, "owner": "X"},
                          {"start": 4, "end": 9, "owner": "Y"}],
                    "new": [{"start": None, "end": None, "owner": "Z"}]})
        self.assertEqual(self.m.query()["view_token"], before["view_token"])
        self.assertEqual(self.m.status()["dim_revision"], 1)

    def test_lock_and_query_without_lock(self):
        unlocked = AttributionMart.open(self.mart_path)
        with self.assertRaises(MartLockRequiredError):
            unlocked.sync()
        with self.assertRaises(MartLockRequiredError):
            unlocked.replace_attribution(1, {"a": []})
        unlocked.close()
        self.m.acquire()
        try:
            other = AttributionMart.open(self.mart_path)
            try:
                with self.assertRaises(MartBusyError):
                    other.acquire()
                other.query()          # readers do not need the lock
                other.expand(self.q["view_token"], 0, "A")
            finally:
                other.close()
        finally:
            self.m.release()

    def test_dim_revision_bumps_even_when_unchanged(self):
        before = self.m.status()
        with self.m.modify():
            self.m.replace_attribution(before["mart_revision"], {"a": [
                {"start": None, "end": 10, "owner": "A"},
                {"start": 10, "end": None, "owner": "B"}]})
        after = self.m.status()
        self.assertEqual(after["dim_revision"], before["dim_revision"] + 1)
        self.assertEqual(after["mart_revision"], before["mart_revision"] + 1)


class TestFormats(Case):
    def test_create_and_open_errors(self):
        _, jid = self.job("j", [[d("L", "lp0", 1, "a", 1),
                                 d("R", "rp0", 1, "a", 2)]])
        with self.assertRaises(MartFormatError):
            AttributionMart.open(self.mart_path)          # missing
        m = self.mart([jid])
        m.close()
        with self.assertRaises(MartFormatError):
            AttributionMart.create(self.mart_path, warehouse=self.wh,
                                   job_ids=[jid], width=10, origin=0)
        with self.assertRaises(MartInputError):
            AttributionMart.create(self.mart_path + "x", warehouse=self.wh,
                                   job_ids=[jid], width=0, origin=0)
        with self.assertRaises(MartInputError):
            AttributionMart.create(self.mart_path + "y", warehouse=self.wh,
                                   job_ids=[], width=10, origin=0)
        # a foreign sqlite file is rejected, not adopted
        foreign = os.path.join(self.base, "foreign.db")
        conn = sqlite3.connect(foreign)
        conn.execute("CREATE TABLE t(x)")
        conn.commit()
        conn.close()
        with self.assertRaises(MartFormatError):
            AttributionMart.open(foreign)
        # unknown format version is rejected
        bad = os.path.join(self.base, "bad.db")
        import shutil
        shutil.copyfile(self.mart_path, bad)
        conn = sqlite3.connect(bad)
        conn.execute("UPDATE mart_meta SET format_version=99")
        conn.commit()
        conn.close()
        with self.assertRaises(MartFormatError):
            AttributionMart.open(bad)

    def test_warehouse_identity(self):
        _, jid = self.job("j", [[d("L", "lp0", 1, "a", 1),
                                 d("R", "rp0", 1, "a", 2)]])
        m = self.mart([jid])
        m.close()
        other_wh = os.path.join(self.base, "other.db")
        job2 = os.path.join(self.base, "j2.db")
        j = JobStore.create(job2, lo=1, hi=4, mode="full_outer",
                            left_partitions=["lp0"],
                            right_partitions=["rp0"])
        with j.modify():
            j.submit_batch({"batch_no": 1, "messages": [
                d("L", "lp0", 1, "a", 1), d("R", "rp0", 1, "a", 2)]})
            deliver_to_warehouse(j, other_wh)
        j.close()
        with self.assertRaises(MartIdentityError):
            AttributionMart.open(self.mart_path, warehouse=other_wh)
        # the same warehouse copied to another path keeps working
        moved = os.path.join(self.base, "moved.db")
        import shutil
        shutil.copyfile(self.wh, moved)
        m = AttributionMart.open(self.mart_path, warehouse=moved)
        with m.modify():
            m.sync()          # warehouse_id matches, path was rebound
        self.assertEqual(m.warehouse_path, moved)
        m.close()

    def test_read_only_source(self):
        _, jid = self.job("j", [[d("L", "lp0", 1, "a", 1),
                                 d("R", "rp0", 1, "a", 2)]])
        m = self.mart([jid])
        with m.modify():
            m.sync()
        m.close()
        # the mart never wrote to the warehouse
        conn = sqlite3.connect(self.wh)
        before = conn.execute("SELECT COUNT(*) FROM wh_receipt").fetchone()[0]
        conn.close()
        m = AttributionMart.open(self.mart_path)
        with m.modify():
            m.sync()
        m.close()
        conn = sqlite3.connect(self.wh)
        after = conn.execute("SELECT COUNT(*) FROM wh_receipt").fetchone()[0]
        conn.close()
        self.assertEqual(after, before)


class TestGapAndUnknownKind(Case):
    def test_offset_gap_fails_and_keeps_version(self):
        path, jid = self.job("j", [[d("L", "lp0", 1, "a", 1),
                                    d("R", "rp0", 1, "a", 2)]])
        m = self.mart([jid])
        with m.modify():
            m.replace_attribution(0, {"a": [
                {"start": None, "end": None, "owner": "A"}]})
            m.sync()
        before = m.query()
        # append two more rows, then delete the first of them
        job = JobStore.open(path)
        with job.modify():
            job.submit_batch({"batch_no": 2, "messages": [
                d("L", "lp0", 2, "a", 11), d("R", "rp0", 2, "a", 12)]})
            job.submit_batch({"batch_no": 3, "messages": [
                d("L", "lp0", 3, "a", 21), d("R", "rp0", 3, "a", 22)]})
            deliver_to_warehouse(job, self.wh)
        job.close()
        conn = sqlite3.connect(self.wh)
        conn.execute("DELETE FROM wh_results WHERE job_id=? AND offset=2",
                     (jid,))
        conn.commit()
        conn.close()
        with self.assertRaises(MartInputError):
            with m.modify():
                m.sync()
        self.assertEqual(m.query()["view_token"], before["view_token"])
        self.assertEqual(m.status()["positions"][jid], 1)

    def test_unknown_result_kind(self):
        _, jid = self.job("j", [[d("L", "lp0", 1, "a", 1),
                                 d("R", "rp0", 1, "a", 2)]])
        conn = sqlite3.connect(self.wh)
        conn.execute("UPDATE wh_results SET kind='bogus' WHERE job_id=?",
                     (jid,))
        conn.commit()
        conn.close()
        m = self.mart([jid])
        with self.assertRaises(MartInputError):
            with m.modify():
                m.sync()
        self.assertEqual(m.query()["groups"], [])


class TestBigIntegers(Case):
    def test_beyond_64_bits(self):
        big = 2 ** 70 + 3
        _, jid = self.job("j", [[
            d("L", "lp0", 1, "a", big), d("R", "rp0", 1, "a", big + 4),
            d("L", "lp0", 2, "a", -big), d("R", "rp0", 2, "a", -big + 2),
        ]])
        m = self.mart([jid])
        with m.modify():
            m.replace_attribution(0, {"a": [
                {"start": -big - 100, "end": 0, "owner": "N"},
                {"start": 0, "end": big + 100, "owner": "P"}]})
            m.sync()
        g = self.groups(m)
        self.assertEqual(g[(window_start(big, 10, 0), "P")]["latency_sum"], 4)
        self.assertEqual(g[(window_start(-big, 10, 0), "N")]["latency_sum"], 2)
        # the stored window starts decode back to exact integers
        for (ws, _) in g:
            self.assertIsInstance(ws, int)
            self.assertEqual(ws, window_start(ws, 10, 0))


if __name__ == "__main__":
    unittest.main()
