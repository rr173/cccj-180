"""Unit tests for the shared-response investigation: the hand fixture,
group changes, first-connection chronology and its evidence chain, node
identity, prefix semantics, refusals and format handling.

System-level drills (independent prefix-by-prefix reference, live append
during a build, gaps, crash residue, 200k-edge scale and query locality)
live in ``drills/drill_response_*.py``.
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from interval_join import SQLiteWarehouse
from response_investigation import (InvestigationBusyError,
                                    InvestigationFormatError,
                                    InvestigationInputError,
                                    ResponseInvestigation)
from response_investigation import fixture

JOB = "job-a"
ROW = {  # request name -> left seq (all partition "lp")
    "A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7,
}


def _match(offset, lseq, rseq):
    return (offset, {"kind": "match", "lp": "lp", "ls": str(lseq),
                     "rp": "rp", "rs": str(rseq), "k": "dev",
                     "ltime": "100", "rtime": "101"})


def _lpad(offset, lseq):
    return (offset, {"kind": "lpad", "lp": "lp", "ls": str(lseq),
                     "rp": None, "rs": None, "k": "dev",
                     "ltime": "100", "rtime": None})


def ident(name, job=JOB):
    return {"job_id": job, "partition": "lp", "seq": ROW[name]}


class Case(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = self._tmp.name
        self.wh = os.path.join(self.base, "wh.db")
        self.inv = os.path.join(self.base, "inv.db")

    def tearDown(self):
        self._tmp.cleanup()

    def warehouse(self, rows, job=JOB):
        wh = SQLiteWarehouse(self.wh, create=not os.path.exists(self.wh))
        try:
            wh.write_page(job, "results", rows[0][0], rows[-1][0], rows)
        finally:
            wh.close()

    def build(self, p, q, jobs=None, **kw):
        jobs = jobs or [JOB]
        return ResponseInvestigation.build(
            self.inv, self.wh, jobs,
            {j: p.get(j, 0) for j in jobs}, {j: q.get(j, 0) for j in jobs},
            **kw)


class TestHandFixture(Case):
    """The brief's fixture, placed through the warehouse write-page path."""

    def setUp(self):
        super().setUp()
        fx = fixture.build_hand(self.base, job_id=JOB)
        self.inv = os.path.join(self.base, "inv.db")
        self.rows = fx
        self.rows = fixture.warehouse_rows(fx["warehouse"], JOB)
        self.wh = fx["warehouse"]
        # Report the offsets as delivered so a wrong fixture cannot pass.
        self.assertEqual([r[0] for r in self.rows], list(range(1, 10)))
        self.assertEqual([r[1] for r in self.rows],
                         ["match"] * 8 + ["lpad"])
        self.investigation = self.build({JOB: 4}, {JOB: 9})

    def gids(self, step):
        return {g["group_id"]: g for g in
                self.investigation.groups(step)["groups"]}

    def test_prefix_four_has_two_groups(self):
        p = self.gids("P")
        self.assertEqual(sorted(p), [1, 3])
        self.assertEqual(p[1]["n_nodes"], 3)   # A, B, X
        self.assertEqual(p[1]["n_edges"], 2)
        self.assertEqual(p[3]["n_nodes"], 3)   # C, D, Y
        self.assertEqual(p[3]["n_edges"], 2)
        self.assertEqual(self.investigation.request_node(
            "P", ident("A"))["group_id"], 1)
        self.assertEqual(self.investigation.request_node(
            "P", ident("D"))["group_id"], 3)

    def test_e_at_five_grows_the_second_group(self):
        inv = ResponseInvestigation.build(
            os.path.join(self.base, "p5.db"), self.wh, [JOB],
            {JOB: 4}, {JOB: 5})
        groups = {g["group_id"]: g for g in inv.groups("Q")["groups"]}
        second = inv.request_node("Q", ident("C"))["group_id"]
        self.assertEqual(second, 3)
        self.assertEqual(groups[3]["n_nodes"], 4)     # C, D, Y, E
        self.assertEqual(groups[3]["change"]["origin"], "same")
        self.assertEqual(inv.change_sources(3)["source_group_ids"], [3])
        self.assertEqual(groups[3]["change"]["added_nodes"], 1)
        self.assertEqual(groups[3]["change"]["added_edges"], 1)  # E-Y
        self.assertEqual(groups[1]["n_nodes"], 3)     # unchanged
        self.assertEqual(groups[1]["change"]["added_nodes"], 0)
        self.assertEqual(groups[1]["change"]["added_edges"], 0)
        inv.close()

    def test_q_groups_merge_and_new(self):
        q = self.gids("Q")
        self.assertEqual(sorted(q), [1, 6])
        self.assertEqual(q[1]["change"]["origin"], "merge")
        self.assertEqual(self.investigation.change_sources(1)
                         ["source_group_ids"], [1, 3])
        self.assertEqual(q[1]["n_nodes"], 7)    # A,B,C,D,E,X,Y (G absent)
        self.assertEqual(q[1]["n_edges"], 7)
        self.assertEqual(q[6]["change"]["origin"], "new")
        self.assertEqual(self.investigation.change_sources(6)
                         ["source_group_ids"], [])
        self.assertEqual(q[6]["n_nodes"], 2)    # F, Z
        members = self.investigation.group_members("Q", 1)["members"]
        self.assertNotIn(7, [m["seq"] for m in members])
        # G is an lpad: it is not a node at all.
        self.assertIsNone(self.investigation.request_node("Q", ident("G")))

    def test_a_and_d_first_connect_at_six(self):
        r = self.investigation.resolve("Q", ident("A"), ident("D"))
        self.assertEqual(r["status"], "connected")
        self.assertEqual(r["first_connected_offset"], 6)
        self.assertFalse(r["connected_at_P"])
        self.assertEqual(r["hop_count"], 4)
        seqs = [(c["node"]["side"], c["node"]["seq"],
                 None if c["edge"] is None else c["edge"]["results_offset"])
                for c in r["chain"]]
        self.assertEqual(seqs, [
            ("L", 1, None), ("R", 1, 1), ("L", 2, 2), ("R", 2, 6),
            ("L", 4, 4)])
        # Offset 7 (A-Y) is deliberately *not* used: it is newer.
        self.assertNotIn(7, [s[2] for s in seqs])

    def test_a_and_b_were_already_connected_at_p(self):
        r = self.investigation.resolve("Q", ident("A"), ident("B"))
        self.assertEqual(r["status"], "connected")
        self.assertEqual(r["first_connected_offset"], 2)
        self.assertTrue(r["connected_at_P"])
        self.assertEqual(r["hop_count"], 2)

    def test_f_and_b_are_disconnected_and_g_is_not_seen(self):
        r = self.investigation.resolve("Q", ident("F"), ident("B"))
        self.assertEqual(r["status"], "disconnected")
        self.assertIsNone(r["first_connected_offset"])
        r = self.investigation.resolve("Q", ident("G"), ident("A"))
        self.assertEqual(r["status"], "not_seen")
        self.assertEqual(r["missing"], [ident("G")])

    def test_restricted_to_the_requested_prefix(self):
        # A 10th matching row exists in the warehouse but is beyond Q.
        inv = ResponseInvestigation.build(
            os.path.join(self.base, "q8.db"), self.wh, [JOB],
            {JOB: 4}, {JOB: 8})
        self.assertEqual([g["group_id"] for g in inv.groups("Q")["groups"]],
                         [1, 6])
        self.assertEqual(inv.status()["counts"]["inv_results"], 8)
        inv.close()

    def test_reopen_without_the_warehouse(self):
        moved = os.path.join(self.base, "moved.db")
        os.replace(self.wh, moved)
        inv = ResponseInvestigation.open(self.inv)
        try:
            r = inv.resolve("Q", ident("A"), ident("D"))
            self.assertEqual(r["first_connected_offset"], 6)
            self.assertEqual(inv.status()["warehouse_id"],
                             self.investigation.warehouse_id)
            self.assertEqual(sorted(g["group_id"] for g in
                                    inv.groups("Q")["groups"]), [1, 6])
        finally:
            inv.close()


class TestIdentity(Case):
    def test_same_names_in_different_jobs_do_not_merge(self):
        rows_a = [_match(1, 1, 1), _match(2, 2, 1)]
        rows_b = [_match(1, 1, 1), _match(2, 2, 1)]
        mirror = os.path.join(self.base, "mirror.db")
        # One warehouse, two jobs with identical identities.
        wh = SQLiteWarehouse(self.wh, create=True)
        try:
            wh.write_page("job-a", "results", 1, 2, rows_a)
            wh.write_page("job-b", "results", 1, 2, rows_b)
        finally:
            wh.close()
        inv = ResponseInvestigation.build(
            mirror, self.wh, ["job-a", "job-b"],
            {"job-a": 2, "job-b": 2}, {"job-a": 2, "job-b": 2})
        try:
            ga = {g["group_id"]: g for g in inv.groups("Q")["groups"]}
            self.assertEqual(len(ga), 2)
            self.assertEqual(sorted(g["n_nodes"] for g in ga.values()),
                             [3, 3])
            r = inv.resolve("Q", ident("A", "job-a"), ident("B", "job-b"))
            self.assertEqual(r["status"], "different_jobs")
            self.assertIsNone(r["first_connected_offset"])
        finally:
            inv.close()

    def test_left_and_right_are_distinct(self):
        # A response with the same partition/seq text as a request must not
        # be confused with it: nodes are (job, side, partition, seq).
        rows = [_match(1, 1, 1)]
        self.warehouse(rows)
        inv = self.build({JOB: 1}, {JOB: 1})
        try:
            members = inv.group_members("Q", 1)["members"]
            self.assertEqual([(m["side"], m["seq"]) for m in members],
                             [("L", 1), ("R", 1)])
        finally:
            inv.close()

    def test_seq_is_arbitrary_precision(self):
        big = 2 ** 80 + 3
        wh = SQLiteWarehouse(self.wh, create=True)
        try:
            wh.write_page(JOB, "results", 1, 2, [
                (1, {"kind": "match", "lp": "lp", "ls": str(big),
                     "rp": "rp", "rs": str(big + 1), "k": "d",
                     "ltime": "1", "rtime": "2"}),
                (2, {"kind": "match", "lp": "lp", "ls": str(big),
                     "rp": "rp", "rs": "9", "k": "d",
                     "ltime": "1", "rtime": "2"}),
            ])
        finally:
            wh.close()
        inv = self.build({JOB: 2}, {JOB: 2})
        try:
            members = inv.group_members("Q", 1)["members"]
            seqs = [(m["side"], m["seq"]) for m in members]
            # numeric order: R#9 before R#(big+1), and L#big sorts after R#9
            # left sorts before right; seqs compare numerically
            self.assertEqual(seqs, [("L", big), ("R", 9), ("R", big + 1)])
            found = inv.request_node(
                "Q", {"job_id": JOB, "partition": "lp", "seq": big})
            self.assertEqual(found["seq"], big)
        finally:
            inv.close()


class TestChanges(Case):
    def test_new_node_inside_one_old_group_is_not_a_merge(self):
        rows = [_match(1, 1, 1), _match(2, 2, 1), _match(3, 3, 1)]
        self.warehouse(rows)
        inv = self.build({JOB: 2}, {JOB: 3})
        try:
            change = inv.group_change(1)
            self.assertEqual(change["origin"], "same")
            self.assertEqual(inv.change_sources(1)["source_group_ids"], [1])
            self.assertEqual(change["added_nodes"], 1)
            self.assertEqual(change["added_edges"], 1)
        finally:
            inv.close()

    def test_new_edge_inside_one_old_group_is_not_a_merge(self):
        rows = [_match(1, 1, 1), _match(2, 2, 1), _match(3, 1, 2),
                _match(4, 2, 2)]
        self.warehouse(rows)
        inv = self.build({JOB: 2}, {JOB: 4})
        try:
            change = inv.group_change(1)
            self.assertEqual(change["origin"], "same")
            self.assertEqual(change["added_edges"], 2)
            self.assertEqual(change["added_nodes"], 1)   # R#2 is new
        finally:
            inv.close()

    def test_new_group_from_scratch(self):
        rows = [_match(1, 1, 1), _match(2, 2, 1), _match(3, 3, 1),
                _match(4, 4, 2)]
        self.warehouse(rows)
        inv = self.build({JOB: 3}, {JOB: 4})
        try:
            gid = inv.request_node("Q", ident("D"))["group_id"]
            change = inv.group_change(gid)
            self.assertEqual(change["origin"], "new")
            self.assertEqual(inv.change_sources(gid)["source_group_ids"], [])
            self.assertEqual(change["added_nodes"], 2)
        finally:
            inv.close()

    def test_empty_prefixes(self):
        rows = [_match(1, 1, 1)]
        self.warehouse(rows)
        inv = self.build({JOB: 0}, {JOB: 1})
        try:
            self.assertEqual(inv.groups("P")["groups"], [])
            change = inv.group_change(1)
            self.assertEqual(change["origin"], "new")
            r = inv.resolve("Q", ident("A"), ident("A"))
            self.assertEqual(r["status"], "connected")
            self.assertEqual(r["first_connected_offset"], 0)
            r = inv.resolve("Q", ident("A"),
                            {"job_id": JOB, "partition": "lp", "seq": 2})
            self.assertEqual(r["status"], "not_seen")
            self.assertFalse(r["connected_at_P"])
        finally:
            inv.close()


class TestEqualLengthTie(Case):
    """Two shortest chains of the same length: the canonical chain must be
    the one that is smallest step by step (next node, then edge offset)."""

    def test_tie_is_broken_by_next_node_then_offset(self):
        # A-X1 (1), A-X2 (2), X1-C (3), X2-D (4), C-M (5), D-M (6).
        # Both A-X1-C-M and A-X2-D-M are 3 hops and connect C/D at offset
        # 5.  Identity order is L#1(A)=1, L#2(B)=2, L#3(C)=3, R#1(X1)=4,
        # R#2(X2)=5, R#3(M)=6, R#4(D)=7.  The smaller next node is X1, so
        # the canonical chain is A-X1-C-M even though C-M (5) is newer
        # than D-M (6)'s step to D... and X1-C (3) shares its offset with
        # nothing else, so only the node order decides.
        rows = [_match(1, 1, 1), _match(2, 1, 2), _match(3, 3, 1),
                _match(4, 4, 2), _match(5, 3, 3), _match(6, 4, 3)]
        self.warehouse(rows)
        inv = self.build({JOB: 5}, {JOB: 6})
        try:
            r = inv.resolve(
                "Q", ident("A"), {"job_id": JOB, "partition": "lp",
                                  "seq": 3})  # A to C
            self.assertEqual(r["status"], "connected")
            self.assertEqual(r["first_connected_offset"], 3)
            self.assertEqual(r["hop_count"], 2)
        finally:
            inv.close()

    def test_chain_is_smallest_next_node_then_smallest_offset(self):
        # A-X1 (1), A-X2 (2), B-X2 (3), B-X1 (4).  A and B first meet at
        # offset 3 through X2; at that prefix only A-X2 and B-X2 exist, so
        # the chain must be the 2-hop A-X2-B and *not* the 2-hop A-X1-B
        # that a later row (B-X1 at 4) would allow.
        rows = [_match(1, 1, 1), _match(2, 1, 2), _match(3, 2, 2),
                _match(4, 2, 1)]
        self.warehouse(rows)
        inv = self.build({JOB: 4}, {JOB: 4})
        try:
            r = inv.resolve("Q", ident("A"), ident("B"))
            self.assertEqual(r["first_connected_offset"], 3)
            self.assertEqual(r["hop_count"], 2)
            seqs = [(c["node"]["side"], c["node"]["seq"],
                     None if c["edge"] is None else c["edge"]["results_offset"])
                    for c in r["chain"]]
            self.assertEqual(seqs, [("L", 1, None), ("R", 2, 2),
                                    ("L", 2, 3)])
            self.assertNotIn(4, [s[2] for s in seqs])
        finally:
            inv.close()

    def test_later_shorter_edge_cannot_explain_the_first_connection(self):
        # A and B first connect at offset 2 through X (A-X 1, X-B 2).
        # A direct A-B row at offset 5 would be a 1-hop chain, but it is
        # newer and must not explain the first connection.
        rows = [_match(1, 1, 1), _match(2, 2, 1), _match(3, 3, 2),
                _match(4, 3, 3), _match(5, 1, 2)]
        self.warehouse(rows)
        inv = self.build({JOB: 2}, {JOB: 5})
        try:
            r = inv.resolve("Q", ident("A"), ident("B"))
            self.assertEqual(r["first_connected_offset"], 2)
            self.assertEqual(r["hop_count"], 2)
            self.assertEqual([c["edge"]["results_offset"]
                              for c in r["chain"][1:]], [1, 2])
        finally:
            inv.close()


class TestRefusals(Case):
    def setUp(self):
        super().setUp()
        self.warehouse([_match(1, 1, 1), _match(2, 2, 1)])

    def test_pq_validation(self):
        for p, q in (({JOB: 3}, {JOB: 2}), ({JOB: -1}, {JOB: 2}),
                     ({JOB: "2"}, {JOB: 2})):
            with self.assertRaises(InvestigationInputError):
                self.build(p, q)
        with self.assertRaises(InvestigationInputError):
            ResponseInvestigation.build(
                self.inv, self.wh, [JOB], {"other": 1}, {JOB: 1})
        self.assertFalse(os.path.exists(self.inv))

    def test_out_of_range_and_gap(self):
        with self.assertRaises(InvestigationInputError) as ctx:
            self.build({JOB: 1}, {JOB: 3})
        self.assertIn(JOB, str(ctx.exception))
        self.assertIn("3", str(ctx.exception))
        self.assertFalse(os.path.exists(self.inv))
        inv = self.build({JOB: 1}, {JOB: 2})
        inv.close()
        self.assertTrue(os.path.exists(self.inv))

    def test_gap_in_warehouse(self):
        wh = SQLiteWarehouse(self.wh, create=False)
        try:
            wh.write_page(JOB, "results", 5, 5,
                          [_match(5, 3, 1)])
        finally:
            wh.close()
        with self.assertRaises(InvestigationInputError) as ctx:
            self.build({JOB: 1}, {JOB: 5})
        self.assertIn("gap", str(ctx.exception))
        self.assertFalse(os.path.exists(self.inv))

    def test_unknown_kind_and_broken_match(self):
        wh = SQLiteWarehouse(self.wh, create=True)
        try:
            wh.write_page(JOB, "results", 3, 3, [
                (3, {"kind": "weird", "lp": "lp", "ls": "1", "rp": "rp",
                     "rs": "1", "k": "d", "ltime": "1", "rtime": "2"})])
        finally:
            wh.close()
        with self.assertRaises(InvestigationInputError) as ctx:
            self.build({JOB: 1}, {JOB: 3})
        msg = str(ctx.exception)
        self.assertIn(JOB, msg)
        self.assertIn("3", msg)
        self.assertIn("weird", msg)

    def test_malformed_match_row(self):
        sh = os.path.join(self.base, "bad.db")
        wh = SQLiteWarehouse(sh, create=True)
        try:
            wh.write_page(JOB, "results", 1, 1, [
                (1, {"kind": "match", "lp": "lp", "ls": None, "rp": "rp",
                     "rs": "1", "k": "d", "ltime": "1", "rtime": "2"})])
        finally:
            wh.close()
        with self.assertRaises(InvestigationInputError) as ctx:
            ResponseInvestigation.build(os.path.join(self.base, "x.db"), sh,
                                        [JOB], {JOB: 1}, {JOB: 1})
        self.assertIn("uninterpretable", str(ctx.exception))

    def test_existing_file_requires_overwrite(self):
        inv = self.build({JOB: 1}, {JOB: 2})
        inv.close()
        with self.assertRaises(InvestigationInputError):
            self.build({JOB: 1}, {JOB: 2})
        inv = self.build({JOB: 1}, {JOB: 1}, overwrite=True)
        inv.close()
        self.assertEqual(ResponseInvestigation.open(
            self.inv).status()["positions"]["Q"], {JOB: 1})

    def test_failed_build_leaves_no_file_and_keeps_the_old_one(self):
        inv = self.build({JOB: 1}, {JOB: 2})
        old_id = inv.investigation_id
        inv.close()
        with self.assertRaises(InvestigationInputError):
            self.build({JOB: 1}, {JOB: 9}, overwrite=True)
        self.assertTrue(os.path.exists(self.inv))
        inv = ResponseInvestigation.open(self.inv)
        try:
            self.assertEqual(inv.investigation_id, old_id)
        finally:
            inv.close()
        leftovers = [n for n in os.listdir(self.base) if ".tmp-" in n]
        self.assertEqual(leftovers, [])

    def test_not_a_warehouse(self):
        bogus = os.path.join(self.base, "bogus.db")
        with open(bogus, "wb") as f:
            f.write(b"not sqlite")
        with self.assertRaises(InvestigationFormatError):
            ResponseInvestigation.build(
                os.path.join(self.base, "y.db"), bogus, [JOB],
                {JOB: 1}, {JOB: 1})


class TestFormat(Case):
    def test_open_errors(self):
        self.warehouse([_match(1, 1, 1)])
        with self.assertRaises(InvestigationFormatError):
            ResponseInvestigation.open(os.path.join(self.base, "nope.db"))
        plain = os.path.join(self.base, "plain.db")
        sqlite3.connect(plain).execute("CREATE TABLE x(y)").connection.commit()
        with self.assertRaises(InvestigationFormatError):
            ResponseInvestigation.open(plain)
        inv = self.build({JOB: 1}, {JOB: 1})
        inv.close()
        # unknown version
        conn = sqlite3.connect(self.inv)
        conn.execute("UPDATE inv_meta SET format_version=99")
        conn.commit()
        conn.close()
        with self.assertRaises(InvestigationFormatError):
            ResponseInvestigation.open(self.inv)

    def test_busy_lock(self):
        self.warehouse([_match(1, 1, 1)])
        inv = self.build({JOB: 1}, {JOB: 1})
        inv.close()
        from response_investigation.store import _Lock
        held = _Lock(os.path.abspath(self.inv))
        held.acquire()
        try:
            with self.assertRaises(InvestigationBusyError):
                self.build({JOB: 1}, {JOB: 1}, overwrite=True)
        finally:
            held.release()
        inv = self.build({JOB: 1}, {JOB: 1}, overwrite=True)
        inv.close()


class TestPaging(Case):
    def test_groups_members_and_edges_paginate(self):
        # Five independent pairs on contiguous offsets 1..5.
        rows = [_match(i + 1, i + 1, i + 1) for i in range(5)]
        self.warehouse(rows)
        inv = self.build({JOB: 5}, {JOB: 5})
        try:
            seen = []
            after = None
            while True:
                page = inv.groups("Q", after=after, limit=2)
                seen.extend(g["group_id"] for g in page["groups"])
                if not page["has_more"]:
                    break
                after = page["next_after"]
            self.assertEqual(seen, [1, 2, 3, 4, 5])
            members = inv.group_members("Q", 1, limit=1)
            self.assertTrue(members["has_more"])
            self.assertEqual(len(members["members"]), 1)
            all_members = []
            after = None
            while True:
                page = inv.group_members("Q", 1, after=after, limit=1)
                all_members.extend(page["members"])
                if not page["has_more"]:
                    break
                after = page["next_after"]
            self.assertEqual(len(all_members), 2)
            page = inv.group_edges("Q", 3, limit=1)
            self.assertEqual([e["results_offset"] for e in page["edges"]],
                             [3])
            self.assertFalse(page["has_more"])
        finally:
            inv.close()

    def test_edge_order_is_job_then_numeric_offset(self):
        rows = [(o, {"kind": "match", "lp": "lp", "ls": str(o),
                     "rp": "rp", "rs": "1", "k": "d", "ltime": "1",
                     "rtime": "2"}) for o in range(1, 11)]
        self.warehouse(rows)
        inv = self.build({JOB: 3}, {JOB: 10})
        try:
            edges = inv.group_edges("Q", 1, limit=1000)["edges"]
            # numeric, not text, offset order
            self.assertEqual([e["results_offset"] for e in edges],
                             [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        finally:
            inv.close()


class TestCli(Case):
    def run_cli(self, *argv):
        from response_investigation.cli import main
        return main(list(argv))

    def test_build_query_and_exit_codes(self):
        self.warehouse([_match(1, 1, 1), _match(2, 2, 1), _match(3, 3, 1)])
        self.assertEqual(self.run_cli(
            "build", "--db", self.inv, "--warehouse", self.wh,
            "--jobs", JOB, "--P", f"{JOB}=2", "--Q", f"{JOB}=3"), 0)
        self.assertEqual(self.run_cli("status", "--db", self.inv), 0)
        self.assertEqual(self.run_cli("groups", "--db", self.inv,
                                      "--step", "Q", "--limit", "1"), 0)
        self.assertEqual(self.run_cli(
            "resolve", "--db", self.inv, "--step", "Q",
            "--a", f"{JOB}:lp:1", "--b", f"{JOB}:lp:3"), 0)
        self.assertEqual(self.run_cli("chain", "--db", self.inv, "--step", "Q",
                                      "--a", f"{JOB}:lp:1",
                                      "--b", f"{JOB}:lp:3", "--limit", "1"), 0)
        # rejected input -> 1
        self.assertEqual(self.run_cli(
            "build", "--db", self.inv + "2", "--warehouse", self.wh,
            "--jobs", JOB, "--P", f"{JOB}=3", "--Q", f"{JOB}=2"), 1)
        # missing file -> 4
        self.assertEqual(self.run_cli("status", "--db",
                                      os.path.join(self.base, "none.db")), 4)
        # unknown job -> 1
        self.assertEqual(self.run_cli(
            "build", "--db", self.inv + "3", "--warehouse", self.wh,
            "--jobs", JOB, "--P", "other=1", "--Q", f"{JOB}=1"), 1)


if __name__ == "__main__":
    unittest.main()
