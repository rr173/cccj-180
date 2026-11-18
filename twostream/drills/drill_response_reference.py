"""Drill: independent reference equivalence, prefix by prefix.

A deterministic small data set is generated (several jobs, shared
partitions and seqs across jobs, responses shared by many requests,
unmatched lpad rows, >64-bit times) and placed in the warehouse with the
real write-page interface.  For every observed prefix pair ``(P, Q)`` the
drill checks, against ``drills.response_reference`` -- a separate dense
implementation that shares no code with the tool:

  * every node's group at P and Q, and the full ordered group listing;
  * every Q group's origin (merge / same / new), its P source groups and
    its added nodes and edges;
  * group members and ordered match edges, paged;
  * for request pairs, the first-connection offset (recomputed by adding
    edges one offset at a time and checking reachability explicitly), the
    ``connected_at_P`` flag and the complete canonical chain at that
    offset.

It then adds another job whose identities repeat another job's, and proves
that no chain crosses jobs.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills import response_reference as ref
from drills.response_common import check, fresh_dir, make_warehouse
from response_investigation import ResponseInvestigation

JOB1 = "job-alpha"
JOB2 = "job-beta"
JOB3 = "job-gamma"
BIG = 2 ** 70


def _row(offset, kind, lp=None, ls=None, rp=None, rs=None, k="dev",
         ltime="10", rtime="11"):
    return (offset, {"kind": kind, "lp": lp, "ls": None if ls is None else str(ls),
                     "rp": rp, "rs": None if rs is None else str(rs), "k": k,
                     "ltime": ltime, "rtime": rtime})


def _data():
    """Deliberately shared responses and reused identities."""
    rows = []
    # Offset 1..13 of job1: prefix 4/9 style scenario with richer merges.
    pattern = [
        (1, "A", "X"), (2, "B", "X"), (3, "C", "Y"), (4, "D", "Y"),
        (5, "E", "Y"), (6, "B", "Y"), (7, "A", "Y"), (8, "F", "Z"),
        (9, "H", "Z"),               # H shares Z with F
        (10, "I", "W"), (11, "J", "W"), (12, "I", "V"),
        (13, "K", "V"),
    ]
    for offset, left, right in pattern:
        rows.append(_row(offset, "match", "lp", ord(left) - 64, "rp",
                         ord(right) - 64))
    # An lpad at 14: K2 exists only as an unmatched request.
    rows.append(_row(14, "lpad", "lp", 99, ltime="10", rtime="0"))
    # job2 repeats the exact same partition/seq numbers about a big time.
    big_rows = []
    for offset, left, right in [(1, "A", "X"), (2, "B", "X"),
                                (3, "A", "Y")]:
        big_rows.append(_row(offset, "match", "lp", ord(left) - 64, "rp",
                             ord(right) - 64, k="big", ltime=str(BIG),
                             rtime=str(BIG + 1)))
    # job3: a response-only start, so its first offsets are one-sided.
    third = [
        _row(1, "match", "lp", 1, "rp", 1),
        _row(2, "rpad", None, None, "rp", 2, rtime="11"),
        _row(3, "match", "lp", 1, "rp", 3),
    ]
    return {JOB1: rows, JOB2: big_rows, JOB3: third}


def _node(job, side, partition, seq):
    return (job, side, partition, int(seq))


def _tool_nodes(inv, step):
    """All nodes of a step from paged member listings, with group ids."""
    out = {}
    lists = []
    after = None
    while True:
        page = inv.groups(step, after=after, limit=4)
        lists.extend(page["groups"])
        if not page["has_more"]:
            break
        after = page["next_after"]
    for g in lists:
        members = []
        cursor = None
        while True:
            page = inv.group_members(step, g["group_id"], after=cursor,
                                     limit=3)
            members.extend(page["members"])
            if not page["has_more"]:
                break
            cursor = page["next_after"]
        check(len(members) == g["n_nodes"],
              f"{step} group {g['group_id']} pages back to its n_nodes")
        for m in members:
            out[_node(m["job_id"], m["side"], m["partition"],
                      int(m["seq"]))] = g["group_id"]
    return lists, out


def _compare_prefix(inv, rows, job_ids, p, q, label):
    rows_q = ref.load_rows(inv_warehouse(inv), job_ids, q)
    comp_q = ref.components(ref.adjacency(rows_q, q))
    comp_p = ref.components(ref.adjacency(rows, p))
    groups_q = {}
    for members in comp_q.values():
        groups_q[members[0]] = members
    groups_p = {}
    for members in comp_p.values():
        groups_p[members[0]] = members

    q_list, q_map = _tool_nodes(inv, "Q")
    p_list, p_map = _tool_nodes(inv, "P")
    q_id_nodes = {g["group_id"]: _node(g["group_node"]["job_id"],
                                       g["group_node"]["side"],
                                       g["group_node"]["partition"],
                                       g["group_node"]["seq"])
                  for g in q_list}
    p_id_nodes = {g["group_id"]: _node(g["group_node"]["job_id"],
                                       g["group_node"]["side"],
                                       g["group_node"]["partition"],
                                       g["group_node"]["seq"])
                  for g in p_list}
    check(set(q_id_nodes.values()) == set(groups_q),
          f"{label}: every Q group id is the smallest node of its component")
    check(set(p_id_nodes.values()) == set(groups_p),
          f"{label}: every P group id is the smallest node of its component")
    check({n: q_id_nodes[g] for n, g in q_map.items()} ==
          {n: members[0] for n, members in comp_q.items()},
          f"{label}: Q memberships match the reference")
    check({n: p_id_nodes[g] for n, g in p_map.items()} ==
          {n: members[0] for n, members in comp_p.items()},
          f"{label}: P memberships match the reference")
    check([g["group_id"] for g in q_list] == sorted(q_id_nodes),
          f"{label}: Q listing is ordered by group id")

    expected_changes = ref.group_changes(comp_p, comp_q)
    q_node_of_gid = {node: gid for gid, node in q_id_nodes.items()}
    p_node_of_gid = {node: gid for gid, node in p_id_nodes.items()}
    for key, (origin, p_keys, p_nodes) in expected_changes.items():
        # Translate the reference's smallest-node keys into tool group ids.
        gid = q_node_of_gid[key]
        expected_sources = sorted(p_node_of_gid[n] for n in p_keys)
        change = inv.group_change(gid)
        check(change["origin"] == origin,
              f"{label}: Q group {gid} origin {origin}")
        got_sources = []
        after = None
        while True:
            page = inv.change_sources(gid, after=after, limit=1)
            got_sources.extend(page["source_group_ids"])
            if not page["has_more"]:
                break
            after = page["next_after"]
        check(got_sources == expected_sources,
              f"{label}: Q group {gid} sources {expected_sources}")
        check(change["p_node_count"] == p_nodes,
              f"{label}: Q group {gid} P node count")
        check(change["added_nodes"] == len(groups_q[key]) - p_nodes,
              f"{label}: Q group {gid} added nodes")
        expected_edges = _edge_count(rows_q, q, key, groups_q[key])
        p_edges = _p_edge_count(rows, p, comp_p, groups_q[key])
        check(change["added_edges"] == expected_edges - p_edges,
              f"{label}: Q group {gid} added edges")
        listed = _page_edges(inv, "Q", gid)
        check(len(listed) == expected_edges,
              f"{label}: Q group {gid} edge page count")
        check([(e["job_id"], e["results_offset"]) for e in listed] ==
              sorted((j, o) for j, o, _k, l, r in
                     _edges_of(rows_q, q, groups_q[key])),
              f"{label}: Q group {gid} edges ordered")

    # P groups are identified by their smallest node; locate the tool id.
    p_gid_of_node = {node: gid for gid, node in p_id_nodes.items()}
    for members in groups_p.values():
        gid = p_gid_of_node[members[0]]
        listed = _page_edges(inv, "P", gid)
        expected = _edges_of(rows, p, members)
        check([(e["job_id"], e["results_offset"]) for e in listed] ==
              sorted((j, o) for j, o, _k, l, r in expected),
              f"{label}: P group {gid} edges ordered")

    # Pairwise chronology for every same-job request pair.
    for job in job_ids:
        by_job = rows_q
        nodes = sorted(n for n in comp_q if n[0] == job and n[1] == "L")
        for i, a in enumerate(nodes):
            for b in nodes[i + 1:]:
                q_map.get(a)
                if a not in q_map or b not in q_map:
                    continue
                k = ref.first_connection(by_job, job, q[job], a, b)
                got = inv.resolve("Q", _ident(a), _ident(b))
                connected = comp_q[a][0] == comp_q[b][0]
                check(got["status"] == ("connected" if connected else
                                        "disconnected"),
                      f"{label}: {a} vs {b} status")
                check(got["first_connected_offset"] == k,
                      f"{label}: {a} vs {b} first offset {k}")
                at_p = (a in comp_p and b in comp_p and
                        comp_p[a][0] == comp_p[b][0])
                check(got["connected_at_P"] == at_p,
                      f"{label}: {a} vs {b} connected_at_P")
                if not connected:
                    check(got["chain"] == [], f"{label}: no chain")
                    continue
                prefix_adj = ref.adjacency(
                    by_job, q, upto={j: (k if j == job else 0)
                                     for j in job_ids})
                expected_chain = ref.canonical_chain(
                    by_job, job, prefix_adj, a, b, k)
                tool = [( _node(c["node"]["job_id"], c["node"]["side"],
                                c["node"]["partition"], c["node"]["seq"]),
                          None if c["edge"] is None
                          else c["edge"]["results_offset"])
                        for c in got["chain"]]
                check(tool == expected_chain,
                      f"{label}: {a} vs {b} canonical chain")
                check(_chain_is_valid(rows, q, got["chain"], k),
                      f"{label}: chain uses only edges at/below {k}")


def _ident(node):
    return {"job_id": node[0], "partition": node[2], "seq": node[3]}


def _edges_of(rows, bounds, members):
    member_set = set(members)
    out = []
    for job, rs in rows.items():
        for offset, kind, left, right in rs:
            if kind != "match" or offset > bounds[job]:
                continue
            if left in member_set and right in member_set:
                out.append((job, offset, kind, left, right))
    return out


def _edge_count(rows, bounds, gid, members):
    return len(_edges_of(rows, bounds, members))


def _p_edge_count(rows, bounds, comp_p, members):
    member_set = set(members)
    out = 0
    for job, rs in rows.items():
        for offset, kind, left, right in rs:
            if kind != "match" or offset > bounds[job]:
                continue
            if left in member_set and right in member_set:
                out += 1
    return out


def _page_edges(inv, step, gid):
    out = []
    after = None
    while True:
        page = inv.group_edges(step, gid, after=after, limit=2)
        out.extend(page["edges"])
        if not page["has_more"]:
            break
        after = page["next_after"]
    return out


def _chain_is_valid(rows, bounds, chain, k):
    """Each hop must be a real match row of the same job at offset <= k."""
    for i, step in enumerate(chain):
        if i == 0:
            continue
        prev = chain[i - 1]["node"]
        cur = step["node"]
        a = _node(prev["job_id"], prev["side"], prev["partition"],
                  prev["seq"])
        b = _node(cur["job_id"], cur["side"], cur["partition"], cur["seq"])
        edge = step["edge"]["results_offset"]
        job = step["edge"]["job_id"]
        if edge > k or edge > bounds[job]:
            return False
        found = False
        for offset, kind, left, right in rows[job]:
            if offset != edge or kind != "match":
                continue
            if {left, right} == {a, b}:
                found = True
                break
        if not found:
            return False
    return True


_ACTIVE_WAREHOUSE = None


def inv_warehouse(inv):
    return _ACTIVE_WAREHOUSE


def main() -> int:
    global _ACTIVE_WAREHOUSE
    base = fresh_dir("reference")
    data = _data()
    wh = os.path.join(base, "warehouse.db")
    make_warehouse(wh, data)
    _ACTIVE_WAREHOUSE = wh
    job_ids = [JOB1, JOB2, JOB3]
    length = {j: len(data[j]) for j in job_ids}

    # Prefix pairs that exercise 0, partial and full prefixes.
    pairs = [
        ({JOB1: 0, JOB2: 0, JOB3: 0}, {JOB1: 0, JOB2: 0, JOB3: 0}),
        ({JOB1: 4, JOB2: 0, JOB3: 0}, {JOB1: 9, JOB2: 0, JOB3: 0}),
        ({JOB1: 9, JOB2: 1, JOB3: 2}, {JOB1: 14, JOB2: 3, JOB3: 3}),
        ({JOB1: 2, JOB2: 3, JOB3: 1}, {JOB1: 14, JOB2: 3, JOB3: 3}),
        ({JOB1: 14, JOB2: 3, JOB3: 3}, {JOB1: 14, JOB2: 3, JOB3: 3}),
        ({JOB1: 13, JOB2: 3, JOB3: 3}, {JOB1: 14, JOB2: 3, JOB3: 3}),
    ]
    for n, (p, q) in enumerate(pairs):
        inv_path = os.path.join(base, f"inv{n}.db")
        inv = ResponseInvestigation.build(inv_path, wh, job_ids, p, q)
        try:
            for j in job_ids:
                check(p[j] <= length[j] and q[j] <= length[j],
                      f"prefix pair {n}: P/Q are within the warehouse")
            cr = ref.load_rows(wh, job_ids, q)
            _compare_prefix(inv, cr, job_ids, p, q, f"prefix pair {n}")
            print(f"   ok: prefix pair {n} matches the reference")
        finally:
            inv.close()
            _ACTIVE_WAREHOUSE = wh

    # Cross-job identities: job2 repeats job1's partition/seq text, but a
    # match edge never crosses a job, so no chain may connect them.
    inv = ResponseInvestigation.open(os.path.join(base, "inv5.db"))
    try:
        r = inv.resolve("Q", {"job_id": JOB1, "partition": "lp", "seq": 1},
                        {"job_id": JOB2, "partition": "lp", "seq": 1})
        check(r["status"] == "different_jobs",
              "requests with equal identities in different jobs are not "
              "connected")
        check(r["first_connected_offset"] is None and r["chain"] == [],
              "a cross-job pair has no offset and no chain")
        n1 = inv.request_node("Q", {"job_id": JOB1, "partition": "lp",
                                    "seq": 1})
        n2 = inv.request_node("Q", {"job_id": JOB2, "partition": "lp",
                                    "seq": 1})
        check(n1["node_id"] != n2["node_id"],
              "the same partition/seq text is a different node per job")
    finally:
        inv.close()
    print("\n   all prefixes agree with the independent reference")
    print("REFERENCE DRILL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
