"""Independent, dense reference for the shared-response investigation.

It shares no code with ``response_investigation``: it re-reads the
warehouse row by row, keeps whole adjacency lists in Python, and computes
connectivity, merge chronology and evidence chains by explicit loops.
Small inputs only -- that is the point: being obviously correct beats
being scalable.
"""

from __future__ import annotations

import sqlite3
from collections import deque


def _node(job, side, partition, seq):
    return (job, side, partition, int(seq))


def load_rows(warehouse: str, job_ids, bounds):
    """``{job: [(offset, kind, left_node|None, right_node|None)]}`` for
    ``1..bounds[job]`` only."""
    conn = sqlite3.connect("file:" + warehouse + "?mode=ro", uri=True)
    out = {}
    try:
        for job in job_ids:
            rows = []
            for (offset, kind, lp, ls, rp, rs) in conn.execute(
                    "SELECT offset, kind, lp, ls, rp, rs FROM wh_results"
                    " WHERE job_id=? AND offset>=1 AND offset<=?"
                    " ORDER BY offset", (job, bounds[job])):
                if kind == "match":
                    left = _node(job, "L", lp, ls)
                    right = _node(job, "R", rp, rs)
                    rows.append((offset, kind, left, right))
                else:
                    rows.append((offset, kind, None, None))
            out[job] = rows
    finally:
        conn.close()
    return out


def adjacency(rows_by_job, step_bounds, upto=None):
    """Undirected adjacency built by explicit loops.

    ``step_bounds`` maps job -> bound; ``upto`` (a dict job -> offset)
    further restricts each job's edges (used for the first-connection
    prefix)."""
    adj = {}
    for job, rows in rows_by_job.items():
        for offset, kind, left, right in rows:
            if kind != "match":
                continue
            if offset > step_bounds[job]:
                continue
            if upto is not None and offset > upto[job]:
                continue
            adj.setdefault(left, set()).add(right)
            adj.setdefault(right, set()).add(left)
    return adj


def components(adj):
    """``{node: sorted member tuple}`` of the full connected components."""
    seen = set()
    out = {}
    for start in sorted(adj):
        if start in seen:
            continue
        comp = set()
        queue = deque([start])
        seen.add(start)
        while queue:
            cur = queue.popleft()
            comp.add(cur)
            for nxt in adj[cur]:
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        members = tuple(sorted(comp))
        for node in members:
            out[node] = members
    return out


def group_map(adj):
    """``{node: group_id}`` with the same rule as the tool: the smallest
    node of the component."""
    comp = components(adj)
    return {node: members[0] for node, members in comp.items()}


def group_changes(comp_p, comp_q):
    """``{q_gid: (origin, sorted p_gids, p_nodes, added_nodes)}`` where
    ``p_gids`` are the P groups the Q group grew from and ``p_nodes`` the
    number of Q-group nodes that already existed at P."""
    q_groups = {}
    for node, members in comp_q.items():
        q_groups[members[0]] = members
    changes = {}
    for qgid, members in sorted(q_groups.items()):
        p_gids = sorted({comp_p[n][0] for n in members if n in comp_p})
        p_nodes = sum(1 for n in members if n in comp_p)
        if not p_gids:
            origin = "new"
        elif len(p_gids) == 1:
            origin = "same"
        else:
            origin = "merge"
        changes[qgid] = (origin, p_gids, p_nodes)
    return changes


def first_connection(rows_by_job, job, q_bound, node_a, node_b):
    """Explicit incremental chronology.

    Add the job's edges one offset at a time, recompute components from
    scratch and remember the first offset where the two nodes are
    together.  This is the definition, not an optimisation."""
    adj = {node_a: set(), node_b: set()}
    offsets = sorted(rows_by_job[job], key=lambda r: r[0])
    for offset, kind, left, right in offsets:
        if kind != "match" or offset > q_bound:
            continue
        adj.setdefault(left, set()).add(right)
        adj.setdefault(right, set()).add(left)
        if _reachable(adj, node_a, node_b):
            return offset
    return None


def _reachable(adj, source, target):
    if source == target:
        return True
    seen = {source}
    queue = deque([source])
    while queue:
        cur = queue.popleft()
        for nxt in adj.get(cur, ()):
            if nxt == target:
                return True
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
    return False


def shortest_chain(adj, node_a, node_b):
    """Explicit BFS listing every shortest node chain (all lengths equal)."""
    if node_a == node_b:
        return [[node_a]]
    dist = {node_a: 0}
    queue = deque([node_a])
    while queue:
        cur = queue.popleft()
        for nxt in sorted(adj.get(cur, ())):
            if nxt not in dist:
                dist[nxt] = dist[cur] + 1
                queue.append(nxt)
    if node_b not in dist:
        return []
    total = dist[node_b]
    results = []

    def walk(prefix):
        cur = prefix[-1]
        if len(prefix) - 1 == total:
            if cur == node_b:
                results.append(list(prefix))
            return
        for nxt in sorted(adj.get(cur, ())):
            if dist.get(nxt) == dist[cur] + 1 and nxt not in prefix:
                walk(prefix + [nxt])

    walk([node_a])
    return results


def canonical_chain(rows_by_job, job, adj, node_a, node_b, bound):
    """The canonical shortest chain: layer by layer, take the smallest
    (next node, smallest observed edge offset) that stays on a shortest
    path.  Independent of the tool's BFS implementation."""
    if node_a == node_b:
        return [(node_a, 0)]
    dist_a = _distances(adj, node_a)
    dist_b = _distances(adj, node_b)
    if node_b not in dist_a or dist_a[node_b] != dist_b[node_a]:
        return None
    total = dist_a[node_b]
    total = dist_a[node_b]
    chain = [(node_a, None)]
    cur = node_a
    for i in range(total):
        best = None
        for nxt in sorted(adj.get(cur, ())):
            da = dist_a.get(nxt)
            db = dist_b.get(nxt)
            if da is None or db is None or da + db != total:
                continue
            if da != dist_a[cur] + 1:
                continue
            offsets = edge_offsets(rows_by_job, job, cur, nxt, bound)
            if not offsets:
                continue
            cand = (nxt, min(offsets))
            if best is None or cand < best:
                best = cand
        if best is None:
            return None
        chain.append(best)
        cur = best[0]
    return chain


def _distances(adj, source):
    dist = {source: 0}
    queue = deque([source])
    while queue:
        cur = queue.popleft()
        for nxt in adj.get(cur, ()):
            if nxt not in dist:
                dist[nxt] = dist[cur] + 1
                queue.append(nxt)
    return dist


def edge_offsets(rows_by_job, job, node_a, node_b, bound):
    """Every observed offset that carries the edge {node_a, node_b}."""
    result = []
    for offset, kind, left, right in rows_by_job[job]:
        if kind != "match" or offset > bound:
            continue
        if {left, right} == {node_a, node_b}:
            result.append(offset)
    return result


def canon_chain(rows_by_job, job, chain, bound):
    """Turn a node chain into the canonical ``[(node, edge_offset)]`` form
    by taking the smallest observed offset for each hop."""
    steps = []
    for i in range(1, len(chain)):
        offsets = edge_offsets(rows_by_job, job, chain[i - 1], chain[i],
                               bound)
        if not offsets:
            return None
        steps.append((chain[i], min(offsets)))
    return steps
