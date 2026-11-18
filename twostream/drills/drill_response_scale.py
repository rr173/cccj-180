"""Drill: scale, locality and bounded memory.

Three data shapes are generated at run time under
``drills/out/response/scale`` (never committed) and placed in one warehouse
through the real write-page interface:

* **fan-out**: one response shared by many requests (a group with a high
  degree);
* **long chain with forks and cycles**: a chain of nodes with extra
  branches and back-edges, so the component is large, paths are long and
  there are many equal-length routes;
* **unrelated groups**: many small independent pairs.

Total: at least 200,000 match rows.  The drill then reports, from the real
queries (not from timings or estimates):

* the group of a few nodes, ordered group listings and paged members and
  edges (with their sizes);
* the first-connection offset and the first hops of the canonical chain for
  a pair inside the long component, and a source-offset sample of its
  group's edges;
* peak RSS and the on-disk size of both files.

Locality is proved with the access counters and `EXPLAIN QUERY PLAN`, not
wall-clock time: locating a node reads O(1) rows, a group page reads only
the requested members/edges, and a chain BFS visits only its own component
(never the unrelated groups).
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drills.response_common import check, db_bytes, fresh_dir, rss_kb
from interval_join import SQLiteWarehouse
from response_investigation import ResponseInvestigation

FAN = 3000              # requests sharing one response
CHAIN = 52000           # nodes along the long chain
FORK = 6000             # extra fork/cycle edges
UNRELATED = 80000       # independent requests (2 rows each)
PREFIX_P = 200000       # fixed P bound (must be <= Q and <= rows)


def _match(offset, lseq, rseq, key="dev"):
    return (offset, {"kind": "match", "lp": "lp", "ls": str(lseq),
                     "rp": "rp", "rs": str(rseq), "k": key,
                     "ltime": "1000", "rtime": "1001"})


def build_rows():
    """Return (rows, expect) where rows are contiguous offsets 1..N."""
    rows = []
    offset = 1

    # -- fan-out: one response R#1 matched by FAN distinct requests.
    for i in range(FAN):
        rows.append(_match(offset, i + 1, 1, key="fan"))
        offset += 1

    # -- long chain: request i -- response (i+1) -- request (i+1) -- ...
    # Each chain node is a response shared by two neighbouring requests
    # (except the ends), so the component is one long simple path.
    for i in range(CHAIN):
        rows.append(_match(offset, 1_000_000 + i, 1_000_000 + i, key="chain"))
        offset += 1
        if i + 1 < CHAIN:
            rows.append(_match(offset, 1_000_000 + i + 1,
                               1_000_000 + i, key="chain"))
            offset += 1
    # -- forks and cycles inside the long component: request 2_000_000+j
    # shares two chain responses, creating chords of equal length.
    for j in range(FORK):
        a = (j * 7) % (CHAIN - 1)
        rows.append(_match(offset, 2_000_000 + j, 1_000_000 + a,
                           key="chain"))
        offset += 1
        rows.append(_match(offset, 2_000_000 + j, 1_000_000 + a + 1,
                           key="chain"))
        offset += 1
    # -- unrelated pairs
    for i in range(UNRELATED):
        rows.append(_match(offset, 3_000_000 + i, 3_000_000 + i,
                           key=f"u{i}"))
        offset += 1
        rows.append(_match(offset, 3_500_000 + i, 3_000_000 + i,
                           key=f"u{i}"))
        offset += 1
    return rows


def _rows(conn, sql, args=()):
    return conn.execute(sql, args).fetchall()


def main() -> int:
    base = fresh_dir("scale")
    wh = os.path.join(base, "warehouse.db")
    inv_path = os.path.join(base, "investigation.db")
    print("generating rows ...")
    t0 = time.time()
    rows = build_rows()
    n = len(rows)
    check(n >= 200_000, f"{n} match rows (>= 200,000)")
    print(f"   {n} match rows generated in {time.time() - t0:.1f}s")

    t0 = time.time()
    writer = SQLiteWarehouse(wh, create=True)
    try:
        page = 50_000
        for start in range(0, n, page):
            chunk = rows[start:start + page]
            writer.write_page("job", "results", chunk[0][0], chunk[-1][0],
                              chunk)
    finally:
        writer.close()
    print(f"   warehouse written in {time.time() - t0:.1f}s, "
          f"{db_bytes(wh) / 1e6:.1f} MB")

    t0 = time.time()
    inv = ResponseInvestigation.build(inv_path, wh, ["job"],
                                      {"job": PREFIX_P}, {"job": n})
    build_s = time.time() - t0
    st = inv.status()
    print(f"built in {build_s:.1f}s: {st['counts']}")
    check(st["counts"]["match_edges"] == n,
          "every match row is evidence in the investigation")
    check(st["positions"]["Q"] == {"job": n}, "Q is the requested bound")

    # ---- fan-out group
    fan_gid = inv.request_node("Q", {"job_id": "job", "partition": "lp",
                                     "seq": 1})["group_id"]
    fan = inv.group("Q", fan_gid)
    node_edges = inv.group_edges("Q", fan_gid, limit=5)
    print(f"fan-out group {fan_gid}: {fan['n_nodes']} nodes, "
          f"{fan['n_edges']} edges; first edge page "
          f"{[(e['results_offset'], e['left']['seq'], e['right']['seq']) for e in node_edges['edges']]}")
    check(fan["n_edges"] == FAN, "the fan-out group counts its edges")
    t0 = time.time()
    inv.stats(reset=True)
    members = []
    after = None
    while len(members) < 200:
        page = inv.group_members("Q", fan_gid, after=after, limit=50)
        members.extend(page["members"])
        if not page["has_more"]:
            break
        after = page["next_after"]
    stats = inv.stats()
    print(f"   paged 200 of {fan['n_nodes']} members in "
          f"{time.time() - t0:.3f}s; rows read: {stats}")
    check(len(members) == 200 and stats["member_rows"] <= 205,
          "member paging reads only the requested page")

    # ---- long component: first connection and chain
    a = {"job_id": "job", "partition": "lp", "seq": 1_000_000}
    b = {"job_id": "job", "partition": "lp", "seq": 1_000_000 + CHAIN // 2}
    inv.stats(reset=True)
    t0 = time.time()
    res = inv.resolve("Q", a, b, chain_limit=10)
    elapsed = time.time() - t0
    stats = inv.stats()
    print(f"first connection of two requests {CHAIN // 2} chain steps apart: "
          f"offset {res['first_connected_offset']}, {res['hop_count']} hops, "
          f"{elapsed:.3f}s, rows read: {stats}")
    check(res["status"] == "connected", "the chain requests are connected")
    # The fan-out block occupies offsets 1..FAN.  After it, request L_i
    # meets response R_i and L_{i+1} meets R_i, so L_0 connects to L_k
    # exactly when L_k's first edge arrives: FAN + 2k.
    check(res["first_connected_offset"] == FAN + 2 * (CHAIN // 2),
          "first connection is where the chain actually joins")
    check(res["chain_truncated"] and len(res["chain"]) == 11,
          "the answer keeps only the requested chain prefix")
    # The component has CHAIN requests + CHAIN responses plus one
    # response-only holder per fork; visiting it twice (forward and
    # backward layers) is the expected ceiling.
    component = 2 * CHAIN + FORK
    check(stats["bfs_nodes"] < 3 * component,
          f"the chain BFS visits only its own component "
          f"({stats['bfs_nodes']} node visits for {component} nodes), not "
          f"the {FAN + UNRELATED} unrelated requests")

    # Every hop must be a real match row at or below the reported first
    # connection, never a newer edge (a direct short cut would be wrong).
    bad = [h for h in res["chain"][1:]
           if h["edge"]["results_offset"] > res["first_connected_offset"]]
    check(not bad,
          "every chain hop's raw offset is at or below the first-connection "
          "offset")
    page = inv.chain_page("Q", a, b, limit=5, after=0)
    print("   canonical chain first hops:",
          [(h["node"]["side"], h["node"]["seq"]) for h in page["hops"]])
    check(len(page["hops"]) == 5, "the chain is paged hop by hop")
    # Page the whole chain.  The canonical chain is computed once and
    # cached in SQLite; each page reads only its own hops.
    t0 = time.time()
    collected = 0
    pages = 0
    more = True
    cursor = None
    while more:
        page = inv.chain_page("Q", a, b, limit=1000, after=cursor)
        collected += len(page["hops"])
        pages += 1
        more = page["has_more"]
        cursor = page["next_after"]
        if pages > 100:
            raise AssertionError("chain paging does not terminate")
    print(f"   paged {collected} chain items in {pages} pages, "
          f"{time.time() - t0:.2f}s")
    check(collected == res["hop_count"] + 1,
          "paging reproduces the whole chain (request node + every hop)")

    # ---- edges of the long group, ordered by (job, numeric offset)
    chain_gid = inv.request_node("Q", a)["group_id"]
    t0 = time.time()
    inv.stats(reset=True)
    edge_page = inv.group_edges("Q", chain_gid, limit=100)
    stats = inv.stats()
    print(f"   first 100 edges of the long group in "
          f"{time.time() - t0:.3f}s; rows read: {stats['edge_rows']} "
          f"(group has {inv.group('Q', chain_gid)['n_edges']})")
    check(stats["edge_rows"] <= 101, "edge paging reads only its page")
    print("   edge sample:",
          [(e["results_offset"], e["left"]["seq"], e["right"]["seq"])
           for e in edge_page["edges"][:5]])

    # ---- source-offset sampling of the long group's evidence
    samples = []
    cursor = None
    while len(samples) < 25:
        page = inv.group_edges("Q", chain_gid, after=cursor, limit=5)
        for e in page["edges"]:
            samples.append((e["results_offset"], e["left"]["seq"],
                            e["right"]["seq"]))
            if len(samples) >= 25:
                break
        if not page["has_more"]:
            break
        cursor = page["next_after"]
    print("   source-offset sample (first 25 edges by (job, offset)):",
          samples[:8], "...")
    check([s[0] for s in samples] == sorted(s[0] for s in samples),
          "group edges are paged in numeric offset order")

    # ---- unrelated group lookup is O(1)
    inv.stats(reset=True)
    ua = {"job_id": "job", "partition": "lp", "seq": 3_000_000 + 12345}
    found = inv.request_node("Q", ua)
    stats = inv.stats()
    check(found["group_id"] != chain_gid,
          "an unrelated request has its own group")
    check(stats["node_lookup"] == 1,
          "locating a node reads one node row")
    print(f"   unrelated request -> group {found['group_id']} "
          f"(node lookups: {stats['node_lookup']})")

    # ---- query plans for the indexed accesses
    conn = sqlite3.connect("file:" + inv_path + "?mode=ro", uri=True)
    try:
        plans = {
            "node": _rows(conn, "EXPLAIN QUERY PLAN SELECT gid FROM node_hist"
                                " WHERE node_id=? AND step=?",
                          (1, "Q")),
            "members": _rows(conn, "EXPLAIN QUERY PLAN SELECT n.id FROM"
                                   " node_hist h JOIN nodes n ON n.id="
                                   " h.node_id WHERE h.step=? AND h.gid=?"
                                   " AND n.id>? ORDER BY n.id LIMIT ?",
                             ("Q", 1, 0, 10)),
            "edges": _rows(conn, "EXPLAIN QUERY PLAN SELECT e.job_id FROM"
                                 " inv_results e JOIN node_hist h ON h.node_id"
                                 " = e.node_l AND h.step=? WHERE"
                                 " e.kind='match' AND h.gid=? AND"
                                 " (e.job_id>? OR (e.job_id=? AND"
                                 " e.results_offset>?)) ORDER BY e.job_id,"
                                 " e.results_offset LIMIT ?",
                           ("Q", 1, "a", "a", 0, 10)),
            "sources": _rows(conn, "EXPLAIN QUERY PLAN SELECT pgid FROM"
                                   " change_sources WHERE gid=? AND pgid>?"
                                   " ORDER BY pgid LIMIT ?", (1, 0, 10)),
        }
    finally:
        conn.close()
    for name, plan in plans.items():
        text = " ".join(str(row) for row in plan)
        print(f"   plan[{name}]: {text}")
        check("SCAN" not in text or "COVERING INDEX" in text,
              f"{name} uses an index rather than a full table scan")

    print(f"\npeak RSS: {rss_kb()} kB; investigation "
          f"{db_bytes(inv_path) / 1e6:.1f} MB; warehouse "
          f"{db_bytes(wh) / 1e6:.1f} MB")
    check(rss_kb() < 1_000_000,
          "peak RSS stays well below an in-memory copy of the graph")
    inv.close()
    print("SCALE DRILL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
