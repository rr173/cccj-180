"""Shared-response investigation: one immutable SQLite file built from a
read-only snapshot of one twostream warehouse.

For two observation points ``P`` and ``Q`` (each job's own inclusive
result-offset bound) it answers:

* which requests must be investigated together -- the full connected
  components of the *match* graph (not just a response's direct
  neighbours; equal partitions/seqs/device keys in different jobs or on
  different sides never merge);
* how the grouping changed between ``P`` and ``Q`` -- a merge of two or
  more old groups / grown from exactly one old group / completely new;
* for two named requests, the earliest offset at which they became
  connected and the shortest original match chain at that offset.

Implementation notes (the drills pin each of them down):

* **Only ``match`` rows are edges.**  ``lpad``/``rpad``/``late`` rows create
  no node and no edge; a request seen only there is not "seen".
* **One read snapshot.**  Bounds are verified and rows are read in a single
  deferred read transaction; rows beyond ``Q[job]`` and rows committed
  later are invisible to the investigation.
* **Per-job prefixes.**  A graph state is ``{job: bound}``; no global order
  is inferred from event times or cross-job offsets.  Match edges never
  cross a job, so a job's prefix graph is its own first ``bound`` rows.
* **Union-find lives in SQLite.**  Match edges are unioned by rank in
  ``(job, offset)`` order, and components are expanded by SQL recursion /
  path doubling in temp tables, so Python memory does not grow with the
  number of edges, the largest component or a path.
* **Node / group identity.**  Node ids are assigned in
  ``(job text, side L<R, partition text, numeric seq)`` order (text by
  Unicode code point, seq arbitrary precision through ``enc_int``), and a
  group is identified by its smallest node id.
* **First connection from the merge tree.**  Union by rank also builds a
  tree whose edges carry the offset of the union.  Along any root-ward
  path the labels strictly increase, so the offset at which two requests
  first became connected is the larger of the two labels directly below
  their lowest common ancestor; nothing newer is consulted.  The ancestor
  chains are O(log n) long.
* **Shortest chain at that offset.**  Layer-by-layer BFS in SQLite temp
  tables over edges with ``offset <= k``; ties are broken step by step by
  (next node, edge offset).  The canonical chain is materialised once per
  pair/prefix into a temp table, so pages read only their own hops.
* **Bounded queries.**  Group lookup, ordered group listings, members,
  edges, old-group sources and chain hops are indexed keyset pages; no API
  loads a whole component, the whole graph or the whole path into Python.
"""

from __future__ import annotations

import fcntl
import os
import sqlite3
import time as _time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import quote

from interval_join.encoding import enc_int

from .cleanup import remove_db
from .errors import (InvestigationBusyError, InvestigationError,
                     InvestigationFormatError, InvestigationInputError)
from .keys import Node, make_edge
from .request import request_identity
from .source import SourceWarehouse

INVESTIGATION_FORMAT_VERSION = 1
STEPS = ("P", "Q")
STAT_KEYS = ("node_lookup", "group_lookup", "member_rows", "edge_rows",
             "source_rows", "chain_hops", "bfs_nodes", "ancestor_steps")
MAX_PAGE = 1000
DEFAULT_PAGE = 100
NODE_CACHE = 20000      # acceleration hint only, never a source of truth

_TABLES_SCHEMA = """
CREATE TABLE inv_meta(
  id INTEGER PRIMARY KEY CHECK(id=1),
  format_version INTEGER NOT NULL,
  investigation_id TEXT NOT NULL,
  warehouse_id TEXT NOT NULL,
  warehouse_path TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE inv_jobs(
  idx INTEGER PRIMARY KEY,
  job_id TEXT NOT NULL UNIQUE
);
CREATE TABLE inv_positions(
  step TEXT NOT NULL CHECK(step IN ('P','Q')),
  idx INTEGER NOT NULL,
  job_id TEXT NOT NULL,
  bound_txt TEXT NOT NULL,
  PRIMARY KEY(step, idx)
);
-- One row per warehouse result used by the investigation.  `match` rows
-- carry the two endpoint node ids; pads are kept as evidence only.
CREATE TABLE inv_results(
  job_id TEXT NOT NULL,
  results_offset INTEGER NOT NULL,
  kind TEXT NOT NULL,
  lp TEXT, ls TEXT, rp TEXT, rs TEXT,
  k TEXT, ltime TEXT, rtime TEXT,
  node_l INTEGER, node_r INTEGER,
  PRIMARY KEY(job_id, results_offset)
);
-- Nodes and the final union-find forest.  `enc` is the order-preserving
-- text of the arbitrary-precision seq: seq comparisons never go through a
-- SQLite integer.  Node ids follow the documented identity order.
CREATE TABLE nodes(
  id INTEGER PRIMARY KEY,
  job_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK(side IN ('L','R')),
  partition TEXT NOT NULL,
  seq_txt TEXT NOT NULL,
  sort_job INTEGER NOT NULL,
  sort_side INTEGER NOT NULL,
  sort_part TEXT NOT NULL,
  enc TEXT NOT NULL,
  first_seen INTEGER NOT NULL,
  parent INTEGER NOT NULL,
  rank INTEGER NOT NULL,
  UNIQUE(job_id, side, partition, seq_txt)
);
-- Union tree edge: dst was attached under sys when the edge at `off` was
-- processed.  Labels strictly increase root-ward.
CREATE TABLE merge_tree(
  dst INTEGER PRIMARY KEY,
  sys INTEGER NOT NULL,
  off INTEGER NOT NULL
);
CREATE TABLE node_hist(
  node_id INTEGER NOT NULL,
  step TEXT NOT NULL CHECK(step IN ('P','Q')),
  gid INTEGER NOT NULL,
  PRIMARY KEY(node_id, step)
);
CREATE TABLE group_steps(
  step TEXT NOT NULL CHECK(step IN ('P','Q')),
  gid INTEGER NOT NULL,
  n_nodes INTEGER NOT NULL,
  n_edges INTEGER NOT NULL,
  PRIMARY KEY(step, gid)
);
CREATE TABLE group_change(
  gid INTEGER PRIMARY KEY,
  origin TEXT NOT NULL CHECK(origin IN ('same','merge','new')),
  source_count INTEGER NOT NULL,
  p_node_count INTEGER NOT NULL,
  p_edge_count INTEGER NOT NULL,
  added_nodes INTEGER NOT NULL,
  added_edges INTEGER NOT NULL
);
CREATE TABLE change_sources(
  gid INTEGER NOT NULL,
  pgid INTEGER NOT NULL,
  PRIMARY KEY(gid, pgid)
);
"""

_INDEXES_SCHEMA = """
CREATE INDEX idx_results_l ON inv_results(node_l, results_offset);
CREATE INDEX idx_results_r ON inv_results(node_r, results_offset);
CREATE INDEX idx_node_hist_group ON node_hist(step, gid, node_id);
CREATE INDEX idx_merge_sys ON merge_tree(sys);
"""

_REQUIRED_TABLES = {
    "inv_meta", "inv_jobs", "inv_positions", "inv_results", "nodes",
    "merge_tree", "node_hist", "group_steps", "group_change",
    "change_sources",
}

_FIND = ("WITH RECURSIVE up(n, p) AS ("
         " SELECT id, parent FROM nodes WHERE id = ?"
         " UNION ALL"
         " SELECT u.id, u.parent FROM nodes u JOIN up ON u.id = up.p"
         "  WHERE up.p <> up.n)"
         " SELECT n FROM up WHERE p = n LIMIT 1")

_EDGE_FROM = """
SELECT e.job_id, e.results_offset, e.k, e.ltime, e.rtime,
       e.lp, e.ls, e.rp, e.rs
FROM ({union_sql}) u
JOIN inv_results e ON e.job_id = u.job_id
                  AND e.results_offset = u.results_offset
WHERE e.kind = 'match'
"""


# --------------------------------------------------------------- helpers
_HOOK_COUNTS: Dict[str, int] = {}


def _test_hook(name: str) -> None:
    """Inert test hook for the acceptance drills.

    ``RESPONSE_CRASH_AT`` is a comma list of ``point`` or ``point@count``;
    a hit calls ``os._exit(117)`` (abrupt kill, no cleanup).
    ``RESPONSE_BARRIER_DIR`` makes a point (when ``<point>.want`` exists)
    create ``<point>.reached`` and wait for ``<point>.release``.  Neither
    variable affects a normal run.
    """
    spec = os.environ.get("RESPONSE_CRASH_AT", "")
    if spec:
        for item in spec.split(","):
            item = item.strip()
            if not item:
                continue
            point, _, n = item.partition("@")
            count = int(n) if n else 1
            if point == name:
                hit = _HOOK_COUNTS.get(name, 0) + 1
                _HOOK_COUNTS[name] = hit
                if hit >= count:
                    os._exit(117)
    bdir = os.environ.get("RESPONSE_BARRIER_DIR")
    if bdir and os.path.exists(os.path.join(bdir, name + ".want")):
        with open(os.path.join(bdir, name + ".reached"), "w",
                  encoding="utf-8") as f:
            f.write("reached\n")
            f.flush()
            os.fsync(f.fileno())
        deadline = _time.monotonic() + 120
        while not os.path.exists(os.path.join(bdir, name + ".release")):
            if _time.monotonic() > deadline:
                raise InvestigationError(f"barrier {name!r} timed out")
            _time.sleep(0.005)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_jobs(job_ids):
    if isinstance(job_ids, str) or not isinstance(job_ids, (list, tuple)):
        raise InvestigationInputError(
            "jobs must be a list of job id strings")
    seen, out = set(), []
    for j in job_ids:
        if not isinstance(j, str) or not j:
            raise InvestigationInputError(
                f"job id must be a non-empty string, got {j!r}")
        if j in seen:
            raise InvestigationInputError(f"duplicate job id {j!r}")
        seen.add(j)
        out.append(j)
    if not out:
        raise InvestigationInputError("jobs must not be empty")
    return out


def _check_positions(job_ids, values, name):
    if not isinstance(values, dict):
        raise InvestigationInputError(
            f"positions {name} must be a mapping job_id -> bound")
    unknown = sorted(set(values) - set(job_ids))
    missing = [j for j in job_ids if j not in values]
    if unknown:
        raise InvestigationInputError(
            f"positions {name} name unknown jobs {unknown}; configured jobs "
            f"are {list(job_ids)}")
    if missing:
        raise InvestigationInputError(
            f"positions {name} are missing jobs {missing}")
    out = {}
    for j in job_ids:
        v = values[j]
        if isinstance(v, bool) or not isinstance(v, int) or v < 0:
            raise InvestigationInputError(
                f"job {j!r}: {name}[{j!r}] must be an integer >= 0, got {v!r}")
        out[j] = v
    return out


def _check_pq(job_ids, p, q):
    bad = [j for j in job_ids if not 0 <= p[j] <= q[j]]
    if bad:
        raise InvestigationInputError(
            "positions must satisfy 0 <= P[job] <= Q[job]; violated for "
            + ", ".join(f"{j!r} (P={p[j]}, Q={q[j]})" for j in bad))


def _page_size(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_PAGE
    if isinstance(limit, bool) or not isinstance(limit, int) \
            or not 1 <= limit <= MAX_PAGE:
        raise InvestigationInputError(
            f"limit must be an integer in 1..{MAX_PAGE}, got {limit!r}")
    return limit


def _check_step(step) -> str:
    if step not in STEPS:
        raise InvestigationInputError(
            f"step must be 'P' or 'Q', got {step!r}")
    return step


# ------------------------------------------------------------------ lock
class _Lock:
    """Single-writer flock on a sidecar file.  A killed writer releases it
    through the kernel; no manual cleanup is ever needed."""

    def __init__(self, path: str):
        self.path = path
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        if self._fd is not None:
            return
        fd = os.open(self.path + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise InvestigationBusyError(
                f"investigation {self.path!r} is locked by another process; "
                f"only one build may publish") from None
        self._fd = fd

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def probe(self) -> bool:
        if self._fd is not None:
            return True
        fd = os.open(self.path + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        return False


# --------------------------------------------------------------- builder
class _Builder:
    """Builds one investigation in a temporary file; the caller publishes
    it atomically only after :meth:`seal` committed."""

    def __init__(self, path: str, job_ids: Sequence[str], warehouse_id: str,
                 warehouse_path: str, p: Dict[str, int], q: Dict[str, int]):
        self.path = path
        self.job_ids = list(job_ids)
        self.warehouse_id = warehouse_id
        self.warehouse_path = warehouse_path
        self.p = p
        self.q = q
        self.investigation_id = uuid.uuid4().hex
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-32000")
        self.conn.execute("PRAGMA temp_store=FILE")
        self._job_index = {j: i for i, j in enumerate(self.job_ids)}
        self._next_node = 0
        self._next_edge = 0
        self._cache: Dict[Tuple[str, str, str, str], int] = {}
        self._recent: List[Tuple[Tuple[str, str, str, str], int]] = []

    def close(self) -> None:
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    # ------------------------------------------------------ structure
    def create_tables(self) -> None:
        c = self.conn
        c.executescript(_TABLES_SCHEMA)
        c.executescript(_INDEXES_SCHEMA)
        c.executemany("INSERT INTO inv_jobs(idx, job_id) VALUES (?,?)",
                      list(enumerate(self.job_ids)))
        c.executemany(
            "INSERT INTO inv_positions(step, idx, job_id, bound_txt)"
            " VALUES (?,?,?,?)",
            [(step, i, j, str(values[j]))
             for step, values in (("P", self.p), ("Q", self.q))
             for i, j in enumerate(self.job_ids)])
        c.commit()

    def seal(self) -> None:
        c = self.conn
        # Deliberately no ANALYZE: its sqlite_stat1 rows make SQLite pick
        # a much slower execution shape for the BFS queries below (measured
        # ~400x on a 4000-node chain).  The indexes were created before the
        # data, so they stay well-filled without statistics.
        # The identity row is the last thing written.  A build killed at any
        # earlier point leaves a file without it, which cannot open; the
        # caller also deletes that temp file.
        c.execute(
            "INSERT INTO inv_meta(id, format_version, investigation_id,"
            " warehouse_id, warehouse_path, created_at) VALUES (1,?,?,?,?,?)",
            (INVESTIGATION_FORMAT_VERSION, self.investigation_id,
             self.warehouse_id, self.warehouse_path, _utcnow()))
        c.commit()

    # -------------------------------------------------------- reading
    def read_and_union(self, source: SourceWarehouse) -> None:
        """Read ``1..Q[job]`` once per job, store the evidence, create
        nodes, and union match edges in ascending offset order."""
        c = self.conn
        for job in self.job_ids:
            bound = self.q[job]
            if bound <= 0:
                continue
            expected = 1
            batch = []
            for raw in source.iter_results(job, bound):
                if raw[1] != expected:
                    raise InvestigationInputError(
                        f"job {job!r}: results offset {raw[1]} where "
                        f"{expected} was expected (non-contiguous warehouse)")
                expected += 1
                batch.append(raw)
                if len(batch) >= 2000:
                    self._process_batch(batch)
                    batch = []
            if batch:
                self._process_batch(batch)
            if expected != bound + 1:
                raise InvestigationInputError(
                    f"job {job!r}: read {expected - 1} results for bound "
                    f"{bound}; the warehouse changed under the snapshot")
            c.commit()

    def _process_batch(self, batch) -> None:
        c = self.conn
        parsed = []
        for raw in batch:
            (job, offset, kind, lp, ls, rp, rs, k, ltime, rtime) = raw
            parsed.append(make_edge(job, offset, kind, lp, ls, rp, rs, k,
                                    ltime, rtime))
        evidence = []
        for raw, edge in zip(batch, parsed):
            (job, offset, kind, lp, ls, rp, rs, k, ltime, rtime) = raw
            if edge is None:
                evidence.append((job, offset, kind, lp, ls, rp, rs, k, ltime,
                                 rtime, None, None))
            else:
                evidence.append((job, offset, kind, lp, ls, rp, rs, k, ltime,
                                 rtime,
                                 self._node_id(edge.left, offset),
                                 self._node_id(edge.right, offset)))
        c.executemany(
            "INSERT INTO inv_results(job_id, results_offset, kind, lp, ls, rp,"
            " rs, k, ltime, rtime, node_l, node_r)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", evidence)
        for raw, edge in zip(batch, parsed):
            if edge is not None:
                job, offset = edge.raw
                self._union(self._node_id(edge.left, offset),
                            self._node_id(edge.right, offset), offset)

    def _node_id(self, node: Node, offset: int) -> int:
        job, side, part, seq = node
        key = (job, side, part, str(seq))
        for k2, v2 in self._recent:
            if k2 == key:
                return v2
        nid = self._cache.get(key)
        if nid is None:
            row = self.conn.execute(
                "SELECT id FROM nodes WHERE job_id=? AND side=? AND"
                " partition=? AND seq_txt=?", key).fetchone()
            if row is not None:
                nid = int(row[0])
            else:
                self._next_node += 1
                nid = self._next_node
                self.conn.execute(
                    "INSERT INTO nodes(id, job_id, side, partition, seq_txt,"
                    " sort_job, sort_side, sort_part, enc, first_seen,"
                    " parent, rank) VALUES (?,?,?,?,?,?,?,?,?,?,?,0)",
                    (nid, job, side, part, str(seq), self._job_index[job],
                     0 if side == "L" else 1, part, enc_int(seq), offset, nid))
            if len(self._cache) >= NODE_CACHE:
                self._cache.clear()
            self._cache[key] = nid
        self._recent.append((key, nid))
        if len(self._recent) > 8:
            self._recent.pop(0)
        return nid

    def _union(self, a: int, b: int, offset: int) -> None:
        c = self.conn
        root_a = c.execute(_FIND, (a,)).fetchone()[0]
        root_b = c.execute(_FIND, (b,)).fetchone()[0]
        if root_a == root_b:
            return
        rows = c.execute("SELECT id, rank FROM nodes WHERE id IN (?,?)",
                         (root_a, root_b)).fetchall()
        ranks = {int(r[0]): int(r[1]) for r in rows}
        rank_a, rank_b = ranks[root_a], ranks[root_b]
        if rank_a < rank_b:
            sys_node, dst = root_b, root_a
        elif rank_b < rank_a:
            sys_node, dst = root_a, root_b
        else:
            sys_node, dst = root_a, root_b
            c.execute("UPDATE nodes SET rank=rank+1 WHERE id=?", (sys_node,))
        c.execute("UPDATE nodes SET parent=? WHERE id=?", (sys_node, dst))
        c.execute("INSERT INTO merge_tree(dst, sys, off) VALUES (?,?,?)",
                  (dst, sys_node, offset))

    # -------------------------------------------------------- finishing
    def _doubling(self, name: str) -> None:
        """Replace temp table ``name(node, cur)`` with its root pointers.

        One set-based substitution per pass (``cur`` moves to its current
        pointer's pointer); union by rank bounds the depth, so O(log n)
        passes suffice.  The loop stops as soon as every row is a root
        (``cur == node``), which is a single aggregate -- cheap even for
        large relations."""
        c = self.conn
        c.execute(f"CREATE INDEX IF NOT EXISTS temp.{name}_cur ON {name}(cur)")
        while True:
            # Done when every pointer target is itself a root.
            moving = c.execute(
                f"SELECT COUNT(*) FROM {name} p JOIN {name} q"
                f" ON q.node = p.cur WHERE q.cur <> q.node").fetchone()[0]
            if not moving:
                return
            c.execute("DROP TABLE IF EXISTS temp._nx")
            c.execute(f"CREATE TEMP TABLE _nx(node INTEGER PRIMARY KEY,"
                      " cur INTEGER NOT NULL)")
            c.execute(f"INSERT INTO _nx SELECT p.node, q.cur FROM {name} p"
                      f" JOIN {name} q ON q.node = p.cur")
            c.execute(f"DELETE FROM {name}")
            c.execute(f"INSERT INTO {name} SELECT node, cur FROM _nx")
            c.execute("DROP TABLE temp._nx")

    def finish(self) -> None:
        self._renumber()
        self._groups()
        self._changes()

    def _renumber(self) -> None:
        """Assign node ids in identity order (job text, L<R, partition
        text, numeric seq).  Text order in SQLite is UTF-8 byte order,
        which equals Unicode code-point order; ``enc`` orders seqs
        numerically.  Done before any index is published."""
        c = self.conn
        c.execute("DROP TABLE IF EXISTS temp.idmap")
        c.execute(
            "CREATE TEMP TABLE idmap AS SELECT id AS old, ROW_NUMBER() OVER"
            " (ORDER BY job_id, sort_side, sort_part, enc, id) AS new"
            " FROM nodes")
        c.execute("CREATE UNIQUE INDEX temp.idmap_old ON idmap(old)")
        c.execute(
            "CREATE TABLE nodes_new("
            " id INTEGER PRIMARY KEY, job_id TEXT NOT NULL, side TEXT NOT NULL,"
            " partition TEXT NOT NULL, seq_txt TEXT NOT NULL,"
            " sort_job INTEGER NOT NULL, sort_side INTEGER NOT NULL,"
            " sort_part TEXT NOT NULL, enc TEXT NOT NULL,"
            " first_seen INTEGER NOT NULL, parent INTEGER NOT NULL,"
            " rank INTEGER NOT NULL,"
            " UNIQUE(job_id, side, partition, seq_txt))")
        c.execute(
            "INSERT INTO nodes_new SELECT m.new, n.job_id, n.side,"
            " n.partition, n.seq_txt, n.sort_job, n.sort_side, n.sort_part,"
            " n.enc, n.first_seen, pm.new, n.rank"
            " FROM nodes n JOIN idmap m ON m.old = n.id"
            " JOIN idmap pm ON pm.old = n.parent")
        c.execute(
            "CREATE TABLE merge_tree_new("
            " dst INTEGER PRIMARY KEY, sys INTEGER NOT NULL,"
            " off INTEGER NOT NULL)")
        c.execute(
            "INSERT INTO merge_tree_new SELECT md.new, ms.new, t.off"
            " FROM merge_tree t JOIN idmap md ON md.old = t.dst"
            " JOIN idmap ms ON ms.old = t.sys")
        c.execute(
            "UPDATE inv_results SET"
            " node_l = (SELECT new FROM idmap WHERE old = node_l),"
            " node_r = (SELECT new FROM idmap WHERE old = node_r)")
        c.execute("DROP TABLE nodes")
        c.execute("ALTER TABLE nodes_new RENAME TO nodes")
        c.execute("DROP TABLE merge_tree")
        c.execute("ALTER TABLE merge_tree_new RENAME TO merge_tree")
        c.execute("DROP TABLE temp.idmap")
        c.commit()

    def _prefix_table(self, step: str, name: str) -> None:
        c = self.conn
        c.execute(f"DROP TABLE IF EXISTS temp.{name}")
        c.execute(f"CREATE TEMP TABLE {name}(job_id TEXT PRIMARY KEY,"
                  " bound INTEGER NOT NULL)")
        c.executemany(f"INSERT INTO {name} VALUES (?,?)",
                      list((("P", self.p), ("Q", self.q))[
                          0 if step == "P" else 1][1].items()))

    def _groups(self) -> None:
        c = self.conn
        # Q: every stored node is present; roots of the final forest.
        c.execute("CREATE TEMP TABLE qpar(node INTEGER PRIMARY KEY,"
                  " cur INTEGER NOT NULL)")
        c.execute("INSERT INTO qpar SELECT id, parent FROM nodes")
        self._doubling("qpar")
        # Declared keys (not `AS SELECT`): SQLite only uses a temp
        # table's index in a LEFT JOIN when its schema has one.
        c.execute("CREATE TEMP TABLE qgid(node INTEGER PRIMARY KEY,"
                  " gid INTEGER NOT NULL)")
        c.execute(
            "INSERT INTO qgid SELECT p.node, m.gid"
            " FROM qpar p JOIN (SELECT cur AS root, MIN(node) AS gid"
            " FROM qpar GROUP BY cur) m ON m.root = p.cur")
        c.execute("CREATE INDEX temp.qgid_gid ON qgid(gid, node)")
        # P: the union tree restricted to merges at offsets <= P[job].
        self._prefix_table("P", "pbound")
        c.execute("CREATE TEMP TABLE ppar(node INTEGER PRIMARY KEY,"
                  " cur INTEGER NOT NULL)")
        c.execute(
            "INSERT INTO ppar SELECT n.id, COALESCE(m.sys, n.id)"
            " FROM nodes n JOIN pbound b ON b.job_id = n.job_id"
            " LEFT JOIN merge_tree m ON m.dst = n.id AND m.off <= b.bound"
            " WHERE n.first_seen <= b.bound")
        self._doubling("ppar")
        c.execute("CREATE TEMP TABLE pgid(node INTEGER PRIMARY KEY,"
                  " gid INTEGER NOT NULL)")
        c.execute(
            "INSERT INTO pgid SELECT p.node, m.gid"
            " FROM ppar p JOIN (SELECT cur AS root, MIN(node) AS gid"
            " FROM ppar GROUP BY cur) m ON m.root = p.cur")
        c.execute("CREATE INDEX temp.pgid_gid ON pgid(gid, node)")

        # Set-based inserts: iterating a live cursor while writing on the
        # same connection would restart the SELECT for every row.
        c.execute("INSERT INTO node_hist(node_id, step, gid)"
                  " SELECT node, 'P', gid FROM pgid")
        c.execute("INSERT INTO node_hist(node_id, step, gid)"
                  " SELECT node, 'Q', gid FROM qgid")

        c.execute("CREATE TEMP TABLE _qn(gid INTEGER PRIMARY KEY,"
                  " c INTEGER NOT NULL)")
        c.execute("INSERT INTO _qn SELECT gid, COUNT(*) FROM qgid"
                  " GROUP BY gid")
        c.execute("INSERT INTO group_steps(step, gid, n_nodes, n_edges)"
                  " SELECT 'Q', gid, c, 0 FROM _qn")
        c.execute("CREATE TEMP TABLE _qe(gid INTEGER PRIMARY KEY,"
                  " c INTEGER NOT NULL)")
        c.execute("INSERT INTO _qe SELECT q.gid, COUNT(*)"
                  " FROM inv_results e JOIN qgid q ON q.node = e.node_l"
                  " WHERE e.kind = 'match' GROUP BY q.gid")
        c.execute("UPDATE group_steps SET n_edges ="
                  " COALESCE(_qe.c, 0) FROM _qe WHERE _qe.gid = group_steps.gid"
                  " AND group_steps.step = 'Q'")
        c.execute("CREATE TEMP TABLE _pn_pre(gid INTEGER PRIMARY KEY,"
                  " c INTEGER NOT NULL)")
        c.execute("INSERT INTO _pn_pre SELECT gid, COUNT(*) FROM pgid"
                  " GROUP BY gid")
        c.execute("INSERT INTO group_steps(step, gid, n_nodes, n_edges)"
                  " SELECT 'P', gid, c, 0 FROM _pn_pre")
        c.execute("CREATE TEMP TABLE _pe(gid INTEGER PRIMARY KEY,"
                  " c INTEGER NOT NULL)")
        c.execute("INSERT INTO _pe SELECT p.gid, COUNT(*)"
                  " FROM inv_results e JOIN pgid p ON p.node = e.node_l"
                  " JOIN pbound b ON b.job_id = e.job_id"
                  " WHERE e.kind = 'match' AND e.results_offset <= b.bound"
                  " GROUP BY p.gid")
        c.execute("UPDATE group_steps SET n_edges ="
                  " COALESCE(_pe.c, 0) FROM _pe WHERE _pe.gid = group_steps.gid"
                  " AND group_steps.step = 'P'")
        c.commit()

    def _changes(self) -> None:
        """Q group origin: merge (>=2 old groups), same (one old group) or
        new.  Counts of already-known nodes and edges come from the same
        prefix restriction, so a new node or edge inside one old group
        never looks like a merge."""
        c = self.conn
        c.execute(
            "INSERT INTO change_sources(gid, pgid)"
            " SELECT q.gid, p.gid FROM node_hist q JOIN node_hist p"
            " ON p.node_id = q.node_id AND p.step = 'P'"
            " WHERE q.step = 'Q' GROUP BY q.gid, p.gid")
        c.execute("CREATE TEMP TABLE _src(gid INTEGER PRIMARY KEY,"
                  " n INTEGER NOT NULL)")
        c.execute("INSERT INTO _src SELECT gid, COUNT(*)"
                  " FROM change_sources GROUP BY gid")
        c.execute("CREATE TEMP TABLE _pn2(gid INTEGER PRIMARY KEY,"
                  " c INTEGER NOT NULL)")
        c.execute("INSERT INTO _pn2 SELECT q.gid, COUNT(*)"
                  " FROM node_hist q JOIN node_hist p ON p.node_id = q.node_id"
                  " AND p.step = 'P' WHERE q.step = 'Q' GROUP BY q.gid")
        c.execute("CREATE TEMP TABLE _pe2(gid INTEGER PRIMARY KEY,"
                  " c INTEGER NOT NULL)")
        c.execute("INSERT INTO _pe2 SELECT q.gid, COUNT(*)"
                  " FROM inv_results e JOIN qgid q ON q.node = e.node_l"
                  " JOIN pbound b ON b.job_id = e.job_id"
                  " WHERE e.kind = 'match' AND e.results_offset <= b.bound"
                  " GROUP BY q.gid")
        c.execute(
            "INSERT INTO group_change(gid, origin, source_count,"
            " p_node_count, p_edge_count, added_nodes, added_edges)"
            " SELECT g.gid,"
            " CASE WHEN s.n IS NULL OR s.n = 0 THEN 'new'"
            "      WHEN s.n = 1 THEN 'same' ELSE 'merge' END,"
            " COALESCE(s.n, 0), COALESCE(pn.c, 0), COALESCE(pe.c, 0),"
            " g.n_nodes - COALESCE(pn.c, 0), g.n_edges - COALESCE(pe.c, 0)"
            " FROM group_steps g"
            " LEFT JOIN _src s ON s.gid = g.gid"
            " LEFT JOIN _pn2 pn ON pn.gid = g.gid"
            " LEFT JOIN _pe2 pe ON pe.gid = g.gid"
            " WHERE g.step = 'Q'")
        c.commit()


class ResponseInvestigation:
    """A built investigation, open read-only.  Queries never touch the
    source warehouse, so the investigation stays usable after the
    warehouse is moved away."""

    def __init__(self, path: str, conn: sqlite3.Connection):
        self.path = path
        self._conn = conn
        self._lock = _Lock(path)
        row = conn.execute(
            "SELECT investigation_id, warehouse_id, warehouse_path,"
            " created_at FROM inv_meta WHERE id=1").fetchone()
        self.investigation_id, self.warehouse_id = row[0], row[1]
        self.source_warehouse_path = row[2]
        self.created_at = row[3]
        self._jobs = [r[0] for r in conn.execute(
            "SELECT job_id FROM inv_jobs ORDER BY idx")]
        self._positions = {"P": {}, "Q": {}}
        for step, job, text in conn.execute(
                "SELECT step, job_id, bound_txt FROM inv_positions"
                " ORDER BY step, idx"):
            self._positions[step][job] = int(text, 10)
        # Access counters: every read made by the public query methods is
        # tallied here, so a drill can state the real rows touched rather
        # than infer them from timings.
        self._stats = {name: 0 for name in STAT_KEYS}
        self._stats_snapshot = dict(self._stats)
        # Cached canonical chain (one per pair/prefix) and its length.
        self._chain_key = None
        self._chain_len = 0

    # ----------------------------------------------------- access stats
    def stats(self, reset: bool = False) -> Dict[str, int]:
        """Rows actually read by this handle since it was opened (or
        since the last ``reset``), per query kind."""
        current = dict(self._stats)
        if reset:
            self._stats_snapshot = dict(current)
        return {k: current[k] - self._stats_snapshot[k] for k in current}

    def _count(self, name: str, n: int = 1) -> None:
        self._stats[name] += n

    # -------------------------------------------------------- lifecycle
    @classmethod
    def open(cls, path: str) -> "ResponseInvestigation":
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise InvestigationFormatError(
                f"no investigation at {path!r} (missing or empty file)")
        conn = _connect_ro(path)
        try:
            cls._validate(conn, path)
        except BaseException:
            conn.close()
            raise
        return cls(os.path.abspath(path), conn)

    @classmethod
    def build(cls, path: str, warehouse: str, jobs: Sequence[str],
              p: Dict[str, int], q: Dict[str, int],
              *, overwrite: bool = False) -> "ResponseInvestigation":
        """Build and atomically publish one investigation.

        The source warehouse is opened strictly read-only and read inside
        one snapshot.  Nothing is written to the warehouse, the job stores
        or any attribution mart.  On failure no investigation is left at
        ``path`` (an existing file is preserved unless ``overwrite``)."""
        job_ids = _validate_jobs(jobs)
        p = _check_positions(job_ids, p, "P")
        q = _check_positions(job_ids, q, "Q")
        _check_pq(job_ids, p, q)
        path = os.path.abspath(path)
        if os.path.exists(path) and not overwrite:
            raise InvestigationInputError(
                f"investigation {path!r} already exists; pass overwrite=True "
                f"to replace it")
        lock = _Lock(path)
        lock.acquire()
        builder = None
        source = None
        tmp = f"{path}.tmp-{uuid.uuid4().hex}"
        try:
            source = SourceWarehouse(warehouse)
            builder = _Builder(tmp, job_ids, source.warehouse_id,
                               source.path, p, q)
            builder.create_tables()
            _test_hook("build.after_tables")
            source.begin_snapshot()
            try:
                _verify_bounds(source, q)
                builder.read_and_union(source)
                _test_hook("build.snapshot_read")
            finally:
                source.end_snapshot()
            builder.finish()
            _test_hook("build.before_commit")
            builder.seal()
            builder.close()
            builder = None
            remove_db(path)          # drop any stale investigation + WAL
            os.replace(tmp, path)
            _test_hook("build.after_commit")
            return cls.open(path)
        except BaseException:
            if builder is not None:
                builder.close()
            remove_db(tmp)
            raise
        finally:
            if source is not None:
                source.close()
            lock.release()

    @staticmethod
    def _validate(conn, path: str) -> None:
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if "inv_meta" not in tables:
                raise InvestigationFormatError(
                    f"{path!r} is not a response investigation (tables: "
                    f"{sorted(tables)})")
            row = conn.execute(
                "SELECT format_version FROM inv_meta WHERE id=1").fetchone()
            if row is None:
                raise InvestigationFormatError(
                    f"{path!r}: investigation metadata row is missing")
            if row[0] != INVESTIGATION_FORMAT_VERSION:
                raise InvestigationFormatError(
                    f"{path!r}: unsupported investigation format version "
                    f"{row[0]!r} (this build understands "
                    f"{INVESTIGATION_FORMAT_VERSION})")
            missing = _REQUIRED_TABLES - tables
            if missing:
                raise InvestigationFormatError(
                    f"{path!r}: investigation is incomplete, missing tables: "
                    f"{sorted(missing)}")
        except sqlite3.DatabaseError as e:
            raise InvestigationFormatError(
                f"{path!r} is corrupt or not a SQLite database: {e}") from e

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._chain_key = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---------------------------------------------------------- context
    @property
    def job_ids(self) -> List[str]:
        return list(self._jobs)

    @property
    def positions(self) -> Dict[str, Dict[str, int]]:
        return {step: dict(values) for step, values in self._positions.items()}

    def status(self) -> Dict[str, Any]:
        c = self._conn
        counts = {}
        for table in ("nodes", "inv_results", "group_steps", "group_change",
                      "node_hist", "merge_tree"):
            counts[table] = int(c.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        counts["match_edges"] = int(c.execute(
            "SELECT COUNT(*) FROM inv_results WHERE kind='match'").fetchone()[0])
        return {"path": self.path,
                "investigation_id": self.investigation_id,
                "warehouse_id": self.warehouse_id,
                "warehouse_path": self.source_warehouse_path,
                "created_at": self.created_at,
                "jobs": self.job_ids,
                "positions": self.positions,
                "counts": counts,
                "locked": self._lock.probe()}

    # ---------------------------------------------------------- lookups
    def _node_row(self, job: str, partition: str, seq: int,
                  side: str = "L"):
        if side not in ("L", "R"):
            raise InvestigationInputError(
                f"side must be 'L' or 'R', got {side!r}")
        return self._conn.execute(
            "SELECT id, job_id, side, partition, seq_txt, first_seen"
            " FROM nodes WHERE job_id=? AND side=? AND partition=?"
            " AND seq_txt=?", (job, side, partition, str(seq))).fetchone()

    @staticmethod
    def _node_json(row) -> Dict[str, Any]:
        return {"node_id": int(row[0]), "job_id": row[1], "side": row[2],
                "partition": row[3], "seq": int(row[4], 10),
                "first_seen_offset": int(row[5])}

    def request_node(self, step: str, identity,
                     side: str = "L") -> Optional[Dict[str, Any]]:
        """A request (``side='L'``) or response (``side='R'``) as seen at
        ``step``: the node plus its group id there, or None when that
        observation does not contain it."""
        _check_step(step)
        job, part, seq = request_identity(identity)
        row = self._node_row(job, part, seq, side)
        if row is None:
            return None
        self._count("node_lookup")
        hist = self._conn.execute(
            "SELECT gid FROM node_hist WHERE node_id=? AND step=?",
            (row[0], step)).fetchone()
        if hist is None:
            return None
        out = self._node_json(row)
        out["step"] = step
        out["group_id"] = int(hist[0])
        return out

    def require_request_node(self, step: str, identity,
                             side: str = "L") -> Dict[str, Any]:
        job, part, seq = request_identity(identity)
        found = self.request_node(step, {"job_id": job, "partition": part,
                                        "seq": seq}, side)
        if found is None:
            raise InvestigationInputError(
                f"job {job!r} request {part!r}#{seq} is not a node at {step}; "
                f"only match rows create nodes, so a request seen only in "
                f"lpad/late records is not part of that observation")
        return found

    # ----------------------------------------------------------- groups
    def groups(self, step: str, *, after: Optional[int] = None,
               limit: Optional[int] = None) -> Dict[str, Any]:
        """Ordered group listing (ascending group id), keyset-paginated by
        ``after``.  Member lists are deliberately not included."""
        _check_step(step)
        limit = _page_size(limit)
        args: List[Any] = [step]
        sql = ("SELECT gid, n_nodes, n_edges FROM group_steps WHERE step=?")
        if after is not None:
            if isinstance(after, bool) or not isinstance(after, int):
                raise InvestigationInputError(
                    f"after must be a group id, got {after!r}")
            sql += " AND gid > ?"
            args.append(after)
        sql += " ORDER BY gid LIMIT ?"
        args.append(limit + 1)
        rows = self._conn.execute(sql, args).fetchall()
        more = len(rows) > limit
        rows = rows[:limit]
        changes = {}
        if step == "Q":
            ids = [int(r[0]) for r in rows]
            if ids:
                marks = ",".join("?" * len(ids))
                for row in self._conn.execute(
                        "SELECT gid, origin, source_count, added_nodes,"
                        " added_edges FROM group_change WHERE gid IN ("
                        + marks + ")", ids):
                    changes[int(row[0])] = row
        out = []
        for gid, n_nodes, n_edges in rows:
            group = {"group_id": int(gid), "step": step,
                     "group_node": self._node_json_by_id(gid),
                     "n_nodes": int(n_nodes), "n_edges": int(n_edges)}
            if step == "Q":
                row = changes[int(gid)]
                group["change"] = {
                    "origin": row[1], "source_count": int(row[2]),
                    "added_nodes": int(row[3]), "added_edges": int(row[4])}
            out.append(group)
        return {"step": step, "groups": out, "has_more": more,
                "next_after": None if not rows else int(rows[-1][0])}

    def group(self, step: str, gid: int) -> Dict[str, Any]:
        row = self._conn.execute(
            "SELECT n_nodes, n_edges FROM group_steps WHERE step=? AND gid=?",
            (_check_step(step), self._as_gid(gid))).fetchone()
        if row is None:
            raise InvestigationInputError(
                f"group {gid!r} does not exist at {step}")
        out = {"group_id": int(gid), "step": step,
               "group_node": self._node_json_by_id(gid),
               "n_nodes": int(row[0]), "n_edges": int(row[1])}
        if step == "Q":
            change = self.group_change(gid)
            out["change"] = {k: change[k] for k in
                             ("origin", "source_count", "added_nodes",
                              "added_edges")}
        return out

    def group_change(self, gid: int) -> Dict[str, Any]:
        """How Q group ``gid`` relates to P.

        ``merge``  -- the Q group contains two or more distinct P groups;
        ``same``   -- it grew from exactly one P group (``added_nodes`` /
                      ``added_edges`` say whether anything is new);
        ``new``    -- no P node of that group existed.
        A new node or an edge inside one old group never counts as a merge.
        """
        row = self._conn.execute(
            "SELECT origin, source_count, p_node_count, p_edge_count,"
            " added_nodes, added_edges FROM group_change WHERE gid=?",
            (self._as_gid(gid),)).fetchone()
        if row is None:
            raise InvestigationInputError(
                f"group {gid!r} is not a Q group of this investigation; use "
                f"groups('Q') to list them")
        return {"group_id": int(gid), "origin": row[0],
                "source_count": int(row[1]),
                "p_node_count": int(row[2]), "p_edge_count": int(row[3]),
                "added_nodes": int(row[4]), "added_edges": int(row[5])}

    def change_sources(self, gid: int, *, after: Optional[int] = None,
                       limit: Optional[int] = None) -> Dict[str, Any]:
        """Page the P group ids a Q group grew from (ascending, by group
        id).  A merge can contain very many old groups, so the ids are
        deliberately not part of the summary."""
        gid = self._as_gid(gid)
        limit = _page_size(limit)
        if self._conn.execute("SELECT 1 FROM group_change WHERE gid=?",
                              (gid,)).fetchone() is None:
            raise InvestigationInputError(
                f"group {gid!r} is not a Q group of this investigation")
        args: List[Any] = [gid]
        sql = "SELECT pgid FROM change_sources WHERE gid=?"
        if after is not None:
            if isinstance(after, bool) or not isinstance(after, int):
                raise InvestigationInputError(
                    f"after must be a group id, got {after!r}")
            sql += " AND pgid > ?"
            args.append(after)
        sql += " ORDER BY pgid LIMIT ?"
        args.append(limit + 1)
        rows = self._conn.execute(sql, args).fetchall()
        more = len(rows) > limit
        rows = rows[:limit]
        self._count("source_rows", len(rows) + (1 if more else 0))
        return {"group_id": gid,
                "source_group_ids": [int(r[0]) for r in rows],
                "has_more": more,
                "next_after": None if not rows else int(rows[-1][0])}

    def group_members(self, step: str, gid: int, *,
                      after: Optional[int] = None,
                      limit: Optional[int] = None) -> Dict[str, Any]:
        """Page one group's nodes in node-id order (= identity order)."""
        _check_step(step)
        gid = self._as_gid(gid)
        limit = _page_size(limit)
        args: List[Any] = [step, gid]
        sql = ("SELECT n.id, n.job_id, n.side, n.partition, n.seq_txt,"
               " n.first_seen FROM node_hist h JOIN nodes n"
               " ON n.id = h.node_id WHERE h.step=? AND h.gid=?")
        if after is not None:
            if isinstance(after, bool) or not isinstance(after, int):
                raise InvestigationInputError(
                    f"after must be a node id, got {after!r}")
            sql += " AND n.id > ?"
            args.append(after)
        sql += " ORDER BY n.id LIMIT ?"
        args.append(limit + 1)
        rows = self._conn.execute(sql, args).fetchall()
        more = len(rows) > limit
        rows = rows[:limit]
        self._count("member_rows", len(rows) + (1 if more else 0))
        return {"step": step, "group_id": gid,
                "members": [self._node_json(r) for r in rows],
                "has_more": more,
                "next_after": None if not rows else int(rows[-1][0])}

    def group_edges(self, step: str, gid: int, *,
                    after: Optional[Sequence] = None,
                    limit: Optional[int] = None) -> Dict[str, Any]:
        """Page one group's match edges ordered by (job text, numeric
        offset).  Each edge is listed once, in its request's group."""
        _check_step(step)
        gid = self._as_gid(gid)
        limit = _page_size(limit)
        args: List[Any] = [step, gid]
        sql = ("SELECT e.job_id, e.results_offset, e.k, e.ltime, e.rtime,"
               " e.lp, e.ls, e.rp, e.rs"
               " FROM inv_results e JOIN node_hist h"
               "   ON h.node_id = e.node_l AND h.step = ?"
               " WHERE e.kind = 'match' AND h.gid = ?")
        if step == "P":
            sql += (" AND EXISTS (SELECT 1 FROM inv_positions v WHERE"
                    " v.step='P' AND v.job_id=e.job_id"
                    " AND e.results_offset <= CAST(v.bound_txt AS INTEGER))")
        if after is not None:
            if isinstance(after, (str, bytes)) or len(after) != 2:
                raise InvestigationInputError(
                    f"after must be (job_id, offset), got {after!r}")
            job0, off0 = after
            sql += (" AND (e.job_id > ? OR (e.job_id = ? AND"
                    " e.results_offset > ?))")
            args.extend([job0, job0, int(off0)])
        sql += " ORDER BY e.job_id, e.results_offset LIMIT ?"
        args.append(limit + 1)
        rows = self._conn.execute(sql, args).fetchall()
        more = len(rows) > limit
        rows = rows[:limit]
        self._count("edge_rows", len(rows) + (1 if more else 0))
        edges = [{
            "job_id": r[0], "results_offset": int(r[1]), "kind": "match",
            "device_key": r[2],
            "left": {"partition": r[5], "seq": int(r[6], 10)},
            "right": {"partition": r[7], "seq": int(r[8], 10)},
            "left_time": None if r[3] is None else int(r[3], 10),
            "right_time": None if r[4] is None else int(r[4], 10),
        } for r in rows]
        return {"step": step, "group_id": gid, "edges": edges,
                "has_more": more,
                "next_after": None if not rows else [rows[-1][0],
                                                     int(rows[-1][1])]}

    def group_edge_count(self, step: str, gid: int) -> int:
        return self.group(step, gid)["n_edges"]

    @staticmethod
    def _as_gid(gid) -> int:
        if isinstance(gid, bool) or not isinstance(gid, int):
            raise InvestigationInputError(
                f"group id must be an integer, got {gid!r}")
        return gid



    # ----------------------------------------------- first connection
    def _gid_at(self, step, node_id):
        row = self._conn.execute(
            "SELECT gid FROM node_hist WHERE node_id=? AND step=?",
            (node_id, step)).fetchone()
        return None if row is None else int(row[0])

    def _ancestors(self, node: int) -> List[Tuple[int, Optional[int]]]:
        """The root-ward merge path of ``node``: pairs ``(member,
        offset_of_the_merge_that_attached_it)``, the root's offset being
        None.  Offsets strictly increase root-ward and the path is O(log n)
        long (union by rank).  Edges are read one SELECT at a time, so the
        path is never materialised beyond the Python list of its own
        O(log n) members."""
        path: List[Tuple[int, Optional[int]]] = [(node, None)]
        cur = node
        while True:
            row = self._conn.execute(
                "SELECT sys, off FROM merge_tree WHERE dst=?",
                (cur,)).fetchone()
            if row is None:
                return path
            parent = self._conn.execute(
                "SELECT parent FROM nodes WHERE id=?", (cur,)).fetchone()[0]
            if parent == cur:
                return path
            path[-1] = (cur, int(row[1]))
            path.append((int(row[0]), None))
            cur = int(row[0])
            self._count("ancestor_steps")

    def _first_offset(self, node_a: int, node_b: int) -> Optional[int]:
        """Earliest offset at which ``node_a`` and ``node_b`` were in one
        component.

        The merge tree labels every node with the offset of the edge that
        attached it under its parent; labels strictly increase root-ward,
        because edges are unioned in ascending offset order.  Find the
        lowest common ancestor ``L``; the merge that first joined the two
        sides is the one that attached the child of ``L`` on either side,
        so the first common offset is the larger of those two labels
        (both labels when neither node is ``L``, the single child label
        when one node is ``L``, and 0 for the same request).  Every merge
        strictly below ``L`` happened earlier and did not connect the two
        sides; nothing above ``L`` can connect them earlier.  Later edges
        are simply not consulted.
        """
        path_a = self._ancestors(node_a)
        path_b = self._ancestors(node_b)
        pos_b = {}
        for i, (member, _off) in enumerate(path_b):
            pos_b[member] = i
        for i, (member, off) in enumerate(path_a):
            j = pos_b.get(member)
            if j is None:
                continue
            if i == 0 and j == 0:
                return 0
            labels = []
            if i > 0:
                labels.append(path_a[i - 1][1])
            if j > 0:
                labels.append(path_b[j - 1][1])
            labels = [int(v) for v in labels if v is not None]
            return max(labels) if labels else 0
        raise InvestigationError(
            f"internal inconsistency: nodes {node_a} and {node_b} are in the "
            f"same group but their merge paths do not meet")

    def resolve(self, step: str, a, b, *, chain_limit: Optional[int] = 200
                ) -> Dict[str, Any]:
        """When and how two requests first became connected.

        ``status``:

        * ``connected``      -- ``first_connected_offset`` and ``chain``;
        * ``disconnected``   -- both seen at Q, different components;
        * ``not_seen``       -- a request has no node (lpad/late/absent);
        * ``different_jobs`` -- edges never cross jobs, so no chain.

        ``connected_at_P`` says whether the two requests already shared a
        group at P.  The offset comes from the stored merge tree; it is
        never derived by scanning newer edges.  ``chain`` holds at most
        ``chain_limit`` hops (``chain_truncated`` says whether more
        exist); :meth:`chain_page` pages the full chain without holding it
        in memory."""
        _check_step(step)
        ja, pa, sa = request_identity(a)
        jb, pb, sb = request_identity(b)
        row_a = self._node_row(ja, pa, sa)
        row_b = self._node_row(jb, pb, sb)
        node_a = None if row_a is None else int(row_a[0])
        node_b = None if row_b is None else int(row_b[0])
        lit_a = {"job_id": ja, "partition": pa, "seq": sa}
        lit_b = {"job_id": jb, "partition": pb, "seq": sb}
        out = {"step": step, "request_a": lit_a, "request_b": lit_b,
               "first_connected_offset": None, "connected_at_P": False,
               "hop_count": None, "status": None, "reason": None,
               "chain": []}
        if ja != jb:
            out["status"] = "different_jobs"
            out["reason"] = ("the requests belong to different jobs; a "
                             "match edge never crosses jobs, so they can "
                             "never be connected")
            missing = []
            if node_a is None:
                missing.append(lit_a)
            if node_b is None:
                missing.append(lit_b)
            if missing:
                out["missing"] = missing
            return out
        missing = []
        if node_a is None:
            missing.append(lit_a)
        if node_b is None:
            missing.append(lit_b)
        if missing:
            out["status"] = "not_seen"
            out["missing"] = missing
            out["reason"] = (
                "not seen in any match row: "
                + ", ".join(f"job {m['job_id']!r} request "
                            f"{m['partition']!r}#{m['seq']}" for m in missing)
                + " (lpad/late rows never create a node)")
            return out
        gid_a = self._gid_at(step, node_a)
        gid_b = self._gid_at(step, node_b)
        if gid_a is None or gid_b is None:
            unseen = [lit_a] if gid_a is None else []
            if gid_b is None:
                unseen.append(lit_b)
            out["status"] = "not_seen"
            out["missing"] = unseen
            out["reason"] = (
                "not seen at " + step + ": "
                + ", ".join(f"job {m['job_id']!r} request "
                            f"{m['partition']!r}#{m['seq']}" for m in unseen)
                + f" (its first match row lies after the {step} bound)")
            return out
        gid_p_a = self._gid_at("P", node_a)
        gid_p_b = self._gid_at("P", node_b)
        out["connected_at_P"] = bool(gid_p_a is not None and
                                     gid_p_b is not None and gid_p_a == gid_p_b)
        if gid_a != gid_b:
            out["status"] = "disconnected"
            out["reason"] = ("both requests are seen at Q but lie in "
                             "different components; no chain exists")
            return out
        k = self._first_offset(node_a, node_b)
        out["status"] = "connected"
        out["first_connected_offset"] = int(k)
        limit = 200 if chain_limit is None else chain_limit
        total = self._chain_total(node_a, node_b, k)
        chain = []
        for i in range(0, min(total, limit) + 1):
            if i == 0:
                chain.append({"node": self._node_json_by_id(node_a),
                              "edge": None, "from_node_id": None,
                              "step_index": 0})
                continue
            nxt, job, off = self._chain_step_cached(i)
            chain.append({"node": self._node_json_by_id(nxt),
                          "edge": {"job_id": job,
                                   "results_offset": off},
                          "from_node_id": (node_a if i == 1
                                           else self._chain_step_cached(
                                               i - 1)[0]),
                          "step_index": i})
        # The chain table is kept so `chain_page` can resume from any hop;
        # the next different pair/prefix replaces it.
        out["hop_count"] = total
        out["chain"] = chain
        out["chain_truncated"] = len(chain) - 1 < total
        out["chain_limit"] = limit
        return out

    def chain_page(self, step: str, a, b, *, after: Optional[int] = None,
                   limit: Optional[int] = None) -> Dict[str, Any]:
        """Page the canonical shortest chain of a connected pair.

        ``after`` is the ``step_index`` of the last hop already delivered
        (``None`` starts at the request node, whose ``step_index`` is 0).
        ``next_after`` is the value to pass to continue.  A page carries at
        most ``limit`` items, always including the request node when
        ``after`` is 0, so ``limit`` must be >= 1.  The canonical chain is computed once per
        pair/prefix into a SQLite temp table, so later pages only read the
        hops they need; Python holds one page, never the whole path."""
        _check_step(step)
        limit = 64 if limit is None else _page_size(limit)
        info = self.resolve(step, a, b, chain_limit=0)
        if info["status"] != "connected":
            raise InvestigationInputError(
                f"the two requests are not connected at {step}: "
                f"{info['status']} ({info['reason']})")
        after = 0 if after is None else after
        if isinstance(after, bool) or not isinstance(after, int) or after < 0:
            raise InvestigationInputError(
                f"after must be a hop index >= 0, got {after!r}")
        node_a = self._node_row(*request_identity(a))[0]
        node_b = self._node_row(*request_identity(b))[0]
        k = info["first_connected_offset"]
        total = self._chain_total(node_a, node_b, k)
        hops = []
        last = None
        if after == 0:
            # The request node is item 0.  If the page is too small to
            # carry it plus a hop, still make progress by starting the
            # page at hop 1 (the node 0 item is available from any
            # page-0 call).
            if limit >= 2:
                hops.append({"node": self._node_json_by_id(node_a),
                             "edge": None, "from_node_id": None,
                             "step_index": 0})
                last = 0
                cur = node_a
            else:
                cur = node_a
        else:
            cur = self._chain_step_cached(after)[0]
        for i in range(after + 1, total + 1):
            if len(hops) >= limit:
                break
            nxt, job, off = self._chain_step_cached(i)
            hops.append({"node": self._node_json_by_id(nxt),
                         "edge": {"job_id": job, "results_offset": off},
                         "from_node_id": cur, "step_index": i})
            cur = nxt
            last = i
        return {"step": step, "first_connected_offset": k,
                "hop_count": total, "hops": hops,
                "has_more": last is not None and last < total,
                # Continue after the last item of this page.
                "next_after": (None if last is None or last >= total
                               else last)}

    def _chain_total(self, node_a: int, node_b: int, k: int) -> int:
        """Build (once per pair/prefix) the canonical chain into a temp
        table ``_chain(step_index, node, job, off)`` and return its hop
        count.  Each page then reads the table instead of re-searching."""
        key = (node_a, node_b, k)
        if getattr(self, "_chain_key", None) == key:
            return self._chain_len
        self._chain_key = None
        self._drop_chain_tables()
        total = self._chain_tables(node_a, node_b, k)
        c = self._conn
        c.execute("DROP TABLE IF EXISTS temp._chain")
        c.execute("CREATE TEMP TABLE _chain(step_index INTEGER PRIMARY KEY,"
                  " node INTEGER NOT NULL, job TEXT, off INTEGER)")
        cur = node_a
        rows = []
        for i in range(1, total + 1):
            step = self._next_step(cur, total, k)
            if step is None:
                self._drop_chain_tables()
                raise InvestigationError(
                    "internal inconsistency: shortest chain has a dead end")
            nxt, job, off = int(step[0]), step[1], int(step[2])
            rows.append((i, nxt, job, off))
            cur = nxt
        c.executemany("INSERT INTO _chain VALUES (?,?,?,?)", rows)
        self._drop_chain_tables()
        self._chain_key = key
        self._chain_len = total
        return total

    def _chain_step_cached(self, index: int):
        row = self._conn.execute(
            "SELECT node, job, off FROM _chain WHERE step_index=?",
            (index,)).fetchone()
        if row is None:
            raise InvestigationError(
                "internal inconsistency: cached chain has no such hop")
        return int(row[0]), row[1], int(row[2])

    # -------------------------------------------------------- the chain
    def _distance_table(self, name: str, source: int, k: int,
                        prefix_bound, cap: int) -> None:
        """Build ``temp.<name>(node, dist)`` with BFS distances from
        ``source`` over match edges of offset ``<= k`` and
        ``<= prefix_bound``.  Every layer lives in SQLite; Python issues
        only set-based statements, so memory does not grow with the
        component or the frontier."""
        c = self._conn
        for table in (name, "_front", "_next"):
            c.execute(f"DROP TABLE IF EXISTS temp.{table}")
        c.execute(f"CREATE TEMP TABLE {name}(node INTEGER PRIMARY KEY,"
                  " dist INTEGER NOT NULL)")
        c.execute("CREATE TEMP TABLE _front(node INTEGER PRIMARY KEY)")
        c.execute("CREATE TEMP TABLE _next(node INTEGER PRIMARY KEY)")
        c.execute(f"INSERT INTO {name} VALUES (?, 0)", (source,))
        c.execute("INSERT INTO _front VALUES (?)", (source,))
        hi = int(k) if prefix_bound is None else min(int(k), int(prefix_bound))
        depth = 0
        total = 1
        # One reusable frontier table (never dropped inside the loop: a
        # temp-table create/drop per layer dominates a long chain) plus
        # exactly two indexed INSERTs per layer.
        while True:
            depth += 1
            # Two statements, not one UNION ALL: SQLite plans each arm
            # with its own (node, offset) index when they are separate,
            # while a single compound INSERT can degrade into a repeated
            # full scan of the edge table.  CROSS JOIN pins the join order
            # for the same reason.
            c.execute("DELETE FROM _next")
            c.execute(
                "INSERT OR IGNORE INTO _next(node) SELECT e.node_r"
                " FROM _front f CROSS JOIN inv_results e"
                "   ON e.node_l = f.node"
                " WHERE e.kind = 'match' AND e.results_offset <= ?"
                f" AND e.node_r NOT IN (SELECT node FROM {name})", (hi,))
            c.execute(
                "INSERT OR IGNORE INTO _next(node) SELECT e.node_l"
                " FROM _front f CROSS JOIN inv_results e"
                "   ON e.node_r = f.node"
                " WHERE e.kind = 'match' AND e.results_offset <= ?"
                f" AND e.node_l NOT IN (SELECT node FROM {name})", (hi,))
            added = c.execute("SELECT COUNT(*) FROM _next").fetchone()[0]
            if not added:
                break
            total += added
            self._count("bfs_nodes", added)
            c.execute(f"INSERT INTO {name} SELECT node, ? FROM _next",
                      (depth,))
            c.execute("DELETE FROM _front")
            c.execute("INSERT INTO _front SELECT node FROM _next")
            if total > cap:
                raise InvestigationInputError(
                    f"chain search exceeded {cap} visited nodes; refusing to "
                    f"grow an unbounded frontier")

    def _next_step(self, node: int, total: int, k: int):
        """The next hop of the canonical shortest chain: among neighbours
        on a shortest path (``fwd + bwd == total``) and exactly one layer
        deeper, take the smallest (next node id, job text, numeric
        offset).  Returns ``(next_node, job, offset)`` or None."""
        return self._conn.execute(
            "SELECT x.b, x.job_id, x.results_offset FROM ("
            " SELECT e.node_l AS a, e.node_r AS b, e.job_id,"
            "        e.results_offset FROM inv_results e"
            "  WHERE e.node_l = ? AND e.kind='match'"
            "    AND e.results_offset <= ?"
            " UNION ALL"
            " SELECT e.node_r, e.node_l, e.job_id, e.results_offset"
            "  FROM inv_results e WHERE e.node_r = ? AND e.kind='match'"
            "    AND e.results_offset <= ?"
            ") x JOIN _fwd f ON f.node = x.b JOIN _bwd b ON b.node = x.b"
            " WHERE f.dist = (SELECT dist FROM _fwd WHERE node = ?) + 1"
            "   AND b.dist = (SELECT dist FROM _bwd WHERE node = ?) - 1"
            "   AND f.dist + b.dist = ?"
            " ORDER BY x.b, x.job_id, x.results_offset LIMIT 1",
            (node, k, node, k, node, node, total)).fetchone()

    def _chain_tables(self, node_a: int, node_b: int, k: int,
                      cap: int = 2_000_000) -> int:
        """Build the forward/backward distance layers once; return the
        number of hops."""
        self._distance_table("_fwd", node_a, k, None, cap)
        self._distance_table("_bwd", node_b, k, None, cap)
        row = self._conn.execute(
            "SELECT dist FROM _fwd WHERE node=?", (node_b,)).fetchone()
        if row is None:
            self._drop_chain_tables()
            raise InvestigationError(
                "internal inconsistency: no chain inside a connected prefix")
        total = int(row[0])
        back = self._conn.execute(
            "SELECT dist FROM _bwd WHERE node=?", (node_a,)).fetchone()
        if back is None or int(back[0]) != total:
            self._drop_chain_tables()
            raise InvestigationError(
                "internal inconsistency: forward and backward distances "
                "disagree")
        return total

    def _drop_chain_tables(self) -> None:
        for table in ("_fwd", "_bwd", "_front", "_next"):
            self._conn.execute(f"DROP TABLE IF EXISTS temp.{table}")
        self._chain_key = None

    def _node_json_by_id(self, node_id: int) -> Dict[str, Any]:
        row = self._conn.execute(
            "SELECT id, job_id, side, partition, seq_txt, first_seen"
            " FROM nodes WHERE id=?", (node_id,)).fetchone()
        return self._node_json(row)


# ------------------------------------------------------- module helpers
def _connect_ro(path: str) -> sqlite3.Connection:
    uri = "file:" + quote(os.path.abspath(path)) + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        conn.execute("PRAGMA busy_timeout=5000")
    except sqlite3.Error as e:
        raise InvestigationFormatError(
            f"cannot open investigation {path!r}: {e}") from e
    return conn


def _verify_bounds(source: SourceWarehouse, q: Dict[str, int]) -> None:
    """Every requested position must be explained by the snapshot.

    For each job the tool checks that ``1..Q[job]`` exists contiguously.
    A position beyond the warehouse's own maximum offset, a hole, an
    unknown result kind or an uninterpretable match row raises an error
    naming the job and the position; the build fails and no investigation
    file survives."""
    for job, bound in q.items():
        top = source.max_offset(job)
        if bound > top:
            raise InvestigationInputError(
                f"job {job!r}: requested Q={bound} but the warehouse holds "
                f"results only up to offset {top} (out of range)")
        source.verify_contiguous(job, bound)
