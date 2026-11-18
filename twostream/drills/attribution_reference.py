"""Independent reference implementation for the attribution mart.

This module deliberately shares nothing with the product's grouping,
attribution lookup, retraction or quantile code.  It reads the warehouse
directly (read-only), owns a straightforward in-memory interval list and
computes each group's numbers from a complete, sorted latency list.  It is
only used by the acceptance drills and unit tests -- never by the mart.

Numbers checked against it:
  * match pairs, distinct matched requests, lpad / rpad counts
  * latency sum / min / max and nearest-rank p50 / p95
  * the expansion identity of every fact (job/offset, both sides)

Because it keeps a dense array and a per-fact list, it is meant for the
controlled acceptance data sets, not for the 200k-scale performance run
(there the drill compares mart numbers against incremental invariants
instead of materialising everything).
"""

from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def floor_window(t: int, w: int, o: int) -> int:
    """O + floor((t-O)/W)*W computed with divmod, independently of the
    product helper."""
    q, r = divmod(t - o, w)       # Python divmod floors for w > 0
    return o + q * w


def nearest_rank(values: Sequence[int], num: int, den: int) -> Optional[int]:
    if not values:
        return None
    rank = -((-(num * len(values))) // den)   # ceil(num*n/den) without +den-1
    return values[rank - 1]


class IntervalList:
    """Plain sorted list of (start, end, owner); start/end None = infinite."""

    def __init__(self, segments: Iterable[dict]):
        self.segs = []
        for s in segments:
            start = s.get("start")
            end = s.get("end")
            owner = s["owner"]
            self.segs.append((start, end, owner))
        # sort by -infinity first; only '-'/'+' sentinels, never compare
        # differently typed bounds
        self.segs.sort(key=lambda x: (x[0] is not None, x[0] or 0))

    def covering(self, t: int):
        for start, end, owner in self.segs:
            if (start is None or start <= t) and (end is None or t < end):
                return (start, end, owner)
        return None


def reference_groups(warehouse: str, job_ids: Sequence[str],
                     histories: Dict[str, List[dict]], width: int,
                     origin: int) -> Tuple[dict, List[dict]]:
    """Compute the full expected statistics and fact list.

    Returns ``(groups, facts)`` where ``groups[(window, owner)]`` is a dict
    with match_pairs / matched_requests / unmatched_requests /
    unmatched_responses / latency_sum / latency_min / latency_max / p50 /
    p95, and ``facts`` is a list of ``(window, owner, fact_identity)`` in
    warehouse (job, offset) order.  ``owner is None`` is the unknown group.
    ``histories`` maps device -> raw segment dicts.
    """
    interval_lists = {d: IntervalList(h) for d, h in histories.items()}
    conn = sqlite3.connect("file:" + warehouse + "?mode=ro", uri=True)
    conn.execute("BEGIN")
    try:
        facts = []
        lat_by_group: Dict[tuple, List[int]] = {}
        count = {}
        requests = set()
        for jid in job_ids:
            rows = conn.execute(
                "SELECT offset, kind, lp, ls, rp, rs, k, ltime, rtime"
                " FROM wh_results WHERE job_id=? ORDER BY offset",
                (jid,)).fetchall()
            for offset, kind, lp, ls, rp, rs, k, ltime, rtime in rows:
                if kind == "match":
                    anchor, lat = int(ltime), int(rtime) - int(ltime)
                elif kind == "lpad":
                    anchor, lat = int(ltime), None
                else:
                    anchor, lat = int(rtime), None
                window = floor_window(anchor, width, origin)
                owner = None
                seg = None
                if k is not None:
                    il = interval_lists.get(k)
                    if il is not None:
                        seg = il.covering(anchor)
                        if seg is not None:
                            owner = seg[2]
                key = (window, owner)
                c = count.setdefault(key, [0, 0, 0, 0])  # match,req,lpad,rpad
                if kind == "match":
                    c[0] += 1
                elif kind == "lpad":
                    c[2] += 1
                else:
                    c[3] += 1
                if kind == "match":
                    requests.add((jid, lp, ls))
                    lat_list = lat_by_group.setdefault(key, [])
                    lat_list.append(lat)
                facts.append({
                    "job_id": jid, "offset": offset, "kind": kind,
                    "window_start": window, "owner": owner,
                    "device_key": k, "anchor_time": anchor,
                    "left": None if lp is None else (lp, int(ls)),
                    "right": None if rp is None else (rp, int(rs)),
                    "left_time": None if ltime is None else int(ltime),
                    "right_time": None if rtime is None else int(rtime),
                    "segment": seg,
                    "latency": lat,
                })
        # distinct requests per group: a request's group is that of its
        # first (lowest-offset) match, exactly like a set of identities
        # grouped by their match facts; recompute from facts.
        req_group: Dict[tuple, tuple] = {}
        for f in facts:
            if f["kind"] != "match":
                continue
            ident = (f["job_id"], f["left"][0], f["left"][1])
            req_group.setdefault(ident, (f["window_start"], f["owner"]))
        for key in count:
            count[key][1] = 0
        for key in req_group.values():
            count[key][1] += 1

        groups = {}
        for key, (n_match, n_req, n_lpad, n_rpad) in count.items():
            if not (n_match or n_req or n_lpad or n_rpad):
                continue
            lat = sorted(lat_by_group.get(key, []))
            groups[key] = {
                "match_pairs": n_match,
                "matched_requests": n_req,
                "unmatched_requests": n_lpad,
                "unmatched_responses": n_rpad,
                "latency_sum": sum(lat),
                "latency_min": lat[0] if lat else None,
                "latency_max": lat[-1] if lat else None,
                "p50": nearest_rank(lat, 1, 2),
                "p95": nearest_rank(lat, 19, 20),
            }
        return groups, facts
    finally:
        conn.execute("COMMIT")
        conn.close()


def facts_for_group(facts: List[dict], window: int,
                    owner: Optional[str]) -> List[dict]:
    return [f for f in facts if f["window_start"] == window
            and f["owner"] == owner]
