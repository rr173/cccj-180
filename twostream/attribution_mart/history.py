"""Attribution histories: [start, end) intervals over arbitrary-precision
integers, with a *distinct* "unknown" result.

Design points that the rest of the mart relies on:

* An interval is half-open: ``start <= t < end``.  ``None`` means -inf
  (start) or +inf (end).  An empty interval (``start >= end``) is illegal.
* Segments may touch (``next.start == prev.end``) and gaps are allowed,
  but they may never overlap.  Input order does not matter: histories are
  sorted by start, with -inf first.
* An owner is a non-empty string.  The literal text ``"unknown"`` is an
  ordinary owner and is never confused with the *unknown group*.
* The unknown group is returned as ``None`` from :meth:`TimeMap.owner_at`.
  It is produced when no segment covers the time; a null *fact key* also
  yields the unknown group, but that decision is made by the caller
  (a null key is not something an attribution history can cover).
* ``changed_intervals`` compares two histories at exactly the integer
  points where the effective owner can change, so a correction only has
  to visit stored facts in the ranges that really changed.  All bounds
  and samples stay Python integers (arbitrary precision); nothing here
  ever converts a time to a float or to a SQLite 64-bit integer.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

from .errors import AttributionInputError


@dataclass(frozen=True)
class Segment:
    start: Optional[int]   # None = -infinity
    end: Optional[int]     # None = +infinity
    owner: str

    @property
    def val(self) -> tuple:
        return (self.start, self.end, self.owner)


def _check_int(v, what: str) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise AttributionInputError(f"{what} must be an integer or null,"
                                    f" got {v!r}")
    return v


def parse_segments(raw: Iterable) -> Tuple[Segment, ...]:
    """Validate and sort one device's complete history.

    Accepts an iterable of dicts ``{"start": int|None, "end": int|None,
    "owner": str}`` (or :class:`Segment`).  Raises AttributionInputError
    for any illegal interval, overlap, inverted or empty bound.
    """
    segments: List[Segment] = []
    for i, item in enumerate(raw):
        where = f"segment #{i}"
        if isinstance(item, Segment):
            start, end, owner = item.start, item.end, item.owner
        elif isinstance(item, dict):
            extra = set(item) - {"start", "end", "owner"}
            if extra:
                raise AttributionInputError(
                    f"{where} has unknown fields {sorted(extra)}")
            try:
                start, end, owner = item["start"], item["end"], item["owner"]
            except KeyError as e:
                raise AttributionInputError(
                    f"{where} is missing field {e.args[0]!r}") from e
        else:
            raise AttributionInputError(
                f"{where} must be an object with start/end/owner")
        if start is not None:
            _check_int(start, f"{where} start")
        if end is not None:
            _check_int(end, f"{where} end")
        if not isinstance(owner, str):
            raise AttributionInputError(
                f"{where} owner must be non-empty text, got {owner!r}")
        if owner == "":
            raise AttributionInputError(f"{where} owner must be non-empty text")
        if start is not None and end is not None and start >= end:
            raise AttributionInputError(
                f"{where} is empty or inverted: [{start}, {end})")
        segments.append(Segment(start, end, owner))

    segments.sort(key=lambda s: (s.start is not None,
                                 0 if s.start is None else s.start))
    for prev, nxt in zip(segments, segments[1:]):
        if prev.end is None:
            raise AttributionInputError(
                f"history overlaps: [{prev.start}, +inf) covers everything"
                f" after [{nxt.start}, ...)")
        if nxt.start is None or nxt.start < prev.end:
            raise AttributionInputError(
                f"history overlaps: [{prev.start}, {prev.end}) and"
                f" [{nxt.start}, {nxt.end})")
    return tuple(segments)


class TimeMap:
    """Lookup structure for one device's validated history."""

    __slots__ = ("segments", "_has_neg_inf", "_finite", "_starts",
                 "_runs_cache")

    def __init__(self, segments: Sequence[Segment]):
        self.segments = tuple(segments)
        self._has_neg_inf = bool(self.segments) and self.segments[0].start is None
        self._finite = tuple(self.segments[1:] if self._has_neg_inf
                             else self.segments)
        self._starts = tuple(s.start for s in self._finite)
        self._runs_cache: Optional[Tuple[Segment, ...]] = None

    @classmethod
    def from_raw(cls, raw: Iterable) -> "TimeMap":
        return cls(parse_segments(raw))

    def _find(self, t: int):
        idx = bisect_right(self._starts, t)
        if idx:
            seg = self._finite[idx - 1]
        elif self._has_neg_inf:
            seg = self.segments[0]
        else:
            return None
        if seg.end is None or t < seg.end:
            return seg
        return None

    def owner_at(self, t: int) -> Optional[str]:
        """Effective owner at integer time ``t`` (None = unknown group).

        Because validated segments never overlap, only the segment with
        the greatest start <= t can possibly cover t; if it ends at or
        before t the time falls into a gap.
        """
        seg = self._find(t)
        return None if seg is None else seg.owner

    def segment_at(self, t: int):
        """The covering storage segment, or None (for debugging)."""
        return self._find(t)

    @property
    def _run_list(self) -> Tuple[Segment, ...]:
        cached = self._runs_cache
        if cached is None:
            cached = self.runs()
            self._runs_cache = cached
        return cached

    def runs(self) -> Tuple[Segment, ...]:
        """Maximal intervals of constant ownership.

        Adjacent segments with the same owner are merged, so splitting or
        merging storage segments *without* changing the effective owner
        leaves these intervals -- and therefore the interval reported by
        expansion -- exactly as it was.  A gap (unknown) separates runs.
        """
        out: List[Segment] = []
        for seg in self.segments:
            if out and out[-1].end is not None \
                    and out[-1].end == seg.start \
                    and out[-1].owner == seg.owner:
                out[-1] = Segment(out[-1].start, seg.end, seg.owner)
            else:
                out.append(seg)
        return tuple(out)

    def run_at(self, t: int):
        """The maximal constant-ownership interval covering t (or None)."""
        return TimeMap(self._run_list)._find(t)


def changed_intervals(old: TimeMap, new: TimeMap) -> List[Tuple[Optional[int],
                                                               Optional[int]]]:
    """Integer ranges (half-open, None = unbounded) where the effective
    *owner* differs between two histories.

    Segment boundaries are the only points where the covering owner can
    change, so the integer line is split at every bound of both histories
    and each part is sampled at one integer.  Adjacent changed parts are
    merged.  Only facts in these ranges can change their statistics group,
    and only they need their reported ownership interval refreshed.
    """
    bounds = set()
    for tm in (old, new):
        for seg in tm.segments:
            if seg.start is not None:
                bounds.add(seg.start)
            if seg.end is not None:
                bounds.add(seg.end)
    changed: List[Tuple[Optional[int], Optional[int]]] = []
    if not bounds:
        return changed
    points = sorted(bounds)
    parts: List[Tuple[Optional[int], Optional[int]]] = [(None, points[0])]
    for lo, hi in zip(points, points[1:]):
        parts.append((lo, hi))
    parts.append((points[-1], None))
    for lo, hi in parts:
        if lo is not None and hi is not None and lo >= hi:
            continue
        sample = hi - 1 if lo is None else lo
        if old.owner_at(sample) != new.owner_at(sample):
            if changed and changed[-1][1] is not None \
                    and changed[-1][1] == lo:
                changed[-1] = (changed[-1][0], hi)
            else:
                changed.append((lo, hi))
    return changed
