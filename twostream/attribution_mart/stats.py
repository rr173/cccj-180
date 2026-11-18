"""Canonical statistics semantics shared by the mart and its reference:
windowing and exact nearest-rank quantiles.

Everything here is pure Python integer arithmetic.  The mart never loads
a group's latency list into memory; it stores per-request latencies in an
ordered SQLite table and answers the order statistics with indexed rank
queries.  These functions state the same numbers for small groups, unit
tests and the independent acceptance reference.

Nearest-rank quantile of probability ``p = num/den`` over ``n`` sorted
values: item ``ceil(p*n)`` (1-based).  Hence p50 = ``(n+1)//2`` and
p95 = ``(19*n + 19)//20``; no interpolation.
"""

from __future__ import annotations

from typing import Optional, Sequence

UNKNOWN = None  # the unknown attribution group is represented by None


def window_start(t: int, width: int, origin: int) -> int:
    """``O + floor((t-O)/W)*W`` for arbitrary-precision integer ``t``.

    Python's ``//`` already floors toward -infinity, which is exactly the
    required behaviour for negative times.
    """
    if isinstance(width, bool) or not isinstance(width, int) or width <= 0:
        raise ValueError(f"window width must be a positive integer, got {width!r}")
    if isinstance(origin, bool) or not isinstance(origin, int):
        raise ValueError(f"window origin must be an integer, got {origin!r}")
    if isinstance(t, bool) or not isinstance(t, int):
        raise ValueError(f"time must be an integer, got {t!r}")
    return origin + ((t - origin) // width) * width


def nearest_rank(sorted_values: Sequence[int], num: int,
                 den: int) -> Optional[int]:
    n = len(sorted_values)
    if n == 0:
        return None
    rank = (num * n + den - 1) // den
    return sorted_values[rank - 1]
