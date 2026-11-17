"""Business result and late-record types.

Event identity is (side, partition, seq): the same key+time value seen
under a different seq is a different event.  Result objects are frozen
and hashable so callers can compare runs with collections.Counter
(output order is scheduling-dependent; the multiset is not).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class EventId:
    side: str  # 'L' or 'R'
    partition: str
    seq: int

    def __str__(self) -> str:  # compact for drill output
        return f"{self.side}/{self.partition}#{self.seq}"


@dataclass(frozen=True)
class Match:
    left: EventId
    right: EventId
    key: str
    left_time: int
    right_time: int


@dataclass(frozen=True)
class LeftPad:
    """A left event proven to have no match: emitted as (left, null)."""
    left: EventId
    key: Optional[str]
    time: int


@dataclass(frozen=True)
class RightPad:
    """A right event proven to have no match: emitted as (null, right)."""
    right: EventId
    key: Optional[str]
    time: int


@dataclass(frozen=True)
class Late:
    """A data message that arrived behind its partition watermark.

    Late events bypass the join entirely: they never match and never
    retract or reopen results that were already emitted.
    """
    event: EventId
    key: Optional[str]
    time: int
    watermark: int  # partition watermark in effect when the event arrived
