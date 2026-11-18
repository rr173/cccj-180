"""Node identities, their canonical ordering, and match-edge validation.

A match row from the warehouse is one undirected edge between a request
node ``(job, 'L', left partition, left seq)`` and a response node
``(job, 'R', right partition, right seq)``.  The job id is part of every
key, so equal partition names, seqs or device keys in different jobs or on
different sides never share an identity.  ``lpad``/``rpad``/``late`` rows
produce neither a node nor an edge.

Ordering (used for group ids, group listings, member listings and
serialisation) is the tuple

    (job_id, side with L before R, partition, numeric seq, ...)

with text compared by Unicode code point (Python's default string order)
and seqs compared as arbitrary-precision integers -- never as text or as
a 64-bit number.  The trailing ``raw`` parts are a deterministic tie-break
so two distinct sequences that happen to share the four identity fields
still have a total order.
"""

from __future__ import annotations

from typing import Optional, Tuple

from .errors import InvestigationInputError

# Side rank: requests (left) sort before responses (right).
SIDE_RANK = {"L": 0, "R": 1}

Node = Tuple[str, str, str, int]
# Raw evidence identity of one warehouse row: (job, results offset).
Raw = Tuple[str, int]


def parse_seq(text, where: str) -> int:
    """Parse a warehouse seq (decimal text) to an arbitrary-precision int."""
    if isinstance(text, bool) or not isinstance(text, int):
        if not isinstance(text, str) or not text:
            raise InvestigationInputError(
                f"{where}: expected decimal integer text for a seq, got {text!r}")
        try:
            value = int(text, 10)
        except ValueError as e:
            raise InvestigationInputError(
                f"{where}: not a decimal integer seq: {text!r}") from e
        return value
    return text


def parse_time(text, where: str) -> int:
    if isinstance(text, bool) or not isinstance(text, int):
        if not isinstance(text, str) or not text:
            raise InvestigationInputError(
                f"{where}: expected decimal integer text for a time, got {text!r}")
        try:
            return int(text, 10)
        except ValueError as e:
            raise InvestigationInputError(
                f"{where}: not a decimal integer time: {text!r}") from e
    return text


def node_key(node: Node, raw: Optional[Raw] = None) -> tuple:
    """Total-order key for a node; optionally disambiguated by raw row."""
    job, side, part, seq = node
    key = (job, SIDE_RANK[side], part, seq)
    if raw is None:
        return key
    return key + (raw[1],)


def short_node(node: Node) -> str:
    """Compact ``job:L/partition#seq`` text used in evidence chains."""
    job, side, part, seq = node
    return f"{job}:{side}/{part}#{seq}"


class Edge:
    """One validated match row: an undirected request-response edge."""

    __slots__ = ("left", "right", "key", "left_time", "right_time", "raw")

    def __init__(self, left: Node, right: Node, key: Optional[str],
                 left_time: int, right_time: int, raw: Raw):
        self.left = left
        self.right = right
        self.key = key
        self.left_time = left_time
        self.right_time = right_time
        self.raw = raw

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (f"Edge({short_node(self.left)}-{short_node(self.right)},"
                f" offset {self.raw})")


def make_edge(job_id: str, offset: int, kind: str, lp, ls, rp, rs, k,
              ltime, rtime) -> Optional[Edge]:
    """Validate one warehouse row.

    Returns an :class:`Edge` for ``match``; ``None`` for the pad kinds,
    which never create a node or an edge.  Anything else is rejected with
    the job and offset in the message.
    """
    where = f"job {job_id!r} results offset {offset}"
    if kind == "lpad" or kind == "rpad":
        # Pads are explicitly not part of this investigation's graph.
        return None
    if kind != "match":
        raise InvestigationInputError(
            f"{where}: unknown result kind {kind!r}; refusing to interpret "
            f"the warehouse")
    missing = [name for name, value in (
        ("left partition", lp), ("left seq", ls),
        ("right partition", rp), ("right seq", rs),
        ("left time", ltime), ("right time", rtime)) if value is None]
    if missing:
        raise InvestigationInputError(
            f"{where}: uninterpretable match row (missing "
            f"{', '.join(missing)})")
    left = (job_id, "L", lp, parse_seq(ls, where))
    right = (job_id, "R", rp, parse_seq(rs, where))
    return Edge(left, right, k, parse_time(ltime, where),
                parse_time(rtime, where), (job_id, offset))
