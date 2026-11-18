"""Shared-response investigation over a twostream SQLite warehouse.

Public API::

    ResponseInvestigation.build(path, warehouse, jobs, P, Q)
    ResponseInvestigation.open(path)

    inv.status()
    inv.request_node(step, identity)          -> node + its group at P/Q
    inv.require_request_node(step, identity)
    inv.groups(step, after=..., limit=...)    -> ordered, paginated
    inv.group(step, gid)
    inv.group_change(q_gid)                   -> merge | same | new
    inv.group_members(step, gid, ...)         -> paginated members
    inv.group_edges(step, gid, ...)           -> paginated edges
    inv.resolve("Q", request_a, request_b)    -> first connection + chain
    inv.walk_chain(node_a, node_b, k)         -> streamed evidence chain

A request identity is ``{"job_id", "partition", "seq"}`` or a
``(job, partition, seq)`` tuple.
"""

from .errors import (InvestigationBusyError, InvestigationError,
                     InvestigationFormatError, InvestigationIdentityError,
                     InvestigationInputError,
                     InvestigationLockRequiredError)
from .keys import SIDE_RANK, make_edge, node_key, short_node
from .request import request_identity
from .source import SourceWarehouse
from .store import DEFAULT_PAGE, INVESTIGATION_FORMAT_VERSION, MAX_PAGE, \
    ResponseInvestigation

__all__ = [
    "ResponseInvestigation", "SourceWarehouse", "request_identity",
    "make_edge", "node_key", "short_node", "SIDE_RANK",
    "InvestigationError", "InvestigationFormatError",
    "InvestigationInputError", "InvestigationIdentityError",
    "InvestigationBusyError", "InvestigationLockRequiredError",
    "INVESTIGATION_FORMAT_VERSION", "DEFAULT_PAGE", "MAX_PAGE",
]
