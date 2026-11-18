"""Device history attribution statistics mart.

Public API:
    AttributionMart     -- create/open a mart bound to a warehouse and a
                           job set; sync, correct attribution, query,
                           expand one group's facts
    StaleVersionError   -- a pinned view/expansion is no longer current
    history             -- [start, end) ownership histories + TimeMap
    stats               -- window arithmetic and nearest-rank quantiles
    SourceWarehouse     -- read-only warehouse access used by the mart
"""

from .errors import (AttributionInputError, MartBusyError, MartError,
                     MartFormatError, MartIdentityError, MartInputError,
                     MartLockRequiredError, StaleRevisionError)
from .history import Segment, TimeMap, changed_intervals, parse_segments
from .mart import (AttributionMart, StaleVersionError, owner_key,
                   owner_value)
from .source import SourceWarehouse, parse_dec
from .stats import nearest_rank, window_start

__all__ = [
    "AttributionMart", "StaleVersionError", "SourceWarehouse",
    "Segment", "TimeMap", "changed_intervals", "parse_segments",
    "window_start", "nearest_rank", "parse_dec", "owner_key", "owner_value",
    "MartError", "MartFormatError", "MartBusyError",
    "MartLockRequiredError", "MartIdentityError", "MartInputError",
    "AttributionInputError", "StaleRevisionError",
]
