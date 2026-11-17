"""Two-stream interval join runtime (device requests x device responses).

Public API:
    JoinEngine          -- the streaming join engine (single-run)
    JobStore            -- persistent, recoverable join job (batch intake,
                           delivery pages, cumulative acks, status)
    deliver_to_warehouse / SQLiteWarehouse -- acknowledged delivery target
    Page / Delivery     -- pending delivery pages and their rows
    Data/Watermark/End  -- input messages
    Match/LeftPad/RightPad/Late/EventId -- output records
    InputError/ResourceError and the JobError family
    run_event_file / load_config / engine_from_config -- event-file drills
"""

from .deliver import SQLiteWarehouse, deliver_to_warehouse
from .encoding import enc_int
from .engine import JoinEngine
from .errors import (AckError, BatchConflictError, BatchGapError, BusyError,
                     DeliveryConflictError, InputError, JobError,
                     JobExistsError, JobFinishedError, LockRequiredError,
                     ResourceError, StoreFormatError)
from .jobstore import Delivery, JobStore, Page
from .messages import Data, End, Watermark, parse_message
from .results import EventId, Late, LeftPad, Match, RightPad

__all__ = [
    "JoinEngine", "JobStore", "Page", "Delivery",
    "SQLiteWarehouse", "deliver_to_warehouse",
    "Data", "Watermark", "End",
    "Match", "LeftPad", "RightPad", "Late", "EventId",
    "InputError", "ResourceError",
    "JobError", "StoreFormatError", "JobExistsError", "BusyError",
    "LockRequiredError", "BatchConflictError", "BatchGapError",
    "JobFinishedError", "AckError", "DeliveryConflictError",
    "parse_message", "enc_int",
    "run_event_file", "load_config", "engine_from_config",
]


def __getattr__(name):
    # Lazy so that `python -m interval_join.runner` does not import the
    # runner module twice (runpy warning).
    if name in ("run_event_file", "load_config", "engine_from_config"):
        from . import runner
        return getattr(runner, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
