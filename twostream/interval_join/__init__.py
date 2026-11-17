"""Two-stream interval join runtime (device requests x device responses).

Public API:
    JoinEngine          -- the streaming join engine
    Data/Watermark/End  -- input messages
    Match/LeftPad/RightPad/Late/EventId -- output records
    InputError/ResourceError
    run_event_file / load_config / engine_from_config -- event-file drills
"""

from .encoding import enc_int
from .engine import JoinEngine
from .errors import InputError, ResourceError
from .messages import Data, End, Watermark, parse_message
from .results import EventId, Late, LeftPad, Match, RightPad

__all__ = [
    "JoinEngine", "Data", "Watermark", "End",
    "Match", "LeftPad", "RightPad", "Late", "EventId",
    "InputError", "ResourceError",
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

__all__ = [
    "JoinEngine", "Data", "Watermark", "End",
    "Match", "LeftPad", "RightPad", "Late", "EventId",
    "InputError", "ResourceError",
    "parse_message", "enc_int",
    "run_event_file", "load_config", "engine_from_config",
]
