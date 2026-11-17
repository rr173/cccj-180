"""Input message types and validation.

Stream grammar per side ('L' = left / device requests, 'R' = right /
device responses):

  data(partition, seq, key, time, payload)
  watermark(partition, w)
  end(partition)

Rules enforced by the engine:
  * partitions are pre-declared per side; unknown partition -> InputError
  * seq is strictly increasing per partition (duplicates -> InputError)
  * watermarks are monotonic per partition (regression -> InputError)
  * data with time < current partition watermark goes to the late bypass
    (time == watermark is still on time)
  * end() seals a partition: watermark becomes +infinity and any further
    message for it is an InputError
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .errors import InputError

SIDES = ("L", "R")


@dataclass(frozen=True)
class Data:
    side: str
    partition: str
    seq: int
    key: Optional[str]
    time: int
    payload: Any = None


@dataclass(frozen=True)
class Watermark:
    side: str
    partition: str
    w: int


@dataclass(frozen=True)
class End:
    side: str
    partition: str


def _check_int(name: str, v: Any) -> None:
    if not isinstance(v, int) or isinstance(v, bool):
        raise InputError(f"{name} must be an integer, got {v!r}")


def _check_side(side: Any) -> None:
    if side not in SIDES:
        raise InputError(f"side must be 'L' or 'R', got {side!r}")


def _check_partition(partition: Any) -> None:
    if not isinstance(partition, str) or not partition:
        raise InputError(f"partition must be a non-empty string, got {partition!r}")


def validate_message(m: Any) -> None:
    """Raise InputError if a programmatically built message is malformed."""
    if isinstance(m, Data):
        _check_side(m.side)
        _check_partition(m.partition)
        _check_int("seq", m.seq)
        _check_int("time", m.time)
        if m.key is not None and not isinstance(m.key, str):
            raise InputError(f"key must be text or null, got {m.key!r}")
    elif isinstance(m, Watermark):
        _check_side(m.side)
        _check_partition(m.partition)
        _check_int("w", m.w)
    elif isinstance(m, End):
        _check_side(m.side)
        _check_partition(m.partition)
    else:
        raise InputError(f"unsupported message {m!r}")


def _int_field(obj: dict, name: str) -> int:
    if name not in obj:
        raise InputError(f"missing field {name!r}")
    v = obj[name]
    if isinstance(v, bool):
        raise InputError(f"field {name!r} must be an integer, got {v!r}")
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s and (s.isdigit() or (s[0] == "-" and s[1:].isdigit())):
            return int(s)
    raise InputError(f"field {name!r} must be an integer or decimal string, got {v!r}")


def parse_message(obj: Any):
    """Parse one JSON-decoded dict into a message. Raises InputError."""
    if not isinstance(obj, dict):
        raise InputError(f"message must be an object, got {obj!r}")
    t = obj.get("type")
    if t == "data":
        key = obj.get("key")
        if key is not None and not isinstance(key, str):
            raise InputError(f"field 'key' must be text or null, got {key!r}")
        return Data(
            side=obj.get("side"),
            partition=obj.get("partition"),
            seq=_int_field(obj, "seq"),
            key=key,
            time=_int_field(obj, "time"),
            payload=obj.get("payload"),
        )
    if t in ("watermark", "wm"):
        return Watermark(
            side=obj.get("side"),
            partition=obj.get("partition"),
            w=_int_field(obj, "w"),
        )
    if t == "end":
        return End(side=obj.get("side"), partition=obj.get("partition"))
    raise InputError(f"unknown message type {t!r}")
