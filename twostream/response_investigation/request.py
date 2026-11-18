"""A request identity: ``(job_id, partition, seq)`` -- always a left event."""

from __future__ import annotations

from typing import Tuple

from .errors import InvestigationInputError


def request_identity(value) -> Tuple[str, str, int]:
    """Normalise one request identity.

    Accepts ``{"job_id": ..., "partition": ..., "seq": ...}`` (extra keys
    rejected) or a ``(job, partition, seq)`` tuple.  The seq may be an
    arbitrary-precision Python int or canonical decimal text.
    """
    if isinstance(value, dict):
        extra = set(value) - {"job_id", "partition", "seq"}
        if extra:
            raise InvestigationInputError(
                f"request identity has unknown fields {sorted(extra)}")
        try:
            job, part, seq = value["job_id"], value["partition"], value["seq"]
        except KeyError as e:
            raise InvestigationInputError(
                f"request identity is missing field {e.args[0]!r}") from e
    elif isinstance(value, (tuple, list)):
        if len(value) != 3:
            raise InvestigationInputError(
                f"request identity tuple must be (job, partition, seq), got "
                f"{value!r}")
        job, part, seq = value
    else:
        raise InvestigationInputError(
            f"request identity must be an object or a 3-tuple, got {value!r}")
    if not isinstance(job, str) or not job:
        raise InvestigationInputError(
            f"request job_id must be non-empty text, got {job!r}")
    if not isinstance(part, str) or not part:
        raise InvestigationInputError(
            f"request partition must be non-empty text, got {part!r}")
    if isinstance(seq, bool):
        raise InvestigationInputError(f"request seq must be an integer, got {seq!r}")
    if isinstance(seq, str):
        try:
            seq = int(seq, 10)
        except ValueError as e:
            raise InvestigationInputError(
                f"request seq is not a decimal integer: {seq!r}") from e
    elif not isinstance(seq, int):
        raise InvestigationInputError(f"request seq must be an integer, got {seq!r}")
    return job, part, seq
