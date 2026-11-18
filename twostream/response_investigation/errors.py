"""Rejections for the shared-response investigation tool.

Every failure is a distinct type so the CLI can map it to an exit code
without parsing messages.  A rejected build never leaves a database that
could be mistaken for a successful investigation.
"""

from __future__ import annotations


class InvestigationError(Exception):
    """Base class for investigation rejections."""


class InvestigationFormatError(InvestigationError):
    """The investigation file is missing/empty, not an investigation,
    of an unknown format version, incomplete, or its metadata cannot be
    interpreted.  Raised before anything is queried or written."""


class InvestigationInputError(InvestigationError):
    """Invalid investigation input: unknown job, bad P/Q, an offset gap,
    an out-of-range position, an unknown result kind, an uninterpretable
    match row, a request the caller named that is not a node, ... .  The
    message names the job and position whenever it has one."""


class InvestigationIdentityError(InvestigationInputError):
    """The source warehouse no longer has the identity recorded in the
    investigation, or a save requested a different warehouse."""


class InvestigationBusyError(InvestigationError):
    """Another process holds the investigation's single-writer lock.  The
    lock is an flock on a sidecar file: a killed writer releases it and no
    manual cleanup is ever needed."""


class InvestigationLockRequiredError(InvestigationError):
    """A mutating operation was attempted without the writer lock."""
