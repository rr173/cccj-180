"""Errors for the device-attribution mart.

Every rejection is a separate exception type so callers (and the CLI exit
codes) can tell "your revision is stale" from "this file is not a mart"
without parsing strings.
"""

from __future__ import annotations


class MartError(Exception):
    """Base class for mart errors.  A rejected operation never poisons the
    mart: the last committed version stays readable and a later legal
    operation is still accepted."""


class MartFormatError(MartError):
    """The file is missing/empty, not a mart, of an unknown format
    version, incomplete, or its metadata cannot be interpreted.  Raised
    before anything is written."""


class MartBusyError(MartError):
    """Another process holds the mart's single-writer lock.  The lock is
    an flock on a sidecar file, so a killed writer releases it (the kernel
    closes the fd) and no manual lock cleanup is ever needed."""


class MartLockRequiredError(MartError):
    """A mutating operation was attempted without the writer lock."""


class MartIdentityError(MartError):
    """The bound warehouse has a different persistent warehouse_id than
    the one recorded when this mart was created.  The same warehouse may
    move to another path; a different warehouse is always a mistake."""


class MartInputError(MartError):
    """Invalid operation input: bad job set, unbound job, unreadable
    warehouse rows, a gap in a newly captured range, unknown result kind,
    malformed replacement history, duplicate device, ... .  Nothing is
    committed."""


class StaleRevisionError(MartInputError):
    """The correction's expected_revision is not the current mart
    revision: someone else committed first.  Nothing is written."""


class AttributionInputError(MartError):
    """Invalid interval history: empty owner text, an empty interval,
    overlapping segments, a segment that does not start strictly after the
    previous one, or inverted bounds."""
