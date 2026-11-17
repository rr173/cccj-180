"""Error types for the two-stream interval join runtime."""


class InputError(Exception):
    """Illegal input message: unknown partition, duplicate/regressed seq,
    watermark regression, message after end, malformed fields, ...

    Any InputError fails the engine: it refuses further input and reports
    status 'failed'.
    """


class ResourceError(Exception):
    """Underlying storage (SQLite) failure: disk full, I/O error, ...

    Like InputError this ends the job as failed.
    """


class JobError(Exception):
    """Base class for persistent-job errors (store, batch, ack, delivery).

    Unlike InputError on the single-run JoinEngine, a rejected operation on
    a JobStore never poisons the job: state stays at the last committed
    point and the next legal operation is still accepted.
    """


class StoreFormatError(JobError):
    """The file at the job/warehouse path is not a usable store: missing or
    empty, an old event-table-only database without recovery metadata, an
    unknown format version, or corrupt/uninterpretable metadata.  Raised
    before anything is written."""


class JobExistsError(JobError):
    """create() was asked to initialise a job where a database (a job store
    or any other non-empty file) already exists.  The existing file is
    never silently replaced, cleared or reconfigured."""


class BusyError(JobError):
    """Another process currently holds the job's modification lock."""


class LockRequiredError(JobError):
    """A mutating operation was attempted without holding the modification
    lock (see JobStore.acquire()/modify())."""


class BatchConflictError(JobError):
    """batch_no was already committed with different content."""


class BatchGapError(JobError):
    """batch_no is not the expected next number (last committed + 1)."""


class JobFinishedError(JobError):
    """All partitions ended; new batches are rejected (resending an
    already-committed batch to obtain its receipt is still allowed)."""


class AckError(JobError):
    """Invalid ack credential: forged, corrupted, from another job or
    channel, not contiguous with the acknowledged position, or not
    matching the committed page content."""


class DeliveryConflictError(JobError):
    """The target warehouse has a different persistent identity than the
    one this job is bound to, or a row/receipt already exists under the
    same (job_id, channel, offset) identity with different content."""
