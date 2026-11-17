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
