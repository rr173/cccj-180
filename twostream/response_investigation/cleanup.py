"""Remove a stale investigation file and its WAL sidecars.

Used before a build so an old, differently-shaped investigation can never
be mistaken for the result of the new one, and after a failed build so no
half-written database survives.
"""

from __future__ import annotations

import os


def remove_db(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = path + suffix
        try:
            os.remove(p)
        except FileNotFoundError:
            pass
