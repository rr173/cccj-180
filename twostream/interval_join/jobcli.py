"""Command line interface for persistent join jobs.

Usage:
  python -m interval_join.jobcli create  --db JOB --lo LO --hi HI
      --mode inner|left_outer|full_outer
      --left-partitions P... --right-partitions P...
  python -m interval_join.jobcli input   --db JOB --batch FILE|-
  python -m interval_join.jobcli pages   --db JOB --channel results|late
      [--limit N]
  python -m interval_join.jobcli ack     --db JOB --credential FILE|-
  python -m interval_join.jobcli status  --db JOB
  python -m interval_join.jobcli deliver --db JOB --warehouse WH
      [--page-size N]

`input` reads a batch envelope {"batch_no": n, "messages": [...]} and
prints the receipt; `pages` prints the next unacknowledged page including
its ack credential; `ack` consumes a credential; `deliver` (re)delivers
all pending pages of both channels to an independent SQLite warehouse and
is the way to resume delivery after a crash, with or without new input.

Exit codes:
  0  success
  1  rejected: invalid input, batch conflict/gap, finished job, ack or
     delivery conflict, warehouse identity mismatch, resource error
  2  usage error (bad arguments or configuration)
  3  busy: another process holds the job's modification lock
  4  store format error: missing/empty/old-format/incompatible/corrupt
     job store or warehouse

A process killed by a test hook exits with 117 (see TWOSTREAM_CRASH_AT in
interval_join.jobstore); that is a simulated crash, never a success.
"""

from __future__ import annotations

import argparse
import json
import sys

from .deliver import deliver_to_warehouse
from .errors import (BusyError, InputError, JobError, ResourceError,
                     StoreFormatError)
from .jobstore import JobStore


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def _read_text(spec: str) -> str:
    if spec == "-":
        return sys.stdin.read()
    with open(spec, "r", encoding="utf-8") as f:
        return f.read()


def _peak_rss_kb() -> int:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmHWM"):
                    return int(line.split()[1])
    except OSError:
        pass
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _page_json(page) -> dict:
    return {"channel": page.channel, "start": page.start, "end": page.end,
            "rows": [dict(d.row, offset=d.offset) for d in page.rows],
            "credential": page.credential}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m interval_join.jobcli",
        description="Persistent join jobs: create, batch input, status, "
                    "acknowledged delivery to an independent SQLite warehouse.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("create", help="create a new job store")
    p.add_argument("--db", required=True)
    p.add_argument("--lo", type=int, required=True)
    p.add_argument("--hi", type=int, required=True)
    p.add_argument("--mode", required=True,
                   choices=["inner", "left_outer", "full_outer"])
    p.add_argument("--left-partitions", nargs="+", required=True)
    p.add_argument("--right-partitions", nargs="+", required=True)

    p = sub.add_parser("input", help="submit one input batch envelope")
    p.add_argument("--db", required=True)
    p.add_argument("--batch", required=True,
                   help="batch envelope JSON file, or - for stdin")

    p = sub.add_parser("pages", help="show the next unacknowledged page")
    p.add_argument("--db", required=True)
    p.add_argument("--channel", required=True, choices=["results", "late"])
    p.add_argument("--limit", type=int, default=100)

    p = sub.add_parser("ack", help="acknowledge a page by its credential")
    p.add_argument("--db", required=True)
    p.add_argument("--credential", required=True,
                   help="credential JSON file, or - for stdin")

    p = sub.add_parser("status", help="show committed job status (read-only)")
    p.add_argument("--db", required=True)

    p = sub.add_parser("deliver",
                       help="deliver all pending pages to a SQLite warehouse")
    p.add_argument("--db", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--page-size", type=int, default=500)

    args = ap.parse_args(argv)

    try:
        if args.cmd == "create":
            with JobStore.create(
                    args.db, lo=args.lo, hi=args.hi, mode=args.mode,
                    left_partitions=args.left_partitions,
                    right_partitions=args.right_partitions) as job:
                _print({"job_id": job.job_id, "db": args.db,
                        "config": {"lo": job.lo, "hi": job.hi,
                                   "mode": job.mode,
                                   "left_partitions": job.left_partitions,
                                   "right_partitions": job.right_partitions}})
            return 0

        if args.cmd == "status":
            with JobStore.open(args.db) as job:
                _print(job.status())
            return 0

        if args.cmd == "pages":
            with JobStore.open(args.db) as job:
                _print(_page_json(job.read_page(args.channel, args.limit)))
            return 0

        if args.cmd == "input":
            envelope = _read_text(args.batch)
            with JobStore.open(args.db) as job, job.modify():
                _print(job.submit_batch(envelope))
            return 0

        if args.cmd == "ack":
            try:
                credential = json.loads(_read_text(args.credential))
            except json.JSONDecodeError as e:
                print(f"USAGE: credential is not valid JSON: {e}",
                      file=sys.stderr)
                return 2
            with JobStore.open(args.db) as job, job.modify():
                _print(job.ack_page(credential))
            return 0

        if args.cmd == "deliver":
            with JobStore.open(args.db) as job, job.modify():
                summary = deliver_to_warehouse(job, args.warehouse,
                                               page_size=args.page_size)
                summary["peak_rss_kb"] = _peak_rss_kb()
                _print(summary)
            return 0

        return 2  # unreachable: argparse requires a command
    except BusyError as e:
        print(f"BUSY: {e}", file=sys.stderr)
        return 3
    except StoreFormatError as e:
        print(f"STORE FORMAT ERROR: {e}", file=sys.stderr)
        return 4
    except (InputError, JobError, ResourceError) as e:
        print(f"REJECTED: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"USAGE: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
