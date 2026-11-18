"""Command line for the shared-response investigation.

Usage::

  python -m response_investigation.cli build --db INV --warehouse WH \
      --jobs JOB [JOB ...] --P JOB=OFFSET [...] --Q JOB=OFFSET [...]
      [--overwrite]
  python -m response_investigation.cli status --db INV
  python -m response_investigation.cli node --db INV --step P|Q \
      --job JOB --partition PART --seq SEQ
  python -m response_investigation.cli groups --db INV --step P|Q \
      [--after GID] [--limit N]
  python -m response_investigation.cli group --db INV --step P|Q --group GID
  python -m response_investigation.cli members --db INV --step P|Q \
      --group GID [--after NODE] [--limit N]
  python -m response_investigation.cli edges --db INV --step P|Q \
      --group GID [--after JOB:OFFSET] [--limit N]
  python -m response_investigation.cli sources --db INV --group GID \
      [--after GID] [--limit N]
  python -m response_investigation.cli resolve --db INV --step Q \
      --a JOB:PART:SEQ --b JOB:PART:SEQ [--chain-limit N]
  python -m response_investigation.cli chain --db INV --step Q \
      --a JOB:PART:SEQ --b JOB:PART:SEQ [--after HOP] [--limit N]

Exit codes:
  0  success
  1  rejected (bad P/Q, unknown job, gap/out of range, unknown kind,
     malformed match row, missing node, wrong step, ...)
  2  usage error
  3  busy (another process is building the same investigation file)
  4  investigation format error (missing/empty/unknown version/incomplete)
"""

from __future__ import annotations

import argparse
import json
import sys

from .errors import (InvestigationBusyError, InvestigationError,
                     InvestigationFormatError, InvestigationInputError)
from .store import ResponseInvestigation


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def _parse_position(spec: str, flag: str):
    if "=" not in spec:
        raise InvestigationInputError(
            f"{flag} expects JOB=OFFSET, got {spec!r}")
    job, _, value = spec.partition("=")
    if not job:
        raise InvestigationInputError(f"{flag}: empty job id in {spec!r}")
    try:
        return job, int(value, 10)
    except ValueError:
        raise InvestigationInputError(
            f"{flag}: offset must be an integer, got {value!r}") from None


def _parse_identity(text: str):
    parts = text.split(":")
    if len(parts) != 3 or not parts[0] or not parts[1]:
        raise InvestigationInputError(
            f"request identity must be JOB:PARTITION:SEQ, got {text!r}")
    return {"job_id": parts[0], "partition": parts[1], "seq": parts[2]}


def _parse_edge_cursor(text):
    if ":" not in text:
        raise InvestigationInputError(
            f"edge cursor must be JOB:OFFSET, got {text!r}")
    job, _, off = text.rpartition(":")
    return [job, int(off, 10)]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m response_investigation.cli",
        description="Shared-response investigation over a twostream SQLite "
                    "warehouse: which requests must be investigated together, "
                    "how the groups changed between two observation points, "
                    "and when two requests first became connected.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("build", help="build one investigation (read-only "
                                     "snapshot of the warehouse)")
    p.add_argument("--db", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--jobs", nargs="+", required=True)
    p.add_argument("--P", nargs="+", required=True, metavar="JOB=OFFSET")
    p.add_argument("--Q", nargs="+", required=True, metavar="JOB=OFFSET")
    p.add_argument("--overwrite", action="store_true")

    p = sub.add_parser("status", help="identity, P/Q and counts")
    p.add_argument("--db", required=True)

    p = sub.add_parser("node", help="group of one request at one step")
    p.add_argument("--db", required=True)
    p.add_argument("--step", required=True, choices=["P", "Q"])
    p.add_argument("--job", required=True)
    p.add_argument("--partition", required=True)
    p.add_argument("--seq", required=True)

    p = sub.add_parser("groups", help="ordered group listing (paginated)")
    p.add_argument("--db", required=True)
    p.add_argument("--step", required=True, choices=["P", "Q"])
    p.add_argument("--after", type=int, default=None)
    p.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("group", help="one group and its Q change")
    p.add_argument("--db", required=True)
    p.add_argument("--step", required=True, choices=["P", "Q"])
    p.add_argument("--group", type=int, required=True)

    p = sub.add_parser("members", help="page one group's nodes")
    p.add_argument("--db", required=True)
    p.add_argument("--step", required=True, choices=["P", "Q"])
    p.add_argument("--group", type=int, required=True)
    p.add_argument("--after", type=int, default=None)
    p.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("edges", help="page one group's original match rows")
    p.add_argument("--db", required=True)
    p.add_argument("--step", required=True, choices=["P", "Q"])
    p.add_argument("--group", type=int, required=True)
    p.add_argument("--after", default=None, metavar="JOB:OFFSET")
    p.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("sources", help="page the old P groups of a Q group")
    p.add_argument("--db", required=True)
    p.add_argument("--group", type=int, required=True)
    p.add_argument("--after", type=int, default=None)
    p.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("resolve", help="first connection of two requests")
    p.add_argument("--db", required=True)
    p.add_argument("--step", required=True, choices=["P", "Q"])
    p.add_argument("--a", required=True, metavar="JOB:PART:SEQ")
    p.add_argument("--b", required=True, metavar="JOB:PART:SEQ")
    p.add_argument("--chain-limit", type=int, default=200,
                   help="hops kept in the answer (see also `chain`)")

    p = sub.add_parser("chain", help="page the canonical shortest chain")
    p.add_argument("--db", required=True)
    p.add_argument("--step", required=True, choices=["P", "Q"])
    p.add_argument("--a", required=True, metavar="JOB:PART:SEQ")
    p.add_argument("--b", required=True, metavar="JOB:PART:SEQ")
    p.add_argument("--after", type=int, default=0)
    p.add_argument("--limit", type=int, default=None)

    args = ap.parse_args(argv)

    try:
        if args.cmd == "build":
            p_map, q_map = {}, {}
            for spec in args.P:
                job, value = _parse_position(spec, "--P")
                p_map[job] = value
            for spec in args.Q:
                job, value = _parse_position(spec, "--Q")
                q_map[job] = value
            inv = ResponseInvestigation.build(
                args.db, args.warehouse, args.jobs, p_map, q_map,
                overwrite=args.overwrite)
            try:
                _print({"built": args.db, **inv.status()})
            finally:
                inv.close()
            return 0

        inv = ResponseInvestigation.open(args.db)
        try:
            if args.cmd == "status":
                _print(inv.status())
                return 0
            if args.cmd == "node":
                identity = {"job_id": args.job, "partition": args.partition,
                            "seq": args.seq}
                node = inv.request_node(args.step, identity)
                if node is None:
                    raise InvestigationInputError(
                        f"job {args.job!r} request {args.partition!r}#"
                        f"{args.seq} is not a node at {args.step}")
                _print(node)
                return 0
            if args.cmd == "groups":
                _print(inv.groups(args.step, after=args.after,
                                  limit=args.limit))
                return 0
            if args.cmd == "group":
                _print(inv.group(args.step, args.group))
                return 0
            if args.cmd == "members":
                _print(inv.group_members(args.step, args.group,
                                         after=args.after, limit=args.limit))
                return 0
            if args.cmd == "edges":
                cursor = (None if args.after is None
                          else _parse_edge_cursor(args.after))
                _print(inv.group_edges(args.step, args.group,
                                       after=cursor, limit=args.limit))
                return 0
            if args.cmd == "sources":
                _print(inv.change_sources(args.group, after=args.after,
                                          limit=args.limit))
                return 0
            if args.cmd == "resolve":
                _print(inv.resolve(args.step, _parse_identity(args.a),
                                   _parse_identity(args.b),
                                   chain_limit=args.chain_limit))
                return 0
            if args.cmd == "chain":
                _print(inv.chain_page(args.step, _parse_identity(args.a),
                                      _parse_identity(args.b),
                                      after=args.after, limit=args.limit))
                return 0
        finally:
            inv.close()
    except InvestigationBusyError as e:
        print(f"busy: {e}", file=sys.stderr)
        return 3
    except InvestigationFormatError as e:
        print(f"format error: {e}", file=sys.stderr)
        return 4
    except InvestigationInputError as e:
        print(f"rejected: {e}", file=sys.stderr)
        return 1
    except InvestigationError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
