"""Command line interface for the device-attribution statistics mart.

Usage:
  python -m attribution_mart.cli create --mart MART --warehouse WH
      --jobs J1 [J2 ...] --width W --origin O
  python -m attribution_mart.cli sync --mart MART
  python -m attribution_mart.cli correct --mart MART
      --expected-revision R --history FILE|-
  python -m attribution_mart.cli query --mart MART [--view TOKEN]
      [--after WINDOW:OWNER] [--limit N]
  python -m attribution_mart.cli expand --mart MART --view TOKEN
      --window WINDOW --owner OWNER|--unknown [--kind match|lpad|rpad]
      [--limit N] [--after FACT_ID]
  python -m attribution_mart.cli status --mart MART
  python -m attribution_mart.cli rebind --mart MART --warehouse WH

Owner in `--after`/`--owner` is the literal owner text; the dedicated
unknown group is written as `--unknown` (or the JSON value ``null`` in a
history/correction document).  A history document is either a bare object
``{"dev": [segments...]}`` or the same object under the ``devices`` field.

Exit codes:
  0  success
  1  rejected: illegal history, stale expected revision, unbound job,
     warehouse mismatch, gap in new input, unknown kind, stale view, ...
  2  usage error
  3  busy: another process holds the mart writer lock
  4  mart format error (missing/empty/unknown version/incomplete/corrupt)
"""

from __future__ import annotations

import argparse
import json
import sys

from .errors import (AttributionInputError, MartBusyError, MartError,
                     MartFormatError, MartIdentityError, MartInputError,
                     MartLockRequiredError)
from .mart import AttributionMart, StaleVersionError


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def _read_json(spec: str):
    if spec == "-":
        text = sys.stdin.read()
    else:
        with open(spec, "r", encoding="utf-8") as f:
            text = f.read()
    return json.loads(text)


def _owner_arg(text: str):
    return None if text is None else text


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m attribution_mart.cli",
        description="Device history attribution statistics over a twostream "
                    "SQLite warehouse: sync, correct histories, query, "
                    "expand one group's facts.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("create", help="create a mart bound to a warehouse")
    p.add_argument("--mart", required=True)
    p.add_argument("--warehouse", required=True)
    p.add_argument("--jobs", nargs="+", required=True)
    p.add_argument("--width", type=int, required=True)
    p.add_argument("--origin", type=int, required=True)

    p = sub.add_parser("sync", help="synchronise new warehouse results")
    p.add_argument("--mart", required=True)

    p = sub.add_parser(
        "correct", help="atomically replace device ownership histories")
    p.add_argument("--mart", required=True)
    p.add_argument("--expected-revision", type=int, required=True)
    p.add_argument("--history", required=True,
                   help="JSON document {device: [segments...]}, or - for stdin")

    p = sub.add_parser("query", help="statistics of one mart version")
    p.add_argument("--mart", required=True)
    p.add_argument("--view", default=None)
    p.add_argument("--after", default=None,
                   help="cursor WINDOW:OWNER (OWNER empty for unknown)")
    p.add_argument("--limit", type=int, default=None)

    p = sub.add_parser("expand", help="page the facts of one group")
    p.add_argument("--mart", required=True)
    p.add_argument("--view", required=True)
    p.add_argument("--window", required=True, type=int)
    owner = p.add_mutually_exclusive_group(required=True)
    owner.add_argument("--owner", default=None)
    owner.add_argument("--unknown", action="store_true")
    p.add_argument("--kind", choices=["match", "lpad", "rpad"], default=None)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--after", type=int, default=None)

    p = sub.add_parser("status", help="mart status (read-only)")
    p.add_argument("--mart", required=True)

    p = sub.add_parser("rebind",
                       help="point the mart at the same warehouse, new path")
    p.add_argument("--mart", required=True)
    p.add_argument("--warehouse", required=True)

    args = ap.parse_args(argv)

    try:
        if args.cmd == "create":
            mart = AttributionMart.create(
                args.mart, warehouse=args.warehouse, job_ids=args.jobs,
                width=args.width, origin=args.origin)
            _print({"created": args.mart, **mart.status()})
            mart.close()
            return 0

        if args.cmd == "status":
            mart = AttributionMart.open(args.mart)
            try:
                _print(mart.status())
            finally:
                mart.close()
            return 0

        if args.cmd == "query":
            after = None
            if args.after is not None:
                if ":" not in args.after:
                    raise MartInputError(
                        "--after must be WINDOW:OWNER (OWNER empty for unknown)")
                win, _, owner = args.after.partition(":")
                after = (int(win), owner if owner != "" else None)
            mart = AttributionMart.open(args.mart)
            try:
                _print(mart.query(view_token=args.view, after=after,
                                  limit=args.limit))
            finally:
                mart.close()
            return 0

        if args.cmd == "expand":
            mart = AttributionMart.open(args.mart)
            try:
                owner = None if args.unknown else _owner_arg(args.owner)
                _print(mart.expand(args.view, args.window, owner,
                                   kind=args.kind, limit=args.limit,
                                   after=args.after))
            finally:
                mart.close()
            return 0

        if args.cmd in ("sync", "correct", "rebind"):
            mart = AttributionMart.open(args.mart)
            try:
                with mart.modify():
                    if args.cmd == "sync":
                        _print(mart.sync())
                    elif args.cmd == "rebind":
                        _print(mart.rebind_warehouse(args.warehouse))
                    else:
                        doc = _read_json(args.history)
                        if isinstance(doc, dict) and "devices" in doc \
                                and len(doc) == 1:
                            doc = doc["devices"]
                        if not isinstance(doc, dict):
                            raise AttributionInputError(
                                "history document must be an object mapping"
                                " device -> list of segments")
                        _print(mart.replace_attribution(
                            args.expected_revision, doc))
            finally:
                mart.close()
            return 0
        return 2
    except MartBusyError as e:
        print(f"BUSY: {e}", file=sys.stderr)
        return 3
    except MartFormatError as e:
        print(f"MART FORMAT ERROR: {e}", file=sys.stderr)
        return 4
    except StaleVersionError as e:
        print(f"STALE VIEW: {e}", file=sys.stderr)
        return 1
    except (MartInputError, AttributionInputError, MartIdentityError,
            MartLockRequiredError, MartError, ValueError) as e:
        print(f"REJECTED: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
