"""Event-file runner and command line entry.

Event file format: JSON Lines, one record per line.  Blank lines and
lines starting with '#' are ignored.

  {"type":"config","lo":-2,"hi":5,"mode":"full_outer",
   "left_partitions":["lp0"],"right_partitions":["rp0","rp1"]}
  {"type":"data","side":"L","partition":"lp0","seq":1,
   "key":"dev-1","time":10,"payload":{"kind":"request"}}
  {"type":"watermark","side":"R","partition":"rp0","w":15}
  {"type":"end","side":"R","partition":"rp0"}
  {"type":"barrier","name":"checkpoint-1"}

Integer fields (seq/time/w) accept JSON integers of any size or decimal
strings, so >64-bit times round-trip through any JSON tooling.  "key" is
a string or null.  A "config" record, if present, must be the first
record.  "barrier" records do not reach the engine; the runner invokes
the on_barrier callback so drills can assert intermediate state.

Exit behaviour (CLI): input/resource errors -> exit 1; the stream not
being complete (partitions still open) at EOF -> exit 3.  Temporarily
having no pending rows never counts as completion.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable, Dict, Optional

from .engine import JoinEngine
from .errors import InputError, ResourceError
from .messages import parse_message
from .results import LeftPad, Match, RightPad


def load_config(path: str) -> Optional[Dict[str, Any]]:
    """Return the config record if it is the first record, else None."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            obj = json.loads(line)
            if isinstance(obj, dict) and obj.get("type") == "config":
                return obj
            return None
    return None


def engine_from_config(cfg: Dict[str, Any], db_path: str) -> JoinEngine:
    try:
        return JoinEngine(
            db_path,
            lo=cfg["lo"],
            hi=cfg["hi"],
            mode=cfg["mode"],
            left_partitions=cfg["left_partitions"],
            right_partitions=cfg["right_partitions"],
        )
    except KeyError as e:
        raise InputError(f"config record missing field {e}") from e
    except ValueError as e:
        raise InputError(f"invalid config record: {e}") from e


def run_event_file(engine: JoinEngine, path: str,
                   on_barrier: Optional[Callable[[str, JoinEngine], None]] = None
                   ) -> Dict[str, int]:
    """Feed every record of an event file into the engine.

    Barrier records invoke on_barrier(name, engine).  InputError raised
    by the engine is re-raised with the file line number attached.
    """
    stats = {"data": 0, "watermark": 0, "end": 0, "barrier": 0}
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise InputError(f"{path}:{lineno}: invalid JSON: {e}") from e
            t = obj.get("type") if isinstance(obj, dict) else None
            if t == "config":
                continue
            if t == "barrier":
                stats["barrier"] += 1
                if on_barrier is not None:
                    on_barrier(str(obj.get("name", "")), engine)
                continue
            try:
                msg = parse_message(obj)
            except InputError as e:
                raise InputError(f"{path}:{lineno}: {e}") from e
            try:
                engine.feed(msg)
            except InputError as e:
                raise InputError(f"{path}:{lineno}: {e}") from e
            stats[type(msg).__name__.lower()] += 1
    return stats


def _fmt_wm(v) -> str:
    if v is None:
        return "-inf"
    if v == float("inf"):
        return "+inf"
    return str(v)


def summarize(engine: JoinEngine, results, late) -> Dict[str, Any]:
    counts = {"match": 0, "lpad": 0, "rpad": 0}
    for r in results:
        if isinstance(r, Match):
            counts["match"] += 1
        elif isinstance(r, LeftPad):
            counts["lpad"] += 1
        else:
            counts["rpad"] += 1
    wm = engine.watermarks()
    return {
        "status": engine.status,
        "results": counts,
        "late": len(late),
        "pending": engine.pending(),
        "left_min_wm": _fmt_wm(wm["left_min"]),
        "right_min_wm": _fmt_wm(wm["right_min"]),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m interval_join.runner",
        description="Run an event file through the interval join engine.")
    ap.add_argument("eventfile")
    ap.add_argument("--db", default=":memory:", help="SQLite state path")
    ap.add_argument("--lo", type=int)
    ap.add_argument("--hi", type=int)
    ap.add_argument("--mode", choices=["inner", "left_outer", "full_outer"])
    ap.add_argument("--left-partitions", nargs="+")
    ap.add_argument("--right-partitions", nargs="+")
    args = ap.parse_args(argv)

    try:
        cfg = load_config(args.eventfile)
        if cfg is not None and args.lo is None:
            engine = engine_from_config(cfg, args.db)
        else:
            missing = [n for n, v in (("--lo", args.lo), ("--hi", args.hi),
                                      ("--mode", args.mode),
                                      ("--left-partitions", args.left_partitions),
                                      ("--right-partitions", args.right_partitions))
                       if v is None]
            if missing:
                print(f"error: no config record in file; required flags missing: "
                      f"{', '.join(missing)}", file=sys.stderr)
                return 2
            engine = JoinEngine(args.db, args.lo, args.hi, args.mode,
                                args.left_partitions, args.right_partitions)
        with engine:
            stats = run_event_file(engine, args.eventfile)
            results = list(engine.pull_results())
            late = list(engine.pull_late())
            summary = summarize(engine, results, late)
            summary["fed"] = stats
            print(json.dumps(summary, indent=2, ensure_ascii=False))
            if engine.status != "finished":
                print("job NOT complete: some partitions are still open "
                      "(no end received)", file=sys.stderr)
                return 3
        return 0
    except (InputError, ResourceError) as e:
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
