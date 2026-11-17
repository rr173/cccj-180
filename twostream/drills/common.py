"""Shared helpers for the drill scripts."""

from __future__ import annotations

import json
import os
import sys

# Allow running drills as plain scripts (python drills/drill_x.py) as well
# as modules (python -m drills.drill_x) from the project root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interval_join import engine_from_config, run_event_file  # noqa: E402

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")


def out_dir() -> str:
    os.makedirs(OUT_DIR, exist_ok=True)
    return OUT_DIR


def write_event_file(name: str, records) -> str:
    """Write records (dicts) as a JSON-lines event file under drills/out/."""
    path = os.path.join(out_dir(), name + ".jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            if isinstance(rec, str):
                f.write(rec + "\n")
            else:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return path


def run_drill(name, records, checkpoints=None, db_name=None):
    """Run an event-file drill.

    checkpoints: {barrier_name: fn(engine, results, late)} called at the
    barrier after draining everything emitted so far into results/late.
    Returns (engine, results, late, event_path).
    """
    results, late = [], []

    def on_barrier(barrier_name, engine):
        results.extend(engine.pull_results())
        late.extend(engine.pull_late())
        if checkpoints and barrier_name in checkpoints:
            checkpoints[barrier_name](engine, results, late)

    event_path = write_event_file(name, records)
    cfg = records[0]
    assert cfg.get("type") == "config", "first record must be config"
    db_path = ":memory:"
    if db_name is not None:
        db_path = os.path.join(out_dir(), db_name)
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(db_path + suffix):
                os.remove(db_path + suffix)
    engine = engine_from_config(cfg, db_path)
    run_event_file(engine, event_path, on_barrier=on_barrier)
    results.extend(engine.pull_results())
    late.extend(engine.pull_late())
    return engine, results, late, event_path


def db_size(engine) -> int:
    """Size of the engine's SQLite files in bytes (0 for :memory:)."""
    path = engine._conn.execute("PRAGMA database_list").fetchone()[2]
    if not path:
        return 0
    total = 0
    for suffix in ("", "-wal"):
        if os.path.exists(path + suffix):
            total += os.path.getsize(path + suffix)
    return total


def fmt_wm(v) -> str:
    import math
    if v is None:
        return "-inf"
    if v == math.inf:
        return "+inf"
    return str(v)


def counts(results):
    from interval_join import LeftPad, Match
    c = {"match": 0, "lpad": 0, "rpad": 0}
    for r in results:
        if isinstance(r, Match):
            c["match"] += 1
        elif isinstance(r, LeftPad):
            c["lpad"] += 1
        else:
            c["rpad"] += 1
    return c
