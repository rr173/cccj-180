"""Event-file runner tests."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from interval_join import (EventId, InputError, JoinEngine, Match,
                           engine_from_config, load_config, run_event_file)
from interval_join.runner import main as cli_main


def write_file(lines):
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line if isinstance(line, str) else json.dumps(line))
            f.write("\n")
    return path


CONFIG = {"type": "config", "lo": -2, "hi": 5, "mode": "full_outer",
          "left_partitions": ["lp0"], "right_partitions": ["rp0"]}


class TestRunner(unittest.TestCase):
    def test_run_file_with_barriers(self):
        path = write_file([
            "# a comment",
            CONFIG,
            {"type": "data", "side": "L", "partition": "lp0", "seq": 1,
             "key": "d", "time": 10},
            {"type": "barrier", "name": "check"},
            {"type": "data", "side": "R", "partition": "rp0", "seq": 1,
             "key": "d", "time": 12},
            {"type": "end", "side": "L", "partition": "lp0"},
            {"type": "end", "side": "R", "partition": "rp0"},
        ])
        seen = []
        engine = engine_from_config(load_config(path), ":memory:")
        stats = run_event_file(engine, path,
                               on_barrier=lambda n, e: seen.append((n, e.pending()["left_state"])))
        self.assertEqual(seen, [("check", 1)])
        self.assertEqual(stats["data"], 2)
        self.assertEqual(stats["end"], 2)
        self.assertEqual(list(engine.pull_results()),
                         [Match(EventId("L", "lp0", 1), EventId("R", "rp0", 1),
                                "d", 10, 12)])
        self.assertEqual(engine.status, "finished")
        engine.close()
        os.remove(path)

    def test_big_time_as_string_in_file(self):
        big = 2 ** 70 + 7
        path = write_file([
            CONFIG,
            {"type": "data", "side": "L", "partition": "lp0", "seq": 1,
             "key": "d", "time": str(big)},
            {"type": "data", "side": "R", "partition": "rp0", "seq": 1,
             "key": "d", "time": str(big + 5)},
            {"type": "end", "side": "L", "partition": "lp0"},
            {"type": "end", "side": "R", "partition": "rp0"},
        ])
        engine = engine_from_config(load_config(path), ":memory:")
        run_event_file(engine, path)
        got = list(engine.pull_results())
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0].left_time, big)
        engine.close()
        os.remove(path)

    def test_input_error_reports_line(self):
        path = write_file([
            CONFIG,
            {"type": "watermark", "side": "L", "partition": "lp0", "w": 10},
            {"type": "watermark", "side": "L", "partition": "lp0", "w": 9},
        ])
        engine = engine_from_config(load_config(path), ":memory:")
        with self.assertRaises(InputError) as ctx:
            run_event_file(engine, path)
        self.assertIn(":3:", str(ctx.exception))
        self.assertEqual(engine.status, "failed")
        engine.close()
        os.remove(path)

    def test_missing_end_is_not_complete(self):
        path = write_file([
            CONFIG,
            {"type": "data", "side": "L", "partition": "lp0", "seq": 1,
             "key": "d", "time": 1},
        ])
        engine = engine_from_config(load_config(path), ":memory:")
        run_event_file(engine, path)
        self.assertEqual(engine.status, "running")  # not "finished"
        engine.close()
        os.remove(path)

    def test_cli_exit_codes(self):
        ok = write_file([CONFIG,
                         {"type": "end", "side": "L", "partition": "lp0"},
                         {"type": "end", "side": "R", "partition": "rp0"}])
        bad = write_file([CONFIG,
                          {"type": "watermark", "side": "L", "partition": "lp0", "w": 5},
                          {"type": "watermark", "side": "L", "partition": "lp0", "w": 4}])
        open_ended = write_file([CONFIG])
        self.assertEqual(cli_main([ok]), 0)
        self.assertEqual(cli_main([bad]), 1)          # input error -> failure
        self.assertEqual(cli_main([open_ended]), 3)   # not complete
        for p in (ok, bad, open_ended):
            os.remove(p)

    def test_cli_requires_config_or_flags(self):
        path = write_file([{"type": "end", "side": "L", "partition": "lp0"}])
        self.assertEqual(cli_main([path]), 2)  # no config, no flags
        self.assertEqual(cli_main([path, "--lo", "-2", "--hi", "5",
                                   "--mode", "inner",
                                   "--left-partitions", "lp0",
                                   "--right-partitions", "rp0"]), 3)
        os.remove(path)


if __name__ == "__main__":
    unittest.main()
