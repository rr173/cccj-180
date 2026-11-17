"""Engine semantics and validation tests."""

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from interval_join import (Data, End, EventId, InputError, JoinEngine,
                           Late, LeftPad, Match, RightPad, Watermark)


def make(mode="full_outer", lo=-2, hi=5, lp=("lp0",), rp=("rp0",)):
    return JoinEngine(":memory:", lo, hi, mode, list(lp), list(rp))


def drain(e):
    return list(e.pull_results())


def drain_late(e):
    return list(e.pull_late())


def end_all(e):
    for p in e.left_partitions:
        e.feed_end("L", p)
    for p in e.right_partitions:
        e.feed_end("R", p)


class TestWindow(unittest.TestCase):
    def test_endpoint_equality(self):
        e = make(mode="inner")
        e.feed_data("L", "lp0", 1, "k", 10)
        for seq, t in enumerate([7, 8, 15, 16], start=1):
            e.feed_data("R", "rp0", seq, "k", t)
        got = drain(e)
        # window is [10-2, 10+5] = [8, 15], both bounds inclusive
        self.assertEqual({r.right.seq for r in got}, {2, 3})
        self.assertTrue(all(isinstance(r, Match) for r in got))
        end_all(e)
        self.assertEqual(e.status, "finished")
        e.close()

    def test_past_and_future_windows(self):
        # lo=-5, hi=-2: window entirely in the past of the left event
        e = make(mode="inner", lo=-5, hi=-2)
        e.feed_data("L", "lp0", 1, "k", 10)
        for seq, t in enumerate([4, 5, 8, 9], start=1):
            e.feed_data("R", "rp0", seq, "k", t)
        self.assertEqual({r.right.seq for r in drain(e)}, {2, 3})  # 5 and 8
        e.close()
        # lo=3, hi=7: window entirely in the future
        e = make(mode="inner", lo=3, hi=7)
        e.feed_data("L", "lp0", 1, "k", 10)
        for seq, t in enumerate([12, 13, 17, 18], start=1):
            e.feed_data("R", "rp0", seq, "k", t)
        self.assertEqual({r.right.seq for r in drain(e)}, {2, 3})  # 13 and 17
        e.close()

    def test_match_on_second_arrival_either_order(self):
        e = make(mode="inner")
        e.feed_data("R", "rp0", 1, "k", 12)
        self.assertEqual(drain(e), [])          # nothing until the pair exists
        e.feed_data("L", "lp0", 1, "k", 10)
        got = drain(e)
        self.assertEqual(got, [Match(EventId("L", "lp0", 1),
                                     EventId("R", "rp0", 1), "k", 10, 12)])
        e.close()

    def test_same_value_distinct_identity(self):
        e = make(mode="inner")
        e.feed_data("L", "lp0", 1, "k", 10)
        e.feed_data("L", "lp0", 2, "k", 10)   # same key+time, other identity
        e.feed_data("R", "rp0", 1, "k", 10)
        e.feed_data("R", "rp0", 2, "k", 10)
        got = drain(e)
        self.assertEqual(len(got), 4)          # full cartesian product
        self.assertEqual({(r.left.seq, r.right.seq) for r in got},
                         {(1, 1), (1, 2), (2, 1), (2, 2)})
        e.close()


class TestNullKeys(unittest.TestCase):
    def test_inner_drops_null_keys(self):
        e = make(mode="inner")
        e.feed_data("L", "lp0", 1, None, 1)
        e.feed_data("R", "rp0", 1, None, 2)
        self.assertEqual(e.pending()["left_state"], 0)
        self.assertEqual(e.pending()["right_state"], 0)
        end_all(e)
        self.assertEqual(drain(e), [])
        self.assertEqual(e.status, "finished")
        e.close()

    def test_null_never_matches_null(self):
        e = make(mode="full_outer")
        e.feed_data("L", "lp0", 1, None, 10)
        e.feed_data("R", "rp0", 1, None, 12)  # would match if null==null
        end_all(e)
        got = drain(e)
        self.assertCountEqual(got, [LeftPad(EventId("L", "lp0", 1), None, 10),
                                    RightPad(EventId("R", "rp0", 1), None, 12)])
        self.assertFalse(any(isinstance(r, Match) for r in got))
        e.close()

    def test_null_key_left_pad_left_outer(self):
        e = make(mode="left_outer")
        e.feed_data("L", "lp0", 1, None, 10)
        e.feed_watermark("R", "rp0", 16)   # 16 > 10+5
        self.assertEqual(drain(e), [LeftPad(EventId("L", "lp0", 1), None, 10)])
        e.close()

    def test_null_key_right_dropped_in_left_outer(self):
        e = make(mode="left_outer")
        e.feed_data("R", "rp0", 1, None, 10)  # can never produce output here
        self.assertEqual(e.pending()["right_state"], 0)
        end_all(e)
        self.assertEqual(drain(e), [])
        e.close()

    def test_null_key_right_pad_full_outer(self):
        e = make(mode="full_outer")
        e.feed_data("R", "rp0", 1, None, 20)
        e.feed_watermark("L", "lp0", 23)   # 23 > 20-(-2)
        self.assertEqual(drain(e), [RightPad(EventId("R", "rp0", 1), None, 20)])
        e.close()


class TestWatermarksAndPads(unittest.TestCase):
    def test_strictly_greater_bound_left(self):
        e = make(mode="left_outer")
        e.feed_data("L", "lp0", 1, "k", 10)
        e.feed_watermark("R", "rp0", 15)   # 15 > 10+5 is false
        self.assertEqual(drain(e), [])
        self.assertEqual(e.pending()["left_state"], 1)
        e.feed_watermark("R", "rp0", 16)   # now strictly greater
        self.assertEqual(drain(e), [LeftPad(EventId("L", "lp0", 1), "k", 10)])
        self.assertEqual(e.pending()["left_state"], 0)
        e.close()

    def test_strictly_greater_bound_right(self):
        e = make(mode="full_outer")
        e.feed_data("R", "rp0", 1, "k", 20)
        e.feed_watermark("L", "lp0", 22)   # 22 > 20-(-2) is false
        self.assertEqual(drain(e), [])
        e.feed_watermark("L", "lp0", 23)
        self.assertEqual(drain(e), [RightPad(EventId("R", "rp0", 1), "k", 20)])
        e.close()

    def test_matched_event_not_reclaimed_early(self):
        e = make(mode="left_outer")
        e.feed_watermark("R", "rp0", 0)
        e.feed_data("L", "lp0", 1, "k", 10)
        e.feed_data("R", "rp0", 1, "k", 12)
        self.assertEqual(e.pending()["left_state"], 1)  # matched, still kept
        e.feed_data("R", "rp0", 2, "k", 15)
        got = drain(e)
        self.assertEqual(len(got), 2)                    # both pairs
        e.feed_watermark("R", "rp0", 16)
        self.assertEqual(e.pending()["left_state"], 0)   # proof reclaim
        self.assertEqual(drain(e), [])                   # matched -> no pad
        e.close()

    def test_own_watermark_does_not_seal(self):
        # left's own watermark advancing must not conclude left rows
        e = make(mode="left_outer")
        e.feed_data("L", "lp0", 1, "k", 10)
        e.feed_watermark("L", "lp0", 10_000)
        self.assertEqual(drain(e), [])
        self.assertEqual(e.pending()["left_state"], 1)
        e.close()

    def test_silent_partition_blocks(self):
        e = make(mode="left_outer", rp=("rp0", "rp1"))
        e.feed_watermark("R", "rp0", 1000)
        e.feed_data("L", "lp0", 1, "k", 10)
        self.assertEqual(drain(e), [])
        e.feed_watermark("R", "rp1", 100)
        self.assertEqual(drain(e), [LeftPad(EventId("L", "lp0", 1), "k", 10)])
        e.close()

    def test_full_outer_both_sides_pad(self):
        e = make(mode="full_outer")
        e.feed_data("L", "lp0", 1, "a", 1)
        e.feed_data("R", "rp0", 1, "b", 100)
        end_all(e)
        self.assertCountEqual(drain(e), [LeftPad(EventId("L", "lp0", 1), "a", 1),
                                         RightPad(EventId("R", "rp0", 1), "b", 100)])
        e.close()

    def test_inner_never_pads(self):
        e = make(mode="inner")
        e.feed_data("L", "lp0", 1, "a", 1)
        e.feed_data("R", "rp0", 1, "b", 100)
        end_all(e)
        self.assertEqual(drain(e), [])
        e.close()

    def test_arrival_after_opposite_end(self):
        # right side already ended: a new left row is concluded on arrival
        e = make(mode="left_outer")
        e.feed_end("R", "rp0")
        e.feed_data("L", "lp0", 1, "k", 10)
        self.assertEqual(drain(e), [LeftPad(EventId("L", "lp0", 1), "k", 10)])
        self.assertEqual(e.pending()["left_state"], 0)
        e.close()
        # inner mode: concluded silently
        e = make(mode="inner")
        e.feed_end("R", "rp0")
        e.feed_data("L", "lp0", 1, "k", 10)
        self.assertEqual(drain(e), [])
        self.assertEqual(e.pending()["left_state"], 0)
        e.close()


class TestLate(unittest.TestCase):
    def test_late_bypass_and_equal_is_ontime(self):
        e = make(mode="inner")
        e.feed_watermark("L", "lp0", 10)
        e.feed_data("L", "lp0", 1, "k", 9)    # late
        e.feed_data("L", "lp0", 2, "k", 10)   # equal to wm: on time
        self.assertEqual(e.pending()["left_state"], 1)
        late = drain_late(e)
        self.assertEqual(late, [Late(EventId("L", "lp0", 1), "k", 9, 10)])
        e.close()

    def test_late_does_not_reopen_sealed_results(self):
        e = make(mode="left_outer")
        e.feed_data("L", "lp0", 1, "k", 10)
        e.feed_watermark("R", "rp0", 16)
        self.assertEqual(drain(e), [LeftPad(EventId("L", "lp0", 1), "k", 10)])
        # a would-be match arrives late; the sealed pad must stand
        e.feed_data("R", "rp0", 1, "k", 15)
        self.assertEqual(drain(e), [])          # no retraction, no match
        late = drain_late(e)
        self.assertEqual(len(late), 1)
        self.assertEqual(late[0].event, EventId("R", "rp0", 1))
        self.assertEqual(late[0].watermark, 16)
        e.close()

    def test_late_never_joins(self):
        e = make(mode="inner")
        e.feed_watermark("R", "rp0", 100)
        e.feed_data("L", "lp0", 1, "k", 50)
        e.feed_data("R", "rp0", 1, "k", 52)   # late (52 < 100)
        self.assertEqual(drain(e), [])
        self.assertEqual(len(drain_late(e)), 1)
        e.close()


class TestBigTime(unittest.TestCase):
    def test_beyond_int64(self):
        base = 2 ** 70 + 12345
        e = make(mode="left_outer")
        e.feed_data("L", "lp0", 1, "k", base)
        e.feed_data("R", "rp0", 1, "k", base + 5)   # hi endpoint
        e.feed_data("R", "rp0", 2, "k", base + 6)   # outside
        got = drain(e)
        self.assertEqual([r.right.seq for r in got], [1])
        e.feed_watermark("R", "rp0", base + 6)      # base+5 < base+6: seal
        self.assertEqual(drain(e), [])              # matched, no pad
        self.assertEqual(e.pending()["left_state"], 0)
        e.close()

    def test_negative_big(self):
        base = -(2 ** 70)
        e = make(mode="inner")
        e.feed_data("L", "lp0", 1, "k", base)
        e.feed_data("R", "rp0", 1, "k", base - 2)   # lo=-2 endpoint
        e.feed_data("R", "rp0", 2, "k", base - 3)   # outside
        self.assertEqual([r.right.seq for r in drain(e)], [1])
        e.close()

    def test_big_watermark_ordering(self):
        e = make(mode="left_outer")
        e.feed_data("L", "lp0", 1, "k", 10)
        e.feed_watermark("R", "rp0", 2 ** 70)
        self.assertEqual(drain(e), [LeftPad(EventId("L", "lp0", 1), "k", 10)])
        e.close()


class TestLifecycle(unittest.TestCase):
    def test_empty_partitions_finish(self):
        e = make(lp=("lp0", "lp1"), rp=("rp0", "rp1", "rp2"))
        end_all(e)
        self.assertEqual(e.status, "finished")
        self.assertEqual(drain(e), [])
        self.assertEqual(e.pending()["left_state"], 0)
        e.close()

    def test_no_pending_is_not_completion(self):
        e = make(mode="inner")
        e.feed_data("L", "lp0", 1, "k", 5)
        e.feed_data("R", "rp0", 1, "k", 5)
        e.feed_watermark("R", "rp0", 11)   # expires L
        e.feed_watermark("L", "lp0", 8)    # expires R
        self.assertEqual(e.pending()["left_state"], 0)
        self.assertEqual(e.pending()["right_state"], 0)
        self.assertEqual(e.status, "running")   # no end() yet: not complete
        end_all(e)
        self.assertEqual(e.status, "finished")
        e.close()

    def test_watermarks_api(self):
        e = make(lp=("lp0", "lp1"), rp=("rp0",))
        wm = e.watermarks()
        self.assertIsNone(wm["left_min"])
        self.assertIsNone(wm["left"]["lp0"]["watermark"])
        e.feed_watermark("L", "lp0", 7)
        self.assertIsNone(e.watermarks()["left_min"])   # lp1 still silent
        e.feed_watermark("L", "lp1", 9)
        self.assertEqual(e.watermarks()["left_min"], 7)
        e.feed_end("L", "lp0")
        self.assertEqual(e.watermarks()["left_min"], 9)  # ended = +inf
        e.feed_end("L", "lp1")
        self.assertEqual(e.watermarks()["left_min"], math.inf)
        e.close()

    def test_pull_drains_exactly_once(self):
        e = make(mode="inner")
        e.feed_data("L", "lp0", 1, "k", 10)
        for seq in (1, 2, 3):
            e.feed_data("R", "rp0", seq, "k", 10)
        first = list(e.pull_results(limit=2))
        self.assertEqual(len(first), 2)
        rest = list(e.pull_results())
        self.assertEqual(len(rest), 1)
        self.assertEqual(drain(e), [])
        self.assertEqual(len({r.right.seq for r in first + rest}), 3)
        e.close()


class TestValidation(unittest.TestCase):
    def test_duplicate_and_regressed_seq(self):
        e = make()
        e.feed_data("L", "lp0", 5, "k", 1)
        with self.assertRaises(InputError):
            e.feed_data("L", "lp0", 5, "k", 2)   # duplicate
        e.close()
        e = make()
        e.feed_data("L", "lp0", 5, "k", 1)
        with self.assertRaises(InputError):
            e.feed_data("L", "lp0", 4, "k", 2)   # regression
        e.close()
        # same seq on another partition is fine
        e = make(lp=("lp0", "lp1"))
        e.feed_data("L", "lp0", 5, "k", 1)
        e.feed_data("L", "lp1", 5, "k", 1)
        e.close()

    def test_watermark_regression(self):
        e = make()
        e.feed_watermark("L", "lp0", 10)
        e.feed_watermark("L", "lp0", 10)   # equal is fine
        with self.assertRaises(InputError):
            e.feed_watermark("L", "lp0", 9)
        e.close()

    def test_messages_after_end(self):
        e = make()
        e.feed_end("L", "lp0")
        for msg in (Data("L", "lp0", 1, "k", 1),
                    Watermark("L", "lp0", 1),
                    End("L", "lp0")):
            with self.assertRaises(InputError):
                e.feed(msg)
        e.close()

    def test_unknown_partition(self):
        e = make()
        with self.assertRaises(InputError):
            e.feed_data("L", "nope", 1, "k", 1)
        e.close()

    def test_bad_message_fields(self):
        e = make()
        bad = [Data("X", "lp0", 1, "k", 1),
               Data("L", "", 1, "k", 1),
               Data("L", "lp0", True, "k", 1),
               Data("L", "lp0", 1, "k", 1.5),
               Data("L", "lp0", 1, 123, 1),
               Watermark("L", "lp0", "5"),
               "not a message"]
        for msg in bad:
            with self.assertRaises(InputError, msg=msg):
                e.feed(msg)
        e.close()

    def test_engine_fails_closed(self):
        e = make()
        e.feed_watermark("L", "lp0", 10)
        with self.assertRaises(InputError):
            e.feed_watermark("L", "lp0", 5)    # regression -> failure
        self.assertEqual(e.status, "failed")
        with self.assertRaises(InputError):
            e.feed_watermark("L", "lp0", 5)    # still failing
        with self.assertRaises(InputError):
            e.feed_data("L", "lp0", 1, "k", 1)  # even valid input refused
        self.assertEqual(e.status, "failed")
        e.close()

    def test_config_validation(self):
        with self.assertRaises(ValueError):
            JoinEngine(":memory:", 5, -2, "inner", ["a"], ["b"])
        with self.assertRaises(ValueError):
            JoinEngine(":memory:", -2, 5, "sideways", ["a"], ["b"])
        with self.assertRaises(ValueError):
            JoinEngine(":memory:", -2, 5, "inner", [], ["b"])
        with self.assertRaises(ValueError):
            JoinEngine(":memory:", -2, 5, "inner", ["a", "a"], ["b"])
        with self.assertRaises(ValueError):
            JoinEngine(":memory:", -2, 5, "inner", ["a"], [""])


if __name__ == "__main__":
    unittest.main()
