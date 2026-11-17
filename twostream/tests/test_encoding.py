"""Order-preserving big-integer encoding tests."""

import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from interval_join import enc_int


class TestEncoding(unittest.TestCase):
    def test_known_vectors(self):
        vals = [-(10 ** 40), -(2 ** 70), -1001, -1000, -11, -10, -2, -1,
                0, 1, 2, 9, 10, 11, 999, 1000, 2 ** 63 - 1, 2 ** 63,
                2 ** 70, 10 ** 40]
        self.assertEqual(vals, sorted(vals))  # fixture must be sorted
        encs = [enc_int(v) for v in vals]
        self.assertEqual(encs, sorted(encs))

    def test_pairwise(self):
        pairs = [(-1, 0), (0, 1), (-2, -1), (-10, -9), (9, 10),
                 (-(2 ** 64), -(2 ** 63)), (2 ** 63, 2 ** 64),
                 (-(10 ** 30), -(10 ** 29)), (10 ** 29, 10 ** 30),
                 (-1, 1), (0, 10 ** 50), (-(10 ** 50), 0)]
        for a, b in pairs:
            self.assertLess(a, b)
            self.assertLess(enc_int(a), enc_int(b), (a, b))

    def test_random_sorted(self):
        rng = random.Random(42)
        vals = []
        for _ in range(2000):
            bits = rng.choice([1, 7, 31, 32, 63, 64, 65, 100, 200])
            v = rng.getrandbits(bits)
            vals.append(v if rng.random() < 0.5 else -v)
        encs = [enc_int(v) for v in vals]
        self.assertEqual([v for _, v in sorted(zip(encs, vals))], sorted(vals))

    def test_rejects_non_int(self):
        with self.assertRaises(TypeError):
            enc_int(True)
        with self.assertRaises(TypeError):
            enc_int("5")


if __name__ == "__main__":
    unittest.main()
