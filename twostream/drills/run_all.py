"""Run every drill in a subprocess, then the unit test suite."""

import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DRILLS = [
    "drills.drill_main",
    "drills.drill_two_matches",
    "drills.drill_reverse",
    "drills.drill_barrier",
    "drills.drill_interleave",
    "drills.drill_hotkey",
    "drills.drill_recover",
    "drills.drill_deliver",
]


def main() -> int:
    failed = []
    for mod in DRILLS:
        print(f"===== {mod} =====", flush=True)
        rc = subprocess.call([sys.executable, "-m", mod], cwd=ROOT)
        if rc != 0:
            failed.append(mod)
        print()
    print("===== unit tests =====", flush=True)
    rc = subprocess.call([sys.executable, "-m", "unittest", "discover",
                          "-s", "tests", "-v"], cwd=ROOT)
    if rc != 0:
        failed.append("unittest")
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    print("ALL DRILLS AND TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
