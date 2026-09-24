"""本地测试入口：运行全部用例，并在日志中打印关键输入与判定依据。

用法：python run_tests.py            # 全部测试，含判定日志
      python run_tests.py -v 2       # 重复 2 轮，验证可重复性
"""

from __future__ import annotations

import logging
import sys
import unittest


def main() -> int:
    repeats = 1
    if "-v" in sys.argv:
        idx = sys.argv.index("-v")
        repeats = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 3

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    loader = unittest.TestLoader()
    runner = unittest.TextTestRunner(verbosity=2)

    def build_suite() -> unittest.TestSuite:
        suite = unittest.TestSuite()
        for module_name in ("tests.test_manager", "tests.test_api"):
            suite.addTests(loader.loadTestsFromName(module_name))
        return suite

    for i in range(repeats):
        print(f"\n===== round {i + 1}/{repeats} =====", file=sys.stderr)
        outcome = runner.run(build_suite())
        if not outcome.wasSuccessful():
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
