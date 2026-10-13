"""Tests for the TypeScript `super(..)` statistic of the report.
Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run  # noqa: E402

SOURCE = """class Base {}
class Child extends Base {
  constructor() {
    super();
  }
}
class Spaced extends Base {
  constructor() {
    super (1);
  }
}
function pick(inst: any) {
  const _super = inst.parse;
  return _super(1, 2);
}
const $super = () => 1;
$super();
"""


def edge(line, callee_line):
    return {
        "caller": {"file": "a.ts", "line": 1, "name": "c"},
        "callee": {"file": "a.ts", "line": callee_line, "name": "t"},
        "sites": ["a.ts:%d" % line],
    }


class SuperEdgeStats(unittest.TestCase):
    def stats(self, edges, missed=()):
        with tempfile.TemporaryDirectory() as repo:
            with open(os.path.join(repo, "a.ts"), "w") as f:
                f.write(SOURCE)
            return run.super_edge_stats({"edges": edges}, repo, list(missed))

    def test_keyword_calls_are_counted_including_a_space_before_the_parenthesis(self):
        self.assertEqual(self.stats([edge(4, 1), edge(9, 1)]), {"oracle": 2, "matched": 2})

    def test_locals_that_end_in_super_are_ordinary_calls(self):
        self.assertEqual(self.stats([edge(14, 12), edge(17, 16)]), {"oracle": 0, "matched": 0})

    def test_a_missed_super_edge_is_counted_as_lost(self):
        missed = [{"caller": {"file": "a.ts", "line": 1}, "callee": {"file": "a.ts", "line": 1}}]
        self.assertEqual(self.stats([edge(4, 1), edge(9, 2)], missed), {"oracle": 2, "matched": 1})


if __name__ == "__main__":
    unittest.main()
