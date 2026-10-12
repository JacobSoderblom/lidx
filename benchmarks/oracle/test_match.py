"""Unit tests for the matcher. Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import match  # noqa: E402


def sym(i, path, line, name, kind="function", qual=None):
    return {"file": path, "line": line, "name": name, "qualname": qual or name, "kind": kind, "key": "%s:%d" % (path, line)}


def edge(s, t, line, res="exact", kind="CALLS"):
    return {"kind": kind, "s": s, "t": t, "target_qualname": None, "resolution_kind": res, "line": line, "end_line": line, "epath": "a.go"}


ORACLE = {
    "files": ["a.go"],
    "callers": [{"file": "a.go", "line": 1, "name": "f"}, {"file": "a.go", "line": 5, "name": "g"}],
    "edges": [{"caller": {"file": "a.go", "line": 1, "name": "f"}, "callee": {"file": "a.go", "line": 5, "name": "g"}, "sites": ["a.go:2"]}],
}


class Wilson(unittest.TestCase):
    def test_bounds(self):
        lo, hi = match.wilson(0, 40)
        self.assertEqual(lo, 0.0)
        self.assertAlmostEqual(hi, 0.0877, places=3)
        self.assertEqual(match.wilson(0, 0), [None, None])
        lo, hi = match.wilson(50, 100)
        self.assertTrue(lo < 0.5 < hi)


class Grade(unittest.TestCase):
    def run_grade(self, syms, edges):
        src = match.Sources("/nonexistent")
        return match.grade(ORACLE, syms, edges, "go", src)

    def test_match_and_fp(self):
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "a.go", 9, "h")}
        summ, fn, fp, gap, *_ = self.run_grade(syms, [edge(1, 2, 2), edge(1, 3, 3, "bare_name")])
        self.assertEqual((summ["tp"], summ["fp"]), (1, 1))
        self.assertEqual(summ["recall"], 1.0)
        self.assertEqual(summ["by_resolution_kind"]["bare_name"]["tp"], 0)

    def test_wrong_edge_is_fp_only_when_callee_file_covered(self):
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "a.go", 9, "h")}
        summ, fn, fp, gap, *_ = self.run_grade(syms, [edge(1, 3, 3, "bare_name")])
        self.assertEqual(summ["fp"], 1)
        self.assertEqual(summ["recall"], 0.0)
        self.assertEqual(fp[0]["resolution_kind"], "bare_name")

    def test_uncovered_caller_is_ungradable(self):
        syms = {1: sym(1, "b.go", 1, "x"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "a.go", 1, "f")}
        summ, fn, fp, gap, *_ = self.run_grade(syms, [edge(1, 2, 2)])
        self.assertEqual(summ["graded"], 0)
        self.assertEqual(summ["ungradable"], {"caller_file_not_covered": 1})

    def test_unindexed_caller_is_capture_gap(self):
        syms = {2: sym(2, "a.go", 5, "g")}
        summ, fn, fp, gap, *_ = self.run_grade(syms, [])
        self.assertEqual(summ["capture_gap_edges"], 1)
        self.assertEqual(gap[0]["reason"], "generated_or_unusual_decl")
        self.assertIsNone(summ["recall"])

    def test_dispatch_edges_do_not_enter_headline_recall(self):
        oracle = dict(ORACLE, edges=[dict(ORACLE["edges"][0], dispatch=True)])
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g")}
        summ, fn, fp, gap, *_ = match.grade(oracle, syms, [], "go", match.Sources("/nonexistent"))
        self.assertIsNone(summ["recall"])
        self.assertEqual(summ["recall_strict"], 0.0)
        self.assertEqual(summ["dispatch_edges"], 1)

    def test_production_edge_into_a_test_file_is_a_wrong_edge_not_an_exclusion(self):
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "a_test.go", 4, "h")}
        summ, fn, fp, gap, *_ = self.run_grade(syms, [edge(1, 3, 3, "bare_name")])
        self.assertEqual(summ["fp"], 1)
        self.assertEqual(fp[0]["category"], "calls_test_code")
        self.assertEqual(summ["ungradable"], {})

    def test_unresolved_call_withholds_only_a_callee_of_the_same_name(self):
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "a.go", 9, "h")}
        oracle = dict(ORACLE, unresolved_calls=[{"site": "a.go:3", "name": "h"}])
        summ, *_ = match.grade(oracle, syms, [edge(1, 3, 3)], "go", match.Sources("/nonexistent"))
        self.assertEqual(summ["ungradable"], {"oracle_unresolved_site": 1})
        oracle = dict(ORACLE, unresolved_calls=[{"site": "a.go:3", "name": "other"}])
        summ, *_ = match.grade(oracle, syms, [edge(1, 3, 3)], "go", match.Sources("/nonexistent"))
        self.assertEqual((summ["graded"], summ["fp"]), (1, 1))

    def test_comparable_recall_charges_capture_gaps_and_dispatch_misses(self):
        oracle = dict(ORACLE, edges=[ORACLE["edges"][0], dict(ORACLE["edges"][0], callee={"file": "a.go", "line": 99, "name": "z"})])
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g")}
        summ, *_ = match.grade(oracle, syms, [edge(1, 2, 2)], "go", match.Sources("/nonexistent"))
        self.assertEqual(summ["recall_comparable"], 0.5)  # the second edge's callee is not indexed: charged, not dropped
        self.assertEqual(summ["capture_gap_edges"], 1)

    def test_exhaustive_oracle_judges_every_caller(self):
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "a.go", 9, "h"), 4: sym(4, "a.go", 12, "k")}
        edges = [edge(4, 3, 13, "bare_name")]
        summ, *_ = self.run_grade(syms, edges)  # k is not a judged caller of a reachability oracle
        self.assertEqual(summ["ungradable"], {"caller_not_judged": 1})
        summ, *_ = match.grade(dict(ORACLE, exhaustive_callers=True), syms, edges, "go", match.Sources("/nonexistent"))
        self.assertEqual((summ["graded"], summ["fp"]), (1, 1))

    def test_uncovered_callee_at_a_known_external_site_is_a_wrong_edge(self):
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "b.go", 4, "h")}
        oracle = dict(ORACLE, ext_sites=["a.go:3"])
        summ, *_ = match.grade(oracle, syms, [edge(1, 3, 3, "bare_name")], "go", match.Sources("/nonexistent"))
        self.assertEqual((summ["graded"], summ["fp"]), (1, 1))

    def test_merged_declarations_compare_equal(self):
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g", qual="g"), 3: sym(3, "a.go", 7, "g", kind="interface", qual="g")}
        summ, fn, fp, gap, *_ = self.run_grade(syms, [edge(1, 3, 2)])
        self.assertEqual(summ["tp"], 1)


if __name__ == "__main__":
    unittest.main()
