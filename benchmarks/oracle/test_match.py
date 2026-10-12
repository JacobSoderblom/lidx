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

    def test_edges_with_null_evidence_lines_grade_without_crashing(self):
        # edges.evidence_start_line / evidence_end_line are NULL for edges the extractor records without a location
        syms = {1: sym(1, "a.go", 1, "f"), 2: sym(2, "a.go", 5, "g"), 3: sym(3, "a.go", 9, "h")}
        summ, fn, fp, gap, *_ = self.run_grade(syms, [edge(1, 2, None), edge(1, 3, None, "bare_name")])
        self.assertEqual((summ["tp"], summ["fp"]), (1, 1))
        self.assertEqual(summ["recall"], 1.0)
        self.assertEqual(fp[0]["evidence_lines"], [])
        summ, fn, *_ = self.run_grade(syms, [edge(1, 3, None)])
        self.assertEqual((summ["tp"], summ["fp"], summ["recall"]), (0, 1, 0.0))
        self.assertEqual(len(fn), 1)  # the oracle edge f -> g is missed, and the miss is reported rather than dropped
        self.assertEqual((fn[0]["caller"]["name"], fn[0]["callee"]["name"]), ("f", "g"))

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


def ts_edge(caller_line, callee_line, name="g", **kw):
    e = {"caller": {"file": "a.ts", "line": caller_line, "name": "c%d" % caller_line},
         "callee": {"file": "a.ts", "line": callee_line, "name": name}, "sites": ["a.ts:%d" % (caller_line + 1)]}
    e.update(kw)
    return e


def ts_oracle(edges):
    callers = [e["caller"] for e in edges]
    return {"files": ["a.ts"], "callers": callers, "edges": edges, "exhaustive_callers": True}


class RepowiseTypeScriptScope(unittest.TestCase):
    """repowise's tsc oracle emits no module-level call and withholds abstract targets (and the function calling them)."""

    def setUp(self):
        self.syms = {1: sym(1, "a.ts", 0, "<module>", kind="module"), 2: sym(2, "a.ts", 1, "f"), 3: sym(3, "a.ts", 5, "g"),
                     4: sym(4, "a.ts", 9, "h"), 5: sym(5, "a.ts", 20, "k"), 6: sym(6, "a.ts", 30, "z")}
        self.src = match.Sources("/nonexistent")

    def edges_of(self, *pairs):
        return [dict(edge(s, t, 3), epath="a.ts") for s, t in pairs]

    def test_module_level_and_abstract_edges_leave_the_denominator(self):
        o = ts_oracle([ts_edge(1, 5), ts_edge(0, 5, name="g"), ts_edge(9, 20, dispatch=True), ts_edge(9, 5)])
        scoped, withheld = match.repowise_ts_scope(o)
        self.assertEqual(len(scoped["edges"]), 2)
        self.assertEqual(len(o["edges"]), 4)  # the input is untouched
        self.assertEqual(withheld, {"a.ts:0", "a.ts:9"})
        strict, *_ = match.grade(o, self.syms, self.edges_of((2, 3)), "ts", self.src)
        comp, *_ = match.grade(scoped, self.syms, self.edges_of((2, 3)), "ts", self.src, withheld_callers=withheld)
        self.assertEqual(strict["oracle_edges"], 4)
        self.assertEqual(comp["oracle_edges"], 2)
        self.assertEqual(comp["recall_comparable"], 0.5)

    def test_capture_gaps_stay_in_the_denominator(self):
        o = ts_oracle([ts_edge(1, 5), ts_edge(1, 77, name="nested")])  # the second callee is not a symbol lidx indexes
        scoped, _ = match.repowise_ts_scope(o)
        comp, *_ = match.grade(scoped, self.syms, self.edges_of((2, 3)), "ts", self.src)
        self.assertEqual((comp["oracle_edges"], comp["capture_gap_edges"]), (2, 1))
        self.assertEqual(comp["recall_comparable"], 0.5)

    def test_unmatched_edge_from_a_withheld_caller_is_ungradable_but_a_match_still_counts(self):
        o = ts_oracle([ts_edge(9, 20, dispatch=True), ts_edge(9, 5)])
        scoped, withheld = match.repowise_ts_scope(o)
        comp, *_ = match.grade(scoped, self.syms, self.edges_of((4, 3), (4, 6)), "ts", self.src, withheld_callers=withheld)
        self.assertEqual((comp["tp"], comp["fp"]), (1, 0))  # h -> g matches the static oracle edge
        self.assertEqual(comp["ungradable"], {"caller_withheld": 1})  # h -> z does not, but h is not judged
        strict, *_ = match.grade(o, self.syms, self.edges_of((4, 3), (4, 6)), "ts", self.src)
        self.assertEqual((strict["tp"], strict["fp"]), (1, 1))  # the stricter grading counts it wrong

    def test_module_symbol_edges_are_not_judged(self):
        o = ts_oracle([ts_edge(0, 5)])
        scoped, withheld = match.repowise_ts_scope(o)
        comp, *_ = match.grade(scoped, self.syms, self.edges_of((1, 5)), "ts", self.src, withheld_callers=withheld)
        self.assertEqual(comp["graded"], 0)

    def test_nested_call_withholds_enclosing_functions_but_not_the_module(self):
        o = ts_oracle([ts_edge(9, 20, dispatch=True, caller_chain=["a.ts:1", "a.ts:0"])])
        _, withheld = match.repowise_ts_scope(o)
        self.assertEqual(withheld, {"a.ts:9", "a.ts:1"})


class GoInterfaceDispatch(unittest.TestCase):
    """A lidx edge to a Go interface method is graded against the interface methods the oracle's dynamic sites invoke."""

    ORACLE = {
        "files": ["a.go"],
        "callers": [{"file": "a.go", "line": 1, "name": "use"}, {"file": "a.go", "line": 20, "name": "Do"}],
        "edges": [{"caller": {"file": "a.go", "line": 1, "name": "use"}, "callee": {"file": "a.go", "line": 20, "name": "Do"},
                   "sites": ["a.go:3"], "dispatch": True, "callee_alts": ["a.go:10"]}],
    }
    SYMS = {
        1: sym(1, "a.go", 1, "use", qual="a.use"),
        2: sym(2, "a.go", 10, "Do", "method", "a.Doer.Do"),
        3: sym(3, "a.go", 20, "Do", "method", "a.Impl.Do"),
        4: sym(4, "a.go", 30, "Do", "method", "a.Other.Do"),
    }

    def grade(self, edges, pairs):
        return match.grade(self.ORACLE, self.SYMS, edges, "go", match.Sources("/nonexistent"), dispatch_pairs=pairs)[0]

    def test_interface_edge_is_true_and_covers_a_known_implementation(self):
        summ = self.grade([edge(1, 2, 3, "receiver_type")], {("a.go:10", "a.go:20")})
        self.assertEqual((summ["tp"], summ["fp"]), (1, 0))
        self.assertEqual(summ["dispatch_matched"], 1)

    def test_an_implementation_lidx_does_not_link_stays_missed(self):
        summ = self.grade([edge(1, 2, 3, "receiver_type")], set())
        self.assertEqual((summ["tp"], summ["fp"]), (1, 0))
        self.assertEqual(summ["dispatch_matched"], 0)

    def test_interface_edge_without_an_oracle_site_is_a_wrong_edge(self):
        summ = self.grade([edge(1, 4, 3, "receiver_type")], {("a.go:30", "a.go:20")})
        self.assertEqual((summ["tp"], summ["fp"]), (0, 1))
        self.assertEqual(summ["dispatch_matched"], 0)

    def test_the_concrete_edge_still_matches_directly(self):
        summ = self.grade([edge(1, 3, 3, "receiver_type")], set())
        self.assertEqual((summ["tp"], summ["fp"]), (1, 0))
        self.assertEqual(summ["dispatch_matched"], 1)

    def test_interface_call_with_no_in_repo_target_is_a_true_edge(self):
        oracle = {"files": ["a.go"], "callers": [{"file": "a.go", "line": 1, "name": "use"}], "edges": [],
                  "iface_calls": [{"caller": {"file": "a.go", "line": 1, "name": "use"}, "method": "a.go:10"}]}
        syms = dict(self.SYMS)
        summ = match.grade(oracle, syms, [edge(1, 2, 3, "receiver_type")], "go", match.Sources("/nonexistent"), dispatch_pairs=set())[0]
        self.assertEqual((summ["tp"], summ["fp"]), (1, 0))
        # an interface method the caller never invokes stays wrong
        summ = match.grade(oracle, syms, [edge(1, 4, 3, "receiver_type")], "go", match.Sources("/nonexistent"), dispatch_pairs=set())[0]
        self.assertEqual((summ["tp"], summ["fp"]), (0, 1))

    def test_repowise_scoring_gives_no_fan_out_credit(self):
        """One lidx edge to `Doer.Do` covers two RTA edges (two implementations) under the lidx-native scoring only."""
        oracle = {
            "files": ["a.go"],
            "callers": [{"file": "a.go", "line": 1, "name": "use"}, {"file": "a.go", "line": 20, "name": "Do"},
                        {"file": "a.go", "line": 30, "name": "Do"}],
            "edges": [{"caller": {"file": "a.go", "line": 1, "name": "use"}, "callee": {"file": "a.go", "line": c, "name": "Do"},
                       "sites": ["a.go:3"], "dispatch": True, "callee_alts": ["a.go:10"]} for c in (20, 30)],
            "iface_calls": [{"caller": {"file": "a.go", "line": 1, "name": "use"}, "method": "a.go:10"}],
        }
        syms = dict(self.SYMS)
        edges = [edge(1, 2, 3, "receiver_type")]
        pairs = {("a.go:10", "a.go:20"), ("a.go:10", "a.go:30")}
        src = match.Sources("/nonexistent")
        native = match.grade(oracle, syms, edges, "go", src, dispatch_pairs=pairs)[0]
        rw = match.grade(match.repowise_go_scope(oracle), syms, edges, "go", src)[0]
        self.assertEqual((native["tp"], native["fp"], native["dispatch_matched"]), (1, 0, 2))
        self.assertEqual(native["recall_comparable"], 1.0)
        # repowise's rules: the interface edge is contradicted (RTA names the implementations) and covers nothing
        self.assertEqual((rw["tp"], rw["fp"], rw["dispatch_matched"]), (0, 1, 0))
        self.assertEqual(rw["recall_comparable"], 0.0)
        self.assertEqual(rw["precision"], 0.0)
        # a concrete edge scores the same under both
        both = [edge(1, 3, 3, "receiver_type")]
        self.assertEqual(match.grade(oracle, syms, both, "go", src, dispatch_pairs=pairs)[0]["recall_comparable"], 0.5)
        self.assertEqual(match.grade(match.repowise_go_scope(oracle), syms, both, "go", src)[0]["recall_comparable"], 0.5)


if __name__ == "__main__":
    unittest.main()
