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



# ---------------------------------------------------------------- dead code (issue 450)
import json  # noqa: E402
import shutil  # noqa: E402
import subprocess  # noqa: E402
import tempfile  # noqa: E402

import match_dead  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "rust_oracle"))
import scip_dead  # noqa: E402


def dead_decl(file, line, name, kind="function", unref="dead", reach="dead", **kw):
    d = {"file": file, "line": line, "name": name, "kind": kind, "subkind": "", "exported": False, "api": False, "unref": unref}
    if kind in ("function", "method"):
        d["reach"] = reach
    d.update(kw)
    return d


def claim(file, line, name, kind="function"):
    return {"key": "%s:%d" % (file, line), "file": file, "kind": kind, "name": name, "qualname": name, "id": line}


def dead_oracle(decls, files=("a.go",), **kw):
    return dict({"files": list(files), "decls": decls, "tests": False}, **kw)


def is_test(lang, path):
    return match.is_test_file(lang, path)


class DeadCodeUniverse(unittest.TestCase):
    def test_u1000_finding_is_matched_to_a_dead_symbols_row(self):
        """staticcheck says `helper` is unused; lidx's dead_symbols returns a symbol at the same file:line: a true claim, and found."""
        o = dead_oracle([dead_decl("a.go", 12, "helper", unref="dead")])
        syms = {7: sym(7, "a.go", 12, "helper")}
        rows = [{"id": 7, "kind": "function"}]
        claims = match_dead.lidx_claims(rows, syms)
        summ, fp, fn, ungr = match_dead.grade_column(o, claims, "go", "unref", is_test)
        self.assertEqual((summ["all"]["tp"], summ["all"]["fp"], summ["all"]["found"], summ["all"]["dead"]), (1, 0, 1, 1))
        self.assertEqual((summ["all"]["precision"], summ["all"]["recall"]), (1.0, 1.0))
        self.assertEqual((fp, fn, ungr), ([], [], []))
        # and a dead declaration lidx does not claim is a missed one
        summ, fp, fn, ungr = match_dead.grade_column(o, [], "go", "unref", is_test)
        self.assertEqual((summ["all"]["recall"], len(fn)), (0.0, 1))

    def test_exported_go_function_is_not_in_the_universe(self):
        """An exported function of an importable package is public API: no verdict, and a lidx claim on it is wrong by construction."""
        pub = dead_decl("a.go", 3, "Public", unref="live", reach="dead", exported=True, api=True)
        internal = dead_decl("internal/b.go", 4, "Helper", unref="live", reach="dead", exported=True, api=False)
        o = dead_oracle([pub, internal], files=("a.go", "internal/b.go"))
        self.assertEqual(match_dead.bucket(pub, "unref", "go"), "public_api")
        self.assertEqual(match_dead.bucket(pub, "reach", "go"), "public_api")
        # U1000 treats every exported identifier as used, so the unreferenced column leaves the internal one out too; RTA judges it
        self.assertEqual(match_dead.bucket(internal, "unref", "go"), "exported_used_by_rule")
        self.assertIsNone(match_dead.bucket(internal, "reach", "go"))
        self.assertEqual(set(match_dead.universe(o, "unref", "go")), set())
        self.assertEqual(set(match_dead.universe(o, "reach", "go")), {"internal/b.go:4"})
        claims = [claim("a.go", 3, "Public"), claim("internal/b.go", 4, "Helper")]
        summ, fp, fn, ungr = match_dead.grade_column(o, claims, "go", "unref", is_test)
        self.assertEqual({f["category"] for f in fp}, {"public_api", "exported_used_by_rule"})
        self.assertEqual((summ["all"]["tp"], summ["all"]["fp"]), (0, 2))
        # an exported-symbol policy drops both claims
        self.assertEqual((summ["no_exported"]["graded"], summ["fp_categories_no_exported"]), (0, {}))
        summ_r, fp_r, _, _ = match_dead.grade_column(o, claims, "go", "reach", is_test)
        self.assertEqual([f["category"] for f in fp_r], ["public_api"])
        self.assertEqual((summ_r["all"]["tp"], summ_r["all"]["fp"]), (1, 1))  # RTA: the internal Helper is unreachable, the claim is right

    def test_generated_file_is_excluded(self):
        gen = dead_decl("x.pb.go", 9, "unused", generated=True)
        o = dead_oracle([gen], files=("x.pb.go",))
        self.assertEqual(match_dead.bucket(gen, "unref", "go"), "generated")
        self.assertEqual(match_dead.universe(o, "unref", "go"), {})
        summ, fp, fn, ungr = match_dead.grade_column(o, [claim("x.pb.go", 9, "unused")], "go", "unref", is_test)
        self.assertEqual((summ["all"]["graded"], summ["dead"], summ["claims_ungradable"]), (0, 0, {"generated": 1}))
        self.assertEqual(ungr[0]["reason"], "generated")

    def test_roots_are_wrong_claims_not_exclusions(self):
        """Go `Test*`, `main` and `init` are live by construction: a claim on them is a wrong claim of its own category."""
        o = dead_oracle([dead_decl("a_test.go", 3, "TestX", unref="live", root="test_root", exported=True), dead_decl("a.go", 4, "init", unref="live", root="main_init")],
                        files=("a_test.go", "a.go"), tests=True)
        claims = [claim("a_test.go", 3, "TestX"), claim("a.go", 4, "init")]
        summ, fp, fn, ungr = match_dead.grade_column(o, claims, "go", "unref", is_test)
        self.assertEqual(summ["fp_categories"], {"test_root": 1, "main_init": 1})
        # `TestX` is exported (an exported-symbol policy removes it), `init` is not
        self.assertEqual(summ["fp_categories_no_exported"], {"main_init": 1})

    def test_claims_outside_the_program_are_named(self):
        o = dead_oracle([dead_decl("a.go", 1, "f")], files=("a.go",))
        claims = [claim("a_test.go", 5, "TestY"), claim("tagged_windows.go", 2, "g"), claim("a.go", 99, "field", kind="variable")]
        summ, _, _, ungr = match_dead.grade_column(o, claims, "go", "unref", is_test)
        self.assertEqual(summ["claims_ungradable"], {"test_file_outside_cell": 1, "file_not_in_program": 1, "not_a_judged_declaration": 1})

    def test_reachable_column_judges_functions_only(self):
        t = dead_decl("a.go", 1, "T", kind="type", subkind="struct", unref="dead")
        self.assertEqual(match_dead.bucket(t, "reach", "go"), "not_a_function")
        self.assertIsNone(match_dead.bucket(t, "unref", "go"))

    def test_withheld_and_unjudged_are_named_buckets(self):
        w = dead_decl("a.rs", 1, "f", unref="", reach="live", withheld="oracle_unresolved_site")
        self.assertEqual(match_dead.bucket(w, "unref", "rust"), "oracle_unresolved_site")
        self.assertEqual(match_dead.bucket(dead_decl("a.rs", 2, "g", unref=""), "unref", "rust"), "no_verdict")

    def test_foreign_impl_methods_and_traits_are_named_buckets(self):
        m = dead_decl("a.rs", 3, "logic_not", kind="method", subkind="trait_impl", foreign_impl=True)
        self.assertEqual(match_dead.bucket(m, "unref", "rust"), "impl_for_foreign_type")
        self.assertEqual(match_dead.bucket(m, "reach", "rust"), "impl_for_foreign_type")
        t = dead_decl("a.rs", 1, "Logic", kind="type", subkind="trait")
        self.assertEqual(match_dead.bucket(t, "unref", "rust"), "trait_declaration")


class RustPublicApi(unittest.TestCase):
    def test_pub_item_of_a_library_crate_is_api_and_of_a_binary_crate_is_kept(self):
        self.assertTrue(scip_dead.is_api(True, "lib"))
        self.assertTrue(scip_dead.is_api(True, "proc_macro"))
        self.assertFalse(scip_dead.is_api(False, "lib"))  # private items of a library are judged
        for k in ("bin", "example", "test", "build", "other"):
            self.assertFalse(scip_dead.is_api(True, k), k)
        self.assertFalse(scip_dead.is_api(True, "lib", root="test_root"))

    def test_crate_kinds_come_from_the_manifests(self):
        with tempfile.TemporaryDirectory() as d:
            def w(rel, text=""):
                p = os.path.join(d, rel)
                os.makedirs(os.path.dirname(p), exist_ok=True)
                with open(p, "w") as fh:
                    fh.write(text)
            w("Cargo.toml", '[package]\nname = "app"\nversion = "0.1.0"\n[[bin]]\nname = "app"\npath = "crates/core/main.rs"\n[workspace]\nmembers = ["crates/lib", "crates/both"]\n')
            w("crates/core/main.rs")
            w("crates/core/flags.rs")
            w("crates/lib/Cargo.toml", '[package]\nname = "lib"\nversion = "0.1.0"\n')
            w("crates/lib/src/lib.rs")
            w("crates/lib/src/deep/mod.rs")
            w("crates/both/Cargo.toml", '[package]\nname = "both"\nversion = "0.1.0"\n')
            w("crates/both/src/lib.rs")
            w("crates/both/src/util.rs")
            w("crates/both/src/main.rs")
            w("crates/both/src/bin/extra.rs")
            w("crates/both/tests/it.rs")
            pk = scip_dead.read_manifests(d)
            kind = lambda p: scip_dead.crate_of(pk, p)
            self.assertEqual(kind("crates/core/main.rs"), ("app", "bin"))
            self.assertEqual(kind("crates/core/flags.rs"), ("app", "bin"))
            self.assertEqual(kind("crates/lib/src/lib.rs"), ("lib", "lib"))
            self.assertEqual(kind("crates/lib/src/deep/mod.rs"), ("lib", "lib"))
            self.assertEqual(kind("crates/both/src/util.rs"), ("both", "lib"))
            self.assertEqual(kind("crates/both/src/main.rs"), ("both", "bin"))
            self.assertEqual(kind("crates/both/src/bin/extra.rs"), ("both", "bin"))
            self.assertEqual(kind("crates/both/tests/it.rs"), ("both", "test"))
            # a `pub` item in each: only the library ones leave the universe
            api = lambda p: scip_dead.is_api(True, kind(p)[1])
            self.assertFalse(api("crates/core/flags.rs"))
            self.assertTrue(api("crates/lib/src/lib.rs"))
            self.assertFalse(api("crates/both/src/main.rs"))


TS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ts_oracle")


@unittest.skipUnless(shutil.which("node") and os.path.isdir(os.path.join(TS, "node_modules", "typescript")),
                     "node or the oracle's typescript dependency is not installed")
class TypeScriptEntryBarrel(unittest.TestCase):
    FILES = {
        "package.json": '{"name": "toy", "main": "./dist/index.js", "types": "./dist/index.d.ts"}',
        "tsconfig.json": '{"compilerOptions": {"strict": true, "target": "es2020", "module": "esnext", "moduleResolution": "bundler"}}',
        "src/index.ts": 'export { exposed } from "./exposed";\nexport * from "./star";\nimport { used } from "./used";\nexport function top() { return used(); }\n',
        "src/exposed.ts": "export function exposed() { return 1; }\n",
        "src/star.ts": "export function viaStar() { return 2; }\nexport class Klass { m() { return 3; } }\n",
        "src/used.ts": "export function used() { return helper(); }\nfunction helper() { return 4; }\nfunction neverCalled() { return 5; }\n",
        "src/orphan.ts": "export function orphanExport() { return orphanTwo(); }\nexport function orphanTwo() { return 1; }\n",
    }

    def run_oracle(self):
        with tempfile.TemporaryDirectory() as d:
            for rel, text in self.FILES.items():
                p = os.path.join(d, rel)
                os.makedirs(os.path.dirname(p), exist_ok=True)
                with open(p, "w") as fh:
                    fh.write(text)
            r = subprocess.run(["node", os.path.join(TS, "dead.mjs"), "--repo", d, "--subdir", "src"], capture_output=True, text=True, check=True)
        return json.loads(r.stdout)

    def test_exports_reachable_from_the_entry_barrel_are_public_api(self):
        out = self.run_oracle()
        by = {(x["file"], x["name"]): x for x in out["decls"]}
        self.assertEqual(out["entry_files"], ["src/index.ts"])
        for k in (("src/exposed.ts", "exposed"), ("src/star.ts", "viaStar"), ("src/star.ts", "Klass"), ("src/star.ts", "m"), ("src/index.ts", "top")):
            self.assertTrue(by[k]["api"], k)
            self.assertEqual(by[k]["unref"], "", k)  # no verdict: other code may use it
        # exported but not reachable from the entry: judged, and dead (only an unreachable sibling mentions it / nothing does)
        self.assertFalse(by[("src/orphan.ts", "orphanExport")]["api"])
        self.assertEqual(by[("src/orphan.ts", "orphanExport")]["unref"], "dead")
        self.assertEqual(by[("src/orphan.ts", "orphanTwo")]["unref"], "live")  # referenced by orphanExport ...
        self.assertEqual(by[("src/orphan.ts", "orphanTwo")]["reach"], "dead")  # ... which nothing reaches
        # the compiler's own unused-locals diagnostic decides the non-exported ones
        self.assertEqual(by[("src/used.ts", "neverCalled")]["unref"], "dead")
        self.assertEqual(by[("src/used.ts", "helper")]["unref"], "live")
        self.assertEqual(by[("src/used.ts", "helper")]["reach"], "live")
# ---------------------------------------------------------------- type hierarchy (issue #449)
import hierarchy  # noqa: E402
import json  # noqa: E402
import subprocess  # noqa: E402
import tempfile  # noqa: E402


def decl(path, line, name):
    return {"file": path, "line": line, "name": name}


def hedge(kind, s, t, tq=None):
    return {"kind": kind, "s": s, "t": t, "tq": tq, "res": "structural", "line": 1, "epath": "a.go"}


def hsym(i, path, line, name, kind, qual=None):
    return sym(i, path, line, name, kind, qual)


class Hierarchy(unittest.TestCase):
    def go_oracle(self, **h):
        base = {"implements": [], "embeds": [], "extends": [], "overrides": [], "external_base": [], "ungradable": [], "excluded": {},
                "types": [decl("a.go", 10, "T")], "ifaces": [decl("a.go", 1, "I")]}
        base.update(h)
        return {"files": ["a.go"], "hierarchy": base}

    def test_go_pointer_receiver_implementation(self):
        """`*T` implements `I` (pointer-receiver methods): the pair is an oracle pair like any other and lidx's edge matches it;
        the method pair comes back through lidx's dispatch pairs."""
        oracle = self.go_oracle(
            implements=[{"sub": decl("a.go", 10, "T"), "base": decl("a.go", 1, "I"), "ptr": True}],
            overrides=[{"sub": decl("a.go", 12, "Do"), "base": decl("a.go", 2, "Do"), "ptr": True}])
        syms = {1: hsym(1, "a.go", 1, "I", "interface"), 2: hsym(2, "a.go", 2, "Do", "method", "I.Do"),
                3: hsym(3, "a.go", 10, "T", "class"), 4: hsym(4, "a.go", 12, "Do", "method", "T.Do")}
        summ, fn, fp, _ = hierarchy.grade_cell("go", oracle, syms, [hedge("IMPLEMENTS", 3, 1)], [(2, 4)])
        r = summ["relations"]
        self.assertEqual((r["implements"]["tp"], r["implements"]["fp"], r["implements"]["recall"]), (1, 0, 1.0))
        self.assertEqual((r["overrides"]["tp"], r["overrides"]["recall"]), (1, 1.0))
        self.assertEqual(fn + fp, [])

    def test_go_embedded_interface_chain(self):
        """A embeds B embeds C: the oracle has A->B and B->C. lidx with only A->B recalls half and is never wrong; a lidx
        A->C edge (transitive, not embedded) is a wrong edge for `extends`."""
        oracle = self.go_oracle(
            extends=[{"sub": decl("a.go", 1, "A"), "base": decl("a.go", 5, "B")}, {"sub": decl("a.go", 5, "B"), "base": decl("a.go", 9, "C")}],
            ifaces=[decl("a.go", 1, "A"), decl("a.go", 5, "B"), decl("a.go", 9, "C")], types=[])
        syms = {1: hsym(1, "a.go", 1, "A", "interface"), 2: hsym(2, "a.go", 5, "B", "interface"), 3: hsym(3, "a.go", 9, "C", "interface")}
        summ, fn, fp, _ = hierarchy.grade_cell("go", oracle, syms, [hedge("EXTENDS", 1, 2)], [])
        e = summ["relations"]["extends"]
        self.assertEqual((e["tp"], e["fp"], e["matched"], e["oracle_pairs"]), (1, 0, 1, 2))
        self.assertEqual([(x["sub"], x["base"], x["category"]) for x in fn], [("a.go:5", "a.go:9", "no_edge")])
        summ, fn, fp, _ = hierarchy.grade_cell("go", oracle, syms, [hedge("EXTENDS", 1, 2), hedge("EXTENDS", 1, 3)], [])
        self.assertEqual(summ["relations"]["extends"]["fp"], 1)
        self.assertEqual(fp[0]["category"], "other")

    def test_go_struct_embedding_is_not_extends(self):
        """Struct embedding is its own relation: an EXTENDS row from a struct is graded as `embeds`, not as interface `extends`."""
        oracle = self.go_oracle(embeds=[{"sub": decl("a.go", 10, "T"), "base": decl("a.go", 20, "U")}],
                                types=[decl("a.go", 10, "T"), decl("a.go", 20, "U")])
        syms = {1: hsym(1, "a.go", 10, "T", "class"), 2: hsym(2, "a.go", 20, "U", "class")}
        summ, fn, fp, _ = hierarchy.grade_cell("go", oracle, syms, [hedge("EXTENDS", 1, 2)], [])
        self.assertEqual(summ["relations"]["embeds"]["tp"], 1)
        self.assertEqual(summ["relations"]["extends"]["graded"], 0)

    def test_ts_interface_extends_interface(self):
        oracle = {"files": ["a.ts"], "hierarchy": {
            "extends": [{"sub": decl("a.ts", 5, "Child"), "base": decl("a.ts", 1, "Parent"), "base_alts": []}],
            "implements": [], "overrides": [], "external_base": [], "structural_implements": [], "ungradable": [], "excluded": {},
            "types": [decl("a.ts", 1, "Parent"), decl("a.ts", 5, "Child")]}}
        syms = {1: hsym(1, "a.ts", 1, "Parent", "interface"), 2: hsym(2, "a.ts", 5, "Child", "interface")}
        summ, fn, fp, _ = hierarchy.grade_cell("ts", oracle, syms, [hedge("EXTENDS", 2, 1)], [])
        self.assertEqual((summ["relations"]["extends"]["tp"], summ["relations"]["extends"]["recall"]), (1, 1.0))
        # the reverse edge is wrong and says so
        summ, fn, fp, _ = hierarchy.grade_cell("ts", oracle, syms, [hedge("EXTENDS", 1, 2)], [])
        self.assertEqual((summ["relations"]["extends"]["fp"], fp[0]["category"]), (1, "reverse_direction"))

    def test_external_base_lands_in_its_bucket(self):
        """`class E extends Error`: the base is outside the repository, so it is never a precision or recall pair; it is
        judged for existence only (lidx has any EXTENDS row from E naming it)."""
        oracle = {"files": ["a.ts"], "hierarchy": {
            "extends": [], "implements": [], "overrides": [], "structural_implements": [], "ungradable": [], "excluded": {},
            "external_base": [{"sub": decl("a.ts", 3, "E"), "base": "Error", "base_kind": "external", "relation": "extends"}],
            "types": [decl("a.ts", 3, "E")]}}
        syms = {1: hsym(1, "a.ts", 3, "E", "class")}
        have = hierarchy.grade_cell("ts", oracle, syms, [hedge("EXTENDS", 1, None, "Error")], [])[0]
        self.assertEqual((have["external_base"]["oracle"], have["external_base"]["found"]), (1, 1))
        self.assertEqual(have["relations"]["extends"]["oracle_pairs"], 0)
        none, fn, _, _ = hierarchy.grade_cell("ts", oracle, syms, [], [])
        self.assertEqual((none["external_base"]["found"], fn[0]["bucket"]), (0, "external_base"))
        self.assertTrue(hierarchy.ext_match("Display::fmt", "ext:std::fmt::Display::fmt"))
        self.assertFalse(hierarchy.ext_match("Display", "ext:std::default::Default"))

    def test_structural_only_edge_is_reported_apart(self):
        oracle = {"files": ["a.ts"], "hierarchy": {
            "extends": [], "implements": [], "overrides": [], "external_base": [], "ungradable": [], "excluded": {},
            "structural_implements": [{"sub": decl("a.ts", 5, "C"), "base": decl("a.ts", 1, "I")}],
            "types": [decl("a.ts", 1, "I"), decl("a.ts", 5, "C")]}}
        syms = {1: hsym(1, "a.ts", 1, "I", "interface"), 2: hsym(2, "a.ts", 5, "C", "class")}
        summ, fn, fp, _ = hierarchy.grade_cell("ts", oracle, syms, [hedge("IMPLEMENTS", 2, 1)], [])
        r = summ["relations"]["implements"]
        self.assertEqual((r["fp"], r["fp_structural_only"], fp[0]["category"]), (1, 1, "structural_only"))

    def test_rust_blanket_impl_is_excluded_and_external_trait_is_bucketed(self):
        """Through a synthetic SCIP index: `impl Shape for Circle` is an implements pair with an override, `impl<T> Shape for T`
        has no in-repo type and is left out as `blanket_impl`, and `impl Display for Circle` is an external base."""
        with tempfile.TemporaryDirectory() as td:
            src = ("pub trait Shape {\n    fn area(&self) -> f64;\n}\npub struct Circle;\n"
                   "impl Shape for Circle {\n    fn area(&self) -> f64 { 1.0 }\n}\n"
                   "impl<T> Shape for T {\n    fn area(&self) -> f64 { 0.0 }\n}\n"
                   "impl Display for Circle {\n    fn fmt(&self) {}\n}\n")
            os.makedirs(os.path.join(td, "src"))
            with open(os.path.join(td, "src", "lib.rs"), "w") as f:
                f.write(src)
            P = "rust-analyzer cargo demo 0.1.0 "
            shape, circle = P + "Shape#", P + "Circle#"
            disp = "rust-analyzer cargo core 0.0.0 fmt/Display#"
            occ = [  # (symbol, line0, col, end col, role)
                (shape, 0, 10, 15, 1), (P + "Shape#area().", 1, 7, 11, 1), (circle, 3, 11, 17, 1),
                (shape, 4, 5, 10, 0), (circle, 4, 15, 21, 0), (P + "impl#[Circle][Shape]area().", 5, 7, 11, 1),
                (shape, 7, 8, 13, 0), (P + "impl#[T][Shape]area().", 8, 7, 11, 1),
                (disp, 10, 5, 12, 0), (circle, 10, 17, 23, 0), (P + "impl#[Circle][Display]fmt().", 11, 7, 10, 1),
            ]
            kinds = {shape: 53, circle: 49}
            idx = _scip_index("src/lib.rs", occ, kinds)
            scip = os.path.join(td, "x.scip")
            with open(scip, "wb") as f:
                f.write(idx)
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, "-I", os.path.join(here, "rust_oracle", "scip_hierarchy.py"), "--repo", td, "--scip", scip],
                                 capture_output=True, text=True, check=True).stdout
            h = json.loads(out)
        self.assertEqual([(p["sub"]["name"], p["base"]["name"]) for p in h["implements"]], [("Circle", "Shape")])
        self.assertEqual([(p["sub"]["line"], p["base"]["line"]) for p in h["overrides"]], [(6, 2)])
        self.assertEqual(h["excluded"].get("blanket_impl"), 1)
        self.assertEqual({(x["base"], x["relation"]) for x in h["external_base"]}, {("Display", "implements"), ("Display::fmt", "overrides")})
        self.assertEqual([t["name"] for t in h["types"]], ["Circle"])


def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _field(num, wire, payload):
    tag = _varint((num << 3) | wire)
    return tag + (_varint(len(payload)) + payload if wire == 2 else payload)


def _scip_index(path, occs, kinds):
    """A minimal SCIP Index (documents only) in protobuf wire format, enough for scip_calls.fields()."""
    doc = _field(1, 2, path.encode())
    for sym, line, c0, c1, role in occs:
        rng = b"".join(_varint(x) for x in (line, c0, c1))
        o = _field(1, 2, rng) + _field(2, 2, sym.encode()) + (_field(3, 0, _varint(role)) if role else b"")
        doc += _field(2, 2, o)
    for sym, kind in kinds.items():
        doc += _field(3, 2, _field(1, 2, sym.encode()) + _field(5, 0, _varint(kind)))
    return _field(2, 2, doc)


if __name__ == "__main__":
    unittest.main()
