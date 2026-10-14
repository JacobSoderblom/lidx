"""Unit tests for the matcher. Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import match  # noqa: E402
import refs  # noqa: E402


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
import scip_calls as sc  # noqa: E402


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
        # the public-API claim is wrong by construction; the internal one is a declaration U1000 has no verdict on
        # (its `live` is by rule 1.2), so the claim is ungradable in this column (issue #503), not wrong
        self.assertEqual([f["category"] for f in fp], ["public_api"])
        self.assertEqual([u["reason"] for u in ungr], ["exported_used_by_rule"])
        self.assertEqual((summ["all"]["tp"], summ["all"]["fp"]), (0, 1))
        self.assertEqual(summ["claims_ungradable"], {"exported_used_by_rule": 1})
        # an exported-symbol policy drops the public-API claim
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


# ---------------------------------------------------------------- value references (issue #452)
def rsym(path, line, name, kind="function", qual=None):
    return sym(0, path, line, name, kind, qual)


def rref(caller, callee, forms=("ident",), ctxs=("call_argument",), called=False, **kw):
    r = {"caller": {"file": caller[0], "line": caller[1], "name": caller[2]}, "callee": {"file": callee[0], "line": callee[1], "name": callee[2]},
         "sites": ["%s:%d" % (caller[0], kw.pop("site", caller[1] + 1))], "forms": list(forms), "ctxs": list(ctxs)}
    if called:
        r["called"] = True
    r.update(kw)
    return r


def roracle(files, refs_, edges=(), **kw):
    o = {"files": list(files), "callers": [], "edges": list(edges), "refs": list(refs_), "ref_callers": [], "exhaustive_callers": True}
    o.update(kw)
    return o


def redge(s, t, line, kind="REFERENCES", res="exact", path="a.go"):
    return {"kind": kind, "s": s, "t": t, "target_qualname": None, "resolution_kind": res, "line": line, "end_line": line, "epath": path}


class ValueRefs(unittest.TestCase):
    def grade(self, oracle, syms, edges, lang="go"):
        return refs.grade_refs(oracle, syms, edges, lang, match.Sources("/nonexistent"))

    def test_go_struct_literal_field_handler(self):
        """`cmd := &Command{RunE: run}` inside main: the compiler reports run as a reference of main (ctx struct_field)."""
        oracle = roracle(["a.go"], [rref(("a.go", 10, "main"), ("a.go", 3, "run"), ctxs=["struct_field"], site=11)], ref_callers=[{"file": "a.go", "line": 10, "name": "main"}])
        syms = {1: rsym("a.go", 10, "main"), 2: rsym("a.go", 3, "run")}
        summ, fn, fp, gap, ungr, _ = self.grade(oracle, syms, [redge(1, 2, 11)])
        self.assertEqual((summ["tp"], summ["fp"], summ["matched_oracle_refs"], summ["recall"], summ["precision"]), (1, 0, 1, 1.0, 1.0))
        summ, fn, *_ = self.grade(oracle, syms, [])
        self.assertEqual((summ["recall"], fn[0]["category"], fn[0]["ctxs"]), (0.0, "no_edge", ["struct_field"]))

    def test_go_method_value_passed_as_an_argument(self):
        """`register(s.handle)`: lidx's REFERENCES row to the method, resolved by exact qualname, is a true reference."""
        oracle = roracle(["a.go"], [rref(("a.go", 14, "wire"), ("a.go", 10, "handle"), forms=["method_value"], site=15)], ref_callers=[{"file": "a.go", "line": 14, "name": "wire"}])
        syms = {1: rsym("a.go", 14, "wire", "method", "Server.wire"), 2: rsym("a.go", 10, "handle", "method", "Server.handle")}
        summ, fn, fp, *_ = self.grade(oracle, syms, [redge(1, 2, 15)])
        self.assertEqual((summ["tp"], summ["recall"]), (1, 1.0))
        self.assertEqual(summ["by_resolution_kind"]["exact"]["tp"], 1)

    def test_ts_object_literal_callback_at_module_level(self):
        """`export const handlers = { onClick: handle }` has no enclosing function: the caller is the module-level const."""
        r = rref(("a.ts", 0, "<module>"), ("a.ts", 15, "handle"), ctxs=["object_property"], site=17, caller_alts=["a.ts:17"], caller_chain=[])
        oracle = roracle(["a.ts"], [r], callers=[{"file": "a.ts", "line": 0, "name": "<module>"}, {"file": "a.ts", "line": 17, "name": "handlers"}])
        syms = {1: rsym("a.ts", 17, "handlers", "const"), 2: rsym("a.ts", 15, "handle")}
        summ, *_ = self.grade(oracle, syms, [redge(1, 2, 17, path="a.ts")], "ts")
        self.assertEqual((summ["tp"], summ["recall"]), (1, 1.0))

    def test_rust_map_argument(self):
        """`[1].iter().map(helper)` inside other(): a reference; lidx's USES row to a function counts, a USES row to a struct does not."""
        oracle = roracle(["src/lib.rs"], [rref(("src/lib.rs", 5, "other"), ("src/lib.rs", 2, "helper"), forms=["ident"], ctxs=["argument_or_element"], site=6)],
                         callers=[{"file": "src/lib.rs", "line": 5, "name": "other"}])
        syms = {1: rsym("src/lib.rs", 5, "other"), 2: rsym("src/lib.rs", 2, "helper"), 3: rsym("src/lib.rs", 8, "Opts", "struct")}
        edges = [redge(1, 2, 6, "USES", path="src/lib.rs"), redge(1, 3, 6, "USES", path="src/lib.rs")]
        summ, fn, fp, *_ = self.grade(oracle, syms, edges, "rust")
        self.assertEqual((summ["tp"], summ["fp"], summ["recall"], summ["lidx_rows"]), (1, 0, 1.0, {"USES": 1}))

    def test_a_pair_with_both_a_call_and_a_reference_is_removed_on_both_sides(self):
        """f calls g and also passes g on: the compiler counts a call, lidx keeps a CALLS edge. Neither side holds a value reference."""
        r = rref(("a.go", 1, "f"), ("a.go", 5, "g"), called=True)
        oracle = roracle(["a.go"], [r], ref_callers=[{"file": "a.go", "line": 1, "name": "f"}])
        syms = {1: rsym("a.go", 1, "f"), 2: rsym("a.go", 5, "g")}
        summ, fn, fp, gap, ungr, _ = self.grade(oracle, syms, [redge(1, 2, 2, "CALLS"), redge(1, 2, 3)])
        self.assertEqual((summ["oracle_refs_called_removed"], summ["lidx_pairs_with_call_dropped"], summ["graded"], summ["oracle_refs_caller_and_callee_indexed"]), (1, 1, 0, 0))
        self.assertEqual((fn, fp, ungr), ([], [], []))
        self.assertIsNone(summ["precision"])

    def test_call_wins_when_only_one_side_keeps_the_reference(self):
        r = rref(("a.go", 1, "f"), ("a.go", 5, "g"), called=True)
        oracle = roracle(["a.go"], [r], ref_callers=[{"file": "a.go", "line": 1, "name": "f"}])
        syms = {1: rsym("a.go", 1, "f"), 2: rsym("a.go", 5, "g")}
        summ, fn, fp, gap, ungr, _ = self.grade(oracle, syms, [redge(1, 2, 3)])  # lidx lost the call but kept the reference
        self.assertEqual((summ["graded"], summ["ungradable"]), (0, {"oracle_call_wins": 1}))

    def test_go_rta_dispatch_edge_does_not_make_a_stored_function_a_call(self):
        """`Test{addEntryFunc: g}` then `t.addEntryFunc(..)`: RTA lists g as a callee of the test through the function value. That is a dynamic
        edge, not a call of g from the test, so the reference stays a reference."""
        dyn = {"caller": {"file": "a.go", "line": 1, "name": "f"}, "callee": {"file": "a.go", "line": 5, "name": "g"}, "sites": ["a.go:3"], "dispatch": True}
        oracle = roracle(["a.go"], [rref(("a.go", 1, "f"), ("a.go", 5, "g"), ctxs=["struct_field"])], [dyn], ref_callers=[{"file": "a.go", "line": 1, "name": "f"}])
        syms = {1: rsym("a.go", 1, "f"), 2: rsym("a.go", 5, "g")}
        summ, *_ = self.grade(oracle, syms, [redge(1, 2, 2)])
        self.assertEqual((summ["oracle_refs_called_removed"], summ["tp"]), (0, 1))

    def test_rust_module_level_reference_is_keyed_at_the_module_and_an_import_row_is_not_a_reference(self):
        """`static TABLE: [fn(); 1] = [handler];` is a reference of the module (line 0); a USES row without a source line is an import."""
        r = rref(("src/lib.rs", 0, "<module>"), ("src/lib.rs", 7, "handler"), forms=["ident"], ctxs=["other"], site=3)
        oracle = roracle(["src/lib.rs"], [r], callers=[])
        syms = {1: rsym("src/lib.rs", 1, "lib", "module"), 2: rsym("src/lib.rs", 7, "handler"), 3: rsym("src/lib.rs", 9, "other")}
        syms[1]["key"] = "src/lib.rs:0"
        imp = redge(1, 3, None, "USES", path="src/lib.rs")
        imp["end_line"] = None
        summ, fn, fp, gap, ungr, _ = self.grade(oracle, syms, [redge(1, 2, 3, "REFERENCES", path="src/lib.rs"), imp], "rust")
        self.assertEqual((summ["tp"], summ["fp"], summ["ungradable"]), (1, 0, {"no_evidence_line": 1}))
        # the same import-like row stands for a reference of some function of the file: a miss there is `coarse_module_level`, not `no_edge`
        oracle2 = roracle(["src/lib.rs"], [rref(("src/lib.rs", 12, "run"), ("src/lib.rs", 9, "other"), site=13)], callers=[{"file": "src/lib.rs", "line": 12, "name": "run"}])
        syms2 = {**syms, 4: rsym("src/lib.rs", 12, "run")}
        summ2, fn2, *_ = self.grade(oracle2, syms2, [imp], "rust")
        self.assertEqual((summ2["recall"], fn2[0]["category"]), (0.0, "coarse_module_level"))

    def test_a_call_in_a_nested_function_is_a_call_of_the_enclosing_declaration(self):
        """`strict() { errorUtil.errToObj; ... errorMap: (i) => errToObj(m) }`: the call sits in an arrow keyed `errorMap`, lifted to strict, which is where
        lidx keeps both the reference and the CALLS edge."""
        call = {"caller": {"file": "a.ts", "line": 9, "name": "errorMap"}, "callee": {"file": "a.ts", "line": 20, "name": "errToObj"}, "sites": ["a.ts:10"],
                "caller_chain": ["a.ts:5", "a.ts:0"]}
        oracle = roracle(["a.ts"], [rref(("a.ts", 5, "strict"), ("a.ts", 20, "errToObj"), forms=["property_access"], ctxs=["other"], site=6)], [call], callers=[])
        syms = {1: rsym("a.ts", 5, "strict", "method"), 2: rsym("a.ts", 20, "errToObj")}
        summ, fn, fp, *_ = self.grade(oracle, syms, [redge(1, 2, 6, "CALLS", path="a.ts"), redge(1, 2, 6, path="a.ts")], "ts")
        self.assertEqual((summ["oracle_refs_called_removed"], summ["lidx_pairs_with_call_dropped"], summ["graded"], fn, fp), (1, 1, 0, [], []))

    def test_rust_trait_path_resolved_to_one_impl_accepts_the_trait_declaration(self):
        """`.map(Node::tree)`: the compiler resolves the path to one impl's method; lidx points at the trait's declaration, which names the same function."""
        r = rref(("src/lib.rs", 5, "run"), ("src/lib.rs", 20, "tree"), forms=["path"], ctxs=["argument_or_element"], site=6, callee_alts=["src/lib.rs:2"])
        oracle = roracle(["src/lib.rs"], [r], callers=[{"file": "src/lib.rs", "line": 5, "name": "run"}])
        syms = {1: rsym("src/lib.rs", 5, "run"), 2: rsym("src/lib.rs", 2, "tree", "method"), 3: rsym("src/lib.rs", 20, "tree", "method")}
        summ, *_ = self.grade(oracle, syms, [redge(1, 2, 6, path="src/lib.rs")], "rust")
        self.assertEqual((summ["tp"], summ["fp"], summ["matched_oracle_refs"]), (1, 0, 1))

    def test_rust_reference_inside_inactive_cfg_code_is_left_out(self):
        """rust-analyzer resolves names in `#[cfg]`-disabled code only partly (a local declared there is invisible), so a reference whose every site is
        gated is not graded, and an unmatched lidx pair with gated evidence is ungradable."""
        gate = lambda f, lines: all(ln >= 50 for ln in lines)  # noqa: E731
        oracle = roracle(["src/lib.rs"], [rref(("src/lib.rs", 40, "f"), ("src/lib.rs", 2, "features"), site=52)], callers=[{"file": "src/lib.rs", "line": 40, "name": "f"}])
        syms = {1: rsym("src/lib.rs", 40, "f"), 2: rsym("src/lib.rs", 2, "features"), 3: rsym("src/lib.rs", 9, "other")}
        summ, fn, fp, gap, ungr, _ = refs.grade_refs(oracle, syms, [redge(1, 3, 53, path="src/lib.rs")], "rust", match.Sources("/nonexistent"), cfg_gate=gate)
        self.assertEqual((summ["oracle_refs_cfg_gated"], summ["oracle_refs_caller_and_callee_indexed"], summ["fp"]), (1, 0, 0))
        self.assertEqual(summ["ungradable"], {"cfg_gated_evidence": 1})

    def test_reference_recorded_as_a_call_is_a_miss_with_a_cross_category(self):
        oracle = roracle(["a.go"], [rref(("a.go", 1, "f"), ("a.go", 5, "g"))], ref_callers=[{"file": "a.go", "line": 1, "name": "f"}])
        syms = {1: rsym("a.go", 1, "f"), 2: rsym("a.go", 5, "g")}
        summ, fn, *_ = self.grade(oracle, syms, [redge(1, 2, 2, "CALLS")])
        self.assertEqual((summ["recall"], fn[0]["category"]), (0.0, "reference_as_call"))

    def test_call_recorded_as_a_reference_is_an_fp(self):
        """The compiler only calls g from f, lidx has a REFERENCES row: a wrong reference, `call_as_reference`."""
        oracle = roracle(["a.go"], [], [{"caller": {"file": "a.go", "line": 1, "name": "f"}, "callee": {"file": "a.go", "line": 5, "name": "g"}, "sites": ["a.go:2"]}],
                         ref_callers=[{"file": "a.go", "line": 1, "name": "f"}])
        syms = {1: rsym("a.go", 1, "f"), 2: rsym("a.go", 5, "g")}
        summ, fn, fp, *_ = self.grade(oracle, syms, [redge(1, 2, 2)])
        self.assertEqual((summ["fp"], fp[0]["category"]), (1, "call_as_reference"))

    def test_same_name_elsewhere(self):
        """lidx bound `register(handle)` to another declaration called handle: the failure of a name-tier guess."""
        oracle = roracle(["a.go", "b.go"], [rref(("a.go", 1, "f"), ("a.go", 5, "handle"), site=2)], ref_callers=[{"file": "a.go", "line": 1, "name": "f"}])
        syms = {1: rsym("a.go", 1, "f"), 2: rsym("a.go", 5, "handle"), 3: rsym("b.go", 9, "handle")}
        summ, fn, fp, *_ = self.grade(oracle, syms, [redge(1, 3, 2, res="bare_name")])
        self.assertEqual((summ["fp"], summ["same_name_elsewhere"], fp[0]["category"]), (1, 1, "same_name_elsewhere"))
        self.assertEqual(summ["by_resolution_kind"]["bare_name"], {"graded": 1, "tp": 0, "precision": 0.0, "ci": summ["by_resolution_kind"]["bare_name"]["ci"]})
        self.assertEqual(fn[0]["category"], "wrong_target")
        self.assertTrue(fn[0]["detail"]["same_name"])

    def test_dead_code_cross_reference_counts_value_referenced_dead_claims(self):
        oracle = roracle(["a.go"], [rref(("a.go", 1, "f"), ("a.go", 5, "g"))], ref_callers=[{"file": "a.go", "line": 1, "name": "f"}])
        syms = {1: rsym("a.go", 1, "f"), 2: rsym("a.go", 5, "g"), 3: rsym("a.go", 9, "h")}
        *_, state = self.grade(oracle, syms, [])
        x = refs.dead_xref(state, [{"key": "a.go:5"}, {"key": "a.go:9"}])
        self.assertEqual(x, {"fp_items": 2, "with_value_reference": 1, "lidx_has_no_reference_to_it": 1})


class ScipColumns(unittest.TestCase):
    """char_col: rust-analyzer SCIP columns are UTF-8 byte offsets; Python strings are indexed by character (issue #476)."""

    def col(self, row, needle):
        """Character index of `needle` in `row` recovered from the byte offset a SCIP index would carry."""
        return sc.char_col(row, row.encode("utf-8").index(needle.encode("utf-8")))

    def test_ascii_is_the_identity(self):
        self.assertEqual(sc.char_col("let x = f(1);", 8), 8)

    def test_two_byte_text_before_the_identifier(self):
        row = 'eqnice!("foo:\u0428\u0435\u0440", cmd.stdout());'
        self.assertEqual(self.col(row, "stdout"), row.index("stdout"))
        self.assertEqual(row[self.col(row, "stdout"):].startswith("stdout"), True)
        self.assertNotEqual(row.encode().index(b"stdout"), row.index("stdout"))

    def test_three_and_four_byte_characters(self):
        # a CJK character is 3 bytes / 1 char / 1 UTF-16 unit; an emoji is 4 bytes / 1 char / 2 UTF-16 units (a surrogate pair)
        for text in ("\u65e5\u672c", "\U0001f600", "\U0001f600\u00e9\u65e5"):
            row = '    f("%s", g());' % text
            i = self.col(row, "g()")
            self.assertEqual(i, row.index("g()"))
            self.assertEqual(row[i:i + 3], "g()")

    def test_tabs_are_one_byte_and_one_character(self):
        row = "\t\tcall(\u00e9\t, other());"
        self.assertEqual(self.col(row, "other"), row.index("other"))
        self.assertEqual(sc.char_col("\t\tfoo()", 2), 2)

    def test_crlf_line_end_does_not_shift_columns(self):
        row = "    foo(\u0428);\r"  # a line split on \n keeps the \r at its end
        e = self.col(row, "foo") + 3
        self.assertEqual(row[e:].lstrip()[:1], "(")

    def test_end_column_of_a_multibyte_identifier(self):
        row = "let \u00e9t\u00e9 = f();"
        start = row.encode().index("\u00e9t\u00e9".encode())
        self.assertEqual(sc.char_col(row, start + len("\u00e9t\u00e9".encode())), row.index(" = "))

    def test_out_of_range_and_mid_character_columns_are_clamped(self):
        self.assertEqual(sc.char_col("ab", 99), 2)
        self.assertEqual(sc.char_col("\u0428\u0428", 99), 2)
        self.assertEqual(sc.char_col("a\u0428b", 2), 1)  # inside the two-byte character: its start


class RustValueReferenceOracle(unittest.TestCase):
    """scip_calls.py through a synthetic SCIP index: a function passed to `.map` is a reference, a call is not, an occurrence inside a
    `use` item or with the Import role is left out."""

    SRC = ("use crate::helper;\n"                      # 0: use item
           "pub fn helper(x: u8) -> u8 { x }\n"        # 1
           "pub fn other() {\n"                         # 2
           "    let _ = [1u8].iter().map(helper);\n"   # 3: a reference
           "    helper(1);\n"                           # 4: a call
           "    let _ = helper::<>;\n"                  # 5: a reference through an (empty) turbofish
           "}\n"
           "pub fn third() {\n"                          # 7
           "    let _ = (\"\u0428\u0428\", helper(2));\n"      # 8: a call after non-ASCII text (SCIP columns are UTF-8 bytes)
           "}\n")

    def run_oracle(self, extra_occ=()):
        P = "rust-analyzer cargo demo 0.1.0 "
        helper, other = P + "helper().", P + "other()."

        def occ(sym, line, c0, c1, role, encl=None):
            rng = b"".join(_varint(x) for x in (line, c0, c1))
            o = _field(1, 2, rng) + _field(2, 2, sym.encode()) + (_field(3, 0, _varint(role)) if role else b"")
            if encl:
                o += _field(7, 2, b"".join(_varint(x) for x in encl))
            return _field(2, 2, o)
        doc = _field(1, 2, b"src/lib.rs")
        doc += occ(helper, 0, 11, 17, 0)                       # `use crate::helper;`
        doc += occ(helper, 1, 7, 13, 1, (1, 0, 1, 33))         # fn helper
        doc += occ(other, 2, 7, 12, 1, (2, 0, 6, 1))           # fn other
        doc += occ(helper, 3, 29, 35, 0)                       # .map(helper)
        doc += occ(helper, 4, 4, 10, 0)                        # helper(1)
        doc += occ(helper, 5, 12, 18, 0)                       # helper::<>
        for o in extra_occ:
            doc += occ(*o[:1], *o[1:])
        idx = _field(2, 2, doc)
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "src"))
            with open(os.path.join(td, "src", "lib.rs"), "w") as f:
                f.write(self.SRC)
            scip = os.path.join(td, "x.scip")
            with open(scip, "wb") as f:
                f.write(idx)
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, "-I", os.path.join(here, "rust_oracle", "scip_calls.py"), "--repo", td, "--scip", scip],
                                 capture_output=True, text=True, check=True).stdout
        return json.loads(out)

    def test_map_argument_is_a_reference_and_the_use_item_is_not(self):
        d = self.run_oracle()
        self.assertEqual([(e["caller"]["name"], e["callee"]["name"]) for e in d["edges"]], [("other", "helper")])
        self.assertEqual(len(d["refs"]), 1)
        r = d["refs"][0]
        self.assertEqual((r["caller"]["name"], r["callee"]["name"]), ("other", "helper"))
        self.assertEqual(sorted(r["sites"]), ["src/lib.rs:4", "src/lib.rs:6"])  # the `use` line (1) is not a site
        self.assertEqual(d["refs_dropped"], {"use_item": 1})
        self.assertEqual(r["ctxs"], ["argument_or_element", "assignment"])

    def test_a_call_after_non_ascii_text_is_a_call_edge(self):
        """SCIP columns count UTF-8 bytes, so on a line with Cyrillic text the characters after it are shifted: `helper(2)` must still be a
        call (an edge of third(), as it is for an ASCII line), not a reference and not lost (issue #476)."""
        P = "rust-analyzer cargo demo 0.1.0 "
        prefix = '    let _ = ("\u0428\u0428", '
        c0 = len(prefix.encode("utf-8"))
        d = self.run_oracle(extra_occ=[(P + "third().", 7, 7, 12, 1, (7, 0, 9, 1)), (P + "helper().", 8, c0, c0 + 6, 0)])
        self.assertEqual(sorted(r["sites"][0] for r in d["refs"]), ["src/lib.rs:4"])  # no reference from third()
        self.assertEqual(sorted((e["caller"]["name"], e["callee"]["name"]) for e in d["edges"]), [("other", "helper"), ("third", "helper")])
        self.assertNotIn("call_on_non_ascii_line", d["refs_dropped"])

    def test_a_real_reference_after_non_ascii_text_is_not_read_as_a_call(self):
        """The shift can also hide the paren-less end of a reference: bytes shifted past `)` read as a call. `map(helper)` after Cyrillic
        text is a reference with the right shape."""
        P = "rust-analyzer cargo demo 0.1.0 "
        src = '    let _ = ["\u0428\u0428"].iter().map(helper);\n'
        prefix = src[:src.index("helper")]
        c0 = len(prefix.encode("utf-8"))
        self.SRC = self.SRC.replace('    let _ = ("\u0428\u0428", helper(2));\n', src)
        d = self.run_oracle(extra_occ=[(P + "third().", 7, 7, 12, 1, (7, 0, 9, 1)), (P + "helper().", 8, c0, c0 + 6, 0)])
        self.assertIn("src/lib.rs:9", [x for r in d["refs"] for x in r["sites"]])
        self.assertEqual([(e["caller"]["name"], e["callee"]["name"]) for e in d["edges"]], [("other", "helper")])
        r = [r for r in d["refs"] if "src/lib.rs:9" in r["sites"]][0]
        self.assertEqual(r["ctxs"], ["argument_or_element"])  # ref_shape reads the text before the reference at the character column

    def test_a_lone_carriage_return_does_not_shift_lines(self):
        """rust-analyzer ends a line only at `\\n`: a lone `\\r` inside a string literal stays in its line, so the call after it is still
        at its SCIP column and later lines keep their numbers."""
        P = "rust-analyzer cargo demo 0.1.0 "
        src = '    let _ = ("a\rb", helper(2));\n'
        c0 = len(src[:src.index("helper")].encode("utf-8"))
        self.SRC = self.SRC.replace('    let _ = ("\u0428\u0428", helper(2));\n', src)
        d = self.run_oracle(extra_occ=[(P + "third().", 7, 7, 12, 1, (7, 0, 9, 1)), (P + "helper().", 8, c0, c0 + 6, 0)])
        self.assertEqual(sorted((e["caller"]["name"], e["callee"]["name"]) for e in d["edges"]), [("other", "helper"), ("third", "helper")])
        self.assertEqual(sorted(r["sites"][0] for r in d["refs"]), ["src/lib.rs:4"])

    def test_split_lines_like_rust_analyzer(self):
        self.assertEqual(sc.split_lines("a\r\nb\rc\nd"), ["a", "b\rc", "d"])

    def test_import_role_is_left_out(self):
        P = "rust-analyzer cargo demo 0.1.0 "
        d = self.run_oracle(extra_occ=[(P + "helper().", 3, 29, 35, 2)])  # the same range again, with the Import role
        self.assertEqual(d["refs_dropped"]["use_item"], 2)


class RustConstInitialiserReferences(unittest.TestCase):
    """A function named in a `const` / `static` initialiser is a reference of that item (issue #514), with the module as the acceptable coarser
    caller; a call there stays out of the call benchmark, and a field or local is never an item."""

    SRC = ("pub fn helper(x: u8) -> u8 { x }\n"            # 0
           "pub struct P { pub f: fn(u8) -> u8 }\n"        # 1
           "pub const PROFILE: P = P {\n"                   # 2
           "    f: helper,\n"                               # 3: a reference inside the const
           "};\n"                                           # 4
           "pub fn main() { let _ = [1u8].iter().map(helper); }\n")  # 5

    def run_oracle(self):
        P = "rust-analyzer cargo demo 0.1.0 "

        def occ(sym, line, c0, c1, role, encl=None):
            rng = b"".join(_varint(x) for x in (line, c0, c1))
            o = _field(1, 2, rng) + _field(2, 2, sym.encode()) + (_field(3, 0, _varint(role)) if role else b"")
            if encl:
                o += _field(7, 2, b"".join(_varint(x) for x in encl))
            return _field(2, 2, o)
        doc = _field(1, 2, b"src/lib.rs")
        doc += occ(P + "helper().", 0, 7, 13, 1, (0, 0, 0, 33))
        doc += occ(P + "P#", 1, 11, 12, 1, (1, 0, 1, 36))
        doc += occ(P + "P#f.", 1, 19, 20, 1, (1, 16, 1, 34))   # a field: not an item a reference can belong to
        doc += occ(P + "PROFILE.", 2, 10, 17, 1, (2, 0, 4, 2))
        doc += occ(P + "helper().", 3, 7, 13, 0)
        doc += occ(P + "main().", 5, 7, 11, 1, (5, 0, 5, 51))
        doc += occ(P + "helper().", 5, 41, 47, 0)
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "src"))
            with open(os.path.join(td, "src", "lib.rs"), "w") as f:
                f.write(self.SRC)
            scip = os.path.join(td, "x.scip")
            with open(scip, "wb") as f:
                f.write(_field(2, 2, doc))
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, "-I", os.path.join(here, "rust_oracle", "scip_calls.py"), "--repo", td, "--scip", scip],
                                 capture_output=True, text=True, check=True).stdout
        return json.loads(out)

    def test_reference_in_an_initialiser_belongs_to_the_item(self):
        d = self.run_oracle()
        by = {r["caller"]["name"]: r for r in d["refs"]}
        self.assertEqual(sorted(by), ["PROFILE", "main"])
        self.assertEqual((by["PROFILE"]["caller"]["line"], by["PROFILE"]["caller_alts"]), (3, ["src/lib.rs:0"]))
        self.assertNotIn("caller_alts", by["main"])  # inside a function the caller is the function
        self.assertEqual([(c["name"], c["line"]) for c in d["ref_callers"]], [("PROFILE", 3)])  # the field `f` is no item
        self.assertEqual([c["name"] for c in d["callers"]], ["helper", "main"])  # a call from an initialiser is not judged by the call benchmark

    def test_grading_accepts_the_item_and_the_module_and_judges_the_item(self):
        d = self.run_oracle()
        syms = {1: rsym("src/lib.rs", 1, "lib", "module"), 2: rsym("src/lib.rs", 1, "helper"), 3: rsym("src/lib.rs", 3, "PROFILE", "const"),
                4: rsym("src/lib.rs", 6, "main")}
        syms[1]["key"] = "src/lib.rs:0"
        syms[2]["key"] = "src/lib.rs:1"
        syms[4]["key"] = "src/lib.rs:6"
        edges = lambda src: [redge(src, 2, 4, path="src/lib.rs"), redge(4, 2, 6, path="src/lib.rs")]  # noqa: E731
        for src in (3, 1):  # attributed to the item, or coarsely to the module
            summ, fn, fp, gap, ungr, _ = refs.grade_refs(d, syms, edges(src), "rust", match.Sources("/nonexistent"))
            self.assertEqual((summ["tp"], summ["fp"], summ["recall"], summ["ungradable"]), (2, 0, 1.0, {}), src)
        # a wrong pair from the item is judged because the item is a `ref_caller`; without that it would be ungradable
        syms[5] = rsym("src/lib.rs", 9, "elsewhere")
        wrong = edges(3) + [redge(3, 5, 4, path="src/lib.rs")]
        summ, *_ = refs.grade_refs(d, syms, wrong, "rust", match.Sources("/nonexistent"))
        self.assertEqual((summ["fp"], summ["ungradable"]), (1, {}))
        summ, *_ = refs.grade_refs(dict(d, ref_callers=[]), syms, wrong, "rust", match.Sources("/nonexistent"))
        self.assertEqual((summ["fp"], summ["ungradable"]), (0, {"caller_not_judged": 1}))


if __name__ == "__main__":
    unittest.main()


class RustSameSymbolDefinitions(unittest.TestCase):
    """rust-analyzer gives a `fn foo` nested in `bar` and a top-level `fn foo` one SCIP symbol: an occurrence inside `bar` means the
    nested one, any other (inside `qux`) the top-level one."""

    SRC = ("pub fn foo() {}\n"            # 0
           "pub fn bar() {\n"             # 1
           "    fn foo() {}\n"            # 2
           "    foo();\n"                 # 3
           "}\n"                          # 4
           "pub fn qux() {\n"             # 5
           "    foo();\n"                 # 6
           "}\n")                         # 7

    def run_oracle(self):
        P = "rust-analyzer cargo demo 0.1.0 "
        foo, bar, qux = P + "foo().", P + "bar().", P + "qux()."

        def occ(sym, line, c0, c1, role, encl=None):
            rng = b"".join(_varint(x) for x in (line, c0, c1))
            o = _field(1, 2, rng) + _field(2, 2, sym.encode()) + (_field(3, 0, _varint(role)) if role else b"")
            if encl:
                o += _field(7, 2, b"".join(_varint(x) for x in encl))
            return _field(2, 2, o)
        doc = _field(1, 2, b"src/lib.rs")
        doc += occ(foo, 0, 7, 10, 1, (0, 0, 0, 15))
        doc += occ(bar, 1, 7, 10, 1, (1, 0, 4, 1))
        doc += occ(foo, 2, 7, 10, 1, (2, 4, 2, 15))
        doc += occ(foo, 3, 4, 7, 0)
        doc += occ(qux, 5, 7, 10, 1, (5, 0, 7, 1))
        doc += occ(foo, 6, 4, 7, 0)
        idx = _field(2, 2, doc)
        with tempfile.TemporaryDirectory() as td:
            os.makedirs(os.path.join(td, "src"))
            with open(os.path.join(td, "src", "lib.rs"), "w") as f:
                f.write(self.SRC)
            scip = os.path.join(td, "x.scip")
            with open(scip, "wb") as f:
                f.write(idx)
            here = os.path.dirname(os.path.abspath(__file__))
            out = subprocess.run([sys.executable, "-I", os.path.join(here, "rust_oracle", "scip_calls.py"), "--repo", td, "--scip", scip],
                                 capture_output=True, text=True, check=True).stdout
        return json.loads(out)

    def test_an_occurrence_means_the_definition_visible_where_it_is(self):
        d = self.run_oracle()
        got = sorted((e["caller"]["name"], e["callee"]["line"]) for e in d["edges"])
        self.assertEqual(got, [("bar", 3), ("qux", 1)])
