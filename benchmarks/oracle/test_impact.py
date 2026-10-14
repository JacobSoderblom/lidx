"""Unit tests of the impact / test-reach / stop-reason grading (impact.py, issue #451). Python 3 stdlib only."""
import unittest

import impact


def sym(i, file, line, name, kind="function", qual=None):
    return {"key": "%s:%d" % (file, line), "file": file, "line": line, "name": name, "qualname": qual or name, "kind": kind}


def decl(file, line, name):
    return {"file": file, "line": line, "name": name}


class FakeSources:
    def __init__(self, files):
        self.files = files

    def lines(self, path):
        return self.files.get(path, [])


class FakeLidx:
    """Answers analyze_impact / trace_flow from canned tables keyed by (seed id, k)."""

    def __init__(self, up=None, tests=None, down=None, lower_bound=None, test_paths=None):
        self.up, self.tests, self.down = up or {}, tests or {}, down or {}
        self.test_paths = test_paths or {}  # (seed id, member id) -> edge kinds along the path
        self.lower_bound = lower_bound or {}
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, params))
        sid = params.get("id", params.get("start_id"))
        k = params.get("max_depth", params.get("max_hops", 3))  # a call without either is the default depth
        if method == "trace_flow":
            hops, start_stop, trunc = self.down.get((sid, k), ([], None, False))
            r = {"trace": hops, "budget": {"truncated": trunc}, "truncated": trunc}
            if start_stop:
                r["start_stop"] = {"stop": start_stop}
            return r
        if "enable_direct" in params and params["enable_direct"] is False:
            affected = self.tests.get((sid, k), [])
            ent = []
            for i in affected:
                e = {"symbol": {"id": i}, "relationship": "TEST"}
                if params.get("include_paths"):
                    e["path"] = {"steps": [{"edge_kind": kd} for kd in self.test_paths.get((sid, i), ["CALLS"])]}
                ent.append(e)
            return {"affected": ent, "truncated": False,
                    "lower_bound": {"is_lower_bound": False, "unresolved_count": 0}, "layers": {"test": {"truncated": False}}}
        affected = self.up.get((sid, k), [])
        lb = self.lower_bound.get(sid, 0)
        return {"affected": [{"symbol": {"id": i}, "relationship": "DIRECT"} for i in affected], "truncated": False,
                "lower_bound": {"is_lower_bound": lb > 0, "unresolved_count": lb}}


def hop(i, stop=None, dist=1, detail=None):
    h = {"symbol": {"id": i}, "distance": dist}
    if stop:
        h["stop"] = stop
    if detail:
        h["stop_detail"] = detail
    return h


def gw(leaf):
    """graded / wrong of a leaf table (drops the external-only annotation)."""
    return {k: leaf[k] for k in ("graded", "wrong")}


def build(oracle_edges, symlist, lang="go", callers=None, files=None):
    syms = {s[0]: s[1] for s in symlist}
    oracle = {"files": files or sorted({s[1]["file"] for s in symlist}), "callers": callers or [], "edges": oracle_edges}
    return syms, impact.Graph(oracle, syms, lang)


def edge(c, t, **kw):
    e = {"caller": c, "callee": t, "sites": ["%s:%d" % (c["file"], c["line"] + 1)]}
    e.update(kw)
    return e


class ClosureTests(unittest.TestCase):
    def test_closure_through_dispatch_edge_resolved_at_one_implementation(self):
        # run() calls Iface.M; RTA resolves the call at the one allocated implementation Impl.M (callee_alts keeps the interface method)
        run, impl, iface, other = decl("a.go", 10, "run"), decl("a.go", 20, "M"), decl("a.go", 5, "M"), decl("a.go", 30, "M")
        syms, g = build([edge(run, impl, callee_alts=["a.go:5"], dispatch=True)],
                        [(1, sym(1, "a.go", 10, "run")), (2, sym(2, "a.go", 20, "M", "method", "Impl.M")), (3, sym(3, "a.go", 5, "M", "method", "Iface.M")),
                         (4, sym(4, "a.go", 30, "M", "method", "Other.M"))],
                        callers=[run, impl, iface, other])
        n = lambda i: g.cn(syms[i]["key"])
        self.assertEqual(set(g.closure(n(2), 3)), {n(1)})  # the implementation RTA names is reached from run()
        self.assertEqual(set(g.closure(n(3), 3)), set())  # the interface method is not a node of the closure
        self.assertEqual(set(g.closure(n(4), 3)), set())  # an implementation RTA did not resolve is not reached
        # lidx fans the call out to every implementation; the one the oracle does not have is an extra, the other a hit
        lidx = FakeLidx(up={(4, 3): [1], (4, 10): [1]})
        rows, lists, tg, nroots = impact.grade_cell({"lang": "go", "variant": "no_tests", "id": "go/x/no_tests"}, g, syms, lidx, FakeSources({}),
                                                    [{"file": "a.go", "line": 30, "name": "M", "indegree": 0, "stratum": "0"}], variant_kinds=False)
        self.assertEqual((rows[0]["up"]["3"]["tp"], rows[0]["up"]["3"]["fp"], rows[0]["up"]["3"]["fn"]), (0, 1, 0))

    def test_depth_bound_and_unbounded(self):
        a, b, c, d = (decl("f.go", 10 * i, n) for i, n in enumerate("abcd", 1))
        syms, g = build([edge(b, a), edge(c, b), edge(d, c)], [(i, sym(i, "f.go", 10 * i, n)) for i, n in enumerate("abcd", 1)], callers=[a, b, c, d])
        k = lambda x: g.cn("f.go:%d" % (10 * x))
        self.assertEqual(g.closure(k(1), 2), {k(2): 1, k(3): 2})
        self.assertEqual(set(g.closure(k(1), None)), {k(2), k(3), k(4)})
        self.assertEqual(g.indegree(k(1)), 1)

    def test_module_level_and_self_edges_left_out(self):
        a, m = decl("f.ts", 10, "a"), decl("f.ts", 0, "<module>")
        syms, g = build([edge(m, a), edge(a, a)], [(1, sym(1, "f.ts", 10, "a"))], lang="ts", callers=[a])
        self.assertEqual(g.closure(g.cn("f.ts:10"), None), {})


class TestReachTests(unittest.TestCase):
    def setUp(self):
        # TestA -> helper -> target : the test root sits two hops up
        self.t, self.h, self.x, self.u = decl("a_test.go", 5, "TestA"), decl("a_test.go", 20, "helper"), decl("a.go", 7, "target"), decl("a.go", 30, "unrelated")
        self.syms, self.g = build([edge(self.t, self.h), edge(self.h, self.x)],
                                  [(1, sym(1, "a_test.go", 5, "TestA")), (2, sym(2, "a_test.go", 20, "helper")), (3, sym(3, "a.go", 7, "target")),
                                   (4, sym(4, "a.go", 30, "unrelated"))],
                                  callers=[self.t, self.h, self.x, self.u])
        self.src = FakeSources({"a_test.go": [""] * 4 + ["func TestA(t *testing.T) {"] + [""] * 14 + ["func helper() {"]})
        self.cell = {"lang": "go", "variant": "with_tests", "id": "go/x/tests"}

    def grade(self, lidx, seed):
        return impact.grade_cell(self.cell, self.g, self.syms, lidx, self.src, [seed], variant_kinds=False)

    def test_test_root_at_depth_two(self):
        lidx = FakeLidx(tests={(3, 3): [1], (3, 10): [1]})
        rows, _, tg, nroots = self.grade(lidx, {"file": "a.go", "line": 7, "name": "target", "indegree": 1, "stratum": "1-3"})
        self.assertTrue(tg)
        self.assertEqual(nroots, 1)  # TestA is a root, helper is not
        e = rows[0]["tests"]["3"]
        self.assertEqual((e["oracle"], e["tp"], e["fp"], e["fn"]), (1, 1, 0, 0))
        # at k = 1 the root is out of reach, so the oracle would not list it
        self.assertEqual(self.g.closure(self.g.cn("a.go:7"), 1).keys(), {self.g.cn("a_test.go:20")})

    def test_helper_in_test_file_is_a_wrong_test(self):
        lidx = FakeLidx(tests={(3, 3): [1, 2], (3, 10): [1, 2]})
        rows, *_ = self.grade(lidx, {"file": "a.go", "line": 7, "name": "target", "indegree": 1, "stratum": "1-3"})
        e = rows[0]["tests"]["3"]
        self.assertEqual((e["tp"], e["fp"], e["fp_not_root"]), (1, 1, 1))

    def test_wrong_test_reached_through_a_reference_is_not_wrong(self):
        # the helper reaches the seed through a REFERENCES hop: a dependent beyond the calls, which a call graph cannot confirm or refute
        lidx = FakeLidx(tests={(3, 3): [1, 2], (3, 10): [1, 2]}, test_paths={(3, 2): ["CALLS", "REFERENCES"]})
        rows, *_ = self.grade(lidx, {"file": "a.go", "line": 7, "name": "target", "indegree": 1, "stratum": "1-3"})
        e = rows[0]["tests"]["3"]
        self.assertEqual((e["tp"], e["fp"], e["fp_noncall"]), (1, 0, 1))

    def test_seed_with_correct_empty_test_answer(self):
        lidx = FakeLidx()
        rows, *_ = self.grade(lidx, {"file": "a.go", "line": 30, "name": "unrelated", "indegree": 0, "stratum": "0"})
        e = rows[0]["tests"]["3"]
        self.assertEqual((e["oracle"], e["lidx"], e["empty_correct"]), (0, 0, True))
        agg = impact.aggregate(rows, True)["tests"]["3"]
        self.assertEqual((agg["empty_correct"], agg["tp"], agg["fp"], agg["fn_untruncated"]), (1, 0, 0, 0))
        self.assertIsNone(agg["precision"])  # no pair either side: neither rate exists
        self.assertIsNone(agg["recall"])

    def test_rust_test_attribute_read_from_lines_above(self):
        src = FakeSources({"s.rs": ["#[test]", "fn a() {}", "", "/// doc", "#[cfg(test)]", "#[test]", "fn b() {}", "fn c() {}", "#[derive(Debug)]", "fn d() {}"]})
        root = impact.test_root_predicate("rust", "with_tests", src)
        self.assertTrue(root({"file": "s.rs", "line": 2, "name": "a"}))
        self.assertTrue(root({"file": "s.rs", "line": 7, "name": "b"}))
        self.assertFalse(root({"file": "s.rs", "line": 8, "name": "c"}))
        self.assertFalse(root({"file": "s.rs", "line": 10, "name": "d"}))
        self.assertFalse(impact.test_root_predicate("ts", "no_tests", src)({"file": "s.rs", "line": 2, "name": "a"}))  # TypeScript: void

    def test_go_example_is_a_root_only_with_an_output_comment(self):
        src = FakeSources({"e_test.go": [
            "func ExampleA() {", "\tf()", "\t// Output:", "\t// 1", "}",
            "func ExampleB() {", "\tf()", "}",
            "func ExampleC() {", "\t// Unordered output:", "\t// x", "}",
            "func ExampleD() {", "\t// Output:", "\tf()", "\t// trailing note", "}",
            "func (t T) ExampleE() {", "}",
            "func TestX(t *testing.T) {", "}",
        ]})
        root = impact.test_root_predicate("go", "tests", src)
        for line, name, want in [(1, "ExampleA", True), (6, "ExampleB", False), (9, "ExampleC", True), (13, "ExampleD", False),
                                 (18, "ExampleE", False), (20, "TestX", True)]:
            self.assertEqual(root({"file": "e_test.go", "line": line, "name": name}), want, name)


class LeafHonestyTests(unittest.TestCase):
    def setUp(self):
        # f calls g, g calls h; h calls nothing in the repository. i calls j.
        self.f, self.g_, self.h, self.i, self.j = (decl("f.go", 10 * n, nm) for n, nm in enumerate("fghij", 1))
        self.syms, self.graph = build([edge(self.f, self.g_), edge(self.g_, self.h), edge(self.i, self.j)],
                                      [(n, sym(n, "f.go", 10 * n, nm)) for n, nm in enumerate("fghij", 1)],
                                      callers=[self.f, self.g_, self.h, self.i, self.j])
        self.cell = {"lang": "go", "variant": "no_tests", "id": "go/x/no_tests"}

    def grade(self, hops, seed_line, start_stop=None, trunc=False):
        seed = {"file": "f.go", "line": seed_line, "name": "s", "indegree": 0, "stratum": "0"}
        sid = seed_line // 10
        lidx = FakeLidx(down={(sid, 3): (hops, start_stop, trunc), (sid, 10): (hops, start_stop, trunc)})
        rows, *_ = impact.grade_cell(self.cell, self.graph, self.syms, lidx, FakeSources({}), [seed], variant_kinds=False)
        return rows[0]["down"]["3"]

    def test_no_callees_leaf_with_oracle_callees_is_false(self):
        # lidx stops at g with no_callees although the oracle has g -> h
        d = self.grade([hop(2, "no_callees")], 10)
        self.assertEqual(gw(d["leaves"]["no_callees"]), {"graded": 1, "wrong": 1})
        self.assertEqual(d["leaf_examples"][0]["leaf"], "f.go:20 g")
        self.assertEqual(d["missing"], ["f.go:30 h"])

    def test_true_leaf_and_false_alarm(self):
        d = self.grade([hop(2), hop(3, "no_callees", 2)], 10)
        self.assertEqual(gw(d["leaves"]["no_callees"]), {"graded": 1, "wrong": 0})
        # unresolved_calls at h, where the oracle has no callee of h: a false alarm; at g, where it has one, a justified alarm
        d = self.grade([hop(2, "unresolved_calls"), hop(3, "unresolved_calls", 2)], 10)
        self.assertEqual(gw(d["leaves"]["unresolved_calls"]), {"graded": 2, "wrong": 1})

    def test_false_alarm_whose_unresolved_calls_are_all_external_is_labelled(self):
        ext = {"count": 2, "top": [["fmt.Println", "external"], ["os.Exit", "external"]]}
        mixed = {"count": 2, "top": [["fmt.Println", "external"], ["helper", "no_candidates"]]}
        d = self.grade([hop(2), hop(3, "unresolved_calls", 2, ext)], 10)
        self.assertEqual(d["leaves"]["unresolved_calls"]["wrong_external_only"], 1)
        d = self.grade([hop(2), hop(3, "unresolved_calls", 2, mixed)], 10)
        self.assertEqual(d["leaves"]["unresolved_calls"]["wrong_external_only"], 0)

    def test_truncated_and_depth_limit_leaves_are_excluded(self):
        d = self.grade([hop(2, "truncated"), hop(3, "depth_limit", 2)], 10)
        self.assertEqual(gw(d["leaves"]["no_callees"]), {"graded": 0, "wrong": 0})
        self.assertEqual(gw(d["leaves"]["unresolved_calls"]), {"graded": 0, "wrong": 0})
        self.assertEqual(d["leaf_total"], 2)

    def test_empty_trace_reports_the_start_leaf(self):
        d = self.grade([], 10, start_stop="no_callees")  # f has an oracle callee: the start is a false leaf
        self.assertEqual(gw(d["leaves"]["no_callees"]), {"graded": 1, "wrong": 1})

    def test_byte_truncated_trace_does_not_count_against_recall(self):
        d = self.grade([hop(2)], 10, trunc=True)
        self.assertTrue(d["truncated"])
        agg = impact.aggregate([{"seed": {}, "lidx_symbol": "f", "up": {}, "up_default": {}, "all_kinds": {}, "tests": {}, "down": {"3": d, "inf": d}}], False)
        self.assertIsNone(agg["down"]["3"]["recall"])


class PathGradabilityTests(unittest.TestCase):
    def test_path_through_code_the_oracle_did_not_analyse_is_ungradable(self):
        a, b, c = decl("a.go", 10, "a"), decl("a.go", 20, "b"), decl("a.go", 30, "c")
        syms = {1: sym(1, "a.go", 10, "a"), 2: sym(2, "a.go", 20, "b"), 3: sym(3, "a.go", 30, "c"), 4: sym(4, "ex/m.go", 5, "viaEx", qual="ex.viaEx"),
                5: sym(5, "a.go", 40, "dead")}
        oracle = {"files": ["a.go"], "callers": [a, b, c], "edges": [edge(b, a)]}
        g = impact.Graph(oracle, syms, "go")
        self.assertTrue(g.path_ok([{"from_symbol": "b", "to_symbol": "a"}]))
        self.assertFalse(g.path_ok([{"from_symbol": "ex.viaEx", "to_symbol": "a"}]))  # a file the oracle never loaded (example / nested module / test file)
        self.assertFalse(g.path_ok([{"from_symbol": "dead", "to_symbol": "a"}]))  # Go: RTA did not reach it (a type nothing allocates)
        ts = impact.Graph(oracle, syms, "ts")
        self.assertTrue(ts.path_ok([{"from_symbol": "dead", "to_symbol": "a"}]))  # only Go's oracle is restricted to what is reachable

    def test_interface_method_hop_of_a_dispatch_does_not_make_a_path_ungradable(self):
        impl, caller = decl("a.go", 10, "M"), decl("a.go", 20, "run")
        syms = {1: sym(1, "a.go", 10, "M", "method", "Impl.M"), 2: sym(2, "a.go", 5, "M", "method", "Iface.M"), 3: sym(3, "a.go", 20, "run"),
                4: sym(4, "a.go", 4, "Iface", "interface")}
        g = impact.Graph({"files": ["a.go"], "callers": [impl, caller], "edges": [edge(caller, impl)]}, syms, "go")
        self.assertTrue(g.path_ok([{"from_symbol": "Impl.M", "to_symbol": "Iface.M"}, {"from_symbol": "Iface.M", "to_symbol": "run"}]))

    def test_rust_trait_method_is_abstract(self):
        syms = {1: sym(1, "a.rs", 5, "doc_long", "method", "crate::flags::Flag::doc_long"), 2: sym(2, "a.rs", 3, "Flag", "trait", "crate::flags::Flag"),
                3: sym(3, "a.rs", 9, "doc_long", "method", "crate::flags::defs::A::doc_long")}
        g = impact.Graph({"files": ["a.rs"], "callers": [decl("a.rs", 9, "doc_long")], "edges": []}, syms, "rust")
        self.assertEqual(g.abstract, {g.cn("a.rs:5")})

    def test_extra_member_whose_path_crosses_unanalysed_code_is_not_called_wrong(self):
        a, b = decl("a.go", 10, "a"), decl("a.go", 20, "b")
        syms = {1: sym(1, "a.go", 10, "a"), 2: sym(2, "a.go", 20, "b"), 3: sym(3, "a.go", 50, "top"), 4: sym(4, "ex/m.go", 5, "mid", qual="ex.mid")}
        oracle = {"files": ["a.go"], "callers": [a, b, decl("a.go", 50, "top")], "edges": [edge(b, a)]}
        g = impact.Graph(oracle, syms, "go")

        class L(FakeLidx):
            def call(self, method, params):
                r = FakeLidx.call(self, method, params)
                if params.get("include_paths") and "affected" in r:
                    for e in r["affected"]:
                        e["path"] = {"steps": [{"from_symbol": "a", "to_symbol": "ex.mid"}, {"from_symbol": "ex.mid", "to_symbol": "top"}]}
                return r
        lidx = L(up={(1, 3): [2, 3], (1, 10): [2, 3]})
        rows, *_ = impact.grade_cell({"lang": "go", "variant": "no_tests", "id": "go/x/no_tests"}, g, syms, lidx, FakeSources({}),
                                     [{"file": "a.go", "line": 10, "name": "a", "indegree": 1, "stratum": "1-3"}], variant_kinds=False)
        e = rows[0]["up"]["3"]
        self.assertEqual((e["tp"], e["fp"], e["ungradable_path"]), (1, 0, 1))


class AlternativeKeyTests(unittest.TestCase):
    def test_module_level_call_credits_every_alternative_declaration(self):
        mod, callee = decl("c.ts", 0, "<module>"), decl("u.ts", 5, "esc")
        syms = {1: sym(1, "c.ts", 0, "<module>", "module"), 2: sym(2, "c.ts", 10, "A"), 3: sym(3, "c.ts", 20, "B"), 4: sym(4, "u.ts", 5, "esc")}
        oracle = {"files": ["c.ts", "u.ts"], "callers": [decl("c.ts", 10, "A"), decl("c.ts", 20, "B"), callee],
                  "edges": [edge(mod, callee, caller_alts=["c.ts:10", "c.ts:20"])]}
        g = impact.Graph(oracle, syms, "ts")
        self.assertEqual(set(g.closure(g.cn("u.ts:5"), 1)), {g.cn("c.ts:10"), g.cn("c.ts:20")})

    def test_new_expression_reaches_the_class_and_its_constructor(self):
        caller, klass, ctor, callee = decl("k.ts", 5, "create"), decl("k.ts", 10, "Ky"), decl("k.ts", 20, "constructor"), decl("k.ts", 40, "merge")
        syms = {1: sym(1, "k.ts", 5, "create"), 2: sym(2, "k.ts", 10, "Ky", "class"), 3: sym(3, "k.ts", 20, "constructor", "method", "Ky.constructor"),
                4: sym(4, "k.ts", 40, "merge")}
        oracle = {"files": ["k.ts"], "callers": [caller, ctor, callee], "edges": [edge(caller, klass, callee_alts=["k.ts:20"]), edge(ctor, callee)]}
        g = impact.Graph(oracle, syms, "ts")
        self.assertEqual(set(g.closure(g.cn("k.ts:40"), 3)), {g.cn("k.ts:20"), g.cn("k.ts:5")})  # merge <- constructor <- create

    def test_bodyless_abstract_class_method_is_not_a_member(self):
        syms = {1: sym(1, "t.ts", 5, "_parse", "method", "T._parse"), 2: sym(2, "t.ts", 9, "run")}
        src = FakeSources({"t.ts": ["", "", "", "", "  abstract _parse(input: Input): Out;", "", "", "", "  run() {}"]})
        g = impact.Graph({"files": ["t.ts"], "callers": [decl("t.ts", 9, "run")], "edges": []}, syms, "ts", src=src)
        self.assertEqual(g.abstract, {g.cn("t.ts:5")})
        self.assertEqual(impact.members({"affected": [{"symbol": {"id": 1}}, {"symbol": {"id": 2}}]}, syms, g), {g.cn("t.ts:9")})


class DiagnosticClosureTests(unittest.TestCase):
    def test_trait_member_edge_expanded_to_overriding_implementations(self):
        caller, trait_m, impl_m = decl("a.rs", 10, "run"), decl("a.rs", 20, "m"), decl("a.rs", 30, "m")
        oracle = {"files": ["a.rs"], "callers": [caller], "edges": [edge(caller, trait_m)],
                  "hierarchy": {"overrides": [{"sub": impl_m, "base": trait_m}]}}
        syms = {1: sym(1, "a.rs", 10, "run"), 2: sym(2, "a.rs", 20, "m", "method", "T.m"), 3: sym(3, "a.rs", 30, "m", "method", "S.m")}
        plain = impact.Graph(oracle, syms, "rust")
        wide = impact.Graph(oracle, syms, "rust", expand=True)
        self.assertEqual(set(plain.closure(plain.cn("a.rs:30"), 3)), set())
        self.assertEqual(set(wide.closure(wide.cn("a.rs:30"), 3)), {wide.cn("a.rs:10")})
        self.assertEqual(set(wide.closure(wide.cn("a.rs:20"), 3)), {wide.cn("a.rs:10")})

    def test_go_closure_edges_can_be_left_out(self):
        f, lit_owner = decl("a.go", 10, "caller"), decl("a.go", 20, "MaximumNArgs")
        oracle = {"files": ["a.go"], "callers": [f, lit_owner], "edges": [edge(f, lit_owner, closure=True)]}
        syms = {1: sym(1, "a.go", 10, "caller"), 2: sym(2, "a.go", 20, "MaximumNArgs")}
        self.assertEqual(len(impact.Graph(oracle, syms, "go").closure("a.go|MaximumNArgs", 3)), 1)
        self.assertEqual(len(impact.Graph(oracle, syms, "go", drop_closure=True).closure("a.go|MaximumNArgs", 3)), 0)


class StatsTests(unittest.TestCase):
    def test_macro_average_is_not_dominated_by_a_big_seed(self):
        # one seed with 1000 pairs all right, nine seeds with 1 pair, none right
        per_seed = [(1000, 0, 0, False)] + [(0, 1, 1, False)] * 9
        pooled = impact.pooled([(t, f, n) for t, f, n, _ in per_seed])
        m = impact.macro(per_seed)
        self.assertGreater(pooled["precision"], 0.99)
        self.assertAlmostEqual(m["macro_precision"], 0.1)
        self.assertLessEqual(m["macro_recall_ci"][0], m["macro_recall"])
        self.assertGreaterEqual(m["macro_recall_ci"][1], m["macro_recall"])

    def test_bootstrap_is_reproducible(self):
        self.assertEqual(impact.boot_mean([0.1, 0.5, 0.9, 1.0]), impact.boot_mean([0.1, 0.5, 0.9, 1.0]))

    def test_lower_bound_two_by_two(self):
        mk = lambda lb, fn: {"seed": {}, "lidx_symbol": "s", "up": {"3": {"tp": 1, "fp": 0, "fn": fn, "oracle": 1 + fn, "lidx": 1, "ungradable": 0,
                                                                          "truncated": False, "lower_bound": lb}, "inf": {"tp": 1, "fp": 0, "fn": fn, "oracle": 1 + fn,
                                                                                                                         "lidx": 1, "ungradable": 0, "truncated": False,
                                                                                                                         "lower_bound": lb}},
                             "up_default": {}, "all_kinds": {}, "tests": {}, "down": {}}
        agg = impact.aggregate([mk(True, 1), mk(True, 0), mk(False, 1), mk(False, 0), mk(False, 0)], False)
        self.assertEqual(agg["lower_bound_honesty"]["3"], {"lb_true_incomplete": 1, "lb_true_complete": 1, "lb_false_incomplete": 1, "lb_false_complete": 2})


class SeedTests(unittest.TestCase):
    def test_draw_is_deterministic_stratified_and_skips_modules(self):
        nodes = [decl("f.go", n, "fn%d" % n) for n in range(1, 41)]
        edges = [edge(nodes[i], nodes[0]) for i in range(1, 15)] + [edge(nodes[20], nodes[1])]
        syms, g = build(edges, [(n, sym(n, "f.go", n, "fn%d" % n)) for n in range(1, 41)], callers=nodes)
        a, meta = impact.draw_seeds(g, "go/x/no_tests")
        b, _ = impact.draw_seeds(g, "go/x/no_tests")
        self.assertEqual(a, b)
        self.assertEqual(len(a), 40)  # fewer functions than seeds: all of them
        self.assertEqual(meta["stratum_pool"][">10"], 1)
        self.assertEqual({s["name"] for s in a if s["stratum"] == ">10"}, {"fn1"})
        c, _ = impact.draw_seeds(g, "go/y/no_tests")
        self.assertEqual(sorted(s["name"] for s in a), sorted(s["name"] for s in c))


if __name__ == "__main__":
    unittest.main()
