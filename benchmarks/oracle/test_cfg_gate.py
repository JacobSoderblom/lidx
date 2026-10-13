"""Tests for the cfg-gate rule (rust_cfg.py + match.grade's `cfg_gated_evidence` bucket) and the Go closure self-edge filter.
Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import match  # noqa: E402
import rust_cfg  # noqa: E402

MAC = "debug_assertions\nunix\ntarget_arch=\"aarch64\"\ntarget_endian=\"little\"\ntarget_env=\"\"\ntarget_family=\"unix\"\n" \
      "target_os=\"macos\"\ntarget_pointer_width=\"64\"\ntarget_vendor=\"apple\"\n"
HOST_ALL = rust_cfg.host_from_rustc(MAC, "all")
HOST_DEF = rust_cfg.host_from_rustc(MAC, "default")


def ev(pred, host=HOST_ALL):
    return rust_cfg.evaluate(rust_cfg.parse_pred(pred), host)


class Predicates(unittest.TestCase):
    def test_platform_predicates(self):
        self.assertIs(ev("windows"), False)
        self.assertIs(ev("unix"), True)
        self.assertIs(ev('target_os = "windows"'), False)
        self.assertIs(ev('target_os = "macos"'), True)
        self.assertIs(ev("not(unix)"), False)
        self.assertIs(ev('any(target_os = "linux", target_os = "android")'), False)
        self.assertIs(ev('all(unix, not(target_os = "linux"))'), True)
        self.assertIs(ev('target_env = "msvc"'), False)

    def test_features(self):
        self.assertIs(ev('feature = "pcre2"', HOST_ALL), True)  # everything is on, so nothing behind a feature is gated off
        self.assertIsNone(ev('feature = "pcre2"', HOST_DEF))  # default features: which ones are on is not decided here

    def test_unknown_never_becomes_false(self):
        for p in ("test", "debug_assertions", 'panic = "abort"', "miri", 'target_has_atomic = "64"', "doc"):
            self.assertIsNone(ev(p), p)
        self.assertIsNone(ev("any(windows, test)"))  # False or unknown is unknown
        self.assertIs(ev("all(windows, test)"), False)  # False and unknown is False
        self.assertIsNone(ev("not(test)"))
        self.assertIsNone(rust_cfg.evaluate(rust_cfg.parse_pred("garbage ((("), HOST_ALL))


def ranges(src, host=HOST_ALL):
    return rust_cfg.gated_ranges(src, host)


class Regions(unittest.TestCase):
    def test_gated_function_and_its_body(self):
        src = "fn a() {}\n#[cfg(windows)]\nfn w() {\n    helper();\n}\nfn b() {}\n"
        self.assertEqual(ranges(src), [(2, 5)])

    def test_ungated_code_is_never_touched(self):
        src = "fn a() { helper(); }\n#[inline]\nfn b() {}\n#[cfg(unix)]\nfn c() { helper(); }\n#[cfg(test)]\nmod t { fn x() {} }\n"
        self.assertEqual(ranges(src), [])

    def test_gated_mod_impl_and_statements(self):
        src = ('#[cfg(not(unix))]\nmod win {\n    fn a() { f(); }\n}\n'
               'fn main() {\n    #[cfg(windows)]\n    {\n        g();\n    }\n    #[cfg(windows)]\n    let x = h();\n    k();\n}\n'
               '#[cfg(target_os = "linux")]\nimpl S {\n    fn m(&self) {}\n}\n')
        self.assertEqual(ranges(src), [(1, 4), (6, 9), (10, 11), (14, 17)])

    def test_brackets_in_strings_comments_and_chars_are_inert(self):
        src = '#[cfg(windows)]\nfn w() {\n    let s = "}{ ; ";\n    // } oops\n    let c = \'}\';\n    let l: &\'static str = r#"}"#;\n}\nfn after() { f(); }\n'
        self.assertEqual(ranges(src), [(1, 7)])

    def test_match_arm_and_field_are_gated_alone(self):
        src = 'fn f(x: u8) {\n    match x {\n        #[cfg(windows)]\n        1 => g(),\n        _ => h(),\n    }\n}\n'
        self.assertEqual(ranges(src), [(3, 4)])

    def test_attribute_stack_and_multiline_attribute(self):
        src = '#[cfg(any(\n    windows,\n    target_os = "linux"\n))]\n#[allow(dead_code)]\npub(crate) unsafe fn w() {\n    f();\n}\nfn z() {}\n'
        self.assertEqual(ranges(src), [(1, 8)])

    def test_inner_attribute_gates_the_file(self):
        src = "//! docs\n#![cfg(windows)]\nfn w() { f(); }\n"
        self.assertEqual(ranges(src), [(1, 4)])
        self.assertEqual(ranges("fn w() {}\n#![cfg(windows)]\n"), [])  # not at the top: not a file attribute, left alone

    def test_unparseable_extent_gates_nothing(self):
        self.assertEqual(ranges("#[cfg(windows)]\nfn broken() {\n    f();\n"), [])

    def test_feature_gate_depends_on_the_configuration(self):
        src = '#[cfg(feature = "x")]\nfn a() {}\n#[cfg(not(feature = "x"))]\nfn b() { g(); }\n'
        self.assertEqual(ranges(src, HOST_ALL), [(3, 4)])
        self.assertEqual(ranges(src, HOST_DEF), [])

    def test_lines_gated_needs_every_line(self):
        self.assertTrue(rust_cfg.lines_gated([(2, 5)], [3, 4]))
        self.assertFalse(rust_cfg.lines_gated([(2, 5)], [3, 9]))  # one site outside the gate: the pair stays graded
        self.assertFalse(rust_cfg.lines_gated([(2, 5)], []))


def sym(i, path, line, name, kind="function"):
    return {"file": path, "line": line, "name": name, "qualname": name, "kind": kind, "key": "%s:%d" % (path, line)}


class UngradableBucket(unittest.TestCase):
    """A lidx edge whose evidence lies in a gated item becomes ungradable instead of a false positive."""

    def run_grade(self, gate):
        syms = {1: sym(1, "a.rs", 1, "caller"), 2: sym(2, "a.rs", 10, "callee"), 3: sym(3, "a.rs", 20, "other")}
        edges = [{"kind": "CALLS", "s": 1, "t": 2, "target_qualname": "callee", "resolution_kind": "exact", "line": 4, "end_line": 4, "epath": "a.rs"},
                 {"kind": "CALLS", "s": 1, "t": 3, "target_qualname": "other", "resolution_kind": "exact", "line": 5, "end_line": 5, "epath": "a.rs"}]
        oracle = {"files": ["a.rs"], "callers": [{"file": "a.rs", "line": 1, "name": "caller"}], "edges": [], "exhaustive_callers": True}
        return match.grade(oracle, syms, edges, "rust", match.Sources("/nonexistent"), cfg_gate=gate)

    def test_gated_evidence_is_ungradable_not_a_false_positive(self):
        summ, _, fp, _, ungr = self.run_grade(lambda path, lines: path == "a.rs" and lines == [4])
        self.assertEqual(summ["ungradable"], {"cfg_gated_evidence": 1})
        self.assertEqual(summ["fp"], 1)  # the ungated edge at line 5 is still graded, and still wrong here
        self.assertEqual([u["reason"] for u in ungr], ["cfg_gated_evidence"])
        self.assertEqual([f["callee"]["name"] for f in fp], ["other"])

    def test_without_a_gate_nothing_changes(self):
        summ, _, fp, _, _ = self.run_grade(None)
        self.assertEqual(summ["fp"], 2)
        self.assertEqual(summ["ungradable"], {})

    def test_a_matched_edge_is_kept_even_when_gated(self):
        syms = {1: sym(1, "a.rs", 1, "caller"), 2: sym(2, "a.rs", 10, "callee")}
        edges = [{"kind": "CALLS", "s": 1, "t": 2, "target_qualname": "callee", "resolution_kind": "exact", "line": 4, "end_line": 4, "epath": "a.rs"}]
        oracle = {"files": ["a.rs"], "callers": [{"file": "a.rs", "line": 1}], "exhaustive_callers": True,
                  "edges": [{"caller": {"file": "a.rs", "line": 1, "name": "caller"}, "callee": {"file": "a.rs", "line": 10, "name": "callee"}, "sites": ["a.rs:4"]}]}
        summ = match.grade(oracle, syms, edges, "rust", match.Sources("/nonexistent"), cfg_gate=lambda p, l: True)[0]
        self.assertEqual((summ["tp"], summ["fp"], summ["ungradable"]), (1, 0, {}))


def edge(cf, ct, closure_self=None, callee_line=None):
    d = {"caller": {"file": "f.go", "line": cf, "name": "F"}, "callee": {"file": "f.go", "line": callee_line or ct, "name": "G"}, "sites": ["f.go:%d" % cf]}
    if closure_self:
        d["closure_self"] = True
    return d


class GoClosureSelfEdges(unittest.TestCase):
    def test_flagged_self_edges_leave_the_oracle_and_are_counted(self):
        o = {"edges": [edge(10, 10, closure_self=True), edge(20, 20), edge(30, 40, closure_self=True), edge(5, 9)], "files": ["f.go"]}
        out, n = match.drop_closure_self(o)
        self.assertEqual(n, 1)
        self.assertEqual([(e["caller"]["line"], e["callee"]["line"]) for e in out["edges"]], [(20, 20), (30, 40), (5, 9)])
        self.assertEqual(len(o["edges"]), 4)  # the input is not modified

    def test_genuine_recursion_stays_graded(self):
        out, n = match.drop_closure_self({"edges": [edge(20, 20)]})
        self.assertEqual((n, len(out["edges"])), (0, 1))

    def test_a_flagged_edge_between_two_declarations_is_not_a_self_edge(self):
        out, n = match.drop_closure_self({"edges": [edge(30, 40, closure_self=True)]})
        self.assertEqual((n, len(out["edges"])), (0, 1))

    def test_dropped_edge_leaves_the_recall_denominator(self):
        syms = {1: sym(1, "f.go", 10, "F")}
        oracle = {"files": ["f.go"], "callers": [{"file": "f.go", "line": 10, "name": "F"}], "edges": [edge(10, 10, closure_self=True)]}
        with_edge = match.grade(oracle, syms, [], "go", match.Sources("/nonexistent"))[0]
        o2, _ = match.drop_closure_self(oracle)
        without = match.grade(o2, syms, [], "go", match.Sources("/nonexistent"))[0]
        self.assertEqual((with_edge["oracle_edges"], without["oracle_edges"]), (1, 0))


if __name__ == "__main__":
    unittest.main()
