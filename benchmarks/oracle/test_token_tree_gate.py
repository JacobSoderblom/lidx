"""The attribute / `macro_rules!` site gate for Rust value references (issues #514, review of #515). Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import unittest

import match
import refs
import rust_token_trees as tt
from test_match import redge, rref, roracle, rsym

SRC = """use serde::Deserialize;
fn default_depth() -> u32 { 1 }
fn other() {}
#[derive(Deserialize)]
struct Cfg {
    #[serde(default = "default_depth")]
    depth: u32,
    #[serde(
        deserialize_with = "keyed::parse"
    )]
    k: u8,
}
macro_rules! sort_prop {
    ($p:expr) => {{
        v.sort_unstable_by(compare_len)
    }};
}
fn plain() {
    let a = [1];
    run(default_depth);
}
#[inline] fn go() { default_depth(); }
#[doc = "see default_depth for details"]
fn documented() {}
#[cfg(feature = "default_depth")]
fn gated() {}
struct R {
    #[serde(rename = "default_depth")]
    r: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    s: u8,
    #[serde(skip_serializing_if = "is_zero")]
    z: u8,
}
mod a {
    pub fn f() {}
}
mod b {
    pub struct S {
        #[serde(default = "f")]
        pub x: u8,
    }
}
"""

def line_of(needle, nth=1):
    hits = [i + 1 for i, l in enumerate(SRC.split("\n")) if needle in l]
    return hits[nth - 1]


class Scan(unittest.TestCase):
    def test_token_ranges_and_spellings(self):
        s = tt.Source(SRC)
        ln = line_of('default = "default_depth"')
        off = s.occurrences(ln, "default_depth")[0]
        self.assertEqual(s.spelling(off, "default_depth"), ("path", []))  # a path-valued string under a path key
        off = s.occurrences(line_of("keyed::parse"), "parse")[0]
        self.assertEqual(s.spelling(off, "parse"), ("path", ["keyed"]))
        off = s.occurrences(line_of("compare_len"), "compare_len")[0]
        self.assertEqual(s.spelling(off, "compare_len"), ("path", []))  # inside a macro_rules! body
        off = s.occurrences(line_of("run(default_depth)"), "default_depth")[0]
        self.assertIsNone(s.spelling(off, "default_depth"))  # ordinary code

    def test_brackets_in_strings_and_comments_do_not_confuse_the_scan(self):
        s = tt.Source('#[doc = "a ] b"]\nfn f() {}\n// #[not_an_attribute]\nfn g() { let x = v[0]; }\n')
        self.assertEqual(len(s.attrs), 1)


class Gate(unittest.TestCase):
    FILES = {"src/lib.rs": SRC, "src/b.rs": "fn default_depth() {}\n", "src/keyed.rs": "pub fn parse() {}\n"}

    def gate(self):
        return tt.SiteGate(lambda p: self.FILES[p])

    def syms(self):
        s = {1: rsym("src/lib.rs", 1, "lib", "module", "crate"), 2: rsym("src/lib.rs", 2, "default_depth", qual="crate::default_depth"),
             3: rsym("src/lib.rs", 3, "other", qual="crate::other"), 4: rsym("src/b.rs", 1, "default_depth", qual="crate::b::default_depth"),
             5: rsym("src/lib.rs", 19, "plain", qual="crate::plain")}
        s[1]["key"] = "src/lib.rs:0"
        return s

    def grade(self, edges, syms, files=("src/lib.rs", "src/b.rs", "src/keyed.rs", "src/other.rs")):
        oracle = roracle(list(files), [], callers=[{"file": "src/lib.rs", "line": 19, "name": "plain"}])
        return refs.grade_refs(oracle, syms, edges, "rust", match.Sources("/nonexistent"), site_gate=self.gate())[0]

    def verdict(self, line, callee_id, syms=None):
        syms = syms or self.syms()
        summ = self.grade([redge(1, callee_id, line, path="src/lib.rs")], syms)
        return "ungradable" if summ["ungradable"] == {"token_tree_site": 1} else "wrong" if summ["fp"] == 1 else summ

    def test_a_reference_the_compiler_cannot_see_is_ungradable_not_wrong(self):
        self.assertEqual(self.verdict(line_of('default = "default_depth"'), 2), "ungradable")

    def test_a_reference_in_a_macro_definition_is_ungradable(self):
        syms = self.syms()
        syms[6] = rsym("src/lib.rs", 40, "compare_len", qual="crate::compare_len")
        summ = self.grade([redge(5, 6, line_of("compare_len"), path="src/lib.rs")], syms)
        self.assertEqual((summ["fp"], summ["ungradable"]), (0, {"token_tree_site": 1}))

    def test_a_site_that_names_another_function_stays_wrong(self):
        self.assertEqual(self.verdict(line_of('default = "default_depth"'), 3), "wrong")

    def test_ordinary_code_stays_graded(self):
        self.assertEqual(self.verdict(line_of("run(default_depth)"), 2), "wrong")

    def test_the_same_name_bound_to_a_declaration_the_site_cannot_mean_stays_wrong(self):
        """Two `default_depth` in the repository: the bare name in `src/lib.rs` means the one in the same file; bound to `src/b.rs` it is wrong."""
        self.assertEqual(self.verdict(line_of('default = "default_depth"'), 4), "wrong")

    # ---- review of #515 -------------------------------------------------------------
    def test_a_written_path_must_agree_even_when_the_name_is_unique(self):
        """`skip_serializing_if = "Option::is_none"` is std's; a repo `Mine::is_none` is the only one of that name, and still not it."""
        syms = self.syms()
        syms[7] = rsym("src/lib.rs", 50, "is_none", "method", "crate::Mine::is_none")
        self.assertEqual(self.verdict(line_of("Option::is_none"), 7, syms), "wrong")
        syms[8] = rsym("src/lib.rs", 51, "is_none", "method", "crate::Option::is_none")
        self.assertEqual(self.verdict(line_of("Option::is_none"), 8, syms), "ungradable")  # the same path agrees

    def test_path_segments_are_a_suffix_in_order_not_a_set(self):
        syms = self.syms()
        syms[7] = rsym("src/keyed.rs", 1, "parse", qual="crate::b::o::parse")  # `keyed::parse` written; `keyed` is nowhere in the callee path
        syms[8] = rsym("src/keyed.rs", 1, "parse", qual="crate::keyed::parse")
        syms[9] = rsym("src/keyed.rs", 2, "parse", qual="crate::keyed::o::parse")  # contains `keyed`, but it is not the last module
        self.assertEqual(self.verdict(line_of("keyed::parse"), 7, syms), "wrong")
        self.assertEqual(self.verdict(line_of("keyed::parse"), 9, syms), "wrong")
        self.assertEqual(self.verdict(line_of("keyed::parse"), 8, syms), "ungradable")

    def test_bare_super_needs_the_parent_module_or_a_unique_name(self):
        files = dict(self.FILES)
        files["src/lib.rs"] = "mod inner;\n"
        files["src/inner.rs"] = '#[serde(default, deserialize_with = "super::target")]\nfn x() {}\n'
        files["src/other.rs"] = "fn target() {}\n"
        files["src/elsewhere.rs"] = "fn target() {}\n"
        gate = tt.SiteGate(lambda p: files[p])

        def run(callee_file, qual, unique):
            syms = {1: rsym("src/inner.rs", 0, "inner", "module", "crate::inner"), 2: rsym(callee_file, 1, "target", qual=qual)}
            syms[1]["key"] = "src/inner.rs:0"
            callee = {k: syms[2][k] for k in ("name", "qualname", "file", "line", "kind")}
            return gate("src/inner.rs", [1], callee, unique, "crate::inner")
        self.assertTrue(run("src/lib.rs", "crate::target", False))  # `super` of crate::inner is the crate root
        self.assertFalse(run("src/other.rs", "crate::other::target", False))
        self.assertTrue(run("src/other.rs", "crate::other::target", True))  # a unique name can mean nothing else

    def test_inline_module_scope_is_respected(self):
        """`mod a { fn f }` and `mod b { #[serde(default = "f")] }`: the bare `f` of b cannot be the one in a, when another `f` exists."""
        files = {"src/lib.rs": 'mod a {\n    pub fn f() {}\n}\nmod b {\n    #[serde(default = "f")]\n    pub x: u8,\n    fn f() {}\n}\nfn f() {}\n'}
        gate = tt.SiteGate(lambda p: files[p])
        cal = lambda line, q: {"name": "f", "qualname": q, "file": "src/lib.rs", "line": line, "kind": "function"}  # noqa: E731
        self.assertFalse(gate("src/lib.rs", [5], cal(2, "crate::a::f"), False))
        self.assertFalse(gate("src/lib.rs", [5], cal(9, "crate::f"), False))
        self.assertTrue(gate("src/lib.rs", [5], cal(7, "crate::b::f"), False))

    def test_code_sharing_a_line_with_an_attribute_stays_graded(self):
        """`#[inline] fn go() { default_depth(); }`: the name is outside the attribute's tokens."""
        self.assertEqual(self.verdict(line_of("#[inline] fn go()"), 2), "wrong")

    def test_prose_and_feature_strings_do_not_pass(self):
        self.assertEqual(self.verdict(line_of("#[doc ="), 2), "wrong")
        self.assertEqual(self.verdict(line_of('#[cfg(feature = "default_depth")]'), 2), "wrong")
        self.assertEqual(self.verdict(line_of('rename = "default_depth"'), 2), "wrong")

    def test_a_pair_the_compiler_confirmed_is_untouched(self):
        oracle = roracle(["src/lib.rs"], [rref(("src/lib.rs", 0, "<module>"), ("src/lib.rs", 2, "default_depth"), site=6)], callers=[])
        summ = refs.grade_refs(oracle, self.syms(), [redge(1, 2, 6, path="src/lib.rs")], "rust", match.Sources("/nonexistent"), site_gate=self.gate())[0]
        self.assertEqual((summ["tp"], summ["ungradable"]), (1, {}))


if __name__ == "__main__":
    unittest.main()
