"""The attribute / `macro_rules!` site gate for Rust value references (issue #514). Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
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
"""


class Ranges(unittest.TestCase):
    def test_attributes_and_macro_definitions_are_found(self):
        r = tt.token_tree_ranges(SRC)
        self.assertIn((4, 4), r)  # #[derive(..)]
        self.assertIn((6, 6), r)  # #[serde(default = "..")]
        self.assertIn((8, 10), r)  # a multi-line attribute
        self.assertIn((13, 17), r)  # the whole macro_rules! body
        self.assertFalse(tt.lines_in_token_trees(r, [20]))  # `run(default_depth)` is ordinary code
        self.assertFalse(tt.lines_in_token_trees(r, [6, 20]))  # every line must be inside
        self.assertFalse(tt.lines_in_token_trees(r, []))

    def test_brackets_in_strings_and_comments_do_not_confuse_the_scan(self):
        src = '#[doc = "a ] b"]\nfn f() {}\n// #[not_an_attribute]\nfn g() { let x = v[0]; }\n'
        self.assertEqual(tt.token_tree_ranges(src), [(1, 1)])

    def test_spelling_and_qualification(self):
        lines = SRC.split("\n")
        self.assertTrue(tt.spells(lines, [6], "default_depth"))
        self.assertFalse(tt.spells(lines, [6], "default_dep"))  # a whole identifier only
        self.assertFalse(tt.spells(lines, [6, 7], "default_depth"))
        self.assertTrue(tt.qualified_by(lines, [9], "parse", "crate::keyed::parse"))
        self.assertFalse(tt.qualified_by(lines, [9], "parse", "crate::other::parse"))  # written `keyed::`, callee is not in `keyed`
        self.assertFalse(tt.qualified_by(lines, [6], "default_depth", "crate::default_depth"))  # not written with a path


class Gate(unittest.TestCase):
    FILES = {"src/lib.rs": SRC, "src/b.rs": "fn default_depth() {}\n"}

    def gate(self):
        return tt.SiteGate(lambda p: self.FILES[p])

    def grade(self, edges, syms, oracle=None):
        oracle = oracle or roracle(["src/lib.rs", "src/b.rs", "src/keyed.rs", "src/other.rs"], [], callers=[{"file": "src/lib.rs", "line": 18, "name": "plain"}])
        return refs.grade_refs(oracle, syms, edges, "rust", match.Sources("/nonexistent"), site_gate=self.gate())

    def syms(self):
        s = {1: rsym("src/lib.rs", 1, "lib", "module"), 2: rsym("src/lib.rs", 2, "default_depth"), 3: rsym("src/lib.rs", 3, "other"),
             4: rsym("src/b.rs", 1, "default_depth"), 5: rsym("src/lib.rs", 18, "plain")}
        s[1]["key"] = "src/lib.rs:0"
        return s

    def test_a_reference_the_compiler_cannot_see_is_ungradable_not_wrong(self):
        summ, fn, fp, gap, ungr, _ = self.grade([redge(1, 2, 6, path="src/lib.rs")], self.syms())
        self.assertEqual((summ["fp"], summ["ungradable"]), (0, {"token_tree_site": 1}))

    def test_a_reference_in_a_macro_definition_is_ungradable(self):
        syms = self.syms()
        syms[6] = rsym("src/lib.rs", 40, "compare_len")
        summ, *_ = self.grade([redge(5, 6, 15, path="src/lib.rs")], syms)
        self.assertEqual((summ["fp"], summ["ungradable"]), (0, {"token_tree_site": 1}))

    def test_a_site_that_names_another_function_stays_wrong(self):
        """The attribute names `default_depth`; lidx bound the pair to `other`. No verdict is hidden: the line does not spell `other`."""
        summ, fn, fp, *_ = self.grade([redge(1, 3, 6, path="src/lib.rs")], self.syms())
        self.assertEqual((summ["fp"], summ["ungradable"]), (1, {}))
        self.assertEqual(fp[0]["category"], "no_reference_at_site")

    def test_ordinary_code_stays_graded(self):
        summ, fn, fp, *_ = self.grade([redge(5, 2, 20, path="src/lib.rs")], self.syms())
        self.assertEqual((summ["fp"], summ["ungradable"]), (1, {}))

    def test_the_same_name_bound_to_a_declaration_the_site_cannot_mean_stays_wrong(self):
        """Two `default_depth` in the repository: the bare name in `src/lib.rs` means the one in the same file; bound to `src/b.rs` it is wrong."""
        summ, fn, fp, *_ = self.grade([redge(1, 4, 6, path="src/lib.rs")], self.syms())
        self.assertEqual((summ["fp"], summ["ungradable"]), (1, {}))

    def test_a_written_path_that_agrees_with_the_callee_binds_it(self):
        syms = self.syms()
        syms[7] = rsym("src/keyed.rs", 3, "parse", qual="crate.keyed.parse")
        syms[8] = rsym("src/other.rs", 3, "parse", qual="crate.other.parse")
        right, *_ = self.grade([redge(1, 7, 9, path="src/lib.rs")], syms)
        wrong, *_ = self.grade([redge(1, 8, 9, path="src/lib.rs")], syms)
        self.assertEqual(right["ungradable"], {"token_tree_site": 1})
        self.assertEqual((wrong["fp"], wrong["ungradable"]), (1, {}))

    def test_a_pair_the_compiler_confirmed_is_untouched(self):
        oracle = roracle(["src/lib.rs"], [rref(("src/lib.rs", 0, "<module>"), ("src/lib.rs", 2, "default_depth"), site=6)], callers=[])
        summ, *_ = self.grade([redge(1, 2, 6, path="src/lib.rs")], self.syms(), oracle)
        self.assertEqual((summ["tp"], summ["ungradable"]), (1, {}))


if __name__ == "__main__":
    unittest.main()
