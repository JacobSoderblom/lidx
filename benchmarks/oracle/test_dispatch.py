"""Tests for the dispatch rules of the TypeScript and Rust oracles.
Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "rust_oracle"))
import scip_calls as sc  # noqa: E402

P = "rust-analyzer cargo ripgrep 15.2.0 "


class RustDispatch(unittest.TestCase):
    def test_trait_declared_method_is_dispatch_at_crate_root_and_in_modules(self):
        self.assertTrue(sc.is_trait_method(P + "Flag#name_short()."))
        self.assertTrue(sc.is_trait_method(P + "flags/defs/Flag#name_short()."))

    def test_inherent_impl_and_trait_impl_and_free_fn_are_static(self):
        self.assertFalse(sc.is_trait_method(P + "impl#[Opts]search_paths()."))  # crate root: used to pass the '/' split
        self.assertFalse(sc.is_trait_method(P + "flags/impl#[Opts]search_paths()."))
        self.assertFalse(sc.is_trait_method(P + "flags/defs/impl#[Unrestricted][Flag]name_short()."))
        self.assertFalse(sc.is_trait_method(P + "walk/stdout_handle()."))
        self.assertFalse(sc.is_trait_method(P + "stdout_handle()."))

    def test_tuple_struct_constructors_are_not_dispatch(self):
        self.assertFalse(sc.is_trait_method(P + "Match#"))
        self.assertFalse(sc.is_trait_method(P + "glob/Glob#"))

    def test_alts_come_from_impl_blocks(self):
        impl = P + "flags/defs/impl#[Unrestricted][Flag]name_short()."
        other = P + "flags/defs/impl#[Opts]name_short()."
        trait = P + "flags/defs/Flag#name_short()."
        idx = sc.trait_impl_index([impl, other, trait])
        self.assertEqual(idx[sc.trait_key(trait)], [impl])

    def test_cfg_test_regions(self):
        text = "fn a() {}\n#[cfg(test)]\nmod tests {\n    fn t() { let s = \"}\"; }\n}\nfn b() {}\n"
        self.assertEqual(sc.test_regions(text), [(1, 4)])
        field = "struct S {\n    #[cfg(test)]\n    pub(crate) x: u8,\n    y: u8,\n}\nimpl S {\n    fn real<A, B>(&self) where A: X, B: Y {}\n}\n"
        self.assertEqual(sc.test_regions(field), [(1, 2)])  # only the gated field, not the next fn body
        self.assertEqual(sc.short_name(P + "types()."), "types")
        self.assertTrue(sc.is_test_path("crates/x/tests/foo.rs"))
        self.assertFalse(sc.is_test_path("crates/x/src/foo.rs"))


FIXTURE = """\
interface Shape { area(): number }
export function useShape(s: Shape) { return s.area() }
export const make = (): number => 1;
export const alias: { (): number } = make;
export function callConst() { return make() }
export function callAlias() { return alias() }
export const build: (n: number) => number = (n) => n + 1;
export function callBuild() { return build(1) }
abstract class A { abstract go(): void }
export function callAbs(a: A) { a.go() }
export class Base { constructor(x: number) { void x } }
export class Sub extends Base {}
export function mk() { return new Sub(1) }
export const objectKeys: ObjectConstructor["keys"] = (o: object) => Object.keys(o);
export function useKeys() { return objectKeys({}) }
"""


@unittest.skipUnless(shutil.which("node") and os.path.isdir(os.path.join(HERE, "ts_oracle", "node_modules", "typescript")),
                     "node and the pinned typescript are needed")
class TsDispatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        os.makedirs(os.path.join(cls.tmp, "src"))
        with open(os.path.join(cls.tmp, "src", "a.ts"), "w") as f:
            f.write(FIXTURE)
        with open(os.path.join(cls.tmp, "tsconfig.json"), "w") as f:
            f.write('{"compilerOptions":{"strict":true,"target":"ES2022"}}')
        out = subprocess.run(["node", os.path.join(HERE, "ts_oracle", "oracle.mjs"), "--repo", cls.tmp, "--subdir", "src"],
                             capture_output=True, text=True, check=True).stdout
        cls.edges = {(e["caller"]["name"], e["callee"]["name"]): e for e in json.loads(out)["edges"]}

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp)

    def test_interface_member_and_abstract_method_are_dispatch(self):
        self.assertTrue(self.edges[("useShape", "area")].get("dispatch"))
        self.assertTrue(self.edges[("callAbs", "go")].get("dispatch"))

    def test_const_with_one_initialiser_is_static_even_if_typed_through_a_type(self):
        self.assertFalse(self.edges[("callConst", "make")].get("dispatch"))
        self.assertFalse(self.edges[("callBuild", "build")].get("dispatch"))
        self.assertFalse(self.edges[("callAlias", "alias")].get("dispatch"))

    def test_const_typed_through_a_library_type_is_the_const_not_the_library(self):
        self.assertIn(("useKeys", "objectKeys"), self.edges)
        self.assertFalse(self.edges[("useKeys", "objectKeys")].get("dispatch"))

    def test_new_of_a_subclass_without_constructor_points_at_the_class(self):
        self.assertIn(("mk", "Sub"), self.edges)
        self.assertNotIn(("mk", "constructor"), self.edges)


if __name__ == "__main__":
    unittest.main()
