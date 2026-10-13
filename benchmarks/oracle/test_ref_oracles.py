"""The value-reference sets the Go and TypeScript oracles emit on small known inputs (issue #452). The Rust oracle's own test is the marker
crate in test_safety.py (and the synthetic SCIP index in test_match.py). Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
GO = os.path.join(HERE, "go_oracle")
TS = os.path.join(HERE, "ts_oracle")


def triples(refs):
    return {(r["caller"]["name"], r["caller"]["line"], r["callee"]["name"], r["callee"]["line"]) for r in refs}


@unittest.skipUnless(shutil.which("go"), "go missing")
class GoValueReferences(unittest.TestCase):
    def test_fixture_package(self):
        fixture = os.path.join(GO, "testdata", "refs")
        with tempfile.TemporaryDirectory() as d:
            exe = os.path.join(d, "gooracle")
            subprocess.run(["go", "build", "-o", exe, "."], cwd=GO, check=True)
            env = dict(os.environ, GOFLAGS="-mod=readonly")
            r = subprocess.run([exe, "-repo", fixture], capture_output=True, text=True, check=True, env=env)
        out = json.loads(r.stdout)
        refs = out["refs"]
        got = triples(refs)
        # package level (key line 0): the struct-literal field handler `RunE: run` and the map value `"a": a`
        self.assertIn(("<module>", 0, "run", 21), got)
        self.assertIn(("<module>", 0, "a", 27), got)
        # a method value passed as an argument, a method expression, an interface method value, a generic instance, a field handler in main
        self.assertIn(("wire", 14, "handle", 10), got)
        self.assertIn(("wire", 14, "Static", 12), got)
        self.assertIn(("main", 37, "Speak", 19), got)
        self.assertIn(("main", 37, "handle", 10), got)
        self.assertIn(("main", 37, "ident", 35), got)
        self.assertIn(("main", 37, "run", 21), got)
        by = {(r["caller"]["name"], r["callee"]["name"]): r for r in refs}
        self.assertEqual(by[("wire", "handle")]["forms"], ["method_value"])
        self.assertEqual(by[("wire", "Static")]["forms"], ["method_expr"])
        self.assertEqual(by[("main", "Speak")]["forms"], ["iface_method_value"])
        self.assertEqual(by[("<module>", "run")]["ctxs"], ["struct_field"])
        self.assertEqual(by[("<module>", "a")]["ctxs"], ["map_value"])
        # `both()` is called and passed on: one pair, flagged as also called; a bare call or a go/defer of a call is never a reference
        self.assertTrue(by[("main", "both")]["called"])
        self.assertNotIn(("main", "register"), by)  # `register(b)` calls register
        self.assertNotIn(("main", "Map"), by)
        self.assertEqual(len(refs), 11, sorted(got))
        # the callers the references were judged against
        self.assertIn(("main", 37), {(c["name"], c["line"]) for c in out["ref_callers"]})


TS_SRC = """export function fmt(x: number): string {
  return String(x);
}
export function both(): void {}
export class Foo {
  static create(): Foo {
    return new Foo();
  }
  onX(): void {}
  wire(emitter: { on(n: string, f: () => void): void }): void {
    emitter.on("x", this.onX);
    emitter.on("y", this.onX.bind(this));
  }
}
function handle(): void {}
const arrow = (): void => {};
export const handlers = { onClick: handle, arrow };
export const mapped = [1, 2].map(fmt);
export const made = [1].map(Foo.create);
export function use(v: unknown): boolean {
  both();
  const again = both;
  const t: typeof fmt = fmt;
  fmt.call(null, 1);
  new Foo();
  return v instanceof Foo && again !== t;
}
let swap = (): void => {};
swap = (): void => {};
"""
TS_B = """import { fmt, Foo, both } from "./a";
export { fmt };
export default both;
export const viaImport = [3].map(fmt);
export class Bar extends Foo {}
"""


@unittest.skipUnless(shutil.which("node") and os.path.isdir(os.path.join(TS, "node_modules", "typescript")),
                     "node or the oracle's typescript dependency is not installed")
class TypeScriptValueReferences(unittest.TestCase):
    def test_fixture_file(self):
        with tempfile.TemporaryDirectory() as d:
            for name, text in (("a.ts", TS_SRC), ("b.ts", TS_B)):
                with open(os.path.join(d, name), "w") as f:
                    f.write(text)
            r = subprocess.run(["node", os.path.join(TS, "oracle.mjs"), "--repo", d], capture_output=True, text=True, check=True)
        out = json.loads(r.stdout)
        refs = out["refs"]
        got = {(r["caller"]["name"], r["callee"]["name"], r["callee"]["line"]) for r in refs}
        self.assertEqual(got, {
            ("<module>", "fmt", 1),       # [1, 2].map(fmt), and b.ts: [3].map(fmt)
            ("<module>", "create", 6),    # [1].map(Foo.create): a static method used as a callback
            ("<module>", "handle", 15),   # object-literal callback `{ onClick: handle }`
            ("<module>", "arrow", 16),    # shorthand `{ arrow }`: a const with a function initialiser
            ("wire", "onX", 9),           # emitter.on("x", this.onX) and this.onX.bind(this)
            ("use", "fmt", 1),            # const t: typeof fmt = fmt  (the `typeof fmt` is a type position)
            ("use", "both", 4),           # const again = both
            ("use", "Foo", 5),            # v instanceof Foo
            ("<module>", "both", 4),      # b.ts: export default both;  (`export { fmt }` is a specifier and is not)
        })
        by = {(r["caller"]["name"], r["callee"]["name"]): r for r in refs}
        self.assertEqual(by[("<module>", "handle")]["ctxs"], ["object_property"])
        self.assertEqual(by[("<module>", "arrow")]["forms"], ["shorthand"])
        self.assertEqual(by[("wire", "onX")]["forms"], ["this_member"])
        self.assertEqual(by[("<module>", "arrow")]["callee_kind"], "variable")
        self.assertEqual(by[("<module>", "both")]["ctxs"], ["export_default"])
        # called as well as passed: the pair is a call (`both()`, `fmt.call(..)`, `new Foo()`)
        self.assertTrue(by[("use", "both")]["called"])
        self.assertTrue(by[("use", "fmt")]["called"])
        self.assertTrue(by[("use", "Foo")]["called"])
        self.assertNotIn("called", by[("<module>", "handle")])
        # excluded positions: the callee, `Foo.create()`'s base, the import and export specifiers, `extends Foo`, type positions
        d = out["refs_dropped"]
        self.assertGreaterEqual(d.get("type_position", 0), 2)  # `typeof fmt`, `extends Foo`
        self.assertEqual(d.get("member_access_base"), 1)  # `Foo.create` inside the map call
        self.assertEqual(d.get("assignment_target"), 1)  # `swap = ..` writes the variable
        self.assertNotIn("swap", {r["callee"]["name"] for r in refs})


if __name__ == "__main__":
    unittest.main()
