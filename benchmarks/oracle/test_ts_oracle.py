"""Runs the TypeScript oracle on a tiny fixture. Skipped when `node` or the oracle's typescript dependency is missing
(`cd benchmarks/oracle/ts_oracle && npm ci --ignore-scripts`)."""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TS = os.path.join(HERE, "ts_oracle")

SRC = """export class Base {
  constructor() {}
}
export class Sub extends Base {
  constructor() {
    super();
  }
}
export function make() {
  return new Base();
}
"""


@unittest.skipUnless(shutil.which("node") and os.path.isdir(os.path.join(TS, "node_modules", "typescript")),
                     "node or the oracle's typescript dependency is not installed")
class SuperCall(unittest.TestCase):
    def test_super_call_targets_the_class_like_new(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.ts"), "w") as f:
                f.write(SRC)
            r = subprocess.run(["node", os.path.join(TS, "oracle.mjs"), "--repo", d], capture_output=True, text=True, check=True)
            edges = {(e["caller"]["name"], e["caller"]["line"], e["callee"]["name"], e["callee"]["line"]) for e in json.loads(r.stdout)["edges"]}
        # super() in Sub's constructor (line 5) calls the class Base (line 1); so does `new Base()` in make (line 9)
        self.assertIn(("constructor", 5, "Base", 1), edges)
        self.assertIn(("make", 9, "Base", 1), edges)
        self.assertEqual(len(edges), 2, edges)


if __name__ == "__main__":
    unittest.main()
