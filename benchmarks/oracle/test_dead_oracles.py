"""End-to-end checks of the dead-code oracles on tiny fixtures. Skipped when a tool is missing."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import safety  # noqa: E402


def touch(path, text=""):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


@unittest.skipUnless(shutil.which("rust-analyzer") and shutil.which("rustc") and shutil.which("cargo"), "rust-analyzer, rustc or cargo missing")
class RustWorkspace(unittest.TestCase):
    """A library crate and a binary crate, each with unused `pub` and private items, through the sandboxed rust-analyzer and scip_dead.py."""

    def test_library_pub_leaves_the_universe_and_binary_pub_stays(self):
        with tempfile.TemporaryDirectory() as d:
            d = os.path.realpath(d)
            ws = os.path.join(d, "ws")
            touch(os.path.join(ws, "Cargo.toml"), '[workspace]\nmembers = ["lib", "app"]\nresolver = "2"\n')
            touch(os.path.join(ws, "lib", "Cargo.toml"), '[package]\nname = "toylib"\nversion = "0.1.0"\nedition = "2021"\n')
            touch(os.path.join(ws, "lib", "src", "lib.rs"),
                  "pub fn lib_pub_unused() {}\nfn lib_private_unused() {}\nfn lib_helper() {}\npub fn lib_pub_used() { lib_helper(); }\n"
                  "pub struct LibPubType;\nstruct LibPrivateType;\n"
                  # a test macro like ripgrep's `rgtest!`: the generated `#[test] fn` owns the references in its body
                  "#[cfg(test)]\nmacro_rules! mk_test { ($n:ident, $body:expr) => { #[test] fn $n() { helper_from_body(); $body } }; }\n"
                  "#[cfg(test)]\nfn helper_from_body() {}\n#[cfg(test)]\nfn helper_from_arg() {}\n"
                  "#[cfg(test)]\nmk_test!(generated_test, helper_from_arg());\n")
            touch(os.path.join(ws, "app", "Cargo.toml"),
                  '[package]\nname = "toyapp"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\ntoylib = { path = "../lib" }\n')
            touch(os.path.join(ws, "app", "src", "main.rs"),
                  "fn main() { toylib::lib_pub_used(); }\npub fn bin_pub_unused() {}\nfn bin_private_unused() {}\npub struct BinPubType;\n")
            subprocess.run(["cargo", "generate-lockfile", "-q"], cwd=ws, check=True)
            subprocess.run(["git", "init", "-q", ws], check=True)
            subprocess.run(["git", "-C", ws, "add", "."], check=True)
            subprocess.run(["git", "-C", ws, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "x"], check=True)
            cache = os.path.join(d, "cache")
            cfg = safety.rust_config(cache, "toy")
            target = os.path.join(cache, "target")
            scip = os.path.join(cache, "toy.scip")
            rc, bad = safety.run_watched(["rust-analyzer", "scip", ws, "--output", scip, "--config-path", cfg],
                                         env=safety.hardened_env(target), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.assertEqual(rc, 0)
            safety.assert_clean_run(bad, ws, target)
            r = subprocess.run([sys.executable, "-I", os.path.join(HERE, "rust_oracle", "scip_dead.py"), "--repo", ws, "--scip", scip],
                               capture_output=True, text=True, check=True)
        out = json.loads(r.stdout)
        by = {x["name"]: x for x in out["decls"]}
        self.assertTrue(out["library"])
        # library crate: `pub` items are public API (no verdict), private ones are judged
        for n in ("lib_pub_unused", "LibPubType"):
            self.assertTrue(by[n]["api"], n)
        self.assertFalse(by["lib_private_unused"]["api"])
        self.assertEqual(by["lib_private_unused"]["unref"], "dead")
        self.assertEqual(by["LibPrivateType"]["unref"], "dead")
        self.assertEqual(by["lib_helper"]["unref"], "live")
        # binary crate: `pub` is not API, so an unused pub item is judged (and dead)
        for n in ("bin_pub_unused", "BinPubType"):
            self.assertFalse(by[n]["api"], n)
            self.assertEqual(by[n]["unref"], "dead", n)
            self.assertTrue(by[n]["exported"], n)
        self.assertEqual(by["bin_private_unused"]["unref"], "dead")
        self.assertEqual(by["bin_pub_unused"]["reach"], "dead")
        self.assertEqual(by["main"].get("root"), "main_init")
        # the macro-generated test is a root (it cannot be classified), so what only it reaches is live
        self.assertEqual(by["generated_test"].get("root"), "macro_generated")
        self.assertEqual(by["helper_from_arg"]["reach"], "live")
        self.assertEqual(by["helper_from_body"]["reach"], "live")


if __name__ == "__main__":
    unittest.main()
