"""Tests for the guards that keep the oracles from executing corpus code. Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import safety  # noqa: E402


def touch(path, text=""):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


class Detection(unittest.TestCase):
    def test_forbidden_process_command_lines(self):
        bad = ["/x/target/debug/build/bat-0123456789abcdef/build-script-main",
               "/r/libexec/rust-analyzer-proc-macro-srv",
               "/r/bin/cargo check --workspace --message-format=json", "cargo -q build"]
        ok = ["/r/bin/cargo metadata --format-version 1", "rustc --print sysroot", "rust-analyzer scip /repo", "/usr/bin/true"]
        for c in bad:
            self.assertTrue(safety.FORBIDDEN_PROCESS.search(c), c)
        for c in ok:
            self.assertFalse(safety.FORBIDDEN_PROCESS.search(c), c)

    def test_allowed_processes_are_not_flagged(self):
        # the probes the oracle needs must never trip the guard, or every run would fail
        for c in ["cargo metadata --format-version 1 --all-features --filter-platform aarch64-apple-darwin",
                  "cargo locate-project", "/r/bin/rustc -vV", "rustc - --crate-name ___ --print=file-names --print=cfg",
                  "rustc --print sysroot", "rust-analyzer scip /repo --output x.scip", "true"]:
            self.assertFalse(safety.FORBIDDEN_PROCESS.search(c), c)

    def test_build_script_output_dir_fails_the_run(self):
        with tempfile.TemporaryDirectory() as d:
            safety.assert_no_build_artifacts(os.path.join(d, "missing"))  # no target dir at all is the clean case
            touch(os.path.join(d, "debug", "deps", "libserde.rmeta"))
            touch(os.path.join(d, "debug", "build", "bat-1234", "output"))  # a build/<pkg>/ dir without out/ is not enough on its own
            safety.assert_no_build_artifacts(d)
            os.makedirs(os.path.join(d, "debug", "build", "bat-1234", "out"))
            with self.assertRaises(safety.UnsafeRun):
                safety.assert_no_build_artifacts(d)

    def test_compiled_build_script_or_proc_macro_fails_the_run(self):
        for rel in ("debug/build/x-1/build-script-build", "debug/deps/libserde_derive-1.dylib"):
            with tempfile.TemporaryDirectory() as d:
                touch(os.path.join(d, rel))
                with self.assertRaises(safety.UnsafeRun):
                    safety.assert_no_build_artifacts(d)

    def test_modified_corpus_checkout_fails_the_run(self):
        with tempfile.TemporaryDirectory() as d:
            subprocess.run(["git", "init", "-q", d], check=True)
            touch(os.path.join(d, "go.mod"), "module x\n")
            subprocess.run(["git", "-C", d, "add", "."], check=True)
            subprocess.run(["git", "-C", d, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "x"], check=True)
            safety.assert_tree_untouched(d)
            touch(os.path.join(d, "go.mod"), "module y\n")
            with self.assertRaises(safety.UnsafeRun):
                safety.assert_tree_untouched(d)
            subprocess.run(["git", "-C", d, "checkout", "-q", "go.mod"], check=True)
            touch(os.path.join(d, "node_modules", "x.js"))
            with self.assertRaises(safety.UnsafeRun):
                safety.assert_tree_untouched(d)

    def test_clean_dir_refuses_paths_outside_the_cache(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(safety.UnsafeRun):
                safety.clean_dir(d, os.path.join(d, "cache"))


BUILD_RS = 'fn main() { std::fs::write(r"%s", "ran").unwrap(); }\n'
MACRO_LIB = ('use proc_macro::TokenStream;\n#[proc_macro]\npub fn mk(_i: TokenStream) -> TokenStream {\n'
             '    std::fs::write(r"%s", "ran").unwrap();\n    "fn made() {}".parse().unwrap()\n}\n')


@unittest.skipUnless(shutil.which("rust-analyzer") and shutil.which("rustc") and shutil.which("cargo"), "rust-analyzer, rustc or cargo missing")
class RustAnalyzerRunsNothing(unittest.TestCase):
    """A crate whose build script and proc macro write marker files: `rust-analyzer scip` must run neither."""

    def test_scip_with_the_oracle_config_runs_no_corpus_code(self):
        with tempfile.TemporaryDirectory() as d:
            d = os.path.realpath(d)
            marks = os.path.join(d, "marks")
            os.makedirs(marks)
            crate = os.path.join(d, "corpus")
            touch(os.path.join(crate, "Cargo.toml"),
                  '[package]\nname = "toy"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\ntoymacros = { path = "macros" }\n')
            touch(os.path.join(crate, "build.rs"), BUILD_RS % os.path.join(marks, "build"))
            touch(os.path.join(crate, "macros", "Cargo.toml"),
                  '[package]\nname = "toymacros"\nversion = "0.1.0"\nedition = "2021"\n[lib]\nproc-macro = true\n')
            touch(os.path.join(crate, "macros", "src", "lib.rs"), MACRO_LIB % os.path.join(marks, "macro"))
            touch(os.path.join(crate, "src", "main.rs"), "toymacros::mk!();\nfn helper() {}\nfn main() { helper(); made(); }\n")
            wrapper = os.path.join(d, "wrapper.sh")
            touch(wrapper, '#!/bin/sh\ntouch "%s/wrapper-$$"\nexec "$@"\n' % marks)
            os.chmod(wrapper, 0o755)
            touch(os.path.join(crate, ".cargo", "config.toml"),
                  '[build]\nrustc-wrapper = "%s"\nrustc-workspace-wrapper = "%s"\n' % (wrapper, wrapper))
            subprocess.run(["cargo", "generate-lockfile", "-q"], cwd=crate, check=True,
                           env=dict(os.environ, CARGO_BUILD_RUSTC_WRAPPER="", CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER=""))  # setup, not under test
            subprocess.run(["git", "init", "-q", crate], check=True)
            subprocess.run(["git", "-C", crate, "add", "."], check=True)
            subprocess.run(["git", "-C", crate, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "x"], check=True)
            cache = os.path.join(d, "cache")
            cfg = safety.rust_config(cache, "toy")
            target = os.path.join(cache, "target-toy")
            rc, bad = safety.run_watched(["rust-analyzer", "scip", crate, "--output", os.path.join(cache, "toy.scip"), "--config-path", cfg],
                                         env=safety.hardened_env(target), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.assertEqual(rc, 0)
            self.assertEqual(os.listdir(marks), [])
            safety.assert_clean_run(bad, crate, target)
            self.assertGreater(os.path.getsize(os.path.join(cache, "toy.scip")), 0)


if __name__ == "__main__":
    unittest.main()
