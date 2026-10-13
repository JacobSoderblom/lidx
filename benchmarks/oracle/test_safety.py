"""Tests for the guards that keep the oracles from executing corpus code. Run: python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dead_code  # noqa: E402
import safety  # noqa: E402


def touch(path, text=""):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


class Detection(unittest.TestCase):
    def test_forbidden_process_command_lines(self):
        bad = ["/x/target/debug/build/bat-0123456789abcdef/build-script-main",
               "/r/libexec/rust-analyzer-proc-macro-srv",
               "/r/bin/cargo check --workspace --message-format=json", "cargo -q build", "cargo rustc --release", "cargo rustc -- -O",
               "cargo rustc --release -- --print cfg", "cargo rustc -- -C x --print=cfg"]
        ok = ["/r/bin/cargo metadata --format-version 1", "rustc --print sysroot", "rust-analyzer scip /repo", "/usr/bin/true"]
        for c in bad:
            self.assertTrue(safety.FORBIDDEN_PROCESS.search(c), c)
        for c in ok:
            self.assertFalse(safety.FORBIDDEN_PROCESS.search(c), c)

    def test_allowed_processes_are_not_flagged(self):
        # the probes the oracle needs must never trip the guard, or every run would fail
        for c in ["cargo metadata --format-version 1 --all-features --filter-platform aarch64-apple-darwin",
                  "cargo locate-project",
                  "/c/bin/cargo rustc -Z unstable-options --print cfg --target aarch64-apple-darwin -- -O",
                  "cargo rustc -Z unstable-options --print target-spec-json --target aarch64-apple-darwin"
                  " -- -Z unstable-options", "/r/bin/rustc -vV", "rustc - --crate-name ___ --print=file-names --print=cfg",
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
                  '[package]\nname = "toy"\nversion = "0.1.0"\nedition = "2021"\n[dependencies]\ntoymacros = { path = "macros" }\n[features]\nextra = []\n')
            touch(os.path.join(crate, "build.rs"), BUILD_RS % os.path.join(marks, "build"))
            touch(os.path.join(crate, "macros", "Cargo.toml"),
                  '[package]\nname = "toymacros"\nversion = "0.1.0"\nedition = "2021"\n[lib]\nproc-macro = true\n')
            touch(os.path.join(crate, "macros", "src", "lib.rs"), MACRO_LIB % os.path.join(marks, "macro"))
            touch(os.path.join(crate, "src", "main.rs"), "toymacros::mk!();\n#[cfg(feature = \"extra\")]\nmod extra;\nfn helper() {}\nfn main() {\n    helper();\n    made();\n    #[cfg(feature = \"extra\")]\n    extra::gated_only_with_feature();\n}\n")
            touch(os.path.join(crate, "src", "extra.rs"), "pub fn gated_only_with_feature() {}\n")
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
            seen = {}
            for all_features in (False, True):
                cfg = safety.rust_config(cache, "toy", all_features=all_features)
                self.assertEqual(json.load(open(cfg))["cargo"].get("features"), "all" if all_features else None)
                target = os.path.join(cache, "target-toy-%s" % all_features)
                scip = os.path.join(cache, "toy-%s.scip" % all_features)
                rc, bad = safety.run_watched(["rust-analyzer", "scip", crate, "--output", scip, "--config-path", cfg],
                                             env=safety.hardened_env(target), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.assertEqual(rc, 0)
                self.assertEqual(os.listdir(marks), [])  # no build script, proc macro or rustc wrapper ran, with or without features
                safety.assert_clean_run(bad, crate, target)
                data = open(scip, "rb").read()
                self.assertGreater(len(data), 0)
                seen[all_features] = b"gated_only_with_feature" in data
            # the feature-gated module is analysed only with cargo.features = "all": that is what the setting buys
            self.assertEqual(seen, {False: False, True: True})


# ---------------------------------------------------------------- the Go dead-code oracle (staticcheck U1000 + RTA)
GO_ORACLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "go_oracle")
HARDENED = {"CGO_ENABLED": "0", "GOFLAGS": "-mod=readonly", "GOWORK": "off", "GOPACKAGESDRIVER": "off"}


class GoDeadOracleHardening(unittest.TestCase):
    """The staticcheck (U1000) and deadcode (RTA) invocations run under the hardened Go environment, in both layers: the environment
    the process is started with (`safety.go_hardened_env`, built into the command by `dead_code.go_dead_invocation`) and the
    packages.Load configuration inside the oracle."""

    def test_go_hardened_env_replaces_whatever_the_caller_had(self):
        env = safety.go_hardened_env({"GOFLAGS": "-toolexec=/evil -overlay=/o.json", "CGO_ENABLED": "1", "GOWORK": "/w/go.work",
                                      "GOPACKAGESDRIVER": "/bin/driver", "PATH": "/usr/bin", "HOME": "/h"})
        for k, v in HARDENED.items():
            self.assertEqual(env[k], v, k)
        self.assertEqual((env["PATH"], env["HOME"]), ("/usr/bin", "/h"))
        self.assertNotIn("toolexec", env["GOFLAGS"])

    def test_the_dead_oracle_command_runs_under_it(self):
        cmd, env = dead_code.go_dead_invocation("/cache/bin/deadoracle-local", "/cache/repo", ["-tests"],
                                                base_env={"GOFLAGS": "-toolexec=/evil", "CGO_ENABLED": "1", "PATH": "/usr/bin"})
        for k, v in HARDENED.items():
            self.assertEqual(env[k], v, k)
        self.assertEqual(cmd, ["/cache/bin/deadoracle-local", "-repo", "/cache/repo", "-tests"])  # the oracle only: no go build/test/run/generate
        self.assertFalse(safety.FORBIDDEN_GO_PROCESS.search(" ".join(cmd)))
        cmd, env = dead_code.go_dead_invocation("/b/deadoracle-go1.26.3", "/r", [], base_env={}, toolchain="go1.26.3")
        self.assertEqual(env["GOTOOLCHAIN"], "go1.26.3")
        self.assertEqual(env["GOFLAGS"], "-mod=readonly")

    def test_the_oracle_source_runs_staticcheck_and_rta_with_the_same_hardening_and_executes_nothing(self):
        with open(os.path.join(GO_ORACLE, "dead", "main.go"), encoding="utf-8") as f:
            src = f.read()
        self.assertIn('"honnef.co/go/tools/unused"', src)  # staticcheck U1000
        self.assertIn('"golang.org/x/tools/go/callgraph/rta"', src)  # the machinery of cmd/deadcode
        self.assertIn('"golang.org/x/tools/go/analysis/checker"', src)  # go/analysis over packages.Load
        for k, v in HARDENED.items():
            self.assertIn('"%s=%s"' % (k, v), src, k)
        self.assertNotIn("os/exec", src)  # the oracle starts no process itself
        self.assertNotIn("exec.Command", src)
        with open(os.path.join(GO_ORACLE, "go.mod"), encoding="utf-8") as f:
            self.assertRegex(f.read(), r"honnef\.co/go/tools v\d+\.\d+\.\d+\n")  # pinned, not floating

    def test_forbidden_go_processes(self):
        for c in ["go build ./...", "/usr/local/go/bin/go test ./...", "go run main.go", "go generate ./...", "go vet ./...", "go tool cgo x.go",
                  "/usr/local/go/pkg/tool/darwin_arm64/compile -o x.a", "/usr/local/go/pkg/tool/linux_amd64/link -o x", "/tmp/pkg.test -test.v"]:
            self.assertTrue(safety.FORBIDDEN_GO_PROCESS.search(c), c)
        for c in ["/usr/local/go/bin/go list -e -json=Name,ImportPath ./...", "go env GOMOD", "go version", "/x/golang.org/toolchain@v0.0.1-go1.26.3.darwin-arm64/bin/go list -deps"]:
            self.assertFalse(safety.FORBIDDEN_GO_PROCESS.search(c), c)

    @unittest.skipUnless(shutil.which("go"), "go missing")
    def test_a_module_whose_init_and_generate_directive_write_markers_runs_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            d = os.path.realpath(d)
            marks = os.path.join(d, "marks")
            os.makedirs(marks)
            mod = os.path.join(d, "corpus")
            touch(os.path.join(mod, "go.mod"), "module toy\n\ngo 1.24\n")
            touch(os.path.join(mod, "main.go"),
                  'package main\n\nimport "os"\n\nfunc init() { os.WriteFile(`%s`, []byte("ran"), 0o644) }\n\nfunc main() { used() }\n\n'
                  "func used() {}\n\nfunc unusedHelper() {}\n\nfunc Exported() {}\n" % os.path.join(marks, "init"))
            touch(os.path.join(mod, "gen.go"), '//go:generate sh -c "touch %s"\npackage main\n' % os.path.join(marks, "generate"))
            touch(os.path.join(mod, "cgo.go"), "//go:build cgo\n\npackage main\n\n/*\n#include <stdlib.h>\n*/\nimport \"C\"\n")
            touch(os.path.join(mod, "sub", "sub.go"), "package sub\n\nfunc Pub() {}\n\nfunc private() {}\n")
            subprocess.run(["git", "init", "-q", mod], check=True)
            subprocess.run(["git", "-C", mod, "add", "."], check=True)
            subprocess.run(["git", "-C", mod, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "x"], check=True)
            exe = os.path.join(d, "deadoracle")
            subprocess.run(["go", "build", "-o", exe, "./dead"], cwd=GO_ORACLE, check=True)
            cmd, env = dead_code.go_dead_invocation(exe, mod, [])
            rc, bad, secs, mb, err = dead_code.run_measured(cmd, env, os.path.join(d, "out.json"), forbidden=safety.FORBIDDEN_GO_PROCESS)
            self.assertEqual(rc, 0, err)
            self.assertEqual(os.listdir(marks), [])  # neither init() nor the go:generate directive ran
            safety.assert_clean_go_run(bad, mod)
            with open(os.path.join(d, "out.json")) as f:
                out = json.load(f)
        by = {x["name"]: x for x in out["decls"]}
        self.assertEqual(by["unusedHelper"]["unref"], "dead")  # staticcheck U1000
        self.assertEqual(by["unusedHelper"]["reach"], "dead")  # RTA
        self.assertEqual(by["used"]["unref"], "live")
        self.assertEqual((by["Pub"]["api"], by["Pub"]["exported"]), (True, True))
        self.assertEqual((by["Exported"]["api"], by["Exported"]["exported"]), (False, True))  # exported, but in package main
        self.assertEqual(by["Exported"]["reach"], "dead")
        self.assertEqual(by["private"]["unref"], "dead")
        self.assertEqual(by["init"].get("root"), "main_init")
        self.assertFalse(out["library"])


if __name__ == "__main__":
    unittest.main()
