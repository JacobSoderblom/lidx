"""Guards that keep the oracles from executing anything from a corpus repository.

Corpus repositories are untrusted. The oracles only read their sources:

- Rust: rust-analyzer's `scip` subcommand always runs `cargo check` (build scripts, and the proc-macro crates that
  rustc loads) and starts the sysroot proc-macro server; `cargo.buildScripts.enable=false` and `procMacro.enable=false`
  are LSP-only settings and are ignored. Two settings it does honour stop that, see `rust_config`.
- Go: `packages.Load` is given CGO_ENABLED=0 and a read-only module mode, so neither `go generate` nor cgo runs.
- TypeScript: only the compiler API reads the sources; the corpus is never installed.

These checks run after every oracle and fail the run, so a regression cannot pass unnoticed.
"""
import json
import os
import re
import shutil
import subprocess
import time

# command lines that mean corpus code (or a build of it) runs: build-script executables, the proc-macro server, and a cargo
# subcommand that compiles. `cargo metadata` (reads manifests only) and rustc's own `--print` probes are allowed.
FORBIDDEN_PROCESS = re.compile(r"proc-macro-srv|(^|/)build-script-|\bcargo\s+(-\S+\s+)*(check|build|clippy|test|run|bench|rustc|doc)\b")


class UnsafeRun(RuntimeError):
    pass


def rust_config(cache, repo_name):
    """Write the rust-analyzer config and the stub sysroot used for `rust-analyzer scip`; return the config path.

    - `cargo.buildScripts.overrideCommand`: the `scip` subcommand loads build-script output by running
      `cargo check --workspace` (load_out_dirs_from_check is hard-coded). With an override command it runs that command
      instead; `true` prints nothing, so there is no build-script output and no proc-macro dylib to load.
    - `cargo.sysroot`: rust-analyzer starts `<sysroot>/libexec/rust-analyzer-proc-macro-srv` (the choice is hard-coded to
      the sysroot server). The stub sysroot holds only `lib` (std sources and metadata), so there is no server to start.
    """
    d = os.path.join(cache, "scip")
    stub = os.path.join(d, "sysroot-stub")
    os.makedirs(stub, exist_ok=True)
    real = subprocess.run(["rustc", "--print", "sysroot"], capture_output=True, text=True, check=True).stdout.strip()
    link = os.path.join(stub, "lib")
    if os.path.islink(link) or os.path.exists(link):
        os.remove(link)
    os.symlink(os.path.join(real, "lib"), link)
    cfg = os.path.join(d, "ra-config.json")
    with open(cfg, "w") as f:
        json.dump({"cargo": {"buildScripts": {"enable": False, "overrideCommand": ["true"]}, "autoreload": False, "sysroot": stub},
                   "procMacro": {"enable": False}, "checkOnSave": False}, f)
    return cfg


def process_tree(root_pid):
    """Command lines of root_pid and all its descendants right now."""
    out = subprocess.run(["ps", "-axo", "pid=,ppid=,command="], capture_output=True, text=True).stdout
    rows = []
    for line in out.splitlines():
        parts = line.split(None, 2)
        if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
            rows.append((int(parts[0]), int(parts[1]), parts[2]))
    kids = {root_pid}
    grew = True
    while grew:
        grew = False
        for pid, ppid, _ in rows:
            if ppid in kids and pid not in kids:
                kids.add(pid)
                grew = True
    return [c for pid, _, c in rows if pid in kids]


def run_watched(cmd, interval=0.05, **kw):
    """Run cmd, sampling its process tree; return (returncode, set of forbidden command lines seen). Sampling can miss a
    process shorter than `interval`, which is why `assert_no_build_artifacts` checks the filesystem as well."""
    p = subprocess.Popen(cmd, **kw)
    bad = set()
    while p.poll() is None:
        for c in process_tree(p.pid):
            if FORBIDDEN_PROCESS.search(c):
                bad.add(c[:200])
        time.sleep(interval)
    return p.returncode, bad


def clean_dir(path, cache):
    """Remove a build directory under the cache so the post-run check only sees what this run produced."""
    path, cache = os.path.abspath(path), os.path.abspath(cache)
    if not path.startswith(cache + os.sep):
        raise UnsafeRun("refusing to delete %s outside the cache %s" % (path, cache))
    shutil.rmtree(path, ignore_errors=True)


def build_artifacts(target_dir):
    """Paths in a cargo target dir that exist only if a build script or proc macro was compiled or run:
    `build/<pkg>-<hash>/out`, `build-script-*` executables, and dynamic libraries in `deps/` (proc-macro crates)."""
    found = []
    for root, dirs, files in os.walk(target_dir):
        parent = os.path.basename(os.path.dirname(root))
        if parent == "build" and "out" in dirs:
            found.append(os.path.join(root, "out"))
        found.extend(os.path.join(root, f) for f in files if f.startswith("build-script-"))
        if os.path.basename(root) == "deps":
            found.extend(os.path.join(root, f) for f in files if f.endswith((".dylib", ".so", ".dll")))
    return sorted(found)


def assert_no_build_artifacts(target_dir):
    if not os.path.isdir(target_dir):
        return
    found = build_artifacts(target_dir)
    if found:
        raise UnsafeRun("build scripts or proc macros were built under %s, e.g. %s" % (target_dir, found[:3]))


def assert_tree_untouched(repo_dir):
    """The corpus checkout must be exactly the pinned commit afterwards: nothing was generated, installed or rewritten
    (`go.mod` under -mod=mod, `node_modules`, generated files)."""
    r = subprocess.run(["git", "-C", repo_dir, "status", "--porcelain", "--ignored"], capture_output=True, text=True, check=True)
    if r.stdout.strip():
        raise UnsafeRun("the oracle modified the corpus checkout %s:\n%s" % (repo_dir, r.stdout[:500]))


def assert_clean_run(rc_bad, repo_dir, target_dir=None):
    if rc_bad:
        raise UnsafeRun("forbidden processes ran: %s" % sorted(rc_bad)[:3])
    if target_dir:
        assert_no_build_artifacts(target_dir)
    assert_tree_untouched(repo_dir)
