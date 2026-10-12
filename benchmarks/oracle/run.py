#!/usr/bin/env python3
"""Compiler-graded call-graph benchmark for lidx. Python 3 stdlib only.

    python3 benchmarks/oracle/run.py                  # all cells, writes results/
    python3 benchmarks/oracle/run.py --only 'go/'     # a subset (regex over cell ids)
    python3 benchmarks/oracle/run.py --report-only    # rebuild report.md from results/

Per repository: clone at the pinned commit (cache dir, outside the repo), index
with a fresh lidx database, run the language's compiler oracle, grade lidx's
CALLS edges (and CALLS+REFERENCES) against it, write per-cell JSON, the FN/FP
lists and the markdown report. See README.md for method and requirements.
"""
import argparse
import datetime
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import match  # noqa: E402
import report  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
RESULTS = os.path.join(HERE, "results")


def sh(cmd, **kw):
    kw.setdefault("check", True)
    return subprocess.run(cmd, text=True, **kw)


def load_lock():
    return json.load(open(os.path.join(HERE, "corpus.lock.json")))


def cells_for(repo):
    """Cell definitions for a corpus repo."""
    n, lang = repo["name"], repo["lang"]
    if lang == "go":
        return [{"id": "go/%s/tests" % n, "repo": n, "lang": lang, "variant": "with_tests", "args": ["-tests"]},
                {"id": "go/%s/no_tests" % n, "repo": n, "lang": lang, "variant": "no_tests", "args": []}]
    if lang == "rust" and repo.get("held_out"):
        # held-out repositories (not used while developing the Rust extractor) get only the repowise-comparable scope
        return [{"id": "rust/%s/path_tests_excluded" % n, "repo": n, "lang": lang, "variant": "path_tests_excluded",
                 "args": ["--path-tests-excluded"]}]
    if lang == "rust":
        # path_tests_excluded is repowise's cell (test files by path only, #[cfg(test)] code kept); no_tests is our stricter
        # variant (also drops #[cfg(test)] items and modules gated in their parent) and is a diagnostic row only
        return [{"id": "rust/%s/path_tests_excluded" % n, "repo": n, "lang": lang, "variant": "path_tests_excluded",
                 "args": ["--path-tests-excluded"]},
                {"id": "rust/%s/no_tests" % n, "repo": n, "lang": lang, "variant": "no_tests", "args": ["--no-tests"]},
                {"id": "rust/%s/tests" % n, "repo": n, "lang": lang, "variant": "with_tests", "args": []}]
    return [{"id": "%s/%s" % (lang, n), "repo": n, "lang": lang, "variant": "no_tests", "args": []}]


def fetch(repo, cache):
    d = os.path.join(cache, "%s@%s" % (repo["name"], repo["sha"][:12]))
    if os.path.isdir(os.path.join(d, ".git")):
        head = sh(["git", "-C", d, "rev-parse", "HEAD"], capture_output=True).stdout.strip()
        if head == repo["sha"]:
            return d
        shutil.rmtree(d)
    os.makedirs(d, exist_ok=True)
    sh(["git", "-C", d, "init", "-q"])
    sh(["git", "-C", d, "fetch", "-q", "--depth", "1", repo["url"] + ".git", repo["sha"]])
    sh(["git", "-C", d, "checkout", "-q", "FETCH_HEAD"])
    return d


def index(binary, repo_dir, name, cache, reuse=False):
    db = os.path.join(cache, "db", "%s.sqlite" % name)
    os.makedirs(os.path.dirname(db), exist_ok=True)
    if reuse and os.path.exists(db):
        return db, 0.0
    for f in glob.glob(db + "*"):
        os.remove(f)  # always a fresh index: results never depend on stale state
    t0 = time.time()
    sh([binary, "reindex", "--repo", repo_dir, "--db", db], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return db, round(time.time() - t0, 1)


# ---------------------------------------------------------------- oracles
def stamp(paths, extra=""):
    h = hashlib.sha256(extra.encode())
    for p in paths:
        h.update(open(p, "rb").read())
    return h.hexdigest()[:16]


def go_toolchain(repo_dir):
    """Toolchain that satisfies the corpus go.mod, so go/types can read its sources."""
    want = None
    for line in open(os.path.join(repo_dir, "go.mod"), encoding="utf-8"):
        m = re.match(r"\s*(go|toolchain)\s+(go)?(\d+\.\d+(\.\d+)?)", line)
        if m:
            want = m.group(3) if m.group(1) == "go" or not want else m.group(3)
    have = re.search(r"go(\d+\.\d+(\.\d+)?)", sh(["go", "version"], capture_output=True).stdout).group(1)

    def tup(v):
        return tuple(int(x) for x in v.split(".")) + (0,) * (3 - len(v.split(".")))
    if want and tup(want) > tup(have):
        return "go" + (want if want.count(".") == 2 else want + ".0")
    return "local"


def run_go_oracle(repo_dir, cell, cache):
    tc = go_toolchain(repo_dir)
    env = dict(os.environ)
    if tc != "local":
        env["GOTOOLCHAIN"] = tc
    exe = os.path.join(cache, "bin", "gooracle-%s" % tc)
    src = os.path.join(HERE, "go_oracle")
    st = stamp([os.path.join(src, "main.go"), os.path.join(src, "go.mod")])
    stf = exe + ".stamp"
    if not (os.path.exists(exe) and os.path.exists(stf) and open(stf).read() == st):
        os.makedirs(os.path.dirname(exe), exist_ok=True)
        sh(["go", "build", "-o", exe, "."], cwd=src, env=env)
        open(stf, "w").write(st)
    r = sh([exe, "-repo", repo_dir] + cell["args"], env=env, capture_output=True)
    return json.loads(r.stdout), {"toolchain": tc}


def run_ts_oracle(repo_dir, cell, cache, repo):
    src = os.path.join(HERE, "ts_oracle")
    if not os.path.isdir(os.path.join(src, "node_modules", "typescript")):
        sh(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=src)
    cmd = ["node", os.path.join(src, "oracle.mjs"), "--repo", repo_dir, "--tsconfig", repo.get("tsconfig", "tsconfig.json")]
    if repo.get("subdirs"):
        cmd += ["--subdir", ",".join(repo["subdirs"])]
    r = sh(cmd, capture_output=True)
    return json.loads(r.stdout), {}


def run_rust_oracle(repo_dir, cell, cache, repo):
    scip = os.path.join(cache, "scip", "%s.scip" % repo["name"])
    os.makedirs(os.path.dirname(scip), exist_ok=True)
    cfg = os.path.join(cache, "scip", "ra-config.json")
    # no build scripts, no proc-macro servers: nothing from the corpus is executed
    json.dump({"cargo": {"buildScripts": {"enable": False}, "autoreload": False}, "procMacro": {"enable": False},
               "checkOnSave": False}, open(cfg, "w"))
    env = dict(os.environ, CARGO_TARGET_DIR=os.path.join(cache, "target-%s" % repo["name"]))
    sh(["rust-analyzer", "scip", repo_dir, "--output", scip, "--config-path", cfg, "--exclude-vendored-libraries"],
       env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    r = sh([sys.executable, "-I", os.path.join(HERE, "rust_oracle", "scip_calls.py"), "--repo", repo_dir, "--scip", scip] + cell["args"],
           capture_output=True)
    return json.loads(r.stdout), {}


def get_oracle(cell, repo, repo_dir, cache, force):
    key_src = {"go": ["go_oracle/main.go"], "ts": ["ts_oracle/oracle.mjs", "ts_oracle/package-lock.json"],
               "rust": ["rust_oracle/scip_calls.py"]}[cell["lang"]]
    st = stamp([os.path.join(HERE, p) for p in key_src], repo["sha"] + " ".join(cell["args"]) + ",".join(repo.get("subdirs", [])))
    path = os.path.join(cache, "oracle", "%s.json" % cell["id"].replace("/", "__"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not force and os.path.exists(path):
        d = json.load(open(path))
        if d.get("_stamp") == st:
            return d, True
    t0 = time.time()
    if cell["lang"] == "go":
        d, meta = run_go_oracle(repo_dir, cell, cache)
    elif cell["lang"] == "ts":
        d, meta = run_ts_oracle(repo_dir, cell, cache, repo)
    else:
        d, meta = run_rust_oracle(repo_dir, cell, cache, repo)
    d["_stamp"] = st
    d["_meta"] = dict(meta, seconds=round(time.time() - t0, 1))
    json.dump(d, open(path, "w"))
    return d, False


# ---------------------------------------------------------------- main
def git_info():
    def g(*a):
        return sh(["git", "-C", REPO_ROOT] + list(a), capture_output=True, check=False).stdout.strip()
    base = g("merge-base", "HEAD", "origin/main") or g("rev-parse", "HEAD")
    return {"main_commit": base, "head_commit": g("rev-parse", "HEAD")}


def toolchain_versions():
    def v(cmd):
        try:
            return sh(cmd, capture_output=True).stdout.strip().splitlines()[0]
        except Exception as e:  # noqa: BLE001
            return "unavailable: %s" % e
    ts_dir = os.path.join(HERE, "ts_oracle", "node_modules", "typescript", "package.json")
    return {"rust-analyzer": v(["rust-analyzer", "--version"]), "node": v(["node", "--version"]), "go": v(["go", "version"]),
            "typescript": json.load(open(ts_dir))["version"] if os.path.exists(ts_dir) else "not installed yet",
            "python": sys.version.split()[0]}


def dump_list(path, data):
    """One compact JSON object per line: diff-friendly and about a third the size of indented JSON."""
    rows = sorted(json.dumps(x, separators=(",", ":"), sort_keys=True) for x in data)  # stable across runs
    with open(path, "w") as f:
        f.write("[\n" + ",\n".join(rows) + "\n]\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binary", default=os.path.join(REPO_ROOT, "target", "release", "lidx"))
    ap.add_argument("--cache", default=os.environ.get("LIDX_ORACLE_CACHE", os.path.expanduser("~/.cache/lidx-oracle")))
    ap.add_argument("--only", default="", help="regex over cell ids")
    ap.add_argument("--force", action="store_true", help="recompute cached oracle outputs")
    ap.add_argument("--report-only", action="store_true")
    ap.add_argument("--reuse-index", action="store_true", help="development only: skip reindexing when a db exists")
    ap.add_argument("--results", default=RESULTS)
    a = ap.parse_args()

    if a.report_only:
        report.build(a.results)
        return 0

    lock = load_lock()
    cache = os.path.abspath(os.path.expanduser(a.cache))
    os.makedirs(cache, exist_ok=True)
    os.makedirs(os.path.join(a.results, "lists"), exist_ok=True)
    binary = os.path.abspath(a.binary)
    if not os.path.exists(binary):
        sys.exit("lidx binary not found: %s (cargo build --release, or pass --binary)" % binary)
    version = sh([binary, "--version"], capture_output=True).stdout.strip()
    rx = re.compile(a.only) if a.only else None

    meta_path = os.path.join(a.results, "cells.json")
    prev = json.load(open(meta_path)) if os.path.exists(meta_path) else {"cells": {}}
    out = {"toolchains": toolchain_versions(), "generated": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"), "lidx_version": version,
           **git_info(), "corpus": {r["name"]: {k: r[k] for k in ("lang", "url", "ref", "sha")} for r in lock["repos"]},
           "cells": prev.get("cells", {}) if rx else {}}
    for repo in lock["repos"]:
        cells = [c for c in cells_for(repo) if not rx or rx.search(c["id"])]
        if not cells:
            continue
        print("== %s (%s @ %s)" % (repo["name"], repo["lang"], repo["sha"][:10]), flush=True)
        repo_dir = fetch(repo, cache)
        db, secs = index(binary, repo_dir, repo["name"], cache, a.reuse_index)
        print("   indexed in %ss" % secs, flush=True)
        syms, edges = match.load_lidx(db, repo_dir, repo["lang"])
        src = match.Sources(repo_dir)
        for cell in cells:
            oracle, cached = get_oracle(cell, repo, repo_dir, cache, a.force)
            print("   %s: oracle %d edges%s" % (cell["id"], len(oracle["edges"]), " (cached)" if cached else ""), flush=True)
            res = {"oracle_ts_version": oracle.get("ts_version"), "oracle_node_version": oracle.get("node_version"),
                   "cell": cell["id"], "repo": repo["name"], "lang": repo["lang"], "variant": cell["variant"],
                   "index_seconds": secs, "oracle_mode": oracle.get("mode"), "oracle_meta": oracle.get("_meta", {}),
                   "oracle_dropped": oracle.get("dropped", {})}
            summ, fn, fp, gap, ungr = match.grade(oracle, syms, edges, repo["lang"], src, ("CALLS",))
            summ_r, fn_r, fp_r, _, _ = match.grade(oracle, syms, edges, repo["lang"], src, ("CALLS", "REFERENCES", "USES"))
            res["calls"] = summ
            if cell["lang"] == "ts":
                # the repowise-comparable TypeScript grading: no module-level callers, no abstract targets (see
                # match.repowise_ts_scope); `calls` stays the stricter grading and is a diagnostic row
                o_cmp, withheld = match.repowise_ts_scope(oracle)
                comp = match.grade(o_cmp, syms, edges, repo["lang"], src, ("CALLS",), withheld_callers=withheld)[0]
                comp["oracle_edges_removed"] = len(oracle["edges"]) - len(o_cmp["edges"])
                comp["module_level_removed"] = sum(1 for e in oracle["edges"] if e["caller"]["line"] == 0)
                comp["abstract_target_removed"] = sum(1 for e in oracle["edges"] if e.get("dispatch"))
                res["comparable"] = comp  # other languages: `calls` is the comparable grading
            res["calls_plus_references"] = {k: summ_r[k] for k in ("graded", "tp", "fp", "precision", "precision_ci", "recall", "recall_ci",
                                                                   "matched_oracle_edges")}
            res["lidx_edge_counts"] = {k: sum(1 for e in edges if e["kind"] == k) for k in ("CALLS", "REFERENCES", "USES")}
            fid = cell["id"].replace("/", "__")
            dispatch_fn = [x for x in fn if x["category"] == "dispatch"]
            fn = [x for x in fn if x["category"] != "dispatch"]
            for suffix, data in (("fn", fn), ("fp", fp), ("capture_gap", gap), ("ungradable", ungr), ("dispatch_fn", dispatch_fn)):
                dump_list(os.path.join(a.results, "lists", "%s.%s.json" % (fid, suffix)), data)
            out["cells"][cell["id"]] = res
            print("   %s: P=%s R=%s (graded=%d, oracle=%d)" % (cell["id"], summ["precision"], summ["recall"], summ["graded"],
                                                              summ["oracle_edges_caller_indexed"]), flush=True)
    out["toolchains"] = toolchain_versions()  # after the oracles ran, so typescript is installed
    json.dump(out, open(meta_path, "w"), indent=1)
    report.build(a.results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
