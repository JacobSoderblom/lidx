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
import dead_code  # noqa: E402
import hierarchy  # noqa: E402
import impact_run  # noqa: E402
import match  # noqa: E402
import refs  # noqa: E402
import report  # noqa: E402
import rust_cfg  # noqa: E402
import safety  # noqa: E402

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
    # diagnostic twin of path_tests_excluded: rust-analyzer analyses every cargo feature, and lidx edges inside `#[cfg(...)]`
    # items that are definitely off in that configuration are ungradable (rust_cfg.py). repowise analyses the default
    # features and grades those edges, so this cell is never given a verdict; `path_tests_excluded` stays repowise's cell.
    allfeat = {"id": "rust/%s/path_tests_excluded_all_features" % n, "repo": n, "lang": lang, "variant": "path_tests_excluded_all_features",
               "args": ["--path-tests-excluded"], "features": "all"}
    if lang == "rust" and repo.get("held_out"):
        # held-out repositories (not used while developing the extractors) get only the repowise-comparable scope
        return [{"id": "rust/%s/path_tests_excluded" % n, "repo": n, "lang": lang, "variant": "path_tests_excluded",
                 "args": ["--path-tests-excluded"]}, allfeat]
    if lang == "rust":
        # path_tests_excluded is repowise's cell (test files by path only, #[cfg(test)] code kept); no_tests is our stricter
        # variant (also drops #[cfg(test)] items and modules gated in their parent) and is a diagnostic row only
        return [{"id": "rust/%s/path_tests_excluded" % n, "repo": n, "lang": lang, "variant": "path_tests_excluded",
                 "args": ["--path-tests-excluded"]},
                {"id": "rust/%s/no_tests" % n, "repo": n, "lang": lang, "variant": "no_tests", "args": ["--no-tests"]},
                {"id": "rust/%s/tests" % n, "repo": n, "lang": lang, "variant": "with_tests", "args": []}, allfeat]
    return [{"id": "%s/%s" % (lang, n), "repo": n, "lang": lang, "variant": "no_tests", "args": []}]


def fetch(repo, cache):
    d = os.path.join(cache, "%s@%s" % (repo["name"], repo["sha"][:12]))
    if os.path.isdir(os.path.join(d, ".git")):
        head = sh(["git", "-C", d, "rev-parse", "HEAD"], capture_output=True).stdout.strip()
        dirty = sh(["git", "-C", d, "status", "--porcelain", "--ignored"], capture_output=True).stdout.strip()
        if head == repo["sha"] and not dirty:
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
    st = stamp([os.path.join(src, "main.go"), os.path.join(src, "hierarchy.go"), os.path.join(src, "refs.go"), os.path.join(src, "go.mod")])
    stf = exe + ".stamp"
    if not (os.path.exists(exe) and os.path.exists(stf) and open(stf).read() == st):
        os.makedirs(os.path.dirname(exe), exist_ok=True)
        sh(["go", "build", "-o", exe, "."], cwd=src, env=env)
        open(stf, "w").write(st)
    # packages.Load runs with CGO_ENABLED=0 and -mod=readonly (see go_oracle/main.go): no go generate, no cgo, no go.mod rewrite
    r = sh([exe, "-repo", repo_dir] + cell["args"], env=env, capture_output=True)
    safety.assert_tree_untouched(repo_dir)
    return json.loads(r.stdout), {"toolchain": tc}


def run_ts_oracle(repo_dir, cell, cache, repo):
    src = os.path.join(HERE, "ts_oracle")
    if not os.path.isdir(os.path.join(src, "node_modules", "typescript")):
        sh(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=src)
    cmd = ["node", os.path.join(src, "oracle.mjs"), "--repo", repo_dir, "--tsconfig", repo.get("tsconfig", "tsconfig.json")]
    if repo.get("subdirs"):
        cmd += ["--subdir", ",".join(repo["subdirs"])]
    # the oracle's own dependencies are installed with --ignore-scripts above; the corpus is never installed, only parsed
    r = sh(cmd, capture_output=True)
    safety.assert_tree_untouched(repo_dir)
    return json.loads(r.stdout), {}


def run_scip(repo_dir, cache, repo, all_features):
    """One `rust-analyzer scip` run under the sandbox; returns (scip path, return code). Raises UnsafeRun if anything from the
    corpus ran."""
    tag = "-allfeat" if all_features else ""
    scip = os.path.join(cache, "scip", "%s%s.scip" % (repo["name"], tag))
    os.makedirs(os.path.dirname(scip), exist_ok=True)
    # nothing from the corpus is executed: no build scripts, no proc macros (see safety.rust_config). The target dir is
    # emptied first so the check afterwards sees only what this run produced.
    cfg = safety.rust_config(cache, repo["name"], all_features=all_features)
    target = os.path.join(cache, "target-%s%s" % (repo["name"], tag))
    safety.clean_dir(target, cache)
    if os.path.exists(scip):
        os.remove(scip)
    env = safety.hardened_env(target)
    rc, bad = safety.run_watched(["rust-analyzer", "scip", repo_dir, "--output", scip, "--config-path", cfg, "--exclude-vendored-libraries"],
                                 env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    safety.assert_clean_run(bad, repo_dir, target)
    return scip, rc


def run_rust_oracle(repo_dir, cell, cache, repo):
    features = "default"
    fallback = None
    scip = None
    if cell.get("features") == "all":
        scip, rc = run_scip(repo_dir, cache, repo, True)
        if rc == 0 and os.path.exists(scip):
            features = "all"
        else:
            fallback = "rust-analyzer scip with cargo.features=all exited %s; this cell is analysed with default features" % rc
    if features == "default":
        scip, rc = run_scip(repo_dir, cache, repo, False)
        if rc != 0:
            raise subprocess.CalledProcessError(rc, "rust-analyzer scip")
    r = sh([sys.executable, "-I", os.path.join(HERE, "rust_oracle", "scip_calls.py"), "--repo", repo_dir, "--scip", scip] + cell["args"],
           capture_output=True)
    d = json.loads(r.stdout)
    # type hierarchy (issue #449): the impl symbols and header occurrences of the same SCIP index, same file scope
    d["hierarchy"] = json.loads(sh([sys.executable, "-I", os.path.join(HERE, "rust_oracle", "scip_hierarchy.py"), "--repo", repo_dir, "--scip", scip]
                                   + cell["args"], capture_output=True).stdout)
    meta = {"build_scripts_run": False, "proc_macros_run": False, "features": features}
    if fallback:
        meta["features_fallback"] = fallback
    return d, meta


def host_cfg():
    """The configuration rust-analyzer analyses: the host's `rustc --print cfg`. Run outside any corpus directory."""
    out = sh(["rustc", "--print", "cfg"], capture_output=True, cwd="/").stdout
    return out


def make_cfg_gate(repo_dir, features):
    """Callable for match.grade: are all these evidence lines inside items behind a definitely-false #[cfg] of this configuration?"""
    host = rust_cfg.host_from_rustc(host_cfg(), features)
    cache = {}

    def gate(path, lines):
        if path not in cache:
            try:
                with open(os.path.join(repo_dir, path), encoding="utf-8", errors="replace") as f:
                    cache[path] = rust_cfg.gated_ranges(f.read(), host)
            except OSError:
                cache[path] = []
        return rust_cfg.lines_gated(cache[path], lines)
    return gate


def get_oracle(cell, repo, repo_dir, cache, force):
    key_src = {"go": ["go_oracle/main.go", "go_oracle/hierarchy.go", "go_oracle/refs.go"],
               "ts": ["ts_oracle/oracle.mjs", "ts_oracle/hierarchy.mjs", "ts_oracle/refs.mjs", "ts_oracle/package-lock.json"],
               "rust": ["rust_oracle/scip_calls.py", "rust_oracle/scip_hierarchy.py", "safety.py"]}[cell["lang"]]
    st = stamp([os.path.join(HERE, p) for p in key_src], repo["sha"] + " ".join(cell["args"]) + ",".join(repo.get("subdirs", [])) + cell.get("features", ""))
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


# `super(..)` the keyword, not a local that merely ends in it: zod's `_super(payload, ctx)` is a function value
# captured from an interface member, an ordinary call (a capture gap), and was once counted as a lost `super(..)`.
SUPER_CALL = re.compile(r"(?<![\w$])super\s*\(")


def super_edge_stats(oracle, repo_dir, missed):
    """Oracle edges made by a `super(..)` call (callee: the base class, like `new X()`), and how many of them lidx has.
    `missed` is the FN plus capture-gap lists of the cell."""
    gone = {(x["caller"]["file"], x["caller"]["line"], x["callee"]["file"], x["callee"]["line"]) for x in missed}
    cache, total, lost = {}, 0, 0
    for e in oracle["edges"]:
        is_super = False
        for s in e["sites"]:
            f, _, ln = s.rpartition(":")
            if f not in cache:
                with open(os.path.join(repo_dir, f), encoding="utf-8", errors="replace") as fh:
                    cache[f] = fh.read().split("\n")
            if 0 < int(ln) <= len(cache[f]) and SUPER_CALL.search(cache[f][int(ln) - 1]):
                is_super = True
        if is_super:
            total += 1
            lost += (e["caller"]["file"], e["caller"]["line"], e["callee"]["file"], e["callee"]["line"]) in gone
    return {"oracle": total, "matched": total - lost}


def hierarchy_inputs(binary, repo_dir, db, lang):
    """lidx's own hierarchy rows, and for Go and TypeScript its dispatch pairs (`lidx dispatch-pairs`: the query behind
    explain_symbol / analyze_impact / dead_symbols), as (base member id, implementing member id)."""
    edges = hierarchy.load_hier_edges(db)
    pairs = None
    if lang in ("go", "ts"):
        r = sh([binary, "dispatch-pairs", "--repo", repo_dir, "--db", db], capture_output=True)
        pairs = [tuple(p) for p in json.loads(r.stdout)]
    return edges, pairs


def grade_hierarchy(res, cell, oracle, syms, hier_edges, dispatch_ids, results_dir):
    """Type hierarchy section of one cell (issue #449): graded by hierarchy.py, lists in results/lists/<cell>.hierarchy_{fn,fp}.json."""
    summ, fn, fp, ungr = hierarchy.grade_cell(cell["lang"], oracle, syms, hier_edges, dispatch_ids)
    if summ is None:
        return
    dump_list(os.path.join(results_dir, "lists", "%s.hierarchy_tp.json" % cell["id"].replace("/", "__")), summ.pop("_tp"))
    res["hierarchy"] = summ
    fid = cell["id"].replace("/", "__")
    dump_list(os.path.join(results_dir, "lists", "%s.hierarchy_fn.json" % fid), fn)
    dump_list(os.path.join(results_dir, "lists", "%s.hierarchy_fp.json" % fid), fp)
    dump_list(os.path.join(results_dir, "lists", "%s.hierarchy_ungradable.json" % fid), ungr)


def grade_value_refs(res, cell, oracle, syms, edges, src, results_dir, repo_dir):
    """Value references section of one cell (issue #452): graded by refs.py, lists in results/lists/<cell>.refs_{fn,fp,capture_gap,ungradable}.json.
    The dead-code cross-reference reads the dead-code FP lists written just before (when the dead-code benchmark ran)."""
    if not refs.has_refs(oracle):
        return
    gate = make_cfg_gate(repo_dir, oracle.get("_meta", {}).get("features", "default")) if cell["lang"] == "rust" else None
    summ, fn, fp, gap, ungr, state = refs.grade_refs(oracle, syms, edges, cell["lang"], src, cfg_gate=gate)
    fid = cell["id"].replace("/", "__")
    if res.get("dead"):
        x = {}
        for col in ("unref", "reach"):
            p = os.path.join(results_dir, "lists", "%s.dead_%s.fp.json" % (fid, col))
            if os.path.exists(p):
                x[col] = refs.dead_xref(state, [i for i in json.load(open(p)) if not i.get("policy")])
        summ["dead_code_xref"] = x
    res["refs"] = summ
    for suffix, data in (("fn", fn), ("fp", fp), ("capture_gap", gap), ("ungradable", ungr), ("call_wins", state["call_wins"]), ("tp", state["tp"])):
        dump_list(os.path.join(results_dir, "lists", "%s.refs_%s.json" % (fid, suffix)), data)


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
    ap.add_argument("--skip-dead", action="store_true", help="development only: leave out the dead-code benchmark (res['dead'])")
    ap.add_argument("--impact", action="store_true",
                    help="grade analyze_impact upstream sets, test reach and trace_flow stop reasons against the oracle call-graph closure "
                         "(issue #451) for the 32 comparable cells, from the cached oracle outputs; writes results/impact/, results/seeds/ and "
                         "rebuilds report.md; leaves the call-edge, hierarchy and dead-code results untouched")
    ap.add_argument("--redraw-seeds", action="store_true", help="with --impact: replace the committed seeds by a fresh draw")
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

    if a.impact:
        n = impact_run.run(a, lock, cache, binary, version, {"cells_for": cells_for, "fetch": fetch, "index": index, "get_oracle": get_oracle})
        print("graded %d cells" % n)
        report.build(a.results)
        return 0

    meta_path = os.path.join(a.results, "cells.json")
    prev = json.load(open(meta_path)) if os.path.exists(meta_path) else {"cells": {}}
    out = {"toolchains": toolchain_versions(), "generated": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"), "lidx_version": version,
           **git_info(), "corpus": {r["name"]: {**{k: r[k] for k in ("lang", "url", "ref", "sha")}, **({"held_out": True} if r.get("held_out") else {})}
                      for r in lock["repos"]},
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
        dpairs = match.go_dispatch_pairs(db, syms) if repo["lang"] == "go" else None
        hier_edges, dispatch_ids = hierarchy_inputs(binary, repo_dir, db, repo["lang"])
        # lidx-native Go grading also counts its DISPATCH edges (a call left on an interface method); the repowise-rules
        # grading and every other language see CALLS only
        native_kinds = ("CALLS", "DISPATCH") if repo["lang"] == "go" else ("CALLS",)
        for cell in cells:
            oracle, cached = get_oracle(cell, repo, repo_dir, cache, a.force)
            print("   %s: oracle %d edges%s" % (cell["id"], len(oracle["edges"]), " (cached)" if cached else ""), flush=True)
            res = {"oracle_ts_version": oracle.get("ts_version"), "oracle_node_version": oracle.get("node_version"),
                   "cell": cell["id"], "repo": repo["name"], "lang": repo["lang"], "variant": cell["variant"],
                   "index_seconds": secs, "oracle_mode": oracle.get("mode"), "oracle_meta": oracle.get("_meta", {}),
                   "oracle_dropped": oracle.get("dropped", {})}
            # Go: the lidx-native (diagnostic) grading leaves out closure self-edges F -> F, an artefact of keying closures at their
            # enclosing declaration; repowise's denominator keeps them (as F -> literal, which nobody can match), so the
            # repowise-comparable grading below keeps them too and reports the without-them figure next to it
            graded_oracle, n_closure_self = (match.drop_closure_self(oracle) if cell["lang"] == "go" else (oracle, 0))
            gate = None
            if cell.get("features"):
                gate = make_cfg_gate(repo_dir, oracle.get("_meta", {}).get("features", "default"))
            summ, fn, fp, gap, ungr = match.grade(graded_oracle, syms, edges, repo["lang"], src, native_kinds, dispatch_pairs=dpairs, cfg_gate=gate)
            summ_r, fn_r, fp_r, _, _ = match.grade(graded_oracle, syms, edges, repo["lang"], src, native_kinds + ("REFERENCES", "USES"),
                                                   dispatch_pairs=dpairs, cfg_gate=gate)
            if cell.get("features"):
                res["features"] = oracle.get("_meta", {}).get("features", "default")
                if oracle.get("_meta", {}).get("features_fallback"):
                    res["features_fallback"] = oracle["_meta"]["features_fallback"]
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
            if cell["lang"] == "go":
                # the repowise-comparable Go grading: no interface edges, no dispatch credit (match.repowise_go_scope);
                # `calls` stays the dispatch-credited, lidx-native grading (a labelled diagnostic)
                res["comparable"] = match.grade(match.repowise_go_scope(oracle), syms, edges, repo["lang"], src, ("CALLS",))[0]
                res["closure_self_edges_dropped"] = n_closure_self
                res["comparable_without_closure_self"] = match.grade(match.repowise_go_scope(graded_oracle), syms, edges, repo["lang"], src, ("CALLS",))[0]
                dump_list(os.path.join(a.results, "lists", "%s.closure_self.json" % cell["id"].replace("/", "__")),
                          [e for e in oracle["edges"] if e.get("closure_self") and match.okey_of(e["caller"]) == match.okey_of(e["callee"])])
            res["calls_plus_references"] = {k: summ_r[k] for k in ("graded", "tp", "fp", "precision", "precision_ci", "recall", "recall_ci",
                                                                   "matched_oracle_edges")}
            res["lidx_edge_counts"] = {k: sum(1 for e in edges if e["kind"] == k) for k in ("CALLS", "REFERENCES", "USES")}
            if cell["lang"] == "ts":
                res["super_edges"] = super_edge_stats(oracle, repo_dir, fn + gap)
                if cell["id"] == "ts/ky":  # ky's error subclasses call super(..): the oracle must map those to the base class
                    assert res["super_edges"]["oracle"] > 0, "ky has super(..) calls the oracle must report as in-repo class edges"
            fid = cell["id"].replace("/", "__")
            dispatch_fn = [x for x in fn if x["category"] == "dispatch"]
            fn = [x for x in fn if x["category"] != "dispatch"]
            for suffix, data in (("fn", fn), ("fp", fp), ("capture_gap", gap), ("ungradable", ungr), ("dispatch_fn", dispatch_fn)):
                dump_list(os.path.join(a.results, "lists", "%s.%s.json" % (fid, suffix)), data)
            if not a.skip_dead:
                res["dead"] = dead_code.run_cell(cell, repo, repo_dir, cache, binary, db, syms, a.force, a.results,
                                                 {"go_toolchain": go_toolchain, "run_scip": run_scip}, oracle.get("_meta"))
                dc = res["dead"]["columns"]
                print("   %s: dead code unref P=%s R=%s, reach P=%s R=%s (oracle %ss, %s MB)" % (
                    cell["id"], dc["unref"]["all"]["precision"], dc["unref"]["all"]["recall"], dc["reach"]["all"]["precision"],
                    dc["reach"]["all"]["recall"], res["dead"]["oracle_meta"].get("seconds"), res["dead"]["oracle_meta"].get("peak_rss_mb")), flush=True)
            grade_hierarchy(res, cell, oracle, syms, hier_edges, dispatch_ids, a.results)
            grade_value_refs(res, cell, oracle, syms, edges, src, a.results, repo_dir)
            out["cells"][cell["id"]] = res
            print("   %s: P=%s R=%s (graded=%d, oracle=%d)" % (cell["id"], summ["precision"], summ["recall"], summ["graded"],
                                                              summ["oracle_edges_caller_indexed"]), flush=True)
    out["toolchains"] = toolchain_versions()  # after the oracles ran, so typescript is installed
    if not a.skip_dead:
        out["dead_toolchains"] = dead_code.toolchains()
    json.dump(out, open(meta_path, "w"), indent=1)
    report.build(a.results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
