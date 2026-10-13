"""Dead-code benchmark: run the oracles, grade lidx's `dead_symbols`, write lists. Python 3 stdlib only.

Called from run.py once per cell, after the call-graph grading (which it never touches). Oracle evidence comes from compilers and
linters only: staticcheck U1000 and RTA for Go (go_oracle/dead), the TypeScript compiler's unused-code diagnostics and
LanguageService for TypeScript (ts_oracle/dead.mjs), rust-analyzer SCIP occurrences for Rust (rust_oracle/scip_dead.py). lidx is
asked only for its claims (`dead_symbols`) and, to label why it missed something, for its own incoming edges.
"""
import json
import os
import re
import sqlite3
import subprocess
import sys
import time

import match
import match_dead
import safety

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = {"go": ["go_oracle/dead/main.go", "go_oracle/go.mod", "go_oracle/go.sum"],
       "ts": ["ts_oracle/dead.mjs", "ts_oracle/package-lock.json"],
       "rust": ["rust_oracle/scip_dead.py", "rust_oracle/scip_calls.py", "safety.py"]}
REACHABILITY = ("CALLS", "DISPATCH", "IMPORTS", "RPC_IMPL", "IMPLEMENTS", "EXTENDS", "INHERITS", "USES", "CONFIG_BIND", "REFERENCES")
# the `lidx request` params that raise the per-category limit above any repository and lift the response byte cap
DEAD_SYMBOLS_PARAMS = {"limit": 10_000_000, "include_unused_imports": False, "include_orphan_tests": False, "max_response_bytes": 2_000_000_000}


# ---------------------------------------------------------------- Go: invocation under the hardened environment
def go_dead_invocation(exe, repo_dir, args, base_env=None, toolchain="local"):
    """(command, environment) for the Go dead-code oracle. The environment is `safety.go_hardened_env`: cgo off, -mod=readonly,
    GOWORK=off, GOPACKAGESDRIVER=off, and GOFLAGS replaced so an inherited -toolexec cannot apply. The command is the oracle
    binary only: staticcheck's U1000 analyzer and RTA run inside it over packages.Load (`go list`); nothing is built or run."""
    env = safety.go_hardened_env(base_env)
    if toolchain != "local":
        env["GOTOOLCHAIN"] = toolchain
    return [exe, "-repo", repo_dir] + list(args), env


def run_measured(cmd, env, out_path, forbidden=None, interval=0.1):
    """Run cmd with stdout to out_path. Returns (returncode, forbidden command lines seen, seconds, peak resident set of the
    process tree in MB, sampled every `interval` seconds)."""
    t0 = time.time()
    peak = 0
    bad = set()
    with open(out_path, "wb") as out:
        p = subprocess.Popen(cmd, env=env, stdout=out, stderr=subprocess.PIPE)
        while p.poll() is None:
            rows = subprocess.run(["ps", "-axo", "pid=,ppid=,rss=,command="], capture_output=True, text=True).stdout.splitlines()
            tree, kids = [], {p.pid}
            parsed = []
            for line in rows:
                parts = line.split(None, 3)
                if len(parts) == 4 and parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit():
                    parsed.append((int(parts[0]), int(parts[1]), int(parts[2]), parts[3]))
            grew = True
            while grew:
                grew = False
                for pid, ppid, rss, c in parsed:
                    if ppid in kids and pid not in kids:
                        kids.add(pid)
                        grew = True
            total = 0
            for pid, ppid, rss, c in parsed:
                if pid in kids:
                    total += rss
                    if forbidden and forbidden.search(c):
                        bad.add(c[:200])
            peak = max(peak, total)
            time.sleep(interval)
        err = p.stderr.read().decode("utf-8", "replace")
        p.stderr.close()
    return p.returncode, bad, round(time.time() - t0, 1), round(peak / 1024), err


def stamp(paths, extra=""):
    import hashlib
    h = hashlib.sha256(extra.encode())
    for p in paths:
        h.update(open(os.path.join(HERE, p), "rb").read())
    return h.hexdigest()[:16]


def build_go_exe(repo_dir, cache, toolchain, go_toolchain_env):
    src = os.path.join(HERE, "go_oracle")
    st = stamp(SRC["go"])
    exe = os.path.join(cache, "bin", "deadoracle-%s" % toolchain)
    stf = exe + ".stamp"
    if not (os.path.exists(exe) and os.path.exists(stf) and open(stf).read() == st):
        os.makedirs(os.path.dirname(exe), exist_ok=True)
        subprocess.run(["go", "build", "-o", exe, "./dead"], cwd=src, env=go_toolchain_env, check=True)
        open(stf, "w").write(st)
    return exe


def run_go(repo_dir, cell, cache, helpers):
    tc = helpers["go_toolchain"](repo_dir)
    benv = dict(os.environ)
    if tc != "local":
        benv["GOTOOLCHAIN"] = tc
    exe = build_go_exe(repo_dir, cache, tc, benv)
    cmd, env = go_dead_invocation(exe, repo_dir, ["-tests"] if cell["variant"] == "with_tests" else [], toolchain=tc)
    out_path = os.path.join(cache, "oracle", "dead__%s.partial" % cell["id"].replace("/", "__"))
    rc, bad, secs, mb, err = run_measured(cmd, env, out_path, forbidden=safety.FORBIDDEN_GO_PROCESS)
    safety.assert_clean_go_run(bad, repo_dir)
    if rc != 0:
        raise subprocess.CalledProcessError(rc, "deadoracle", stderr=err[-2000:])
    d = json.load(open(out_path))
    os.remove(out_path)
    return d, {"toolchain": tc, "seconds": secs, "peak_rss_mb": mb, "tool": "staticcheck U1000 (honnef.co/go/tools %s) + RTA" % go_mod_version("honnef.co/go/tools")}


def go_mod_version(mod):
    for line in open(os.path.join(HERE, "go_oracle", "go.mod"), encoding="utf-8"):
        m = re.match(r"\s*(?:require\s+)?%s\s+(v\S+)" % re.escape(mod), line)
        if m:
            return m.group(1)
    return "?"


# ---------------------------------------------------------------- TypeScript and Rust
def run_ts(repo_dir, cell, cache, repo):
    src = os.path.join(HERE, "ts_oracle")
    if not os.path.isdir(os.path.join(src, "node_modules", "typescript")):
        subprocess.run(["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=src, check=True)
    cmd = ["node", os.path.join(src, "dead.mjs"), "--repo", repo_dir, "--tsconfig", repo.get("tsconfig", "tsconfig.json")]
    if repo.get("subdirs"):
        cmd += ["--subdir", ",".join(repo["subdirs"])]
    out_path = os.path.join(cache, "oracle", "dead__%s.partial" % cell["id"].replace("/", "__"))
    rc, bad, secs, mb, err = run_measured(cmd, dict(os.environ), out_path)
    safety.assert_tree_untouched(repo_dir)
    if rc != 0:
        raise subprocess.CalledProcessError(rc, "dead.mjs", stderr=err[-2000:])
    d = json.load(open(out_path))
    os.remove(out_path)
    return d, {"seconds": secs, "peak_rss_mb": mb, "tool": "tsc %s noUnusedLocals + LanguageService.findReferences" % d.get("ts_version")}


def run_rust(repo_dir, cell, cache, repo, helpers, call_meta):
    features = (call_meta or {}).get("features", "default") if cell.get("features") == "all" else "default"
    tag = "-allfeat" if features == "all" else ""
    scip = os.path.join(cache, "scip", "%s%s.scip" % (repo["name"], tag))
    if not os.path.exists(scip):
        scip, rc = helpers["run_scip"](repo_dir, cache, repo, features == "all")
        if rc != 0:
            raise subprocess.CalledProcessError(rc, "rust-analyzer scip")
    out_path = os.path.join(cache, "oracle", "dead__%s.partial" % cell["id"].replace("/", "__"))
    cmd = [sys.executable, "-I", os.path.join(HERE, "rust_oracle", "scip_dead.py"), "--repo", repo_dir, "--scip", scip] + cell["args"]
    rc, bad, secs, mb, err = run_measured(cmd, dict(os.environ), out_path)
    if rc != 0:
        raise subprocess.CalledProcessError(rc, "scip_dead.py", stderr=err[-2000:])
    d = json.load(open(out_path))
    os.remove(out_path)
    return d, {"seconds": secs, "peak_rss_mb": mb, "features": features, "tool": "rust-analyzer SCIP occurrences"}


def get_dead_oracle(cell, repo, repo_dir, cache, force, helpers, call_meta=None):
    st = stamp(SRC[cell["lang"]], repo["sha"] + " ".join(cell["args"]) + ",".join(repo.get("subdirs", [])) + cell.get("features", ""))
    path = os.path.join(cache, "oracle", "dead__%s.json" % cell["id"].replace("/", "__"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not force and os.path.exists(path):
        d = json.load(open(path))
        if d.get("_stamp") == st:
            return d, True
    if cell["lang"] == "go":
        d, meta = run_go(repo_dir, cell, cache, helpers)
    elif cell["lang"] == "ts":
        d, meta = run_ts(repo_dir, cell, cache, repo)
    else:
        d, meta = run_rust(repo_dir, cell, cache, repo, helpers, call_meta)
    d["_stamp"] = st
    d["_meta"] = meta
    json.dump(d, open(path, "w"))
    return d, False


# ---------------------------------------------------------------- lidx
def lidx_claims(binary, repo_dir, db, syms):
    r = subprocess.run([binary, "request", "--repo", repo_dir, "--db", db, "--method", "dead_symbols", "--params", json.dumps(DEAD_SYMBOLS_PARAMS)],
                       capture_output=True, text=True, check=True)
    res = json.loads(r.stdout.strip().splitlines()[-1])["result"]
    if res.get("truncated"):
        raise RuntimeError("dead_symbols response was truncated: raise the limit")
    return match_dead.lidx_claims(res["dead_symbols"], syms), res["counts"]["dead_symbols"]


def incoming_edges(db, syms, keys, lang, dead_keys):
    """{declaration key: ["KIND:tag", ...]} for the given declaration keys: lidx's own incoming reachability edges, tagged `test` (the
    source is in a test file), `self`, `deadsrc` (the source is itself dead per the oracle) or `live`."""
    want = {}
    for i, s in syms.items():
        if "key" in s and s["key"] in keys:
            want.setdefault(s["key"], []).append(i)
    ids = {i: k for k, v in want.items() for i in v}
    out = {k: [] for k in keys}
    if not ids:
        return out
    con = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    kinds = ",".join("'%s'" % k for k in REACHABILITY)
    idl = list(ids)
    for a in range(0, len(idl), 500):
        chunk = idl[a:a + 500]
        q = ("select e.target_symbol_id, e.kind, e.source_symbol_id from edges e join files f on f.id = e.file_id "
             "where e.kind in (%s) and f.deleted_version is null and e.target_symbol_id in (%s)" % (kinds, ",".join(str(x) for x in chunk)))
        for t, kind, s in con.execute(q):
            src = syms.get(s)
            tk = ids[t]
            if s == t or (src and src.get("key") == tk):
                tag = "self"
            elif src and "file" in src and match.is_test_file(lang, src["file"]):
                tag = "test"
            elif src and src.get("key") in dead_keys:
                tag = "deadsrc"
            else:
                tag = "live"
            out[tk].append("%s:%s" % (kind, tag))
    con.close()
    return out


# ---------------------------------------------------------------- file level
def file_level(oracle, lang):
    """Counts for the file-level answer a future lidx feature (or repowise's `unreachable_file`) can be graded against."""
    infos = oracle.get("file_info", [])
    if lang == "go":
        cand = [f for f in infos if not f.get("main") and not f.get("test") and not f.get("generated")]
        no_imp = [f["file"] for f in cand if f["importers"] == 0]
        unreach = [f["file"] for f in infos if not f.get("test") and not f.get("generated") and f["funcs"] > 0 and f["funcs_reachable"] == 0]
    elif lang == "ts":
        cand = [f for f in infos if not f.get("entry")]
        no_imp = [f["file"] for f in cand if f["importers"] == 0 and f["importers_type_only"] == 0]
        unreach = [f["file"] for f in infos if not f.get("module_reached")]
    else:
        cand = [f for f in infos if f["crate_kind"] in ("lib", "bin", "proc_macro") and not os.path.basename(f["file"]) in ("lib.rs", "main.rs")
                and "/bin/" not in "/" + f["file"]]
        no_imp = [f["file"] for f in cand if f["importers"] == 0]
        unreach = [f["file"] for f in infos if f["funcs"] > 0 and f["funcs_reachable"] == 0]
    return {"files": len(infos), "importer_candidates": len(cand), "no_importer": len(no_imp), "unreachable": len(unreach),
            "no_importer_files": sorted(no_imp)[:200], "unreachable_files": sorted(unreach)[:200]}


# ---------------------------------------------------------------- one cell
def dump_list(path, data):
    rows = sorted(json.dumps(x, separators=(",", ":"), sort_keys=True) for x in data)
    with open(path, "w") as f:
        f.write("[\n" + ",\n".join(rows) + "\n]\n")


def universe_note(oracle, lang, cell):
    """One sentence per cell: library or binary, and which universe applies."""
    if lang == "go":
        if oracle.get("library"):
            return ("library (no main package): exported identifiers of importable packages are public API and leave both universes; "
                    "U1000 additionally treats every exported identifier as used" +
                    ("; with no tests the exported surface is also the set of roots, so the unreachable column judges unexported declarations only"
                     if oracle.get("mode") == "library" else "; test functions are the roots of the unreachable column"))
        return ("binary (has a main package): exported identifiers of importable packages are public API and leave both universes; exported ones in "
                "`internal/` and `main` packages are judged by the unreachable column (U1000 still treats them as used)")
    if lang == "ts":
        return ("library: declarations reachable by re-export from the package entry (package.json exports/main/module/types/bin) are public API "
                "and leave both universes" if oracle.get("library") else "no package entry found: every declaration is judged")
    kinds = oracle.get("crate_kinds", {})
    return ("library crates present (%s): their `pub` items are public API and leave both universes; `pub` items of binary crates are judged"
            % ", ".join("%s %d" % kv for kv in sorted(kinds.items())) if oracle.get("library") else
            "binary crates only: `pub` items are judged")


def run_cell(cell, repo, repo_dir, cache, binary, db, syms, force, results_dir, helpers, call_meta=None):
    """Run the dead-code oracle for one cell, grade lidx, write the lists. Returns the dict stored under cells.json[cell]['dead']."""
    lang = cell["lang"]
    oracle, cached = get_dead_oracle(cell, repo, repo_dir, cache, force, helpers, call_meta)
    claims, n_claims = lidx_claims(binary, repo_dir, db, syms)
    fid = cell["id"].replace("/", "__")
    out = {"oracle_meta": oracle.get("_meta", {}), "library": bool(oracle.get("library")), "universe_note": universe_note(oracle, lang, cell),
           "lidx_claims": n_claims, "load_errors": len(oracle.get("load_errors", []) or []), "oracle_stats": oracle.get("stats", {}),
           "decls": len(oracle["decls"]), "columns": {}}
    if lang == "go":
        out["oracle_mode"] = oracle.get("mode")
    if lang == "rust":
        out["crate_kinds"] = oracle.get("crate_kinds", {})
    dead_keys = {col: {match_dead.key_of(d) for d in oracle["decls"] if d.get(col) == "dead"} for col in match_dead.COLUMNS}
    for col in match_dead.COLUMNS:
        keys = dead_keys[col]
        inc = incoming_edges(db, syms, keys, lang, keys)
        summ, fp, fn, ungr = match_dead.grade_column(oracle, claims, lang, col, match.is_test_file, incoming=inc)
        out["columns"][col] = summ
        for suffix, data in (("fp", fp), ("fn", fn), ("ungradable", ungr)):
            dump_list(os.path.join(results_dir, "lists", "%s.dead_%s.%s.json" % (fid, col, suffix)), data)
    dump_list(os.path.join(results_dir, "lists", "%s.dead_oracle.json" % fid),
              [{k: d[k] for k in ("file", "line", "name", "kind", "subkind", "exported", "api", "unref", "reach") if k in d} | {k: d[k] for k in ("root", "withheld", "generated") if k in d}
               for d in oracle["decls"] if d.get("unref") == "dead" or d.get("reach") == "dead"])
    # declarations the oracle left out because it cannot judge them (the exclusion buckets the spot-check samples); roots and public API are rules
    excluded = []
    for d in oracle["decls"]:
        for col in match_dead.COLUMNS:
            b = match_dead.bucket(d, col, lang)
            if b in ("generated", "macro_generated", "cfg_twin", "oracle_unresolved_site", "impl_for_foreign_type", "trait_declaration", "no_verdict"):
                excluded.append({"column": col, "bucket": b, "file": d["file"], "line": d["line"], "name": d["name"], "kind": d["kind"]})
    dump_list(os.path.join(results_dir, "lists", "%s.dead_excluded.json" % fid), excluded)
    out["file_level"] = file_level(oracle, lang)
    out["cached_oracle"] = cached
    return out


def toolchains():
    """Versions of the tools the dead-code oracles add to the call-graph ones (kept apart from cells.json['toolchains'], which feeds the
    existing report line)."""
    out = {"staticcheck (honnef.co/go/tools)": go_mod_version("honnef.co/go/tools"), "golang.org/x/tools (RTA, go/analysis)": go_mod_version("golang.org/x/tools")}
    ts_dir = os.path.join(HERE, "ts_oracle", "node_modules", "typescript", "package.json")
    if os.path.exists(ts_dir):
        out["typescript (noUnusedLocals, LanguageService)"] = json.load(open(ts_dir))["version"]
    return out
