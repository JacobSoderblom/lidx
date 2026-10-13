"""Driver of `run.py --impact` (issue #451): grades analyze_impact, its test layer and trace_flow stop reasons per cell.

Works from the cached oracle outputs (the compilers are not re-run unless a cached output is stale) and talks to the lidx binary
under test over its JSONL RPC only; no corpus code runs. Writes results/seeds/<cell>.json (committed seeds),
results/impact/<cell>.json (per-cell tables and per-seed records) and results/lists/<cell>.impact_{extra,missing}.json.
Python 3 stdlib only.
"""
import json
import os
import re
import sys

import impact
import match


def comparable(cell):
    """The all-features Rust cells are diagnostics (no verdict anywhere in this benchmark) and are not graded here."""
    return not cell["id"].endswith("_all_features")


def dump_list(path, data):
    rows = sorted(json.dumps(x, separators=(",", ":"), sort_keys=True) for x in data)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("[\n" + ",\n".join(rows) + "\n]\n")


def run(a, lock, cache, binary, version, h):
    """h: callables and helpers from run.py (cells_for, fetch, index, get_oracle)."""
    rx = re.compile(a.only) if a.only else None
    rdir = os.path.join(a.results, "impact")
    sdir = os.path.join(a.results, "seeds")
    os.makedirs(rdir, exist_ok=True)
    os.makedirs(sdir, exist_ok=True)
    done = 0
    for repo in lock["repos"]:
        cells = [c for c in h["cells_for"](repo) if comparable(c) and (not rx or rx.search(c["id"]))]
        if not cells:
            continue
        print("== %s (%s @ %s)" % (repo["name"], repo["lang"], repo["sha"][:10]), flush=True)
        repo_dir = h["fetch"](repo, cache)
        db, secs = h["index"](binary, repo_dir, repo["name"], cache, a.reuse_index)
        syms, _ = match.load_lidx(db, repo_dir, repo["lang"])
        src = match.Sources(repo_dir)
        lidx = impact.Lidx(binary, repo_dir, db)
        try:
            for cell in cells:
                oracle, cached = h["get_oracle"](cell, repo, repo_dir, cache, a.force)
                graded = match.drop_closure_self(oracle)[0] if cell["lang"] == "go" else oracle
                graph = impact.Graph(graded, syms, cell["lang"], src=src)
                eligible = set(impact.seed_ids(syms))
                seeds, smeta = impact.draw_seeds(graph, cell["id"], eligible)
                fid = cell["id"].replace("/", "__")
                spath = os.path.join(sdir, fid + ".json")
                sdoc = {"cell": cell["id"], "repo": repo["name"], "repo_sha": repo["sha"], "rng_seed": impact.SEED, "per_cell": impact.PER_CELL,
                        "strata": dict(zip(impact.STRATUM_NAMES, impact.STRATA)), "draw": smeta, "seeds": seeds}
                if os.path.exists(spath) and not a.redraw_seeds:
                    old = json.load(open(spath))
                    if old["seeds"] != seeds:
                        print("   WARNING: %s: the committed seeds differ from a fresh draw (oracle or lidx symbol table changed); "
                              "grading the committed seeds, pass --redraw-seeds to replace them" % cell["id"], flush=True)
                    seeds = old["seeds"]
                else:
                    with open(spath, "w") as f:
                        json.dump(sdoc, f, indent=1)
                        f.write("\n")
                print("   %s: oracle %d edges%s, %d seeds %s" % (cell["id"], len(oracle["edges"]), " (cached)" if cached else "", len(seeds),
                                                                  smeta["stratum_drawn"]), flush=True)
                # diagnostic closure: Go without the edges whose callee is a function literal keyed at its enclosing declaration; TypeScript and
                # Rust with edges to abstract / trait members expanded to the overriding implementations (the compiler's own hierarchy)
                # TypeScript and Rust are graded against the closure with abstract / trait members expanded to the members that override them
                # (the compiler's own hierarchy; what RTA does for Go); the diagnostic is the compiler's edges as stated, without the expansion
                graph = impact.Graph(graded, syms, cell["lang"], expand=cell["lang"] != "go", src=src)
                alt = impact.Graph(graded, syms, "go", drop_closure=True, src=src) if cell["lang"] == "go" else impact.Graph(graded, syms, cell["lang"], src=src)
                rows, lists, tests_graded, nroots = impact.grade_cell(cell, graph, syms, lidx, src, seeds, log=lambda m: print(m, flush=True), alt_graph=alt)
                agg = impact.aggregate(rows, tests_graded)
                worst = {kn: impact.worst_seeds(rows, graph, kn) for kn in ("3", "inf")}
                extra, missing, miss_alt = impact.pair_lists(rows, graph)
                if miss_alt:
                    dump_list(os.path.join(a.results, "lists", fid + ".impact_missing_alt.json"), miss_alt)
                dump_list(os.path.join(a.results, "lists", fid + ".impact_extra.json"), extra)
                dump_list(os.path.join(a.results, "lists", fid + ".impact_missing.json"), missing)
                out = {"cell": cell["id"], "repo": repo["name"], "lang": cell["lang"], "variant": cell["variant"], "lidx_version": version,
                       "index_seconds": secs, "tests_graded": tests_graded, "test_roots": nroots, "seed_draw": smeta, "alt_closure": "no_function_value_edges" if cell["lang"] == "go" else "compiler_edges_as_stated", "tables": agg, "worst": worst,
                       "rows": impact.strip_private(rows)}
                with open(os.path.join(rdir, fid + ".json"), "w") as f:
                    json.dump(out, f, separators=(",", ":"))
                u = agg["up"]
                print("   %s: up k=3 P=%s R=%s | inf P=%s R=%s" % (cell["id"], u["3"]["precision"], u["3"]["recall"], u["inf"]["precision"], u["inf"]["recall"]),
                      flush=True)
                done += 1
        finally:
            lidx.close()
    return done
