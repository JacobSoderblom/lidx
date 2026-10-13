"""The "Impact, tests and flows" section of results/report.md (issue #451) and results/impact-outliers.md. Python 3 stdlib only."""
import glob
import json
import os

import report

LANGS = ("go", "ts", "rust")
KN = (("3", "k = 3 (lidx default)"), ("inf", "unbounded (lidx caps at 10)"))


def load(results_dir):
    cells = {}
    for p in sorted(glob.glob(os.path.join(results_dir, "impact", "*.json"))):
        d = json.load(open(p))
        cells[d["cell"]] = d
    return cells


def _order(cells):
    ranks = {cid: i for i, cid in enumerate(report.BASE_ORDER)}
    return sorted(cells, key=lambda c: (LANGS.index(cells[c]["lang"]), ranks.get(c, 999), c))


def p(x):
    return "-" if x is None else "%.3f" % x


def pci(x, c):
    return "-" if x is None else "%.3f %s" % (x, "[%.3f, %.3f]" % tuple(c) if c and c[0] is not None else "")


def rate(d):
    if not d["graded"]:
        return "-"
    s = "%d/%d = %.1f%% [%.1f, %.1f]" % (d["wrong"], d["graded"], 100 * d["rate"], 100 * d["ci"][0], 100 * d["ci"][1])
    if "wrong_external_only" in d and d["wrong"]:
        s += " (%d external-only)" % d["wrong_external_only"]
    if d.get("wrong_closure_only"):
        s += " (%d function-literal callees only)" % d["wrong_closure_only"]
    return s


def edge_recall(results_dir):
    """Call-edge recall of each cell from the call-edge benchmark (cells.json), for the test reach comparison."""
    try:
        meta = json.load(open(os.path.join(results_dir, "cells.json")))
    except OSError:
        return {}
    return {cid: c["calls"]["recall"] for cid, c in meta["cells"].items() if "calls" in c}


def cell_short(cid):
    return cid.split("/", 1)[1]


def section(results_dir):
    cells = load(results_dir)
    if not cells:
        return ""
    L = []
    w = L.append
    ids = _order(cells)
    erec = edge_recall(results_dir)
    n_seeds = sum(c["tables"]["seeds"] for c in cells.values())
    w("## Impact, tests and flows")
    w("")
    w("**Quoting policy:** a figure from this section is quoted in `README.md` only for a language and kind whose hand spot-check artefact rate is under 10% (see \"Spot-check of impact pairs\" below); the Go closure without function-value edges is an upper-bound view built after the fact, not a verdict.")
    w("")
    w("`analyze_impact` upstream sets, its test layer and `trace_flow` stop reasons graded against the **closure** of the compiler call graph "
      "of each cell (Go RTA with dynamic calls at the concrete implementations, tsc, rust-analyzer SCIP; no compiler is re-run). "
      "%d cells (the Rust all-features cells are diagnostics and are not graded), %d seeds, 100 per cell, stratified by oracle in-degree "
      "(0, 1-3, 4-10, >10) and drawn with seed 2026 from the declarations the oracle judged (`results/seeds/<cell>.json`). Definitions, the lidx calls "
      "and every exclusion: README, \"Impact, tests and flows\"." % (len(cells), n_seeds))
    w("")
    w("- **Pooled** figures are pair-level: a pair is (seed, member). Intervals are 95% Wilson over pairs, which cluster by seed and are optimistic; "
      "**macro** figures average per-seed precision and recall (a seed with no members on a side has no rate for it) with a 1,000-sample bootstrap "
      "interval over seeds, so one high-fan-out seed cannot dominate.")
    w("- A lidx member is **right** when it is in the oracle closure, **wrong** when the oracle analysed that function and it is not in the closure, "
      "**ungradable** otherwise (file or function the oracle did not analyse; counted in the `ungr.` column). Recall counts the oracle members lidx has "
      "a symbol for; seeds where lidx truncated its answer (`limit` 2,000) leave the recall figures and stay in the precision figures.")
    w("- The graded `analyze_impact` call is `direction=up`, `include_tests=true`, the test and historical layers off, `kinds=[CALLS, DISPATCH]`, and `max_response_bytes` "
      "raised so the default 30,000-byte cut does not apply (the plain call is graded separately below). The oracle is a call graph and cannot judge a dependent reached "
      "through a reference, a `USES` or an import, so the **default call** (no `kinds`: every edge kind) is reported beside it, and the members it adds that the oracle "
      "lacks (\"beyond calls\") are counted, not called wrong. lidx caps `max_depth` at 10, so its \"unbounded\" column is depth 10 against the unbounded oracle closure.")
    w("")
    for lang in LANGS:
        lc = [c for c in ids if cells[c]["lang"] == lang]
        if not lc:
            continue
        w("### %s" % report.LANG_NAMES[lang])
        w("")
        for kn, label in KN:
            w("**Upstream set, %s**" % label)
            w("")
            w("| Cell | Seeds | Oracle members | Wrong | Ungr. | Precision | Recall | Macro precision | Macro recall | Truncated | Default call precision | Default call recall | Default call beyond calls |")
            w("|---|---:|---:|---:|---:|---|---|---|---|---:|---|---|---:|")
            for c in lc:
                t = cells[c]["tables"]
                u = t["up"][kn]
                dk = t["all_kinds"][kn]
                w("| %s | %d | %d | %d | %d | %s | %s | %s | %s | %d | %s | %s | %d |" % (
                    cell_short(c), t["seeds_graded"], u["oracle_members"], u["fp"], u["ungradable"], pci(u["precision"], u["precision_ci"]),
                    pci(u["recall"], u["recall_ci"]), pci(u["macro_precision"], u["macro_precision_ci"]), pci(u["macro_recall"], u["macro_recall_ci"]),
                    u["truncated_seeds"], pci(dk["precision"], dk["precision_ci"]), pci(dk["recall"], dk["recall_ci"]), dk["fp_beyond_calls"]))
            w("")
        if lang == "go":
            w("**Go, without function-value edges (upper-bound view: it excludes real function-value calls that lidx cannot resolve, and was built post hoc, after seeing lidx's misses; no verdict).** The Go oracle resolves the call of a function value (`parg(cmd, args)`, `p.cancel()`, `cmd()`) to every address-taken function "
              "of that signature that RTA reaches and keys a function literal at its enclosing declaration, so such an edge names the function that built the closure "
              "(or an unrelated closure of the same signature). lidx does not resolve a call of a function value. The same grading with these edges (`closure` or "
              "`dispatch` without interface alternatives in the oracle; interface invocations stay) left out of the closure (calls-only; precision differs only through the members these edges add):")
        else:
            w("**%s, the compiler's edges as stated.** The graded closure expands an edge to a %s member to the members the compiler's own hierarchy lists as "
              "overriding it (what RTA does for Go, without allocation analysis). The same grading without that expansion, i.e. the call graph exactly as the oracle emits it "
              "(calls-only):" % (report.LANG_NAMES[lang], "trait" if lang == "rust" else "abstract / interface"))
        w("")
        w("| Cell | k = 3 oracle members | precision | recall | unbounded oracle members | precision | recall |")
        w("|---|---:|---|---|---:|---|---|")
        for c in lc:
            nc = cells[c]["tables"].get("up_alt", {})
            if "3" in nc and "inf" in nc:
                a, b = nc["3"], nc["inf"]
                w("| %s | %d | %s | %s | %d | %s | %s |" % (cell_short(c), a["oracle_members"], pci(a["precision"], a["precision_ci"]), pci(a["recall"], a["recall_ci"]),
                                                          b["oracle_members"], pci(b["precision"], b["precision_ci"]), pci(b["recall"], b["recall_ci"])))
        w("")
        if lang in ("go", "ts"):
            w("**lidx's interface-dispatch fan-out, counted.** A member whose path to the seed crosses a hop bound by the dispatch query (`resolution_kind: interface_dispatch`) is a "
              "dispatch-derived claim; it is graded like every other member, against the oracle's concrete-implementation edges (RTA / the checker); lidx gets no credit from the "
              "oracle's interface-method alternatives. k = 3, calls-only.")
            w("")
            w("| Cell | Seeds with a dispatch-derived member | Dispatch-derived members | Right | Wrong |")
            w("|---|---:|---:|---:|---:|")
            for c in lc:
                dsp = cells[c]["tables"].get("dispatch") or {}
                if dsp:
                    w("| %s | %d | %d | %d | %d |" % (cell_short(c), dsp["seeds_with_dispatch"], dsp["members"], dsp["right"], dsp["wrong"]))
            w("")
        w("**What the default hides and what a plain call returns**")
        w("")
        w("`hidden` = seeds where the depth-3 set is under half of the unbounded set (oracle closure / lidx's own answer). `plain call` = "
          "`analyze_impact(id, direction=up, max_depth=3, include_tests=true)` with every other parameter at its default (30,000-byte response cut, all layers); "
          "`beyond 10` = seeds whose oracle closure has callers farther than 10 hops.")
        w("")
        w("| Cell | hidden (oracle) | hidden (oracle, alt. closure) | hidden (lidx) | Plain call recall | Plain call truncated seeds | beyond 10 |")
        w("|---|---|---|---|---:|---:|")
        for c in lc:
            t = cells[c]["tables"]
            hd = t["depth_hides_over_half"]
            pl = t["up_default"]["3"]
            w("| %s | %d/%d = %s | %d/%d = %s | %d/%d = %s | %s | %d | %d |" % (
                cell_short(c), hd["oracle_hidden"], hd["oracle_seeds"], p(hd["oracle_share"]), hd["alt_hidden"], hd["alt_seeds"], p(hd["alt_share"]),
                hd["lidx_hidden"], hd["lidx_seeds"], p(hd["lidx_share"]),
                pci(pl["recall"], pl["recall_ci"]), pl["truncated_seeds"], t["oracle_deeper_than_cap_seeds"]))
        w("")
        w("**`lower_bound` against per-seed completeness** (does `lower_bound: true` hold exactly when lidx misses an oracle caller?). "
          "Cells are `lb and incomplete / lb and complete / no lb and incomplete / no lb and complete` over seeds lidx did not truncate; "
          "the agreeing cells are the first and the last.")
        w("")
        w("| Cell | k = 3 | agree | unbounded | agree |")
        w("|---|---|---:|---|---:|")
        for c in lc:
            hs = cells[c]["tables"]["lower_bound_honesty"]
            row = [cell_short(c)]
            for kn in ("3", "inf"):
                h = hs[kn]
                tot = sum(h.values())
                row.append("%d / %d / %d / %d" % (h["lb_true_incomplete"], h["lb_true_complete"], h["lb_false_incomplete"], h["lb_false_complete"]))
                row.append("%s" % ("-" if not tot else "%.1f%%" % (100 * (h["lb_true_incomplete"] + h["lb_false_complete"]) / tot)))
            w("| " + " | ".join(row) + " |")
        w("")
        tc = [c for c in lc if cells[c]["tests_graded"]]
        if tc:
            w("**Test reach** (`Tests(s)`: %s; lidx: the test layer of `analyze_impact` alone, `enable_direct=false`). A seed with no oracle test and no lidx test "
              "is a correct empty answer and is counted in `empty ok`. `not root` = wrong tests that are functions the oracle analysed that are not test roots "
              "(helpers); `beyond calls` = tests lidx reaches only through a reference, use or import hop (the test layer follows every edge kind and cannot be restricted), "
              "counted, not wrong; `missed Benchmark/Fuzz` = missing pairs whose test is a `Benchmark*` or `Fuzz*` function. `Upstream recall` is the recall of the same cell's "
              "upstream set at the same k (the transitive call recall; the test layer should not be lower), `edge recall` the call-edge benchmark's recall for the cell." % ("Go `Test*`/`Benchmark*`/`Fuzz*` in `_test.go`" if lang == "go" else "Rust `#[test]` functions"))
            w("")
            w("| Cell | k | Seeds | Oracle pairs | Wrong | not root | beyond calls | Precision | Recall | Macro precision | Macro recall | Upstream recall | edge recall | missed Benchmark/Fuzz | empty ok | oracle test, lidx none |")
            w("|---|---|---:|---:|---:|---:|---:|---|---|---|---|---|---|---:|---:|---:|")
            for c in tc:
                for kn, label in KN:
                    u = cells[c]["tables"]["tests"][kn]
                    w("| %s | %s | %d | %d | %d | %d | %d | %s | %s | %s | %s | %s | %s | %d | %d | %d |" % (
                        cell_short(c), kn, u["seeds"], u["oracle_pairs"], u["fp"], u["fp_not_root"], u["fp_noncall"], pci(u["precision"], u["precision_ci"]),
                        pci(u["recall"], u["recall_ci"]), pci(u["macro_precision"], u["macro_precision_ci"]), pci(u["macro_recall"], u["macro_recall_ci"]),
                        p(cells[c]["tables"]["up"][kn]["recall"]), p(erec.get(c)), u["fn_bench_fuzz"], u["empty_correct"], u["nonempty_oracle_lidx_empty"]))
            w("")
            w("")
        elif lang == "ts":
            w("**Test reach**: void for TypeScript. Test files are out of scope on both sides (the oracle does not analyse them, lidx's test layer would be graded "
              "against nothing), so no figure is reported.")
            w("")
        w("**`trace_flow` stop reasons and reach** (`direction=downstream`, `max_hops=k`, `max_bytes=200000`). False leaf = a leaf marked `no_callees` where the "
          "oracle has an in-scope callee (Go: `function-literal callees only` = every such callee is an edge whose callee is a closure keyed at its builder, the call of a function value); **gap with no in-repo callee left** = a leaf marked `unresolved_calls` where the oracle has no in-scope callee. This is not a claim that lidx is wrong: lidx defines `unresolved_calls` as a resolution gap (pending unresolved references), and an unresolved external name is a true gap. The narrower point it supports is that lidx itself tags most of these names `external` and still reports `unresolved_calls`, so the signal says little. Leaves marked `truncated`, "
          "`depth_limit` (and every reason other than these two) are excluded from both. Reach is the set of symbols the trace names against `Down_k(s)`.")
        w("")
        w("| Cell | k | False leaf | Gap, no in-repo callee left | Reach precision | Reach recall | Macro precision | Macro recall | Byte-truncated seeds |")
        w("|---|---|---|---|---|---|---|---|---:|")
        for c in lc:
            for kn, label in KN:
                d = cells[c]["tables"]["down"][kn]
                w("| %s | %s | %s | %s | %s | %s | %s | %s | %d |" % (
                    cell_short(c), kn, rate(d["leaves"]["no_callees"]), rate(d["leaves"]["unresolved_calls"]), pci(d["precision"], d["precision_ci"]),
                    pci(d["recall"], d["recall_ci"]), pci(d["macro_precision"], d["macro_precision_ci"]), pci(d["macro_recall"], d["macro_recall_ci"]),
                    d["truncated_seeds"]))
        w("")
    w("Per-seed outliers (worst 10 seeds per cell and k with their missing and extra symbols): [impact-outliers.md](impact-outliers.md). "
      "Committed pairs: `results/lists/<cell>.impact_extra.json`, `.impact_missing.json` (up to 10 per seed and k).")
    w("")
    sc = os.path.join(results_dir, "spotcheck-impact.json")
    if os.path.exists(sc):
        w(spot_section(json.load(open(sc))))
    return "\n".join(L)


KIND_LABEL = {"extra": "extra (precision)", "missing": "missing (recall)", "missing_alt": "missing, Go closure without function-value edges (recall, diagnostic)"}


def spot_section(sc):
    L = []
    w = L.append
    w("### Spot-check of impact pairs")
    w("")
    w("Every pair below was read against the source along the whole chain (seed %s; cells %s). An *extra* pair is a lidx member the oracle closure lacks, an *missing* "
      "pair an oracle caller lidx lacks, both from the committed lists (`results/lists/<cell>.impact_{extra,missing,missing_alt}.json`), k = 3 pairs first, at most 3 per seed, "
      "20 per language and kind (Go has only 18 extra pairs). Verdicts: `lidx_error` (lidx is wrong or misses a real chain), `oracle_artefact` (the compiler closure is not "
      "the right judge: the chain is real and the oracle cannot show it, or the oracle's chain is not real), `matching_artefact` (our matching or keying mis-states the pair). "
      "A number is quoted in `README.md` only for a language and kind whose artefact rate is under 10%%." % (sc["seed"], ", ".join(sc["cells"])))
    w("")
    w("| Language | Sample | lidx_error | oracle_artefact | matching_artefact | artefact rate [95% Wilson CI] | quoted |")
    w("|---|---|---:|---:|---:|---|---|")
    for lang, kinds in sc["languages"].items():
        for kind in ("extra", "missing", "missing_alt"):
            items = kinds.get(kind)
            if not items:
                continue
            c = {k: sum(1 for i in items if i["verdict"] == k) for k in ("lidx_error", "oracle_artefact", "matching_artefact")}
            n = len(items)
            a = c["oracle_artefact"] + c["matching_artefact"]
            w("| %s | %s (n=%d) | %d | %d | %d | %d/%d = %.1f%% %s | %s |" % (lang, KIND_LABEL[kind], n, c["lidx_error"], c["oracle_artefact"], c["matching_artefact"], a, n,
                                                                         100 * a / max(n, 1), report.ci_pct(a, n), "yes" if a / max(n, 1) < 0.10 else "no"))
    w("")
    w(sc.get("summary", ""))
    w("")
    for lang, kinds in sc["languages"].items():
        w("#### %s impact samples" % lang)
        w("")
        for kind in ("extra", "missing", "missing_alt"):
            if not kinds.get(kind):
                continue
            w("**%s**" % KIND_LABEL[kind])
            w("")
            for i in kinds[kind]:
                w("- `%s` seed `%s` k=%s, `%s`: **%s**, %s" % (i["id"], i["seed"], i["k"], i["member"], i["verdict"], i["note"]))
            w("")
    return "\n".join(L)


def outliers(results_dir):
    cells = load(results_dir)
    L = ["# Impact outliers: worst 10 seeds per cell", "",
         "Generated by `run.py --impact`. Seeds ranked by per-seed F1 of the upstream set (a seed with no members on either side scores 1; ties by the number of "
         "wrong plus missing members). `extra` = lidx members the oracle closure lacks (the oracle analysed them), `missing` = oracle callers lidx lacks, at most "
         "10 shown each. Unbounded = lidx at its depth cap of 10 against the unbounded oracle closure.", ""]
    for cid in _order(cells):
        L.append("## %s" % cid)
        L.append("")
        for kn, label in KN:
            L.append("### %s" % label)
            L.append("")
            for s in cells[cid]["worst"][kn]:
                L.append("- `%s` (in-degree stratum %s): F1 %.2f, oracle %d, lidx %d, right %d, extra %d, missing %d%s" % (
                    s["seed"], s["stratum"], s["f1"], s["oracle"], s["lidx"], s["tp"], s["extra_n"], s["missing_n"], ", lidx truncated" if s["truncated"] else ""))
                if s["extra"]:
                    L.append("  - extra: " + "; ".join(s["extra"]))
                if s["missing"]:
                    L.append("  - missing: " + "; ".join(s["missing"]))
            L.append("")
    return "\n".join(L)


def write_outliers(results_dir):
    if os.path.isdir(os.path.join(results_dir, "impact")) and glob.glob(os.path.join(results_dir, "impact", "*.json")):
        with open(os.path.join(results_dir, "impact-outliers.md"), "w") as f:
            f.write(outliers(results_dir) + "\n")
