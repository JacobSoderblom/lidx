"""The "Dead code" section of the report. Python 3 stdlib only."""
import json
import os
from collections import Counter

import match_dead

COL_NAMES = {"unref": "Unreferenced", "reach": "Unreachable"}
LANGS = (("go", "Go"), ("ts", "TypeScript"), ("rust", "Rust"))
ORACLE_NAMES = {
    "go": {"unref": "staticcheck U1000 (go/analysis over packages.Load)", "reach": "RTA reachability, the machinery of `golang.org/x/tools/cmd/deadcode`"},
    "ts": {"unref": "`noUnusedLocals` diagnostics 6133/6196 + LanguageService `findReferences`", "reach": "closure over the checker's reference graph from the package entry"},
    "rust": {"unref": "rust-analyzer SCIP occurrences", "reach": "closure over SCIP references from `main`, tests and library `pub` items"},
}


def pct(x):
    return "-" if x is None else "%.3f" % x


def ci(c):
    return "-" if not c or c[0] is None else "[%.3f, %.3f]" % (c[0], c[1])


def pr(r, k="precision"):
    return "-" if r.get(k) is None else "%s %s" % (pct(r.get(k)), ci(r.get(k + "_ci")))


NOT_REPORTED = "not reported"
# Rust recall is not reported: against the noisy SCIP dead set, 2 of the 5 missed declarations that exist in the corpus
# (every one of them was read) were oracle artefacts (40%), above the 20% bar; precision is unaffected.
RECALL_NOT_REPORTED = {"rust"}


def rec(lang, r, key="recall"):
    """Recall cell: `not reported` for the languages in RECALL_NOT_REPORTED, else the usual figure."""
    return NOT_REPORTED if lang in RECALL_NOT_REPORTED else pr(r, key)


def pooled(readings):
    """Pool several readings (dicts with graded/tp/dead/found) into one precision and recall with Wilson intervals."""
    tp = sum(r["tp"] for r in readings)
    g = sum(r["graded"] for r in readings)
    f = sum(r["found"] for r in readings)
    d = sum(r["dead"] for r in readings)
    return {"graded": g, "tp": tp, "fp": g - tp, "precision": match_dead.ratio(tp, g), "precision_ci": match_dead.wilson(tp, g),
            "dead": d, "found": f, "recall": match_dead.ratio(f, d), "recall_ci": match_dead.wilson(f, d)}


def cell_order(cells):
    rank = {l: i for i, (l, _) in enumerate(LANGS)}
    return sorted((cid for cid, c in cells.items() if "dead" in c), key=lambda cid: (rank[cells[cid]["lang"]], list(cells).index(cid)))


def kind_of_cell(c):
    d = c["dead"]
    if c["lang"] == "go":
        return "library (no main package)" if d["library"] else "binary"
    if c["lang"] == "ts":
        return "library" if d["library"] else "no package entry"
    ck = d.get("crate_kinds", {})
    has_bin = any(k in ck for k in ("bin", "example", "build"))
    if d["library"]:
        return "library + binary crates" if has_bin else "library"
    return "binary"


def section(meta, cells, results_dir):
    ids = cell_order(cells)
    if not ids:
        return ""
    L = []
    w = L.append
    w("## Dead code")
    w("")
    w("lidx's `dead_symbols` (`src/db/analytics.rs`) graded against oracles that neither lidx nor repowise wrote. Two columns, because the "
      "tools make the weaker claim and users want the stronger one:")
    w("")
    w("- **Unreferenced** (what `dead_symbols` and repowise's `unused_export` / `unused_internal` compute): Go `staticcheck -checks U1000` "
      "(`honnef.co/go/tools` pinned in `go_oracle/go.mod`, run as a go/analysis pass over `packages.Load`; it treats every exported identifier as used, so "
      "the Go universe is the unexported declarations); TypeScript `noUnusedLocals` diagnostics 6133/6196 for non-exported declarations (0 on zod, hono, h3, ky and trpc, whose dead set comes from `findReferences`; 8 on immer) and "
      "LanguageService `findReferences` for exported ones; Rust SCIP occurrences (a declaration with no occurrence outside its own text, `use` lines "
      "and the impl blocks of its own type). rustc's `dead_code` lint is not used: it needs `cargo check`, which runs build scripts and proc macros.")
    w("- **Unreachable** (closure from roots): Go RTA from `main`/`init`/tests (the machinery of `golang.org/x/tools/cmd/deadcode`; `res.Reachable` is "
      "emitted instead of running the binary), TypeScript and Rust the reference closure (calls, `new`, callbacks and every other mention the checker or "
      "SCIP resolves) from the package entry / `main` / tests / library `pub` items. Only functions and methods get this verdict.")
    w("")
    w("Universe: functions, methods and types of the cell's scope (TypeScript also module-level consts, shown as their own kind); lidx claims are "
      "all `dead_symbols` rows (per-category limit raised past the repository size). Excluded from the universe, each a **named bucket** with "
      "counts per cell in the tables below: `public_api` (exported from a library; Go: exported in an importable package, TypeScript: reachable "
      "by re-export from the package entry, Rust: `pub` in a library crate), roots (`test_root`, `main_init`, `library_root`: live by construction), "
      "and what the oracle cannot judge (`exported_used_by_rule`: Go, unreferenced column, an exported identifier of an unimportable `internal/` or "
      "`main` package, which U1000's rules 1.1-1.4 and 2.1 mark used whatever the code does, so its verdict is by rule and the unreachable column "
      "judges the declaration instead (issue #503); `generated`, "
      "`macro_generated`, `cfg_twin`, `oracle_unresolved_site`, `impl_for_foreign_type` (Rust: methods of impls for foreign or primitive self types), `trait_declaration` (Rust), `not_a_function`). A claim on a declaration in the universe is right or wrong by "
      "the oracle's verdict; a claim on a public-API or root declaration is **wrong by construction** (counted under its bucket name, so the cost "
      "of having no exported-symbol policy is visible); a claim on anything the oracle cannot judge is ungradable (its own bucket).")
    w("")
    w("Tests as roots (issue #472): the `no_tests` cells (Go without `-tests`, Rust `--no-tests`, TypeScript on production code) leave test code out of the "
      "oracle's roots, so lidx is asked with `tests_are_roots: false` there: references from test files and test functions do not keep a declaration "
      "alive, and what only tests reach is claimed (reason `reached_only_from_tests`). The cells that include tests, and the Rust `path_tests_excluded` "
      "cells (inline `#[cfg(test)]` code stays a root there), use the default.")
    w("")
    w("Two readings per cell. **All claims**: every `dead_symbols` row. **Without exported/`pub` symbols**: the same rows after dropping every "
      "one on an exported (Go identifier rule, TypeScript `export`, Rust `pub`) declaration, with recall over the unexported dead declarations: what "
      "an exported-symbol policy would buy. Every figure carries a 95% Wilson interval; recall is over oracle-dead declarations in the universe. "
      "Cells without tests judge a repository index that includes its tests against a production-only program, so a declaration used only by "
      "tests is dead for the oracle and live for lidx (`incoming_from_tests_only` in the missed-declaration categories).")
    w("")
    dt = meta.get("dead_toolchains")
    if dt:
        w("Tools: " + "; ".join("%s %s" % kv for kv in dt.items()) + ".")
        w("")
    # ---- per-cell tables -------------------------------------------------------------------------------
    for col in match_dead.COLUMNS:
        w("### %s: per cell" % COL_NAMES[col])
        w("")
        w("| Cell | Library / binary | Universe | Oracle-dead | Claims graded | P, all claims [95% CI] | R [95% CI] | P without exported [95% CI] | R without exported [95% CI] |")
        w("|---|---|---:|---:|---:|---|---|---|---|")
        for cid in ids:
            c = cells[cid]
            x = c["dead"]["columns"][col]
            lib = kind_of_cell(c)
            w("| %s | %s | %d | %d | %d | %s | %s | %s | %s |" % (cid, lib, x["universe"], x["dead"], x["all"]["graded"], pr(x["all"]), rec(c["lang"], x["all"]),
                                                                pr(x["no_exported"]), rec(c["lang"], x["no_exported"])))
        w("")
    w("### Library or binary, and the universe that applies")
    w("")
    for cid in ids:
        c = cells[cid]
        w("- `%s`: %s" % (cid, c["dead"]["universe_note"]))
    w("")
    # ---- pooled per language ---------------------------------------------------------------------------
    w("### Pooled per language")
    w("")
    w("Sums over the cells of a language (the `tests` and `no_tests` cells of one repository overlap, so the pooled interval is narrower than the "
      "evidence supports; per-cell rows are the figures to quote).")
    w("")
    w("Rust recall is **not reported** (shown as `not reported`): the spot-check read all 5 missed Rust declarations the corpus has and 2 of them "
      "(40%) were oracle artefacts, calls inside `rgtest!`-generated tests that SCIP does not resolve, above the 20% bar. Precision is unaffected. "
      "Methods of impls for foreign or primitive self types (`impl Logic for bool`) and trait items are named buckets (`impl_for_foreign_type`, "
      "`trait_declaration`) and are not in the Rust universe.")
    w("")
    w("| Language | Column | Oracle-dead | Claims graded | P, all claims [95% CI] | R [95% CI] | P without exported [95% CI] | R without exported [95% CI] |")
    w("|---|---|---:|---:|---|---|---|---|")
    for lang, name in LANGS:
        lid = [cid for cid in ids if cells[cid]["lang"] == lang]
        if not lid:
            continue
        for col in match_dead.COLUMNS:
            a = pooled([cells[cid]["dead"]["columns"][col]["all"] for cid in lid])
            b = pooled([cells[cid]["dead"]["columns"][col]["no_exported"] for cid in lid])
            w("| %s | %s | %d | %d | %s | %s | %s | %s |" % (name, COL_NAMES[col], a["dead"], a["graded"], pr(a), rec(lang, a), pr(b), rec(lang, b)))
    w("")
    # ---- confidence tiers and the public_api group -----------------------------------------------------
    w("### Confidence tiers and the `public_api` group")
    w("")
    w("`dead_symbols` tags each claim `high` / `medium` (see CONTEXT.md, \"Dead-symbol confidence tier\") and, by default, leaves public-API declarations "
      "out, counting them in `counts.public_api`. Precision per tier, pooled per language and column (tp/graded); `untiered` is a binary that "
      "predates the tiers. The last column is the size of the opt-in `public_api` group and how many of its declarations the oracle calls dead.")
    w("")
    w("| Language | Column | high | medium | low | untiered | public_api group | of them oracle-dead |")
    w("|---|---|---|---|---|---|---|---|")
    for lang, name in LANGS:
        lid = [cid for cid in ids if cells[cid]["lang"] == lang]
        for col in match_dead.COLUMNS:
            if not lid:
                continue
            tiers = {}
            for t in ("high", "medium", "low", "untiered"):
                parts = [cells[cid]["dead"]["columns"][col].get("by_tier", {}).get(t) for cid in lid]
                parts = [x for x in parts if x]
                tp, g = sum(x["tp"] for x in parts), sum(x["graded"] for x in parts)
                tiers[t] = "-" if not g else "%s (%d/%d)" % (pct(match_dead.ratio(tp, g)), tp, g)
            group = sum(cells[cid]["dead"].get("public_api_group", 0) for cid in lid)
            withheld = sum(len(cells[cid]["dead"]["columns"][col].get("public_api_oracle_dead", [])) for cid in lid)
            w("| %s | %s | %s | %s | %s | %s | %d | %d |" % (name, COL_NAMES[col], tiers["high"], tiers["medium"], tiers["low"], tiers["untiered"], group, withheld))
    w("")
    # ---- per kind ------------------------------------------------------------------------------------
    w("### Per symbol kind")
    w("")
    w("Precision is tp/graded over lidx claims whose oracle declaration has that kind; recall is found/oracle-dead of that kind. "
      "`const` is TypeScript only. `-` where the cell has none.")
    w("")
    w("| Cell | Kind | Unreferenced P [95% CI] | Unreferenced R [95% CI] | Unreachable P [95% CI] | Unreachable R [95% CI] |")
    w("|---|---|---|---|---|---|")
    for cid in ids:
        cols = cells[cid]["dead"]["columns"]
        kinds = [k for k in ("function", "method", "type", "const") if k in cols["unref"]["by_kind"] or k in cols["reach"]["by_kind"]]
        for k in kinds:
            def cellfmt(col, key):
                if key == "recall" and cells[cid]["lang"] in RECALL_NOT_REPORTED:
                    return NOT_REPORTED
                bk = cols[col]["by_kind"].get(k)
                if not bk:
                    return "-"
                r = bk["all"]
                return "%s (%d/%d)" % (pr(r, key), r["tp" if key == "precision" else "found"], r["graded" if key == "precision" else "dead"])
            w("| %s | %s | %s | %s | %s | %s |" % (cid, k, cellfmt("unref", "precision"), cellfmt("unref", "recall"), cellfmt("reach", "precision"), cellfmt("reach", "recall")))
    w("")
    # ---- exported-symbol reading ---------------------------------------------------------------------------
    w("### The exported-symbol reading")
    w("")
    w("What an exported-symbol policy (never claim an exported / `pub` declaration) buys, pooled per language and column: claims dropped, and "
      "the wrong claims among them by category.")
    w("")
    w("| Language | Column | Graded claims | Dropped by the policy | of them wrong | P all claims | P without exported | Wrong claims, by category (all claims) |")
    w("|---|---|---:|---:|---:|---|---|---|")
    for lang, name in LANGS:
        lid = [cid for cid in ids if cells[cid]["lang"] == lang]
        for col in match_dead.COLUMNS:
            if not lid:
                continue
            a = pooled([cells[cid]["dead"]["columns"][col]["all"] for cid in lid])
            b = pooled([cells[cid]["dead"]["columns"][col]["no_exported"] for cid in lid])
            cats = Counter()
            for cid in lid:
                cats.update(cells[cid]["dead"]["columns"][col]["fp_categories"])
            dropped = a["graded"] - b["graded"]
            w("| %s | %s | %d | %d | %d | %s | %s | %s |" % (name, COL_NAMES[col], a["graded"], dropped, a["fp"] - b["fp"], pct(a["precision"]), pct(b["precision"]),
                                                         ", ".join("%s %d" % kv for kv in cats.most_common(8))))
    w("")
    # ---- categories ----------------------------------------------------------------------------------
    w("### Wrong claims and missed declarations, by category")
    w("")
    w("Wrong claims (lidx says dead, oracle says live, or live by construction), pooled per language and column. Categories are in "
      "`results/lists/<cell>.dead_<column>.fp.json`. Missed declarations (oracle-dead, lidx does not claim) are labelled from lidx's own incoming "
      "reachability edges to the declaration: `incoming_from_tests_only`, `incoming_from_dead_code_only` (a dead cluster keeps itself alive), "
      "`incoming_call_from_live_code` (an edge the oracle contradicts), `kind_not_in_dead_symbols` (interfaces, enums and traits are not in "
      "`dead_symbols`' kind list), `no_incoming_edge_but_filtered` (a filter in the query rescued it).")
    w("")
    for lang, name in LANGS:
        lid = [cid for cid in ids if cells[cid]["lang"] == lang]
        if not lid:
            continue
        for col in match_dead.COLUMNS:
            fp, fn = Counter(), Counter()
            for cid in lid:
                fp.update(cells[cid]["dead"]["columns"][col]["fp_categories"])
                fn.update(cells[cid]["dead"]["columns"][col]["fn_categories"])
            w("- **%s, %s.** Wrong claims: %s. Missed: %s." % (name, COL_NAMES[col].lower(), ", ".join("%s %d" % kv for kv in fp.most_common(10)) or "none",
                                                          ", ".join("%s %d" % kv for kv in fn.most_common(10)) or "none"))
    w("")
    # ---- buckets --------------------------------------------------------------------------------------
    w("### Exclusion buckets")
    w("")
    w("Oracle side: declarations left out of the universe, per cell and column (every bucket is sampled in the spot-check below). "
      "lidx side: claims the oracle cannot judge.")
    w("")
    w("| Cell | Column | Oracle-side buckets | Ungradable lidx claims |")
    w("|---|---|---|---|")
    for cid in ids:
        for col in match_dead.COLUMNS:
            x = cells[cid]["dead"]["columns"][col]
            w("| %s | %s | %s | %s |" % (cid, COL_NAMES[col], ", ".join("%s %d" % kv for kv in x["oracle_buckets"].items()) or "-",
                                       ", ".join("%s %d" % kv for kv in x["claims_ungradable"].items()) or "-"))
    w("")
    # ---- file level ------------------------------------------------------------------------------------
    w("### File level")
    w("")
    w("lidx has no file-level dead answer today. The oracle records the two facts a file-level claim (lidx's, or repowise's `unreachable_file`) would "
      "be graded against: **no importer** (Go: no in-repo package imports the file's package, from `packages.Package.Imports`; TypeScript: no program "
      "file resolves an import or re-export to it, `ts.resolveModuleName`; Rust: no other file refers to its module symbol in SCIP) and **unreachable "
      "from roots** (Go and Rust: the file declares functions and none is reachable; TypeScript: not in the import closure of the entry files). "
      "Main packages, tests, generated files, entry files and `lib.rs`/`main.rs` are not counted as candidates. Lists are in "
      "`results/lists/<cell>.dead_oracle.json`; the counts:")
    w("")
    w("| Cell | Files | Candidates for no-importer | No importer | Unreachable from roots |")
    w("|---|---:|---:|---:|---:|")
    for cid in ids:
        f = cells[cid]["dead"]["file_level"]
        w("| %s | %d | %d | %d | %d |" % (cid, f["files"], f["importer_candidates"], f["no_importer"], f["unreachable"]))
    w("")
    # ---- time and memory ----------------------------------------------------------------------------------
    w("### Time and memory")
    w("")
    w("Wall-clock seconds and peak resident set of the oracle process tree (sampled every 0.1 s), cold, one cell at a time on one machine. "
      "These come on top of the call-graph oracles' time (U1000 and RTA each load and type-check the program).")
    w("")
    w("| Cell | Oracle | Seconds | Peak RSS (MB) |")
    w("|---|---|---:|---:|")
    for cid in ids:
        m = cells[cid]["dead"]["oracle_meta"]
        w("| %s | %s | %s | %s |" % (cid, m.get("tool", "-"), m.get("seconds", "-"), m.get("peak_rss_mb", "-")))
    w("")
    sy = cells.get("go/syft/tests")
    if sy:
        m = sy["dead"]["oracle_meta"]
        w("`go/syft/tests` (the largest cell): %s s, %s MB peak, recorded in `cells.json` under `cells['go/syft/tests']['dead']['oracle_meta']`." % (
            m.get("seconds"), m.get("peak_rss_mb")))
        w("")
    # ---- value references (issue #452): dead claims that are missing references ----------------------------------
    import report_refs
    xref = report_refs.dead_xref_table(meta, heading="### Missing value references")
    if xref:
        w(xref)
        w("(The value references themselves are graded in \"Value references\" below.)")
        w("")
    # ---- spot-check ----------------------------------------------------------------------------------------
    sc = os.path.join(results_dir, "spotcheck-dead.json")
    if os.path.exists(sc):
        w(spotcheck_section(json.load(open(sc))))
        w("")
    return "\n".join(L)


def spotcheck_section(sc):
    from report import ci_pct
    L = []
    w = L.append
    w("### Dead-code spot-check")
    w("")
    w("Read by hand against the source (seed %s). Per language: %d wrong claims (FP, claims on declarations the oracle judged live) and %d missed "
      "declarations (FN, oracle-dead) drawn at random across the language's cells and both columns, plus up to %d items from every exclusion bucket. "
      "Verdicts: `lidx_error` (lidx is wrong: it claims a used symbol / misses a dead one), `oracle_artefact` (the oracle is the one that is wrong: the "
      "symbol is dead where it said live, or live where it said dead), `matching_artefact` (the key or the bucket was wrong); for buckets "
      "`correct_exclusion` or `misbucketed`." % (sc.get("seed"), sc.get("n"), sc.get("n"), sc.get("nb")))
    w("")
    w("| Language | Sample | lidx_error | oracle_artefact | matching_artefact | artefact rate [95% Wilson CI] |")
    w("|---|---|---:|---:|---:|---|")
    tot_a = tot_n = 0
    for lang, kinds in sc["languages"].items():
        lt = Counter()
        for kind in ("fp", "fn"):
            items = kinds[kind]
            c = Counter(i["verdict"] for i in items)
            lt.update(c)
            art = c["oracle_artefact"] + c["matching_artefact"]
            w("| %s | %s (n=%d) | %d | %d | %d | %d/%d = %.1f%% %s |" % (lang, kind.upper(), len(items), c["lidx_error"], c["oracle_artefact"], c["matching_artefact"],
                                                                      art, len(items), 100 * art / max(len(items), 1), ci_pct(art, len(items))))
        n = sum(lt.values())
        art = lt["oracle_artefact"] + lt["matching_artefact"]
        tot_a += art
        tot_n += n
        w("| %s | both (n=%d) | %d | %d | %d | **%d/%d = %.1f%%** %s |" % (lang, n, lt["lidx_error"], lt["oracle_artefact"], lt["matching_artefact"], art, n,
                                                                    100 * art / max(n, 1), ci_pct(art, n)))
    w("")
    bk = sc.get("buckets", {})
    if bk:
        w("| Language | Bucket | Bucket size | Sampled | misbucketed | rate [95% Wilson CI] |")
        w("|---|---|---:|---:|---:|---|")
        bm = bn = 0
        for lang, names in bk.items():
            for name, items in names.items():
                if not items:
                    continue
                mb = sum(1 for i in items if i["verdict"] == "misbucketed")
                bm += mb
                bn += len(items)
                w("| %s | %s | %d | %d | %d | %.1f%% %s |" % (lang, name, items[0]["bucket_size"], len(items), mb, 100 * mb / len(items), ci_pct(mb, len(items))))
        w("| all | all buckets | | %d | %d | **%.1f%%** %s |" % (bn, bm, 100 * bm / max(bn, 1), ci_pct(bm, bn)))
        w("")
        tot_a += bm
        tot_n += bn
    w("Pooled over every sampled item: %d/%d = %.1f%% %s." % (tot_a, tot_n, 100 * tot_a / max(tot_n, 1), ci_pct(tot_a, tot_n)))
    w("")
    w(sc.get("notes", ""))
    w("")
    for lang, kinds in sc["languages"].items():
        w("#### %s samples" % lang)
        w("")
        for kind in ("fp", "fn"):
            w("**%s**" % kind.upper())
            w("")
            for i in kinds[kind]:
                w("- `%s` %s (%s, %s): **%s**, %s" % (i["id"], i["decl"], i["cell"], i["column"], i["verdict"], i["note"]))
            w("")
        for name, items in bk.get(lang, {}).items():
            w("**bucket %s**" % name)
            w("")
            for i in items:
                w("- `%s` %s (%s): **%s**, %s" % (i["id"], i["decl"], i["cell"], i["verdict"], i["note"]))
            w("")
    return "\n".join(L)
