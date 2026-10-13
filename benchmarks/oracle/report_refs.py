"""The "Value references" section of results/report.md (issue #452) and its cross-reference from the dead-code section. Python 3 stdlib only."""
import json
import os

import report

NOTES = {
    "go": "Oracle: every identifier in `types.Info.Uses` whose object is a `*types.Func` and which is not the `Fun` of a call (parentheses, selectors and "
          "generic instantiation unwrapped); method values `x.M` and method expressions `T.M` included; a use outside any function declaration (a "
          "package-level table) is keyed at line 0 of its file. Callers judged: every function declaration of the analysed files and the file itself.",
    "ts": "Oracle: identifiers and property accesses whose checker symbol is a function, method, class or const/property with a function "
          "initialiser, outside callee position (call, `new`, tagged template, decorator, JSX tag, `f.call` / `f.apply`), declaration names, type positions "
          "(`typeof f`, annotations, `extends`/`implements`), import and export specifiers, the object of a static access (`Foo.create`) and abstract targets. "
          "Callers judged: every declaration of the analysed files.",
    "rust": "Oracle: rust-analyzer SCIP references to a function symbol not followed by `(` (a turbofish `::<T>` is skipped first), minus occurrences with the "
            "`Import` role, occurrences inside a `use` item, implicit calls (an overloaded operator, a deref, an index: the text is not the function's name), a call that "
            "the older reading took for a reference on a line with non-ASCII text, and references in `#[cfg]`-disabled code. A reference outside any function body (a `static` table) "
            "is keyed at line 0 of its file. `Trait::method` resolved to one impl accepts the trait's declaration. Callers judged: function definitions and the module of every analysed file.",
}


def _cells(meta, lang):
    return [cid for cid in report.order(meta) if cid in meta["cells"] and meta["cells"][cid]["lang"] == lang and "refs" in meta["cells"][cid]]


def _p(x):
    return report.pct(x)


def _ci(c):
    return report.ci(c) if c and c[0] is not None else ""


def section(meta, results_dir):
    cells = meta["cells"]
    if not any("refs" in c for c in cells.values()):
        return ""
    L = []
    w = L.append
    w("## Value references")
    w("")
    w("A *value reference* is a function, method or class used as a value rather than called: a callback argument, a handler-table entry, a struct-field handler "
      "(`cobra.Command{RunE: run}`), a method value, `.map(f)`. It is what keeps a handler layer out of `dead_symbols` and what `explain_symbol` shows under "
      "`other_edges`. The compiler is the only source of the oracle pairs. The lidx side is its `REFERENCES` rows plus its `USES` rows whose target is a function, "
      "both ends in-repo symbols, deduplicated to (referencing declaration, referenced declaration) pairs. Precision = lidx pairs the compiler confirms / lidx pairs "
      "it can judge; recall = oracle pairs lidx has / oracle pairs whose ends are both lidx symbols (pairs with no lidx symbol at an end are listed as capture gaps). "
      "**A pair that is also called is a call, on both sides**: the oracle pair is dropped when the compiler also sees a call from the same declaration to the same "
      "function, and the lidx pair is dropped when lidx has a `CALLS` / `DISPATCH` edge for it. Intervals are 95% Wilson. Definitions, keying and exclusions: README, "
      "\"Value references\". The \"Alongside REFERENCES and USES\" table of the call tables says what adding these edges does to a *call* grading; this section grades "
      "the edges themselves.")
    w("")
    for lang in ("go", "ts", "rust"):
        ids = _cells(meta, lang)
        if not ids:
            continue
        w("### %s" % report.LANG_NAMES[lang])
        w("")
        w(NOTES[lang])
        w("")
        w("| Cell | P [95% CI] | graded (wrong) | R [95% CI] | oracle refs (matched) | removed: also called, oracle / lidx | capture gap | ungradable lidx pairs |")
        w("|---|---|---:|---|---:|---|---:|---|")
        for cid in ids:
            r = cells[cid]["refs"]
            ung = ", ".join("%s %d" % kv for kv in sorted(r["ungradable"].items())) or "0"
            w("| %s | %s %s | %d (%d) | %s %s | %d (%d) | %d / %d | %d | %s |" % (
                cid, _p(r["precision"]), _ci(r["precision_ci"]), r["graded"], r["fp"], _p(r["recall"]), _ci(r["recall_ci"]),
                r["oracle_refs_caller_and_callee_indexed"], r["matched_oracle_refs"], r["oracle_refs_called_removed"], r["lidx_pairs_with_call_dropped"],
                r["capture_gap_refs"], ung))
        w("")
        w("**Precision by lidx resolution kind** (the resolution kind of the pair's rows; `same_name_elsewhere` is a wrong pair whose target has the name of the reference at "
          "the site but is another declaration, the failure a name-tier guess produces)")
        w("")
        w("| Cell | resolution_kind | graded | correct | P | 95% CI |")
        w("|---|---|---:|---:|---:|---|")
        for cid in ids:
            for k, v in cells[cid]["refs"]["by_resolution_kind"].items():
                w("| %s | %s | %d | %d | %s | %s |" % (cid, k, v["graded"], v["tp"], _p(v["precision"]), _ci(v["ci"])))
        w("")
        w("**Wrong and missed pairs, by category**")
        w("")
        w("| Cell | wrong pairs by category | `same_name_elsewhere` | missed pairs by category |")
        w("|---|---|---:|---|")
        for cid in ids:
            r = cells[cid]["refs"]
            w("| %s | %s | %d | %s |" % (cid, ", ".join("%s %d" % kv for kv in r["fp_categories"].items()) or "none", r["same_name_elsewhere"],
                                           ", ".join("%s %d" % kv for kv in r["fn_categories"].items()) or "none"))
        w("")
        w("**Missed pairs with no lidx edge at the site (`no_edge`), by the shape of the reference** (`module_level` = outside any function; form / context from the compiler's syntax)")
        w("")
        w("| Cell | shapes (count) |")
        w("|---|---|")
        for cid in ids:
            sh = cells[cid]["refs"]["fn_no_edge_shapes"]
            w("| %s | %s |" % (cid, "; ".join("%s %s %s: %d" % (k[0], k[1], k[2], n) for k, n in sh[:6]) or "none"))
        w("")
    w("Cross-categories with the call tables: an oracle reference lidx records as a call is a miss `reference_as_call` here and a wrong edge `reference_as_call` "
      "in the call table (the same edge, labelled once on each side); a call lidx records as a reference is a wrong pair `call_as_reference` here and a missed call "
      "`call_as_reference` in the call table.")
    w("")
    w("Lists: `results/lists/<cell>.refs_fn.json` (oracle pairs lidx lacks), `<cell>.refs_fp.json` (wrong lidx pairs), `<cell>.refs_capture_gap.json`, "
      "`<cell>.refs_ungradable.json`, `<cell>.refs_call_wins.json` (the pairs the call-wins rule removed, either side), `<cell>.refs_tp.json`; each row carries its category.")
    w("")
    w(dead_xref_table(meta, heading="### Dead-code claims that are missing references"))
    sc = os.path.join(results_dir, "spotcheck-refs.json")
    if os.path.exists(sc):
        w(spotcheck_section(json.load(open(sc))))
    return "\n".join(L)


def dead_xref_table(meta, heading):
    """For each cell with both benchmarks: of the declarations lidx's `dead_symbols` claims dead although the oracle judged them live, how many are the target of a
    value reference the compiler sees, and for how many of those lidx holds no reference pair at all."""
    cells = meta["cells"]
    rows = [(cid, cells[cid]["refs"].get("dead_code_xref")) for cid in report.order(meta) if cid in cells and "refs" in cells[cid]]
    rows = [(cid, x) for cid, x in rows if x]
    if not rows:
        return ""
    L = []
    w = L.append
    w(heading)
    w("")
    w("Of the declarations lidx claims dead although the oracle judged them live (rule-based policy claims on public API and test roots left out), how many are the "
      "target of a value reference the compiler sees, and for how many of those lidx holds no reference pair at all (so the dead claim is a missing reference). "
      "A declaration can be live for a reason other than a value reference (a call lidx did not resolve), so the right-hand figure bounds, it does not equal, "
      "the dead-code false positives that are missing references.")
    w("")
    w("| Cell | column | wrong dead claims | target of a value reference | lidx has no reference pair into it |")
    w("|---|---|---:|---:|---:|")
    for cid, x in rows:
        for col, name in (("unref", "unreferenced"), ("reach", "unreachable")):
            if col in x:
                v = x[col]
                w("| %s | %s | %d | %d | %d |" % (cid, name, v["fp_items"], v["with_value_reference"], v["lidx_has_no_reference_to_it"]))
    w("")
    return "\n".join(L)


def spotcheck_section(sc):
    L = []
    w = L.append
    w("### Value reference spot-check")
    w("")
    w("20 random wrong pairs (FP), 20 random missed pairs (FN) and 20 random **correct** pairs (TP, so that precision does not rest on the matcher alone) per language, read against the source (seed %s; pairs pooled over the cells of the language, `_all_features` twins left out, a pair counted once), plus up to 10 random items from each exclusion bucket. "
      "Verdicts: `lidx_error` (lidx is wrong or missed a real reference), `oracle_artefact` (the compiler pair is not the right judge), `matching_artefact` (our keying, "
      "category or bucketing mis-attributed the pair), `misbucketed` (an excluded item that should have been graded)." % sc.get("seed"))
    w("")
    w("| Language | Sample | lidx_error | oracle_artefact | matching_artefact | artefact rate [95% Wilson CI] |")
    w("|---|---|---:|---:|---:|---|")
    tot_a = tot_n = 0
    for lang, kinds in sc["languages"].items():
        lt = {"lidx_error": 0, "oracle_artefact": 0, "matching_artefact": 0}
        for kind in ("fp", "fn", "tp"):
            items = kinds.get(kind, [])
            c = {k: sum(1 for i in items if i["verdict"] == k) for k in lt}
            for k in lt:
                lt[k] += c[k]
            n = len(items)
            art = c["oracle_artefact"] + c["matching_artefact"]
            label = kind.upper() + (", %d confirmed in source" % sum(1 for i in items if i["verdict"] == "correct") if kind == "tp" else "")
            pop = kinds.get(kind + "_population")
            w("| %s | %s (n=%d%s) | %d | %d | %d | %d/%d = %.1f%% %s |" % (lang, label, n, " of %d" % pop if pop is not None else "", c["lidx_error"], c["oracle_artefact"],
                                                                         c["matching_artefact"], art, n, 100 * art / max(n, 1), report.ci_pct(art, n)))
        n = sum(len(v) for v in kinds.values() if isinstance(v, list))
        art = lt["oracle_artefact"] + lt["matching_artefact"]
        tot_a += art
        tot_n += n
        w("| %s | both (n=%d) | %d | %d | %d | **%d/%d = %.1f%%** %s |" % (lang, n, lt["lidx_error"], lt["oracle_artefact"], lt["matching_artefact"], art, n,
                                                                        100 * art / max(n, 1), report.ci_pct(art, n)))
    w("")
    bk = sc.get("buckets", {})
    if bk:
        w("**Exclusion buckets.** `misbucketed` means the item should have been graded.")
        w("")
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
                w("| %s | %s | %d | %d | %d | %.1f%% %s |" % (lang, name, items[0]["bucket_size"], len(items), mb, 100 * mb / len(items), report.ci_pct(mb, len(items))))
        w("| all | all buckets | | %d | %d | **%.1f%%** %s |" % (bn, bm, 100 * bm / max(bn, 1), report.ci_pct(bm, bn)))
        w("")
        tot_a += bm
        tot_n += bn
    w("Pooled over every sampled item: %d/%d = %.1f%% %s." % (tot_a, tot_n, 100 * tot_a / max(tot_n, 1), report.ci_pct(tot_a, tot_n)))
    w("")
    w(sc.get("notes", ""))
    w("")
    for lang, kinds in sc["languages"].items():
        w("#### %s samples" % lang)
        w("")
        for kind in ("fp", "fn", "tp"):
            if kind not in kinds:
                continue
            w("**%s**" % kind.upper())
            w("")
            for i in kinds[kind]:
                w("- `%s` %s: **%s**, %s" % (i["id"], i["decl"], i["verdict"], i["note"]))
            w("")
    return "\n".join(L)
