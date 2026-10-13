"""The "Type hierarchy" section of results/report.md (issue #449). Python 3 stdlib only."""
import json
import os

import hierarchy
import report

REL_TITLES = {
    "implements": "implements (type -> interface/trait)",
    "extends": "extends (class -> class, interface -> interface, trait -> supertrait)",
    "embeds": "embeds (Go struct embedding, struct -> embedded type)",
    "overrides": "overrides (implementing method -> interface/trait/base method)",
}
RELATION_NOTES = {
    "go": "Go `extends` is interface embedding (`Interface.EmbeddedType`), `embeds` is struct embedding (`Struct.Field(i).Embedded()`); "
          "lidx's EXTENDS rows are split the same way by the source's kind. `overrides` is graded on lidx's dispatch pairs "
          "(`lidx dispatch-pairs`).",
    "ts": "`overrides` is graded on lidx's dispatch pairs (`lidx dispatch-pairs`): the pairing lidx uses at query time, which for a "
          "base *class* needs an `override` modifier and otherwise is a name match through IMPLEMENTS / EXTENDS rows.",
    "rust": "`implements` is a type -> trait row, `overrides` a method-level IMPLEMENTS row (impl method -> trait method), `extends` a "
            "supertrait row. rust-analyzer's SCIP carries no implementation relationships, so the oracle reads the impl symbols and "
            "the reference occurrences on the impl header.",
}


def _p(x):
    return report.pct(x)


def _ci(c):
    return report.ci(c) if c and c[0] is not None else ""


def _cells(meta, lang):
    return [cid for cid in report.order(meta) if cid in meta["cells"] and meta["cells"][cid]["lang"] == lang and "hierarchy" in meta["cells"][cid]]


def section(meta, results_dir):
    cells = meta["cells"]
    if not any("hierarchy" in c for c in cells.values()):
        return ""
    L = []
    w = L.append
    w("## Type hierarchy")
    w("")
    w("`EXTENDS`, `IMPLEMENTS` and method-override pairs graded against the type checker: Go `go/types`, TypeScript's checker, Rust "
      "rust-analyzer's impl symbols. The compiler is the only source of the oracle pairs; nothing is derived from lidx. A lidx pair is "
      "**correct** when the compiler has it, **wrong** when both of its ends are declarations the oracle judged and the compiler does "
      "not have it, and **ungradable** otherwise (an end outside the analysed files, a generic declaration the oracle cannot judge). "
      "Precision = correct / (correct + wrong); recall = oracle pairs lidx has / all oracle pairs (a pair lidx has no symbol for stays "
      "in the denominator, counted as a capture gap). Intervals are 95% Wilson. Definitions, keying and exclusions: README, \"Type hierarchy\".")
    w("")
    for lang in ("go", "ts", "rust"):
        ids = _cells(meta, lang)
        if not ids:
            continue
        w("### %s" % report.LANG_NAMES[lang])
        w("")
        w(RELATION_NOTES[lang])
        w("")
        for rel in hierarchy.RELATIONS[lang]:
            rows = [(cid, cells[cid]["hierarchy"]["relations"][rel]) for cid in ids]
            if all(r["oracle_pairs"] == 0 and r["lidx_pairs"] == 0 for _, r in rows):
                w("**%s**: no pair in any %s cell (compiler: 0, lidx: 0)." % (REL_TITLES[rel], report.LANG_NAMES[lang]))
                w("")
                continue
            w("**%s**" % REL_TITLES[rel])
            w("")
            w("| Cell | P [95% CI] | graded (wrong) | R [95% CI] | oracle pairs (matched) | capture gap | ungradable lidx pairs |")
            w("|---|---|---:|---|---:|---:|---:|")
            for cid, r in rows:
                w("| %s | %s %s | %d (%d) | %s %s | %d (%d) | %d | %d |" % (
                    cid, _p(r["precision"]), _ci(r["precision_ci"]), r["graded"], r["fp"], _p(r["recall"]), _ci(r["recall_ci"]),
                    r["oracle_pairs"], r["matched"], r["capture_gap"], r["ungradable"]))
            w("")
        # external bases: existence only
        w("**External base** (the base is declared outside the repository: `impl Display for X`, `extends Error`, an embedded "
          "`sync.Mutex`). Judged for existence only: the oracle pair is *found* when lidx has an EXTENDS / IMPLEMENTS row from the "
          "same declaration whose target names the same base. No precision is computed.")
        w("")
        w("| Cell | oracle pairs | lidx has a row | existence recall [95% CI] |")
        w("|---|---:|---:|---|")
        for cid in ids:
            e = cells[cid]["hierarchy"]["external_base"]
            w("| %s | %d | %d | %s %s |" % (cid, e["oracle"], e["found"], _p(e["recall"]), _ci(e["recall_ci"])))
        w("")
        if lang == "ts":
            w("**Structural satisfaction (diagnostic only).** `checker.isTypeAssignableTo(class, interface)` for a class and an interface "
              "in the analysed files that the class does not declare (weak interfaces, with only optional members, are skipped). lidx has no "
              "structural rule for TypeScript; the table says how many of its IMPLEMENTS rows missing from the declared set are "
              "structurally true, and what precision would be if they counted as correct.")
            w("")
            w("| Cell | structural pairs (checker) | lidx IMPLEMENTS rows that are structural only | P declared [95% CI] | P incl. structural |")
            w("|---|---:|---:|---|---|")
            for cid in ids:
                hh = cells[cid]["hierarchy"]
                r = hh["relations"]["implements"]
                w("| %s | %d | %d | %s %s | %s |" % (cid, hh["structural_implements"]["oracle_pairs"], r.get("fp_structural_only", 0),
                                                     _p(r["precision"]), _ci(r["precision_ci"]), _p(r.get("precision_incl_structural"))))
            w("")
        if lang == "go":
            w("**Go `implements` recall and the dispatch-credited call recall.** The dispatch-credited, lidx-native call grading "
              "(\"Go interface edges\" above) covers an RTA edge to an implementation only when lidx's own graph links that implementation "
              "to the interface method, which is exactly what the IMPLEMENTS rows and the override pairs below provide. So a missing "
              "`implements` or `overrides` pair caps the dispatch-credited recall of every call made through that interface.")
            w("")
            w("| Cell | implements R | overrides R | oracle dispatch edges | dispatch edges matched | dispatch-credited call R |")
            w("|---|---|---|---:|---:|---|")
            for cid in ids:
                c = cells[cid]
                r = c["hierarchy"]["relations"]
                w("| %s | %s %s | %s %s | %d | %d | %s %s |" % (cid, _p(r["implements"]["recall"]), _ci(r["implements"]["recall_ci"]),
                                                              _p(r["overrides"]["recall"]), _ci(r["overrides"]["recall_ci"]),
                                                              c["calls"]["dispatch_edges"], c["calls"]["dispatch_matched"],
                                                              _p(c["calls"]["recall_comparable"]), _ci(c["calls"]["recall_comparable_ci"])))
            w("")
        w("Excluded, by bucket (counts are oracle-side, summed over the %s cells): %s" % (report.LANG_NAMES[lang], _excluded(cells, ids)))
        w("")
        w("Lists: `results/lists/<cell>.hierarchy_fn.json` (oracle pairs lidx lacks, plus unmatched external bases) and `<cell>.hierarchy_fp.json` (wrong lidx pairs), each row with a category.")
        w("")
    hs = os.path.join(results_dir, "spotcheck-hierarchy.json")
    if os.path.exists(hs):
        w(spotcheck_section(json.load(open(hs))))
    return "\n".join(L)


def _excluded(cells, ids):
    tot = {}
    for cid in ids:
        for k, v in cells[cid]["hierarchy"].get("excluded", {}).items():
            tot[k] = tot.get(k, 0) + v
    return ", ".join("%s %d" % kv for kv in sorted(tot.items())) or "none"


def spotcheck_section(sc):
    L = []
    w = L.append
    w("### Type hierarchy spot-check")
    w("")
    w("20 random wrong pairs (FP), 20 random missed pairs (FN) and 20 random **correct** pairs (TP, so that precision does not rest on the matcher alone) per language, read against the source (seed %s; cells %s). Verdicts: "
      "`lidx_error` (lidx is wrong or missed a real relation), `oracle_artefact` (the compiler pair is not the right judge), "
      "`matching_artefact` (our keying or bucketing mis-attributed the pair). Where a language has fewer than 20 wrong pairs, every one is read." % (
          sc.get("seed"), ", ".join(sc.get("cells", []))))
    w("")
    w("| Language | Sample | lidx_error | oracle_artefact | matching_artefact | artefact rate [95% Wilson CI] |")
    w("|---|---|---:|---:|---:|---|")
    tot_a = tot_n = 0
    for lang, kinds in sc["languages"].items():
        lt = {"lidx_error": 0, "oracle_artefact": 0, "matching_artefact": 0}
        for kind in ("fp", "fn", "tp"):
            if kind not in kinds:
                continue
            items = kinds[kind]
            c = {k: sum(1 for i in items if i["verdict"] == k) for k in lt}
            for k in lt:
                lt[k] += c[k]
            n = len(items)
            art = c["oracle_artefact"] + c["matching_artefact"]
            ncorrect = sum(1 for i in items if i["verdict"] == "correct")
            label = kind.upper() + (", %d confirmed in source" % ncorrect if kind == "tp" else "")
            w("| %s | %s (n=%d) | %d | %d | %d | %d/%d = %.1f%% %s |" % (lang, label, n, c["lidx_error"], c["oracle_artefact"], c["matching_artefact"],
                                                                      art, n, 100 * art / max(n, 1), report.ci_pct(art, n)))
        n = sum(len(v) for v in kinds.values())
        art = lt["oracle_artefact"] + lt["matching_artefact"]
        tot_a += art
        tot_n += n
        w("| %s | both (n=%d) | %d | %d | %d | **%d/%d = %.1f%%** %s |" % (lang, n, lt["lidx_error"], lt["oracle_artefact"], lt["matching_artefact"], art, n,
                                                                        100 * art / max(n, 1), report.ci_pct(art, n)))
    w("")
    bk = sc.get("buckets", {})
    if bk:
        w("**Exclusion buckets** (up to 10 random items each: capture gaps, ungradable lidx pairs by reason, and the external-base misses). "
          "`misbucketed` means the item should have been graded.")
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
                w("- `%s` %s %s -> %s: **%s**, %s" % (i["id"], i["relation"], i["sub"], i["base"], i["verdict"], i["note"]))
            w("")
    return "\n".join(L)
