"""Grade lidx's value references against the compiler oracles (issue #452). Python 3 stdlib only.

A *value reference* is a function, method or class used as a value rather than called: a callback argument, a handler-table
entry, a struct-field handler (`cobra.Command{RunE: run}`), a method value, `.map(f)`. Oracle side: the `refs` list each oracle
emits (go_oracle/refs.go, ts_oracle/refs.mjs, rust_oracle/scip_calls.py): pairs (enclosing declaration, referenced declaration)
keyed at the declaration's name, nothing derived from lidx. lidx side: its `REFERENCES` rows plus `USES` rows whose target is a
function, with both ends in-repo symbols, deduplicated to pairs.

Call wins, on both sides: a pair the compiler also calls is not a value reference of the oracle (the call is the stronger claim),
and a pair lidx also has a `CALLS` / `DISPATCH` edge for is not a reference of lidx's (it keeps only the call). A lidx reference
the compiler agrees is a reference but whose pair the compiler also calls is ungradable (`oracle_call_wins`); an oracle
reference lidx records as a call is a miss with the cross-category `reference_as_call` (and an FP `reference_as_call` of the call
benchmark); a call lidx records as a reference is an FP `call_as_reference` here and a miss `call_as_reference` of the call
benchmark.
"""
import re
from collections import Counter, defaultdict

from match import RES_ORDER, called_by_syntax, gap_reason, is_test_file, pick_res, ratio, wilson  # noqa: F401

REF_KINDS = ("REFERENCES", "USES")
CALL_KINDS = ("CALLS", "DISPATCH")
FN_KINDS = ("function", "method")


def _span(a, b):
    a = a or 0
    b = b if b and b >= a else a
    return range(a, min(b, a + 11) + 1)


def okey(d):
    return "%s:%d" % (d["file"], d["line"])


def _sites_gated(cfg_gate, sites):
    by_file = defaultdict(list)
    for st in sites:
        f, _, ln = st.rpartition(":")
        by_file[f].append(int(ln))
    return all(cfg_gate(f, lns) for f, lns in by_file.items())


def has_refs(oracle):
    return isinstance(oracle.get("refs"), list)


def judged_callers(oracle, lang):
    """Keys of the declarations whose references the oracle saw completely, or None when every caller in an analysed file counts."""
    if lang == "go":
        return {okey(c) for c in oracle.get("ref_callers", [])}
    if lang == "ts":
        return None if oracle.get("exhaustive_callers") else {okey(c) for c in oracle["callers"]}
    # Rust: function definitions, and the module of every analysed file (a reference outside any function body is keyed at line 0)
    return {okey(c) for c in oracle["callers"]} | {"%s:0" % f for f in oracle["files"]}


def grade_refs(oracle, syms, edges, lang, src, cfg_gate=None):
    """Returns (summary, fn_list, fp_list, capture_gap_list, ungradable_list, state). `state` feeds dead_xref().

    cfg_gate (Rust): callable (file, [lines]) -> True when every one of those lines lies inside an item behind a `#[cfg(...)]` that is definitely
    false in the configuration rust-analyzer analysed (rust_cfg.py). rust-analyzer resolves names in such code only partly (a local `features`
    declared there is not seen, so `features.push(..)` resolves to a function of that name): an oracle reference whose every site is gated is
    left out (`oracle_refs_cfg_gated`), and an unmatched lidx pair whose evidence is gated is ungradable (`cfg_gated_evidence`)."""
    sym_keys = {s["key"] for s in syms.values() if "key" in s}
    canon = {}
    for s in syms.values():
        if "key" in s:
            c = "%s|%s" % (s["file"], s["qualname"])
            if s["key"] not in canon or c < canon[s["key"]]:
                canon[s["key"]] = c
    cn = lambda k: canon.get(k, k)  # noqa: E731
    name_by_key = {s["key"]: s["name"].split(".")[-1] for s in syms.values() if "key" in s}
    ofiles = set(oracle["files"])
    jc = judged_callers(oracle, lang)
    jc_c = {cn(k) for k in jc} if jc is not None else None

    # ---- oracle calls: a pair the compiler calls is a call ------------------------------
    ocall = set()
    for e in oracle["edges"]:
        if lang == "go" and e.get("dispatch"):
            # RTA lists every function a function value may reach as a callee of the calls through it (`t.addEntryFunc(..)` calls each
            # function stored in the field): not a call from the declaration that stores it, so it does not make the reference a call.
            # Go's own evidence of a direct call is the `called` flag of the reference (callee positions of the same package).
            continue
        # a call made inside a nested function is lifted to the enclosing declaration lidx indexes, as the call grading does (`caller_chain`; its
        # trailing `file:0` is the module and would make every call in the file a call from the module)
        cks = [okey(e["caller"])] + list(e.get("caller_alts", [])) + [k for k in e.get("caller_chain", []) if not k.endswith(":0")]
        tks = [okey(e["callee"])] + list(e.get("callee_alts", []))
        for a in cks:
            for b in tks:
                ocall.add((cn(a), cn(b)))

    # ---- oracle value references --------------------------------------------------------
    orefs = []
    cfg_gated_refs = 0
    by_pair = defaultdict(list)
    site_idx = defaultdict(list)  # site -> [rec]
    fnlike_targets = set()  # keys of declarations the compiler says are function-valued variables (TS const f = () => ..)
    for i, r in enumerate(oracle["refs"]):
        if cfg_gate and r.get("sites") and _sites_gated(cfg_gate, r["sites"]):
            cfg_gated_refs += 1
            continue
        ck, tk = okey(r["caller"]), okey(r["callee"])
        cks = [ck] + list(r.get("caller_alts", []))
        lifted = False
        if not any(k in sym_keys for k in cks):
            for k in r.get("caller_chain", []):
                if k in sym_keys:
                    cks, lifted = [k], True
                    break
        # Rust: a path to a trait method (`Trait::method`) that the compiler resolved to one impl; the trait declaration is an acceptable target
        tks = [tk] + [a for a in r.get("callee_alts", []) if a != tk]
        rec = {"i": i, "caller": r["caller"], "callee": r["callee"], "ck": cks[0], "tk": tk, "tks": tks, "cks": cks, "sites": r.get("sites", []),
               "forms": r.get("forms", []), "ctxs": r.get("ctxs", []), "lifted": lifted, "matched": False,
               "called": bool(r.get("called")) or any((cn(a), cn(b)) in ocall for a in cks for b in tks)}
        orefs.append(rec)
        for a in cks:
            for b in tks:
                by_pair[(cn(a), cn(b))].append(rec)
        for s in rec["sites"]:
            site_idx[s].append(rec)
        if r.get("callee_kind") == "variable":
            fnlike_targets.add(tk)

    # ---- lidx: call pairs, reference pairs ---------------------------------------------
    lcall = set()
    lref = {}  # (caller key, target key) -> pair
    by_line = defaultdict(list)  # (caller key, line) -> [(status, detail, edge kind)] for every REFERENCES / USES row
    n_rows = Counter()
    unresolved_rows = 0
    for e in edges:
        k = e["kind"]
        if k not in REF_KINDS and k not in CALL_KINDS:
            continue
        s = syms.get(e["s"])
        if not s or "key" not in s:
            continue
        t = syms.get(e["t"]) if e["t"] else None
        if k in CALL_KINDS:
            if t is not None and "key" in t:
                lcall.add((cn(s["key"]), cn(t["key"])))
            continue
        # REFERENCES / USES
        if t is None or "key" not in t:
            if k == "REFERENCES":
                unresolved_rows += 1
                for ln in _span(e["line"], e["end_line"]):
                    by_line[(cn(s["key"]), ln)].append(("external" if t is not None else "unresolved", e["target_qualname"], k, e["resolution_kind"]))
            continue
        if k == "USES" and not (t["kind"] in FN_KINDS or t["key"] in fnlike_targets):
            continue  # a USES row whose target is not a function is a type or value use, not a function used as a value
        n_rows[k] += 1
        for ln in _span(e["line"], e["end_line"]):
            by_line[(cn(s["key"]), ln)].append(("resolved", t["key"], k, e["resolution_kind"]))
        pk = (cn(s["key"]), cn(t["key"]))
        p = lref.setdefault(pk, {"caller": s, "callee": t, "kinds": Counter(), "lines": set(), "span": set(), "edge_kinds": set(), "epaths": set()})
        if (s["key"], t["key"]) < (p["caller"]["key"], p["callee"]["key"]):
            p["caller"], p["callee"] = s, t
        p["kinds"][e["resolution_kind"] or "none"] += 1
        p["lines"].add(e["line"])
        p["span"].update(_span(e["line"], e["end_line"]))
        p["edge_kinds"].add(k)
        p["epaths"].add(e["epath"])

    # lidx rows that cannot be attributed: a USES row from a module symbol with no source line (what the Rust extractor records for a path used as a value)
    coarse = {(p["caller"]["file"], pk[1]) for pk, p in lref.items() if p["caller"]["kind"] == "module" and not any(x for x in p["lines"])}
    o_notcalled = {(cn(a), cn(b)) for r in orefs if not r["called"] for a in r["cks"] for b in r["tks"]}
    o_called_refs = {(cn(a), cn(b)) for r in orefs if r["called"] for a in r["cks"] for b in r["tks"]}

    # ---- precision --------------------------------------------------------------------
    tp = fp = 0
    called_dropped = 0
    by_kind = defaultdict(lambda: [0, 0])
    fp_list, ungr_list, tp_list = [], [], []
    ungradable = Counter()
    wins = []  # the pairs removed by the call-wins rule, both sides, for the hand spot-check
    for pk, p in lref.items():
        if pk in lcall:
            called_dropped += 1  # lidx keeps only the call
            wins.append({"side": "lidx", "caller": {k: p["caller"][k] for k in ("file", "line", "name", "qualname")},
                         "callee": {k: p["callee"][k] for k in ("file", "line", "name", "qualname")},
                         "evidence_lines": sorted(x for x in p["lines"] if x is not None)[:5], "evidence_file": sorted(p["epaths"])[0]})
            continue
        csym, tsym = p["caller"], p["callee"]
        res = pick_res(set(p["kinds"]))
        base = {"caller": {k: csym[k] for k in ("file", "line", "name", "qualname")}, "callee": {k: tsym[k] for k in ("file", "line", "name", "qualname")},
                "resolution_kind": res, "edge_kinds": sorted(p["edge_kinds"]), "evidence_lines": sorted(x for x in p["lines"] if x is not None)[:5],
                "evidence_file": sorted(p["epaths"])[0]}
        if pk in o_notcalled:
            for h in by_pair[pk]:
                h["matched"] = True
            tp += 1
            by_kind[res][0] += 1
            by_kind[res][1] += 1
            tp_list.append(dict(base, sites=by_pair[pk][0]["sites"][:3] if by_pair[pk] else []))
            continue
        reason = None
        if pk in o_called_refs:
            reason = "oracle_call_wins"  # the compiler agrees it is a reference, and also calls it: the pair is a call
        elif not any(x for x in p["lines"]):
            reason = "no_evidence_line"  # a USES row with no source line: an import of the function, not a use of it as a value
        elif csym["file"] not in ofiles:
            reason = "caller_file_not_covered"
        elif jc_c is not None and cn(csym["key"]) not in jc_c:
            reason = "caller_not_judged"
        elif tsym["file"] not in ofiles and not (is_test_file(lang, tsym["file"]) and not is_test_file(lang, csym["file"])):
            reason = "callee_file_not_covered"
        elif cfg_gate and len(p["epaths"]) == 1 and p["lines"] - {None, 0} and cfg_gate(sorted(p["epaths"])[0], sorted(x for x in p["lines"] if x)):
            reason = "cfg_gated_evidence"
        if reason:
            if reason == "oracle_call_wins":
                for h in by_pair[pk]:
                    h["matched"] = True  # lidx has the reference; the oracle pair is excluded from recall as a call, so it matters not
            ungradable[reason] += 1
            ungr_list.append(dict(base, reason=reason))
            continue
        fp += 1
        by_kind[res][0] += 1
        sites = ["%s:%d" % (csym["file"], ln) for ln in sorted(p["span"]) if ln]
        cat, detail = "no_reference_at_site", None
        if pk in ocall:
            cat = "call_as_reference"  # the compiler calls it (no value reference): lidx recorded a call as a reference
        else:
            name = tsym["name"].split(".")[-1]
            near = None
            for st in sites:
                for rr in site_idx.get(st, ()):
                    if cn(csym["key"]) in {cn(x) for x in rr["cks"]}:
                        near = rr
                        if rr["callee"]["name"].split(".")[-1] == name:
                            break
                if near and near["callee"]["name"].split(".")[-1] == name:
                    break
            if near is not None:
                same = near["callee"]["name"].split(".")[-1] == name
                cat = "same_name_elsewhere" if same else "wrong_target"
                detail = {"oracle_callee": near["tk"], "same_name": same}
            else:
                # the same name referenced anywhere in this caller, bound to another declaration
                anyname = [rr for rr in orefs if cn(csym["key"]) in {cn(x) for x in rr["cks"]} and rr["callee"]["name"].split(".")[-1] == name and not rr["called"]]
                if anyname:
                    cat = "same_name_elsewhere"
                    detail = {"oracle_callee": anyname[0]["tk"], "same_name": True, "site_agnostic": True}
                elif any(st in site_idx for st in sites):
                    cat = "caller_attribution"
                    detail = {"oracle_callers": sorted({rr["ck"] for st in sites for rr in site_idx.get(st, ())})[:3]}
                elif called_by_syntax(src, lang, sorted(p["epaths"])[0], p["span"], tsym["name"]):
                    cat = "call_syntax_at_site"
        fp_list.append(dict(base, category=cat, detail=detail, callee_kind=tsym["kind"], caller_kind=csym["kind"]))

    # ---- recall -----------------------------------------------------------------------
    capture_gap, fn_list = [], []
    judged = matched = lifted = 0
    removed_called = 0
    shapes = Counter()
    for r in orefs:
        if r["called"]:
            removed_called += 1  # the compiler also calls this pair: it is a call, not a value reference
            wins.append({"side": "oracle", "caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3]})
            continue
        caller_indexed = any(k in sym_keys for k in r["cks"])
        if not caller_indexed:
            capture_gap.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3], "reason": gap_reason(src, lang, r["caller"]) if r["caller"]["line"] else "no_module_symbol"})
            continue
        if not any(k in sym_keys for k in r["tks"]):
            capture_gap.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3], "reason": "callee_not_indexed"})
            continue
        lifted += r["lifted"]
        judged += 1
        if r["matched"] or any((cn(a), cn(b)) in lref and (cn(a), cn(b)) not in lcall for a in r["cks"] for b in r["tks"]):
            matched += 1
            continue
        cat, detail = "no_edge", None
        if any((cn(a), cn(b)) in lcall for a in r["cks"] for b in r["tks"]):
            cat = "reference_as_call"  # lidx kept a CALLS edge where the compiler sees only a reference
        elif any((r["caller"]["file"], cn(b)) in coarse for b in r["tks"]):
            cat = "coarse_module_level"  # lidx has the reference, attributed to the file's module with no source line (Rust path values): not the pair
        else:
            best = None
            for st in r["sites"]:
                ln = int(st.rpartition(":")[2])
                for ck in r["cks"]:
                    for status, d, ek, res in by_line.get((cn(ck), ln), []):
                        if status == "resolved" and cn(d) not in {cn(b) for b in r["tks"]}:
                            cand = ("wrong_target", d, res)
                        elif status == "resolved":
                            continue
                        else:
                            cand = ("unresolved" if status == "unresolved" else "marked_external", d, res)
                        prio = {"wrong_target": 0, "marked_external": 1, "unresolved": 2}[cand[0]]
                        if best is None or prio < best[0]:
                            best = (prio,) + cand
            if best:
                cat = best[1]
                detail = {"lidx_target": best[2], "resolution_kind": best[3]}
                if cat == "wrong_target":
                    tn = r["callee"]["name"].split(".")[-1]
                    detail["same_name"] = isinstance(best[2], str) and name_by_key.get(best[2]) == tn
            else:
                shapes[("module_level" if r["caller"]["line"] == 0 else "in_function", ",".join(r["forms"]), ",".join(r["ctxs"]))] += 1
        fn_list.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3], "category": cat, "detail": detail,
                        "forms": r["forms"], "ctxs": r["ctxs"], "module_level": r["caller"]["line"] == 0})
    graded = tp + fp
    n_oracle = len(orefs)
    summary = {
        "oracle_refs": n_oracle,
        "oracle_refs_cfg_gated": cfg_gated_refs,
        "oracle_refs_called_removed": removed_called,
        "oracle_refs_graded": n_oracle - removed_called,
        "oracle_refs_caller_and_callee_indexed": judged,
        "capture_gap_refs": len(capture_gap),
        "capture_gap_reasons": dict(Counter(g["reason"] for g in capture_gap)),
        "lifted_caller_refs": lifted,
        "matched_oracle_refs": matched,
        "lidx_pairs_total": len(lref),
        "lidx_pairs_with_call_dropped": called_dropped,
        "lidx_rows": dict(n_rows),
        "lidx_unresolved_reference_rows": unresolved_rows,
        "graded": graded, "tp": tp, "fp": fp,
        "precision": ratio(tp, graded), "precision_ci": wilson(tp, graded),
        "recall": ratio(matched, judged), "recall_ci": wilson(matched, judged),
        "recall_incl_capture_gap": ratio(matched, judged + len(capture_gap)),
        "recall_incl_capture_gap_ci": wilson(matched, judged + len(capture_gap)),
        "ungradable": dict(ungradable),
        "by_resolution_kind": {k: {"graded": v[0], "tp": v[1], "precision": ratio(v[1], v[0]), "ci": wilson(v[1], v[0])}
                               for k, v in sorted(by_kind.items(), key=lambda kv: -kv[1][0])},
        "fp_categories": dict(Counter(f["category"] for f in fp_list).most_common()),
        "fn_categories": dict(Counter(f["category"] for f in fn_list).most_common()),
        "same_name_elsewhere": sum(1 for f in fp_list if f["category"] == "same_name_elsewhere"),
        "fn_no_edge_shapes": [[list(k), v] for k, v in shapes.most_common(12)],
        "fn_by_form": dict(Counter(",".join(f["forms"]) for f in fn_list if f["category"] == "no_edge").most_common()),
        "fn_by_ctx": dict(Counter(",".join(f["ctxs"]) for f in fn_list if f["category"] == "no_edge").most_common()),
        "fn_module_level": sum(1 for f in fn_list if f["module_level"]),
        "oracle_forms": dict(Counter(",".join(r["forms"]) for r in orefs if not r["called"]).most_common()),
    }
    state = {"call_wins": wins, "tp": tp_list, "cn": cn, "target_keys": {cn(b): r for r in orefs if not r["called"] for b in r["tks"]}, "lidx_targets": {pk[1] for pk in lref if pk not in lcall},
             "matched_targets": {cn(b) for r in orefs if not r["called"] and r["matched"] for b in r["tks"]}}
    return summary, fn_list, fp_list, capture_gap, ungr_list, state


def dead_xref(state, items):
    """Cross-reference with the dead-code benchmark: of the declarations lidx claims dead although the oracle judged them live (`items`,
    the non-policy rows of a dead-code FP list), how many are the target of a value reference the compiler sees, and how many of those
    does lidx hold no reference pair for at all (the dead-code claim is then at least partly a missing reference)."""
    cn = state["cn"]
    withref = none = 0
    for it in items:
        k = cn(it["key"])
        if k in state["target_keys"]:
            withref += 1
            none += k not in state["lidx_targets"]
    return {"fp_items": len(items), "with_value_reference": withref, "lidx_has_no_reference_to_it": none}
