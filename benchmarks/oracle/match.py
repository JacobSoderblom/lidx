"""Grade lidx call edges against a compiler oracle. Python 3 stdlib only.

An edge matches when caller and callee land on the same declaration, identified
by (file, line of the declaration's name identifier). The oracle emits those
keys directly; for lidx symbols the name line is recovered from the source so
that decorators, attributes and doc comments above a declaration do not matter.
"""
import json
import math
import os
import random
import re
import sqlite3
from collections import Counter, defaultdict

RES_ORDER = ["exact", "receiver_type", "import", "two_segment", "inherited", "bare_name", "external"]


def wilson(k, n, z=1.96):
    if n == 0:
        return [None, None]
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return [round(max(0.0, c - h), 4), round(min(1.0, c + h), 4)]


def ratio(k, n):
    return round(k / n, 4) if n else None


class Sources:
    def __init__(self, repo):
        self.repo = repo
        self.cache = {}

    def lines(self, path):
        if path not in self.cache:
            try:
                with open(os.path.join(self.repo, path), encoding="utf-8", errors="replace") as f:
                    self.cache[path] = f.read().split("\n")
            except OSError:
                self.cache[path] = []
        return self.cache[path]


def name_line(src, lang, kind, name, start, end, path):
    """1-based line of the declaration's name identifier."""
    if kind == "module":
        return 0  # a module / file-level symbol has no name token; it is keyed at line 0 in every language
    lines = src.lines(path)
    if not lines:
        return start
    bare = re.escape(name.split(".")[-1]) if name else ""
    if not bare:
        return start
    if lang == "go" and kind in ("function", "method"):
        pat = re.compile(r"^\s*func\s*(\([^)]*\)\s*)?" + bare + r"\b")
    elif lang == "rust" and kind in ("function", "method"):
        pat = re.compile(r"\bfn\s+(r#)?" + bare + r"\b")
    else:
        pat = re.compile(r"(?<![\w$])" + bare + r"(?![\w$])")
    hi = min(len(lines), max(end, start) , start + 60)
    for ln in range(max(start, 1), hi + 1):
        text = lines[ln - 1]
        st = text.lstrip()
        if st.startswith(("//", "#[", "#![", "@", "*", "/*")) and lang != "go":
            continue
        if lang == "go" and st.startswith("//"):
            continue
        if pat.search(text):
            return ln
    return start


def load_lidx(db_path, repo, lang):
    src = Sources(repo)
    con = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    con.row_factory = sqlite3.Row
    syms = {}
    for r in con.execute(
        "select s.id, s.kind, s.name, s.qualname, s.start_line, s.end_line, f.path "
        "from symbols s join files f on f.id = s.file_id where f.deleted_version is null"
    ):
        if r["kind"] == "external":
            syms[r["id"]] = {"external": True, "qualname": r["qualname"], "kind": "external"}
            continue
        nl = name_line(src, lang, r["kind"], r["name"], r["start_line"], r["end_line"], r["path"])
        syms[r["id"]] = {"file": r["path"], "line": nl, "name": r["name"], "qualname": r["qualname"], "kind": r["kind"],
                         "key": "%s:%d" % (r["path"], nl)}
    edges = []
    for r in con.execute(
        "select e.kind, e.source_symbol_id as s, e.target_symbol_id as t, e.target_qualname, e.resolution_kind, "
        "e.evidence_start_line as line, e.evidence_end_line as end_line, f.path as epath from edges e join files f on f.id = e.file_id "
        "where e.kind in ('CALLS','REFERENCES','USES') and f.deleted_version is null"
    ):
        edges.append(dict(r))
    con.close()
    return syms, edges


def is_test_file(lang, path):
    if lang == "go":
        return path.endswith("_test.go")
    if lang == "ts":
        return bool(re.search(r"(^|/)(__tests__|tests?|test-utils|e2e|runtime-tests)/", path) or re.search(r"\.(test|spec|bench)\.[cm]?[tj]sx?$", path))
    parts = path.split("/")
    name = parts[-1]
    return "tests" in parts[:-1] or name in ("tests.rs", "test.rs") or name.endswith("_tests.rs")


def gap_reason(src, lang, caller):
    """Why lidx has no symbol for an oracle caller: macro-generated functions have no `fn name` at the reported line."""
    lines = src.lines(caller["file"])
    ln = caller["line"]
    text = lines[ln - 1] if 0 < ln <= len(lines) else ""
    name = re.escape(caller["name"])
    if lang == "rust" and not re.search(r"\bfn\s+(r#)?" + name + r"\b", text):
        return "macro_generated"
    if lang == "go" and not re.search(r"\bfunc\b", text):
        return "generated_or_unusual_decl"
    return "not_indexed"


def called_by_syntax(src, lang, path, lns, name):
    """True when the callee's name appears at one of the evidence lines in call position."""
    bare = re.escape(name.split(".")[-1]) if name else ""
    if not bare:
        return True
    call = re.compile(r"(?<![\w$])" + bare + r"\s*(::<[^()]*>)?\s*(<[^()]*>)?\s*(\?\.|!)?\s*[(`]")
    new = re.compile(r"\bnew\s+[\w$.]*" + bare + r"\b" if lang != "ts" else r"(\bnew\s+[\w$.]*|<[\w$.]*)" + bare + r"(?![\w$])")
    lines = src.lines(path)
    for ln in lns:
        if 0 < ln <= len(lines) and (call.search(lines[ln - 1]) or new.search(lines[ln - 1])):
            return True
    return False


def pick_res(kinds):
    for k in RES_ORDER:
        if k in kinds:
            return k
    return sorted(kinds)[0] if kinds else "none"


def repowise_ts_scope(oracle):
    """The TypeScript oracle as repowise's `tsc` oracle emits it (graph/experiments/g4-oracle-anchored/oracle-ts/oracle.mjs).

    repowise (1) records no call made outside a function (module level): the edge is not emitted and the caller position is
    kept out of the judged set; (2) withholds a call resolved to an abstract target (an interface or type-literal member, an
    abstract method, a function-typed field: anything without a body): the edge is not emitted and the enclosing function
    stops being judged. Our oracle marks (2) as `dispatch` and (1) as a caller on line 0, so both are recognisable.

    Returns (oracle without those edges, set of caller keys that stay out of the judged set). Everything else, including
    capture gaps (nested functions lidx has no symbol for), stays in the recall denominator as it does for repowise.
    """
    kept, withheld = [], set()
    for e in oracle["edges"]:
        module_level = e["caller"]["line"] == 0
        if module_level or e.get("dispatch"):
            # the enclosing named functions stop being judged; the file's `<module>` only when the call is at module level
            withheld.update([okey_of(e["caller"])] + list(e.get("caller_alts", []))
                            + [k for k in e.get("caller_chain", []) if not k.endswith(":0")])
            continue
        kept.append(e)
    out = dict(oracle)
    out["edges"] = kept
    return out, withheld


def okey_of(d):
    return "%s:%d" % (d["file"], d["line"])


def grade(oracle, syms, edges, lang, src, edge_kinds=("CALLS",), withheld_callers=None):
    """Compare lidx edges of the given kinds against the oracle. Returns (summary, fn_list, fp_list, extra).

    withheld_callers: caller keys the oracle declines to judge (see repowise_ts_scope); an unmatched lidx edge from one is
    ungradable (`caller_withheld`), a matched one still counts."""
    sym_keys = {s["key"] for s in syms.values() if "key" in s}
    # Declaration merging (an interface and a const of the same name, overloads) is one declaration: compare
    # declarations by (file, qualname) whenever lidx has a symbol at the position.
    canon = {}
    for s in syms.values():
        if "key" in s:
            c = "%s|%s" % (s["file"], s["qualname"])
            if s["key"] not in canon or c < canon[s["key"]]:
                canon[s["key"]] = c
    cn = lambda k: canon.get(k, k)
    # ---- oracle side -------------------------------------------------
    ofiles = set(oracle["files"])
    ocallers = {"%s:%d" % (c["file"], c["line"]) for c in oracle["callers"]}
    ocallers_c = {cn(k) for k in ocallers}
    unresolved = set((oracle.get("unresolved_sites") or []))
    # An unresolved call only withholds a lidx edge whose callee carries the unresolved call's name: another call on
    # the same line is still graded. Oracles without call names fall back to excluding the whole line.
    unresolved_names = defaultdict(set)
    named = oracle.get("unresolved_calls")
    for u in named or []:
        unresolved_names[u["site"]].add(u["name"])

    def unresolved_hit(path, lines, callee_name):
        for ln in lines:
            site = "%s:%d" % (path, ln)
            if named is None:
                if site in unresolved:
                    return True
            elif callee_name in unresolved_names.get(site, ()):
                return True
        return False
    ext_sites = set((oracle.get("ext_sites") or []))
    closure_sites = set((oracle.get("closure_sites") or []))

    def okey(d):
        return "%s:%d" % (d["file"], d["line"])

    oedges = []
    by_pair = defaultdict(list)
    site_idx = defaultdict(list)  # site -> [(caller_key, callee_key)]
    for i, e in enumerate(oracle["edges"]):
        ck, tk = okey(e["caller"]), okey(e["callee"])
        cks = [ck] + list(e.get("caller_alts", []))
        lifted = False
        if not any(k in sym_keys for k in cks):
            # the caller is not a symbol lidx indexes (object-literal method, nested callback, ...):
            # lift to the nearest enclosing declaration lidx does index (the module at the latest)
            for k in e.get("caller_chain", []):
                if k in sym_keys:
                    cks, lifted = [k], True
                    break
        tks = [tk] + list(e.get("callee_alts", []))
        rec = {"i": i, "caller": e["caller"], "callee": e["callee"], "ck": cks[0], "tk": tk, "cks": cks, "tks": tks,
               "sites": e.get("sites", []), "matched": False, "lifted": lifted,
               "dispatch": bool(e.get("dispatch")), "closure": bool(e.get("closure"))}
        oedges.append(rec)
        for a in cks:
            for b in tks:
                by_pair[(cn(a), cn(b))].append(rec)
        for s in rec["sites"]:
            site_idx[s].append(rec)
    for r in oedges:
        r["fanout"] = max((len(site_idx[s]) for s in r["sites"]), default=1)
    # a caller that carries an oracle edge (including an alt or lifted caller) is a judgeable caller, so lidx edges
    # from e.g. a `const X = core.$constructor(...)` container are graded, not dropped as "not judged"
    ocallers_c |= {cn(k) for r in oedges for k in r["cks"]}
    withheld_c = {cn(k) for k in (withheld_callers or ())}

    # ---- lidx side ---------------------------------------------------
    def span(a, b):
        a = a or 0
        b = b if b and b >= a else a
        return range(a, min(b, a + 11) + 1)

    pairs = {}
    lidx_by_caller_line = defaultdict(list)  # (caller_key, line) -> [(status, detail)]
    lidx_by_file_line = defaultdict(list)  # (file, line) -> [(caller_key, callee_key)] for resolved edges
    uses_by_line = defaultdict(list)  # edges of kinds not graded here, for FN diagnostics
    for e in edges:
        if e["kind"] not in edge_kinds:
            s0 = syms.get(e["s"])
            t0 = syms.get(e["t"]) if e["t"] else None
            if s0 and "key" in s0 and e["kind"] in ("USES", "REFERENCES"):
                for ln in span(e["line"], e["end_line"]):
                    uses_by_line[(cn(s0["key"]), ln)].append((e["kind"], (t0 or {}).get("key") or (t0 or {}).get("qualname") or e["target_qualname"]))
            continue
        s = syms.get(e["s"])
        if not s or "key" not in s:
            continue
        ck = s["key"]
        cck = cn(ck)
        t = syms.get(e["t"]) if e["t"] else None
        res = e["resolution_kind"] or "none"
        line = e["line"]
        if t is None:
            for ln in span(line, e["end_line"]):
                lidx_by_caller_line[(cck, ln)].append(("unresolved", e["target_qualname"], res))
            continue
        if t.get("external"):
            for ln in span(line, e["end_line"]):
                lidx_by_caller_line[(cck, ln)].append(("external", t["qualname"], res))
            continue
        for ln in span(line, e["end_line"]):
            lidx_by_caller_line[(cck, ln)].append(("resolved", t["key"], res))
            lidx_by_file_line[(s["file"], ln)].append((cck, cn(t["key"])))
        p = pairs.setdefault((cck, cn(t["key"])), {"caller": s, "callee": t, "kinds": Counter(), "lines": set(), "edge_kinds": set(),
                                              "epaths": set()})
        if (s["key"], t["key"]) < (p["caller"]["key"], p["callee"]["key"]):
            p["caller"], p["callee"] = s, t  # merged declarations: keep the smallest key so output is stable across runs
        p["kinds"][res] += 1
        p["lines"].add(line)
        p.setdefault("span", set()).update(span(line, e["end_line"]))
        p["edge_kinds"].add(e["kind"])
        p["epaths"].add(e["epath"])

    # ---- precision ---------------------------------------------------
    ungradable = Counter()
    ungr_list = []
    tp = fp = 0
    by_kind = defaultdict(lambda: [0, 0])  # kind -> [graded, tp]
    fp_list = []
    for (ck, tk), p in pairs.items():
        csym, tsym = p["caller"], p["callee"]
        res = pick_res(set(p["kinds"]))
        hits = by_pair.get((cn(ck), cn(tk)))
        if hits:
            for h in hits:
                h["matched"] = True
            tp += 1
            by_kind[res][0] += 1
            by_kind[res][1] += 1
            continue
        # judgeable?
        reason = None
        if csym["file"] not in ofiles:
            reason = "caller_file_not_covered"
        elif not oracle.get("exhaustive_callers") and cn(csym["key"]) not in ocallers_c:
            reason = "caller_not_judged"
        elif cn(csym["key"]) in withheld_c:
            reason = "caller_withheld"
        elif (tsym["file"] not in ofiles and not (is_test_file(lang, tsym["file"]) and not is_test_file(lang, csym["file"]))
              and not any("%s:%d" % (csym["file"], ln) in ext_sites for ln in p["span"])):
            # a callee in a file the oracle did not analyse (build tags, other scope) cannot be judged ... unless the
            # oracle knows this very site calls code outside the repository, which makes an in-repo target wrong
            reason = "callee_file_not_covered"
        elif unresolved_hit(csym["file"], p["span"], tsym["name"].split(".")[-1]):
            reason = "oracle_unresolved_site"
        if reason:
            ungradable[reason] += 1
            ungr_list.append({"caller": {k: csym[k] for k in ("file", "line", "name", "qualname")},
                              "callee": {k: tsym[k] for k in ("file", "line", "name", "qualname")}, "reason": reason,
                              "resolution_kind": res, "evidence_lines": sorted(l for l in p["lines"] if l is not None)[:5],
                              "evidence_file": sorted(p["epaths"])[0]})
            continue
        fp += 1
        by_kind[res][0] += 1
        # classify
        sites = ["%s:%d" % (csym["file"], ln) for ln in sorted(p["span"]) if ln]
        cat = "no_call_at_site"
        detail = None
        by_syntax = called_by_syntax(src, lang, p["epaths"] and sorted(p["epaths"])[0], p["span"], tsym["name"])
        if not by_syntax:
            cat = "reference_as_call"
        for st in (sites if by_syntax else []):
            if st in site_idx:
                same = [r for r in site_idx[st] if cn(ck) in [cn(x) for x in r["cks"]]]
                if same:
                    cat = "wrong_target"
                    names = {r["callee"]["name"] for r in same}
                    detail = {"oracle_callees": sorted({r["tk"] for r in same})[:4],
                              "same_name": tsym["name"] in names}
                    break
                cat = "caller_attribution"
                detail = {"oracle_callers": sorted({r["ck"] for r in site_idx[st]})[:3]}
            elif st in ext_sites:
                cat = "external_callee_at_site"
                break
            elif st in closure_sites:
                cat = "closure_callee_at_site"
                break
        if is_test_file(lang, tsym["file"]) and not is_test_file(lang, csym["file"]):
            cat = "calls_test_code"  # production code cannot call into a test file: certainly wrong
        fp_list.append({"caller": {k: csym[k] for k in ("file", "line", "name", "qualname")},
                        "callee": {k: tsym[k] for k in ("file", "line", "name", "qualname")},
                        "resolution_kind": res, "kinds": dict(p["kinds"]), "evidence_lines": sorted(l for l in p["lines"] if l is not None)[:5],
                        "evidence_file": sorted(p["epaths"])[0], "category": cat, "detail": detail,
                        "callee_kind": tsym["kind"], "caller_kind": csym["kind"]})

    # ---- recall ------------------------------------------------------
    capture_gap = []
    fn_list = []
    judged = 0
    lifted = 0
    both = 0
    disp_total = disp_matched = 0
    matched = 0
    for r in oedges:
        caller_indexed = any(k in sym_keys for k in r["cks"])
        if not caller_indexed:
            capture_gap.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3],
                                "reason": gap_reason(src, lang, r["caller"])})
            continue
        callee_indexed = any(k in sym_keys for k in r["tks"])
        if not callee_indexed:
            # the callee is not a symbol lidx indexes (a nested function, an enum variant, ...): the edge cannot be
            # expressed in lidx's graph, so it is a capture gap like an unindexed caller, not a missed call
            capture_gap.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3], "reason": "callee_not_indexed"})
            continue
        lifted += r["lifted"]
        if r["dispatch"]:
            # dynamic dispatch: the compiler lists every possible target, so a miss is not evidence of a wrong edge
            disp_total += 1
            disp_matched += r["matched"]
            if not r["matched"]:
                fn_list.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3], "category": "dispatch",
                                "fanout": r["fanout"], "detail": None, "dispatch": True})
            continue
        judged += 1
        both += callee_indexed
        if r["matched"]:
            matched += 1
            continue
        cat = "no_edge_at_site"
        detail = None
        if callee_indexed:
            best = None
            sibling = False
            prio = {"wrong_target": 0, "marked_external": 1, "unresolved": 2, "attributed_to_other_caller": 3}
            names = {"resolved": "wrong_target", "external": "marked_external", "unresolved": "unresolved"}
            for st in r["sites"]:
                _, _, ln = st.rpartition(":")
                for ck in r["cks"]:
                    for status, d, res in lidx_by_caller_line.get((cn(ck), int(ln)), []):
                        if status == "resolved" and by_pair.get((cn(ck), cn(d))):
                            sibling = True  # a different call on this line was matched
                            continue
                        c = names[status]
                        if best is None or prio[c] < best[0]:
                            best = (prio[c], c, d, res)
            if best is None:
                # a lidx edge to this very callee at the site, but from a different caller (misattribution)?
                for st in r["sites"]:
                    sf, _, ln = st.rpartition(":")
                    others = [c for c, t in lidx_by_file_line.get((sf, int(ln)), []) if t in {cn(x) for x in r["tks"]}]
                    if others:
                        best = (3, "attributed_to_other_caller", others[0], None)
                        break
            if best is None and not sibling:
                for st in r["sites"]:
                    _, _, ln = st.rpartition(":")
                    for ck in r["cks"]:
                        u = uses_by_line.get((cn(ck), int(ln)))
                        if u:
                            cat, detail = "recorded_as_uses", {"edge_kind": u[0][0], "lidx_target": u[0][1]}
                            break
                    if cat == "recorded_as_uses":
                        break
            if best:
                cat = best[1]
                detail = {"lidx_target": best[2], "resolution_kind": best[3]}
                if cat == "wrong_target":
                    detail["same_file"] = isinstance(best[2], str) and best[2].split(":")[0] == r["callee"]["file"]
            elif sibling:
                cat = "other_call_on_line_missed"
        fn_list.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3], "category": cat,
                        "fanout": r["fanout"], "detail": detail})
    graded = tp + fp
    summary = {
        "oracle_edges": len(oedges),
        "oracle_edges_caller_indexed": judged,
        "capture_gap_edges": len(capture_gap),
        "capture_gap_reasons": dict(Counter(g["reason"] for g in capture_gap)),
        "lifted_caller_edges": lifted,
        "matched_oracle_edges": matched,
        "dispatch_edges": disp_total,
        "dispatch_matched": disp_matched,
        # repowise's denominator: every in-scope oracle edge, capture gaps and unindexed callees included
        "recall_comparable": ratio(matched + disp_matched, len(oedges)),
        "recall_comparable_ci": wilson(matched + disp_matched, len(oedges)),
        "closure_edges": sum(1 for r in oedges if r["closure"]),
        "unresolved_pairs_excluded": ungradable.get("oracle_unresolved_site", 0),
        "precision_if_unresolved_counted_wrong": ratio(tp, graded + ungradable.get("oracle_unresolved_site", 0)),
        "recall_strict": ratio(matched + disp_matched, judged + disp_total),
        "recall_strict_ci": wilson(matched + disp_matched, judged + disp_total),
        "lidx_pairs_total": len(pairs),
        "graded": graded, "tp": tp, "fp": fp,
        "precision": ratio(tp, graded), "precision_ci": wilson(tp, graded),
        "recall": ratio(matched, judged), "recall_ci": wilson(matched, judged),
        "oracle_edges_both_ends_indexed": both,
        "recall_both_ends_indexed": ratio(matched, both),
        "recall_incl_capture_gap": ratio(matched, len(oedges)),
        "ungradable": dict(ungradable),
        "by_resolution_kind": {k: {"graded": v[0], "tp": v[1], "precision": ratio(v[1], v[0]), "ci": wilson(v[1], v[0])}
                               for k, v in sorted(by_kind.items(), key=lambda kv: -kv[1][0])},
        "fp_categories": dict(Counter(f["category"] for f in fp_list).most_common()),
        "fn_categories": dict(Counter(f["category"] for f in fn_list if f["category"] != "dispatch").most_common()),
    }
    return summary, fn_list, fp_list, capture_gap, ungr_list
