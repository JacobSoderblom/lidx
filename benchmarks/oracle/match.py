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
    if lang == "ts" and kind == "module":
        return 0
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
        "e.evidence_start_line as line, f.path as epath from edges e join files f on f.id = e.file_id "
        "where e.kind in ('CALLS','REFERENCES') and f.deleted_version is null"
    ):
        edges.append(dict(r))
    con.close()
    return syms, edges


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
    call = re.compile(r"(?<![\w$])" + bare + r"\s*(::<[^()]*>)?\s*(<[^()]*>)?\s*\(")
    new = re.compile(r"\bnew\s+[\w$.]*" + bare + r"\b")
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


def grade(oracle, syms, edges, lang, src, edge_kinds=("CALLS",)):
    """Compare lidx edges of the given kinds against the oracle. Returns (summary, fn_list, fp_list, extra)."""
    sym_keys = {s["key"] for s in syms.values() if "key" in s}
    # ---- oracle side -------------------------------------------------
    ofiles = set(oracle["files"])
    ocallers = {"%s:%d" % (c["file"], c["line"]) for c in oracle["callers"]}
    unresolved = set(oracle.get("unresolved_sites", []))
    ext_sites = set(oracle.get("ext_sites", []))
    closure_sites = set(oracle.get("closure_sites", []))

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
               "sites": e.get("sites", []), "matched": False, "lifted": lifted}
        oedges.append(rec)
        for a in cks:
            for b in tks:
                by_pair[(a, b)].append(rec)
        for s in rec["sites"]:
            site_idx[s].append(rec)
    for r in oedges:
        r["fanout"] = max((len(site_idx[s]) for s in r["sites"]), default=1)

    # ---- lidx side ---------------------------------------------------
    pairs = {}
    lidx_by_caller_line = defaultdict(list)  # (caller_key, line) -> [(status, detail)]
    for e in edges:
        if e["kind"] not in edge_kinds:
            continue
        s = syms.get(e["s"])
        if not s or "key" not in s:
            continue
        ck = s["key"]
        t = syms.get(e["t"]) if e["t"] else None
        res = e["resolution_kind"] or "none"
        line = e["line"]
        if t is None:
            lidx_by_caller_line[(ck, line)].append(("unresolved", e["target_qualname"], res))
            continue
        if t.get("external"):
            lidx_by_caller_line[(ck, line)].append(("external", t["qualname"], res))
            continue
        lidx_by_caller_line[(ck, line)].append(("resolved", t["key"], res))
        p = pairs.setdefault((ck, t["key"]), {"caller": s, "callee": t, "kinds": Counter(), "lines": set(), "edge_kinds": set(),
                                              "epaths": set()})
        p["kinds"][res] += 1
        p["lines"].add(line)
        p["edge_kinds"].add(e["kind"])
        p["epaths"].add(e["epath"])

    # ---- precision ---------------------------------------------------
    ungradable = Counter()
    tp = fp = 0
    by_kind = defaultdict(lambda: [0, 0])  # kind -> [graded, tp]
    fp_list = []
    for (ck, tk), p in pairs.items():
        csym, tsym = p["caller"], p["callee"]
        res = pick_res(set(p["kinds"]))
        hits = by_pair.get((ck, tk))
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
        elif ck not in ocallers:
            reason = "caller_not_judged"
        elif tsym["file"] not in ofiles:
            reason = "callee_file_not_covered"
        elif any(("%s:%d" % (csym["file"], ln)) in unresolved for ln in p["lines"]):
            reason = "oracle_unresolved_site"
        if reason:
            ungradable[reason] += 1
            continue
        fp += 1
        by_kind[res][0] += 1
        # classify
        sites = ["%s:%d" % (csym["file"], ln) for ln in sorted(p["lines"]) if ln]
        cat = "no_call_at_site"
        detail = None
        by_syntax = called_by_syntax(src, lang, p["epaths"] and sorted(p["epaths"])[0], p["lines"], tsym["name"])
        if not by_syntax:
            cat = "reference_as_call"
        for st in (sites if by_syntax else []):
            if st in site_idx:
                same = [r for r in site_idx[st] if r["ck"] == ck or ck in r["cks"]]
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
        fp_list.append({"caller": {k: csym[k] for k in ("file", "line", "name", "qualname")},
                        "callee": {k: tsym[k] for k in ("file", "line", "name", "qualname")},
                        "resolution_kind": res, "kinds": dict(p["kinds"]), "evidence_lines": sorted(p["lines"])[:5],
                        "evidence_file": sorted(p["epaths"])[0], "category": cat, "detail": detail,
                        "callee_kind": tsym["kind"], "caller_kind": csym["kind"]})

    # ---- recall ------------------------------------------------------
    capture_gap = []
    fn_list = []
    judged = 0
    lifted = 0
    matched = 0
    for r in oedges:
        caller_indexed = any(k in sym_keys for k in r["cks"])
        if not caller_indexed:
            capture_gap.append({"caller": r["caller"], "callee": r["callee"], "sites": r["sites"][:3],
                                "reason": gap_reason(src, lang, r["caller"])})
            continue
        lifted += r["lifted"]
        judged += 1
        if r["matched"]:
            matched += 1
            continue
        callee_indexed = any(k in sym_keys for k in r["tks"])
        cat = "callee_not_indexed" if not callee_indexed else "no_edge_at_site"
        detail = None
        if callee_indexed:
            best = None
            sibling = False
            prio = {"wrong_target": 0, "marked_external": 1, "unresolved": 2}
            names = {"resolved": "wrong_target", "external": "marked_external", "unresolved": "unresolved"}
            for st in r["sites"]:
                _, _, ln = st.rpartition(":")
                for ck in r["cks"]:
                    for status, d, res in lidx_by_caller_line.get((ck, int(ln)), []):
                        if status == "resolved" and by_pair.get((ck, d)):
                            sibling = True  # a different call on this line was matched
                            continue
                        c = names[status]
                        if best is None or prio[c] < best[0]:
                            best = (prio[c], c, d, res)
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
        "lidx_pairs_total": len(pairs),
        "graded": graded, "tp": tp, "fp": fp,
        "precision": ratio(tp, graded), "precision_ci": wilson(tp, graded),
        "recall": ratio(matched, judged), "recall_ci": wilson(matched, judged),
        "recall_incl_capture_gap": ratio(matched, len(oedges)),
        "ungradable": dict(ungradable),
        "by_resolution_kind": {k: {"graded": v[0], "tp": v[1], "precision": ratio(v[1], v[0]), "ci": wilson(v[1], v[0])}
                               for k, v in sorted(by_kind.items(), key=lambda kv: -kv[1][0])},
        "fp_categories": dict(Counter(f["category"] for f in fp_list).most_common()),
        "fn_categories": dict(Counter(f["category"] for f in fn_list).most_common()),
    }
    return summary, fn_list, fp_list, capture_gap
