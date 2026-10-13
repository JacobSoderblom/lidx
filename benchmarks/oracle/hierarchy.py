"""Grade lidx's type hierarchy (EXTENDS / IMPLEMENTS edges and method override pairs) against the compiler oracles.

Oracle side: the `hierarchy` object each oracle emits (go_oracle/hierarchy.go, ts_oracle/hierarchy.mjs,
rust_oracle/scip_hierarchy.py): pairs keyed by (file, line of the declaration's name), the judged type declarations, and a
named bucket for what was left out. lidx side: its own EXTENDS/IMPLEMENTS/INHERITS rows with both ends in the repository and,
for Go and TypeScript method overrides, the pairs of its dispatch query (`lidx dispatch-pairs`, the same rows
`explain_symbol`, `analyze_impact` and `dead_symbols` use); for Rust the method-level IMPLEMENTS rows.

A lidx pair is a *true* pair when the oracle has it, a *wrong* pair when both of its ends are declarations the oracle judged
and it does not, and *ungradable* otherwise (an end outside the analysed files or a declaration the oracle cannot judge).
Python 3 stdlib only.
"""
import re
import sqlite3
from collections import defaultdict

from match import ratio, wilson

HIER_KINDS = ("EXTENDS", "IMPLEMENTS", "INHERITS")
RELATIONS = {"go": ("implements", "extends", "embeds", "overrides"),
             "ts": ("implements", "extends", "overrides"),
             "rust": ("implements", "extends", "overrides")}
# the oracle key holding each relation's pairs
ORACLE_KEY = {"implements": "implements", "extends": "extends", "embeds": "embeds", "overrides": "overrides"}


def load_hier_edges(db_path):
    """EXTENDS / IMPLEMENTS / INHERITS rows: dicts with kind, source/target symbol ids, unresolved target text."""
    con = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(
        "select e.kind, e.source_symbol_id as s, e.target_symbol_id as t, e.target_qualname as tq, e.resolution_kind as res, "
        "e.evidence_start_line as line, f.path as epath from edges e join files f on f.id = e.file_id "
        "where e.kind in ('EXTENDS','IMPLEMENTS','INHERITS') and f.deleted_version is null")]
    con.close()
    return rows


def _in_repo(sym):
    return sym is not None and "key" in sym


def edge_relation(lang, edge, syms):
    """Which relation a lidx row speaks about (or None). Go struct embedding and interface embedding are both EXTENDS rows;
    the source's kind tells them apart. Rust method-level IMPLEMENTS rows are the overrides."""
    s, t = syms.get(edge["s"]), syms.get(edge["t"]) if edge["t"] is not None else None
    sk = (s or {}).get("kind")
    if lang == "go":
        if edge["kind"] == "IMPLEMENTS":
            return "implements"
        return "extends" if sk == "interface" else "embeds"
    if lang == "ts":
        return "implements" if edge["kind"] == "IMPLEMENTS" else "extends"
    # rust
    if edge["kind"] == "IMPLEMENTS":
        if sk in ("method", "function"):
            return "overrides"
        return "extends" if sk in ("trait", "interface") else "implements"
    return "extends"


def lidx_pairs(lang, syms, edges, dispatch_ids):
    """relation -> {(sub key, base key): evidence}; external bases -> list of (sub key, relation, target text)."""
    pairs = {r: {} for r in RELATIONS[lang]}
    ext = []
    for e in edges:
        s = syms.get(e["s"])
        if not _in_repo(s):
            continue
        rel = edge_relation(lang, e, syms)
        if rel not in pairs:
            continue
        t = syms.get(e["t"]) if e["t"] is not None else None
        if _in_repo(t):
            pairs[rel].setdefault((s["key"], t["key"]), e)
        else:
            text = (t or {}).get("qualname") or e.get("tq") or ""
            ext.append((s["key"], rel, text))
    if lang in ("go", "ts") and dispatch_ids is not None:
        for b, i in dispatch_ids:
            sb, si = syms.get(b), syms.get(i)
            if _in_repo(sb) and _in_repo(si):
                pairs["overrides"].setdefault((si["key"], sb["key"]), {"kind": "DISPATCH_PAIR", "s": i, "t": b})
    return pairs, ext


def _segments(text):
    text = re.sub(r"<[^<>]*>", "", text.replace("ext:", "", 1))
    return [x for x in re.split(r"::|\.|/", text) if x]


def ext_match(base, found):
    """Does an unresolved / external lidx target `found` name the oracle's external base `base`? Compares as many trailing
    segments as the oracle's name has (`Display::fmt` against `std::fmt::Display::fmt`, `Error` against `Error`)."""
    b, f = _segments(base), _segments(found)
    if not b or not f:
        return False
    k = min(len(b), 2)
    return b[-k:] == f[-k:]


class Cell:
    """Everything needed to judge pairs in one cell."""

    def __init__(self, oracle, hier, syms, lang):
        self.lang = lang
        self.files = set(oracle["files"])
        self.h = hier
        self.canon = {}
        for s in syms.values():
            if "key" in s:
                c = "%s|%s" % (s["file"], s["qualname"])
                if s["key"] not in self.canon or c < self.canon[s["key"]]:
                    self.canon[s["key"]] = c
        self.sym_keys = {s["key"] for s in syms.values() if "key" in s}
        key = lambda d: "%s:%d" % (d["file"], d["line"])  # noqa: E731
        self.types = {self.cn(key(d)) for d in hier.get("types", [])}
        self.ifaces = {self.cn(key(d)) for d in hier.get("ifaces", [])}
        if lang == "ts":
            self.ifaces = self.types  # classes, interfaces and aliases all appear on both sides of TS pairs
        self.ungradable = {self.cn(key(u["decl"])): u["reason"] for u in hier.get("ungradable", [])}
        # method -> container key, to tell whether an override pair's two ends sit in judged types
        by_q = {s["qualname"]: s for s in syms.values() if "key" in s}
        self.container = {}
        for s in syms.values():
            if "key" in s and s.get("kind") in ("method", "function", "property", "field") and s.get("name"):
                q = s["qualname"]
                for cut in (len(s["name"]) + 1, len(s["name"]) + 2):
                    c = by_q.get(q[:-cut]) if len(q) > cut else None
                    if c:
                        self.container[s["key"]] = c["key"]
                        break

    def cn(self, k):
        return self.canon.get(k, k)

    def judged(self, rel, side, key):
        """Is this lidx declaration one the oracle judged for this relation (`side`: 'sub' or 'base')?"""
        k = self.cn(key)
        if rel == "overrides":
            c = self.container.get(key)
            if c is None:
                return False
            c = self.cn(c)
            return c in self.types if side == "sub" else c in (self.ifaces | self.types)
        if rel == "implements":
            return k in self.types if side == "sub" else k in self.ifaces
        if rel in ("extends", "embeds"):
            if self.lang == "rust":
                return k in self.ifaces
            if self.lang == "go":
                return (k in self.ifaces) if rel == "extends" else (k in self.types if side == "sub" else k in (self.types | self.ifaces))
            return k in self.types
        return False


def grade_cell(lang, oracle, syms, edges, dispatch_ids, src=None):
    """Returns (summary, fn_list, fp_list, ungradable_list)."""
    h = oracle.get("hierarchy")
    if h is None:
        return None, [], [], []
    cell = Cell(oracle, h, syms, lang)
    lp, lext = lidx_pairs(lang, syms, edges, dispatch_ids)
    cn = cell.cn
    summary = {"relations": {}, "excluded": h.get("excluded", {}), "judged_types": len(cell.types), "judged_interfaces": len(cell.ifaces)}
    fn_list, fp_list, ungr_list = [], [], []
    # type-level pairs lidx holds, canonical, per relation: used to classify override misses
    held = {r: {(cn(a), cn(b)) for (a, b) in lp[r]} for r in lp}
    held_any = defaultdict(set)
    for r, ps in held.items():
        for p in ps:
            held_any[p].add(r)
    # type-level reachability over lidx's own IMPLEMENTS / EXTENDS rows, to tell a missing type edge from a missing method pairing
    succ = defaultdict(set)
    for r in ("implements", "extends"):
        for (a, b) in held.get(r, ()):
            succ[a].add(b)

    def reaches(a, b):
        todo, seen = [a], {a}
        while todo:
            x = todo.pop()
            if x == b:
                return True
            for y in succ.get(x, ()):
                if y not in seen:
                    seen.add(y)
                    todo.append(y)
        return False

    def type_key(v):
        """Key of a pair's declaring types: the oracle writes `Name@file:line` (Go) or `file:line` (TypeScript)."""
        if not v:
            return None
        v = v.rsplit("@", 1)[-1]
        return v if re.match(r".+:\d+$", v) else None

    for rel in RELATIONS[lang]:
        opairs = h.get(ORACLE_KEY[rel], [])
        okeys = {}
        for p in opairs:
            sub = "%s:%d" % (p["sub"]["file"], p["sub"]["line"])
            bases = ["%s:%d" % (p["base"]["file"], p["base"]["line"])] + list(p.get("base_alts", []))
            okeys[(cn(sub), cn(bases[0]))] = (p, [cn(b) for b in bases])
        # an oracle pair with several acceptable bases (merged declarations) is matched through any of them
        lp_c = defaultdict(list)
        for (a, b), ev in lp[rel].items():
            lp_c[(cn(a), cn(b))].append((a, b, ev))
        matched = set()
        for (a, b) in list(lp_c):
            for (osub, obase), (p, bases) in okeys.items():
                if osub == a and b in bases:
                    matched.add((osub, obase))
        tp = fp = ungr = 0
        reasons = defaultdict(int)
        okeyset = {(osub, bb) for (osub, obase), (p, bases) in okeys.items() for bb in bases}
        for (a, b), evs in lp_c.items():
            a_raw, b_raw, ev = evs[0]
            if (a, b) in okeyset:
                tp += 1
                continue
            sf = a_raw.rsplit(":", 1)[0]
            bf = b_raw.rsplit(":", 1)[0]
            why = None
            if sf not in cell.files or bf not in cell.files:
                why = "file_not_analysed"
            elif a in cell.ungradable or b in cell.ungradable:
                why = cell.ungradable.get(a) or cell.ungradable.get(b)
            elif not (cell.judged(rel, "sub", a_raw) and cell.judged(rel, "base", b_raw)):
                why = "end_not_judged"
            if why:
                ungr += 1
                reasons[why] += 1
                ungr_list.append({"relation": rel, "reason": why, "sub": a_raw, "base": b_raw, "kind": ev.get("kind"), "evidence_file": ev.get("epath"),
                                  "evidence_line": ev.get("line")})
                continue
            fp += 1
            cat = "other"
            if (b, a) in okeyset:
                cat = "reverse_direction"
            elif rel == "implements" and lang == "ts" and any(
                    cn("%s:%d" % (x["sub"]["file"], x["sub"]["line"])) == a and cn("%s:%d" % (x["base"]["file"], x["base"]["line"])) == b
                    for x in h.get("structural_implements", [])):
                cat = "structural_only"
            elif rel == "overrides":
                ca, cb = cell.container.get(a_raw), cell.container.get(b_raw)
                if ca and cb and (cn(ca), cn(cb)) not in {(cn("%s:%d" % (x["sub"]["file"], x["sub"]["line"])), cn("%s:%d" % (x["base"]["file"], x["base"]["line"])))
                                                         for x in h.get("implements", []) + h.get("extends", [])}:
                    cat = "types_unrelated_in_compiler"
                else:
                    cat = "no_such_override"
            elif held_any.get((a, b), set()) - {rel}:
                cat = "other_relation_in_oracle"
            fp_list.append({"relation": rel, "category": cat, "sub": a_raw, "base": b_raw, "kind": ev.get("kind"), "evidence_line": ev.get("line"),
                            "evidence_file": ev.get("epath"), "resolution": ev.get("res")})
        # oracle pairs: recall
        n = len(okeys)
        miss = 0
        gap = 0
        for (osub, obase), (p, bases) in okeys.items():
            if (osub, obase) in matched:
                continue
            miss += 1
            sub_k = "%s:%d" % (p["sub"]["file"], p["sub"]["line"])
            base_k = "%s:%d" % (p["base"]["file"], p["base"]["line"])
            if sub_k not in cell.sym_keys or base_k not in cell.sym_keys:
                cat = "not_indexed"
                gap += 1
            elif any(rel2 != rel for rel2 in held_any.get((osub, obase), ())):
                cat = "other_kind_of_edge"
            elif rel == "overrides":
                st, bt = type_key(p.get("sub_type")), type_key(p.get("base_type"))
                if p.get("promoted"):
                    cat = "promoted_method"
                elif not (st and bt and reaches(cn(st), cn(bt))):
                    cat = "type_edge_missing"
                elif cn(cell.container.get(sub_k, "")) != cn(st):
                    cat = "method_not_in_type_qualname"  # lidx names the method after another container than its type
                else:
                    cat = "method_pairing_missing"
            else:
                cat = "no_edge"
            row = {"relation": rel, "category": cat, "sub": sub_k, "base": base_k, "sub_name": p["sub"]["name"], "base_name": p["base"]["name"]}
            for kk in ("ptr", "promoted", "depth", "via", "sub_type", "base_type"):
                if kk in p:
                    row[kk] = p[kk]
            fn_list.append(row)
        graded = tp + fp
        summary["relations"][rel] = {
            "lidx_pairs": len(lp_c), "graded": graded, "tp": tp, "fp": fp, "ungradable": ungr,
            "ungradable_reasons": dict(reasons),
            "precision": ratio(tp, graded), "precision_ci": wilson(tp, graded),
            "oracle_pairs": n, "matched": n - miss, "fn": miss, "capture_gap": gap,
            "recall": ratio(n - miss, n), "recall_ci": wilson(n - miss, n),
        }
        if lang == "ts" and rel == "implements":
            structural = sum(1 for x in fp_list if x["relation"] == rel and x["category"] == "structural_only")
            summary["relations"][rel]["fp_structural_only"] = structural
            summary["relations"][rel]["precision_incl_structural"] = ratio(tp + structural, graded)
    # external bases: existence only
    ext_by_sub = defaultdict(list)
    for sk, rel, text in lext:
        ext_by_sub[sk].append((rel, text))
    ext_total = ext_found = 0
    ext_missing = []
    for x in h.get("external_base", []):
        sub_k = cn("%s:%d" % (x["sub"]["file"], x["sub"]["line"]))
        if sub_k not in {cn(k) for k in cell.sym_keys}:
            ext_total += 1
            ext_missing.append({"relation": x["relation"], "category": "not_indexed", "sub": "%s:%d" % (x["sub"]["file"], x["sub"]["line"]), "base": x["base"], "bucket": "external_base"})
            continue
        ext_total += 1
        found = any(ext_match(x["base"], text) for k, lst in ext_by_sub.items() if cn(k) == sub_k for (_, text) in lst)
        if found:
            ext_found += 1
        else:
            ext_missing.append({"relation": x["relation"], "category": "no_edge", "sub": "%s:%d" % (x["sub"]["file"], x["sub"]["line"]), "base": x["base"], "bucket": "external_base"})
    summary["external_base"] = {"oracle": ext_total, "found": ext_found, "recall": ratio(ext_found, ext_total), "recall_ci": wilson(ext_found, ext_total)}
    fn_list.extend(ext_missing)
    if lang == "ts":
        summary["structural_implements"] = {"oracle_pairs": len(h.get("structural_implements", [])),
                                            "of_lidx_implements_fp": sum(1 for x in fp_list if x["category"] == "structural_only")}
    return summary, fn_list, fp_list, ungr_list
