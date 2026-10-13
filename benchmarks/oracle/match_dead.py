"""Grade lidx's `dead_symbols` claims against the dead-code oracles. Python 3 stdlib only.

Each language's dead-code oracle (go_oracle/dead, ts_oracle/dead.mjs, rust_oracle/scip_dead.py) lists the declarations it examined
(function, method, type; TypeScript also module-level consts) with two verdicts, `unref` (unreferenced) and `reach` (unreachable
from the roots). A declaration is keyed by file and the line of its name identifier, like the call-graph oracles, and lidx's
symbols get the same key from `match.name_line`.

Per column (`unref`, `reach`) a declaration is either **in the universe** (the oracle judges it: dead or live) or in a **named
bucket** that is excluded from precision and recall as a root or as something the oracle cannot judge:

  public_api             exported from a library: Go exported in an importable package, TypeScript reachable from the package
                         entry, Rust `pub` in a library crate. Other code may use it, so no tool can prove it dead.
  exported_used_by_rule  Go, unreferenced column: U1000 treats every exported identifier as used.
  test_root, main_init, library_root, ffi_or_macro_entry
                         live by construction (test functions, `main`/`init`, the exported surface a library cell uses as roots).
  generated, macro_generated, cfg_twin, oracle_unresolved_site, not_a_function, no_verdict
                         the oracle cannot judge or does not examine (the reachable column judges functions and methods only).

lidx claims are graded in two readings. **All claims**: a claim on a declaration in the universe is right when the oracle says
dead and wrong when it says live; a claim on a public-API or root declaration is wrong by construction (the symbol is live or
may be used from outside; deleting it breaks someone), counted under its bucket name so the cost of not having an
exported-symbol policy is visible; a claim on anything the oracle cannot judge is ungradable (named bucket). **Without exported
symbols**: the same claims after dropping every one on an exported / `pub` declaration, and recall over the unexported dead
declarations: what an exported-symbol policy would buy.
"""
import math
from collections import Counter, defaultdict

POLICY_BUCKETS = ("public_api", "exported_used_by_rule")
ROOT_BUCKETS = ("test_root", "main_init", "library_root", "ffi_or_macro_entry")
# lidx symbol kinds dead_symbols can return (src/db/analytics.rs), and the oracle kind each belongs to
KIND_MAP = {"function": "function", "method": "method", "class": "type", "struct": "type", "type": "type", "interface": "type",
            "enum": "type", "trait": "type", "record": "type", "const": "const"}
CLAIMABLE_LIDX_KINDS = {"function", "method", "class", "struct", "type", "const"}
COLUMNS = ("unref", "reach")


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


def key_of(d):
    return "%s:%d" % (d["file"], d["line"])


def bucket(d, column, lang):
    """None when declaration d is in the universe of `column`, else the name of the bucket it is excluded under."""
    if d.get("generated"):
        return "generated"
    root = d.get("root")
    if root == "macro_generated":
        return "macro_generated"
    if root:
        return root
    if column == "reach" and d["kind"] not in ("function", "method"):
        return "not_a_function"
    if d.get("api"):
        return "public_api"
    if lang == "go" and column == "unref" and d.get("exported"):
        return "exported_used_by_rule"
    if not d.get(column):
        return d.get("withheld") or "no_verdict"
    return None


def claimable(d):
    """Can lidx's dead_symbols ever return this declaration? (Interfaces, enums and traits are not in its kind list.)"""
    if d["kind"] in ("function", "method", "const"):
        return True
    return d.get("subkind", "") not in ("interface", "enum", "trait", "union")


def universe(oracle, column, lang):
    """{key: decl} of the declarations the oracle judges in this column."""
    return {key_of(d): d for d in oracle["decls"] if bucket(d, column, lang) is None}


def claim_file_reason(c, oracle, lang, is_test_file, ofiles):
    if c["file"] in ofiles:
        return "not_a_judged_declaration"
    if is_test_file(lang, c["file"]) and not oracle.get("tests", False):
        return "test_file_outside_cell"
    subdirs = oracle.get("subdirs") or []
    if subdirs and not any(c["file"] == s or c["file"].startswith(s + "/") for s in subdirs):
        return "outside_scope_directory"
    return "file_not_in_program"


def grade_column(oracle, claims, lang, column, is_test_file, incoming=None):
    """Grade lidx claims (list of {key, file, kind, name, qualname}) against one column of one dead-code oracle.

    incoming: optional {claim key -> [descriptions of lidx's own incoming reachability edges]} used only to label why lidx did not
    claim a dead declaration.
    Returns (summary, fp, fn, ungradable)."""
    decls = {}
    for d in oracle["decls"]:
        decls.setdefault(key_of(d), d)
    ofiles = set(oracle["files"])
    uni = universe(oracle, column, lang)
    dead = {k for k, d in uni.items() if d[column] == "dead"}
    by_kind_dead = Counter(uni[k]["kind"] for k in dead)

    seen = set()
    tp = []
    fp = []
    ungr = []
    claimed_keys = set()
    for c in claims:
        k = c["key"]
        if k in seen:
            continue
        seen.add(k)
        d = decls.get(k)
        rec = {"key": k, "file": c["file"], "name": c["name"], "qualname": c["qualname"], "lidx_kind": c["kind"]}
        if d is None:
            ungr.append(dict(rec, reason=claim_file_reason(c, oracle, lang, is_test_file, ofiles)))
            continue
        rec.update(kind=d["kind"], subkind=d.get("subkind", ""), exported=bool(d.get("exported")), line=d["line"])
        b = bucket(d, column, lang)
        claimed_keys.add(k)
        if b is None:
            if d[column] == "dead":
                tp.append(rec)
            else:
                fp.append(dict(rec, category=fp_category(d, column), policy=False))
        elif b in POLICY_BUCKETS or b in ROOT_BUCKETS:
            fp.append(dict(rec, category=b, policy=True))
        else:
            ungr.append(dict(rec, reason=b))

    def read(recs_tp, recs_fp, dead_keys, all_dead_universe_keys):
        """Precision over the claims given and recall over `dead_keys`."""
        n_tp, n_fp = len(recs_tp), len(recs_fp)
        got = {r["key"] for r in recs_tp} & dead_keys
        return {"graded": n_tp + n_fp, "tp": n_tp, "fp": n_fp, "precision": ratio(n_tp, n_tp + n_fp), "precision_ci": wilson(n_tp, n_tp + n_fp),
                "dead": len(dead_keys), "found": len(got), "recall": ratio(len(got), len(dead_keys)), "recall_ci": wilson(len(got), len(dead_keys))}

    all_reading = read(tp, fp, dead, dead)
    unexp = {k for k in dead if not uni[k].get("exported")}
    tp_u = [r for r in tp if not r["exported"]]
    fp_u = [r for r in fp if not r["exported"]]
    no_export = read(tp_u, fp_u, unexp, unexp)
    claimable_dead = {k for k in dead if claimable(uni[k])}
    all_reading["recall_claimable_kinds"] = ratio(len({r["key"] for r in tp} & claimable_dead), len(claimable_dead))
    all_reading["dead_claimable_kinds"] = len(claimable_dead)

    kinds = sorted({d["kind"] for d in uni.values()} | {r["kind"] for r in tp + fp})
    per_kind = {}
    for kd in kinds:
        dk = {k for k in dead if uni[k]["kind"] == kd}
        per_kind[kd] = {"all": read([r for r in tp if r["kind"] == kd], [r for r in fp if r["kind"] == kd], dk, dk),
                        "no_exported": read([r for r in tp_u if r["kind"] == kd], [r for r in fp_u if r["kind"] == kd],
                                            {k for k in dk if not uni[k].get("exported")}, None),
                        "universe": sum(1 for d in uni.values() if d["kind"] == kd)}

    fn = []
    for k in sorted(dead - claimed_keys):
        d = uni[k]
        fn.append({"key": k, "file": d["file"], "line": d["line"], "name": d["name"], "kind": d["kind"], "subkind": d.get("subkind", ""),
                   "exported": bool(d.get("exported")), "category": fn_category(d, (incoming or {}).get(k)),
                   "incoming": ((incoming or {}).get(k) or [])[:3]})

    bucket_counts = Counter(b for b in (bucket(d, column, lang) for d in oracle["decls"]) if b)
    summary = {
        "universe": len(uni), "universe_by_kind": dict(Counter(d["kind"] for d in uni.values())),
        "dead": len(dead), "dead_by_kind": dict(by_kind_dead), "live": len(uni) - len(dead),
        "oracle_buckets": dict(bucket_counts.most_common()),
        "claims": len(seen), "claims_ungradable": dict(Counter(u["reason"] for u in ungr).most_common()),
        "all": all_reading, "no_exported": no_export, "by_kind": per_kind,
        "fp_categories": dict(Counter(f["category"] for f in fp).most_common()),
        "fp_categories_no_exported": dict(Counter(f["category"] for f in fp_u).most_common()),
        "fn_categories": dict(Counter(f["category"] for f in fn).most_common()),
    }
    return summary, fp, fn, ungr


def fp_category(d, column):
    """Why a claim on a live declaration is wrong, in terms of what the oracle saw."""
    sub = d.get("subkind", "")
    if sub == "trait_impl":
        return "trait_impl_method"
    if d["kind"] == "method" and d.get("overrides"):
        return "overrides_or_implements"
    if column == "reach":
        return "reachable_%s" % d["kind"]
    return "referenced_%s" % d["kind"]


def fn_category(d, incoming):
    """Why lidx did not claim a declaration the oracle says is dead, from lidx's own incoming reachability edges (tags from
    dead_code.incoming_edges: `test` source in a test file, `self`, `deadsrc` source dead per the oracle, `live`)."""
    if not claimable(d):
        return "kind_not_in_dead_symbols"
    if incoming is None:
        return "not_claimed"
    if not incoming:
        return "no_incoming_edge_but_filtered"
    live = [i for i in incoming if i.endswith(":live")]
    if live:
        kinds = {i.split(":")[0] for i in live}
        if kinds & {"CALLS", "DISPATCH"}:
            return "incoming_call_from_live_code"
        if kinds & {"IMPLEMENTS", "EXTENDS", "INHERITS"}:
            return "incoming_type_edge_from_live_code"
        if kinds <= {"IMPORTS"}:
            return "incoming_import_only"
        return "incoming_reference_from_live_code"
    tags = {i.rsplit(":", 1)[1] for i in incoming}
    if tags == {"test"}:
        return "incoming_from_tests_only"
    if tags == {"self"}:
        return "incoming_self_edge_only"
    if tags == {"deadsrc"}:
        return "incoming_from_dead_code_only"
    return "incoming_from_tests_or_dead_code"


def lidx_claims(rows, syms):
    """Claims from `dead_symbols` rows: only symbols with a source key."""
    out = []
    for r in rows:
        s = syms.get(r["id"])
        if not s or "key" not in s:
            continue
        out.append({"key": s["key"], "file": s["file"], "kind": r["kind"], "name": s["name"], "qualname": s["qualname"], "id": r["id"]})
    return out
