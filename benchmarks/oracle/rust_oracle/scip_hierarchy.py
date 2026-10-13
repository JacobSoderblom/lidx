#!/usr/bin/env python3 -I
"""Type hierarchy oracle from a rust-analyzer SCIP index (issue #449).

Usage: scip_hierarchy.py --repo DIR --scip FILE [--no-tests | --path-tests-excluded] > hierarchy.json

rust-analyzer's SCIP carries no implementation relationships (the `is_implementation` flag is never set), so the
implementation facts come from the impl symbols themselves: every item of `impl Trait for Type { .. }` is a symbol
`impl#[Type][Trait]item`, whose definition sits inside the impl block. The *names* in the brackets are only text; which
declarations `Type` and `Trait` are is read from the reference occurrences rust-analyzer emits on the impl header
(`impl<..> Trait for Type`), so two types with one name are never confused. Likewise supertraits are the reference
occurrences on a `trait A: B` header.

Relations (keys are `file`, `line` of the declaration's name, like the call edges):
  implements  type -> trait
  overrides   impl method -> trait method (a method the trait declares and the impl defines)
  extends     trait -> supertrait
Left out, each a named bucket in `excluded`: blanket impls (`impl<T> Trait for T`, no in-repo type), impls for a type the
repository does not declare (`impl Trait for Vec<Foo>`, `&str`), impls whose Self type is a type alias, and impls with no
item (marker traits: rust-analyzer emits no symbol for an empty impl). Bases outside the repository (`impl Display for X`,
`trait A: Send`) go to `external_base`, judged for existence only.
"""
import argparse
import importlib.util
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("scip_calls", os.path.join(HERE, "scip_calls.py"))
sc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sc)

KIND_STRUCT, KIND_ENUM, KIND_TRAIT, KIND_ALIAS, KIND_UNION = 49, 11, 53, 55, 62
HEADER_RE = re.compile(r"^\s*(?:unsafe\s+)?impl\b")
IMPL_ITEM_RE = re.compile(r"^impl#\[.+?\]\[.+?\]")


def fn_name(sym):
    """Method name of a function symbol; for `impl#[Type][Trait]name().` the name after the brackets."""
    m = sc.IMPL_RE.match(sc.last_segment(sym))
    return m.group(3).strip("`") if m else sc.short_name(sym)


def parse_kinds(data):
    """symbol -> SCIP SymbolInformation.kind for every document."""
    kinds = {}
    for f, w, v in sc.fields(data):
        if f != 2:
            continue
        for f2, w2, v2 in sc.fields(v):
            if f2 != 3:
                continue
            sym, kind = "", None
            for f3, w3, v3 in sc.fields(v2):
                if f3 == 1:
                    sym = v3.decode()
                elif f3 == 5:
                    kind = v3
            if sym:
                kinds[sym] = kind
    return kinds


def header_text(lines, first, limit=12):
    """(text, [(line0, offset of that line in text)]) from `first` through the line holding the opening brace or semicolon."""
    parts, offs, pos = [], [], 0
    for ln in range(first, min(len(lines), first + limit)):
        offs.append((ln, pos))
        parts.append(lines[ln])
        pos += len(lines[ln]) + 1
        if "{" in lines[ln] or lines[ln].rstrip().endswith(";"):
            break
    return "\n".join(parts), offs


def depth_at(text, off):
    """Angle / paren / bracket nesting depth of text[off] (0 = top level of the header)."""
    depth, i = 0, 0
    while i < off:
        ch = text[i]
        if ch in "<([":
            depth += 1
        elif ch == ">" and i > 0 and text[i - 1] == "-":
            pass
        elif ch in ">)]":
            depth = max(0, depth - 1)
        i += 1
    return depth


def skip_generics(text, i):
    """Index after a balanced `<...>` starting at or after i (skipping spaces); i unchanged when there is none."""
    j = i
    while j < len(text) and text[j].isspace():
        j += 1
    if j < len(text) and text[j] == "<":
        d = 0
        while j < len(text):
            if text[j] == "<":
                d += 1
            elif text[j] == ">" and text[j - 1] != "-":
                d -= 1
                if d == 0:
                    return j + 1
            j += 1
    return i


def top_level_for(text, start):
    """Offset of the `for` keyword of `impl Trait for Type` at nesting depth 0 after `start`; -1 if none."""
    for m in re.finditer(r"\bfor\b", text[start:]):
        off = start + m.start()
        if text[off + 3:off + 5].lstrip().startswith("<"):
            continue  # `for<'a>` higher-ranked bound
        if depth_at(text[start:], off - start) == 0:
            return off
    return -1


def end_of_bounds(text, start):
    m = re.compile(r"\bwhere\b|\{|;").search(text, start)
    return m.start() if m else len(text)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--scip", required=True)
    ap.add_argument("--no-tests", action="store_true")
    ap.add_argument("--path-tests-excluded", action="store_true")
    a = ap.parse_args()
    repo = os.path.realpath(a.repo)
    data = open(a.scip, "rb").read()
    kinds = parse_kinds(data)
    docs_all = [sc.parse_document(v) for f, w, v in sc.fields(data) if f == 2]

    # the same scope as scip_calls.py
    docs = docs_all
    regions = {}
    if a.path_tests_excluded:
        docs = [d for d in docs if not (d["path"].endswith(".rs") and sc.is_path_test(d["path"]))]
    if a.no_tests:
        kept, gated_files, gated_dirs = [], set(), []
        for d in docs:
            if d["path"].endswith(".rs") and sc.is_test_path(d["path"]):
                continue
            kept.append(d)
            try:
                text = open(os.path.join(repo, d["path"]), encoding="utf-8", errors="replace").read()
            except OSError:
                text = ""
            regions[d["path"]] = sc.test_regions(text)
            for name in sc.gated_modules(text):
                fs, dr = sc.module_files(d["path"], name)
                gated_files |= fs
                gated_dirs.append(dr)
        docs = [d for d in kept if d["path"] not in gated_files and not d["path"].startswith(tuple(gated_dirs))]

    def in_test(path, line0):
        return any(a0 <= line0 <= b0 for a0, b0 in regions.get(path, ()))

    in_scope = {d["path"] for d in docs}
    texts = {}

    def lines_of(path):
        if path not in texts:
            try:
                texts[path] = open(os.path.join(repo, path), encoding="utf-8", errors="replace").read().split("\n")
            except OSError:
                texts[path] = []
        return texts[path]

    # declarations of types and traits, from the definition occurrences of every document (an out-of-scope file's type is
    # then "outside the analysed scope", not "outside the repository")
    tdefs = {}  # symbol -> (path, line, name)
    fdefs = {}  # fn symbol -> (path, line, name)  (in-scope only)
    for d in docs_all:
        if not d["path"].endswith(".rs"):
            continue
        for o in d["occ"]:
            if not (o["roles"] & 1) or not o["range"]:
                continue
            sym = o["symbol"]
            l = sc.rng(o)[0]
            seg = sc.desc(sym).rsplit("/", 1)[-1]
            if sym.endswith("#") and seg.count("#") == 1 and not seg.startswith("impl#"):
                if kinds.get(sym) in (KIND_STRUCT, KIND_ENUM, KIND_TRAIT, KIND_ALIAS, KIND_UNION):
                    tdefs.setdefault(sym, (d["path"], l + 1, sc.ctor_name(sym)))
            elif sym.endswith("().") and d["path"] in in_scope and not (a.no_tests and in_test(d["path"], l)):
                fdefs.setdefault(sym, (d["path"], l + 1, fn_name(sym)))

    def decl(t):
        return {"file": t[0], "line": t[1], "name": t[2]}

    def in_scope_type(sym):
        t = tdefs.get(sym)
        return t is not None and t[0] in in_scope and not (a.no_tests and in_test(t[0], t[1] - 1))

    out = {"implements": [], "overrides": [], "extends": [], "external_base": [], "types": [], "ifaces": [], "excluded": {}}
    ex = out["excluded"]

    def bump(k):
        ex[k] = ex.get(k, 0) + 1

    for sym, t in tdefs.items():
        if not in_scope_type(sym):
            continue
        k = kinds.get(sym)
        if k == KIND_TRAIT:
            out["ifaces"].append(decl(t))
        elif k in (KIND_STRUCT, KIND_ENUM, KIND_UNION):
            out["types"].append(decl(t))

    seen_impl, seen_ov, seen_ext, seen_x = set(), set(), set(), set()
    external = lambda sub, base, rel: (  # noqa: E731
        None if (sub["file"], sub["line"], base, rel) in seen_x or seen_x.add((sub["file"], sub["line"], base, rel))
        else out["external_base"].append({"sub": sub, "base": base, "relation": rel}))

    def occs_in(d, header_lines):
        """Reference occurrences ending in `#` on the header lines, as (line0, col, symbol)."""
        res = []
        for o in d["occ"]:
            if o["roles"] & 1 or not o["range"] or not o["symbol"].endswith("#"):
                continue
            l, c, el, ec = sc.rng(o)
            if l in header_lines and el == l:
                res.append((l, c, o["symbol"]))
        return sorted(res)

    def name_of(sym):
        return sc.ctor_name(sym)

    handled_blocks = set()
    for d in docs:
        if not d["path"].endswith(".rs"):
            continue
        lines = lines_of(d["path"])
        # ---- impl blocks: group the impl items by the header they sit under
        blocks = {}  # header line0 -> list of (item symbol, def line0, fn symbol or None)
        for o in d["occ"]:
            if not (o["roles"] & 1) or not o["range"]:
                continue
            last = sc.last_segment(o["symbol"])
            if not IMPL_ITEM_RE.match(last):
                continue
            l = sc.rng(o)[0]
            if a.no_tests and in_test(d["path"], l):
                continue
            indent = len(lines[l]) - len(lines[l].lstrip()) if l < len(lines) else 0
            h = l
            while h >= 0 and not (HEADER_RE.match(lines[h]) and len(lines[h]) - len(lines[h].lstrip()) < max(indent, 1)):
                h -= 1
            if h < 0:
                bump("impl_header_not_found")
                continue
            blocks.setdefault(h, []).append((o["symbol"], l, o["symbol"] if o["symbol"].endswith("().") else None))
        for h, items in sorted(blocks.items()):
            text, offs = header_text(lines, h)
            header_lines = {ln for ln, _ in offs}
            offmap = dict(offs)
            m = re.search(r"\bimpl\b", text)
            start = skip_generics(text, m.end())
            f = top_level_for(text, start)
            if f < 0:
                bump("impl_header_unparsed")
                continue
            tr_end = f
            ty_end = end_of_bounds(text, f + 3)
            occs = []
            for (l, c, sym) in occs_in(d, header_lines):
                off = offmap[l] + c
                occs.append((off, sym))
            trait_sym = next((s for off, s in occs if start <= off < tr_end and depth_at(text, off) == 0), None)
            type_sym = next((s for off, s in occs if f + 3 <= off < ty_end and depth_at(text, off) == 0), None)
            if trait_sym is None:
                bump("impl_trait_unresolved")
                continue
            in_repo_trait = trait_sym in tdefs
            if type_sym is None:
                bump("blanket_impl")  # `impl<T> Trait for T`: no in-repo type to relate
                continue
            if type_sym not in tdefs:
                bump("impl_for_foreign_type")
                continue
            if kinds.get(type_sym) == KIND_ALIAS:
                bump("impl_for_type_alias")
                continue
            if not in_scope_type(type_sym):
                bump("impl_type_out_of_scope")
                continue
            sub = decl(tdefs[type_sym])
            if not in_repo_trait:
                external(sub, name_of(trait_sym), "implements")
            elif not in_scope_type(trait_sym):
                bump("impl_trait_out_of_scope")
            else:
                k = (sub["file"], sub["line"], tdefs[trait_sym][0], tdefs[trait_sym][1])
                if k not in seen_impl:
                    seen_impl.add(k)
                    out["implements"].append({"sub": sub, "base": decl(tdefs[trait_sym])})
            # overrides: the impl's methods against the trait's declarations of the same name
            for item, l, fn in items:
                if fn is None or fn not in fdefs:
                    continue
                mname = fn_name(fn)
                msub = {"file": fdefs[fn][0], "line": fdefs[fn][1], "name": mname}
                if not in_repo_trait:
                    external(msub, "%s::%s" % (name_of(trait_sym), mname), "overrides")
                    continue
                tm = trait_sym + mname + "()."
                tm_def = fdefs.get(tm)
                if tm_def is None:
                    if tdefs[trait_sym][0] in in_scope:
                        bump("override_trait_method_not_found")
                    continue
                k = (msub["file"], msub["line"], tm_def[0], tm_def[1])
                if k not in seen_ov:
                    seen_ov.add(k)
                    out["overrides"].append({"sub": msub, "base": {"file": tm_def[0], "line": tm_def[1], "name": tm_def[2]},
                                             "sub_type": "%s:%d" % (sub["file"], sub["line"]), "base_type": name_of(trait_sym)})

        # ---- supertraits: `trait A: B + C`
        for o in d["occ"]:
            sym = o["symbol"]
            if not (o["roles"] & 1) or not o["range"] or kinds.get(sym) != KIND_TRAIT or tdefs.get(sym, (None,))[0] != d["path"]:
                continue
            l, c, _, _ = sc.rng(o)
            if a.no_tests and in_test(d["path"], l):
                continue
            text, offs = header_text(lines, l)
            offmap = dict(offs)
            start = offmap[l] + c + len(name_of(sym))
            after = skip_generics(text, start)
            rest = text[after:]
            if not rest.lstrip().startswith(":"):
                continue
            colon = after + rest.index(":")
            end = end_of_bounds(text, colon)
            header_lines = {ln for ln, _ in offs}
            for (ll, cc, bsym) in occs_in(d, header_lines):
                off = offmap[ll] + cc
                if colon < off < end and depth_at(text, off) == 0:
                    sub = decl(tdefs[sym])
                    if bsym in tdefs:
                        if kinds.get(bsym) == KIND_TRAIT and in_scope_type(bsym):
                            k = (sub["file"], sub["line"], tdefs[bsym][0], tdefs[bsym][1])
                            if k not in seen_ext:
                                seen_ext.add(k)
                                out["extends"].append({"sub": sub, "base": decl(tdefs[bsym])})
                    else:
                        external(sub, name_of(bsym), "extends")

    keyf = lambda x: (x["sub"]["file"], x["sub"]["line"], x["base"]["file"], x["base"]["line"])  # noqa: E731
    for k in ("implements", "overrides", "extends"):
        out[k].sort(key=keyf)
    out["external_base"].sort(key=lambda x: (x["sub"]["file"], x["sub"]["line"], x["base"], x["relation"]))
    for k in ("types", "ifaces"):
        out[k].sort(key=lambda x: (x["file"], x["line"]))
    json.dump(out, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
