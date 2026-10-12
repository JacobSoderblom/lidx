#!/usr/bin/env python3 -I
"""Turn a rust-analyzer SCIP index into the oracle JSON used by the matcher.

Usage: scip_calls.py --repo DIR --scip FILE > out.json

The SCIP protobuf is decoded with a tiny wire-format reader (stdlib only), so no
protobuf runtime is needed. A reference occurrence of a function/method symbol is
a *call* when the identifier is followed by `(` (optionally through a turbofish
`::<...>`); other references (function values) are emitted separately under
`refs` so they are never mixed with calls. The caller is the innermost function
definition whose enclosing range contains the occurrence.
"""
import argparse
import json
import os
import sys
from collections import defaultdict


def varint(b, i):
    r = s = 0
    while True:
        c = b[i]
        i += 1
        r |= (c & 0x7F) << s
        if c < 0x80:
            return r, i
        s += 7


def fields(b):
    """Yield (field_number, wire_type, value) for one message."""
    i, n = 0, len(b)
    while i < n:
        k, i = varint(b, i)
        f, w = k >> 3, k & 7
        if w == 0:
            v, i = varint(b, i)
        elif w == 2:
            l, i = varint(b, i)
            v = b[i:i + l]
            i += l
        elif w == 1:
            v = b[i:i + 8]
            i += 8
        elif w == 5:
            v = b[i:i + 4]
            i += 4
        else:
            raise ValueError("wire type %d" % w)
        yield f, w, v


def packed(b):
    out, i = [], 0
    while i < len(b):
        v, i = varint(b, i)
        out.append(v)
    return out


def parse_occurrence(b):
    o = {"range": [], "symbol": "", "roles": 0, "encl": []}
    for f, w, v in fields(b):
        if f == 1:
            o["range"] = packed(v) if w == 2 else o["range"] + [v]
        elif f == 2:
            o["symbol"] = v.decode()
        elif f == 3:
            o["roles"] = v
        elif f == 7:
            o["encl"] = packed(v) if w == 2 else o["encl"] + [v]
    return o


def parse_syminfo(b):
    s = {"symbol": "", "impls": []}
    for f, w, v in fields(b):
        if f == 1:
            s["symbol"] = v.decode()
        elif f == 4:
            rel = {"symbol": "", "impl": False}
            for f2, w2, v2 in fields(v):
                if f2 == 1:
                    rel["symbol"] = v2.decode()
                elif f2 == 3:
                    rel["impl"] = bool(v2)
            if rel["impl"]:
                s["impls"].append(rel["symbol"])
    return s


def parse_document(b):
    d = {"path": "", "occ": [], "sym": []}
    for f, w, v in fields(b):
        if f == 1:
            d["path"] = v.decode()
        elif f == 2:
            d["occ"].append(parse_occurrence(v))
        elif f == 3:
            d["sym"].append(parse_syminfo(v))
    return d


def rng(o):
    r = o["range"]
    if len(r) == 3:
        return r[0], r[1], r[0], r[2]
    return r[0], r[1], r[2], r[3]


def is_fn(sym):
    return sym.endswith("().")


def is_ctor(sym):
    """Tuple-struct (`Name#`) or enum-variant (`Enum#Variant.`) symbol: callable when followed by `(`."""
    return sym.endswith("#") or is_variant(sym)


def is_variant(sym):
    last = sym.rsplit("/", 1)[-1]
    return (sym.endswith(".") and "#" in last) or (sym.endswith("#") and "#" in last[:-1])


def ctor_name(sym):
    name = sym.rstrip("#.").rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    if name.startswith("["):
        name = name.split("]", 1)[-1]
    return name.strip("`")


def is_pattern(rest):
    """`rest` starts at the `(` of `Variant(...)`: true when the closing paren is followed by `=>`, `|`, `=`, `if` (a pattern)."""
    depth = 0
    for k, ch in enumerate(rest):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                tail = rest[k + 1:].lstrip()
                return tail.startswith(("=>", "|", "if ", "@")) or (tail.startswith("=") and not tail.startswith("=="))
    return False


def is_trait_method(sym):
    """`.../Trait#method().` is declared on a trait; `.../impl#[Type][Trait]method().` is a concrete implementation."""
    last = sym.rsplit("/", 1)[-1]
    return "#" in last and not last.startswith("impl#")


def short_name(sym):
    # ".../Type#method()." -> method
    body = sym[:-3]
    name = body
    for sep in ("#", "/"):
        name = name.rsplit(sep, 1)[-1]
    if name.startswith("["):
        name = name.split("]", 1)[-1]
    return name.strip("`")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--scip", required=True)
    a = ap.parse_args()
    repo = os.path.realpath(a.repo)
    data = open(a.scip, "rb").read()
    docs = []
    for f, w, v in fields(data):
        if f == 2:
            docs.append(parse_document(v))

    defs = {}  # symbol -> (file, line, name)
    ctors = {}  # tuple-struct / variant symbol -> same
    impls_of = defaultdict(list)  # trait method symbol -> impl method symbols
    for d in docs:
        for s in d["sym"]:
            for t in s["impls"]:
                impls_of[t].append(s["symbol"])
        for o in d["occ"]:
            if o["roles"] & 1 and is_fn(o["symbol"]) and o["range"]:
                l, c, _, _ = rng(o)
                defs.setdefault(o["symbol"], (d["path"], l + 1, short_name(o["symbol"])))
            elif o["roles"] & 1 and is_ctor(o["symbol"]) and o["range"]:
                l, c, _, _ = rng(o)
                ctors.setdefault(o["symbol"], (d["path"], l + 1, ctor_name(o["symbol"])))

    out = {"repo": repo, "files": [], "callers": [], "edges": [], "refs": [], "dropped": defaultdict(int), "unresolved_sites": [], "ext_sites": []}
    ext = set()
    seen, seen_ref = {}, {}
    callers = set()
    for d in docs:
        path = d["path"]
        if not path.endswith(".rs"):
            continue
        out["files"].append(path)
        try:
            lines = open(os.path.join(repo, path), encoding="utf-8", errors="replace").read().split("\n")
        except OSError:
            continue
        fdefs = []  # (start, end, symbol)
        for o in d["occ"]:
            if o["roles"] & 1 and is_fn(o["symbol"]) and o["range"]:
                e = o["encl"] or list(rng(o))
                if len(e) == 3:
                    e = [e[0], e[1], e[0], e[2]]
                fdefs.append(((e[0], e[1]), (e[2], e[3]), o["symbol"]))
                dl = defs[o["symbol"]]
                callers.add((dl[0], dl[1], dl[2]))
        for o in d["occ"]:
            sym = o["symbol"]
            ctor = is_ctor(sym) and sym in ctors
            if o["roles"] & 1 or not (is_fn(sym) or ctor) or not o["range"]:
                continue
            l, c, el, ec = rng(o)
            tgt = defs.get(sym) or ctors.get(sym)
            if tgt is None:
                out["dropped"]["callee_external"] += 1
                ext.add("%s:%d" % (path, rng(o)[0] + 1))
                continue
            # the innermost enclosing function definition
            best = None
            for s, e, fs in fdefs:
                if s <= (l, c) <= e and (best is None or s >= best[0]):
                    best = (s, e, fs)
            if best is None:
                out["dropped"]["caller_not_in_function"] += 1
                continue
            cdef = defs[best[2]]
            row = lines[el] if el < len(lines) else ""
            rest = row[ec:].lstrip()
            if rest.startswith("::<"):
                depth, k = 0, 0
                for k, ch in enumerate(rest):
                    if ch == "<":
                        depth += 1
                    elif ch == ">":
                        depth -= 1
                        if depth == 0:
                            break
                rest = rest[k + 1:].lstrip()
            is_call = rest.startswith("(")
            if ctor:
                if not is_call or is_pattern(rest):
                    continue  # a type mention or a pattern, not a call
                if is_variant(sym):
                    # enum variants are not declarations lidx models: the site is not judgeable either way
                    out["unresolved_sites"].append("%s:%d" % (path, l + 1))
                    continue
            kind_edges = out["edges"] if is_call else out["refs"]
            seen_set = seen if is_call else seen_ref
            key = (cdef[:2], tgt[:2])
            site = "%s:%d" % (path, l + 1)
            if key in seen_set:
                if len(seen_set[key]["sites"]) < 20:
                    seen_set[key]["sites"].append(site)
                continue
            e = {"caller": {"file": cdef[0], "line": cdef[1], "name": cdef[2]},
                 "callee": {"file": tgt[0], "line": tgt[1], "name": tgt[2]},
                 "sites": [site]}
            seen_set[key] = e
            alts = sorted({"%s:%d" % defs[i][:2] for i in impls_of.get(sym, []) if i in defs})
            if alts:
                e["callee_alts"] = alts
            if is_trait_method(sym):
                e["dispatch"] = True  # declared on a trait: the receiver decides which implementation runs
            kind_edges.append(e)
    out["callers"] = [{"file": f, "line": l, "name": n} for f, l, n in sorted(callers)]
    out["ext_sites"] = sorted(ext)
    out["dropped"] = dict(out["dropped"])
    out["files"].sort()
    out["edges"].sort(key=lambda e: (e["caller"]["file"], e["caller"]["line"], e["callee"]["file"], e["callee"]["line"]))
    json.dump(out, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
