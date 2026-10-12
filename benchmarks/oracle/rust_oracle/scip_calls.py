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
import re
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


def desc(sym):
    """The descriptor part of a SCIP symbol: `scheme manager package version <descriptors>`."""
    return sym.split(" ", 4)[-1]


def last_segment(sym):
    return desc(sym).rsplit("/", 1)[-1]


def is_fn(sym):
    return sym.endswith("().")


def is_ctor(sym):
    """Tuple-struct (`Name#`) or enum-variant (`Enum#Variant.`) symbol: callable when followed by `(`."""
    return sym.endswith("#") or is_variant(sym)


def is_variant(sym):
    last = last_segment(sym)
    return (sym.endswith(".") and "#" in last) or (sym.endswith("#") and "#" in last[:-1])


def ctor_name(sym):
    name = desc(sym).rstrip("#.").rsplit("#", 1)[-1].rsplit("/", 1)[-1]
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
    """A function symbol declared on a trait (`.../Trait#method().`), as opposed to an inherent method
    (`impl#[Type]method().`), a trait implementation (`impl#[Type][Trait]method().`), a free function, or a
    tuple-struct / variant constructor (those end in `#`, not `().`). A call to a trait-declared method is
    resolved by the receiver, so the compiler names the declaration, not the implementation that runs."""
    if not sym.endswith("()."):
        return False
    last = last_segment(sym)
    return "#" in last and not last.startswith("impl#")


IMPL_RE = re.compile(r"^impl#\[(.+?)\]\[(.+?)\]([^\[\]#/]+)\(\)\.$")
TRAIT_RE = re.compile(r"^(?:.*#)?([^#\[\]]+)#([^#\[\]]+)\(\)\.$")


def trait_impl_index(symbols):
    """Map (trait name, method) -> implementing method symbols, from `impl#[Type][Trait]method().` symbols.
    rust-analyzer's SCIP carries no implementation relationships, so the impl blocks themselves are the source."""
    idx = defaultdict(list)
    for sym in symbols:
        m = IMPL_RE.match(last_segment(sym))
        if m:
            trait = m.group(2).strip("`").split("<")[0].split("::")[-1]
            idx[(trait, m.group(3).strip("`"))].append(sym)
    return idx


def trait_key(sym):
    m = TRAIT_RE.match(last_segment(sym))
    return (m.group(1).strip("`"), m.group(2).strip("`")) if m else None


# --- test code ---------------------------------------------------------------
CFG_TEST_RE = re.compile(r"#\[cfg\((?:all\()?\s*test\b")
STRIP_RE = re.compile(r"//[^\n]*|/\*.*?\*/|r(#*)\".*?\"\1|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])'", re.S)


def is_test_path(path):
    parts = path.split("/")
    name = parts[-1]
    return "tests" in parts[:-1] or name in ("tests.rs", "test.rs") or name.endswith("_tests.rs")


ITEM_RE = re.compile(r"^(?:pub\s*(?:\([^)]*\))?\s*)?(?:(?:unsafe|async|const|extern(?:\s+\"[^\"]*\")?)\s+)*(fn|mod|impl|trait|struct|enum|union|static|type|use|const)\b")


def _skip_attrs(clean, i):
    """Index after any further `#[...]` attributes starting at or after i."""
    while True:
        j = i
        while j < len(clean) and clean[j].isspace():
            j += 1
        if clean.startswith("#[", j) or clean.startswith("#![", j):
            depth, k = 0, clean.index("[", j)
            while k < len(clean):
                depth += clean[k] == "["
                depth -= clean[k] == "]"
                if depth == 0:
                    break
                k += 1
            i = k + 1
        else:
            return j


def _match_brace(clean, brace):
    depth, j = 0, brace
    while j < len(clean):
        if clean[j] == "{":
            depth += 1
        elif clean[j] == "}":
            depth -= 1
            if depth == 0:
                return j
        j += 1
    return len(clean) - 1


def _item_end(clean, i):
    """Offset of the last character of the item / field / statement that starts at i."""
    m = ITEM_RE.match(clean[i:i + 200])
    block_kinds = {"fn", "mod", "impl", "trait", "struct", "enum", "union"}
    depth, j = 0, i
    while j < len(clean):
        ch = clean[j]
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif depth == 0:
            if ch == "{":
                if m and m.group(1) in block_kinds or not m and j == i:
                    return _match_brace(clean, j)
                if m:  # const / static / type / use with a braced initialiser
                    j = _match_brace(clean, j)
            elif ch == ";":
                return j
            elif ch == "," and not (m and m.group(1) in block_kinds):
                return j
            elif ch == "}":
                return j - 1  # the enclosing block ends: a trailing field / expression without a comma
        j += 1
    return len(clean) - 1


def test_regions(text):
    """0-based (first_line, last_line) ranges of the single item (module, fn, impl, struct field, statement) that
    follows each `#[cfg(test)]`."""
    clean = STRIP_RE.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), text)
    regions = []
    for m in CFG_TEST_RE.finditer(clean):
        close = clean.find("]", m.end())
        start = _skip_attrs(clean, close + 1)
        last = _item_end(clean, start)
        regions.append((clean.count("\n", 0, m.start()), clean.count("\n", 0, last)))
    return regions



def short_name(sym):
    # ".../Type#method()." -> method
    body = desc(sym)[:-3]
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
    ap.add_argument("--no-tests", action="store_true", help="drop tests/ directories, tests.rs files and #[cfg(test)] items")
    a = ap.parse_args()
    repo = os.path.realpath(a.repo)
    data = open(a.scip, "rb").read()
    docs = []
    for f, w, v in fields(data):
        if f == 2:
            docs.append(parse_document(v))

    defs = {}  # symbol -> (file, line, name)
    ctors = {}  # tuple-struct / variant symbol -> same
    regions = {}  # path -> test regions (--no-tests)
    texts = {}
    if a.no_tests:
        kept = []
        for d in docs:
            if d["path"].endswith(".rs") and is_test_path(d["path"]):
                continue
            kept.append(d)
            try:
                regions[d["path"]] = test_regions(open(os.path.join(repo, d["path"]), encoding="utf-8", errors="replace").read())
            except OSError:
                regions[d["path"]] = []
        docs = kept

    def in_test(path, line0):
        return any(a0 <= line0 <= b0 for a0, b0 in regions.get(path, ()))

    for d in docs:
        for o in d["occ"]:
            if a.no_tests and o["range"] and in_test(d["path"], rng(o)[0]):
                continue
            if o["roles"] & 1 and is_fn(o["symbol"]) and o["range"]:
                l, c, _, _ = rng(o)
                defs.setdefault(o["symbol"], (d["path"], l + 1, short_name(o["symbol"])))
            elif o["roles"] & 1 and is_ctor(o["symbol"]) and o["range"]:
                l, c, _, _ = rng(o)
                ctors.setdefault(o["symbol"], (d["path"], l + 1, ctor_name(o["symbol"])))

    impl_idx = trait_impl_index(defs)
    out = {"repo": repo, "tests": not a.no_tests, "files": [], "callers": [], "edges": [], "refs": [], "dropped": defaultdict(int), "unresolved_sites": [], "ext_sites": []}
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
            if a.no_tests and o["range"] and in_test(path, rng(o)[0]):
                continue
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
            if a.no_tests and in_test(path, rng(o)[0]):
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
                    # an enum variant is not a declaration lidx models; the compiler says this site constructs a variant,
                    # so a lidx edge to any in-repo symbol here is a wrong edge (reported as callee-not-a-declaration)
                    out["dropped"]["callee_variant"] += 1
                    ext.add("%s:%d" % (path, l + 1))
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
            if is_trait_method(sym):
                # declared on a trait: the receiver decides which implementation runs; any implementation in the
                # repository is an acceptable target for a graph that resolves the receiver
                e["dispatch"] = True
                tk = trait_key(sym)
                alts = sorted({"%s:%d" % defs[i][:2] for i in impl_idx.get(tk, []) if i in defs})
                if alts:
                    e["callee_alts"] = alts
            kind_edges.append(e)
    out["callers"] = [{"file": f, "line": l, "name": n} for f, l, n in sorted(callers)]
    out["ext_sites"] = sorted(ext)
    out["dropped"] = dict(out["dropped"])
    out["files"].sort()
    out["edges"].sort(key=lambda e: (e["caller"]["file"], e["caller"]["line"], e["callee"]["file"], e["callee"]["line"]))
    json.dump(out, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
