#!/usr/bin/env python3 -I
"""Dead-code oracle from a rust-analyzer SCIP index.

Usage: scip_dead.py --repo DIR --scip FILE [--no-tests | --path-tests-excluded] > out.json

rustc's `dead_code` lint would be the natural oracle, but it needs `cargo check`, which runs build scripts and proc macros:
refused by the sandbox (safety.py). This reads the SCIP index the sandboxed `rust-analyzer scip` already produced, so it is
reference-based, not a compiler lint. Macro-generated items have no source declaration and are left out.

Per declaration (function, method, type) two verdicts:

  unref  "dead" when no occurrence of the symbol (or of one of its fields / variants) outside the item's own text, `use` lines
         and the impl blocks of the type itself refers to it; for a trait-impl method the verdict comes from the trait method
         (a method of an external trait is "live": the trait's callers are not in the repository).
  reach  functions and methods: "live" when a root reaches it through references (calls, callbacks, any mention). Roots: `main`
         of binary crates and build scripts, test functions, `pub` items of library crates, FFI / proc-macro entry points.
         An impl method of an in-repo trait is reached when the trait method is; one of an external trait when its type is.

Library crates (a `lib` target) have public API: their `pub` items get no verdict (`api`). The `pub` items of binary crates
(`main.rs`, `src/bin`, `[[bin]]` paths, examples, build scripts) stay in the universe. Crate kinds are read from the
Cargo.toml files (data only; nothing is run).
"""
import argparse
import bisect
import json
import os
import re
import sys
import tomllib
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scip_calls as sc  # noqa: E402  (same decoder and test-path rules as the call-graph oracle)

VIS_RE = re.compile(r"\bpub\b\s*(\([^)]*\))?")
TOP = "<module-level code>"
USE_RE = re.compile(r"(?m)^[ \t]*(?:pub(?:[ \t]*\([^)]*\))?[ \t]+)?use\b")
IMPL_START = re.compile(r"(?m)^[ \t]*(?:unsafe[ \t]+)?impl\b")
TEST_ATTR = re.compile(r"#\[\s*(?:[\w:]+::)?(test|bench|rstest|test_case|wasm_bindgen_test)\b")
ENTRY_ATTR = re.compile(r"#\[\s*(?:unsafe\s*\(\s*)?(no_mangle|export_name|proc_macro|proc_macro_derive|proc_macro_attribute)\b")


# ---------------------------------------------------------------- crates (Cargo.toml, read as data)
def read_manifests(repo):
    pkgs = []
    for root, dirs, files in os.walk(repo):
        dirs[:] = [d for d in dirs if d not in (".git", "target", "node_modules")]
        if "Cargo.toml" not in files:
            continue
        try:
            with open(os.path.join(root, "Cargo.toml"), "rb") as f:
                m = tomllib.load(f)
        except (OSError, tomllib.TOMLDecodeError):
            continue
        p = m.get("package")
        if not p:
            continue
        rel = os.path.relpath(root, repo)
        rel = "" if rel == "." else rel.replace(os.sep, "/")
        lib = m.get("lib")
        has_lib = lib is not None or os.path.exists(os.path.join(root, "src", "lib.rs"))
        if lib is not None and lib.get("path") is None and not os.path.exists(os.path.join(root, "src", "lib.rs")):
            has_lib = True
        bins = [b.get("path") or "src/bin/%s.rs" % b.get("name") for b in m.get("bin", [])]
        if os.path.exists(os.path.join(root, "src", "main.rs")) and "src/main.rs" not in bins:
            bins.append("src/main.rs")
        bd = os.path.join(root, "src", "bin")
        if os.path.isdir(bd):
            for e in sorted(os.listdir(bd)):
                if e.endswith(".rs"):
                    bins.append("src/bin/" + e)
                elif os.path.exists(os.path.join(bd, e, "main.rs")):
                    bins.append("src/bin/%s/main.rs" % e)
        build = p.get("build")
        pkgs.append({"name": p.get("name", os.path.basename(root)), "dir": rel, "lib": has_lib, "proc_macro": bool((lib or {}).get("proc-macro")),
                     "lib_path": (lib or {}).get("path") or "src/lib.rs", "bins": sorted(set(bins)),
                     "build": ("build.rs" if os.path.exists(os.path.join(root, "build.rs")) else None) if build is None or build is True else (build or None)})
    return sorted(pkgs, key=lambda x: -len(x["dir"]))


def crate_of(pkgs, path):
    """(crate name, kind) of a repository-relative file. kind: lib, proc_macro, bin, example, test, build, other."""
    for p in pkgs:
        d = p["dir"]
        if d and not (path == d or path.startswith(d + "/")):
            continue
        rel = path[len(d) + 1:] if d else path
        if rel.startswith(("tests/", "benches/")):
            return p["name"], "test"
        if rel.startswith("examples/"):
            return p["name"], "example"
        if p["build"] and rel == p["build"]:
            return p["name"], "build"
        for b in p["bins"]:
            bdir = os.path.dirname(b)
            if b == "src/main.rs" and p["lib"]:
                if rel == b:
                    return p["name"], "bin"
            elif b.startswith("src/bin/") and b.endswith(".rs") and os.path.basename(b) != "main.rs":
                if rel == b:
                    return p["name"], "bin"
            elif rel == b or rel.startswith(bdir + "/"):
                return p["name"], "bin"
        if p["lib"]:
            return p["name"], "proc_macro" if p["proc_macro"] else "lib"
        return p["name"], "other"
    return "", "other"


# ---------------------------------------------------------------- helpers
def strip_text(text):
    return sc.STRIP_RE.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), text)


def desc_parts(sym):
    d = sc.desc(sym)
    mod, _, last = d.rpartition("/")
    return mod + "/" if mod else "", last


def classify(sym):
    """(kind, subkind) for a definition symbol, or None for locals, fields, variants, modules, consts, associated types."""
    if sym.startswith("local "):
        return None
    mod, last = desc_parts(sym)
    if sym.endswith("()."):
        if last.startswith("impl#"):
            return ("method", "trait_impl" if sc.IMPL_RE.match(last) else "inherent")
        if "#" in last:
            return ("method", "trait_method")
        return ("function", "fn")
    if sym.endswith("#"):
        if last.startswith("impl#") or last.count("#") > 1:
            return None
        return ("type", "")
    return None


def is_api(exported, crate_kind, root=None):
    """Public API: a `pub` item of a library (or proc-macro) crate. A `pub` item of a binary crate, an example, a test or a build
    script is not: nothing outside the crate can name it."""
    return bool(exported and crate_kind in ("lib", "proc_macro") and root is None)


def type_prefixes(sym):
    """Prefixes of a symbol that name a type: `a/T#field.` -> [`a/T#`]; `a/T#V#` -> [`a/T#`, `a/T#V#`]."""
    mod, last = desc_parts(sym)
    pre = sym[:len(sym) - len(last)]
    out, i = [], 0
    while True:
        j = last.find("#", i)
        if j < 0:
            break
        if not last.startswith("impl#"):
            out.append(pre + last[:j + 1])
        i = j + 1
    return out


def first_ident(part):
    """Base identifier of a type / trait path in an impl header: `&'a mut a::B<T>` -> B."""
    part = re.sub(r"\bwhere\b.*", "", part, flags=re.S).strip()
    part = re.sub(r"^(&\s*('\w+\s*)?(mut\s+)?|dyn\s+|\*\s*(const|mut)\s+)", "", part)
    m = re.match(r"((?:[A-Za-z_]\w*::)*)([A-Za-z_]\w*)", part)
    return m.group(2) if m else None


def split_impl_header(h):
    """h: text from `impl` to the opening brace. Returns (trait_ident or None, self_ident or None)."""
    h = re.sub(r"^\s*(unsafe\s+)?impl\b", "", h).strip()
    if h.startswith("<"):
        depth = 0
        for i, ch in enumerate(h):
            depth += ch == "<"
            depth -= ch == ">" and h[i - 1] != "-"
            if depth == 0:
                h = h[i + 1:].strip()
                break
    depth, cut = 0, None
    for m in re.finditer(r"<|>|\bfor\b", h):
        t = m.group(0)
        if t == "<":
            depth += 1
        elif t == ">":
            depth -= 1
        elif depth == 0:
            cut = m.start()
            break
    if cut is None:
        return None, first_ident(h)
    return first_ident(h[:cut].lstrip("!")), first_ident(h[cut + 3:])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--scip", required=True)
    ap.add_argument("--no-tests", action="store_true")
    ap.add_argument("--path-tests-excluded", action="store_true")
    a = ap.parse_args()
    repo = os.path.realpath(a.repo)
    data = open(a.scip, "rb").read()
    docs = [sc.parse_document(v) for f, w, v in sc.fields(data) if f == 2]
    docs = [d for d in docs if d["path"].endswith(".rs")]

    # ---- the cell's scope: the same rules as scip_calls.py (a copy of its document filter) ----------------------------
    texts = {}

    def text(path):
        if path not in texts:
            try:
                texts[path] = open(os.path.join(repo, path), encoding="utf-8", errors="replace").read()
            except OSError:
                texts[path] = ""
        return texts[path]

    regions = {}
    if a.path_tests_excluded:
        docs = [d for d in docs if not sc.is_path_test(d["path"])]
    if a.no_tests:
        kept, gated_files, gated_dirs = [], set(), []
        for d in docs:
            if sc.is_test_path(d["path"]):
                continue
            kept.append(d)
            regions[d["path"]] = sc.test_regions(text(d["path"]))
            for name in sc.gated_modules(text(d["path"])):
                fs, dr = sc.module_files(d["path"], name)
                gated_files |= fs
                gated_dirs.append(dr)
        docs = [d for d in kept if d["path"] not in gated_files and not d["path"].startswith(tuple(gated_dirs))]

    def in_test_region(path, l0):
        return any(a0 <= l0 <= b0 for a0, b0 in regions.get(path, ()))

    pkgs = read_manifests(repo)
    clean = {}

    def cleaned(path):
        if path not in clean:
            clean[path] = strip_text(text(path))
        return clean[path]

    def lines_of(path):
        return text(path).split("\n")

    # ---- definitions ------------------------------------------------------------------------------------------------
    defs = {}  # symbol -> record
    owners = []  # (file, encl range, symbol) for every non-local, non-module definition, for attributing references
    owners_by_file = defaultdict(list)
    for d in docs:
        path = d["path"]
        for o in d["occ"]:
            if not (o["roles"] & 1) or not o["range"] or o["symbol"].startswith("local "):
                continue
            if a.no_tests and in_test_region(path, sc.rng(o)[0]):
                continue
            sym = o["symbol"]
            l0, c0, l1, c1 = sc.rng(o)
            e = o["encl"] or [l0, c0, l1, c1]
            if len(e) == 3:
                e = [e[0], e[1], e[0], e[2]]
            if not sym.endswith("/"):
                owners_by_file[path].append(((e[0], e[1]), (e[2], e[3]), sym))
            cl = classify(sym)
            if cl is None or sym in defs:
                continue
            defs[sym] = {"symbol": sym, "file": path, "line": l0 + 1, "col": c0, "ecol": c1, "encl": e, "kind": cl[0], "subkind": cl[1],
                         "name": re.sub(r"^(?:\[[^\]]*\])+", "", sc.short_name(sym)) if cl[0] != "type" else sc.ctor_name(sym)}

    # ---- classify each definition from its source line ------------------------------------------------------------------
    for r in defs.values():
        ls = lines_of(r["file"])
        row = ls[r["line"] - 1] if 0 < r["line"] <= len(ls) else ""
        prefix = row[:r["col"]]
        r["generated_decl"] = False
        if r["kind"] == "type":
            mm = re.search(r"\b(struct|enum|union|trait|type)\s+(?:r#)?$", prefix)
            m = mm.group(1) if mm else None
            r["subkind"] = {"type": "alias"}.get(m, m) if m else ""
            if not m:
                r["macro_generated"] = True
        else:
            if not re.search(r"\bfn\s+(?:r#)?$", prefix):
                r["macro_generated"] = True
        # attributes and visibility live between the start of the enclosing range and the name
        e = r["encl"]
        head = "\n".join(ls[e[0]:r["line"]])[:]
        head_clean = re.sub(r"///.*|//!.*", "", head)
        r["attrs"] = head_clean
        vis = None
        m = VIS_RE.search(prefix) or None
        if m:
            vis = "pub" if not m.group(1) else "pub" + m.group(1).replace(" ", "")
        r["vis"] = vis

    # ---- impl blocks and `use` lines (excluded from "references") ------------------------------------------------------
    occs_by_file = defaultdict(list)  # path -> [(l0, c0, ec, sym)] non-definition occurrences
    for d in docs:
        for o in d["occ"]:
            if o["roles"] & 1 or not o["range"] or o["symbol"].startswith("local "):
                continue
            l0, c0, l1, c1 = sc.rng(o)
            occs_by_file[d["path"]].append((l0, c0, c1 if l1 == l0 else c0, o["symbol"]))
    use_ranges = defaultdict(list)
    impls = defaultdict(list)  # path -> [(h0, h1, end, self_sym, trait_sym)]
    for d in docs:
        path = d["path"]
        cl = cleaned(path)
        for m in USE_RE.finditer(cl):
            end = cl.find(";", m.end())
            if end < 0:
                continue
            use_ranges[path].append((cl.count("\n", 0, m.start()), cl.count("\n", 0, end)))
        ls = lines_of(path)
        for m in IMPL_START.finditer(cl):
            br = cl.find("{", m.end())
            if br < 0:
                continue
            semi = cl.find(";", m.end())
            if 0 <= semi < br:
                continue
            end = sc._match_brace(cl, br)
            h0, h1, e1 = cl.count("\n", 0, m.start()), cl.count("\n", 0, br), cl.count("\n", 0, end)
            trait_id, self_id = split_impl_header(cl[m.start():br])
            self_sym = trait_sym = None
            for (l0, c0, ec, sym) in occs_by_file[path]:
                if h0 <= l0 <= h1 and sym.endswith("#") and not sym.startswith("local "):
                    tok = ls[l0][c0:ec] if l0 < len(ls) else ""
                    if self_id and tok == self_id:
                        self_sym = sym
                    if trait_id and tok == trait_id and trait_sym is None:
                        trait_sym = sym
            impls[path].append((h0, h1, e1, self_sym, trait_sym))

    def in_ranges(rs, l0):
        return any(a0 <= l0 <= b0 for a0, b0 in rs)

    # ---- references per target ---------------------------------------------------------------------------------------------
    refs = defaultdict(list)  # target symbol (and each type prefix of it) -> [(path, l0, c0)]
    for path, lst in occs_by_file.items():
        for (l0, c0, ec, sym) in lst:
            if in_ranges(use_ranges[path], l0):
                continue
            targets = [sym] + type_prefixes(sym)
            for t in set(targets):
                refs[t].append((path, l0, c0))

    def count_refs(r, extra_syms=()):
        sym = r["symbol"]
        e = r["encl"]
        n = 0
        for t in (sym,) + tuple(extra_syms):
            for (path, l0, c0) in refs.get(t, ()):
                if path == r["file"] and ((e[0], e[1]) <= (l0, c0) <= (e[2], e[3])):
                    continue  # inside the item itself: recursion, own signature
                skip = False
                for (h0, h1, e1, self_sym, trait_sym) in impls[path]:
                    if self_sym == t and h0 <= l0 <= e1:
                        skip = True  # the type's own impl block
                    elif trait_sym == t and h0 <= l0 <= h1:
                        skip = True  # `impl Trait for X`: implementing a trait is not using it
                    if skip:
                        break
                if not skip:
                    n += 1
        return n

    # ---- trait relations ---------------------------------------------------------------------------------------------------
    traits_by_name = defaultdict(list)
    for r in defs.values():
        if r["kind"] == "type" and r["subkind"] == "trait":
            traits_by_name[r["name"]].append(r)
    trait_methods = {}  # (trait symbol, method name) -> def record
    for r in defs.values():
        if r["subkind"] == "trait_method":
            mod, last = desc_parts(r["symbol"])
            tsym = r["symbol"][:len(r["symbol"]) - len(last)] + last[:last.index("#") + 1]
            r["trait_symbol"] = tsym
            trait_methods[(tsym, r["name"])] = r
    type_by_name = defaultdict(list)
    for r in defs.values():
        if r["kind"] == "type":
            type_by_name[r["name"]].append(r)
    for r in defs.values():
        if r["subkind"] != "trait_impl":
            continue
        mod, last = desc_parts(r["symbol"])
        m = sc.IMPL_RE.match(last)
        tname = m.group(2).strip("`").split("<")[0].split("::")[-1]
        ty = m.group(1).strip("`").split("<")[0].split("::")[-1].lstrip("&").strip()
        r["recv"] = ty
        r["trait_name"] = tname
        cands = [t for t in traits_by_name.get(tname, [])]
        tm = None
        for t in cands:
            tm = trait_methods.get((t["symbol"], r["name"]))
            if tm:
                break
        r["trait_method"] = tm["symbol"] if tm else None  # None: the trait is not defined in the repository
        pre = r["symbol"][:len(r["symbol"]) - len(last)]
        tys = [t for t in type_by_name.get(ty, []) if t["subkind"] in ("struct", "enum", "union", "alias")]
        same = [t for t in tys if t["symbol"].startswith(pre)]
        r["type_symbol"] = (same or tys or [{"symbol": None}])[0]["symbol"]
    for r in defs.values():
        if r["subkind"] == "inherent":
            mod, last = desc_parts(r["symbol"])
            m = re.match(r"impl#\[(.+?)\]", last)
            r["recv"] = m.group(1).strip("`").split("<")[0].split("::")[-1] if m else ""
        elif r["subkind"] == "trait_method":
            r["recv"] = desc_parts(r["symbol"])[1].split("#")[0]

    # ---- crates, visibility, roots --------------------------------------------------------------------------------------------
    out_decls = []
    kinds_seen = defaultdict(int)
    for r in defs.values():
        crate, ckind = crate_of(pkgs, r["file"])
        r["crate"], r["crate_kind"] = crate, ckind
        vis = r["vis"]
        if r["subkind"] == "trait_method":
            t = defs.get(r.get("trait_symbol"))
            vis = t["vis"] if t else None
        r["exported"] = vis == "pub"
        kinds_seen[ckind] += 1
        text_head = r["attrs"]
        root = None
        if r.get("macro_generated"):
            root = "macro_generated"
        elif TEST_ATTR.search(text_head) or (r["kind"] == "function" and r["file"].split("/")[0:1] == ["benches"]) or "/benches/" in "/" + r["file"]:
            root = "test_root"
        elif r["kind"] == "function" and r["name"] == "main" and ckind in ("bin", "example", "build", "other", "test"):
            root = "main_init"
        elif ENTRY_ATTR.search(text_head) or re.search(r'\bextern\s+"C"', text_head):
            root = "ffi_or_macro_entry"
        r["root"] = root
        r["api"] = is_api(r["exported"], ckind, root)
    # a trait impl of a public in-repo trait in a library is API too (callers outside the repository reach it through the trait)
    for r in defs.values():
        if r["subkind"] == "trait_impl" and r["trait_method"]:
            tm = defs[r["trait_method"]]
            r["api"] = is_api(tm["api"], r["crate_kind"], r["root"])

    # ---- unreferenced ------------------------------------------------------------------------------------------------------------
    impls_of_trait_method = defaultdict(list)
    for r in defs.values():
        if r["subkind"] == "trait_impl" and r["trait_method"]:
            impls_of_trait_method[r["trait_method"]].append(r)
    for r in defs.values():
        sym = r["symbol"]
        if r["subkind"] == "trait_impl":
            if r["trait_method"] is None:
                r["unref"], r["unref_how"] = "live", "external_trait_impl"
                continue
            n = count_refs(r) + count_refs(defs[r["trait_method"]])
        elif r["subkind"] == "trait_method":
            n = count_refs(r, extra_syms=[i["symbol"] for i in impls_of_trait_method.get(sym, [])])
        else:
            n = count_refs(r)
        r["refs"] = n
        r["unref"] = "dead" if n == 0 else "live"
        r["unref_how"] = "scip_references"

    # ---- unresolved use sites ------------------------------------------------------------------------------------------------
    # A token spelled like a declaration that rust-analyzer attached no symbol to cannot be judged: it may be a use the analysis
    # could not see (the sandbox runs no proc macros, so `#[arg(value_parser = parse_millis)]` and the calls of derive-generated
    # types are never expanded; macro_rules bodies are templates). `X::name` where X resolved to an item outside the repository
    # is an external call and does not count. A declaration with such a token gets no verdict (`withheld`) in the column where
    # it would be dead; lidx claims on it are ungradable. A token that is itself a declaration (`fn name`) in code rust-analyzer
    # did not analyse is a `cfg_twin` (the same item under another configuration), also withheld.
    names = defaultdict(list)
    for r in defs.values():
        if not r.get("macro_generated"):
            names[r["name"]].append(r)
    repo_crates = {p["name"].replace("-", "_") for p in pkgs} | {p["name"] for p in pkgs}
    occ_sym = defaultdict(dict)
    for d in docs:
        for o in d["occ"]:
            if o["range"]:
                l0, c0, l1, c1 = sc.rng(o)
                occ_sym[d["path"]][(l0, c0)] = o["symbol"]
    tok = re.compile(r"(?<![\w'])(?:r#)?([A-Za-z_]\w*)")
    DEF_KW = re.compile(r"\b(fn|struct|enum|union|trait|type)\s+(?:r#)?$")
    doubt = defaultdict(lambda: {"unresolved_site": None, "cfg_twin": None})
    for d in docs:
        path = d["path"]
        cl = cleaned(path)
        line_starts = [0]
        for m in re.finditer("\n", cl):
            line_starts.append(m.end())
        osym = occ_sym[path]
        raw = text(path)
        att = list(re.finditer(r"#!?\[", cl))
        regions = []
        for am in att:
            depth, k = 0, am.end() - 1
            while k < len(cl):
                depth += cl[k] == "["
                depth -= cl[k] == "]"
                if depth == 0:
                    break
                k += 1
            regions.append((am.start(), k))
        # code outside strings and comments, plus everything inside attributes (`default = "name"`, `value_parser = name`)
        view = list(cl)
        for a0, b0 in regions:
            view[a0:b0 + 1] = list(raw[a0:b0 + 1])
        view = "".join(view)
        for m in tok.finditer(view):
            nm = m.group(1)
            if nm not in names:
                continue
            ln = bisect.bisect_right(line_starts, m.start()) - 1
            col = m.start(1) - line_starts[ln]
            if (ln, col) in osym:
                continue
            if in_ranges(use_ranges[path], ln):
                continue
            before = cl[line_starts[ln]:m.start()]
            site = "%s:%d" % (path, ln + 1)
            if DEF_KW.search(before):
                doubt[nm]["cfg_twin"] = doubt[nm]["cfg_twin"] or site
                continue
            q = re.search(r"([A-Za-z_]\w*)\s*::\s*$", before)
            if q:
                qsym = osym.get((ln, q.start(1)))
                if qsym and not qsym.startswith("local ") and qsym.split(" ")[2] not in repo_crates:
                    continue
            doubt[nm]["unresolved_site"] = doubt[nm]["unresolved_site"] or site

    def withhold(r):
        dd = doubt.get(r["name"])
        if not dd:
            return None
        if dd["unresolved_site"]:
            return "oracle_unresolved_site"
        if dd["cfg_twin"]:
            return "cfg_twin"
        return None

    for r in defs.values():
        if r["unref"] == "dead" and not r.get("macro_generated") and withhold(r):
            r["withheld"] = withhold(r)
            r["unref"], r["unref_how"] = "", r["withheld"]

    # ---- reachability ----------------------------------------------------------------------------------------------------------------
    edges = defaultdict(set)
    for path, lst in occs_by_file.items():
        own = sorted(owners_by_file[path], key=lambda x: (x[0], [-v for v in x[1]]))
        starts = [x[0] for x in own]
        for (l0, c0, ec, sym) in lst:
            if in_ranges(use_ranges[path], l0):
                continue
            best = None
            for i in range(bisect.bisect_right(starts, (l0, c0)) - 1, -1, -1):
                s_, e_, osym = own[i]
                if (l0, c0) <= e_:
                    best = (s_, e_, osym)
                    break
            if best is None:
                # module-level code (a macro invocation that registers tests or benches, a static initialiser): runs with the module
                if any(h0 <= l0 <= h1 for (h0, h1, _e1, _a, _b) in impls[path]):
                    continue
                best = (None, None, TOP)
            if best[2] == sym:
                continue
            edges[best[2]].add(sym)
            for p in type_prefixes(sym):
                edges[best[2]].add(p)
    for r in defs.values():
        if r["subkind"] == "trait_impl":
            if r["trait_method"]:
                edges[r["trait_method"]].add(r["symbol"])
                edges[r["symbol"]].add(r["trait_method"])  # a call on the concrete type reaches the declaration it implements
            elif r.get("type_symbol"):
                edges[r["type_symbol"]].add(r["symbol"])
    roots = {TOP}
    for r in defs.values():
        if r["api"] or r["root"] in ("main_init", "test_root", "ffi_or_macro_entry") or r.get("withheld") or r.get("withheld_reach"):
            roots.add(r["symbol"])
        elif r["subkind"] == "trait_impl" and r["trait_method"] is None and not r.get("type_symbol"):
            roots.add(r["symbol"])  # blanket / foreign-type impl of a foreign trait: callers are outside the repository
    # a child symbol (field, variant) keeps its type alive and is kept alive by it for the purpose of the closure
    def closure(rts):
        seen, stack = set(), list(rts)
        while stack:
            s_ = stack.pop()
            if s_ in seen:
                continue
            seen.add(s_)
            stack.extend(edges.get(s_, ()))
        return seen

    reached = closure(roots)
    # unreachable functions with an unresolved use site get no verdict either, and count as roots (their callees are not condemned with them)
    extra = set()
    for r in defs.values():
        if r["kind"] != "type" and r["symbol"] not in reached and not r.get("macro_generated") and withhold(r):
            r["withheld_reach"] = withhold(r)
            extra.add(r["symbol"])
    reached = closure(roots | extra)
    for r in defs.values():
        if r["kind"] != "type":
            r["reach"] = "" if (r.get("withheld_reach") or (r.get("withheld") and r["symbol"] not in reached)) else ("live" if r["symbol"] in reached else "dead")
            if r.get("withheld") and r["symbol"] in reached:
                r["reach"] = "live"

    # ---- file level --------------------------------------------------------------------------------------------------------------
    mod_defs = {}  # module symbol -> file (a module definition covers its whole file)
    for d in docs:
        for o in d["occ"]:
            if (o["roles"] & 1) and o["symbol"].endswith("/") and o["range"] and sc.rng(o)[:2] == (0, 0):
                mod_defs.setdefault(o["symbol"], d["path"])
    importers = defaultdict(set)
    for path, lst in occs_by_file.items():
        for (l0, c0, ec, sym) in lst:
            f = mod_defs.get(sym)
            if f and f != path:
                importers[f].add(path)
    by_file = defaultdict(lambda: [0, 0])
    for r in defs.values():
        if r["kind"] in ("function", "method") and not r.get("macro_generated"):
            by_file[r["file"]][0] += 1
            by_file[r["file"]][1] += r["reach"] == "live"
    file_info = []
    for d in sorted(docs, key=lambda x: x["path"]):
        crate, ckind = crate_of(pkgs, d["path"])
        fn, fr = by_file[d["path"]]
        file_info.append({"file": d["path"], "crate": crate, "crate_kind": ckind, "importers": len(importers.get(d["path"], ())), "funcs": fn, "funcs_reachable": fr})

    for r in sorted(defs.values(), key=lambda r: (r["file"], r["line"], r["symbol"])):
        gen = bool(re.search(r"@generated|Code generated|DO NOT EDIT", "\n".join(lines_of(r["file"])[:5])))
        out_decls.append({"file": r["file"], "line": r["line"], "name": r["name"], "kind": r["kind"],
                          "subkind": r["subkind"], **({"recv": r["recv"]} if r.get("recv") else {}), "crate": r["crate"], "crate_kind": r["crate_kind"],
                          "vis": r["vis"], "exported": r["exported"], "api": r["api"], **({"generated": True} if gen else {}),
                          **({"root": r["root"]} if r["root"] else {}), "unref": r["unref"], "unref_how": r["unref_how"],
                          **({"refs": r["refs"]} if "refs" in r else {}), **({"reach": r["reach"]} if "reach" in r else {}),
                          **({"withheld": r.get("withheld") or r.get("withheld_reach")} if (r.get("withheld") or r.get("withheld_reach")) else {})})
    out = {"repo": repo, "tests": not (a.no_tests or a.path_tests_excluded), "files": sorted(d["path"] for d in docs),
           "library": any(k in ("lib", "proc_macro") for k in kinds_seen), "crate_kinds": dict(kinds_seen),
           "packages": [{k: p[k] for k in ("name", "dir", "lib", "proc_macro", "bins")} for p in sorted(pkgs, key=lambda x: x["dir"])],
           "decls": out_decls, "file_info": file_info,
           "stats": {"definitions": len(defs), "reference_edges": sum(len(v) for v in edges.values()), "roots": len(roots)}}
    json.dump(out, sys.stdout, indent=1)


if __name__ == "__main__":
    main()
