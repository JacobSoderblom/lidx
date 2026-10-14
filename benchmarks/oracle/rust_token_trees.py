"""Rust source lines that rust-analyzer's SCIP index holds no occurrences for because they are unexpanded token trees. Python 3 stdlib only.

rust-analyzer resolves a name only where the code is parsed as Rust, or where a macro expansion maps back to the source. Two kinds of
site are never either, in the sandbox this benchmark runs it in (no proc-macro server, no build scripts):

* an **attribute** `#[..]` / `#![..]`: its arguments are a token tree. `#[serde(default = "f")]`, `#[arg(value_parser = f)]` and
  `#[clap(add = ArgValueCompleter::new(Completer::f))]` are read by a derive macro, which the sandbox does not run, so the function they
  name gets no occurrence. A built-in attribute (`#[cfg(..)]`, `#[derive(..)]`) is in the same range; nothing about a function is
  lost, since a function is never named there.
* the body of a **`macro_rules!` definition**: its tokens are the macro's template, not code; only an expansion is analysed, and a
  token of the template gets no occurrence even where the expansion is analysed.

A function named at such a site is still referenced (the derive or the macro uses it); the compiler's index just cannot say so. The
benchmark therefore leaves a lidx reference ungradable instead of wrong (`token_tree_site`, refs.py) when, on every evidence line, the callee's
name is spelled *inside* the token range of such an attribute or macro body (code sharing the line stays graded) and the spelling is one that
names a function: a bare path, or a string that is exactly a path, as the value of `default`, `deserialize_with`, `serialize_with` or
`skip_serializing_if` (never `doc`, `cfg`, `cfg_attr`, `rename`, `alias` or any other string). The name must then bind to the callee: a written
path must agree, segment by segment as a suffix, with the callee's module path; a bare name needs the callee to be the only function of that
name, or declared in the same inline-module scope of the same file. The gate is deliberately narrow: it never touches a reference the compiler
confirmed or a site outside these two shapes. Unit-tested in `test_token_tree_gate.py`.
"""
import re

from rust_cfg import _match_close, _skip_ws, mask


def _line_starts(m):
    starts = [0]
    for mm in re.finditer("\n", m):
        starts.append(mm.end())
    return starts


def _line_of(starts, pos):
    lo, hi = 0, len(starts) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if starts[mid] <= pos:
            lo = mid
        else:
            hi = mid - 1
    return lo + 1


PATH_KEYS = ("default", "deserialize_with", "serialize_with", "skip_serializing_if")
NOT_CODE_ATTRS = ("doc", "cfg", "cfg_attr", "cfg_eval")
KEYWORD_SEGS = ("crate", "self", "super")


class Source:
    """One file scanned once: attribute and `macro_rules!` token ranges (offsets), inline `mod` blocks, and line starts."""

    def __init__(self, text):
        self.text = text
        self.masked = mask(text)
        m = self.masked
        self.starts = _line_starts(m)
        self.attrs, self.macros, self.mods = [], [], []
        for mm in re.finditer(r"#\s*!?\s*\[", m):
            close = _match_close(m, mm.end() - 1)
            if close > 0:
                nm = re.match(r"\s*([A-Za-z_]\w*)", m[mm.end():mm.end() + 80])
                self.attrs.append((mm.start(), close, nm.group(1) if nm else ""))
        for mm in re.finditer(r"(?<![\w$])macro_rules\s*!\s*[A-Za-z_]\w*\s*", m):
            i = _skip_ws(m, mm.end())
            if i < len(m) and m[i] in "({[":
                close = _match_close(m, i)
                if close > 0:
                    self.macros.append((mm.start(), close))
        for mm in re.finditer(r"(?<![\w])mod\s+([A-Za-z_]\w*)\s*\{", m):
            close = _match_close(m, mm.end() - 1)
            if close > 0:
                self.mods.append((mm.end() - 1, close, mm.group(1)))

    def line_span(self, ln):
        a = self.starts[ln - 1]
        b = self.starts[ln] - 1 if ln < len(self.starts) else len(self.text)
        return a, b

    def def_offset(self, ln):
        a, b = self.line_span(ln)
        return a + len(self.text[a:b]) - len(self.text[a:b].lstrip(" \t"))

    def inline_mods(self, off):
        return [n for a, b, n in sorted(self.mods) if a < off < b]

    def occurrences(self, ln, name):
        """Offsets of whole-identifier spellings of `name` on 1-based line `ln`."""
        if not 0 < ln <= len(self.starts):
            return []
        a, b = self.line_span(ln)
        return [a + mm.start() for mm in re.finditer(r"(?<![\w])%s(?![\w])" % re.escape(name), self.text[a:b])]

    def spelling(self, off, name):
        """How the spelling at `off` names a function, or None when it does not: ('path', [written segments]) for a path token or a string that is
        exactly a path under one of PATH_KEYS, inside an attribute (not doc / cfg / cfg_attr) or a macro_rules! body."""
        attr = next(((a, b, n) for a, b, n in self.attrs if a < off < b), None)
        in_macro = any(a < off < b for a, b in self.macros)
        if attr is None and not in_macro:
            return None
        if attr is not None and attr[2] in NOT_CODE_ATTRS:
            return None
        t = self.text
        blanked = self.masked[off] == " " and t[off] != " "  # inside a string or comment
        if blanked:
            if attr is None:
                return None  # prose or a format string in a macro body
            left, right = t.rfind('"', 0, off), t.find('"', off)
            if left < 0 or right < 0:
                return None
            km = re.search(r"([A-Za-z_]\w*)\s*=\s*$", t[:left])
            if not km or km.group(1) not in PATH_KEYS:
                return None
            content = t[left + 1:right]
            pm = re.fullmatch(r"\s*((?:[A-Za-z_]\w*\s*::\s*)*)%s\s*" % re.escape(name), content)
            if not pm:
                return None
            return ("path", [x.strip() for x in pm.group(1).split("::") if x.strip()])
        pm = re.search(r"((?:[A-Za-z_]\w*\s*::\s*)+)$", t[:off])
        segs = [x.strip() for x in pm.group(1).split("::") if x.strip()] if pm else []
        return ("path", segs)


def _segs(qualname):
    return [x for x in re.split(r"::|\.", qualname) if x]


def binds(site, off, spelled, callee, unique, caller_module, callee_src):
    """Does the spelling at `off` of `site` mean `callee`? `callee`: {name, qualname, file, line, kind}; `callee_src`: the callee's file scanned
    (the same object as `site` when it is the same file); `caller_module`: the qualified name of the site file's module symbol or None."""
    segs = list(spelled[1])
    parent = _segs(callee["qualname"])[:-1]
    if not segs:
        if unique:
            return True
        if callee_src is not site or callee["kind"] != "function":
            return False
        return site.inline_mods(off) == callee_src.inline_mods(callee_src.def_offset(callee["line"]))
    if all(x in KEYWORD_SEGS for x in segs):
        # only `super::` / `self::` / `crate::`: the callee's module must be where that leads from the site, or the name must be unique
        if unique:
            return True
        if segs[0] == "crate" and len(segs) == 1:
            return parent == ["crate"]
        if caller_module is None:
            return False
        here = _segs(caller_module) + site.inline_mods(off)
        n = sum(1 for x in segs if x == "super")
        return n <= len(here) and parent == here[:len(here) - n]
    lead = 0
    while lead < len(segs) and segs[lead] in KEYWORD_SEGS:
        lead += 1
    rest = segs[lead:]
    if segs[0] == "crate":
        return parent == ["crate"] + rest
    return len(rest) <= len(parent) and parent[len(parent) - len(rest):] == rest


class SiteGate:
    """Callable for refs.grade_refs: is this lidx reference only at attribute / macro-definition sites that name the callee, unambiguously?

    Every evidence line needs a spelling of the callee's name inside the token range of an attribute or a `macro_rules!` body (Source.spelling)
    that binds to the callee (`binds`). A site that names another function, a spelling in prose, a feature string, a `rename`, or code that merely
    shares the line with an attribute stays graded."""

    def __init__(self, read):
        self.read = read  # path -> source text
        self.cache = {}

    def _get(self, path):
        if path not in self.cache:
            try:
                self.cache[path] = Source(self.read(path))
            except OSError:
                self.cache[path] = Source("")
        return self.cache[path]

    def __call__(self, path, lines, callee, unique, caller_module=None):
        site = self._get(path)
        callee_src = site if callee["file"] == path else self._get(callee["file"])
        if not lines:
            return False
        for ln in lines:
            ok = False
            for off in site.occurrences(ln, callee["name"]):
                sp = site.spelling(off, callee["name"])
                if sp and binds(site, off, sp, callee, unique, caller_module, callee_src):
                    ok = True
                    break
            if not ok:
                return False
        return True
