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
benchmark therefore leaves a lidx reference whose every evidence line is such a site, and whose line spells the function's name,
ungradable instead of wrong (`token_tree_site`, refs.py). The gate is deliberately narrow: it never touches a reference the compiler
confirmed, a site outside these two shapes, or a line that does not spell the name. Unit-tested in `test_token_tree_gate.py`.
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


def token_tree_ranges(src):
    """1-based inclusive (first, last) line ranges of every attribute and every `macro_rules!` definition in `src`."""
    m = mask(src)
    starts = _line_starts(m)
    out = []
    for mm in re.finditer(r"#\s*!?\s*\[", m):
        close = _match_close(m, mm.end() - 1)
        if close > 0:
            out.append((_line_of(starts, mm.start()), _line_of(starts, close)))
    for mm in re.finditer(r"(?<![\w$])macro_rules\s*!\s*[A-Za-z_]\w*\s*", m):
        i = _skip_ws(m, mm.end())
        if i < len(m) and m[i] in "({[":
            close = _match_close(m, i)
            if close > 0:
                out.append((_line_of(starts, mm.start()), _line_of(starts, close)))
    return out


def lines_in_token_trees(ranges, lines):
    """True when `lines` is non-empty and every line lies inside one of the ranges."""
    return bool(lines) and all(any(a <= ln <= b for a, b in ranges) for ln in lines)


def spells(text_lines, lines, name):
    """True when every one of `lines` (1-based) spells `name` as a whole identifier."""
    pat = re.compile(r"(?<![\w])%s(?![\w])" % re.escape(name))
    return bool(lines) and all(0 < ln <= len(text_lines) and pat.search(text_lines[ln - 1]) for ln in lines)


def qualified_by(text_lines, lines, name, qualname):
    """True when on every one of `lines` the first spelling of `name` is written with a path (`keyed::name`, `Completer::name`) whose
    segments (other than `super`, `self`, `crate`, which only locate it) all occur in the callee's qualified name."""
    segs = {s for s in re.split(r"::|\.", qualname) if s}
    pat = re.compile(r"((?:[A-Za-z_]\w*::)+)%s(?![\w])" % re.escape(name))
    for ln in lines:
        m = pat.search(text_lines[ln - 1]) if 0 < ln <= len(text_lines) else None
        if not m:
            return False
        written = [s for s in m.group(1).split("::") if s and s not in ("super", "self", "crate")]
        if not written or not all(s in segs for s in written):
            return False
    return True


class SiteGate:
    """Callable for refs.grade_refs: is this lidx reference only at attribute / macro-definition sites that name the callee, unambiguously?

    `binds_ok` (the caller computes it): the callee is the only function or method of that name in the repository, or is declared in the
    caller's own file, so the name at the site can mean nothing else. Otherwise the site must write a path to it that agrees with the
    callee's qualified name. A site that names another function, or the same name bound to a different declaration, stays graded."""

    def __init__(self, read):
        self.read = read  # path -> source text
        self.cache = {}

    def _get(self, path):
        if path not in self.cache:
            try:
                text = self.read(path)
            except OSError:
                text = ""
            self.cache[path] = (token_tree_ranges(text), text.split("\n"))
        return self.cache[path]

    def __call__(self, path, lines, name, qualname, binds_ok):
        ranges, text = self._get(path)
        if not lines_in_token_trees(ranges, lines) or not spells(text, lines, name):
            return False
        return binds_ok or qualified_by(text, lines, name, qualname)
