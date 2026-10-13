"""Positive detection of `#[cfg(...)]`-gated Rust items that the oracle did not analyse. Python 3 stdlib only.

rust-analyzer analyses one configuration: the host platform, and either every feature or the default ones. Code behind a
predicate that is *definitely false* in that configuration (`#[cfg(windows)]` on macOS, `#[cfg(not(unix))]`,
`#[cfg(target_os = "linux")]`, `#[cfg(feature = "x")]` when features are default) is invisible to the oracle, so a lidx edge
inside it cannot be graded: the compiler never saw the call. This module finds those regions from the source text.

It is deliberately one-sided. A predicate it cannot evaluate (`test`, `debug_assertions`, `panic = ...`, unknown keys) is
*unknown*, not false, and unknown never gates anything; an item whose extent it cannot determine gates nothing. Code
without a `#[cfg]` is never touched. Its mistakes can therefore only leave an edge graded, never hide a lidx error behind
a guess about ungated code. Unit-tested in `test_cfg_gate.py`.
"""
import re

# keys whose value the host answers; anything else is unknown
HOST_KEYS = ("target_os", "target_family", "target_arch", "target_env", "target_vendor", "target_endian", "target_pointer_width")


def host_from_rustc(output, features="default"):
    """Host description from `rustc --print cfg` output. features: "all" (every feature is on) or "default" (unknown)."""
    names, kv = set(), {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r'^([A-Za-z_][\w]*)="(.*)"$', line)
        if m:
            kv.setdefault(m.group(1), set()).add(m.group(2))
        else:
            names.add(line)
    return {"names": names, "kv": kv, "features": features}


# ---------------------------------------------------------------- predicates
def _tokens(s):
    return re.findall(r'"(?:[^"\\]|\\.)*"|[A-Za-z_][\w]*|[(),=]', s)


def parse_pred(text):
    """`windows`, `target_os = "linux"`, `not(x)`, `all(a, b)`, `any(a, b)` -> nested tuples, or None when it does not parse."""
    toks = _tokens(text)
    pos = 0

    def peek():
        return toks[pos] if pos < len(toks) else None

    def expr():
        nonlocal pos
        t = peek()
        if t is None or not re.match(r"[A-Za-z_]", t):
            raise ValueError(t)
        pos += 1
        if peek() == "(" and t in ("not", "all", "any"):
            pos += 1
            args = []
            while peek() != ")":
                args.append(expr())
                if peek() == ",":
                    pos += 1
                elif peek() != ")":
                    raise ValueError(peek())
            pos += 1
            return (t, args)
        if peek() == "=":
            pos += 1
            v = peek()
            if v is None or not v.startswith('"'):
                raise ValueError(v)
            pos += 1
            return ("kv", t, v[1:-1])
        return ("name", t)

    try:
        node = expr()
    except (ValueError, IndexError):
        return None
    return node if pos == len(toks) else None


def evaluate(node, host):
    """True / False / None (unknown) under the oracle's configuration, three-valued so that unknown never becomes False."""
    if node is None:
        return None
    kind = node[0]
    if kind == "name":
        if node[1] in ("unix", "windows"):
            return node[1] in host["names"]
        return None
    if kind == "kv":
        k, v = node[1], node[2]
        if k == "feature":
            return True if host.get("features") == "all" else None
        if k in HOST_KEYS and k in host["kv"]:
            return v in host["kv"][k]
        return None
    if kind == "not":
        r = evaluate(node[1][0], host) if len(node[1]) == 1 else None
        return None if r is None else (not r)
    vals = [evaluate(a, host) for a in node[1]]
    if kind == "all":
        return False if False in vals else (None if None in vals else True)
    if kind == "any":
        return True if True in vals else (None if None in vals else False)
    return None


# ---------------------------------------------------------------- source scanning
def mask(src):
    """`src` with comments, string and char literals replaced by spaces (newlines kept), so brackets inside them are inert."""
    out = list(src)
    n, i = len(src), 0

    def blank(a, b):
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = src[i]
        if c == "/" and src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif c == "/" and src.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif src.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif c == '"' or (c in "rb" and re.match(r'(?:b?r(#*)")|(?:b")', src[i:i + 12]) and (i == 0 or not (src[i - 1].isalnum() or src[i - 1] == "_"))):
            m = re.match(r'b?r(#*)"', src[i:i + 12])
            if m:  # raw string
                end = '"' + m.group(1)
                j = src.find(end, i + m.end())
                j = n if j < 0 else j + len(end)
            else:
                j = i + (2 if c == "b" else 1)
                while j < n and src[j] != '"':
                    j += 2 if src[j] == "\\" else 1
                j += 1
            blank(i, min(j, n))
            i = min(j, n)
        elif c == "'":
            m = re.match(r"'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'", src[i:i + 14])
            if m:
                blank(i, i + m.end())
                i += m.end()
            else:
                i += 1  # a lifetime
        else:
            i += 1
    return "".join(out)


OPEN, CLOSE = "([{", ")]}"
BLOCK_KW = {"fn", "mod", "impl", "trait", "enum", "union", "struct", "if", "match", "for", "while", "loop", "extern"}


def _skip_ws(m, i):
    while i < len(m) and m[i].isspace():
        i += 1
    return i


def _match_close(m, i):
    """Index of the bracket matching the opener at m[i], or -1."""
    depth = 0
    for j in range(i, len(m)):
        if m[j] in OPEN:
            depth += 1
        elif m[j] in CLOSE:
            depth -= 1
            if depth == 0:
                return j
    return -1


def _item_end(m, i):
    """Index of the last character of the item/statement/field/arm starting at m[i], or -1 when it is not found."""
    # block-ended items: the first top-level `{...}` closes them (`struct S;`, `mod m;`, `fn f();` end at `;`); a
    # statement, `use`, `const X`, field, variant or match arm ends at the next top-level `;` or `,`
    j = i
    while True:
        w = re.match(r"[A-Za-z_]\w*", m[j:j + 40])
        if not w or w.group(0) not in ("pub", "unsafe", "async", "default", "const"):
            break
        nj = _skip_ws(m, j + w.end())
        if w.group(0) == "pub" and m[nj:nj + 1] == "(":
            end = _match_close(m, nj)
            if end < 0:
                return -1
            nj = _skip_ws(m, end + 1)
        if w.group(0) == "const" and not re.match(r"(unsafe|async|extern|fn)\b", m[nj:nj + 10]):
            break
        j = nj
    w = re.match(r"[A-Za-z_]\w*", m[j:j + 40])
    block_ended = (w is not None and w.group(0) in BLOCK_KW) or m[j:j + 1] == "{"
    first_real = w.group(0) if w else None
    k = i
    depth = 0
    n = len(m)
    while k < n:
        c = m[k]
        if c in OPEN:
            depth += 1
        elif c in CLOSE:
            if depth == 0:
                return k - 1  # the enclosing block closed: the element ended just before
            depth -= 1
            if depth == 0 and c == "}" and block_ended:
                if first_real == "if":
                    nxt = _skip_ws(m, k + 1)
                    if m.startswith("else", nxt):
                        k = nxt + 4
                        continue
                return k
        elif depth == 0 and (c == ";" or (c == "," and not block_ended)):
            return k
        k += 1
    return -1


def gated_ranges(src, host):
    """1-based inclusive (first, last) line ranges of items whose `#[cfg(...)]` is definitely false under `host`."""
    m = mask(src)
    starts = [0]
    for mm in re.finditer("\n", m):
        starts.append(mm.end())

    def line_of(pos):
        lo, hi = 0, len(starts) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if starts[mid] <= pos:
                lo = mid
            else:
                hi = mid - 1
        return lo + 1

    total = len(starts)
    out = []
    # an inner attribute gates the whole file (or the enclosing module, which for a file is the same thing)
    for mm in re.finditer(r"#!\s*\[\s*cfg\s*\(", m):
        close = _match_close(m, mm.end() - 1)
        if close < 0:
            continue
        pred = src[mm.end():close]
        if not re.sub(r"#!\s*\[[^\]]*\]", "", m[:mm.start()]).strip():  # only other inner attributes before it
            if evaluate(parse_pred(pred.strip()), host) is False:
                return [(1, total)]
    for mm in re.finditer(r"#\s*\[\s*cfg\s*\(", m):
        close = _match_close(m, mm.end() - 1)
        if close < 0:
            continue
        after = _skip_ws(m, close + 1)
        if after >= len(m) or m[after] != "]":
            continue
        if evaluate(parse_pred(src[mm.end():close].strip()), host) is not False:
            continue
        i = _skip_ws(m, after + 1)
        while m.startswith("#", i) and re.match(r"#\s*\[", m[i:i + 10]):  # more attributes on the same item
            i = _skip_ws(m, _match_close(m, m.index("[", i)) + 1)
        end = _item_end(m, i)
        if end < i:
            continue
        out.append((line_of(mm.start()), line_of(end)))
    return out


def lines_gated(ranges, lines):
    """True when `lines` is non-empty and every line lies inside one of the gated ranges."""
    return bool(lines) and all(any(a <= ln <= b for a, b in ranges) for ln in lines)
