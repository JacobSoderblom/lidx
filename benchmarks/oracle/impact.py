"""Impact sets, test reach and trace_flow stop reasons graded against the closure of the compiler call graph (issue #451).

Oracle side: the call graph each oracle already emits (Go RTA with dynamic calls at the concrete implementations, tsc, SCIP), closed
transitively. For a seed `s`:

* `Up_k(s)`   callers of `s` within `k` hops (k = 3, lidx's default depth, and unbounded),
* `Tests(s)`  the test roots in `Up_k(s)` (Go `Test*`/`Benchmark*`/`Fuzz*` and the `Example*` functions with an `// Output:` comment in `_test.go`; Rust `#[test]` functions, the attribute read
              from the lines above the declaration; TypeScript: void, test files are out of scope on both sides),
* `Down_k(s)` callees of `s` within `k` hops.

lidx side (RPC, no corpus code): `analyze_impact` upstream for `Up_k` (direct layer) and for `Tests` (test layer alone), `trace_flow`
downstream for `Down_k` and the stop reason of every leaf. Matching is by canonical declaration key, as in match.py.

Python 3 stdlib only.
"""
import json
import os
import random
import re
import select
import subprocess
from collections import defaultdict, deque

from match import ratio, wilson

SEED = 2026
PER_CELL = 100
STRATA = ((0, 0), (1, 3), (4, 10), (11, None))
STRATUM_NAMES = ("0", "1-3", "4-10", ">10")
LIDX_MAX = 10  # analyze_impact max_depth and trace_flow max_hops are capped at 10: lidx's "unbounded"
KS = (3, None)  # None: unbounded in the oracle, the cap of 10 in lidx
EXAMPLES = 5
LIST_CAP = 10  # committed extra/missing pairs per seed, k and kind
BOOT = 1000
GO_TEST_ROOT = re.compile(r"^(Test|Benchmark|Fuzz)([^a-z]|$)")
GO_EXAMPLE = re.compile(r"^Example([^a-z]|$)")
RUST_TEST_ATTR = re.compile(r"^#\[\s*(?:[A-Za-z_][\w]*::)*test\b")
CALL_KINDS = ["CALLS", "DISPATCH"]
ABSTRACT_RE = re.compile(r"^\s*(?:(?:public|protected|private|override|static)\s+)*abstract\b")


def kname(k):
    return "inf" if k is None else str(k)


def lidx_depth(k):
    return LIDX_MAX if k is None else k


# ------------------------------------------------------------------ the oracle closure
def container_of(qualname):
    """Qualname of the container: everything before the last `.` or `::`."""
    cut = max(qualname.rfind("."), qualname.rfind("::"))
    return qualname[:cut] if cut > 0 else ""


def decl_of(key, key_name):
    f, _, ln = key.rpartition(":")
    return {"file": f, "line": int(ln), "name": key_name.get(key, "?")}


class Graph:
    """Call graph of one oracle cell over canonical declaration keys (`file|qualname` where lidx has a symbol at the position,
    else `file:line`).

    Callers are lifted to the nearest enclosing declaration lidx indexes (as match.grade does); the callee of a TypeScript edge may
    be one of its alternative keys (`callee_alts`); Go's `callee_alts` are interface methods and are not used: the closure follows the
    concrete implementations RTA names. Module-level nodes (line 0) are leaves nobody calls and lidx drops them from impact sets, so
    they are left out. Self edges do not matter to a closure and are dropped.
    """

    def __init__(self, oracle, syms, lang, drop_closure=False, expand=False, src=None):
        self.lang = lang
        # expand: an edge to a trait / interface / abstract member also reaches every member that overrides it according to the compiler's
        # own hierarchy (the oracle's `hierarchy.overrides`): what Go's RTA does for dynamic calls, without its allocation analysis
        subs = defaultdict(set)
        if expand:
            for o in (oracle.get("hierarchy") or {}).get("overrides", []):
                subs["%s:%d" % (o["base"]["file"], o["base"]["line"])].add("%s:%d" % (o["sub"]["file"], o["sub"]["line"]))
        sym_keys = {s["key"] for s in syms.values() if "key" in s}
        canon = {}
        for s in syms.values():
            if "key" in s:
                c = "%s|%s" % (s["file"], s["qualname"])
                if s["key"] not in canon or c < canon[s["key"]]:
                    canon[s["key"]] = c
        self.canon = canon
        self.sym_keys = sym_keys
        self.cn = lambda k: canon.get(k, k)
        self.qual_nodes = defaultdict(list)  # lidx qualname -> canonical nodes, to look up the symbols on an impact path
        container_kind = {s["qualname"]: s["kind"] for s in syms.values() if "key" in s}
        self.abstract = set()  # members of an interface / trait: pass-through hops of a dispatch, never nodes of a compiler call graph
        for s in syms.values():
            if "key" in s:
                n = canon.get(s["key"], s["key"])
                self.qual_nodes[s["qualname"]].append(n)
                if container_kind.get(container_of(s["qualname"])) in ("interface", "trait"):
                    self.abstract.add(n)
                elif src is not None and s["kind"] == "method":
                    lines = src.lines(s["file"])  # a bodyless `abstract m();` of a class: a declaration, not a caller
                    if 0 < s["line"] <= len(lines) and ABSTRACT_RE.match(lines[s["line"] - 1]):
                        self.abstract.add(n)
        self.files = set(oracle["files"])
        self.fwd = defaultdict(set)
        self.rev = defaultdict(set)
        self.sites = defaultdict(list)  # (caller node, callee node) -> call sites of the oracle edge(s) behind it, for reading a chain against the source
        self.info = {}  # node -> {file, line, name}
        self.judged = set()  # nodes the oracle analysed as functions
        self.unindexed = set()  # nodes lidx has no symbol for
        okey = lambda d: "%s:%d" % (d["file"], d["line"])
        key_name = {s["key"]: s["name"] for s in syms.values() if "key" in s}

        def reg(d, key):
            n = self.cn(key)
            self.info.setdefault(n, {"file": d["file"], "line": d["line"], "name": d["name"], "key": key})
            if key not in sym_keys:
                self.unindexed.add(n)
            return n

        for c in oracle["callers"]:
            if c["line"] > 0:
                self.judged.add(reg(c, okey(c)))
        for e in oracle["edges"]:
            if drop_closure and (e.get("closure") or (e.get("dispatch") and not e.get("callee_alts"))):
                # Go diagnostic closure: no function-value calls. `closure`: the callee is a function literal keyed at its enclosing declaration;
                # `dispatch` without `callee_alts`: a call of a function value, which RTA resolves to every address-taken function of the signature
                # (an interface invocation has `callee_alts`, the interface methods it invokes, and stays)
                continue
            ck = okey(e["caller"])
            cks = [ck] + list(e.get("caller_alts", []))
            # every alternative caller lidx indexes (a module-level call inside several `const X = core.$constructor(..)` initialisers is listed under
            # the module with each const as an alternative), else the nearest enclosing declaration lidx indexes
            callers = [k for k in cks if k in sym_keys and not k.endswith(":0")]
            if not callers:
                lift = next((k for k in e.get("caller_chain", []) if k in sym_keys and not k.endswith(":0")), None)
                callers = [lift] if lift else ([] if ck.endswith(":0") else [ck])
            # Go: `callee_alts` are interface methods (not followed). Elsewhere they are alternative keys of the same callee (the class and its
            # constructor for `new X()`, merged declarations): every one lidx indexes is a callee
            tks = [okey(e["callee"])] + ([] if lang == "go" else list(e.get("callee_alts", [])))
            callees = [k for k in tks if k in sym_keys and not k.endswith(":0")] or ([] if tks[0].endswith(":0") else [tks[0]])
            for pick in callers:
                a = reg(e["caller"] if pick == ck else decl_of(pick, key_name), pick)
                self.judged.add(a)
                for tpick in callees:
                    b = reg(e["callee"] if tpick == tks[0] else decl_of(tpick, key_name), tpick)
                    self.judged.add(b)
                    if a != b:
                        self.fwd[a].add(b)
                        self.rev[b].add(a)
                        self.sites[(a, b)].extend(e.get("sites", []))
                    for sk in sorted(subs.get(tpick, ())):
                        if sk.endswith(":0"):
                            continue
                        b2 = reg(decl_of(sk, key_name), sk)
                        self.judged.add(b2)
                        if a != b2:
                            self.fwd[a].add(b2)
                            self.rev[b2].add(a)
                            self.sites[(a, b2)].extend(e.get("sites", []))

    def file_of(self, node):
        return node.split("|", 1)[0] if "|" in node else node.rpartition(":")[0]

    def indegree(self, node):
        return len(self.rev.get(node, ()))

    def closure(self, node, k, rev=True):
        """{node: distance} within k hops (None: unbounded), the start excluded."""
        adj = self.rev if rev else self.fwd
        dist = {node: 0}
        q = deque([node])
        while q:
            x = q.popleft()
            d = dist[x]
            if k is not None and d >= k:
                continue
            for y in adj.get(x, ()):
                if y not in dist:
                    dist[y] = d + 1
                    q.append(y)
        del dist[node]
        return dist

    def path_ok(self, steps):
        """Can the oracle judge a lidx path? Every symbol on it must be in a file the oracle analysed and, for Go (RTA analyses only what is reachable
        from its roots), a function RTA reached: a chain through a type nothing allocates, an example or nested module the oracle never loaded, or
        test code in a production-only cell, is outside what the compiler analysed."""
        for st in steps:
            for q in (st.get("from_symbol"), st.get("to_symbol")):
                cands = self.qual_nodes.get(q)
                if cands and not any(n in self.abstract or (self.file_of(n) in self.files and (self.lang != "go" or n in self.judged)) for n in cands):
                    return False
        return True

    def gradable(self, node):
        """A lidx member outside the oracle set can be called wrong only when the oracle analysed that function."""
        return self.file_of(node) in self.files and node in self.judged


# ------------------------------------------------------------------ seeds
def draw_seeds(graph, cell_id, eligible=None):
    """`eligible`: the declaration keys lidx indexes as a function, method or const (arrow-function binding), so every seed can be queried;
    classes, types and modules are not seeds. 100 seeds stratified by oracle in-degree (0, 1-3, 4-10, >10), drawn with seed 2026 from the oracle's judged functions
    (for Go: the functions RTA proved reachable, so code RTA cannot reach is never drawn). 25 per stratum; the shortfall of a thin
    stratum goes to the thicker ones in turn. Deterministic in (oracle output, cell id, seed)."""
    pool = [n for n in sorted(graph.judged) if graph.info[n]["line"] > 0 and not graph.info[n]["name"].startswith("<")
            and graph.info[n]["name"] != "constructor"  # the compiler keys `new X()` at the class, so a constructor has no callers by construction
            and (eligible is None or graph.info[n]["key"] in eligible)]
    buckets = [[] for _ in STRATA]
    for n in pool:
        d = graph.indegree(n)
        for i, (lo, hi) in enumerate(STRATA):
            if d >= lo and (hi is None or d <= hi):
                buckets[i].append(n)
                break
    rng = random.Random("%d|%s" % (SEED, cell_id))
    for b in buckets:
        rng.shuffle(b)
    quota = [PER_CELL // len(STRATA)] * len(STRATA)
    take = [min(q, len(b)) for q, b in zip(quota, buckets)]
    spare = PER_CELL - sum(take)
    while spare > 0 and any(take[i] < len(buckets[i]) for i in range(len(buckets))):
        for i in reversed(range(len(buckets))):
            if spare > 0 and take[i] < len(buckets[i]):
                take[i] += 1
                spare -= 1
    seeds = []
    for i, b in enumerate(buckets):
        for n in b[:take[i]]:
            inf = graph.info[n]
            seeds.append({"file": inf["file"], "line": inf["line"], "name": inf["name"], "indegree": graph.indegree(n), "stratum": STRATUM_NAMES[i]})
    seeds.sort(key=lambda s: (s["file"], s["line"]))
    return seeds, {"pool": len(pool), "stratum_pool": dict(zip(STRATUM_NAMES, (len(b) for b in buckets))),
                   "stratum_drawn": dict(zip(STRATUM_NAMES, take))}


def seed_node(graph, seed):
    return graph.cn("%s:%d" % (seed["file"], seed["line"]))


# ------------------------------------------------------------------ test roots
def go_example_has_output(lines, start):
    """`go test` runs an `Example*` function only when its body ends in an `// Output:` (or `// Unordered output:`) comment group:
    the last comment group of the body starts with that word. `start` is the 0-based line of `func Example...`; the body ends at the
    first line that is exactly `}` (gofmt'd top-level function) or on the declaration line itself when it closes there."""
    first = lines[start]
    if first.rstrip().endswith("}"):
        return False  # a one-line body holds no comment group of its own
    end = start + 1
    while end < len(lines) and lines[end].rstrip() != "}":
        end += 1
    group, last = [], []
    for ln in lines[start + 1:end]:
        t = ln.strip()
        if t.startswith("//") or t.startswith("/*"):
            group.append(t.lstrip("/*").strip())
        else:
            if group:
                last = group
            group = []
    if group:
        last = group
    return bool(last) and last[0].lower().startswith(("output:", "unordered output:"))


def test_root_predicate(lang, cell_variant, src):
    """node info -> bool: is the oracle declaration a test root. TypeScript has none (test files are out of scope)."""
    if lang == "go":
        def go_root(inf):
            if not inf["file"].endswith("_test.go"):
                return False
            lines = src.lines(inf["file"])
            text = lines[inf["line"] - 1] if 0 < inf["line"] <= len(lines) else ""
            # a top-level function, not a method: `func Name(` with no receiver
            if re.match(r"^\s*func\s+" + re.escape(inf["name"]) + r"\b", text) is None:
                return False
            if GO_EXAMPLE.match(inf["name"]):
                return go_example_has_output(lines, inf["line"] - 1)
            return bool(GO_TEST_ROOT.match(inf["name"]))
        return go_root
    if lang == "rust":
        def rust_root(inf):
            lines = src.lines(inf["file"])
            i = inf["line"] - 2  # the line above the declaration's name line
            # the declaration's name line may be several lines below the attribute (`pub async fn` is one line; where clauses and
            # multi-line signatures start on the `fn` line), so walk up through attributes, doc comments and visibility
            while 0 <= i < len(lines):
                t = lines[i].strip()
                if RUST_TEST_ATTR.match(t):
                    return True
                if t.startswith(("#[", "///", "//", "#![")) or t == "":
                    i -= 1
                    continue
                break
            return False
        return rust_root
    return lambda inf: False


# ------------------------------------------------------------------ statistics
def pooled(rows):
    tp = sum(r[0] for r in rows)
    fp = sum(r[1] for r in rows)
    fn = sum(r[2] for r in rows)
    return {"tp": tp, "fp": fp, "fn": fn, "precision": ratio(tp, tp + fp), "precision_ci": wilson(tp, tp + fp),
            "recall": ratio(tp, tp + fn), "recall_ci": wilson(tp, tp + fn)}


def boot_mean(vals, seed=SEED):
    """(mean, [lo, hi]) of the values, 95% percentile interval over BOOT resamples of the seeds."""
    if not vals:
        return None, [None, None]
    rng = random.Random(seed)
    n = len(vals)
    means = sorted(sum(vals[rng.randrange(n)] for _ in range(n)) / n for _ in range(BOOT))
    return round(sum(vals) / n, 4), [round(means[int(0.025 * BOOT)], 4), round(means[int(0.975 * BOOT) - 1], 4)]


def macro(per_seed):
    """per_seed: [(tp, fp, fn, truncated)]. Per-seed precision where lidx returned a gradable member, per-seed recall where the oracle
    set is non-empty and lidx did not truncate; each averaged over seeds with a bootstrap interval."""
    ps = [t / (t + f) for t, f, n, tr in per_seed if t + f > 0]
    rs = [t / (t + n) for t, f, n, tr in per_seed if t + n > 0 and not tr]
    p, pci = boot_mean(ps)
    r, rci = boot_mean(rs)
    return {"macro_precision": p, "macro_precision_ci": pci, "macro_precision_seeds": len(ps),
            "macro_recall": r, "macro_recall_ci": rci, "macro_recall_seeds": len(rs)}


def compare(oset, lset, graph):
    """oset: oracle members (indexed ones only); lset: lidx members. -> (tp, fp, fn, ungradable, extra list, missing list)"""
    tp = oset & lset
    extra = lset - oset
    fp = {x for x in extra if graph.gradable(x)}
    missing = oset - lset
    return len(tp), len(fp), len(missing), len(extra) - len(fp), sorted(fp), sorted(missing)


# ------------------------------------------------------------------ lidx client
class Lidx:
    """One `lidx serve` process over a prepared index; JSONL requests, one answer per line."""

    def __init__(self, binary, repo, db, timeout=180):
        self.cmd = [binary, "serve", "--repo", repo, "--db", db, "--watch", "off"]
        self.timeout = timeout
        self.n = 0
        self.p = None
        self.start()

    def start(self):
        self.p = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)

    def close(self):
        if self.p:
            self.p.kill()
            self.p.wait()
            self.p = None

    def call(self, method, params):
        """Result dict, or {"_error": message} (also on timeout, after restarting the server)."""
        self.n += 1
        try:
            self.p.stdin.write(json.dumps({"id": self.n, "method": method, "params": params}) + "\n")
            self.p.stdin.flush()
            ready, _, _ = select.select([self.p.stdout], [], [], self.timeout)
            if not ready:
                self.close()
                self.start()
                return {"_error": "timeout after %ss" % self.timeout}
            line = self.p.stdout.readline()
        except (BrokenPipeError, OSError):
            self.close()
            self.start()
            return {"_error": "server died"}
        if not line:
            self.close()
            self.start()
            return {"_error": "server closed"}
        r = json.loads(line)
        if "error" in r:
            return {"_error": r["error"].get("message", str(r["error"]))}
        return r["result"]


RESPONSE_BYTES = 64_000_000  # analyze_impact cuts its answer at 30,000 bytes unless max_response_bytes says otherwise


def impact_params(sid, k, tests, kinds=None):
    p = {"id": sid, "direction": "up", "max_depth": lidx_depth(k), "enable_historical": False, "include_paths": False, "limit": 2000,
         "max_response_bytes": RESPONSE_BYTES}
    if tests:  # the test layer alone: it traverses upstream through test files itself, and its entries are the only ones returned
        p.update(enable_direct=False, enable_test=True)
    else:
        p.update(include_tests=True, enable_test=False)
        if kinds:
            p["kinds"] = kinds
            # containment is lidx's hop from a function to the functions declared inside it: a relation of declaration, not a call
            p["exclude_resolution_kinds"] = ["containment"]
    return p


def trace_params(sid, k):
    return {"start_id": sid, "direction": "downstream", "max_hops": lidx_depth(k), "max_bytes": 200000, "include_snippets": False}


# ------------------------------------------------------------------ grading one cell
SEED_KINDS = ("function", "method", "const")


def seed_ids(syms):
    by_key = defaultdict(list)
    for i, s in syms.items():
        if "key" in s and s["kind"] in ("function", "method", "test"):
            by_key[s["key"]].append(i)
    return by_key


def members(res, syms, graph, exclude=()):
    """Canonical nodes of an analyze_impact answer's affected symbols (in-repo symbols only)."""
    out = set()
    for e in res.get("affected", []):
        s = syms.get(e["symbol"]["id"])
        if not s or "key" not in s:
            continue
        n = graph.cn(s["key"])
        if n not in exclude and n not in graph.abstract:  # an interface / abstract member is a pass-through of a dispatch, not a caller
            out.add(n)
    return out


def trace_nodes(res, syms, graph, seed):
    hops = res.get("trace", [])
    nodes = set()
    leaves = []
    for h in hops:
        s = syms.get(h["symbol"]["id"])
        if not s or "key" not in s:
            continue
        n = graph.cn(s["key"])
        if n != seed:
            nodes.add(n)
        if "stop" in h:
            leaves.append((n, h["stop"], h.get("stop_detail")))
    if not hops and res.get("start_stop"):
        leaves.append((seed, res["start_stop"]["stop"], res["start_stop"].get("stop_detail")))
    return nodes, leaves


def new_acc():
    return defaultdict(list)


def grade_cell(cell, graph, syms, lidx, src, seeds, variant_kinds=True, log=None, alt_graph=None):
    """Run the lidx side for every seed and grade it. Returns the cell result dict and the committed lists."""
    lang = cell["lang"]
    ids = seed_ids(syms)
    is_root = test_root_predicate(lang, cell["variant"], src)
    tests_graded = lang in ("go", "rust") and cell["variant"] == "with_tests"
    roots = {n for n in graph.judged if is_root(graph.info[n])} if tests_graded else set()
    rows = []
    lists = {"extra": [], "missing": []}
    for si, seed in enumerate(seeds):
        node = seed_node(graph, seed)
        sid_list = ids.get(seed["file"] + ":%d" % seed["line"]) or []
        rec = {"seed": seed, "node": node}
        rows.append(rec)
        if not sid_list:
            rec["lidx_symbol"] = None
            continue
        sid = min(sid_list)
        rec["lidx_symbol"] = syms[sid]["qualname"]
        rec["up"], rec["up_default"], rec["all_kinds"], rec["tests"], rec["down"] = {}, {}, {}, {}, {}
        for k in KS:
            ocl = graph.closure(node, k)
            if k is None:
                rec["deeper_than_cap"] = any(d > LIDX_MAX for d in ocl.values())
            oup = {n for n in ocl if n not in graph.unindexed}
            alt = None
            if alt_graph is not None:
                ocl_nc = alt_graph.closure(node, k)
                alt = (alt_graph, {n for n in ocl_nc if n not in alt_graph.unindexed})
            # graded: the like-for-like call: kinds=[CALLS, DISPATCH], because the oracle is a call graph and cannot judge other dependents
            rec["up"][kname(k)] = up_one(rec, node, k, oup, len(ocl) - len(oup), graph, syms, lidx, sid, lists, seed, CALL_KINDS, alt, refine=True)
            if k == 3 and lang in ("go", "ts"):
                rec["up"]["3"]["dispatch"] = dispatch_one(oup, graph, syms, lidx, sid, node)
            if k == 3:
                rec["up_default"] = {"3": plain_one(oup, graph, syms, lidx, sid, node, depth=3), "default": plain_one(oup, graph, syms, lidx, sid, node)}
            if variant_kinds:
                # the default call (every edge kind): its answer beyond the calls is reported, not graded
                a = rec["all_kinds"][kname(k)] = up_one(rec, node, k, oup, len(ocl) - len(oup), graph, syms, lidx, sid, None, seed, None)
                c = rec["up"][kname(k)]
                if "error" not in a and "error" not in c:
                    a["fp_beyond_calls"] = len(set(a["_extra_all"]) - c["_lset"])
            if tests_graded:
                otests = {n for n in oup if n in roots}
                alt_tests = None
                if alt_graph is not None and lang == "go":
                    alt_tests = (alt_graph, {n for n in alt_graph.closure(node, k) if n in roots and n not in alt_graph.unindexed})
                rec["tests"][kname(k)] = tests_one(otests, graph, syms, lidx, sid, k, node, roots, alt_tests)
            rec["down"][kname(k)] = down_one(rec, node, k, graph, syms, lidx, sid, alt_graph)
        if log and (si + 1) % 25 == 0:
            log("      %d/%d seeds" % (si + 1, len(seeds)))
    return rows, lists, tests_graded, len(roots)


def refine_extras(graph, syms, lidx, sid, k, kinds, extra):
    """Split the extras of an upstream answer into those whose path to the seed the oracle can judge (graph.path_ok) and those it cannot."""
    if not extra:
        return [], []
    p = impact_params(sid, k, False, kinds)
    p["include_paths"] = True
    res = lidx.call("analyze_impact", p)
    if "_error" in res:
        return list(extra), []
    wanted = set(extra)
    steps = {}
    for e in res.get("affected", []):
        sy = syms.get(e["symbol"]["id"])
        n = graph.cn(sy["key"]) if sy and "key" in sy else None
        if n in wanted:
            steps[n] = (e.get("path") or {}).get("steps", [])
    keep = [x for x in extra if graph.path_ok(steps.get(x, []))]
    return keep, [x for x in extra if x not in set(keep)]


def up_one(rec, node, k, oup, o_unindexed, graph, syms, lidx, sid, lists, seed, kinds, alt=None, refine=False):
    res = lidx.call("analyze_impact", impact_params(sid, k, False, kinds))
    if "_error" in res:
        return {"error": res["_error"]}
    lset = members(res, syms, graph, exclude={node})
    tp, fp, fn, ungr, extra, missing = compare(oup, lset, graph)
    dropped = []
    if refine:
        extra, dropped = refine_extras(graph, syms, lidx, sid, k, kinds, extra)
        fp, ungr = len(extra), ungr + len(dropped)
    out = {"oracle": len(oup), "oracle_unindexed": o_unindexed, "lidx": len(lset), "tp": tp, "fp": fp, "fn": fn, "ungradable": ungr,
           "truncated": bool(res.get("truncated")), "lower_bound": bool(res["lower_bound"]["is_lower_bound"]),
           "unresolved_count": res["lower_bound"]["unresolved_count"]}
    if alt is not None:
        tp2, fp2, fn2, _, extra2, miss2 = compare(alt[1], lset, alt[0])
        out["alt"] = {"oracle": len(alt[1]), "tp": tp2, "fp": len([x for x in extra2 if x not in set(dropped)]), "fn": fn2}
        out["_alt_missing_all"] = miss2
    out["ungradable_path"] = len(dropped)
    out["extra"] = [graph_name(graph, x) for x in extra[:EXAMPLES]]
    out["missing"] = [graph_name(graph, x) for x in missing[:EXAMPLES]]
    out["_extra_all"] = extra
    out["_missing_all"] = missing
    out["_lset"] = lset
    return out


def dispatch_one(oup, graph, syms, lidx, sid, node):
    """lidx's interface-dispatch fan-out counted: the same k = 3 call with paths; a member whose path crosses a hop bound by the dispatch
    query (resolution_kind interface_dispatch) is a dispatch-derived claim, graded like any other member."""
    p = impact_params(sid, 3, False, CALL_KINDS)
    p["include_paths"] = True
    res = lidx.call("analyze_impact", p)
    if "_error" in res:
        return {"error": res["_error"]}
    via, ok = set(), set()
    for e in res.get("affected", []):
        steps = (e.get("path") or {}).get("steps", [])
        if any(st.get("resolution_kind") == "interface_dispatch" for st in steps):
            sy = syms.get(e["symbol"]["id"])
            if sy and "key" in sy:
                n = graph.cn(sy["key"])
                via.add(n)
                if graph.path_ok(steps):
                    ok.add(n)
    via.discard(node)
    right = len(via & oup)
    wrong = len({x for x in via - oup if graph.gradable(x) and x in ok})
    return {"members": len(via), "right": right, "wrong": wrong}


def plain_one(oup, graph, syms, lidx, sid, node, depth=None):
    """What a plain call returns: `analyze_impact(id, direction=up, max_depth=depth, include_tests=true)` with every other parameter at its
    default (response cap 30,000 bytes, paths included, all three layers). `depth=None` leaves `max_depth` out too: what the default depth gives."""
    params = {"id": sid, "direction": "up", "include_tests": True}
    if depth is not None:
        params["max_depth"] = depth
    res = lidx.call("analyze_impact", params)
    if "_error" in res:
        return {"error": res["_error"]}
    res = dict(res, affected=[e for e in res.get("affected", []) if e["relationship"] != "HISTORICAL" and not e["relationship"].startswith("CO")])
    lset = members(res, syms, graph, exclude={node})
    tp, fp, fn, ungr, extra, missing = compare(oup, lset, graph)
    return {"oracle": len(oup), "lidx": len(lset), "tp": tp, "fp": fp, "fn": fn, "ungradable": ungr, "truncated": bool(res.get("truncated")),
            "depth_limited": bool(res.get("depth_limited")), "beyond_depth": res.get("beyond_depth", 0), "max_depth": (res.get("config") or {}).get("max_depth"),
            "bytes": len(json.dumps(res)),
            "lower_bound": bool(res["lower_bound"]["is_lower_bound"]), "unresolved_count": res["lower_bound"]["unresolved_count"]}


def tests_one(otests, graph, syms, lidx, sid, k, node, roots, alt_tests=None):
    res = lidx.call("analyze_impact", impact_params(sid, k, True))
    if "_error" in res:
        return {"error": res["_error"]}
    lset = members(res, syms, graph, exclude={node})
    # a lidx test entry is graded like any member: it is right when the oracle has it as a test root reaching the seed, wrong when it
    # is a function the oracle analysed that is not such a root, ungradable when the oracle did not analyse it
    tp, fp, fn, ungr, extra, missing = compare(otests, lset, graph)
    # The test layer follows every edge kind (it cannot be restricted), so a test can reach the seed through a reference or a use that a call graph
    # cannot confirm. A wrong test whose path to the seed crosses a non-call hop is a dependent beyond the calls: counted apart, not wrong.
    noncall, unjudged = set(), set()
    if extra:
        p = impact_params(sid, k, True)
        p["include_paths"] = True
        res2 = lidx.call("analyze_impact", p)
        wanted = set(extra)
        for e in res2.get("affected", []):
            sy = syms.get(e["symbol"]["id"])
            n = graph.cn(sy["key"]) if sy and "key" in sy else None
            steps = (e.get("path") or {}).get("steps", []) if n in wanted else []
            if n in wanted and any(st["edge_kind"] not in CALL_KINDS for st in steps):
                noncall.add(n)
            elif n in wanted and not graph.path_ok(steps):
                unjudged.add(n)
    extra = [x for x in extra if x not in noncall and x not in unjudged]
    ungr += len(unjudged)
    fp = len(extra)
    nonroot = sum(1 for x in extra if x not in roots)
    bench = sum(1 for x in missing if graph.info[x]["name"].startswith(("Benchmark", "Fuzz")))
    out = {"oracle": len(otests), "lidx": len(lset), "tp": tp, "fp": fp, "fn": fn, "ungradable": ungr, "fp_not_root": nonroot, "fn_bench_fuzz": bench,
           "fp_noncall": len(noncall),
           "truncated": bool(res.get("truncated")) or res["layers"]["test"].get("truncated", False),
            "extra": [graph_name(graph, x) for x in extra[:EXAMPLES]], "missing": [graph_name(graph, x) for x in missing[:EXAMPLES]],
            "empty_correct": len(otests) == 0 and len(lset) == 0}
    if alt_tests is not None:
        # Go: the same test reach against the closure without function-value edges (RTA joins a call of a function value to every address-taken
        # function of its signature, so a benchmark that calls `handler(..)` "reaches" unrelated functions); a diagnostic beside the verdict
        tp2, _, fn2, _, _, miss2 = compare(alt_tests[1], lset, alt_tests[0])
        out["alt"] = {"oracle": len(alt_tests[1]), "tp": tp2, "fn": fn2,
                      "fn_bench_fuzz": sum(1 for x in miss2 if alt_tests[0].info[x]["name"].startswith(("Benchmark", "Fuzz")))}
    return out


def down_one(rec, node, k, graph, syms, lidx, sid, alt_graph=None):
    res = lidx.call("trace_flow", trace_params(sid, k))
    if "_error" in res or "trace" not in res:
        return {"error": res.get("_error", "no trace (%s)" % ",".join(sorted(res)[:4]))}
    ocl = graph.closure(node, k, rev=False)
    odown = {n for n in ocl if n not in graph.unindexed}
    nodes, leaves = trace_nodes(res, syms, graph, node)
    tp, fp, fn, ungr, extra, missing = compare(odown, nodes, graph)
    # data lost: the byte budget cut settled hops, or a cap refused some. The `max_hops` ceiling is not a loss: the oracle closure is cut at
    # the same depth (k), and for the unbounded column lidx's ceiling is 10
    trunc = bool(res.get("budget", {}).get("truncated")) or bool(res.get("truncation_reason"))
    out = {"oracle": len(odown), "lidx": len(nodes), "tp": tp, "fp": fp, "fn": fn, "ungradable": ungr, "truncated": trunc,
           "depth_limited": bool(res.get("depth_limited")), "extra": [graph_name(graph, x) for x in extra[:EXAMPLES]],
           "missing": [graph_name(graph, x) for x in missing[:EXAMPLES]]}
    # leaf honesty: a leaf marked no_callees where the oracle has an in-scope callee is a false leaf; one marked unresolved_calls where
    # the oracle shows nothing missing (no in-scope callee) is a gap with no in-repo callee left (not a claim of wrongness). truncated / depth_limit leaves are excluded from both.
    lv = {"no_callees": [0, 0], "unresolved_calls": [0, 0]}  # reason -> [gradable, wrong]
    ext_only = 0  # such gaps whose every named unresolved call lidx itself flags `external`
    only_closure = 0
    ex = []
    for n, stop, detail in leaves:
        if stop not in lv or not graph.gradable(n):
            continue
        callees = {c for c in graph.fwd.get(n, ()) if c not in graph.unindexed and c != n}
        wrong = bool(callees) if stop == "no_callees" else not callees
        lv[stop][0] += 1
        lv[stop][1] += wrong
        if wrong and stop == "no_callees" and alt_graph is not None and alt_graph.lang == "go":
            # Go: every oracle callee of the leaf is a function-literal edge (the call of a function value, keyed at the declaration that built it)
            only_closure += not {c for c in alt_graph.fwd.get(n, ()) if c not in alt_graph.unindexed and c != n}
        if wrong and stop == "unresolved_calls":
            top = (detail or {}).get("top") or []
            ext_only += bool(top) and all(isinstance(t, (list, tuple)) and len(t) > 1 and t[1] == "external" for t in top)
        if wrong and len(ex) < EXAMPLES:
            ex.append({"leaf": graph_name(graph, n), "stop": stop, "oracle_callees": [graph_name(graph, c) for c in sorted(callees)[:3]]})
    out["leaves"] = {k2: {"graded": v[0], "wrong": v[1]} for k2, v in lv.items()}
    out["leaves"]["unresolved_calls"]["wrong_external_only"] = ext_only
    out["leaves"]["no_callees"]["wrong_closure_only"] = only_closure
    out["leaf_examples"] = ex
    out["leaf_total"] = len(leaves)
    return out


def graph_name(graph, node):
    inf = graph.info.get(node)
    if inf:
        return "%s:%d %s" % (inf["file"], inf["line"], inf["name"])
    return node


# ------------------------------------------------------------------ aggregation
def aggregate(rows, tests_graded):
    """Tables of one cell from the per-seed records."""
    out = {"seeds": len(rows), "seeds_lidx_unindexed": sum(1 for r in rows if r.get("lidx_symbol") is None)}
    live = [r for r in rows if r.get("lidx_symbol") is not None]
    out["seeds_graded"] = len(live)

    def family(name, ks=("3", "inf"), variant_field=None):
        res = {}
        for kn in ks:
            es = [r[name][kn] for r in live if kn in r[name] and "error" not in r[name][kn]]
            errs = sum(1 for r in live if kn in r[name] and "error" in r[name][kn])
            trunc = sum(1 for e in es if e["truncated"])
            tuples = [(e["tp"], e["fp"], e["fn"]) for e in es]
            # pooled recall leaves out the seeds lidx truncated (its answer is a prefix by design); pooled precision keeps them
            pp = pooled(tuples)
            rec_rows = [(e["tp"], e["fp"], e["fn"]) for e in es if not e["truncated"]]
            pr = pooled(rec_rows)
            d = {"seeds": len(es), "errors": errs, "truncated_seeds": trunc, "tp": pp["tp"], "fp": pp["fp"], "fn_untruncated": pr["fn"],
                 "precision": pp["precision"], "precision_ci": pp["precision_ci"],
                 "recall": pr["recall"], "recall_ci": pr["recall_ci"], "oracle_members": sum(e["oracle"] for e in es),
                 "oracle_unindexed_members": sum(e.get("oracle_unindexed", 0) for e in es),
                 "ungradable": sum(e["ungradable"] for e in es)}
            d.update(macro([(e["tp"], e["fp"], e["fn"], e["truncated"]) for e in es]))
            res[kn] = d
        return res

    out["up"] = family("up")
    out["up_default"] = family("up_default", ks=("3", "default"))
    for kn, d in out["up_default"].items():
        es = [r["up_default"][kn] for r in live if kn in r["up_default"] and "error" not in r["up_default"][kn]]
        d["depth_limited_seeds"] = sum(1 for e in es if e.get("depth_limited"))
        d["max_depth"] = sorted({e.get("max_depth") for e in es if e.get("max_depth") is not None})
        d["mean_bytes"] = round(sum(e.get("bytes", 0) for e in es) / len(es)) if es else 0
    ds = [r["up"]["3"]["dispatch"] for r in live if "dispatch" in r["up"].get("3", {}) and "error" not in r["up"]["3"]["dispatch"]]
    out["dispatch"] = {"seeds": len(ds), "members": sum(d["members"] for d in ds), "right": sum(d["right"] for d in ds), "wrong": sum(d["wrong"] for d in ds),
                       "seeds_with_dispatch": sum(1 for d in ds if d["members"])} if ds else {}
    nc = {}
    for kn in ("3", "inf"):
        es = [r["up"][kn]["alt"] for r in live if kn in r["up"] and "alt" in r["up"][kn]]
        if es:
            ok = [r["up"][kn]["alt"] for r in live if "alt" in r["up"].get(kn, {}) and not r["up"][kn]["truncated"]]
            a, b = pooled([(e["tp"], e["fp"], e["fn"]) for e in es]), pooled([(e["tp"], e["fp"], e["fn"]) for e in ok])
            nc[kn] = {"oracle_members": sum(e["oracle"] for e in es), "precision": a["precision"], "precision_ci": a["precision_ci"],
                      "recall": b["recall"], "recall_ci": b["recall_ci"]}
    out["up_alt"] = nc
    out["all_kinds"] = family("all_kinds") if any(r["all_kinds"] for r in live) else {}
    for kn, d in out["all_kinds"].items():
        d["fp_beyond_calls"] = sum(r["all_kinds"][kn].get("fp_beyond_calls", 0) for r in live if kn in r["all_kinds"] and "error" not in r["all_kinds"][kn])
    # honesty of lower_bound against per-seed completeness (missing == 0), seeds lidx did not truncate
    hon = {}
    for kn in ("3", "inf"):
        t = {"lb_true_incomplete": 0, "lb_true_complete": 0, "lb_false_incomplete": 0, "lb_false_complete": 0}
        for r in live:
            e = (r.get("all_kinds") or {}).get(kn) or r["up"].get(kn)  # the default call's lower_bound is the one users see
            if not e or "error" in e or e["truncated"]:
                continue
            key = "lb_%s_%s" % ("true" if e["lower_bound"] else "false", "incomplete" if e["fn"] > 0 else "complete")
            t[key] += 1
        hon[kn] = t
    out["lower_bound_honesty"] = hon
    # what the default depth hides
    hide_o = hide_l = nz_o = nz_l = hide_a = nz_a = 0
    for r in live:
        a, b = r["up"].get("3"), r["up"].get("inf")
        if not a or not b or "error" in a or "error" in b:
            continue
        if "alt" in a and "alt" in b and b["alt"]["oracle"] > 0:
            nz_a += 1
            hide_a += (b["alt"]["oracle"] - a["alt"]["oracle"]) * 2 > b["alt"]["oracle"]
        # oracle: tp+fn of each k is the oracle set size; lidx: tp+fp+ungradable is its answer size
        if b["oracle"] > 0:
            nz_o += 1
            hide_o += (b["oracle"] - a["oracle"]) * 2 > b["oracle"]
        if b["lidx"] > 0 and not b["truncated"]:
            nz_l += 1
            hide_l += (b["lidx"] - a["lidx"]) * 2 > b["lidx"]
    out["depth_hides_over_half"] = {"oracle_seeds": nz_o, "oracle_hidden": hide_o, "oracle_share": ratio(hide_o, nz_o),
                                    "lidx_seeds": nz_l, "lidx_hidden": hide_l, "lidx_share": ratio(hide_l, nz_l),
                                    "alt_seeds": nz_a, "alt_hidden": hide_a, "alt_share": ratio(hide_a, nz_a)}
    out["oracle_deeper_than_cap_seeds"] = sum(1 for r in live if r.get("deeper_than_cap"))
    if tests_graded:
        out["tests"] = test_family(live)
    out["down"] = down_family(live)
    return out


def test_family(live):
    res = {}
    for kn in ("3", "inf"):
        es = [r["tests"][kn] for r in live if kn in r["tests"] and "error" not in r["tests"][kn]]
        pp = pooled([(e["tp"], e["fp"], e["fn"]) for e in es])
        pr = pooled([(e["tp"], e["fp"], e["fn"]) for e in es if not e["truncated"]])
        d = {"seeds": len(es), "truncated_seeds": sum(1 for e in es if e["truncated"]), "tp": pp["tp"], "fp": pp["fp"], "fn_untruncated": pr["fn"],
             "precision": pp["precision"], "precision_ci": pp["precision_ci"], "recall": pr["recall"], "recall_ci": pr["recall_ci"],
             "fp_not_root": sum(e["fp_not_root"] for e in es), "fn_bench_fuzz": sum(e["fn_bench_fuzz"] for e in es), "fp_noncall": sum(e["fp_noncall"] for e in es), "ungradable": sum(e["ungradable"] for e in es),
             "oracle_pairs": sum(e["oracle"] for e in es),
             "empty_correct": sum(1 for e in es if e["empty_correct"]),
             "empty_oracle_lidx_nonempty": sum(1 for e in es if e["oracle"] == 0 and e["lidx"] > 0),
             "nonempty_oracle_lidx_empty": sum(1 for e in es if e["oracle"] > 0 and e["lidx"] == 0)}
        d.update(macro([(e["tp"], e["fp"], e["fn"], e["truncated"]) for e in es]))
        alts = [e["alt"] for e in es if "alt" in e and not e["truncated"]]
        if alts:
            pa = pooled([(a["tp"], 0, a["fn"]) for a in alts])
            d["alt_recall"], d["alt_recall_ci"] = pa["recall"], pa["recall_ci"]
            d["alt_oracle_pairs"] = sum(a["oracle"] for a in alts)
            d["alt_fn_bench_fuzz"] = sum(a["fn_bench_fuzz"] for a in alts)
        res[kn] = d
    return res


def down_family(live):
    res = {}
    for kn in ("3", "inf"):
        es = [r["down"][kn] for r in live if kn in r["down"] and "error" not in r["down"][kn]]
        pp = pooled([(e["tp"], e["fp"], e["fn"]) for e in es])
        pr = pooled([(e["tp"], e["fp"], e["fn"]) for e in es if not e["truncated"]])
        d = {"seeds": len(es), "truncated_seeds": sum(1 for e in es if e["truncated"]), "precision": pp["precision"], "precision_ci": pp["precision_ci"],
             "recall": pr["recall"], "recall_ci": pr["recall_ci"], "tp": pp["tp"], "fp": pp["fp"], "fn_untruncated": pr["fn"]}
        d.update(macro([(e["tp"], e["fp"], e["fn"], e["truncated"]) for e in es]))
        leaves = {}
        for reason in ("no_callees", "unresolved_calls"):
            g = sum(e["leaves"][reason]["graded"] for e in es)
            w = sum(e["leaves"][reason]["wrong"] for e in es)
            leaves[reason] = {"graded": g, "wrong": w, "rate": ratio(w, g), "ci": wilson(w, g)}
            if reason == "unresolved_calls":
                leaves[reason]["wrong_external_only"] = sum(e["leaves"][reason].get("wrong_external_only", 0) for e in es)
            else:
                leaves[reason]["wrong_closure_only"] = sum(e["leaves"][reason].get("wrong_closure_only", 0) for e in es)
        d["leaves"] = leaves
        d["leaf_total"] = sum(e["leaf_total"] for e in es)
        res[kn] = d
    return res


def f1(e):
    p = e["tp"] / (e["tp"] + e["fp"]) if e["tp"] + e["fp"] else None
    r = e["tp"] / (e["tp"] + e["fn"]) if e["tp"] + e["fn"] else None
    if p is None and r is None:
        return 1.0
    if p is None or r is None:
        return 0.0
    return 0.0 if p + r == 0 else 2 * p * r / (p + r)


def worst_seeds(rows, graph, kn, n=10):
    """The n seeds with the lowest per-seed F1 (a seed with no members on either side scores 1), with their missing and extra symbols."""
    cand = []
    for r in rows:
        e = r.get("up", {}).get(kn)
        if not e or "error" in e:
            continue
        cand.append((f1(e), -(e["fp"] + e["fn"]), r["seed"]["file"], r["seed"]["line"], r, e))
    cand.sort(key=lambda t: t[:4])
    out = []
    for score, _, _, _, r, e in cand[:n]:
        out.append({"seed": "%s:%d %s" % (r["seed"]["file"], r["seed"]["line"], r["seed"]["name"]), "stratum": r["seed"]["stratum"], "f1": round(score, 3),
                    "tp": e["tp"], "extra_n": e["fp"], "missing_n": e["fn"], "lidx": e["lidx"], "oracle": e["oracle"], "truncated": e["truncated"],
                    "extra": [graph_name(graph, x) for x in e["_extra_all"][:10]], "missing": [graph_name(graph, x) for x in e["_missing_all"][:10]]})
    return out


def pair_lists(rows, graph):
    """Committed pairs (seed, k, member): up to LIST_CAP extra and missing per seed and k; `alt` the missing pairs of the diagnostic closure."""
    extra, missing, alt = [], [], []
    for r in rows:
        for kn, e in r.get("up", {}).items():
            if "error" in e:
                continue
            sd = "%s:%d" % (r["seed"]["file"], r["seed"]["line"])
            for x in e["_extra_all"][:LIST_CAP]:
                extra.append({"seed": sd, "seed_name": r["seed"]["name"], "k": kn, "member": graph_name(graph, x), "node": x})
            for x in e["_missing_all"][:LIST_CAP]:
                missing.append({"seed": sd, "seed_name": r["seed"]["name"], "k": kn, "member": graph_name(graph, x), "node": x})
            for x in e.get("_alt_missing_all", [])[:LIST_CAP]:
                alt.append({"seed": sd, "seed_name": r["seed"]["name"], "k": kn, "member": graph_name(graph, x), "node": x})
    return extra, missing, alt


def strip_private(rows):
    for r in rows:
        for fam in ("up", "all_kinds"):
            for e in r.get(fam, {}).values():
                e.pop("_extra_all", None)
                e.pop("_missing_all", None)
                e.pop("_alt_missing_all", None)
                e.pop("_lset", None)
    return rows
