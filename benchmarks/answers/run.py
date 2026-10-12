#!/usr/bin/env python3
"""lidx answer benchmark runner.

Scores a lidx binary against developer questions whose answers were derived
from source code (never from lidx).  Python 3 stdlib only.  See README.md.

    run.py --binary BIN --suite suites/x.json [--suite ...] [--workdir DIR] [--json out.json]
    run.py --compare a.json b.json
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_WORKDIR = os.path.join(HERE, ".work")

# explain_symbol / dead_symbols / trace_flow all cap response size server-side
# (~200 KB) regardless of what we ask for, so we ask for a lot and then verify
# completeness from the response itself (callers_total, pagination).
BIG = 50_000_000
TRACE_KINDS = ["CALLS", "RPC_IMPL", "RPC_CALL", "HTTP_CALL", "HTTP_ROUTE",
               "CHANNEL_PUBLISH", "CHANNEL_SUBSCRIBE"]
LANG_EXT = {
    "csharp": [".cs"], "python": [".py"], "typescript": [".ts", ".tsx"],
    "javascript": [".js", ".jsx", ".mjs", ".cjs"], "rust": [".rs"], "go": [".go"],
    "lua": [".lua"], "sql": [".sql"], "proto": [".proto"], "bicep": [".bicep"],
    "yaml": [".yaml", ".yml"], "markdown": [".md"],
}


class BenchError(Exception):
    """A question could not be answered reliably (reported loudly, scored 0)."""


class Fatal(Exception):
    pass


# --------------------------------------------------------------------------
# repo + index preparation
# --------------------------------------------------------------------------

def sh(cmd, cwd=None, timeout=1800):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise Fatal("command failed (%d): %s\n%s" % (p.returncode, " ".join(cmd), p.stderr.strip()[-2000:]))
    return p.stdout


def prepare_repo(suite, suite_path, workdir):
    repo = suite.get("repo") or {}
    if "path" in repo:
        raw = repo["path"]
        expanded = os.path.expandvars(os.path.expanduser(raw))
        if "$" in expanded:
            raise Fatal("suite %s: environment variable in repo.path %r is not set" % (suite["name"], raw))
        if not os.path.isabs(expanded):
            expanded = os.path.join(os.path.dirname(os.path.abspath(suite_path)), expanded)
        expanded = os.path.normpath(expanded)
        if not os.path.isdir(expanded):
            raise Fatal("suite %s: repo path does not exist: %s" % (suite["name"], expanded))
        return expanded
    if "url" in repo:
        commit = repo.get("commit")
        if not commit:
            raise Fatal("suite %s: repo.url requires repo.commit (pinned sha)" % suite["name"])
        dest = os.path.join(workdir, "repos", "%s-%s" % (suite["name"], commit[:12]))
        if not os.path.isdir(os.path.join(dest, ".git")):
            os.makedirs(dest, exist_ok=True)
            print("  cloning %s @ %s ..." % (repo["url"], commit[:12]), flush=True)
            sh(["git", "init", "-q"], cwd=dest)
            sh(["git", "remote", "add", "origin", repo["url"]], cwd=dest)
            try:
                sh(["git", "fetch", "-q", "--depth", "1", "origin", commit], cwd=dest)
            except Fatal:
                sh(["git", "fetch", "-q", "origin"], cwd=dest)
            sh(["git", "checkout", "-q", commit], cwd=dest)
        head = sh(["git", "rev-parse", "HEAD"], cwd=dest).strip()
        if head != commit and not head.startswith(commit):
            raise Fatal("suite %s: cached clone at %s is %s, expected %s" % (suite["name"], dest, head, commit))
        return dest
    raise Fatal("suite %s: repo needs either {path} or {url, commit}" % suite["name"])


def binary_label(binary):
    base = re.sub(r"[^A-Za-z0-9_.-]", "_", os.path.basename(binary))
    return "%s-%s" % (base, hashlib.md5(os.path.abspath(binary).encode()).hexdigest()[:6])


class Lidx:
    """Thin wrapper around `<bin> request` plus read-only db access."""

    def __init__(self, binary, repo, db):
        self.binary, self.repo, self.db = binary, repo, db
        self._con = None

    def request(self, method, params, timeout=600):
        p = subprocess.run(
            [self.binary, "request", "--repo", self.repo, "--db", self.db,
             "--method", method, "--params", json.dumps(params)],
            capture_output=True, text=True, timeout=timeout)
        msg = None
        for line in p.stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    cand = json.loads(line)
                except ValueError:
                    continue
                if isinstance(cand, dict) and ("result" in cand or "error" in cand):
                    msg = cand
                    break
        if msg is None:
            raise BenchError("%s: no JSON response (exit %d): %s" % (method, p.returncode, (p.stderr or p.stdout)[-400:]))
        if msg.get("error"):
            raise BenchError("%s: %s" % (method, json.dumps(msg["error"])[:500]))
        res = msg["result"]
        if isinstance(res, dict) and set(res) == {"results"} and isinstance(res["results"], dict):
            res = res["results"]  # tolerate wrapper differences between builds
        return res

    @property
    def con(self):
        if self._con is None:
            self._con = sqlite3.connect("file:%s?mode=ro" % self.db, uri=True)
            self._con.row_factory = sqlite3.Row
        return self._con

    def close(self):
        if self._con:
            self._con.close()
            self._con = None


def index_repo(binary, repo, db):
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(db + suffix)
        except FileNotFoundError:
            pass
    os.makedirs(os.path.dirname(db), exist_ok=True)
    t = time.time()
    p = subprocess.run([binary, "reindex", "--repo", repo, "--db", db],
                       capture_output=True, text=True, timeout=7200)
    if p.returncode != 0:
        raise Fatal("reindex failed (%d): %s" % (p.returncode, p.stderr[-1500:]))
    return time.time() - t


# --------------------------------------------------------------------------
# symbol table + location mapping
# --------------------------------------------------------------------------

class SymbolMap:
    def __init__(self, lx):
        self.lx = lx
        self.repo = lx.repo
        con = lx.con
        gv = con.execute("SELECT MAX(graph_version) FROM symbols").fetchone()[0]
        rows = con.execute(
            "SELECT s.id, f.path, f.language, s.kind, s.name, s.qualname, s.start_line, s.end_line "
            "FROM symbols s JOIN files f ON f.id = s.file_id "
            "WHERE s.graph_version = ? AND s.kind != 'external' AND f.deleted_version IS NULL", (gv,)).fetchall()
        self.by_id = {}
        self.by_file = {}
        for r in rows:
            d = dict(r)
            self.by_id[d["id"]] = d
            self.by_file.setdefault(d["path"], []).append(d)
        self._src = {}

    def _lines(self, path):
        if path not in self._src:
            try:
                with open(os.path.join(self.repo, path), encoding="utf-8", errors="replace") as fh:
                    self._src[path] = fh.read().split("\n")
            except OSError:
                self._src[path] = []
        return self._src[path]

    def decl_line(self, sym):
        """Line of the declaration's name token (skipping attribute/decorator lines)."""
        if "_dl" in sym:
            return sym["_dl"]
        lines = self._lines(sym["path"])
        name = sym["name"].lstrip("~")
        best = sym["start_line"]
        if lines and re.fullmatch(r"[\w$]+", name or ""):
            pat = re.compile(r"(?<![\w$])" + re.escape(name) + r"(?![\w$])")
            fallback = None
            for ln in range(sym["start_line"], min(sym["end_line"], sym["start_line"] + 25) + 1):
                if ln - 1 >= len(lines):
                    break
                text = lines[ln - 1]
                if not pat.search(text):
                    continue
                stripped = text.lstrip()
                if stripped.startswith(("[", "@", "//", "#", "/*", "*")):
                    fallback = fallback or ln
                    continue
                best = ln
                break
            else:
                best = fallback or sym["start_line"]
        sym["_dl"] = best
        return best

    def find(self, loc):
        """Symbol whose declaration is at loc, or None."""
        cands = self.by_file.get(loc["file"], [])
        hits = [s for s in cands if self.decl_line(s) == loc["line"]]
        if not hits:
            hits = [s for s in cands if s["start_line"] == loc["line"]]
        if not hits:
            return None
        hits.sort(key=lambda s: (s["kind"] in ("namespace", "module", "file"),
                                 s["end_line"] - s["start_line"], s["id"]))
        return hits[0]

    def keys(self, sym):
        """All (file, line) aliases under which this symbol's decl may be named."""
        return {(sym["path"], self.decl_line(sym)), (sym["path"], sym["start_line"])}


def lockey(loc):
    return (loc["file"], int(loc["line"]))


def fmtkey(k):
    return "%s:%s" % (k[0], k[1])


# --------------------------------------------------------------------------
# question runners.  Each returns a record dict (see aggregate()).
# --------------------------------------------------------------------------

def call_sites_for(lx, target_id, entries):
    """Resolve call-site (file, line) pairs for explain_symbol caller entries.

    No RPC method exposes the call-site line, so lines come from the `edges`
    table (evidence_start_line..evidence_end_line), read-only.  The RPC decides
    WHICH callers exist; the db only supplies WHERE inside them the call happens.
    Returns (path, start, end) spans: a multi-line call chain is recorded from
    the start of the chain, so the expected name-token line may lie inside it.
    """
    sites, unlocated = set(), []
    con = lx.con
    # `new T()` binds to T's explicit constructor, and a class's callers
    # aggregate its members, so a class target's sites are edges to the class
    # or its constructor symbol.
    qual = con.execute("SELECT qualname FROM symbols WHERE id = ?", (target_id,)).fetchone()
    target_ids = [target_id]
    if qual:
        target_ids += [r[0] for r in con.execute(
            "SELECT id FROM symbols WHERE qualname IN (?, ?) AND kind = 'method' "
            "AND graph_version = (SELECT graph_version FROM symbols WHERE id = ?)",
            (qual[0] + ".constructor", qual[0] + "..ctor", target_id))]
    marks = ",".join("?" * len(target_ids))
    for ent in entries:
        sym = ent.get("symbol") or {}
        cid = sym.get("id")
        kind = ent.get("edge_kind")
        rows = con.execute(
            "SELECT f.path, e.evidence_start_line AS ln, e.evidence_end_line AS ln_end FROM edges e "
            "JOIN files f ON f.id = e.file_id "
            "WHERE e.source_symbol_id = ? AND e.target_symbol_id IN (%s) AND (? IS NULL OR e.kind = ?) "
            "AND e.evidence_start_line IS NOT NULL" % marks, (cid, *target_ids, kind, kind)).fetchall()
        if not rows and ent.get("evidence"):
            rows = con.execute(
                "SELECT f.path, e.evidence_start_line AS ln, e.evidence_end_line AS ln_end FROM edges e "
                "JOIN files f ON f.id = e.file_id "
                "WHERE e.source_symbol_id = ? AND e.evidence_snippet = ? AND e.evidence_start_line IS NOT NULL",
                (cid, ent["evidence"])).fetchall()
        if rows:
            for r in rows:
                sites.add((r["path"], r["ln"], max(r["ln"], r["ln_end"] or r["ln"])))
        else:
            unlocated.append("%s (caller %s)" % (ent.get("evidence"), sym.get("qualname")))
    return sites, unlocated


def get_callers(lx, sm, target_sym):
    res = lx.request("explain_symbol", {"id": target_sym["id"], "sections": ["callers"],
                                        "max_refs": 1000000, "max_bytes": BIG})
    if res.get("candidates") or res.get("overloads"):
        raise BenchError("explain_symbol returned an overload picker for id %s" % target_sym["id"])
    entries = res.get("callers")
    if entries is None:
        raise BenchError("explain_symbol response has no 'callers' (keys: %s)" % sorted(res))
    total = res.get("callers_total")
    if total is not None and total > len(entries):
        raise BenchError("callers truncated: %d of %d returned" % (len(entries), total))
    if total is None and (res.get("budget") or {}).get("truncated"):
        raise BenchError("callers response truncated by byte budget and no callers_total to verify")
    return entries


def set_record(q, expected, got, extra_info=None):
    exp, got = set(expected), set(got)
    tp = exp & got
    return {"expected": len(exp), "returned": len(got), "tp": len(tp),
            "missing": sorted(fmtkey(k) for k in exp - got),
            "extra": sorted(fmtkey(k) for k in got - exp), **(extra_info or {})}


def span_record(expected, spans):
    """Match expected (path, line) call sites to returned (path, start, end) spans.

    A span matches an expected line inside it; each side is used at most once,
    exact start-line hits first, then the nearest line inside the span.
    """
    exp_left = set(expected)
    extra, tp = [], 0
    ordered = sorted(spans, key=lambda sp: (sp[0], sp[2] - sp[1], sp[1]))
    for path, a, b in ordered:
        hits = sorted((l for (p, l) in exp_left if p == path and a <= l <= b), key=lambda l: abs(l - a))
        if hits:
            exp_left.discard((path, hits[0]))
            tp += 1
        else:
            extra.append((path, a))
    return {"expected": len(expected), "returned": len(spans), "tp": tp,
            "missing": sorted(fmtkey(k) for k in exp_left),
            "extra": sorted(fmtkey(k) for k in extra)}


def run_callers(lx, sm, q):
    target = sm.find(q["target"])
    expected = {lockey(l) for l in q.get("expect", [])}
    if target is None:
        return {"status": "target_not_indexed", "expected": len(expected), "returned": 0, "tp": 0,
                "missing": sorted(map(fmtkey, expected)), "extra": []}
    entries = get_callers(lx, sm, target)
    sites, unlocated = call_sites_for(lx, target["id"], entries)
    rec = span_record(expected, sites)
    if unlocated:
        rec["unlocated_callers"] = unlocated
        rec["returned"] += len(unlocated)
        rec["extra"] += ["(no call-site line) " + u for u in unlocated]
    rec["status"] = "ok"
    return rec


def run_impact(lx, sm, q):
    target = sm.find(q["target"])
    expected = {lockey(l) for l in q.get("expect", [])}
    if target is None:
        return {"status": "target_not_indexed", "expected": len(expected), "returned": 0, "tp": 0,
                "missing": sorted(map(fmtkey, expected)), "extra": []}
    entries = get_callers(lx, sm, target)
    syms = {}
    for ent in entries:
        s = ent.get("symbol") or {}
        full = sm.by_id.get(s.get("id"))
        if full:
            syms[full["id"]] = full
    # Match by either alias: normalise got to an expected key if any alias hits.
    got = set()
    for s in syms.values():
        aliases = sm.keys(s)
        hit = [a for a in aliases if a in expected]
        got.add(hit[0] if hit else (s["path"], sm.decl_line(s)))
    rec = set_record(q, expected, got)
    rec["status"] = "ok"
    return rec


def run_dead(lx, sm, q):
    scope = q.get("scope") or {}
    params = {"limit": 1000000, "include_unused_imports": False, "include_orphan_tests": False}
    if scope.get("path"):
        params["path"] = scope["path"]
    if scope.get("lang"):
        params["languages"] = [scope["lang"]]
    res = lx.request("dead_symbols", params)
    items = res.get("dead_symbols")
    if items is None:
        raise BenchError("dead_symbols response has no 'dead_symbols' (keys: %s)" % sorted(res))
    if len(items) >= params["limit"]:
        raise BenchError("dead_symbols hit the limit; result is truncated")
    prefix = scope.get("path")
    exts = tuple(LANG_EXT.get(scope.get("lang", ""), ())) if scope.get("lang") else None
    reported = set()
    for it in items:
        fp = it.get("file_path") or ""
        if prefix and not fp.startswith(prefix):
            continue  # older builds may ignore the path filter
        if exts and not fp.endswith(exts):
            continue
        full = sm.by_id.get(it.get("id"))
        reported.add((fp, it["start_line"]))
        if full:
            reported |= sm.keys(full)
        else:
            reported.add((fp, it["start_line"]))
    live = [lockey(l) for l in q.get("live", [])]
    dead = [lockey(l) for l in q.get("dead", [])]
    fa = [k for k in live if k in reported]
    det = [k for k in dead if k in reported]
    unindexed = [fmtkey(lockey(l)) for l in q.get("live", []) + q.get("dead", []) if sm.find(l) is None]
    return {"status": "ok", "live": len(live), "false_alarms": len(fa), "dead": len(dead),
            "detected": len(det),
            "false_alarm_items": sorted(map(fmtkey, fa)),
            "missed_dead": sorted(fmtkey(k) for k in dead if k not in reported),
            "unindexed_decls": unindexed, "reported_total": len(items)}


def trace_all(lx, start_id, max_hops, kinds):
    seen, pages, offset = {}, 0, 0
    while True:
        params = {"start_id": start_id, "direction": "downstream", "max_hops": max_hops,
                  "kinds": kinds, "max_bytes": BIG}
        if offset:
            params["trace_offset"] = offset
        res = lx.request("trace_flow", params)
        trace = res.get("trace")
        if trace is None:
            raise BenchError("trace_flow response has no 'trace' (keys: %s)" % sorted(res))
        for ent in trace:
            s = ent.get("symbol") or {}
            if s.get("id") is not None and ent.get("distance", 1) <= max_hops:
                seen[s["id"]] = s
        pages += 1
        if not res.get("truncated"):
            return seen
        if not trace:
            return seen  # truncated flag with an empty continuation page: done
        nxt = None
        for h in res.get("next_hops") or []:
            p = (h or {}).get("params") or {}
            if h.get("method") == "trace_flow" and "trace_offset" in p and p["trace_offset"] > offset:
                nxt = p["trace_offset"]
                break
        if nxt is None:
            raise BenchError("trace_flow truncated and no continuation offset offered")
        offset = nxt
        if pages > 500:
            raise BenchError("trace_flow pagination exceeded 500 pages")


def run_trace(lx, sm, q):
    start = sm.find(q["start"])
    must = [lockey(l) for l in q.get("must_reach", [])]
    avoid = [lockey(l) for l in q.get("must_not_reach", [])]
    if start is None:
        return {"status": "target_not_indexed", "reach_n": len(must), "reach_hit": 0,
                "avoid_n": len(avoid), "avoid_viol": 0, "missed": sorted(map(fmtkey, must)),
                "violations": []}
    hops = int(q.get("max_hops", 3))
    kinds = q.get("kinds") or TRACE_KINDS
    seen = trace_all(lx, start["id"], hops, kinds)
    reached = set()
    for sid, s in seen.items():
        if sid == start["id"]:
            continue
        full = sm.by_id.get(sid)
        if full:
            reached |= sm.keys(full)
        elif s.get("file_path"):
            reached.add((s["file_path"], s.get("start_line")))
    hit = [k for k in must if k in reached]
    viol = [k for k in avoid if k in reached]
    return {"status": "ok", "reach_n": len(must), "reach_hit": len(hit), "avoid_n": len(avoid),
            "avoid_viol": len(viol), "missed": sorted(fmtkey(k) for k in must if k not in reached),
            "violations": sorted(map(fmtkey, viol)), "reached_count": len(reached) // 1}


RUNNERS = {"callers": run_callers, "impact_direct": run_impact, "dead": run_dead, "trace_reaches": run_trace}


def expected_counts(q):
    """Denominators contributed by a question that could not be answered."""
    k = q["kind"]
    if k in ("callers", "impact_direct"):
        return {"expected": len(q.get("expect", [])), "returned": 0, "tp": 0}
    if k == "dead":
        return {"live": len(q.get("live", [])), "false_alarms": 0, "dead": len(q.get("dead", [])), "detected": 0}
    return {"reach_n": len(q.get("must_reach", [])), "reach_hit": 0,
            "avoid_n": len(q.get("must_not_reach", [])), "avoid_viol": 0}


# --------------------------------------------------------------------------
# aggregation + reporting
# --------------------------------------------------------------------------

SUM_FIELDS = ["expected", "returned", "tp", "live", "false_alarms", "dead", "detected",
              "reach_n", "reach_hit", "avoid_n", "avoid_viol"]


def aggregate(records):
    a = {f: 0 for f in SUM_FIELDS}
    a["questions"] = len(records)
    a["not_indexed"] = sum(1 for r in records if r["status"] == "target_not_indexed")
    a["errors"] = sum(1 for r in records if r["status"] == "error")
    for r in records:
        for f in SUM_FIELDS:
            a[f] += r.get(f, 0)
    a["precision"] = ratio(a["tp"], a["returned"])
    a["recall"] = ratio(a["tp"], a["expected"])
    p, rc = a["precision"], a["recall"]
    a["f1"] = (2 * p * rc / (p + rc)) if (p is not None and rc is not None and p + rc > 0) else (0.0 if a["expected"] else None)
    a["false_alarm_rate"] = ratio(a["false_alarms"], a["live"])
    a["detection_rate"] = ratio(a["detected"], a["dead"])
    a["reach_recall"] = ratio(a["reach_hit"], a["reach_n"])
    a["wrong_reach_rate"] = ratio(a["avoid_viol"], a["avoid_n"])
    return a


def ratio(n, d):
    return None if not d else n / d


def pct(v):
    return "  -  " if v is None else "%5.1f%%" % (100 * v)


def finish_record(q, rec, seconds):
    rec.update({"id": q["id"], "lang": q.get("lang", "?"), "kind": q["kind"], "tags": q.get("tags", []),
                "seconds": round(seconds, 3)})
    if rec.get("status") != "ok":
        # unanswered questions still count in the denominators (zero credit)
        for k, v in expected_counts(q).items():
            rec.setdefault(k, v)
    k = q["kind"]
    if k in ("callers", "impact_direct"):
        rec["precision"] = ratio(rec.get("tp", 0), rec.get("returned", 0))
        if rec.get("status") == "ok" and not rec.get("expected") and not rec.get("returned"):
            rec["precision"] = rec["recall"] = 1.0
        else:
            rec["recall"] = ratio(rec.get("tp", 0), rec.get("expected", 0))
            if rec.get("expected", 0) == 0:
                rec["recall"] = None
    return rec


def row_summary(r):
    k = r["kind"]
    st = r["status"]
    if st == "error":
        return "ERROR  " + r.get("error", "")[:90]
    if st == "target_not_indexed":
        return "target_not_indexed (counts as 0)"
    if k in ("callers", "impact_direct"):
        return "P %s R %s  tp=%d/%d ret=%d" % (pct(r.get("precision")), pct(r.get("recall")),
                                                 r["tp"], r["expected"], r["returned"])
    if k == "dead":
        return "false-alarms %d/%d  detected %d/%d" % (r["false_alarms"], r["live"], r["detected"], r["dead"])
    return "reached %d/%d  wrong-reach %d/%d" % (r["reach_hit"], r["reach_n"], r["avoid_viol"], r["avoid_n"])


def detail_lines(r):
    out = []
    for key, label in (("missing", "missing"), ("extra", "extra"), ("false_alarm_items", "false alarm"),
                       ("missed_dead", "missed dead"), ("missed", "not reached"),
                       ("violations", "wrongly reached"), ("unindexed_decls", "decl not indexed")):
        vals = r.get(key) or []
        if vals:
            shown = ", ".join(vals[:4]) + (" ... (+%d)" % (len(vals) - 4) if len(vals) > 4 else "")
            out.append("        %-15s %s" % (label + ":", shown))
    return out


def print_summary_line(label, a):
    bits = []
    if a["expected"] or a["returned"]:
        bits.append("callers/impact P %s R %s F1 %s" % (pct(a["precision"]), pct(a["recall"]), pct(a["f1"])))
    if a["live"] or a["dead"]:
        bits.append("dead: false-alarm %s (%d/%d) detection %s (%d/%d)" % (
            pct(a["false_alarm_rate"]), a["false_alarms"], a["live"], pct(a["detection_rate"]), a["detected"], a["dead"]))
    if a["reach_n"] or a["avoid_n"]:
        bits.append("trace: reach %s (%d/%d) wrong-reach %s (%d/%d)" % (
            pct(a["reach_recall"]), a["reach_hit"], a["reach_n"], pct(a["wrong_reach_rate"]), a["avoid_viol"], a["avoid_n"]))
    flags = ""
    if a["not_indexed"]:
        flags += "  [not_indexed=%d]" % a["not_indexed"]
    if a["errors"]:
        flags += "  [ERRORS=%d]" % a["errors"]
    print("  %-14s n=%-3d %s%s" % (label, a["questions"], " | ".join(bits), flags))


def summarize(records, title):
    print("\n== %s ==" % title)
    print_summary_line("TOTAL", aggregate(records))
    for kind in ("callers", "impact_direct", "dead", "trace_reaches"):
        rs = [r for r in records if r["kind"] == kind]
        if rs:
            print_summary_line("kind:" + kind, aggregate(rs))
    for lang in sorted({r["lang"] for r in records}):
        print_summary_line("lang:" + lang, aggregate([r for r in records if r["lang"] == lang]))


# --------------------------------------------------------------------------
# main run
# --------------------------------------------------------------------------

def load_suite(path):
    try:
        with open(path, encoding="utf-8") as fh:
            suite = json.load(fh)
    except (OSError, ValueError) as e:
        raise Fatal("cannot read suite %s: %s" % (path, e))
    for key in ("name", "repo", "questions"):
        if key not in suite:
            raise Fatal("suite %s: missing required key %r" % (path, key))
    seen = set()
    for q in suite["questions"]:
        for key in ("id", "kind"):
            if key not in q:
                raise Fatal("suite %s: question without %r: %s" % (path, key, json.dumps(q)[:100]))
        if q["kind"] not in RUNNERS:
            raise Fatal("suite %s: question %s has unknown kind %r (want %s)" % (path, q["id"], q["kind"], sorted(RUNNERS)))
        if q["id"] in seen:
            raise Fatal("suite %s: duplicate question id %s" % (path, q["id"]))
        seen.add(q["id"])
    return suite


def run_suite(binary, suite_path, workdir, only):
    suite = load_suite(suite_path)
    print("\n### suite %s (%d questions)" % (suite["name"], len(suite["questions"])), flush=True)
    try:
        repo = prepare_repo(suite, suite_path, workdir)
    except Fatal as e:
        if "environment variable" in str(e) or "does not exist" in str(e):
            print("  SKIPPED: %s" % e)
            return None
        raise
    db = os.path.join(workdir, "db", "%s--%s.sqlite" % (suite["name"], binary_label(binary)))
    idx_s = index_repo(binary, repo, db)
    print("  indexed in %.1fs" % idx_s, flush=True)
    lx = Lidx(binary, repo, db)
    sm = SymbolMap(lx)
    records = []
    for q in suite["questions"]:
        if only and not re.search(only, q["id"]):
            continue
        t = time.time()
        try:
            rec = RUNNERS[q["kind"]](lx, sm, q)
        except BenchError as e:
            rec = {"status": "error", "error": str(e)}
        except subprocess.TimeoutExpired:
            rec = {"status": "error", "error": "request timed out"}
        rec = finish_record(q, rec, time.time() - t)
        records.append(rec)
        print("  %-44s %-13s %-10s %5.2fs  %s" % (rec["id"][:44], rec["kind"], rec["lang"], rec["seconds"], row_summary(rec)))
        for line in detail_lines(rec):
            print(line)
    lx.close()
    summarize(records, "suite " + suite["name"])
    return {"name": suite["name"], "repo": repo, "index_seconds": round(idx_s, 2), "questions": records}


def version_of(binary):
    try:
        return subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception:
        return "?"


def cmd_run(args):
    binary = os.path.abspath(os.path.expanduser(args.binary))
    if not (os.path.isfile(binary) and os.access(binary, os.X_OK)):
        raise Fatal("binary not found or not executable: %s" % binary)
    if not args.suite:
        raise Fatal("at least one --suite is required")
    workdir = os.path.abspath(args.workdir or DEFAULT_WORKDIR)
    os.makedirs(workdir, exist_ok=True)
    t0 = time.time()
    suites = []
    for sp in args.suite:
        s = run_suite(binary, sp, workdir, args.only)
        if s:
            suites.append(s)
    if not suites:
        raise Fatal("no suite ran (all skipped?)")
    allrecs = [r for s in suites for r in s["questions"]]
    if len(suites) > 1:
        summarize(allrecs, "ALL SUITES")
    wall = time.time() - t0
    errors = [r for r in allrecs if r["status"] == "error"]
    if errors:
        print("\n!! %d question(s) ERRORED (scored as zero, answers NOT trusted):" % len(errors))
        for r in errors:
            print("   - %s: %s" % (r["id"], r.get("error")))
    print("\ntotal wall time: %.1fs (query time %.1fs)" % (wall, sum(r["seconds"] for r in allrecs)))
    if args.json:
        out = {"binary": binary, "version": version_of(binary), "wall_seconds": round(wall, 2),
               "suites": suites, "totals": aggregate(allrecs)}
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=2)
        print("wrote %s" % args.json)
    return 2 if errors else 0


def cmd_compare(a_path, b_path):
    def load(p):
        try:
            with open(p, encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, ValueError) as e:
            raise Fatal("cannot read %s: %s" % (p, e))
    A, B = load(a_path), load(b_path)

    def index(doc):
        return {(s["name"], r["id"]): r for s in doc["suites"] for r in s["questions"]}
    ia, ib = index(A), index(B)

    def score(r):
        k = r["kind"]
        if r["status"] != "ok":
            return 0.0
        if k in ("callers", "impact_direct"):
            p, rc = r.get("precision"), r.get("recall")
            if p is None or rc is None:
                return 0.0
            return 2 * p * rc / (p + rc) if p + rc else 0.0
        if k == "dead":
            d = (r["detected"] / r["dead"]) if r["dead"] else 1.0
            f = 1 - (r["false_alarms"] / r["live"]) if r["live"] else 1.0
            return (d + f) / 2
        rc = (r["reach_hit"] / r["reach_n"]) if r["reach_n"] else 1.0
        w = 1 - (r["avoid_viol"] / r["avoid_n"]) if r["avoid_n"] else 1.0
        return (rc + w) / 2

    print("A = %s (%s)\nB = %s (%s)\n" % (A["binary"], A.get("version"), B["binary"], B.get("version")))
    print("%-46s %-13s %6s %6s %7s" % ("question", "kind", "A", "B", "delta"))
    changed = 0
    for key in sorted(set(ia) | set(ib)):
        ra, rb = ia.get(key), ib.get(key)
        sa = score(ra) if ra else None
        sb = score(rb) if rb else None
        kind = (ra or rb)["kind"]
        if sa is None or sb is None:
            print("%-46s %-13s %6s %6s   (only in %s)" % (key[1][:46], kind, "-" if sa is None else "%.2f" % sa,
                                                       "-" if sb is None else "%.2f" % sb, "B" if sa is None else "A"))
            continue
        d = sb - sa
        if abs(d) > 1e-9:
            changed += 1
            print("%-46s %-13s %6.2f %6.2f %+7.2f" % (key[1][:46], kind, sa, sb, d))
    print("\n%d of %d common questions changed" % (changed, len(set(ia) & set(ib))))
    ta, tb = A["totals"], B["totals"]
    print("\n%-26s %10s %10s %9s" % ("totals", "A", "B", "delta"))
    for label, f in (("callers/impact precision", "precision"), ("callers/impact recall", "recall"),
                     ("callers/impact F1", "f1"), ("dead false-alarm rate (lower)", "false_alarm_rate"),
                     ("dead detection rate", "detection_rate"), ("trace reach recall", "reach_recall"),
                     ("trace wrong-reach (lower)", "wrong_reach_rate")):
        va, vb = ta.get(f), tb.get(f)
        if va is None and vb is None:
            continue
        dd = "" if va is None or vb is None else "%+8.1fpp" % (100 * (vb - va))
        print("%-30s %10s %10s %s" % (label, pct(va), pct(vb), dd))
    print("%-30s %9.1fs %9.1fs" % ("wall time", A.get("wall_seconds", 0), B.get("wall_seconds", 0)))
    return 0


def main():
    ap = argparse.ArgumentParser(description="lidx answer benchmark runner (see README.md)")
    ap.add_argument("--binary", help="path to the lidx binary under test")
    ap.add_argument("--suite", action="append", help="suite JSON file (repeatable)")
    ap.add_argument("--workdir", help="clones/indexes live here (default benchmarks/answers/.work)")
    ap.add_argument("--json", help="write full results to this file")
    ap.add_argument("--only", help="regex; run only question ids matching it")
    ap.add_argument("--compare", nargs=2, metavar=("A.json", "B.json"), help="compare two --json outputs")
    args = ap.parse_args()
    try:
        if args.compare:
            return cmd_compare(*args.compare)
        if not args.binary:
            raise Fatal("--binary is required (or use --compare A.json B.json)")
        return cmd_run(args)
    except Fatal as e:
        print("error: %s" % e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
