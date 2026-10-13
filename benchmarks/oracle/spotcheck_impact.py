#!/usr/bin/env python3
"""Draw and record the hand spot-check of the impact benchmark (issue #451). Python 3 stdlib only.

    spotcheck_impact.py sample --binary B --cache C   # results/spotcheck-impact-sample.json: 20 extra + 20 missing pairs per language
    spotcheck_impact.py merge                         # joins spotcheck-impact-verdicts.json (hand-written) into results/spotcheck-impact.json

The draw is seeded and made from the committed lists (results/lists/<cell>.impact_{extra,missing}.json), k = 3 pairs first so that the
chain is at most three calls long (topped up with unbounded-depth pairs when a language has fewer than 20). An *extra* pair is a lidx member the oracle closure lacks: the item carries lidx's
own path from the member to the seed, each hop with the edge kind, the resolution tier and the source line of the evidence. A *missing*
pair is an oracle caller lidx lacks: the item carries the oracle's shortest chain from that caller to the seed with the source line of each
call site and, per hop, what lidx has for it (a resolved edge, or the unresolved reference at that line). At most 3 pairs per seed.
"""
import argparse
import hashlib
import json
import os
import random
import sqlite3
import sys
from collections import defaultdict, deque

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import impact  # noqa: E402
import match  # noqa: E402
import spotcheck as base  # noqa: E402  (redaction of fixture secrets)

SEED = 451
N = 20
PER_SEED = 3


def ident(*parts):
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:8]


def load_list(results, cid, kind):
    p = os.path.join(results, "lists", "%s.impact_%s.json" % (cid.replace("/", "__"), kind))
    return json.load(open(p)) if os.path.exists(p) else []


def src_line(repo, f, ln):
    if not ln:
        return ""
    try:
        with open(os.path.join(repo, f), encoding="utf-8", errors="replace") as fh:
            lines = fh.read().split("\n")
        return base.redact(lines[ln - 1].strip()[:200]) if 0 < ln <= len(lines) else ""
    except OSError:
        return ""


def pick(results, cids, kind, rng):
    """N pairs, at most PER_SEED per seed; k = 3 pairs first (a chain of at most three calls), topped up with unbounded-depth pairs when a
    language has fewer than N."""
    out, per_seed, seen = [], defaultdict(int), set()
    for kk in ("3", "inf"):
        pool = defaultdict(list)
        for cid in sorted(cids):
            repo = cid.split("/")[1]
            for it in load_list(results, cid, kind):
                if it["k"] == kk and (repo, it["seed"], it["node"]) not in seen:
                    pool[(repo, it["seed"], it["node"])].append((cid, it))
        items = [(k, v[0]) for k, v in sorted(pool.items())]
        rng.shuffle(items)
        for (repo, seed, node), (cid, it) in items:
            if per_seed[(repo, seed)] >= PER_SEED:
                continue
            per_seed[(repo, seed)] += 1
            seen.add((repo, seed, node))
            out.append((cid, it))
            if len(out) == N:
                return out
    return out


class RepoCtx:
    """Per repository: the index, the symbols and a live lidx server."""

    def __init__(self, binary, cache, repo, lock):
        self.name = repo
        self.info = next(r for r in lock["repos"] if r["name"] == repo)
        self.dir = os.path.join(cache, "%s@%s" % (repo, self.info["sha"][:12]))
        self.db = os.path.join(cache, "db", "%s.sqlite" % repo)
        self.syms, _ = match.load_lidx(self.db, self.dir, self.info["lang"])
        self.by_key = defaultdict(list)
        for i, s in self.syms.items():
            if "key" in s:
                self.by_key[s["key"]].append(i)
        self.con = sqlite3.connect("file:%s?mode=ro" % self.db, uri=True)
        self.lidx = impact.Lidx(binary, self.dir, self.db)
        self.qual = {s["qualname"]: i for i, s in self.syms.items() if "key" in s}

    def close(self):
        self.lidx.close()

    def edge_rows(self, a_id, b_id):
        """All edges between two symbols, either direction, with evidence file and line."""
        return [{"kind": k, "res": r, "file": f, "line": ln, "dir": "->" if s == a_id else "<-"} for k, r, f, ln, s in self.con.execute(
            "select e.kind, e.resolution_kind, f.path, e.evidence_start_line, e.source_symbol_id from edges e join files f on f.id = e.file_id "
            "where f.deleted_version is null and ((e.source_symbol_id = ? and e.target_symbol_id = ?) or (e.source_symbol_id = ? and e.target_symbol_id = ?))",
            (a_id, b_id, b_id, a_id))]

    def unresolved_at(self, sid, lines, f):
        out = []
        for k, tq, res, ln in self.con.execute(
                "select e.kind, e.target_qualname, e.resolution_kind, e.evidence_start_line from edges e join files fl on fl.id = e.file_id "
                "where e.source_symbol_id = ? and e.target_symbol_id is null and fl.path = ?", (sid, f)):
            if ln in lines:
                out.append({"kind": k, "target_text": tq, "line": ln})
        return out


def extra_item(ctx, cid, it, lang):
    seed_key = it["seed"]
    sid = min(ctx.by_key.get(seed_key, [0]) or [0])
    res = ctx.lidx.call("analyze_impact", {"id": sid, "direction": "up", "max_depth": impact.lidx_depth(None if it['k'] == 'inf' else int(it['k'])), "include_tests": True, "enable_test": False, "enable_historical": False, "kinds": impact.CALL_KINDS, "exclude_resolution_kinds": ["containment"],
                                           "include_paths": True, "limit": 2000, "max_response_bytes": impact.RESPONSE_BYTES})
    node = it["node"]  # canonical declaration: `file|qualname` where lidx has a symbol at an oracle position, else `file:line`
    chain = []
    for e in res.get("affected", []):
        s = ctx.syms.get(e["symbol"]["id"])
        if s and "key" in s and (("%s|%s" % (s["file"], s["qualname"])) == node or s["key"] == node):
            for st in (e.get("path") or {}).get("steps", []):
                a, b = ctx.qual.get(st["from_symbol"]), ctx.qual.get(st["to_symbol"])
                rows = ctx.edge_rows(a, b) if a and b else []
                rows = [r for r in rows if r["kind"] == st["edge_kind"]] or rows
                chain.append({"from": st["from_symbol"], "to": st["to_symbol"], "edge_kind": st["edge_kind"], "resolution_kind": st.get("resolution_kind"),
                              "evidence": [{"where": "%s:%s" % (r["file"], r["line"] or "-"), "dir": r["dir"], "text": src_line(ctx.dir, r["file"], r["line"])} for r in rows[:2]]})
            break
    return {"chain": chain, "lidx_depth": e["distance"] if chain else None}


def oracle_chain(graph, oracle_edges, member_node, seed_node, k):
    """Shortest call chain member -> ... -> seed in the oracle graph, with each hop's call sites."""
    prev = {member_node: None}
    q = deque([member_node])
    while q:
        x = q.popleft()
        if x == seed_node:
            break
        for y in sorted(graph.fwd.get(x, ())):
            if y not in prev:
                prev[y] = x
                q.append(y)
    if seed_node not in prev:
        return []
    path = [seed_node]
    while prev[path[-1]] is not None:
        path.append(prev[path[-1]])
    path.reverse()  # member ... seed
    return path


def missing_item(ctx, cid, it, graph, oracle, lang, alt=False):
    node, seed_key = it["node"], it["seed"]
    seed_node = graph.cn(seed_key)
    path = oracle_chain(graph, oracle["edges"], node, seed_node, 3)
    chain = []
    for a, b in zip(path, path[1:]):
        ss = list(dict.fromkeys(graph.sites.get((a, b), [])))
        if not hasattr(graph, "node_ids"):
            graph.node_ids = defaultdict(list)
            for i, sy in ctx.syms.items():
                if "key" in sy:
                    graph.node_ids[graph.cn(sy["key"])].append(i)
        ia, ib = graph.node_ids.get(a, []), graph.node_ids.get(b, [])
        has = ctx.edge_rows(min(ia), min(ib)) if ia and ib else []
        has = [r for r in has if r["dir"] == "->"]
        lines = set()
        for st in ss:
            lines.add(int(st.rpartition(":")[2]))
        unres = ctx.unresolved_at(min(ia), lines, graph.info[a]["file"]) if ia else []
        chain.append({"from": impact.graph_name(graph, a), "to": impact.graph_name(graph, b),
                      "sites": [{"where": s, "text": src_line(ctx.dir, s.rpartition(":")[0], int(s.rpartition(":")[2]))} for s in ss[:2]],
                      "lidx_edge": ["%s %s" % (r["kind"], r["res"]) for r in has[:2]] or None, "lidx_unresolved_at_site": unres[:3] or None,
                      "from_indexed": bool(ia), "to_indexed": bool(ib)})
    return {"chain": chain}


def sample(results, cache, binary):
    lock = json.load(open(os.path.join(HERE, "corpus.lock.json")))
    cells = {os.path.basename(p)[:-5].replace("__", "/"): None for p in __import__("glob").glob(os.path.join(results, "impact", "*.json"))}
    langs = {}
    for cid in cells:
        langs.setdefault(cid.split("/")[0], []).append(cid)
    out = {"seed": SEED, "n": N, "cells": sorted(cells), "languages": {},
           "cells_note": "", "instructions": ("Read the source. EXTRA items: lidx puts the member in the upstream set of the seed through the chain shown; the oracle closure does not. "
                            "`lidx_error` if the chain is wrong (a hop is not a call/reference of the next one, or the target is wrong), `oracle_artefact` if the chain is "
                            "real in the source but the compiler graph cannot show it (dispatch/RTA limits, function values, build tags, generated code, closure keying), "
                            "`matching_artefact` if the keys mis-state the pair. MISSING items: the oracle chain from the caller to the seed; `lidx_error` if the calls are "
                            "real and lidx lacks an edge on the chain, `oracle_artefact` if a hop of the oracle chain is not a real call to that target (over-approximated "
                            "dispatch, closure keying), `matching_artefact` if keys or lifting mis-state the pair.")}
    ctxs, graphs = {}, {}
    try:
        for lang, cids in sorted(langs.items()):
            rng = random.Random("%d|%s" % (SEED, lang))
            out["languages"][lang] = {}
            for kind in ("extra", "missing") + (("missing_alt",) if lang == "go" else ()):
                items = []
                for cid, it in pick(results, cids, kind, rng):
                    repo = cid.split("/")[1]
                    if repo not in ctxs:
                        ctxs[repo] = RepoCtx(binary, cache, repo, lock)
                    ctx = ctxs[repo]
                    row = {"id": ident(cid, kind, it["seed"], it["node"], it["k"]), "cell": cid, "kind": kind, "seed": "%s %s" % (it["seed"], it["seed_name"]), "k": it["k"],
                           "member": it["member"]}
                    if kind == "extra":
                        row.update(extra_item(ctx, cid, it, lang))
                    else:
                        gk = (cid, kind)
                        if gk not in graphs:
                            oracle = json.load(open(os.path.join(cache, "oracle", "%s.json" % cid.replace("/", "__"))))
                            og = match.drop_closure_self(oracle)[0] if lang == "go" else oracle
                            # the graph the pair was drawn from: the primary closure (TypeScript, Rust: abstract / trait members expanded) or, for
                            # `missing_alt`, the Go closure without function-value edges
                            g = impact.Graph(og, ctx.syms, lang, drop_closure=(kind == "missing_alt"), expand=(lang != "go"), src=match.Sources(ctx.dir))
                            graphs[gk] = (g, og)
                        row.update(missing_item(ctx, cid, it, graphs[gk][0], graphs[gk][1], lang, kind == "missing_alt"))
                    items.append(row)
                out["languages"][lang][kind] = items
    finally:
        for c in ctxs.values():
            c.close()
    json.dump(out, open(os.path.join(results, "spotcheck-impact-sample.json"), "w"), indent=1)
    print("wrote", os.path.join(results, "spotcheck-impact-sample.json"), {l: {k: len(v) for k, v in d.items()} for l, d in out["languages"].items()})


def merge(results):
    sample_doc = json.load(open(os.path.join(results, "spotcheck-impact-sample.json")))
    verdicts = json.load(open(os.path.join(HERE, "spotcheck-impact-verdicts.json")))
    out = {"seed": sample_doc["seed"], "cells": sample_doc["cells"], "languages": {}}
    missing = []
    for lang, kinds in sample_doc["languages"].items():
        out["languages"][lang] = {}
        for kind, items in kinds.items():
            rows = []
            for it in items:
                v = verdicts.get(it["id"])
                if not v:
                    missing.append(it["id"])
                    continue
                rows.append({"id": it["id"], "seed": it["seed"], "k": it["k"], "member": it["member"], "verdict": v["verdict"], "note": v["note"]})
            out["languages"][lang][kind] = rows
    if missing:
        sys.exit("no verdict for: " + ", ".join(missing))
    out["summary"] = verdicts.get("_summary", {}).get("note", "") if isinstance(verdicts.get("_summary"), dict) else ""
    json.dump(out, open(os.path.join(results, "spotcheck-impact.json"), "w"), indent=1)
    print("wrote", os.path.join(results, "spotcheck-impact.json"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["sample", "merge"])
    ap.add_argument("--results", default=os.path.join(HERE, "results"))
    ap.add_argument("--cache", default=os.path.expanduser("~/.cache/lidx-oracle"))
    ap.add_argument("--binary", default=os.path.join(HERE, "..", "..", "target", "release", "lidx"))
    a = ap.parse_args()
    if a.cmd == "sample":
        sample(a.results, os.path.abspath(a.cache), os.path.abspath(a.binary))
    else:
        merge(a.results)


if __name__ == "__main__":
    main()
