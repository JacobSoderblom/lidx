#!/usr/bin/env python3
"""Draw and record the hand spot-check. Python 3 stdlib only.

    spotcheck.py sample   # writes results/spotcheck-sample.json: 20 FP + 20 FN per language, plus 10 per exclusion bucket
    spotcheck.py merge    # joins spotcheck-verdicts.json (hand-written) into results/spotcheck.json for the report

The draw is seeded and made from the committed lists, so the sample is
reproducible for a given report. Verdicts are written by a person reading the
context in the sample file; `merge` refuses a verdict file that does not cover
the current sample.
"""
import argparse
import hashlib
import json
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SEED = 412
N = 20
NB = 10  # per exclusion bucket
# cells pooled per language (production-code variant; cobra has no main so it uses its test binaries)
POOL = {
    "go": ["go/cobra/tests", "go/gitleaks/no_tests", "go/syft/no_tests"],
    "ts": ["ts/zod", "ts/hono"],
    "rust": ["rust/ripgrep/no_tests", "rust/fd/no_tests"],
}


def key(d):
    return "%s:%d" % (d["file"], d["line"])


def ident(cell, kind, item):
    return hashlib.sha1(("%s|%s|%s|%s" % (cell, kind, key(item["caller"]), key(item["callee"]))).encode()).hexdigest()[:8]


def snippet(repo, path, line, before=1, after=1):
    try:
        lines = open(os.path.join(repo, path), encoding="utf-8", errors="replace").read().split("\n")
    except OSError:
        return []
    lo, hi = max(1, line - before), min(len(lines), line + after)
    return ["%d: %s" % (i, lines[i - 1][:200]) for i in range(lo, hi + 1)]


def sample(results, cache):
    meta = json.load(open(os.path.join(results, "cells.json")))
    out = {"seed": SEED, "cells": [c for cs in POOL.values() for c in cs], "languages": {}}
    legacy = random.Random(SEED)  # go fp / go fn keep the first draws of the original single sequence
    for lang, cells in POOL.items():
        out["languages"][lang] = {}
        for kind in ("fp", "fn"):
            # one independent generator per list, so a change to one language's lists never reshuffles another's sample
            rng = legacy if lang == "go" else random.Random("%s|%s|%s" % (SEED, lang, kind))
            pool = []
            for c in cells:
                fid = c.replace("/", "__")
                for it in json.load(open(os.path.join(results, "lists", "%s.%s.json" % (fid, kind)))):
                    if it.get("category") == "dispatch":
                        continue  # dispatch misses are a separate bucket, not part of the headline recall
                    pool.append((c, it))
            pool.sort(key=lambda x: ident(x[0], kind, x[1]))
            picks = rng.sample(pool, min(N, len(pool)))
            rows = []
            for c, it in picks:
                repo_name = c.split("/")[1]
                sha = meta["corpus"][repo_name]["sha"]
                repo = os.path.join(cache, "%s@%s" % (repo_name, sha[:12]))
                row = {"id": ident(c, kind, it), "cell": c, "kind": kind, "caller": it["caller"].get("qualname") or it["caller"]["name"],
                       "callee": it["callee"].get("qualname") or it["callee"]["name"], "category": it.get("category"),
                       "detail": it.get("detail"), "caller_at": key(it["caller"]), "callee_at": key(it["callee"])}
                if kind == "fp":
                    row["resolution_kind"] = it["resolution_kind"]
                    ef = it["evidence_file"]
                    row["call_site"] = {"%s:%d" % (ef, ln): snippet(repo, ef, ln, 1, 1) for ln in it["evidence_lines"][:2]}
                else:
                    row["call_site"] = {s: snippet(repo, s.rpartition(":")[0], int(s.rpartition(":")[2]), 1, 1) for s in it["sites"][:2]}
                    row["fanout"] = it.get("fanout")
                row["caller_decl"] = snippet(repo, it["caller"]["file"], it["caller"]["line"], 0, 0)
                row["callee_decl"] = snippet(repo, it["callee"]["file"], it["callee"]["line"], 0, 2)
                rows.append(row)
            out["languages"][lang][kind] = rows
    out["buckets"] = {}
    for lang, cells in POOL.items():
        out["buckets"][lang] = {}
        pools = {}
        for c in cells:
            fid = c.replace("/", "__")
            for suffix in ("dispatch_fn", "ungradable", "capture_gap"):
                p = os.path.join(results, "lists", "%s.%s.json" % (fid, suffix))
                if not os.path.exists(p):
                    continue
                for it in json.load(open(p)):
                    name = "dispatch" if suffix == "dispatch_fn" else "%s:%s" % (suffix, it.get("reason"))
                    pools.setdefault(name, []).append((c, it))
        for name in sorted(pools):
            rng = random.Random("%s|%s|%s" % (SEED, lang, name))
            pool = sorted(pools[name], key=lambda x: ident(x[0], name, x[1]))
            rows = []
            for c, it in rng.sample(pool, min(NB, len(pool))):
                repo_name = c.split("/")[1]
                repo = os.path.join(cache, "%s@%s" % (repo_name, meta["corpus"][repo_name]["sha"][:12]))
                row = {"id": ident(c, name, it), "cell": c, "bucket": name, "bucket_size": len(pool),
                       "caller": it["caller"].get("qualname") or it["caller"]["name"],
                       "callee": it["callee"].get("qualname") or it["callee"]["name"],
                       "caller_at": key(it["caller"]), "callee_at": key(it["callee"])}
                if "evidence_file" in it:
                    ef = it["evidence_file"]
                    row["call_site"] = {"%s:%d" % (ef, ln): snippet(repo, ef, ln, 1, 1) for ln in it["evidence_lines"][:2]}
                    row["resolution_kind"] = it.get("resolution_kind")
                else:
                    row["call_site"] = {s: snippet(repo, s.rpartition(":")[0], int(s.rpartition(":")[2]), 1, 1) for s in it.get("sites", [])[:2]}
                row["caller_decl"] = snippet(repo, it["caller"]["file"], it["caller"]["line"], 0, 0)
                row["callee_decl"] = snippet(repo, it["callee"]["file"], it["callee"]["line"], 0, 2)
                rows.append(row)
            out["buckets"][lang][name] = rows
    path = os.path.join(results, "spotcheck-sample.json")
    json.dump(out, open(path, "w"), indent=1)
    print("wrote", path, {l: {k: len(v) for k, v in d.items()} for l, d in out["languages"].items()},
          {l: {k: len(v) for k, v in d.items()} for l, d in out["buckets"].items()})


def merge(results):
    sam = json.load(open(os.path.join(results, "spotcheck-sample.json")))
    ver = json.load(open(os.path.join(HERE, "spotcheck-verdicts.json")))
    sc = {"seed": sam["seed"], "cells": sam["cells"], "notes": ver.get("_notes", ""), "languages": {}}
    missing = []
    for lang, kinds in sam["languages"].items():
        sc["languages"][lang] = {}
        for kind, rows in kinds.items():
            out = []
            for r in rows:
                v = ver.get(r["id"])
                if not v:
                    missing.append(r["id"])
                    continue
                assert v["verdict"] in ("lidx_error", "oracle_artefact", "matching_artefact"), v
                out.append({"id": r["id"], "caller": r["caller"], "callee": r["callee"], "verdict": v["verdict"], "note": v["note"]})
            sc["languages"][lang][kind] = out
    sc["buckets"] = {}
    for lang, bks in sam.get("buckets", {}).items():
        sc["buckets"][lang] = {}
        for name, rows in bks.items():
            out = []
            for r in rows:
                v = ver.get(r["id"])
                if not v:
                    missing.append(r["id"])
                    continue
                assert v["verdict"] in ("correct_exclusion", "misbucketed"), v
                out.append({"id": r["id"], "cell": r["cell"], "caller": r["caller"], "callee": r["callee"], "verdict": v["verdict"], "note": v["note"],
                            "bucket_size": r["bucket_size"]})
            sc["buckets"][lang][name] = out
    if missing:
        sys.exit("verdicts missing for sample ids: %s" % ", ".join(missing))
    json.dump(sc, open(os.path.join(results, "spotcheck.json"), "w"), indent=1)
    print("wrote spotcheck.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["sample", "merge"])
    ap.add_argument("--results", default=os.path.join(HERE, "results"))
    ap.add_argument("--cache", default=os.environ.get("LIDX_ORACLE_CACHE", os.path.expanduser("~/.cache/lidx-oracle")))
    a = ap.parse_args()
    sample(a.results, a.cache) if a.cmd == "sample" else merge(a.results)
