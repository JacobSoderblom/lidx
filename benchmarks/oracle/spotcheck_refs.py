#!/usr/bin/env python3
"""Draw and record the hand spot-check of the value-reference benchmark (issue #452). Python 3 stdlib only.

    spotcheck_refs.py sample   # results/spotcheck-refs-sample.json: 20 FP + 20 FN + 20 TP per language, plus 10 per exclusion bucket
    spotcheck_refs.py merge    # joins spotcheck-refs-verdicts.json (hand-written) into results/spotcheck-refs.json for the report

The draw is seeded and made from the committed lists (`<cell>.refs_{fp,fn,capture_gap,ungradable,call_wins}.json`), one pair counted once
across the cells of a repository. FP = a lidx pair the oracle does not confirm; FN = an oracle pair lidx lacks. Buckets: capture gaps, ungradable
lidx pairs by reason, and the pairs the call-wins rule removed on either side. Every item carries the source lines at the evidence sites and the
declaration of the referenced function, so the verdict can be written by reading the code.
"""
import argparse
import hashlib
import json
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import spotcheck as base  # noqa: E402  (redaction of fixture secrets)

SEED = 452
N = 20  # per language, for each of FP and FN
NB = 10  # per bucket per language


def ident(*parts):
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:8]


def load(results, cid, suffix):
    p = os.path.join(results, "lists", "%s.refs_%s.json" % (cid.replace("/", "__"), suffix))
    return json.load(open(p)) if os.path.exists(p) else []


def pair_key(it):
    return (it["caller"]["file"], it["caller"]["line"], it["callee"]["file"], it["callee"]["line"])


def sample(results, cache):
    meta = json.load(open(os.path.join(results, "cells.json")))
    out = {"seed": SEED, "n": N, "nb": NB, "languages": {}, "buckets": {}, "cells": [],
           "instructions": ("Read the source. FP items: lidx has a value-reference pair the oracle does not confirm. `lidx_error` if the reference does not exist or is bound "
                            "to the wrong declaration (the pair is wrong), `oracle_artefact` if the source shows the function really is used as a value by that "
                            "declaration (the compiler pair is missing or mis-keyed), `matching_artefact` if the keying, category or matching mis-attributed it. "
                            "TP items: lidx and the compiler agree; `correct` if the source shows the reference, otherwise `lidx_error` / `oracle_artefact` / `matching_artefact` as for FP. FN items: the compiler says the declaration references the function as a value and lidx has no such pair. `lidx_error` if the "
                            "reference is real (lidx misses it), `oracle_artefact` if it is not a value reference (a call, a type position, an import, a "
                            "macro argument), `matching_artefact` if the keys point at a different declaration. Bucket items: `correct_exclusion` / `misbucketed`.")}
    by_lang = {}
    for cid, c in meta["cells"].items():
        if "refs" in c and not cid.endswith("_all_features"):
            by_lang.setdefault(c["lang"], []).append(cid)
    out["cells"] = sorted(c for cs in by_lang.values() for c in cs)

    def repo_dir(cid):
        name = meta["cells"][cid]["repo"]
        return os.path.join(cache, "%s@%s" % (name, meta["corpus"][name]["sha"][:12]))

    def row(cid, kind, it, bucket=None, size=None):
        repo = repo_dir(cid)
        c, t = it["caller"], it["callee"]
        sites = it.get("sites") or ["%s:%d" % (it.get("evidence_file", c["file"]), l) for l in it.get("evidence_lines", [])]
        snips = []
        for s in sites[:2]:
            f, _, ln = s.rpartition(":")
            snips.append({"at": s, "lines": base.snippet(repo, f, int(ln), 1, 1)})
        r = {"id": ident(meta["cells"][cid]["repo"], kind, bucket or "", *pair_key(it)), "cell": cid, "kind": kind,
             "decl": "%s (%s:%d) -> %s (%s:%d)" % (c["name"], c["file"], c["line"], t["name"], t["file"], t["line"]),
             "category": it.get("category") or it.get("reason") or it.get("side"), "detail": it.get("detail"), "forms": it.get("forms"), "ctxs": it.get("ctxs"),
             "resolution_kind": it.get("resolution_kind"), "edge_kinds": it.get("edge_kinds"),
             "evidence": snips, "callee_decl": base.snippet(repo, t["file"], t["line"], 0, 0)}
        if bucket:
            r["bucket"], r["bucket_size"] = bucket, size
        return r

    for lang, cids in sorted(by_lang.items()):
        out["languages"][lang] = {}
        out["buckets"][lang] = {}
        for kind in ("fp", "fn", "tp"):
            pool = {}
            for cid in sorted(cids):
                for it in load(results, cid, kind):
                    pool.setdefault((meta["cells"][cid]["repo"],) + pair_key(it), (cid, it))
            items = sorted(pool.values(), key=lambda x: ident(x[0], kind, *pair_key(x[1])))
            rng = random.Random("%s|%s|%s" % (SEED, lang, kind))
            out["languages"][lang][kind] = [row(cid, kind, it) for cid, it in rng.sample(items, min(N, len(items)))]
            out["languages"][lang][kind + "_population"] = len(items)
        pools = {}
        for cid in sorted(cids):
            repo = meta["cells"][cid]["repo"]
            for it in load(results, cid, "capture_gap"):
                pools.setdefault("capture_gap:%s" % it["reason"], {}).setdefault((repo,) + pair_key(it), (cid, it))
            for it in load(results, cid, "ungradable"):
                pools.setdefault("ungradable:%s" % it["reason"], {}).setdefault((repo,) + pair_key(it), (cid, it))
            for it in load(results, cid, "call_wins"):
                pools.setdefault("call_wins:%s" % it["side"], {}).setdefault((repo,) + pair_key(it), (cid, it))
        for name in sorted(pools):
            items = sorted(pools[name].values(), key=lambda x: ident(x[0], name, *pair_key(x[1])))
            rng = random.Random("%s|%s|%s" % (SEED, lang, name))
            out["buckets"][lang][name] = [row(cid, "bucket", it, bucket=name, size=len(items)) for cid, it in rng.sample(items, min(NB, len(items)))]
    path = os.path.join(results, "spotcheck-refs-sample.json")
    json.dump(out, open(path, "w"), indent=1)
    print("wrote", path, {l: {k: len(v) if isinstance(v, list) else v for k, v in d.items()} for l, d in out["languages"].items()},
          {l: {k: len(v) for k, v in d.items()} for l, d in out["buckets"].items()})


def merge(results):
    sam = json.load(open(os.path.join(results, "spotcheck-refs-sample.json")))
    ver = json.load(open(os.path.join(HERE, "spotcheck-refs-verdicts.json")))
    sc = {"seed": sam["seed"], "cells": sam["cells"], "notes": ver.get("_notes", ""), "languages": {}, "buckets": {}}
    missing = []

    def take(r, allowed):
        v = ver.get(r["id"])
        if not v:
            missing.append(r["id"])
            return None
        assert v["verdict"] in allowed, v
        return {"id": r["id"], "decl": r["decl"], "cell": r["cell"], "verdict": v["verdict"], "note": v["note"], "bucket_size": r.get("bucket_size")}

    for lang, kinds in sam["languages"].items():
        sc["languages"][lang] = {k: [x for x in (take(r, ("lidx_error", "oracle_artefact", "matching_artefact", "correct")) for r in rows) if x]
                                 for k, rows in kinds.items() if k in ("fp", "fn", "tp")}
        sc["languages"][lang].update({k: v for k, v in kinds.items() if k.endswith("_population")})
    for lang, bks in sam["buckets"].items():
        sc["buckets"][lang] = {n: [x for x in (take(r, ("correct_exclusion", "misbucketed")) for r in rows) if x] for n, rows in bks.items()}
    if missing:
        sys.exit("verdicts missing for sample ids: %s" % ", ".join(missing))
    json.dump(sc, open(os.path.join(results, "spotcheck-refs.json"), "w"), indent=1)
    print("wrote spotcheck-refs.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["sample", "merge"])
    ap.add_argument("--results", default=os.path.join(HERE, "results"))
    ap.add_argument("--cache", default=os.environ.get("LIDX_ORACLE_CACHE", os.path.expanduser("~/.cache/lidx-oracle")))
    a = ap.parse_args()
    sample(a.results, a.cache) if a.cmd == "sample" else merge(a.results)
