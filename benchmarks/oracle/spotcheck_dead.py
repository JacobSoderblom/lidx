#!/usr/bin/env python3
"""Draw and record the hand spot-check of the dead-code benchmark. Python 3 stdlib only.

    spotcheck_dead.py sample   # results/spotcheck-dead-sample.json: 20 FP + 20 FN per language, plus 10 per exclusion bucket
    spotcheck_dead.py merge    # joins spotcheck-dead-verdicts.json (hand-written) into results/spotcheck-dead.json for the report

The draw is seeded and made from the committed lists. FP = a lidx claim on a declaration the oracle judged live; FN = an oracle-dead
declaration lidx does not claim; each language gets 10 of each per column (unreferenced, unreachable). Buckets: the rule-based
exclusions (claims on public API / test roots, which are wrong by construction), what the oracle could not judge, and what lidx
claimed that the oracle left out. Every item carries the source around the declaration and up to 8 textual mentions of its name found
with `git grep -w`, so the verdict can be written by reading the code.
"""
import argparse
import hashlib
import json
import os
import random
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import spotcheck as base  # noqa: E402  (redaction of fixture secrets)

SEED = 450
N = 10  # per language, per column, for each of FP and FN
NB = 10  # per bucket per language
GLOB = {"go": ["*.go"], "ts": ["*.ts", "*.tsx"], "rust": ["*.rs"]}
POLICY = ("public_api", "test_root", "main_init", "library_root", "ffi_or_macro_entry")
EXPLAIN = {
    "unref": "unreferenced: nothing refers to the declaration (outside its own text, imports, the declaring file's export list, `use` lines and the "
             "impl blocks of its own type)",
    "reach": "unreachable: no chain of references (calls, `new`, callbacks, any mention) leads to it from the roots (main/init, test functions, the "
             "package entry / public API)",
}


def ident(*parts):
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()[:8]


def mentions(repo, lang, name, file, line):
    try:
        r = subprocess.run(["git", "-C", repo, "grep", "-n", "-w", "-I", "--max-count=6", name, "--"] + GLOB[lang], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return []
    out = []
    for ln in r.stdout.splitlines():
        f, _, rest = ln.partition(":")
        n, _, text = rest.partition(":")
        if f == file and n == str(line):
            continue
        out.append("%s:%s: %s" % (f, n, base.redact(text.strip()[:160])))
        if len(out) >= 8:
            break
    return out


def load(results, cid, col, suffix):
    p = os.path.join(results, "lists", "%s.dead_%s.%s.json" % (cid.replace("/", "__"), col, suffix))
    return json.load(open(p)) if os.path.exists(p) else []


def sample(results, cache):
    meta = json.load(open(os.path.join(results, "cells.json")))
    out = {"seed": SEED, "n": 2 * N, "nb": NB, "languages": {}, "buckets": {},
           "instructions": ("Read the source. FP items: lidx claims the declaration dead but the oracle judged it live in the stated column. Verdict "
                            "`lidx_error` if the oracle is right (it is used / reachable, so the claim is wrong), `oracle_artefact` if the oracle is wrong "
                            "(by the column's definition the declaration really is dead), `matching_artefact` if the key points at a different declaration. "
                            "FN items: the oracle says dead and lidx does not claim it. `lidx_error` if the declaration really is dead (lidx misses it), "
                            "`oracle_artefact` if it is in fact used / reachable (the oracle is wrong), `matching_artefact` if mis-keyed. Bucket items: "
                            "`correct_exclusion` if the bucket rule is right for the item, `misbucketed` if the item should have been judged.")}
    cells_by_lang = {}
    for cid, c in meta["cells"].items():
        if "dead" in c:
            cells_by_lang.setdefault(c["lang"], []).append(cid)

    def repo_dir(cid):
        name = cid.split("/")[1] if meta["cells"][cid]["lang"] != "ts" else cid.split("/")[1]
        return os.path.join(cache, "%s@%s" % (name, meta["corpus"][name]["sha"][:12]))

    oracles = {}

    def decl_info(cid, key):
        if cid not in oracles:
            oracles[cid] = {}
            p = os.path.join(cache, "oracle", "dead__%s.json" % cid.replace("/", "__"))
            for d in json.load(open(p))["decls"]:
                oracles[cid]["%s:%d" % (d["file"], d["line"])] = d
        return oracles[cid].get(key)

    def row(cid, col, kind, item, bucket=None, bucket_size=None):
        lang = meta["cells"][cid]["lang"]
        repo = repo_dir(cid)
        key = item.get("key") or "%s:%d" % (item["file"], item["line"])
        f, _, ln = key.rpartition(":")
        ln = int(ln)
        d = decl_info(cid, key) or {}
        r = {"id": ident(repo, col, kind, bucket or "", key), "cell": cid, "column": col, "kind": kind, "decl": "%s %s (%s:%d)" % (item.get("kind", d.get("kind", "?")), item["name"], f, ln),
             "name": item["name"], "file": f, "line": ln, "decl_kind": item.get("kind") or d.get("kind"), "subkind": d.get("subkind"),
             "exported": d.get("exported"), "api": d.get("api"), "oracle": {k: d[k] for k in ("unref", "reach", "unref_how", "refs", "root", "withheld", "generated") if k in d},
             "column_means": EXPLAIN[col], "cell_scope": "tests are part of the program" if meta["cells"][cid]["variant"] in ("with_tests",) or cid.endswith("/tests") else "production code only (tests are not part of the program)",
             "category": item.get("category") or item.get("reason") or item.get("bucket"),
             "source": base.snippet(repo, f, ln, 1, 6), "mentions": mentions(repo, lang, item["name"], f, ln)}
        if bucket:
            r["bucket"], r["bucket_size"] = bucket, bucket_size
        return r

    for lang, cids in cells_by_lang.items():
        out["languages"][lang] = {}
        out["buckets"][lang] = {}
        for kind in ("fp", "fn"):
            rows = []
            for col in ("unref", "reach"):
                pool = {}
                for cid in sorted(cids):
                    for it in load(results, cid, col, kind):
                        if kind == "fp" and it.get("policy"):
                            continue
                        pool.setdefault((cid.split("/")[1], it["key"]), (cid, it))
                items = sorted(pool.values(), key=lambda x: ident(x[0], col, kind, x[1]["key"]))
                rng = random.Random("%s|%s|%s|%s" % (SEED, lang, kind, col))
                for cid, it in rng.sample(items, min(N, len(items))):
                    rows.append(row(cid, col, kind, it))
            out["languages"][lang][kind] = rows
        pools = {}
        for cid in sorted(cids):
            for col in ("unref", "reach"):
                for it in load(results, cid, col, "fp"):
                    if it.get("policy"):
                        pools.setdefault("policy:%s" % it["category"], {}).setdefault((cid.split("/")[1], col, it["key"]), (cid, col, it))
                for it in load(results, cid, col, "ungradable"):
                    pools.setdefault("ungradable:%s" % it["reason"], {}).setdefault((cid.split("/")[1], col, it["key"]), (cid, col, it))
            for it in json.load(open(os.path.join(results, "lists", "%s.dead_excluded.json" % cid.replace("/", "__")))):
                pools.setdefault("excluded:%s" % it["bucket"], {}).setdefault((cid.split("/")[1], it["column"], "%s:%d" % (it["file"], it["line"])), (cid, it["column"], it))
        for name in sorted(pools):
            items = sorted(pools[name].values(), key=lambda x: ident(x[0], x[1], name, x[2].get("key") or x[2]["file"], x[2].get("line")))
            rng = random.Random("%s|%s|%s" % (SEED, lang, name))
            rows = []
            for cid, col, it in rng.sample(items, min(NB, len(items))):
                if "key" not in it:
                    it = dict(it, key="%s:%d" % (it["file"], it["line"]))
                rows.append(row(cid, col, "bucket", it, bucket=name, bucket_size=len(items)))
            out["buckets"][lang][name] = rows
    path = os.path.join(results, "spotcheck-dead-sample.json")
    json.dump(out, open(path, "w"), indent=1)
    print("wrote", path, {l: {k: len(v) for k, v in d.items()} for l, d in out["languages"].items()},
          {l: {k: len(v) for k, v in d.items()} for l, d in out["buckets"].items()})


def merge(results):
    sam = json.load(open(os.path.join(results, "spotcheck-dead-sample.json")))
    ver = json.load(open(os.path.join(HERE, "spotcheck-dead-verdicts.json")))
    sc = {"seed": sam["seed"], "n": sam["n"], "nb": sam["nb"], "notes": ver.get("_notes", ""), "languages": {}, "buckets": {}}
    missing = []

    def take(r, allowed):
        v = ver.get(r["id"])
        if not v:
            missing.append(r["id"])
            return None
        assert v["verdict"] in allowed, v
        return {"id": r["id"], "decl": r["decl"], "cell": r["cell"], "column": r["column"], "verdict": v["verdict"], "note": v["note"], "bucket_size": r.get("bucket_size")}

    for lang, kinds in sam["languages"].items():
        sc["languages"][lang] = {k: [x for x in (take(r, ("lidx_error", "oracle_artefact", "matching_artefact")) for r in rows) if x] for k, rows in kinds.items()}
    for lang, bks in sam["buckets"].items():
        sc["buckets"][lang] = {n: [x for x in (take(r, ("correct_exclusion", "misbucketed")) for r in rows) if x] for n, rows in bks.items()}
    if missing:
        sys.exit("verdicts missing for sample ids: %s" % ", ".join(missing))
    json.dump(sc, open(os.path.join(results, "spotcheck-dead.json"), "w"), indent=1)
    print("wrote spotcheck-dead.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["sample", "merge"])
    ap.add_argument("--results", default=os.path.join(HERE, "results"))
    ap.add_argument("--cache", default=os.environ.get("LIDX_ORACLE_CACHE", os.path.expanduser("~/.cache/lidx-oracle")))
    a = ap.parse_args()
    sample(a.results, a.cache) if a.cmd == "sample" else merge(a.results)
