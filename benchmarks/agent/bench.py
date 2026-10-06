#!/usr/bin/env python3
"""lidx agent benchmark: does lidx make Claude Code better at repo questions?

Paired A/B over the same questions: profile `baseline` (Claude Code read-only
built-ins) vs `lidx` (same + the lidx MCP server).  A blinded judge scores each
answer against an isolated reference.  Python 3 stdlib only.  See README.md.

    bench.py run    --tasks tasks/x.json --profiles baseline,lidx --trials N --model M --budget USD --out DIR
    bench.py judge  DIR --references references/x.json
    bench.py report DIR [--json out.json]
    bench.py dry-run
"""
import argparse
import collections
import concurrent.futures as cf
import glob
import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))      # the lidx checkout holding references/ and results
DEFAULT_WORKDIR = os.path.join(HERE, ".work")
PROFILES = ("baseline", "lidx")
BASE_TOOLS = ["Read", "Grep", "Glob"]
LIDX_TOOL = "mcp__lidx__lidx"
TrialResult = collections.namedtuple("TrialResult", "status task profile n")
# metric -> (value format, change is relative %)
METRIC_SPECS = {"judge": ("%.1f", False), "input_tokens": ("%.0f", True),
                "tool_calls": ("%.1f", True), "wall_seconds": ("%.1f", True)}
INSTRUCTION = ("Answer using the repository in the current directory. "
               "Be specific: name the files, classes and functions involved.")
DIMS = ("correctness", "completeness", "relevance", "clarity", "coherence")
JUDGE_SCHEMA = {
    "type": "object",
    "properties": {d: {"type": "integer", "minimum": 1, "maximum": 20} for d in DIMS},
    "required": list(DIMS),
    "additionalProperties": False,
}
DRY_MODEL = "claude-haiku-4-5-20251001"
DRY_TASKS = "^(requests:16|sqlfluff:2)$"
DEFAULT_JUDGE_MODEL = "claude-opus-5-5"
FLAG_JUDGE_RANGE = 20       # judge-score spread across trials that raises a flag
FLAG_CALLS_RATIO = 2.0      # tool-call max/min across trials that raises a flag


class Fatal(Exception):
    pass


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def sh(cmd, cwd=None, timeout=1800):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise Fatal("command failed (%d): %s\n%s" % (p.returncode, " ".join(cmd), p.stderr.strip()[-1500:]))
    return p.stdout


def safe(name):
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


def file_hash(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()[:16]


def version_of(cmd):
    try:
        return subprocess.run(cmd + ["--version"], capture_output=True, text=True, timeout=20).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_json(path, doc):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)


def effort_args(model, effort):
    # Haiku does not support --effort.
    if not effort or "haiku" in (model or "").lower():
        return []
    return ["--effort", effort]


# --------------------------------------------------------------------------
# tasks and repos
# --------------------------------------------------------------------------

def load_tasks(paths, only=None):
    """-> (tasks, skipped messages).  Each task carries its source file."""
    tasks, notes = [], []
    pat = re.compile(only) if only else None
    for path in paths:
        doc = read_json(path)
        for t in doc["tasks"]:
            repo = t.get("repo") or {}
            if "path" in repo:
                expanded = os.path.expandvars(os.path.expanduser(repo["path"]))
                if "$" in expanded:
                    notes.append("skip %s: environment variable in repo.path %r is not set" % (t["id"], repo["path"]))
                    continue
                if not os.path.isabs(expanded):
                    expanded = os.path.join(os.path.dirname(os.path.abspath(path)), expanded)
                t = dict(t, repo={"path": os.path.normpath(expanded)})
            elif not (repo.get("url") and repo.get("commit")):
                raise Fatal("task %s: repo needs {url, commit} or {path}" % t["id"])
            if pat and not pat.search(t["id"]):
                continue
            tasks.append(dict(t, source=os.path.abspath(path)))
    ids = [t["id"] for t in tasks]
    if len(ids) != len(set(ids)):
        raise Fatal("duplicate task ids across --tasks files")
    return tasks, notes


_cache_lock = threading.Lock()


def cached_repo(repo, workdir):
    """Pinned-commit clone cache: git init + fetch --depth 1 + checkout."""
    if "path" in repo:
        return repo["path"]
    key = "%s-%s" % (hashlib.sha1(repo["url"].encode()).hexdigest()[:8], repo["commit"][:12])
    dest = os.path.join(workdir, "cache", key)
    with _cache_lock:
        if not os.path.isfile(os.path.join(dest, ".cache-ok")):
            if os.path.isdir(dest):
                shutil.rmtree(dest)
            os.makedirs(dest)
            print("  caching %s @ %s ..." % (repo["url"], repo["commit"][:12]), flush=True)
            sh(["git", "init", "-q"], cwd=dest)
            sh(["git", "fetch", "-q", "--depth", "1", repo["url"], repo["commit"]], cwd=dest)
            sh(["git", "checkout", "-q", repo["commit"]], cwd=dest)
            open(os.path.join(dest, ".cache-ok"), "w").close()   # untracked marker; cloned away
    return dest


def fresh_clone(src, dest):
    try:
        sh(["git", "clone", "-q", "--no-hardlinks", src, dest])
    except Fatal:
        if os.path.isdir(dest):
            shutil.rmtree(dest)
        shutil.copytree(src, dest)     # source is not a git checkout


# --------------------------------------------------------------------------
# stream-json parsing
# --------------------------------------------------------------------------

def parse_stream(lines):
    """Parse `claude --output-format stream-json` lines.

    -> {init, result, tool_counts, tool_calls, lidx_calls}
    Tool calls are tool_use blocks in assistant events; usage is cumulative
    over the session, taken from the final result event.
    """
    init, result, counts = None, None, {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        kind = ev.get("type")
        if kind == "system" and ev.get("subtype") == "init":
            init = {"tools": ev.get("tools", []), "mcp_servers": ev.get("mcp_servers", []),
                    "model": ev.get("model")}
        elif kind == "assistant":
            content = (ev.get("message") or {}).get("content") or []
            for block in content if isinstance(content, list) else []:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    counts[block.get("name", "?")] = counts.get(block.get("name", "?"), 0) + 1
        elif kind == "result":
            result = ev
    return {"init": init, "result": result, "tool_counts": counts,
            "tool_calls": sum(counts.values()),
            "lidx_calls": sum(n for k, n in counts.items() if k.startswith("mcp__lidx__"))}


def trial_metrics(parsed):
    """Metrics for trial.json from parse_stream output; (metrics, error|None)."""
    res = parsed["result"]
    if res is None:
        return None, "no result event in stream"
    u = res.get("usage") or {}
    comp = {"input": u.get("input_tokens", 0) or 0,
            "cache_read": u.get("cache_read_input_tokens", 0) or 0,
            "cache_creation": u.get("cache_creation_input_tokens", 0) or 0}
    m = {"input_tokens": sum(comp.values()), "input_components": comp,
         "output_tokens": u.get("output_tokens", 0) or 0,
         "tool_calls": parsed["tool_calls"], "lidx_calls": parsed["lidx_calls"],
         "tool_counts": parsed["tool_counts"], "num_turns": res.get("num_turns"),
         "duration_ms": res.get("duration_ms"), "cost_usd": res.get("total_cost_usd") or 0.0}
    err = None
    if res.get("subtype") != "success" or res.get("is_error"):
        err = "claude ended with subtype=%s" % res.get("subtype")
    return m, err


def expected_tools(profile):
    return sorted(BASE_TOOLS + ([LIDX_TOOL] if profile == "lidx" else []))


def check_init(profile, init):
    """Isolation check on the system/init event; -> error string or None."""
    if init is None:
        return "no system/init event in stream"
    want = expected_tools(profile)
    got = sorted(init.get("tools") or [])
    if got != want:
        return "tool set %s != expected %s" % (got, want)
    servers = init.get("mcp_servers") or []
    if profile == "baseline":
        if servers:
            return "baseline has MCP servers: %s" % json.dumps(servers)
        return None
    st = [s for s in servers if s.get("name") == "lidx"]
    if not st or st[0].get("status") != "connected":
        return "lidx mcp server not connected: %s" % json.dumps(servers)
    return None


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

def build_prompt(question):
    return "%s\n\n%s" % (question.strip(), INSTRUCTION)


def deny_rules(paths):
    """Read deny rules for absolute paths (`//` = absolute in permission syntax; Read rules cover Grep/Glob)."""
    seen = []
    for p in paths:
        for q in (os.path.abspath(p), os.path.realpath(p)):
            if q not in seen:
                seen.append(q)
    return ["Read(//%s/**)" % q.lstrip("/") for q in seen]


def deny_args(out):
    """--disallowedTools hiding the lidx checkout (references) and the run dir (other trials' results)."""
    return ["--disallowedTools"] + deny_rules([REPO_ROOT, out])


def default_scratch(out):
    return os.path.join(os.path.realpath(tempfile.gettempdir()), "lidx-agent-bench", safe(os.path.basename(out)))


def claude_cmd(args, profile, mcp_json, prompt):
    allowed = BASE_TOOLS + ([LIDX_TOOL] if profile == "lidx" else [])
    cmd = ["claude", "-p", prompt, "--model", args.model] + effort_args(args.model, args.effort)
    cmd += ["--output-format", "stream-json", "--verbose",
            "--setting-sources", "", "--strict-mcp-config", "--mcp-config", mcp_json,
            "--no-session-persistence", "--disable-slash-commands",
            "--max-budget-usd", str(args.budget),
            "--tools", ",".join(BASE_TOOLS), "--allowedTools", ",".join(allowed)]
    return cmd + deny_args(args.out)


def run_trial(args, task, profile, n, cache):
    tdir = os.path.join(args.out, safe(task["id"]), profile, str(n))
    sdir = os.path.join(args.scratch, safe(task["id"]), profile, str(n))     # clone + db, outside the run dir
    tjson = os.path.join(tdir, "trial.json")
    if os.path.isfile(tjson):
        try:
            if read_json(tjson).get("status") == "ok":
                return TrialResult("skip", task, profile, n)
        except ValueError:
            pass
    if os.path.isdir(tdir):
        shutil.rmtree(tdir)
    os.makedirs(tdir)
    if os.path.isdir(sdir):
        shutil.rmtree(sdir)
    os.makedirs(sdir)
    repo_dir, db = os.path.join(sdir, "repo"), os.path.join(sdir, "lidx.sqlite")   # db outside repo cwd
    doc = {"task": task["id"], "category": task.get("category"), "profile": profile, "trial": n,
           "question": task["question"], "model": args.model, "effort": args.effort,
           "status": "error", "error": None, "index_seconds": None, "wall_seconds": None}
    try:
        fresh_clone(cache, repo_dir)
        mcp = {"mcpServers": {}}
        if profile == "lidx":
            t0 = time.monotonic()
            p = subprocess.run([args.lidx, "reindex", "--repo", repo_dir, "--db", db],
                               capture_output=True, text=True, timeout=args.timeout)
            doc["index_seconds"] = round(time.monotonic() - t0, 2)
            if p.returncode != 0:
                raise Fatal("lidx reindex failed (%d): %s" % (p.returncode, p.stderr[-500:]))
            mcp = {"mcpServers": {"lidx": {"command": args.lidx, "args": [
                "mcp-serve", "--repo", repo_dir, "--db", db, "--watch", "off"]}}}
        mcp_json = os.path.join(tdir, "mcp.json")
        write_json(mcp_json, mcp)
        cmd = claude_cmd(args, profile, mcp_json, build_prompt(task["question"]))
        t0 = time.monotonic()
        try:
            with open(os.path.join(tdir, "stream.jsonl"), "w") as out, \
                 open(os.path.join(tdir, "stderr.txt"), "w") as err:
                p = subprocess.run(cmd, cwd=repo_dir, stdout=out, stderr=err, timeout=args.timeout)
            doc["wall_seconds"] = round(time.monotonic() - t0, 2)
            doc["exit_code"] = p.returncode
        except subprocess.TimeoutExpired:
            doc["wall_seconds"] = round(time.monotonic() - t0, 2)
            raise Fatal("timeout after %ss" % args.timeout)
        with open(os.path.join(tdir, "stream.jsonl"), encoding="utf-8") as f:
            parsed = parse_stream(f)
        doc["init"] = parsed["init"]
        metrics, err = trial_metrics(parsed)
        err = check_init(profile, parsed["init"]) or err
        res = parsed["result"] or {}
        doc.update(metrics=metrics, answer=res.get("result"), subtype=res.get("subtype"),
                   permission_denials=res.get("permission_denials") or [])
        if doc["permission_denials"]:
            print("  WARNING %s/%s/%s: %d permission denial(s)" %
                  (task["id"], profile, n, len(doc["permission_denials"])), flush=True)
        if err is None and not (res.get("result") or "").strip():
            err = "empty answer"
        if err is None:
            doc["status"] = "ok"
        else:
            doc["error"] = err
    except Exception as e:  # noqa: BLE001 - recorded in trial.json, never aborts the run
        doc["error"] = "%s: %s" % (type(e).__name__, e)
    finally:
        for p in [repo_dir] + glob.glob(db + "*"):     # clone + sqlite/-wal/-shm/lock sidecars
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.isfile(p):
                os.remove(p)
        shutil.rmtree(sdir, ignore_errors=True)
    write_json(tjson, doc)
    return TrialResult(doc["status"], task, profile, n)


def cmd_run(args):
    profiles = [p.strip() for p in args.profiles.split(",") if p.strip()]
    for p in profiles:
        if p not in PROFILES:
            raise Fatal("unknown profile %r (choose from %s)" % (p, ",".join(PROFILES)))
    if getattr(args, "scratch", None) is None:
        args.scratch = default_scratch(args.out)
    if "lidx" in profiles and not os.access(args.lidx, os.X_OK):
        raise Fatal("lidx binary not found or not executable: %s" % args.lidx)
    tasks, notes = load_tasks(args.tasks, args.only)
    for m in notes:
        print(m)
    if not tasks:
        raise Fatal("no tasks selected")
    os.makedirs(args.out, exist_ok=True)
    write_json(os.path.join(args.out, "run.json"), {
        "started": time.strftime("%Y-%m-%dT%H:%M:%S"), "model": args.model, "effort": args.effort,
        "budget_usd": args.budget, "timeout": args.timeout, "trials": args.trials, "profiles": profiles,
        "claude_version": version_of(["claude"]),
        "lidx_version": version_of([args.lidx]) if "lidx" in profiles else None,
        "prompt_instruction": INSTRUCTION,
        "task_files": {os.path.basename(p): file_hash(p) for p in args.tasks},
        "tasks": [t["id"] for t in tasks]})
    caches = {t["id"]: cached_repo(t["repo"], args.workdir) for t in tasks}
    jobs = [(t, p, n) for t in tasks for p in profiles for n in range(1, args.trials + 1)]
    print("%d trial(s), parallel %d" % (len(jobs), args.jobs), flush=True)
    bad = 0
    with cf.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futs = [ex.submit(run_trial, args, t, p, n, caches[t["id"]]) for t, p, n in jobs]
        for f in cf.as_completed(futs):
            r = f.result()
            bad += r.status == "error"
            print("  %-5s %s / %s / %d" % (r.status, r.task["id"], r.profile, r.n), flush=True)
    if bad:
        print("%d trial(s) errored (kept in raw output, excluded from means)" % bad)
    return 0


# --------------------------------------------------------------------------
# judge
# --------------------------------------------------------------------------

def judge_prompt(question, reference, candidate):
    # Rubric from zvec-ai/zvec-grep (Apache-2.0), see NOTICE.  No profile/tool info.
    return """You are a strict evaluator. Score the candidate only against the supplied question and reference answer.

Score each dimension as an integer from 1 through 20:
- correctness: factual agreement with the reference; penalize errors.
- completeness: coverage of the reference's important points; penalize omissions.
- relevance: focus on the question; penalize tangents.
- clarity: precision and ease of understanding.
- coherence: logical organization and consistency of the explanation.

Scores 16-20 are reserved for excellent answers. When uncertain, choose the lower score. Treat the reference as judge-only evidence, not text to reproduce.

Question:
%s

Reference answer:
%s

Candidate answer:
%s

Return only one strict JSON object with exactly these five integer fields and no markdown:
{"correctness": 1, "completeness": 1, "relevance": 1, "clarity": 1, "coherence": 1}
""" % (question, reference, candidate)


def parse_scores(doc):
    """Scores from `claude --output-format json --json-schema` output; None if invalid."""
    obj = doc.get("structured_output")
    if obj is None:
        try:
            obj = json.loads(doc.get("result") or "")
        except ValueError:
            return None
    if not isinstance(obj, dict) or set(obj) != set(DIMS):
        return None
    for v in obj.values():
        if type(v) is not int or not 1 <= v <= 20:      # strict ints, no bools/floats
            return None
    return {d: obj[d] for d in DIMS}


def judge_one(args, tdir, trial, reference, empty_mcp):
    prompt = judge_prompt(trial["question"], reference, trial["answer"])
    cmd = ["claude", "-p", prompt, "--model", args.model] + effort_args(args.model, args.effort)
    cmd += ["--tools", "", "--setting-sources", "", "--strict-mcp-config", "--mcp-config", empty_mcp,
            "--no-session-persistence", "--disable-slash-commands",
            "--output-format", "json", "--json-schema", json.dumps(JUDGE_SCHEMA)]
    cost, usage, last = 0.0, [], "unknown"
    with tempfile.TemporaryDirectory(prefix="bench-judge-") as cwd:
        for attempt in range(1, 4):
            t0 = time.monotonic()
            p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=args.timeout)
            secs = round(time.monotonic() - t0, 2)
            try:
                out = json.loads(p.stdout)
            except ValueError:
                last = "unparseable output (exit %d): %s" % (p.returncode, p.stderr[-300:])
                continue
            cost += out.get("total_cost_usd") or 0.0
            usage.append(out.get("usage"))
            scores = parse_scores(out)
            if scores:
                return {"scores": scores, "total": sum(scores.values()), "model": args.model,
                        "effort": args.effort, "attempts": attempt, "seconds": secs,
                        "cost_usd": cost, "usage": usage}
            last = "invalid scores: %s" % json.dumps(out.get("structured_output") or out.get("result"))[:200]
    raise Fatal(last)


def cmd_judge(args):
    refs = {}
    for p in args.references:
        refs.update(read_json(p)["references"])
    todo = []
    for tj in sorted(glob.glob(os.path.join(args.run_dir, "*", "*", "*", "trial.json"))):
        trial = read_json(tj)
        jpath = os.path.join(os.path.dirname(tj), "judge.json")
        if trial.get("status") != "ok" or os.path.isfile(jpath):
            continue
        if trial["task"] not in refs:
            print("  no reference for %s, not judged" % trial["task"])
            continue
        todo.append((jpath, trial))
    print("%d trial(s) to judge" % len(todo), flush=True)
    with tempfile.TemporaryDirectory(prefix="bench-mcp-") as d:
        empty = os.path.join(d, "mcp.json")
        write_json(empty, {"mcpServers": {}})
        failed = 0

        def work(item):
            jpath, trial = item
            try:
                write_json(jpath, judge_one(args, os.path.dirname(jpath), trial, refs[trial["task"]], empty))
                return trial, None
            except Exception as e:  # noqa: BLE001
                return trial, e

        with cf.ThreadPoolExecutor(max_workers=args.jobs) as ex:
            for trial, err in ex.map(work, todo):
                tag = "%s / %s / %s" % (trial["task"], trial["profile"], trial["trial"])
                if err:
                    failed += 1
                    print("  judge FAILED %s: %s" % (tag, err), flush=True)
                else:
                    print("  judged %s" % tag, flush=True)
    return 2 if failed else 0


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------

def load_trials(run_dir):
    out = []
    for tj in sorted(glob.glob(os.path.join(run_dir, "*", "*", "*", "trial.json"))):
        t = read_json(tj)
        jp = os.path.join(os.path.dirname(tj), "judge.json")
        t["judge"] = read_json(jp) if os.path.isfile(jp) else None
        out.append(t)
    return out


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def spread(xs):
    return {"mean": mean(xs), "sd": statistics.stdev(xs) if len(xs) > 1 else None,
            "min": min(xs) if xs else None, "max": max(xs) if xs else None, "n": len(xs)}


METRICS = tuple(METRIC_SPECS)


def trial_values(t):
    """Per-trial metric values; judge is None when unjudged."""
    m = t["metrics"]
    return {"judge": t["judge"]["total"] if t.get("judge") else None,
            "input_tokens": m["input_tokens"], "tool_calls": m["tool_calls"],
            "wall_seconds": t["wall_seconds"]}


def flag_task(per_profile):
    """Reasons trials of one profile disagree (list of strings)."""
    why = []
    j, c = per_profile["judge"], per_profile["tool_calls"]
    if j["n"] > 1 and j["max"] - j["min"] > FLAG_JUDGE_RANGE:
        why.append("judge range %d" % (j["max"] - j["min"]))
    if c["n"] > 1 and c["max"] > FLAG_CALLS_RATIO * c["min"] and c["max"] > c["min"]:
        why.append("tool calls %d-%d" % (c["min"], c["max"]))
    return why


def pct_change(base, other):
    if base in (None, 0) or other is None:
        return None
    return (other - base) / base * 100.0


def overall_agg(m, xs):
    return mean(xs) if m == "judge" else sum(xs)      # zvec-grep: mean judge, summed efficiency


def changes(b, l):
    """Per-metric change from baseline dict b to lidx dict l: points if absolute, else relative %."""
    def one(m):
        if METRIC_SPECS[m][1]:
            return pct_change(b[m], l[m])
        return None if b[m] is None or l[m] is None else l[m] - b[m]
    return {m: one(m) for m in METRICS}


def overall_sd(per_task, tasks, m, profile):
    """Stdev over trial index i of the overall aggregate at i (tasks lacking trial i make i unusable)."""
    idx = [set(per_task[t][profile][m]) for t in tasks]
    common = sorted(set.intersection(*idx)) if idx else []
    if len(common) < 2:
        return None
    return statistics.stdev(overall_agg(m, [per_task[t][profile][m][i] for t in tasks]) for i in common)


def aggregate(trials):
    """Pure aggregation of trial dicts -> report structure."""
    ok = [t for t in trials if t["status"] == "ok"]
    errors = [t for t in trials if t["status"] != "ok"]
    profiles = sorted({t["profile"] for t in trials}, key=lambda p: PROFILES.index(p) if p in PROFILES else 9)
    tasks, by_trial = {}, {}
    for t in trials:
        tasks.setdefault(t["task"], {})
    for t in ok:
        v = trial_values(t)
        tasks[t["task"]].setdefault(t["profile"], []).append(v)
        for m in METRICS:
            if v[m] is not None:
                by_trial.setdefault(t["task"], {}).setdefault(t["profile"], {}).setdefault(m, {})[t["trial"]] = v[m]
    err_counts = {}
    for t in errors:
        c = err_counts.setdefault(t["task"], {})
        c[t["profile"]] = c.get(t["profile"], 0) + 1
    rows = []
    for task in sorted(tasks):
        errs = {p: err_counts.get(task, {}).get(p, 0) for p in profiles}
        row = {"task": task, "profiles": {}, "flags": {}, "index_seconds": None, "errors": errs,
               "error_asymmetry": len(set(errs.values())) > 1}
        for p, vals in tasks[task].items():
            row["profiles"][p] = {m: spread([v[m] for v in vals if v[m] is not None]) for m in METRICS}
            why = flag_task(row["profiles"][p])
            if why:
                row["flags"][p] = why
        idx = [t["index_seconds"] for t in ok if t["task"] == task and t["profile"] == "lidx"
               and t.get("index_seconds") is not None]
        row["index_seconds"] = mean(idx)
        if "baseline" in row["profiles"] and "lidx" in row["profiles"]:
            row["change"] = changes({m: row["profiles"]["baseline"][m]["mean"] for m in METRICS},
                                    {m: row["profiles"]["lidx"][m]["mean"] for m in METRICS})
        rows.append(row)
    overall = {"profiles": {}, "sd": {}, "change": {}, "paired_tasks": {},
               "errors": {p: sum(err_counts.get(t, {}).get(p, 0) for t in tasks) for p in profiles}}
    both = [r for r in rows if "baseline" in r["profiles"] and "lidx" in r["profiles"]]
    for m in METRICS:
        # paired: only tasks where both profiles have a value for this metric
        use = [r for r in both if r["profiles"]["baseline"][m]["n"] and r["profiles"]["lidx"][m]["n"]]
        overall["paired_tasks"][m] = len(use)
        for p in ("baseline", "lidx"):
            overall["profiles"].setdefault(p, {})[m] = \
                overall_agg(m, [r["profiles"][p][m]["mean"] for r in use]) if use else None
            overall["sd"].setdefault(p, {})[m] = \
                overall_sd(by_trial, [r["task"] for r in use], m, p) if use else None
    if both:
        overall["change"] = changes(overall["profiles"]["baseline"], overall["profiles"]["lidx"])
    agent_cost = sum((t.get("metrics") or {}).get("cost_usd", 0.0) for t in trials)
    judge_cost = sum(t["judge"]["cost_usd"] for t in trials if t.get("judge"))
    return {"profiles": profiles, "rows": rows, "overall": overall,
            "cost": {"agent_usd": agent_cost, "judge_usd": judge_cost, "total_usd": agent_cost + judge_cost},
            "unjudged": sum(1 for t in ok if not t.get("judge")),
            "denials": [[t["task"], t["profile"], t["trial"], len(t.get("permission_denials") or [])]
                        for t in trials if t.get("permission_denials")],
            "errors": [{"task": t["task"], "profile": t["profile"], "trial": t["trial"], "error": t.get("error"),
                        "cost_usd": (t.get("metrics") or {}).get("cost_usd", 0.0)} for t in errors]}


def fnum(v, metric):
    return "-" if v is None else METRIC_SPECS[metric][0] % v


def fchange(v, metric):
    if v is None:
        return "-"
    return ("%+.0f%%" if METRIC_SPECS[metric][1] else "%+.1f") % v


def cell(s, metric, sd=True):
    if not s or s["n"] == 0:
        return "-"
    txt = fnum(s["mean"], metric)
    if sd and s["sd"] is not None:
        txt += "±" + fnum(s["sd"], metric)
    return txt


def err_cell(errs):
    return "/".join(str(errs.get(p, 0)) for p in ("baseline", "lidx"))


def render(rep, settings=None):
    lines = []
    if settings:
        lines.append("Model `%s`%s, budget $%s/trial, %s trial(s) per profile.  claude %s, lidx %s." % (
            settings.get("model"), " effort " + settings["effort"] if settings.get("effort") else "",
            settings.get("budget_usd"), settings.get("trials"), settings.get("claude_version"),
            settings.get("lidx_version")))
        lines.append("")
    lines.append("Each cell: baseline / lidx / change (judge: points; others: relative %). `±` is the "
                 "per-profile trial stdev (shown when trials > 1).  Means exclude errored trials; "
                 "Errors = errored trials, baseline/lidx.")
    lines.append("")
    lines.append("| Task | Judge (5-100) | Input tokens | Tool calls | Wall s | Index s | Errors | |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in rep["rows"]:
        b, l = r["profiles"].get("baseline"), r["profiles"].get("lidx")
        cols = []
        for m in METRICS:
            parts = [cell(b[m], m) if b else "-", cell(l[m], m) if l else "-"]
            if "change" in r:
                parts.append(fchange(r["change"][m], m))
            cols.append(" / ".join(parts))
        notes = ["%s: %s" % (p, ", ".join(w)) for p, w in sorted(r["flags"].items())]
        if r["error_asymmetry"]:
            notes.append("errors differ between profiles")
        lines.append("| %s | %s | %s | %s | %s |" % (
            r["task"], " | ".join(cols), fnum(r["index_seconds"], "wall_seconds"), err_cell(r["errors"]),
            ("⚠ " + "; ".join(notes)) if notes else ""))
    o = rep["overall"]
    if o["profiles"]:
        b, l = o["profiles"].get("baseline", {}), o["profiles"].get("lidx", {})
        bs, ls = o["sd"].get("baseline", {}), o["sd"].get("lidx", {})
        cols = []
        for m in METRICS:
            def sdv(mean_, sd_, m=m):
                return fnum(mean_, m) + (("±" + fnum(sd_, m)) if sd_ is not None and mean_ is not None else "")
            cols.append(" / ".join([sdv(b.get(m), bs.get(m)), sdv(l.get(m), ls.get(m))] +
                                   ([fchange(o["change"].get(m), m)] if o["change"] else [])))
        asym = len(set(o["errors"].values())) > 1
        lines.append("| **Overall** | %s | | %s | %s |" % (
            " | ".join(cols), err_cell(o["errors"]), "⚠ errors differ between profiles" if asym else ""))
        lines.append("")
        lines.append("Overall: mean judge score across tasks; input tokens, tool calls and wall time are sums of "
                     "per-task means; change is computed from those aggregates.  `±` on the Overall row is the "
                     "stdev across trial index i of that aggregate computed on trial i alone (needs every paired "
                     "task to have trial i).  Paired tasks per metric: %s." %
                     ", ".join("%s=%d" % kv for kv in o["paired_tasks"].items()))
    c = rep["cost"]
    lines += ["", "Cost: agent $%.2f + judge $%.2f = $%.2f." % (c["agent_usd"], c["judge_usd"], c["total_usd"])]
    if rep["unjudged"]:
        lines.append("%d successful trial(s) have no judge score yet (run `bench.py judge`)." % rep["unjudged"])
    if rep["denials"]:
        lines += ["", "Permission denials (answers may be affected): " +
                  "; ".join("%s/%s/%s x%d" % tuple(d) for d in rep["denials"])]
    lines += ["", "Errored / excluded trials: %s" % ("none" if not rep["errors"] else "")]
    for e in rep["errors"]:
        lines.append("- %s / %s / %s: %s (cost $%.2f)" % (e["task"], e["profile"], e["trial"], e["error"], e["cost_usd"]))
    return "\n".join(lines)


def cmd_report(args):
    rep = aggregate(load_trials(args.run_dir))
    rp = os.path.join(args.run_dir, "run.json")
    print(render(rep, read_json(rp) if os.path.isfile(rp) else None))
    if args.json:
        write_json(args.json, rep)
    return 0


# --------------------------------------------------------------------------
# cli
# --------------------------------------------------------------------------

PATH_ARGS = ("out", "workdir", "scratch", "lidx", "tasks", "references", "run_dir", "json")


def normalize_paths(args):
    """Absolute-ize every path arg: claude runs with cwd=<clone>, so relative paths would break."""
    for name in PATH_ARGS:
        v = getattr(args, name, None)
        if isinstance(v, list):
            setattr(args, name, [os.path.abspath(os.path.expanduser(x)) for x in v])
        elif v:
            setattr(args, name, os.path.abspath(os.path.expanduser(v)))
    return args


def default_lidx():
    return os.path.expanduser("~/.local/bin/lidx")


def add_run_args(p, dry=False):
    p.add_argument("--tasks", action="append", required=not dry, help="task file (repeatable)")
    p.add_argument("--only", help="regex on task ids")
    p.add_argument("--profiles", default="baseline,lidx")
    p.add_argument("--trials", type=int, default=1)
    p.add_argument("--model", required=not dry, help="required: runs spend money")
    p.add_argument("--effort", help="omitted for haiku models")
    p.add_argument("--budget", type=float, required=not dry, help="max USD per trial")
    p.add_argument("--timeout", type=int, default=900, help="seconds per claude/lidx call")
    p.add_argument("-j", "--jobs", type=int, default=1, help="parallel trials")
    p.add_argument("--out", required=not dry, help="run directory")
    p.add_argument("--lidx", default=default_lidx(), help="lidx binary (default ~/.local/bin/lidx)")
    p.add_argument("--workdir", default=DEFAULT_WORKDIR, help="repo clone cache")
    p.add_argument("--scratch", help="per-trial clone/db root (default: <tmp>/lidx-agent-bench/<run name>)")


def add_judge_args(p, dry=False):
    p.add_argument("--references", action="append", required=not dry, help="reference file (repeatable)")
    p.add_argument("--model", default=DEFAULT_JUDGE_MODEL if not dry else DRY_MODEL)
    p.add_argument("--effort", default="high" if not dry else None)
    p.add_argument("--timeout", type=int, default=600)
    p.add_argument("-j", "--jobs", type=int, default=1)


def cmd_dry_run(args):
    out = args.out or os.path.join(DEFAULT_WORKDIR, "dry-run-" + time.strftime("%Y%m%d-%H%M%S"))
    run = argparse.Namespace(
        tasks=[os.path.join(HERE, "tasks", "swe-qa.json")], only=DRY_TASKS, profiles="baseline,lidx",
        trials=1, model=DRY_MODEL, effort=None, budget=0.5, timeout=900, jobs=args.jobs, out=out,
        lidx=args.lidx, workdir=args.workdir, scratch=args.scratch)
    cmd_run(run)
    judge = argparse.Namespace(run_dir=out, references=[os.path.join(HERE, "references", "swe-qa.json")],
                               model=DRY_MODEL, effort=None, timeout=600, jobs=args.jobs)
    cmd_judge(judge)
    print()
    return cmd_report(argparse.Namespace(run_dir=out, json=None))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run paired trials")
    add_run_args(r)
    j = sub.add_parser("judge", help="score answers with a blinded judge")
    j.add_argument("run_dir")
    add_judge_args(j)
    rp = sub.add_parser("report", help="markdown report")
    rp.add_argument("run_dir")
    rp.add_argument("--json", help="also write aggregates here")
    d = sub.add_parser("dry-run", help="2 tasks, 1 trial, Haiku, both profiles, judge, report (~$0.30)")
    d.add_argument("--out")
    d.add_argument("--lidx", default=default_lidx())
    d.add_argument("--workdir", default=DEFAULT_WORKDIR)
    d.add_argument("--scratch")
    d.add_argument("-j", "--jobs", type=int, default=2)
    args = ap.parse_args()
    normalize_paths(args)
    try:
        return {"run": cmd_run, "judge": cmd_judge, "report": cmd_report, "dry-run": cmd_dry_run}[args.cmd](args)
    except Fatal as e:
        print("error: %s" % e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
