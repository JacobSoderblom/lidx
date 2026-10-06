# lidx agent benchmark

Measures whether lidx makes Claude Code better at answering questions about a
repository. Paired A/B over identical questions:

| profile | tools |
|---|---|
| `baseline` | Claude Code built-ins, read-only: `Read`, `Grep`, `Glob` |
| `lidx` | the same, plus the lidx MCP server (`mcp__lidx__lidx`) |

An independent, blinded judge scores each answer against a reference. The
report gives judge score, input tokens, tool calls and wall time per task and
overall, with variance across trials. Python 3 stdlib only; needs `claude`
(logged in; no API key is read) and a `lidx` binary (default `~/.local/bin/lidx`).

This is separate from `benchmarks/answers/` (deterministic precision/recall of
lidx's own graph answers, no LLM).

## Running

```bash
cd benchmarks/agent
python3 bench.py dry-run          # 2 tasks, 1 trial, Haiku, both profiles, judge, report (~$0.50)

# full run: model, budget and output dir are always explicit
python3 bench.py run --tasks tasks/swe-qa.json --tasks tasks/derived.json \
    --profiles baseline,lidx --trials 3 --model <model> --effort <level> \
    --budget 2 --timeout 900 -j 4 --out .work/run-1
python3 bench.py judge .work/run-1 --references references/swe-qa.json --references references/derived.json
python3 bench.py report .work/run-1 [--json out.json]
python3 test_bench.py             # parser/report/isolation tests
```

`run` is resumable: trials whose `trial.json` has `status: ok` are skipped, errored ones are rerun.
`judge` skips trials that already have a `judge.json`. `--only REGEX` filters task ids.
All path arguments (`--out`, `--workdir`, `--scratch`, `--lidx`, task/reference files, run dir) are made absolute at parse time,
so relative paths work even though `claude` runs with the clone as its cwd.
`--reuse-baseline RUN_DIR` copies the finished baseline trials (selected tasks, trials 1..`--trials`) from an earlier run and runs only
the `lidx` profile: use it to iterate on lidx changes, since the baseline only needs to be run once per model/effort/task set.
It refuses if the source run's model, effort, budget or task-file hashes differ, or a needed baseline trial is missing or failed.
The new `run.json` records `reused_baseline_from`, `lidx_version` and `lidx_path`; the report header shows them. Copied trials keep
their `judge.json`, so `judge` only scores the new lidx trials.
Output layout: `RUN/<task>/<profile>/<trial>/{stream.jsonl, stderr.txt, mcp.json, trial.json, judge.json}`
plus `RUN/run.json` (model, effort, budget, claude and lidx versions, task-file hashes).
Judge defaults: `--model claude-opus-5-5 --effort high` (dry-run: Haiku). `--effort` is never passed to Haiku models.

**Cost.** Every trial is a full agent session and every answer is a judge call.
The dry run is roughly $0.50 on Haiku. A full run is tasks x profiles x trials
agent sessions (30 tasks x 2 x 3 = 180) plus as many judge calls on the judge
model, so budget accordingly; `--budget` caps each trial in USD (a trial that
exceeds it is recorded as an error). Nothing runs without `--model`.

## Tasks

* `tasks/swe-qa.json` + `references/swe-qa.json`: 20 SWE-QA questions on public
  repos (what/where/how/why), pinned commits. From zvec-grep, see attribution.
* `tasks/derived.json` + `references/derived.json`: 10 tasks derived from the
  public `answers` suites (eshop, hono, httpx): "list every call site",
  "which methods are directly affected", "what does X reach within N calls". References are the
  suite `expect` lists, verified against source (rendered as `file:line — source line`);
  they were never produced by running lidx.
* Private tasks: `tasks/private/*.json` and `references/private/*.json`
  (gitignored). Use `"repo": {"path": "$MY_REPO"}`; an unset variable skips the
  task with a message. Never commit private-repo content.

Task format: `{"name", "tasks": [{"id", "category", "repo": {"url","commit"} | {"path"}, "question"}]}`;
reference format: `{"name", "references": {"<task id>": "<text>"}}`.

## Protocol

Identical between profiles: prompt (the question plus one fixed instruction, no
tool hints; lidx's own MCP instructions are its usage guidance), model, effort,
budget, timeout, base tools (`--tools Read,Grep,Glob`), repo commit, and
isolation flags (`--setting-sources ""`, `--strict-mcp-config`,
`--no-session-persistence`, `--disable-slash-commands`). The only difference is
the MCP config and the allow-list entry for the lidx tool. Permission mode is
the default (never bypass); tools stay read-only.

Isolation:
* Each trial gets a fresh `git clone` of a per-commit cache in its own directory,
  used as cwd, so there is no shared Claude memory (keyed by cwd), no session
  persistence, and no user hooks, plugins or CLAUDE.md.
* Clones and the lidx db live in a per-trial scratch dir outside the run dir
  (`--scratch`, default `<tmp>/lidx-agent-bench/<run name>/<task>/<profile>/<trial>`,
  removed after each trial), never inside the clone, so `Grep` cannot see the db
  and the agent sees no sibling results.
* References live in this repo and results in the run dir (default `.work/`, also in this
  repo), which are readable by absolute path. Every trial therefore passes
  `--disallowedTools "Read(//<lidx repo root>/**)" "Read(//<run dir>/**)"` (`//` = absolute
  path; Read rules also govern Grep and Glob). Verified with a Haiku probe: Read, Grep and
  Glob on `references/` are denied, a read inside the clone works. Any `permission_denials`
  are recorded in `trial.json` and reported.
* The `system/init` event (tools, mcp servers) is stored in `trial.json`. A
  trial fails if the tool list is not exactly Glob, Grep, Read (+ `mcp__lidx__lidx` for lidx),
  if the lidx profile's server is not `connected`, or if the baseline has any MCP server.

Index timing: before each lidx trial `lidx reindex` builds a fresh db; its time
is `index_seconds`, reported separately and **not** part of agent wall time.
With `-j > 1`, trials share the machine, so timings are noisier.

Judge blinding: a separate `claude -p` call (no tools, no settings, no MCP, temp
cwd) sees only question, reference and candidate answer, never the profile or
tool usage; the candidate is passed unaltered. Rubric (from zvec-grep): five
integer dimensions 1-20 (correctness, completeness, relevance, clarity,
coherence), total 5-100, requested through `--json-schema`; ints are validated
strictly and invalid output is retried up to 3 times. Judge cost is tracked separately.

## Metrics

* **Judge**: sum of the five dimensions, 5-100.
* **Input tokens**: `input_tokens + cache_read + cache_creation` from the final
  `result` event (cumulative over the session); components are kept in `trial.json`.
* **Tool calls**: `tool_use` blocks in assistant events (per-tool counts and lidx calls kept).
* **Wall time**: seconds measured around the `claude` process (excludes indexing; `duration_ms` is kept too).
* A trial whose result subtype is not `success` (e.g. budget exceeded), a failed
  isolation check, or an empty answer is an **error**: kept in raw output,
  excluded from means, listed in the report.
* **Errors**: the report shows errored-trial counts per profile (baseline/lidx) per task and
  overall; a task or the overall row is flagged ⚠ when the two profiles' error counts differ,
  since excluding errors from means can bias the comparison.
* **Per task**: mean over successful trials per profile, `±` stdev when trials > 1,
  change = point difference for judge and relative % for the rest. A warning flag
  appears when trials disagree (judge range across trials > 20 points, or tool-call max > 2x min).
* **Overall** (zvec-grep convention): mean of per-task judge means; sums of
  per-task means for tokens, calls and time, with change computed from those
  aggregates. Only tasks where both profiles have a value count. The `±` on the Overall row is
  the stdev across trial index i of the overall aggregate computed from trial i alone (per task,
  trial i's judge score or metric; then mean judge / sum of metric across tasks). It needs trials
  >= 2 and every paired task to have a successful trial i; otherwise it is omitted.
* **Cost**: agent + judge total.

## Attribution

The SWE-QA task subset, references and judge rubric come from
[zvec-ai/zvec-grep](https://github.com/zvec-ai/zvec-grep) (Apache-2.0), which
selected them from peng-weihan/SWE-QA-Bench. See `NOTICE`.

## Results

`results/` keeps the report of each full run.

**2026-10-06, Opus 5.5 high, 30 tasks × 3 trials, lidx 0.7.0** ([report](results/2026-10-06-opus55-high.md)):

| | baseline | lidx | change |
|---|---|---|---|
| Judge (5–100) | 81.2 | 81.7 | +0.5 |
| Input tokens | 2.49M | 4.23M | +70% |
| Tool calls | 257 | 242 | −6% |
| Wall time | 1160 s | 1142 s | −2% |
| Agent cost | $18.91 | $20.29 | +7% |

The agent called lidx in only 16 of 90 lidx trials: 32 of 725 tool calls, all
`outline`, `read_symbol`, `search` and `trace_flow`. Almost all of the extra
input tokens come from lidx's tool definition and server instructions, about
9.2k tokens added to the prompt prefix and re-read on every turn (first-turn
prefix 14.8k vs 5.5k). They are cache reads, so the cost difference stays
small. The judge difference is within trial noise.
