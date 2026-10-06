# lidx answer benchmark

Scores a lidx binary on developer questions ("who calls X?", "is Y dead?",
"does a trace from A reach B?") whose correct answers were **derived from source
code, never from lidx**. Per question it reports precision and recall; there is
no LLM in scoring. Use it to compare builds (`main` vs a branch) on the same
questions.

**Rule: answers come from reading source (grep + reading the code). Never run
lidx to decide an answer, or the benchmark just mirrors lidx's current behaviour.**

## Running

```bash
cd benchmarks/answers
python3 run.py --binary /path/to/lidx --suite suites/selftest.json            # smoke test, ~1s
python3 run.py --binary /path/to/lidx --suite suites/eshop.json --json out-new.json
python3 run.py --binary /path/to/old-lidx --suite suites/eshop.json --json out-old.json
python3 run.py --compare out-old.json out-new.json
```

Options: `--suite` (repeatable), `--workdir DIR` (clones and fresh indexes;
default `benchmarks/answers/.work`, gitignored), `--json FILE`, `--only REGEX`
(question ids). Python 3 stdlib only. Exit code 2 if any question errored.

Public suites clone the repo at a pinned commit into the workdir (cached by
commit); the only network use is `git clone`/`fetch`. Every run indexes into a
**fresh** db (`lidx reindex --repo R --db DB`), so results never depend on stale
state.

**Private suites** live in `private/` (gitignored; lidx is a public repo, so
never commit private-repo content). Their `repo.path` uses an env var:

```bash
MY_REPO=/path/to/checkout python3 run.py --binary BIN --suite private/my-suite.json
```

An unset variable skips that suite with a message rather than failing.

## Suite format

```json
{ "name": "eshop",
  "repo": {"url": "https://github.com/dotnet/eShop", "commit": "<full sha>"},
  "questions": [ ... ] }
```

`repo` is `{url, commit}` or `{path}` (env vars expanded; a relative path is
relative to the suite file). Everything is identified by **source location**, not
by lidx qualname: `loc = {"file": "repo/relative.cs", "line": 42}`, 1-based. For
a declaration it is the line of the declaration's *name token* (not an attribute
or decorator line above it); for a call site it is the line where the called name
appears. `note` is a free-text field allowed anywhere.

| kind | fields | meaning |
|---|---|---|
| `callers` | `target`, `expect[]` | every real call site (file, line) of the target, tests included |
| `impact_direct` | `target`, `expect[]` | the declaration locs of the methods enclosing those call sites |
| `dead` | `scope{path,lang}`, `live[]`, `dead[]` | declarations that must NOT / SHOULD be reported dead |
| `trace_reaches` | `start`, `max_hops`, `must_reach[]`, `must_not_reach[]` | declarations a downstream trace must / must not reach |

Common fields: `id` (unique), `lang`, `kind`, `tags[]` (e.g. `overload`,
`extension`, `interface`), `note`.

## Scoring rules

* **callers**: compare sets of call-site `(file, line)`. Precision = correct /
  returned, recall = correct / expected. Micro-averaged across questions
  (totals sum counts, they are not averages of per-question ratios).
* **impact_direct**: same, on the set of enclosing-method declaration locs.
* **dead**: *false-alarm rate* = `live` entries reported dead / `live` entries
  (lower is better); *detection rate* = `dead` entries reported / `dead` entries.
  Only function/method/class/struct results are considered, filtered to `scope`.
* **trace_reaches**: *reach recall* = `must_reach` locs found among symbols
  within `max_hops` downstream; *wrong-reach rate* = `must_not_reach` locs
  found (lower is better). The start symbol itself is excluded.
* **target_not_indexed**: if the question's target/start `loc` maps to no lidx
  symbol the question scores zero recall (expected items still count in the
  denominator) and is reported separately.
* **errors**: an RPC failure, an overload picker, or a **truncated** response
  (e.g. `callers_total` greater than the callers returned, a capped
  `dead_symbols`, an un-pageable `trace_flow`) fails the question loudly. It is
  scored as zero and the run exits 2; a partial answer is never scored as a real one.

### Authoring rules that affect scoring

* **Interface dispatch rule.** A call through an interface or virtual/base
  member counts as a caller of the **interface/base declaration only**, not of
  its implementations. Do not list it under an implementation's `expect`.
* **Framework entry points are excluded from `dead` lists** (put them in neither
  `live` nor `dead`): DI-registered services, reflection-loaded types, ASP.NET
  controllers/endpoints, test methods, `Main`/`main`, handlers registered by
  attribute or convention, serialization hooks.
* Method-group references and implicit calls (`new X()` constructors,
  `Deconstruct`) count as calls of that declaration when the language makes them so.
* Expect lists must be **complete**; otherwise recall means nothing.

## How answers are read from lidx

* loc to symbol: the symbol in lidx's db whose declaration name-token line (or
  start line) equals the loc. Opened read-only.
* `callers`: RPC `explain_symbol` `{id, sections:["callers"], max_refs: huge}`
  decides which callers exist; no RPC method exposes the call-site line, so the
  lines come from the db `edges` table (`evidence_start_line` joined to `files`).
  This is the one db fallback.
* `dead`: RPC `dead_symbols` with `path`/`languages` scope and a huge `limit`.
* `trace_reaches`: RPC `trace_flow` downstream, paging with `trace_offset`
  until the continuation is empty; edge kinds default to CALLS, RPC_*, HTTP_*,
  CHANNEL_* (override per question with `"kinds"`).

## Adding questions

1. Pick a symbol someone would plausibly change. Mix easy and hard: overloads,
   extension methods, generics, virtual dispatch, DI-resolved locals, method
   groups, chained calls, constructors, aliased imports/re-exports, same-named
   symbols in other modules (traps), tests calling the code.
2. Enumerate from source: grep every textual occurrence of the name, then decide
   each by reading the code (types, overloads, imports, shadowing). Record how in
   `note` (e.g. `grep -n 'Parse(' -> 7 hits, 2 are Guid.Parse`).
3. Write locs; check each line number against the pinned commit.
4. Run `--binary` on it. A missing item is either a lidx gap or a mistake in the
   suite; re-read the source before blaming lidx, and never edit the answer to
   match lidx output.

`suites/selftest.json` over `fixtures/selftest/` (hand-written C#/Python/TS) is
the harness smoke test; every kind plus a `target_not_indexed` case is covered.
Targets: 15-25 questions per public suite, 25-40 for private ones.
