# lidx oracle benchmark: call graph graded by compilers

lidx's own benchmarks (`benchmarks/answers`) score against answer keys we wrote. This one scores lidx's
call graph against the answer a **compiler** gives for the same checkout of a public repository, so the
number cannot be tuned by editing a key:

- **precision**: of the call edges lidx emits (with a resolved in-repo target), how many does the compiler confirm;
- **recall**: of the compiler's call edges, how many does lidx have.

Every figure carries a 95% Wilson interval and the count it is computed over. The code here is our own;
nothing was copied from repowise-bench (which has no licence); its cell definitions (commits, scope, which
edges the denominator holds) were read for method only. repowise's *published numbers* are quoted below.

## Run it

```bash
cargo build --release                       # the lidx binary under test
python3 benchmarks/oracle/run.py            # all 7 repositories, 12 cells, writes benchmarks/oracle/results/
python3 benchmarks/oracle/run.py --only '^ts/'        # a subset (regex over cell ids)
python3 benchmarks/oracle/run.py --binary /path/to/lidx --cache /big/disk/lidx-oracle
python3 benchmarks/oracle/run.py --report-only        # rebuild report.md from results/
python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'
```

Needs `go` (1.24+; the right toolchain for each corpus module is fetched by Go itself), `node` + `npm`,
`rust-analyzer` with the `scip` subcommand (1.94 used), `git` and Python 3 (stdlib only). The first run
clones the corpus into the cache dir (default `~/.cache/lidx-oracle`, outside the repository) and runs
`npm ci --ignore-scripts` once inside `ts_oracle/`. Every run indexes each repo into a fresh database. A full run
takes about 10 minutes, dominated by Go's SSA build of syft, which needs roughly 9 GB of memory with tests included.
`cells.json` records the toolchain versions (rust-analyzer, node, typescript, Go) and the lidx build.

Outputs in `results/`: `report.md`, `cells.json` (all numbers), `lists/<cell>.{fn,fp,capture_gap,ungradable}.json`
(every miss, wrong edge, capture-gap edge and excluded edge with source locations and a category, for fixing;
`<cell>.dispatch_fn.json` too, except the very large `go__syft__tests` one), `spotcheck.json` / `spotcheck-sample.json`.

## Corpus

Pinned in `corpus.lock.json` to the **commits repowise-bench pins for the same cells** (`sha` is always the commit sha,
never an annotated-tag object, and `run.py` compares commit shas, so a cached clone is reused). Never committed here.

| Language | Repositories | Compiler | Cells |
|---|---|---|---|
| Go | cobra, gitleaks, syft | `golang.org/x/tools/go/callgraph/rta` over the type-checked program | with and without test files |
| TypeScript | zod (`packages/zod/src`), hono (`src`) | `typescript` 5.9.3 checker API | production code in the scoped directory |
| Rust | ripgrep 15.2.0, fd v10.5.0 | rust-analyzer 1.94 `scip` | `path_tests_excluded` (the repowise-comparable cell), `no_tests` (stricter, diagnostic) and `tests` |
| Rust, held-out | bat v0.25.0, tokei v12.1.2, xsv 0.13.0, lidx @ ccc405a | rust-analyzer 1.94 `scip` | `path_tests_excluded` only (`"held_out"` in the lock): not used while developing the Rust extractor, they check that a change tuned on ripgrep and fd generalises; no repowise figure |

Corpus repositories are untrusted: nothing from them is built, tested or installed. Go is only type-checked (`go list`
may download modules); TypeScript uses the compiler API without installing the repository's dependencies (calls into
packages that are not in the repository are out of scope); rust-analyzer runs with build scripts and proc-macro servers
disabled.

## Method

**Oracle edges** are distinct (caller declaration, callee declaration) pairs inside the repository, keyed by file and the
line of the declaration's *name identifier*, so decorators, attributes and doc comments above it do not matter.

- *Go* (`go_oracle/`): `packages.Load` + SSA + RTA. Roots: `main`/`init` of main packages; with tests also every
  `Test*`/`Benchmark*`/`Fuzz*`/`Example*`; a library with no `main` package (cobra without tests) uses its exported
  surface. Closures resolve to their enclosing declaration, **including closures that are callees**. repowise
  keys a closure callee at the function literal instead, which no graph has a symbol for; in every Go cell here 0 closure
  edges are matched (307 oracle edges in cobra, 12 in gitleaks, 1,101 in syft have a closure callee), so the choice does
  not change any numerator; generic instances to their origin; synthetic wrappers are collapsed;
  range-over-func bodies belong to the enclosing function. Only functions RTA proved reachable can be judged as callers.
- *TypeScript* (`ts_oracle/`): every call, `new`, JSX component element and tagged template, resolved with
  `getResolvedSignature`; `--subdir` restricts the analysed files to the cell's directory (callees outside it are out of
  scope on both sides). A call whose symbol is a const or field *with an initialiser* targets that variable, whatever its
  type annotation says; `new X()` targets the class X, not an inherited constructor. Module-level code is a `<module>`
  caller (line 0). tsc sees every call in an analysed file, so every caller in those files is judgeable.
- *Rust* (`rust_oracle/`): rust-analyzer SCIP decoded with a small protobuf reader. A reference to a function symbol is a
  call when followed by `(`; tuple-struct constructors count; enum-variant constructors are sites that call something
  that is not a declaration lidx models. Two ways to leave tests out: `--path-tests-excluded` is repowise's rule, files
  under a `tests/` or `test/` directory (their `is_test_file`), nothing inside a file, so `#[cfg(test)]` code and helper
  modules such as `crates/searcher/src/testutil.rs` stay in; `--no-tests` is ours and also drops `tests.rs` files, the
  single item under each `#[cfg(test)]` and files of modules gated in their parent (`#[cfg(test)] mod testutil;`).

**Dispatch edges** are calls through a path the compiler resolves only to a *possible* target: Go interface calls and
calls of function values; TypeScript interface and type-literal members and abstract methods (a const or field with one
initialiser is a static call); Rust methods declared on a trait (`Trait#method()`, not `impl#[T][Trait]method()`),
which also carry the trait's in-repo implementations as acceptable targets. They stay in the comparable recall.

**lidx side** (`match.py`): CALLS edges whose source and target are in-repo symbols, deduplicated to (caller, callee)
pairs, keyed the same way (name line recovered from source; `module` symbols are line 0 in every language). A lidx edge
matches an oracle edge when both ends land on the same declaration; declarations are compared by (file, qualname) so a
merged interface and const or a set of overloads are one declaration.

**What recall means.** The head-to-head recall is `(matched + dispatch matched) / all in-scope oracle edges`, with
capture gaps and unindexed callees charged to lidx: the denominator repowise uses. The report also shows static-only
recall, recall including dispatch, and recall over edges whose both ends lidx indexes, as diagnostics.

**Exclusions, each a named bucket in the report and sampled by hand:**

| Bucket | Definition |
|---|---|
| callee outside the repo / scope | the oracle cannot judge; excluded on both sides |
| capture gap | oracle edge whose caller or callee is not a symbol lidx indexes (macro-generated Rust tests, interface members, nested functions); kept in the comparable-recall denominator |
| dispatch | see above; in the comparable recall, out of the static recall |
| ungradable lidx edge | caller file or function not in the analysed program, callee file not analysed, or an unresolved call site |

Two rules keep exclusions from flattering lidx. A production-code edge into a test file, and an edge to an in-repo symbol at
a site the oracle knows calls code outside the repository, are **wrong edges**, not exclusions. An unresolved call
(`oracle_unresolved_site`: tsc could not resolve the call, e.g. a package that is not installed) withholds only a lidx edge
whose callee has the **same name** as that call, not every edge on the line; the report prints, per cell, how many pairs this
withheld and what precision would be if they all counted as wrong.

**Rust denominator.** repowise excludes Rust test code by path only (`tests/` and `test/` directories; `#[cfg(test)]` modules
inside production files stay in) and builds its denominator from the SCIP index of the remaining files. The
`path_tests_excluded` cell applies that rule, and it analyses the same 83 ripgrep files as their cell. The edge counts then
land within a few percent of theirs: ripgrep 4,796 compiler edges here against 4,555 in repowise-bench (+5.3%), fd 257
against 231 (+11%, 22 files against their 21). The remaining difference is what their parser captured versus what we read
from every SCIP reference followed by `(`. Our stricter `no_tests` cell (2,315 ripgrep edges) removes `#[cfg(test)]` code as
well, which is why its denominator is about half of theirs; it is a diagnostic row, never compared.

**TypeScript scope.** repowise's `tsc` oracle (1) emits no call made outside a function and (2) declines to judge calls
resolved to abstract targets (interface or type-literal members, abstract methods, function-typed fields: anything without
a body), withholding the calling function too. The head-to-head TypeScript rows apply the same two rules
(`match.repowise_ts_scope`): module-level callers and dispatch edges leave the recall denominator, and lidx edges from the
withheld callers are not judged for precision. Capture gaps (nested functions and other callees lidx has no symbol for)
stay in the denominator, as they do for repowise. Two differences remain, both making our grading stricter, not looser:
repowise judges only outermost named functions as callers (a lidx edge from a class, constant or nested symbol is outside
its oracle, ours is graded) and withholds every function containing an unresolved call (ours withholds only an edge whose
callee has the unresolved call's name). Their tsconfig is null (defaults); ours reads the repository's own. The stricter,
all-edges grading stays in the report as a diagnostic row.

**Classification** of every miss and wrong edge is in `results/lists/`. FP: `external_callee_at_site`, `wrong_target`,
`reference_as_call` (the name appears but is not called), `calls_test_code`, `no_call_at_site`, `caller_attribution`. FN:
`no_edge_at_site`, `other_call_on_line_missed`, `wrong_target`, `marked_external`, `recorded_as_uses`,
`attributed_to_other_caller`, `unresolved`.

**Hand spot-check** (`spotcheck.py sample`, then `merge`; verdicts and a note per item in `spotcheck-verdicts.json`): 20
random FPs and 20 random FNs per language **plus up to 10 random items from every exclusion bucket per language**
(dispatch, each ungradable reason, capture gaps), read against the source. Items are classified `lidx_error`,
`oracle_artefact` or `matching_artefact` (FP/FN), or `correct_exclusion` / `misbucketed` (buckets). The rules above were
changed until the artefact and mis-bucketing rates were under 10%; the history of that iteration, which found real
problems in the first version, is in the report. The Rust sample is drawn from the `path_tests_excluded` cells that are compared with repowise; re-sampling it found a caller-keying defect in the SCIP reader (`build.rs` and `crates/core/main.rs` both define `main()`), now fixed.

## Results next to repowise

Ours: lidx 0.7.0 on main (`results/report.md` has the diagnostic columns, resolution-kind tables, categories and the
spot-check). repowise: the numbers they publish in `docs/BENCHMARKS.md` on repowise main (2026-10-03) ("Call graph against a
compiler, five tools" for Go and TypeScript, "Accuracy by language" for Rust). The repowise intervals are Wilson intervals we
computed from their published point estimate and the edge counts of the same cells in repowise-bench (an earlier run of the
same cells), not intervals they published. **ahead / tied / behind compares the two 95% intervals, never the point
estimates.** Recall is the comparable recall. Both sides at the same commits and the same scope: the TypeScript rows use
repowise's `tsc` scope (no module-level callers, no abstract targets) and the Rust rows its path-only test rule; our stricter
gradings of the same repositories (TypeScript with every edge, Rust without `#[cfg(test)]` code) are diagnostic rows in
`results/report.md` and are never given a verdict.

| Cell | lidx P [95% CI] | repowise P [95% CI] | P | lidx R [95% CI] | repowise R [95% CI] | R | oracle edges (lidx / repowise) |
|---|---|---|---|---|---|---|---|
| go/cobra/tests | 0.933 [0.918, 0.946] | 0.995 [0.990, 0.998] | **behind** | 0.589 [0.567, 0.611] | 0.705 [0.685, 0.724] | **behind** | 1923 / 2059 |
| go/gitleaks/no_tests | 0.984 [0.973, 0.991] | 0.991 [0.985, 0.995] | **tied** | 0.477 [0.452, 0.501] | 0.971 [0.961, 0.978] | **behind** | 1584 / 1584 |
| go/gitleaks/tests | 0.985 [0.974, 0.992] | 0.989 [0.983, 0.993] | **tied** | 0.456 [0.433, 0.480] | 0.939 [0.927, 0.949] | **behind** | 1747 / 1747 |
| go/syft/no_tests | 0.877 [0.866, 0.888] | 0.979 [0.974, 0.983] | **behind** | 0.367 [0.356, 0.377] | 0.539 [0.528, 0.550] | **behind** | 7977 / 7898 |
| go/syft/tests | 0.880 [0.871, 0.889] | 0.982 [0.979, 0.985] | **behind** | 0.162 [0.158, 0.167] | 0.341 [0.335, 0.347] | **behind** | 25902 / 22590 |
| ts/zod | 0.971 [0.961, 0.979] | 0.995 [0.989, 0.998] | **behind** | 0.791 [0.770, 0.810] | 0.796 [0.773, 0.817] | **tied** | 1626 / 1269 |
| ts/hono | 0.666 [0.636, 0.695] | 0.976 [0.958, 0.986] | **behind** | 0.793 [0.765, 0.820] | 0.806 [0.775, 0.833] | **tied** | 833 / 706 |
| rust/ripgrep/path_tests_excluded | 0.878 [0.867, 0.889] | 0.852 [0.838, 0.865] | **ahead** | 0.645 [0.632, 0.659] | 0.532 [0.517, 0.546] | **ahead** | 4796 / 4555 |
| rust/fd/path_tests_excluded | 0.890 [0.847, 0.922] | 0.899 [0.838, 0.939] | **tied** | 0.914 [0.874, 0.943] | 0.619 [0.555, 0.679] | **ahead** | 257 / 231 |

Not published by repowise, so not compared: cobra without tests, the Rust cells other than `path_tests_excluded`, TypeScript with tests.
