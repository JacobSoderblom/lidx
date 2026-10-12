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
python3 benchmarks/oracle/run.py            # all 19 repositories, 30 cells, writes benchmarks/oracle/results/
python3 benchmarks/oracle/run.py --only '^ts/'        # a subset (regex over cell ids)
python3 benchmarks/oracle/run.py --binary /path/to/lidx --cache /big/disk/lidx-oracle
python3 benchmarks/oracle/run.py --report-only        # rebuild report.md from results/
python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'
```

Needs `go` (1.24+; the right toolchain for each corpus module is fetched by Go itself), `node` + `npm`,
`rust-analyzer` with the `scip` subcommand (1.94 used), `git` and Python 3 (stdlib only). The first run
clones the corpus into the cache dir (default `~/.cache/lidx-oracle`, outside the repository) and runs
`npm ci --ignore-scripts` once inside `ts_oracle/` (the oracle's own pinned dependencies; the corpus is never installed). Every run indexes each repo into a fresh database. A full run
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
| TypeScript, held-out | trpc (`packages/server/src` @ 10e2ef98), ky (`source` @ 35418888), immer (`src` @ 8848a5b1), h3 (`src` @ 66cfb8a3) | `typescript` 5.9.3 checker API | same cell as zod and hono (repowise's `tsc` scope in the comparable grading); `"held_out"` in the lock, no repowise figure |
| Go, held-out | chi v5.4.0, urfave/cli v2.27.7, viper v1.21.0, bubbletea v1.3.10 | RTA | with and without test files; `"held_out"` in the lock, no repowise figure |

### Nothing from the corpus runs

Corpus repositories are untrusted. The oracles read sources; none of them builds, tests, generates or installs the
corpus, and `safety.py` fails the run if one did:

- **Rust.** `rust-analyzer scip` ignores `cargo.buildScripts.enable=false` and `procMacro.enable=false` (those are LSP-only):
  it always runs `cargo check --workspace` for build-script output (which also compiles and runs build scripts and loads
  proc-macro crates into rustc) and starts the sysroot proc-macro server. Verified on a crate whose `build.rs` and proc macro
  write marker files: with those two settings both markers appear. What it does honour, and what `run.py` writes to
  `<cache>/scip/ra-config.json`: `cargo.buildScripts.overrideCommand = ["true"]` (that command replaces `cargo check`, prints no
  build-script output, so no build script runs and no proc-macro dylib is known) and `cargo.sysroot` pointing at a stub sysroot
  that holds only `lib` (no `libexec/rust-analyzer-proc-macro-srv` to start). `cargo metadata` still runs; it reads manifests only.
  A corpus `.cargo/config.toml` can also name a `build.rustc-wrapper`, `build.rustc-workspace-wrapper` or `build.rustc`, which
  cargo runs for every rustc call (`cargo metadata` probes `rustc -vV`). `safety.hardened_env` sets both wrapper variables
  empty (environment beats config files), pins `RUSTC` and `RUSTUP_TOOLCHAIN`, and `test_safety.py` puts such a wrapper in
  the marker crate. The cost of the sandbox: crates whose items come from build-script output lose them in the oracle; tokei's
  `LanguageType` is generated by `build.rs`, so its held-out precision is 0.77 here against 0.83 before (lidx did not change:
  bisected on the merges of #427, #430, #435 and #433, see the held-out section of `results/report.md`).
- **Go.** `packages.Load` runs `go list`, which never runs `go generate`. It gets `CGO_ENABLED=0` (no cgo tool, no C compiler),
  `GOFLAGS=-mod=readonly` (replaces any inherited flags such as `-toolexec`; go.mod and go.sum are never rewritten),
  `GOWORK=off` and `GOPACKAGESDRIVER=off`. Go may download module sources and the toolchain the module asks for; downloading is not executing.
- **TypeScript.** Only the compiler API parses the sources. The corpus is not installed (calls into packages that are not in the
  repository are out of scope); the oracle's own dependencies are installed with `npm ci --ignore-scripts`.

Checks after every oracle run (`safety.py`, unit-tested in `test_safety.py`, which also runs a real `rust-analyzer scip` on a crate
with a marker-writing build script and proc macro): the Rust target dir is emptied before the run and the run fails if any
`build/*/out` directory, `build-script-*` executable or dynamic library in `deps/` exists afterwards, or if sampling the
rust-analyzer process tree saw a proc-macro server, a build-script executable or a compiling `cargo` subcommand; every oracle
fails the run if the corpus checkout is not byte-for-byte the pinned commit afterwards (`git status --porcelain --ignored`
is empty), and a cached checkout that is not is re-cloned.

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
  type annotation says; `new X()` and `super(..)` target the class X, not an inherited constructor. Module-level code is a `<module>`
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

**Two Go scorings.** The head-to-head with repowise (`comparable` in `cells.json`, the verdict and held-out tables) uses
repowise's rules: `calls` edges keyed (caller declaration, callee declaration), a dynamic call keyed at the concrete implementation
RTA names, dispatch edges never expanded (`match.repowise_go_scope`). A lidx edge to `Iface.Method` has no oracle edge to match
there, so it is a wrong edge, and it covers no implementation. The **dispatch-credited, lidx-native** scoring (`calls`, its own
labelled table in the report, no verdict) is described next; it measures what lidx's graph can answer through its dispatch
machinery and is not comparable to repowise.

**Go interface edges (dispatch-credited, lidx-native).** RTA keys a call through an interface at the concrete implementations; lidx binds it to the interface
method (`Doer.Do`) and fans out to implementations through its dispatch machinery. The Go oracle therefore also records, per
edge, the interface methods its dynamic call sites invoke (`callee_alts`, `file:line` of the method name). Grading a lidx edge
to an interface method: it is a **true edge** when the caller has such a site invoking exactly that method, and a **wrong edge**
otherwise (the call exists in no form the compiler knows). It **covers** an oracle dispatch edge to an implementation only when
lidx's own graph links that implementation to the interface method (`match.go_dispatch_pairs`: lidx's `IMPLEMENTS` edges plus
same-named methods), so an implementation lidx cannot reach stays a missed edge and the recall cannot be flattered by the
mapping. Unit-tested in `test_match.py` (`GoInterfaceDispatch`).

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

Ours: lidx 0.7.0 on main (origin/main 60c50f0, release build) (`results/report.md` has the diagnostic columns, resolution-kind tables, categories and the
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
| go/cobra/tests | 0.946 [0.933, 0.956] | 0.995 [0.990, 0.998] | **behind** | 0.796 [0.777, 0.813] | 0.705 [0.685, 0.724] | **ahead** | 1923 / 2059 |
| go/gitleaks/no_tests | 0.980 [0.971, 0.986] | 0.991 [0.985, 0.995] | **tied** | 0.922 [0.907, 0.934] | 0.971 [0.961, 0.978] | **behind** | 1584 / 1584 |
| go/gitleaks/tests | 0.978 [0.969, 0.984] | 0.989 [0.983, 0.993] | **tied** | 0.895 [0.879, 0.908] | 0.939 [0.927, 0.949] | **behind** | 1747 / 1747 |
| go/syft/no_tests | 0.968 [0.963, 0.973] | 0.979 [0.974, 0.983] | **behind** | 0.514 [0.503, 0.525] | 0.539 [0.528, 0.550] | **behind** | 7977 / 7898 |
| go/syft/tests | 0.973 [0.969, 0.977] | 0.982 [0.979, 0.985] | **behind** | 0.282 [0.277, 0.288] | 0.341 [0.335, 0.347] | **behind** | 25902 / 22590 |
| ts/zod | 0.999 [0.996, 1.000] | 0.995 [0.989, 0.998] | **tied** | 0.916 [0.901, 0.928] | 0.796 [0.773, 0.817] | **ahead** | 1626 / 1269 |
| ts/hono | 0.996 [0.987, 0.999] | 0.976 [0.958, 0.986] | **ahead** | 0.845 [0.819, 0.868] | 0.806 [0.775, 0.833] | **tied** | 833 / 706 |
| rust/ripgrep/path_tests_excluded | 0.974 [0.969, 0.979] | 0.852 [0.838, 0.865] | **ahead** | 0.835 [0.824, 0.845] | 0.532 [0.517, 0.546] | **ahead** | 4796 / 4555 |
| rust/fd/path_tests_excluded | 0.977 [0.950, 0.989] | 0.899 [0.838, 0.939] | **ahead** | 0.973 [0.945, 0.987] | 0.619 [0.555, 0.679] | **ahead** | 256 / 231 |

Not published by repowise, so not compared: cobra without tests, the Rust cells other than `path_tests_excluded`, TypeScript with tests, and the held-out cells (`results/report.md`).
