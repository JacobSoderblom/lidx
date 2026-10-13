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
python3 benchmarks/oracle/run.py            # all 21 repositories, 40 cells, writes benchmarks/oracle/results/
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
(every miss, wrong edge, capture-gap edge and excluded edge with source locations and a category, for fixing; Go cells also `<cell>.closure_self.json`, the oracle self-edges left out of the lidx-native grading;
`<cell>.dispatch_fn.json` too, except the very large `go__syft__tests` one), `spotcheck.json` / `spotcheck-sample.json`.

## Corpus

Pinned in `corpus.lock.json` to the **commits repowise-bench pins for the same cells** (`sha` is always the commit sha,
never an annotated-tag object, and `run.py` compares commit shas, so a cached clone is reused). Never committed here.

| Language | Repositories | Compiler | Cells |
|---|---|---|---|
| Go | cobra, gitleaks, syft | `golang.org/x/tools/go/callgraph/rta` over the type-checked program | with and without test files |
| TypeScript | zod (`packages/zod/src`), hono (`src`) | `typescript` 5.9.3 checker API | production code in the scoped directory |
| Rust | ripgrep 15.2.0, fd v10.5.0 | rust-analyzer 1.94 `scip` | `path_tests_excluded` (the repowise-comparable cell), `no_tests` (stricter, diagnostic), `tests`, and `path_tests_excluded_all_features` (all cargo features analysed, cfg-gated edges ungradable; diagnostic) |
| Rust, held-out | bat v0.25.0, tokei v12.1.2, xsv 0.13.0, lidx @ ccc405a, just 7f4ef81, nextest c8d5aac | rust-analyzer 1.94 `scip` | `path_tests_excluded` and its `_all_features` diagnostic twin (`"held_out"` in the lock): not used while developing the Rust extractor, they check that a change tuned on ripgrep and fd generalises; no repowise figure |
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
  the marker crate. `cargo.features = "all"` (the `_all_features` cells) is honoured too and changes only which cfgs rust-analyzer enables while reading the sources; `test_safety.py` runs the marker crate with a feature-gated module both ways, asserts the gated function is analysed only with the setting, and that no marker file, build-script executable or proc-macro library appears. rust-analyzer also asks `cargo rustc ... --print cfg|target-spec-json` for the target's cfg: that form prints and compiles nothing, so `safety.FORBIDDEN_PROCESS` allows `cargo rustc` only when `--print` is on its command line (the first complete run of this change tripped on it by sampling luck; any other `cargo rustc` is still refused, and the post-run artefact check is unchanged). The cost of the sandbox: crates whose items come from build-script output lose them in the oracle; tokei's
  `LanguageType` is generated by `build.rs`, so its held-out precision was 0.77 under the sandbox against 0.83 before (0.93 on current main) (lidx did not change:
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
  surface. Closures resolve to their enclosing declaration, **including closures that are callees**. A call site that starts a closure where it is written (`func(){...}()`, `go func(){...}()`, `defer func(){...}()`) inside F gives an edge F -> F flagged `closure_self` (see "Oracle artefacts"). repowise
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

**Two Go scorings.** lidx binds a call on an interface-typed receiver to the concrete method when it can (the receiver's dynamic
type is proven at the call site, or the repository has exactly one implementation): a `CALLS` edge, graded like any other against
the concrete callee RTA names. When it cannot, the edge goes to the interface method as a `DISPATCH` edge, a kind of its own that
`analyze_impact`, `trace_flow` and `explain_symbol` follow and fan out through `IMPLEMENTS`; it is **not a call** and the
repowise-rules grading never sees it.
- The head-to-head with repowise (`comparable` in `cells.json`, the verdict and held-out tables) uses repowise's rules: `calls`
  edges keyed (caller declaration, callee declaration), a dynamic call keyed at the concrete implementation RTA names, dispatch
  edges never expanded (`match.repowise_go_scope`); lidx's `CALLS` edges only. A `DISPATCH` edge is neither a true nor a wrong edge
  there, and the oracle edges it stands for stay missed.
- The **dispatch-credited, lidx-native** scoring (`calls`, its own labelled table in the report, no verdict) adds the `DISPATCH`
  edges, described next; it measures what lidx's graph can answer through its dispatch machinery and is not comparable to repowise.

**Go interface edges (dispatch-credited, lidx-native).** RTA keys a call through an interface at the concrete implementations; a
`DISPATCH` edge points at the interface method (`Doer.Do`). The Go oracle therefore also records, per
edge, the interface methods its dynamic call sites invoke (`callee_alts`, `file:line` of the method name). Grading a `DISPATCH` edge:
it is a **true edge** when the caller has such a site invoking exactly that method, and a **wrong edge**
otherwise (the call exists in no form the compiler knows). It **covers** an oracle dispatch edge to an implementation only when
lidx's own graph links that implementation to the interface method (`match.go_dispatch_pairs`: lidx's `IMPLEMENTS` edges plus
same-named methods), so an implementation lidx cannot reach stays a missed edge and the recall cannot be flattered by the
mapping. The oracle also lists every interface invocation of a reachable function (`iface_calls`), including those RTA found no
in-repo target behind. Unit-tested in `test_match.py` (`GoInterfaceDispatch`).

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

### Type hierarchy

The call-edge grading above says nothing about the edges that make dispatch work. A second grading, in its own section of
`results/report.md` ("Type hierarchy"), checks `EXTENDS`, `IMPLEMENTS` and the method-level pairing (implementing method ->
interface, trait or base method) against the same compilers. **The call-edge tables and their numbers are untouched**; this is a new
`hierarchy` object in each oracle's JSON and a new `hierarchy` key per cell in `cells.json`. Nothing in an oracle pair is derived from
lidx. Pairs are keyed `file:line` of the declaration's name identifier, like the call edges (a method by its own name token), and a
declaration is compared by (file, qualname) so merged interface declarations are one.

| Language | `implements` | `extends` | `embeds` (Go) | `overrides` |
|---|---|---|---|---|
| Go (`go_oracle/hierarchy.go`, `go/types`) | every in-repo named non-interface type `T` and in-repo interface `I` with at least one method (embedded methods included): `types.Implements(T, I)` or `types.Implements(*T, I)` (`ptr` marks the pointer-only case). Local types, aliases and empty or constraint-only interfaces are not candidates. | interface embedding: `Interface.EmbeddedType(i)` of an in-repo interface that is itself an in-repo named interface | struct embedding: `Struct.Field(i).Embedded()` whose type is an in-repo named type (pointer and generic instantiation collapse to the origin) | for each implements pair, `MethodSet(T or *T).Lookup(I.Method(j))`: the implementing method (possibly promoted from an embedded type; `promoted`) against the interface method, where both are declared in the repository |
| TypeScript (`ts_oracle/hierarchy.mjs`, checker) | the resolved `implements` heritage nodes of every class (declared clauses only) | `checker.getBaseTypes` of every class and interface | - | for every instance member of a class, `checker.getPropertyOfType(ancestorType, name)` over the class's `extends` and `implements` ancestors, transitively; the pair is (member, ancestor's member), `depth` and `via` say how far and through which clause |
| Rust (`rust_oracle/scip_hierarchy.py`, rust-analyzer SCIP) | `impl Trait for Type`: `Type` and `Trait` are the declarations of the reference occurrences on the impl header (first top-level occurrence before and after `for`), never found by name | supertraits: the top-level reference occurrences after the `:` of a `trait A: B + C` header | - | an item of the impl whose name the trait declares (`impl#[T][Trait]m()` against `Trait#m()`) |

Why the Rust oracle reads impl symbols: rust-analyzer's SCIP carries no implementation relationships (the `is_implementation` flag is
never set), so the impl symbols (`impl#[Type][Trait]item`) are the source. The bracket text is only a name, so the declarations come from
the occurrences on the header line(s). `where Self: X` bounds on a trait are not read as supertraits.

**Exclusions, each a named bucket in the report and sampled by hand:**

| Bucket | Definition |
|---|---|
| external base | the base is declared outside the repository (`impl Display for X`, `class E extends Error`, an embedded `sync.Mutex`, `interface R extends io.Reader`). Judged for **existence only**: the oracle pair is found when lidx has an EXTENDS / IMPLEMENTS row from the same declaration naming the same base (the last one or two name segments). No precision. Go has no `implements` clause, so its external bucket holds embedded external types only. |
| blanket impl (Rust) | `impl<T> Trait for T`: no in-repo type |
| foreign self type (Rust) | `impl Trait for Vec<Foo>`, `for &str`: the Self type is not declared in the repository |
| type-alias self type (Rust) | `impl Trait for Alias` |
| marker impl (Rust) | an impl with no item (`impl Marker for X {}`): rust-analyzer emits no symbol for it |
| generic declaration (Go) | a generic type or interface: `types.Implements` on an uninstantiated declaration compares unrelated type parameters, so lidx pairs touching it are ungradable |
| interface method outside the repository (Go) | an `overrides` pair whose interface method comes from an embedded stdlib interface |
| override base outside scope or external (TS) | the member's base is declared in a `.d.ts`, `lib`, or outside the cell's `--subdir` |
| ungradable lidx pair | an end in a file the oracle did not analyse, a declaration the oracle did not judge (not a candidate type or interface), or a generic Go declaration |

**lidx side** (`hierarchy.py`): its own `EXTENDS`/`IMPLEMENTS`/`INHERITS` rows with both ends in-repo symbols, relation chosen by kind
and by the source's symbol kind (Go: an EXTENDS row from an interface is `extends`, from anything else `embeds`; Rust: an IMPLEMENTS row from
a method is `overrides`, from a trait `extends`, otherwise `implements`). Go and TypeScript `overrides` read the pairs of lidx's dispatch
query, dumped by `lidx dispatch-pairs --repo R [--db D]` (a small subcommand that prints `Db::all_dispatch_pairs`, the rows behind `explain_symbol`, `analyze_impact` and
`dead_symbols`); the harness does not substitute a pairing of its own. A lidx pair is **correct** when the oracle has it, **wrong** when both of
its ends are declarations the oracle judged for that relation and it does not, **ungradable** otherwise. Recall = oracle pairs lidx has / all
oracle pairs, with pairs lidx has no symbol for kept in the denominator (`capture_gap`). TypeScript also reports `structural_implements`
(`checker.isTypeAssignableTo`, interfaces with at least one required member and at least one member the class has) as a diagnostic column: it
never changes the headline numbers.

FN categories: `no_edge`, `not_indexed` (a declaration of the pair has no lidx symbol), `other_kind_of_edge`, and for `overrides`
`type_edge_missing` (lidx lacks the type-level pair), `method_pairing_missing` (it has the type-level pair but the dispatch query does not pair the
methods) and `promoted_method` (Go: the method is promoted from an embedded type). FP categories: `reverse_direction`, `structural_only` (TS),
`other_relation_in_oracle`, `no_such_override`, `types_unrelated_in_compiler`, `other`.

## Oracle artefacts

The PR #445 spot-check found oracle artefacts concentrated in two places. Each change below removes an **oracle blind spot**, never a lidx
error, and each is stated against repowise's own harness (`graph/experiments/g4-oracle-anchored/oracle/main.go` and
`g9-language-oracles`, read for method only). The rule: a change that would make a head-to-head row less like-for-like with repowise's method lives in a
diagnostic row, not in the repowise-comparable column. **None of the three changes touches a head-to-head row**: every figure in the
verdict table below comes from the same oracle edges and the same lidx edges as before these changes (the ripgrep precision moved
0.985 to 0.987 because of resolver changes merged to main in between, #447).

1. **Go closure self-edges** (`closure_self` in `go_oracle/main.go`, `match.drop_closure_self`). An edge F -> F whose every call site is a
   function literal invoked on the spot inside F (`func(){...}()`, `go func(){...}()`, `defer func(){...}()`, decided from the syntax: the call's
   opening parenthesis follows the literal's closing brace, or the call is a `go`/`defer`) is not recursion; it exists only because closures are
   keyed at their enclosing declaration. A named function calling itself, and a closure bound to a variable and called later, are never flagged
   and stay graded. *repowise*: its caller side is keyed the same way (outermost declaration), and its callee side is left at the literal, so the
   same call is an `F -> literal` edge that no tool can match and that sits in the denominator of every tool, repowise's included. The blind spot is
   therefore shared, but removing the edge from our comparable recall would shrink a denominator repowise keeps, so the **repowise-rules column
   keeps these edges**; the lidx-native (dispatch-credited, diagnostic) grading leaves them out, and the report prints the repowise-rules recall with
   and without them side by side. Counts per cell are in the report (cobra tests 16, gitleaks 7 and 9, syft 33 and 60, plus the held-out cells).
2. **Rust features** (`safety.rust_config(all_features=True)`, `_all_features` cells). rust-analyzer analyses `cargo.features = "all"`, so
   calls behind `#[cfg(feature = "...")]` (ripgrep's `pcre2`) are in the compiler graph. *repowise*: runs `cargo check` then `rust-analyzer scip .`
   with default features, so it has the same blind spot (and lidx's edges there would be wrong edges in a repowise-style grading). Because the
   all-features oracle has more edges than repowise's, the cell is **diagnostic only**; `path_tests_excluded` keeps default features and
   stays the compared cell. If rust-analyzer cannot load a workspace with all features the cell falls back to default features and records
   `features_fallback` (reported per cell; none fell back in the committed run).
3. **Rust platform gates** (`rust_cfg.py`, `match.grade(cfg_gate=...)`). A lidx edge whose every evidence line lies inside an item behind a
   `#[cfg(...)]` that is *definitely false* on the analysed configuration (host `rustc --print cfg`: `windows`, `target_os`, `target_family`,
   `target_arch`, ..., `not(unix)`, `any`/`all`/`not` combinations, and `feature = "..."` when features are default) becomes ungradable
   (`cfg_gated_evidence`, its own bucket and list) instead of a false positive. The gate must be positively found in the source: an inner
   `#![cfg]` at the top of a file, or an outer `#[cfg]` on an item, statement, block, match arm or field whose extent is found. `test`,
   `debug_assertions`, unknown keys and anything unparsable are *unknown*, never false, and an item whose end cannot be found gates nothing; ungated code
   is never touched, a pair with even one evidence line outside a gate stays graded, and a matched edge stays a true edge. *repowise*: its Rust
   oracle has no gate detection, so lidx-style edges inside `cfg(windows)` code grade as wrong there too. Diagnostic only, applied to the `_all_features` cells.

`test_cfg_gate.py` unit-tests the predicate evaluator, the region finder (strings, comments, chars, attribute stacks, match arms, inner
attributes, unparsable extents), the ungradable bucket and the Go self-edge filter.

**Hand spot-check** (`spotcheck.py sample`, then `merge`; verdicts and a note per item in `spotcheck-verdicts.json`): 20
random FPs and 20 random FNs per language **plus up to 10 random items from every exclusion bucket per language**
(dispatch, each ungradable reason, capture gaps), read against the source. Items are classified `lidx_error`,
`oracle_artefact` or `matching_artefact` (FP/FN), or `correct_exclusion` / `misbucketed` (buckets). The rules above were
changed until the artefact and mis-bucketing rates were under 10%; the history of that iteration, which found real
problems in the first version, is in the report. The Rust sample is drawn from the `path_tests_excluded_all_features` diagnostic cells of ripgrep and fd (the grading with the two artefact removals; the repowise-comparable Rust cells keep both artefacts by design, so their artefact rate is the higher one measured in #445); re-sampling it found a caller-keying defect in the SCIP reader (`build.rs` and `crates/core/main.rs` both define `main()`), now fixed.

## Results next to repowise

Ours: lidx 0.7.0 on main (origin/main 65c6f57, release build) (`results/report.md` has the diagnostic columns, resolution-kind tables, categories and the
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
| go/cobra/tests | 1.000 [0.998, 1.000] | 0.995 [0.990, 0.998] | **tied** | 0.799 [0.780, 0.816] | 0.705 [0.685, 0.724] | **ahead** | 1923 / 2059 |
| go/gitleaks/no_tests | 1.000 [0.998, 1.000] | 0.991 [0.985, 0.995] | **ahead** | 0.982 [0.975, 0.988] | 0.971 [0.961, 0.978] | **tied** | 1584 / 1584 |
| go/gitleaks/tests | 1.000 [0.998, 1.000] | 0.989 [0.983, 0.993] | **ahead** | 0.957 [0.946, 0.965] | 0.939 [0.927, 0.949] | **tied** | 1747 / 1747 |
| go/syft/no_tests | 1.000 [0.999, 1.000] | 0.979 [0.974, 0.983] | **ahead** | 0.584 [0.573, 0.594] | 0.539 [0.528, 0.550] | **ahead** | 7977 / 7898 |
| go/syft/tests | 1.000 [1.000, 1.000] | 0.982 [0.979, 0.985] | **ahead** | 0.337 [0.331, 0.343] | 0.341 [0.335, 0.347] | **tied** | 25902 / 22590 |
| ts/zod | 0.999 [0.996, 1.000] | 0.995 [0.989, 0.998] | **tied** | 0.933 [0.920, 0.944] | 0.796 [0.773, 0.817] | **ahead** | 1626 / 1269 |
| ts/hono | 0.996 [0.989, 0.999] | 0.976 [0.958, 0.986] | **ahead** | 0.935 [0.916, 0.950] | 0.806 [0.775, 0.833] | **ahead** | 833 / 706 |
| rust/ripgrep/path_tests_excluded | 0.987 [0.983, 0.990] | 0.852 [0.838, 0.865] | **ahead** | 0.906 [0.898, 0.914] | 0.532 [0.517, 0.546] | **ahead** | 4796 / 4555 |
| rust/fd/path_tests_excluded | 0.996 [0.978, 0.999] | 0.899 [0.838, 0.939] | **ahead** | 0.984 [0.961, 0.994] | 0.619 [0.555, 0.679] | **ahead** | 256 / 231 |

Not published by repowise, so not compared: cobra without tests, the Rust cells other than `path_tests_excluded`, TypeScript with tests, and the held-out cells (`results/report.md`).

## Type hierarchy results

`EXTENDS`, `IMPLEMENTS` and method-override pairs against the compiler (README "Method / Type hierarchy"; full per-cell tables for all 40 cells, the
`embeds` relation for Go, the external-base and structural-implements diagnostics and the Go dispatch-credited tie-in are in `results/report.md`).
P = correct / (correct + wrong) over the lidx pairs the oracle can judge, R = oracle pairs lidx has / all oracle pairs, 95% Wilson intervals, n in
brackets. Go `overrides` and TypeScript `overrides` are lidx's dispatch pairs (`lidx dispatch-pairs`); Rust `overrides` are method-level IMPLEMENTS rows. Quoted only
after the hand spot-check came in under the 10% bar: 235 sampled items (20 FP + 20 FN + 20 correct pairs per language where that many exist, plus up to 10 per exclusion bucket and up to 20 of
`end_not_judged`, the one bucket that could hide a wrong pair), 0 oracle or matching artefacts, all 60 sampled correct pairs confirmed in the source, and 1 mis-bucketed exclusion (pooled 0.4%, [0.1%, 2.4%]).
`end_not_judged`: Go 13 of 13 read, all true relations on generic receivers; Rust 20 of 85 read, 19 true (impls on foreign or blanket types) and 1 real lidx error (a name collision, #457). The first TypeScript sample was 12 of 61 artefacts (aliases named in `extends` and merged
interfaces keyed at the wrong declaration); the oracle was fixed and the sample redrawn.

| Cell | implements | extends | overrides |
|---|---|---|---|
| go/cobra/tests | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) | n/a (0 pairs) | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) |
| go/gitleaks/tests | P 1.00 [0.68, 1.00] (n=8); R 1.00 [0.68, 1.00] (n=8) | n/a (0 pairs) | P 1.00 [0.68, 1.00] (n=8); R 1.00 [0.68, 1.00] (n=8) |
| go/syft/tests | P 1.00 [0.98, 1.00] (n=212); R 0.95 [0.91, 0.97] (n=223) | P 1.00 [0.57, 1.00] (n=5); R 0.83 [0.44, 0.97] (n=6) | P 1.00 [0.99, 1.00] (n=308); R 0.99 [0.98, 1.00] (n=310) |
| go/chi/tests | P 1.00 [0.61, 1.00] (n=6); R 0.50 [0.25, 0.75] (n=12) | P - (n=0); R 0.00 [0.00, 0.79] (n=1) | P 1.00 [0.88, 1.00] (n=29); R 0.85 [0.70, 0.94] (n=34) |
| go/urfavecli/tests | P 1.00 [0.99, 1.00] (n=270); R 1.00 [0.99, 1.00] (n=270) | P 1.00 [0.76, 1.00] (n=12); R 1.00 [0.76, 1.00] (n=12) | P 1.00 [0.98, 1.00] (n=189); R 0.54 [0.49, 0.59] (n=351) |
| go/viper/tests | P 1.00 [0.86, 1.00] (n=23); R 0.96 [0.80, 0.99] (n=24) | P 1.00 [0.34, 1.00] (n=2); R 0.50 [0.15, 0.85] (n=4) | P 1.00 [0.86, 1.00] (n=23); R 0.96 [0.80, 0.99] (n=24) |
| go/bubbletea/tests | P 1.00 [0.57, 1.00] (n=5); R 0.83 [0.44, 0.97] (n=6) | n/a (0 pairs) | P 1.00 [0.94, 1.00] (n=57); R 0.95 [0.86, 0.98] (n=60) |
| ts/zod | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) | P 1.00 [0.99, 1.00] (n=652); R 1.00 [0.99, 1.00] (n=652) | P - (n=0); R 0.00 [0.00, 0.09] (n=39) |
| ts/hono | P 1.00 [0.65, 1.00] (n=7); R 1.00 [0.65, 1.00] (n=7) | P 0.99 [0.93, 1.00] (n=75); R 1.00 [0.95, 1.00] (n=74) | P - (n=0); R 0.00 [0.00, 0.08] (n=45) |
| ts/trpc | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) | P 1.00 [0.90, 1.00] (n=33); R 1.00 [0.90, 1.00] (n=33) | P - (n=0); R 0.00 [0.00, 0.49] (n=4) |
| ts/ky | n/a (0 pairs) | P 1.00 [0.65, 1.00] (n=7); R 1.00 [0.65, 1.00] (n=7) | P - (n=0); R 0.00 [0.00, 0.43] (n=5) |
| ts/immer | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) | P 1.00 [0.57, 1.00] (n=5); R 1.00 [0.57, 1.00] (n=5) | P - (n=0); R 0.00 [0.00, 0.66] (n=2) |
| ts/h3 | P 1.00 [0.44, 1.00] (n=3); R 1.00 [0.44, 1.00] (n=3) | P 1.00 [0.68, 1.00] (n=8); R 1.00 [0.68, 1.00] (n=8) | P - (n=0); R 0.00 [0.00, 0.15] (n=21) |
| rust/ripgrep/path_tests_excluded | P 1.00 [0.97, 1.00] (n=111); R 0.93 [0.87, 0.97] (n=119) | n/a (0 pairs) | P 1.00 [1.00, 1.00] (n=810); R 1.00 [1.00, 1.00] (n=810) |
| rust/fd/path_tests_excluded | n/a (0 pairs) | n/a (0 pairs) | n/a (0 pairs) |
| rust/bat/path_tests_excluded | P 1.00 [0.68, 1.00] (n=8); R 1.00 [0.68, 1.00] (n=8) | P - (n=0); R - (n=0) | P 1.00 [0.84, 1.00] (n=20); R 1.00 [0.84, 1.00] (n=20) |
| rust/tokei/path_tests_excluded | n/a (0 pairs) | n/a (0 pairs) | P - (n=0); R - (n=0) |
| rust/xsv/path_tests_excluded | P - (n=0); R - (n=0) | n/a (0 pairs) | P - (n=0); R - (n=0) |
| rust/lidx/path_tests_excluded | P 1.00 [0.78, 1.00] (n=14); R 0.93 [0.70, 0.99] (n=15) | P - (n=0); R - (n=0) | P 1.00 [0.92, 1.00] (n=46); R 1.00 [0.92, 1.00] (n=46) |
| rust/just/path_tests_excluded | P 1.00 [0.85, 1.00] (n=21); R 0.66 [0.48, 0.80] (n=32) | n/a (0 pairs) | P 1.00 [0.90, 1.00] (n=36); R 1.00 [0.90, 1.00] (n=36) |
| rust/nextest/path_tests_excluded | P 1.00 [0.70, 1.00] (n=9); R 0.90 [0.60, 0.98] (n=10) | n/a (0 pairs) | P 1.00 [0.86, 1.00] (n=24); R 1.00 [0.86, 1.00] (n=24) |

What the numbers say: lidx's precision is 1.00 almost everywhere (one wrong TypeScript `extends`: an unimported global `Event` resolved to an unrelated alias). Recall is where the
gaps are: Go struct embedding has no edge at all (0 of 94 `embeds` pairs), Go methods declared in another file than their receiver type never join it
(163 of 788 override pairs, issue #455; embedding, #456), TypeScript has no override pairs (0 of 116; interface members are not symbols and base-class overrides need the
`override` keyword, #458) and Rust loses the type-level IMPLEMENTS when the impl sits in another file than its type (#457). `extends` for TypeScript and `implements` for
Go are good (779 of 779 and 525 of 544). Go `implements` recall caps the dispatch-credited call recall (see the report).

After #455 and #456 (the tables above are the run before them) the Go cells with tests read: `embeds` 94 of 94, `overrides` 786 of 788, `implements` 532 of 544, `extends` unchanged at 19 of 23; precision stays 1.00 and the call-edge figures do not move.
