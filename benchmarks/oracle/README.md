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
python3 benchmarks/oracle/run.py            # all 21 repositories, 40 cells (call graph and dead code), writes benchmarks/oracle/results/
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
(every miss, wrong edge, capture-gap edge and excluded edge with source locations and a category, for fixing; value references also `<cell>.refs_{fn,fp,capture_gap,ungradable,call_wins}.json`; Go cells also `<cell>.closure_self.json`, the oracle self-edges left out of the lidx-native grading;
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
  `GOWORK=off` and `GOPACKAGESDRIVER=off`. Go may download module sources and the toolchain the module asks for; downloading is not executing. The dead-code oracle (`go_oracle/dead`: staticcheck's U1000 analyzer and RTA over the same `packages.Load`) sets the same four variables on the load and is started under `safety.go_hardened_env`; `safety.FORBIDDEN_GO_PROCESS` fails the run if a `go build|test|run|generate|vet`, the compiler, the linker, cgo or a test binary shows up in its process tree; `test_safety.py` asserts the environment and the command, and runs the oracle on a module whose `init()` and `//go:generate` directive write marker files.
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

### Value references

A function, method or class used as a value rather than called (a callback argument, a handler-table entry, a struct-field handler such as
`cobra.Command{RunE: run}`, a method value, `.map(f)`) is a `REFERENCES` edge in lidx. It keeps handler layers out of `dead_symbols` and is what
`explain_symbol` shows under `other_edges`. The "Alongside REFERENCES and USES" table of the call grading only shows that adding these edges to a
*call* grading costs precision; it says nothing about whether the references themselves are right. A third grading, in its own section of
`results/report.md` ("Value references"), checks them against the same compilers. **The call-edge, hierarchy and dead-code tables and their numbers are
untouched**; this is a new `refs` list in each oracle's JSON (`ref_callers` for Go, `refs_dropped` for the buckets below) and a new `refs` key per cell in
`cells.json`. Code: `go_oracle/refs.go`, `ts_oracle/refs.mjs`, `rust_oracle/scip_calls.py` (oracles), `refs.py` (matching and grading), `report_refs.py`,
`spotcheck_refs.py`. Pairs are keyed (enclosing declaration, referenced declaration) at the declaration's name identifier, like the call edges; a use outside
any function (a package-level table, a module-level object literal) is keyed at line 0 of its file (`<module>`), and in TypeScript also accepted at the
module-level const or class that contains it, as for calls.

| Language | Oracle definition |
|---|---|
| Go (`go_oracle/refs.go`, `go/types`) | every identifier in `types.Info.Uses` whose object is a `*types.Func`, in an analysed in-repo file, whose declaration is in the repository, and which is not the `Fun` of a `CallExpr` after unwrapping parentheses, selectors and generic instantiation (`f[int](x)`). Method values `x.M`, method expressions `T.M` and interface method values are references. A generic instance collapses to its origin. `go f()` and `defer f()` are calls. Each pair records its forms (`ident`, `qualified`, `method_value`, `method_expr`, `iface_method_value`) and contexts (`call_argument`, `struct_field`, `map_value`, `composite_element`, `assignment`, `return`, `var_init`, `other`) |
| TypeScript (`ts_oracle/refs.mjs`, checker) | identifiers and property-access names whose (alias-resolved) checker symbol is function-like: a function, a method with a body, a class, or a const / property whose initialiser is a function. Excluded, each a named bucket in `refs_dropped`: callee position of a call, `new`, tagged template, decorator or JSX tag (and `f.call(..)` / `f.apply(..)`, which are calls); declaration names (including the key of `{ key: value }`); type positions (`typeof f`, annotations, type arguments, `implements X`, `extends X`); import and export specifiers and clauses (`export { f }`; `export default f` is a reference, context `export_default`); the left side of an assignment (`assignment_target`: `f = ..` writes the variable); the object of a static access (`Foo.create`: the class is not used as a value; `Foo.prototype` is kept) or of a property read on a function (`f.displayName`); `f.bind(..)` is kept (it binds the function as a value); symbols declared in a `.d.ts`, `lib` or outside the cell's `--subdir` (`external_target`); interface / type-literal members and abstract methods (`abstract_target`: no body to point at). Shorthand `{ fn }` is a reference |
| Rust (`rust_oracle/scip_calls.py`, rust-analyzer SCIP) | the `refs` the call reader already computes: a non-definition occurrence of a function symbol not followed by `(` (a turbofish `::<T>` is skipped before looking, so `Type::method::<T>(..)` is a call), minus occurrences with the SCIP `Import` role and occurrences inside a `use` item (`use_item`). An occurrence outside any function body (a `static` table, a closure in a `static`, a macro invocation at module level) is keyed at line 0 of its file, like Go and TypeScript (`ref_at_module_level`). SCIP columns are UTF-8 byte offsets: on a line with non-ASCII text the oracle re-reads what follows the identifier at the byte column and a call that the older reading took for a reference is dropped (`call_on_non_ascii_line`). An occurrence whose text is not the function's name is an implicit call (an overloaded operator `a += b`, a deref `*x`, an index `m[k]`), not a reference (`implicit_call`). `Trait::method` resolved by inference to one impl carries the trait's own declaration as `callee_alts`, an acceptable target. A reference whose every site is inside `#[cfg(...)]`-disabled code is left out (`oracle_refs_cfg_gated`, `rust_cfg.py`): rust-analyzer resolves names there only partly (a local declared in the disabled block is invisible and the name resolves to a function of the same name). Tuple-struct and variant constructors are not references |

**lidx side** (`refs.py`): its `REFERENCES` rows plus its `USES` rows whose target is a function (symbol kind `function` or `method`; TypeScript also a
`const` the compiler says holds a function; this is the call grading's `recorded_as_uses` category), both ends in-repo symbols, deduplicated to pairs. A
`REFERENCES` row with no resolved target is counted (`lidx_unresolved_reference_rows`) and used to classify misses (`unresolved`), not graded.

**Call wins, on both sides.** A reference pair that is also a call is a call. On the oracle side the pair is dropped when the compiler also sees a call from
the same declaration to the same function (`also called`: Go's callee positions of the same package, whether or not RTA reaches the caller; TypeScript's
own callee positions; Rust's call edges) or the call oracle lists the pair. An RTA edge that is dynamic (a call through a function value or interface, `dispatch`) is not such a call: `t.addEntryFunc(..)` makes RTA list every function stored in the field as a callee of the test, which would otherwise turn each `addEntryFunc: f` reference into a call. On the lidx side the pair is dropped when lidx has a `CALLS` or `DISPATCH`
edge for it, because lidx keeps only the call. Both removals are counted in the table (`removed: also called, oracle / lidx`) and sampled by hand
(`call_wins:oracle`, `call_wins:lidx`). A lidx reference the compiler agrees is a reference but whose pair the compiler also calls is **ungradable**
(`oracle_call_wins`), never a wrong pair.

**Counting.** A lidx pair is **correct** when the oracle has it as a reference that is not also a call, **wrong** when it does not and the pair is
judgeable, **ungradable** otherwise (`oracle_call_wins`; a caller or callee in a file the oracle did not analyse; a Rust caller that is neither a function definition nor a module; `cfg_gated_evidence`, evidence in `#[cfg]`-disabled code;
`no_evidence_line`, a USES row with no source line: a module-level row that names no referencing declaration. The Rust extractor used to record every `Type::f` path in value position that way; since #475 it records them as `REFERENCES` from the function that writes them, with the line, so the share is near 0 on the Rust cells). Precision = correct / (correct + wrong). Recall = oracle pairs lidx has / oracle pairs whose caller and
callee are both lidx symbols; pairs with no lidx symbol at an end are `capture_gap` rows (also reported as `recall_incl_capture_gap`).

Categories. Wrong pairs: `same_name_elsewhere` (lidx bound the reference to a different declaration of the same name as the one the compiler names at the
site, or elsewhere in that caller: the failure a name-tier guess produces), `wrong_target` (the compiler has a reference of another name at the site),
`call_as_reference` (the compiler only **calls** the function from that declaration), `caller_attribution`, `call_syntax_at_site`, `no_reference_at_site`.
Missed pairs: `no_edge` (nothing at the site; the report breaks these down by the reference's form, context and whether it is module level),
`coarse_module_level` (Rust: lidx has the reference only as a module-level USES row with no line),
`reference_as_call` (lidx has a `CALLS` edge for the pair), `wrong_target` (lidx has a reference there to another declaration; `same_name` in the detail),
`unresolved`, `marked_external`. The cross-categories are one edge labelled on both sides: an oracle reference lidx records as a call is a miss
`reference_as_call` here and a wrong edge `reference_as_call` in the call table; a call lidx records as a reference is a wrong pair `call_as_reference` here and a
missed call `call_as_reference` in the call table (the call table's `recorded_as_uses` keeps the `USES` rows).

**Precision by resolution kind.** Each lidx pair carries the `resolution_kind` of its rows; the report gives precision per cell and kind, so the failure of a
tier (`bare_name`, `receiver_type`, ...) is visible without reading lists.

**Dead-code cross-reference.** The dead-code section ("Missing value references") counts, per cell and column, how many of the declarations `dead_symbols`
claims dead although the oracle judged them live are the target of a value reference the compiler sees, and for how many lidx holds no reference pair at all.

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


## Dead code

`dead_symbols` graded by oracles neither lidx nor repowise wrote (issue #450). `results/report.md`, section "Dead code", has every table; this is the method and the reading. Code: `go_oracle/dead`, `ts_oracle/dead.mjs`, `rust_oracle/scip_dead.py` (oracles), `match_dead.py` (universe, buckets, grading), `dead_code.py` (orchestration, run once per cell by `run.py`), `report_dead.py`, `spotcheck_dead.py`. The call-edge grading and its numbers above are untouched: with the dead-code run added, every call-edge table in `results/report.md` is byte-identical except the lidx commit hash and the index wall-clock times.

**Two oracle columns per cell.**

| | Go | TypeScript | Rust |
|---|---|---|---|
| Unreferenced | staticcheck U1000 (`honnef.co/go/tools` v0.6.1, pinned in `go_oracle/go.mod`), go/analysis over `packages.Load`; exported identifiers are used by rule, so the universe is the unexported declarations; transitive, so a cluster whose only caller is unused is dead | `noUnusedLocals` diagnostics 6133/6196 for non-exported declarations (these contribute 0 on zod, hono, h3, ky and trpc (8 on immer), so the TypeScript dead set there comes from `findReferences` alone), LanguageService `findReferences` for exported ones | a declaration with no SCIP occurrence outside its own text, `use` lines and its own type's impl blocks (a type is live through its fields / variants; a trait-impl method through the trait method) |
| Unreachable | RTA from `main`/`init`/tests plus the exported functions and methods of importable packages (the `public_api` bucket, which other code may call, so what only they reach is not dead; generic functions and methods of generic types are entered through their static callees, since RTA cannot take an uninstantiated generic as a root). Only `Example*` functions with an `// Output:` (or `// Unordered output:`) comment are roots, as `go test` and `deadcode -test` only run those. `res.Reachable` is emitted, the machinery of `cmd/deadcode`; the same analysis from `main`/`init`/tests alone is run as a check and on gitleaks with tests its 7 unreachable functions are exactly `deadcode -test`'s 7 (`strict_roots_unreachable_functions` in the oracle stats); the published column also roots the exported API, so on the non-API universe it agrees with `deadcode -test` and reports fewer (gitleaks: 1, `Config.extendURL`, dead in both) | closure over the checker's reference graph (calls, `new`, callbacks, any mention; overrides reached through the base member) from the package entry and the module code of the files it imports | closure over SCIP references from `main`, tests and benches, library `pub` items and module-level code |

rustc's `dead_code` lint is not used (it needs `cargo check`, which runs build scripts and proc macros). The unreachable column judges functions and methods only. TypeScript cells are production code only, like the Go and Rust `no_tests` cells.

**Universe and buckets.** Functions, methods and types (TypeScript also module-level consts, reported as their own kind). Each exclusion is a named bucket with counts per cell and column: `public_api` (Go: exported in an importable package, TypeScript: reachable by re-export from the package entry, Rust: `pub` in a library crate; `pub` items of binary crates stay in), `exported_used_by_rule` (Go, unreferenced), roots (`test_root`, `main_init`, `library_root`), generated files, `macro_generated`, `cfg_twin`, `oracle_unresolved_site` (Rust: a token spelled like the declaration that rust-analyzer attached no symbol to; the sandbox runs no proc macros), `not_a_function`. The report states per cell whether it is a library and which universe applies. In library cells only unexported / internal items can be judged. A lidx claim on a public-API or root declaration is a wrong claim by construction, counted under its bucket name; a claim on anything the oracle cannot judge is ungradable.

**Two readings.** All `dead_symbols` rows (per-category limit raised above any repository), and the same rows without exported / `pub` declarations (recall over the unexported dead ones): what an exported-symbol policy buys. File level (no importer, unreachable from roots) is recorded in `cells.json` and `results/lists/<cell>.dead_oracle.json` as a target for a future file-level answer.

**Results (pooled over the cells of a language; per-cell and per-kind tables with Wilson intervals are in the report, and the `tests` / `no_tests` cells of one repository overlap).**

| Language | Column | Oracle-dead | P, all claims [95% CI] | R [95% CI] | P without exported [95% CI] |
|---|---|---:|---|---|---|
| Go | Unreferenced | 15 | 0.002 [0.001, 0.004] | 0.667 [0.417, 0.848] | 0.013 [0.007, 0.023] |
| Go | Unreachable | 290 | 0.020 [0.016, 0.025] | 0.255 [0.208, 0.308] | 0.117 [0.071, 0.186] |
| TypeScript | Unreferenced | 41 | 0.024 [0.015, 0.037] | 0.463 [0.321, 0.613] | 0.000 [0.000, 0.793] |
| TypeScript | Unreachable | 25 | 0.025 [0.014, 0.042] | 0.480 [0.300, 0.665] | 0.000 [0.000, 0.793] |
| Rust | Unreferenced | 90 | 0.052 [0.043, 0.064] | not reported | 0.075 [0.061, 0.092] |
| Rust | Unreachable | 121 | 0.088 [0.074, 0.105] | not reported | 0.135 [0.113, 0.161] |

Reading: precision is low because most of what `dead_symbols` returns is public API or a root (Go `Test*`/`init`: 2,296 + 48 claims; exported / `pub`: Go 1,858, TypeScript 742, Rust 546), or a type / function the tools show to be used (Go 669 types, Rust 457 types and 540 functions and methods). The repositories themselves have almost nothing dead (summed over the 14 Go cells the unreferenced column has 15 dead declarations; cobra, viper, bubbletea, fd and xsv have none), so recall rests on small counts and its intervals are wide. A policy that never claims exported symbols lifts Go unreferenced precision from 0.002 to 0.013 and removes 765 of 785 TypeScript claims. Findings filed as lidx issues: #461 (exported / `pub` claims), #462 (Go `Test*` / `init` / `main`), #463 (used types), #464 (functions and methods called through method values, interfaces, traits, overrides).

**Tests as roots (issue #472).** `dead_symbols` takes `tests_are_roots` (default true: a reference from test code keeps a declaration alive). The harness passes `tests_are_roots: false` for the cells whose oracle leaves test code out of its roots (the `no_tests` cells: Go without `-tests`, Rust `--no-tests`, TypeScript on production code), so what only tests reach is claimed (reason `reached_only_from_tests`); the cells that include tests, and the Rust `path_tests_excluded` cells (inline `#[cfg(test)]` code stays a root there), use the default. The option recovers the tests-only misses: Go unreachable `incoming_from_tests_only` 57 to 4, recall 0.252 to 0.583 [0.525, 0.638], precision 0.211 to 0.371, measured on main plus only this option. A test file is one `is_test_file` names; a test function is one marked `#[test]`/`#[rstest]`/`#[test_case]`.

**Methods of external interfaces (issue #472).** A method that implements an interface or trait of the standard library or a dependency is reached through its receiver type, so `dead_symbols` no longer claims it when the type is live (`implements_external_interface`, listed with `include_external_interface`). A type is live when the reference edges from the roots reach it (a type referenced only from claims or from an unreachable cluster, or only from tests under `tests_are_roots: false`, is dead, and a method of a dead type stays a claim). Evidence: Go, a name-and-signature match against the standard-library interface table, or a `var _ pkg.Iface = (*T)(nil)` assertion (any form, any file of the package) or an embedded `pkg.Iface` naming a package outside the repository, in which case the exported methods of T are live (only an exported method can satisfy another package's interface; a dependency's source is not indexed, so its method set is unknown); Rust, the method's `IMPLEMENTS` edge to an external trait stub; TypeScript, runtime protocol names and well-known ECMAScript/DOM/Node interfaces named in `implements`. Pooled, main plus the `tests_are_roots` option versus this change: Go unreachable claims 456 to 350, precision 0.371 to 0.477 [0.425, 0.529], recall 0.583 to 0.576 [0.518, 0.631] (wrong `reachable_method` claims 284 to 180); Go unreferenced claims 460 to 354, precision 0.024 to 0.031, recall 0.733 unchanged; without exported: unchanged (Go 0.795 / 0.383); Rust and TypeScript are identical; the call-edge, hierarchy and impact tables are unchanged except wall-clock times, and the value-reference `unreachable` counts. The TypeScript cells run with the option too, which recovers their tests-only misses (3 and 2) and adds the wrong claims on public API and one reachable function that only tests used: precision 0.710 / 0.737, recall 0.537 / 0.560 (unreferenced / unreachable), the same for main plus the option and for this change.

**Spot-check** (`spotcheck_dead.py`, verdicts and a note per item in `spotcheck-dead-verdicts.json`; up to 20 wrong claims and 20 missed declarations per language, 10 per exclusion bucket, read against the source by one pass per language). Artefact plus mis-bucket rate: Go 2/35 = 5.7% (wrong claims 0/20, missed declarations 2/15), TypeScript 5/40 = 12.5% (0/20 and 5/20), Rust 2/25 = 8.0% (0/20 and 2/5); buckets 4/255 = 1.6%; pooled 13/355 = 3.7% [2.2%, 6.2%]. Every wrong claim sampled in all three languages was a real use. Oracle bugs found while sampling and fixed: trait-impl method names, uses inside attribute strings, bench and module-level roots, generic types' exported methods (entered through their static callees), examples without an `// Output:` comment (not run by `go test`), `impl Trait for [u8]` method names, and library API as roots in binary cells (a library that also ships a `cmd/` tool had its unexported helpers judged dead because only the exported API called them).

**Graded list.** The harness grades the **default** `dead_symbols` output, which is what a user gets: public-API declarations (exported Go identifiers of importable packages, TypeScript exports reachable from the package entry, `pub` items of Rust library crates; issue #461) and conventional roots (Go `Test*`/`Benchmark*`/`Fuzz*`/`Example*`, `init`, `main`; issue #462) are not in it. Each row carries `confidence` (`high` / `medium`); `results/report.md` has precision per tier. A second request with `include_public_api: true` only sizes the opt-in `public_api` group and records which of its declarations the oracle calls dead (`public_api_oracle_dead` in `cells.json`, the recall the policy costs). A binary that predates the policy rejects that parameter (`unknown param(s)`), which the harness reads as no group (any other failure is raised), and returns untiered rows, so the same harness grades both sides of the comparison.

**Recall caveats.** Rust recall is **not reported**: the corpus has only 5 missed Rust declarations and the spot-check read all of them; 2 (40%) were oracle artefacts (`sort_lines` and `cmd_exists` in ripgrep's tests are called inside `rgtest!`-generated tests that SCIP does not resolve), above the 20% bar, so the pooled and per-cell tables print `not reported`. The earlier draw had 9 of 10 missed Rust items as artefacts, mostly methods of in-repo traits implemented for foreign or primitive types (`impl Logic for bool`, `SliceExt for [u8]`); those methods, and trait methods all of whose impls are such, are now the named bucket `impl_for_foreign_type` (124 declarations), and `trait` items themselves are the bucket `trait_declaration` (not claimable). TypeScript unreachable recall is a lower bound: 5 of 20 sampled missed items were in fact used, all in hono and all reached through dynamic indexing of a namespace import (`intrinsicElementTags[tag]`), which the reference closure cannot follow. The same cause slightly inflates TypeScript precision: of the 15 oracle-dead hono declarations in those files, 8 are lidx claims counted correct that are in fact used. Rust precision is not affected: all 20 sampled Rust wrong claims were real uses.

**Cost.** The dead-code oracle adds 0.2 to 35 s per cell; `go/syft/tests`, the largest: 35.1 s and 6.4 GB peak (measured with other jobs running on the machine; 22 s on an idle one) (recorded in `cells.json` under `cells['go/syft/tests']['dead']['oracle_meta']`), well inside the +5 minute budget.

## Type hierarchy results

`EXTENDS`, `IMPLEMENTS` and method-override pairs against the compiler (README "Method / Type hierarchy"; full per-cell tables for all 40 cells, the
`embeds` relation for Go, the external-base and structural-implements diagnostics and the Go dispatch-credited tie-in are in `results/report.md`).
P = correct / (correct + wrong) over the lidx pairs the oracle can judge, R = oracle pairs lidx has / all oracle pairs, 95% Wilson intervals, n in
brackets. Go `overrides` and TypeScript `overrides` are lidx's dispatch pairs (`lidx dispatch-pairs`); Rust `overrides` are method-level IMPLEMENTS rows. Quoted only
after the hand spot-check came in under the 10% bar: 235 sampled items (20 FP + 20 FN + 20 correct pairs per language where that many exist, plus up to 10 per exclusion bucket and up to 20 of
`end_not_judged`, the one bucket that could hide a wrong pair), 0 oracle or matching artefacts, all 60 sampled correct pairs confirmed in the source, and 1 mis-bucketed exclusion (pooled 0.4%, [0.1%, 2.4%]).
`end_not_judged`: Go 13 of 13 read, all true relations on generic receivers; Rust 20 of 85 read, 19 true (impls on foreign or blanket types) and 1 real lidx error (a name collision, fixed by #457). The first TypeScript sample was 12 of 61 artefacts (aliases named in `extends` and merged
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
| ts/zod | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) | P 1.00 [0.99, 1.00] (n=652); R 1.00 [0.99, 1.00] (n=652) | P 1.00 [0.91, 1.00] (n=39); R 1.00 [0.91, 1.00] (n=39) |
| ts/hono | P 1.00 [0.65, 1.00] (n=7); R 1.00 [0.65, 1.00] (n=7) | P 1.00 [0.95, 1.00] (n=74); R 1.00 [0.95, 1.00] (n=74) | P 1.00 [0.92, 1.00] (n=45); R 1.00 [0.92, 1.00] (n=45) |
| ts/trpc | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) | P 1.00 [0.90, 1.00] (n=33); R 1.00 [0.90, 1.00] (n=33) | P 1.00 [0.51, 1.00] (n=4); R 1.00 [0.51, 1.00] (n=4) |
| ts/ky | n/a (0 pairs) | P 1.00 [0.65, 1.00] (n=7); R 1.00 [0.65, 1.00] (n=7) | P 1.00 [0.57, 1.00] (n=5); R 1.00 [0.57, 1.00] (n=5) |
| ts/immer | P 1.00 [0.21, 1.00] (n=1); R 1.00 [0.21, 1.00] (n=1) | P 1.00 [0.57, 1.00] (n=5); R 1.00 [0.57, 1.00] (n=5) | P 1.00 [0.34, 1.00] (n=2); R 1.00 [0.34, 1.00] (n=2) |
| ts/h3 | P 1.00 [0.44, 1.00] (n=3); R 1.00 [0.44, 1.00] (n=3) | P 1.00 [0.68, 1.00] (n=8); R 1.00 [0.68, 1.00] (n=8) | P 1.00 [0.85, 1.00] (n=21); R 1.00 [0.85, 1.00] (n=21) |
| rust/ripgrep/path_tests_excluded | P 1.00 [0.97, 1.00] (n=119); R 1.00 [0.97, 1.00] (n=119) | n/a (0 pairs) | P 1.00 [1.00, 1.00] (n=810); R 1.00 [1.00, 1.00] (n=810) |
| rust/fd/path_tests_excluded | n/a (0 pairs) | n/a (0 pairs) | n/a (0 pairs) |
| rust/bat/path_tests_excluded | P 1.00 [0.68, 1.00] (n=8); R 1.00 [0.68, 1.00] (n=8) | P - (n=0); R - (n=0) | P 1.00 [0.84, 1.00] (n=20); R 1.00 [0.84, 1.00] (n=20) |
| rust/tokei/path_tests_excluded | n/a (0 pairs) | n/a (0 pairs) | P - (n=0); R - (n=0) |
| rust/xsv/path_tests_excluded | P - (n=0); R - (n=0) | n/a (0 pairs) | P - (n=0); R - (n=0) |
| rust/lidx/path_tests_excluded | P 1.00 [0.80, 1.00] (n=15); R 1.00 [0.80, 1.00] (n=15) | P - (n=0); R - (n=0) | P 1.00 [0.92, 1.00] (n=46); R 1.00 [0.92, 1.00] (n=46) |
| rust/just/path_tests_excluded | P 1.00 [0.89, 1.00] (n=32); R 1.00 [0.89, 1.00] (n=32) | n/a (0 pairs) | P 1.00 [0.90, 1.00] (n=36); R 1.00 [0.90, 1.00] (n=36) |
| rust/nextest/path_tests_excluded | P 1.00 [0.72, 1.00] (n=10); R 1.00 [0.72, 1.00] (n=10) | n/a (0 pairs) | P 1.00 [0.86, 1.00] (n=24); R 1.00 [0.86, 1.00] (n=24) |

What the numbers say: lidx's precision is 1.00 everywhere. Recall is where the
gaps are: at the commit of the committed results, Go struct embedding had no edge and Go methods declared in another file than their receiver type never joined it (issues #455, #456, fixed since; regenerate the results to see the current figures), Rust's `implements` recall is 1.00 on every cell with pairs since #457. TypeScript is complete (`overrides` 116 of 116: interface and type-literal members are `decl` symbols and `override` is optional, #458) and Go `implements` is good (see the tables). Go `implements` recall caps the dispatch-credited call recall (see the report).

## Impact, tests and flows

`analyze_impact` upstream sets, its test layer and `trace_flow` stop reasons graded against the **closure** of the compiler call graphs above (issue #451). `results/report.md`, section "Impact, tests and flows", has every table; `results/impact-outliers.md` the worst 10 seeds per cell; this is the method and the reading. Code: `impact.py` (closure, seeds, grading, statistics), `impact_run.py` (driver of `run.py --impact`), `report_impact.py`, `spotcheck_impact.py` (verdicts and a note per pair in `spotcheck-impact-verdicts.json`), tests in `test_impact.py`. `run.py --impact` grades the 32 comparable cells (the Rust `*_all_features` cells are diagnostics and get no verdict anywhere) from the oracle outputs: it reuses the cached ones and only re-runs a compiler when the oracle sources changed since the cache was written (as for every other section); no corpus code runs, and the only thing talked to is `lidx serve` (JSONL RPC) over a fresh index. The call-edge, hierarchy, dead-code and value-reference results and tables are untouched; the new section is appended to the end of `results/report.md`.

**Oracle closure.** The graph of a cell is its oracle edge list, with the Go closure self-edges `F -> F` removed as the call-edge grading does (`match.drop_closure_self`). Go: RTA, dynamic calls at the concrete implementations RTA names (the interface-method alternatives `callee_alts` are not followed, so a dispatch edge is one edge to the implementation). Rust: SCIP; an edge to a trait method carries every in-repository implementation as `callee_alts`, and all of them are callees. TypeScript: tsc; an edge to an abstract / interface member is expanded to the members the checker's own override relation (`hierarchy.overrides` of the oracle) lists as overriding it, which is what RTA does for Go without the allocation analysis. Alternative keys of one callee (a class and its constructor for `new X()`, merged declarations) are all callees, every alternative caller lidx indexes is a caller (a module-level call inside several `const X = core.$constructor(..)` initialisers is listed under the module with each const as an alternative), and a caller lidx has no symbol for is lifted to the nearest enclosing declaration it has, as `match.grade` does. Module-level nodes (line 0) are left out of the sets (lidx drops module symbols from impact answers). For a seed `s`: `Up_k(s)` = callers within `k` hops, `Down_k(s)` = callees within `k` hops, for `k = 3` (lidx's default depth) and unbounded; `Tests(s)` = the test roots in `Up_k(s)`. A test root is a top-level Go function `Test*`/`Benchmark*`/`Fuzz*` in a `_test.go` file (`Example*` is not one), or a Rust function whose attribute lines above the declaration include `#[test]` (the attribute is read from the source, walking up through attributes, doc comments and blank lines). TypeScript has none: test files are out of scope on both sides, so its test-reach column is void. Nodes lidx has no symbol for stay out of the recall denominator (`Oracle unindexed`).

**Seeds.** 100 per cell, stratified by oracle in-degree (0, 1-3, 4-10, >10; 25 each, the shortfall of a thin stratum goes to the thicker ones; the in-degree is that of the compiler's edges as emitted), drawn with `random.Random("2026|<cell id>")` from the declarations the oracle judged (`callers`; for Go the functions RTA proved reachable, so RTA-unreachable code is never drawn) that lidx indexes as a function, method or const (arrow-function binding), so every seed can be queried; constructors are not seeds (the compiler keys `new X()` at the class, so a constructor has no callers by construction). The draw is a pure function of the oracle output, the cell id, the seed 2026 and that eligibility; the result is committed in `results/seeds/<cell>.json` and `run.py --impact` grades the committed seeds (it warns if a fresh draw differs; `--redraw-seeds` replaces them).

**What lidx exposes, and the calls.** `find_tests_for` no longer exists; tests surface through the test layer of `analyze_impact` (a symbol counts when `is_test_symbol` holds and it reaches the seed within `max_depth` upstream hops over every edge kind; Go: a `_test.go` function whose lowercased name starts with `test`, so `Benchmark*`, `Fuzz*` and `Example*` are not tests; Rust: `#[test]`-family attributes in the signature or `test_*` functions under `tests/`). All calls use the seed's symbol id.

| Graded | Call |
|---|---|
| Upstream set `Up_k` | `analyze_impact(id, direction=up, max_depth=k, include_tests=true, kinds=[CALLS, DISPATCH], exclude_resolution_kinds=[containment], enable_test=false, enable_historical=false, include_paths=false, limit=2000, max_response_bytes=64000000)`; `k` is 3, and 10 for "unbounded" (lidx caps `max_depth` at 10; the oracle closure is unbounded, and the report counts the seeds whose oracle closure reaches beyond 10 hops). The oracle is a call graph and cannot judge a dependent reached through a reference, a `USES` or an import, nor lidx's `containment` hop (a function to the functions declared inside it): the graded call is restricted to calls |
| Default call | the same without `kinds` and `exclude_resolution_kinds` (every edge kind, which is what a bare call follows; the parameter text used to say `CALLS, RPC_IMPL`, it now names the real default): reported beside it; the members it adds that the oracle lacks are counted as "beyond calls", not called wrong |
| Plain call | `analyze_impact(id, direction=up, max_depth=3, include_tests=true)`, every other parameter at its default (response cut at 30,000 bytes, paths on, all layers); the default call leaves `max_depth` out as well. What a user gets |
| Tests `Tests_k` | `analyze_impact(id, direction=up, max_depth=k, enable_direct=false, enable_test=true, ...)`: the test layer alone, so the answer holds exactly the layer's tests. It follows every edge kind and cannot be restricted; a wrong test whose path to the seed crosses a non-call hop (one more call with paths) is counted apart as "beyond calls" |
| Flows `Down_k` and leaves | `trace_flow(start_id, direction=downstream, max_hops=k, max_bytes=200000, include_snippets=false)` |

**Matching and gradability.** A lidx member is mapped to its canonical declaration key as in `match.py` and is **right** when it is in the oracle set, **wrong** when it is not, the oracle analysed that function (one of its `callers`, in a file it covers) and the oracle can judge the whole lidx path to the seed, and **ungradable** otherwise. The oracle can judge a path when every symbol on it is in a file the oracle analysed and, for Go (RTA analyses only what is reachable from its roots), a function RTA reached; interface / trait members on the path are pass-through hops of a dispatch and are exempt, and an interface, trait or bodyless `abstract` member is never a member (it is a contract, not a caller). A chain through an example or nested module the oracle never loaded, test code in a production-only cell, or a type nothing allocates is therefore ungradable, not wrong. Pooled precision and recall are pair-level over (seed, member) pairs with Wilson intervals (pairs cluster by seed, so those intervals are optimistic); **macro** precision and recall average the per-seed rates (a seed with no members on a side has no rate for it) with a 1,000-sample bootstrap percentile interval over seeds (seed 2026), so one high-fan-out seed cannot dominate. Seeds where lidx truncates its answer (`truncated` from the 2,000-entry `limit`, or for `trace_flow` the byte budget) leave the recall figures and stay in the precision figures; the `max_hops` ceiling is not a truncation (the oracle is cut at the same k). A seed whose oracle test set and lidx test set are both empty is a correct empty answer and is counted separately (`empty ok`), not in any rate.

**Honesty.** (1) `lower_bound`: the 2x2 of `lower_bound: true/false` of the default call against per-seed completeness (the upstream set has no missing oracle caller) over seeds lidx did not truncate. (2) `trace_flow` leaves: **false leaf** = a leaf marked `no_callees` where the oracle has an in-scope callee (a callee lidx has a symbol for), **gap with no in-repo callee left** = a leaf marked `unresolved_calls` where the oracle has no in-scope callee. This is reported, not graded as an error: lidx defines `unresolved_calls` as a resolution gap, and an unresolved external name is a true gap. Every other stop reason (`depth_limit`, `truncated`, `already_visited`, `external`, `filtered`) is excluded from both. For `unresolved_calls` the report adds how many of these gaps name only calls lidx itself flags `external` (at most three names per leaf are visible), and for Go false leaves how many have only function-literal callees.

**Dispatch.** lidx has no stored `DISPATCH` edge kind: interface dispatch is a query-time overlay (`resolution_kind: interface_dispatch`, hops with edge id 0) that `analyze_impact` and `trace_flow` cross like any CALLS edge, with the interface method as an intermediate symbol. The pass-through costs no depth level (#481): a call through an interface is one hop, as in the compiler's graph, and it is followed even at the depth ceiling. Its fan-out is counted as claims: a member whose path crosses such a hop is a dispatch-derived claim and is graded like every other member, against the oracle's concrete-implementation edges, with no credit from the oracle's interface alternatives; the report lists per cell how many such members there are and how many are right. lidx has no dispatch overlay for Rust traits.

**Diagnostic closures (no verdict is drawn from them).** Go, without function-value edges: the Go oracle resolves a call of a function value (`parg(cmd, args)` in cobra's `MatchAll`, `p.cancel()`) to the address-taken functions of that signature that RTA reaches and keys a function literal at its enclosing declaration, so the edge names the function that built the closure (`MaximumNArgs`) or an unrelated closure of the same signature; lidx does not resolve a call of a function value; the closure is rebuilt without the edges flagged `closure` or `dispatch` without interface alternatives (interface invocations stay). TypeScript: the compiler's edges as stated, without the override expansion.

**Plain call and depth.** A plain call is cut at 30,000 bytes. Before #480 that cut hit 220 of 1,400 Go seeds, 60 of 600 TypeScript and 172 of 1,200 Rust seeds (recall of the entries that fit: syft 0.05 to 0.08 against 0.15 to 0.22 once the cut is lifted); an answer over the cap now sheds its explanations first (alternative paths, paths of entries three or more calls away, all paths, the per-file symbol lists of the summary: `paths_omitted`, `summary.symbols_omitted`) and only then its farthest symbols, which brings the cut seeds to 105, 12 and 123. The depth default stays 3: an adaptive depth (three levels, then deeper while the answer holds fewer than 40 or 100 symbols) was built and graded on seven cells, raised all-seed recall by at most 3 points (cobra 0.615 to 0.635, urfavecli `tests` 0.458 to 0.468, trpc 0.779 to 0.783) and one to three times the seeds that come back cut (cobra `no_tests` 8 to 28, trpc 8 to 22), because the seeds that hide most are hubs that stop at three levels anyway; it was removed. What was missing is the saying: an answer cut by the depth now carries `depth_limited`, `beyond_depth` (the distinct symbols one level deeper, a lower bound on what is hidden) and a next hop that retries with twice the depth. An interface-dispatch hop used to cost two depth levels, so a caller three calls away behind a dispatch was found only from `max_depth` 4 (#481).

### Impact, tests and flows: results

Pooled over the cells of a language (the `tests` and `no_tests` cells of one repository overlap; per-cell tables with Wilson and bootstrap intervals are in `results/report.md`). Quoted only where the hand spot-check (below) allows it.

| Language | k | Precision (calls) | Recall (calls) | Macro recall [95% bootstrap CI] | Recall, Go without function-value edges (upper-bound view, excludes real function-value calls, built post hoc; not a verdict) |
|---|---|---|---|---|---|
| Go | 3 | not quoted (3 of 18 sampled extra pairs are oracle artefacts) | not quoted (14 of 20 sampled missing pairs are oracle artefacts) | | 0.990 [0.989, 0.991] (0.876 before #481) |
| Go | unbounded | not quoted | not quoted | | 0.969 [0.968, 0.971] (0.957 before #481) |
| TypeScript | 3 | 0.995 [0.993, 0.997] | 0.726 [0.714, 0.738] | 0.854 [0.828, 0.881] | |
| TypeScript | unbounded | 0.988 [0.985, 0.991] | 0.543 [0.532, 0.553] | 0.824 [0.796, 0.853] | |
| Rust | 3 | not quoted (13 of 20 sampled extra pairs are oracle artefacts) | 0.483 [0.476, 0.491] | 0.761 [0.736, 0.787] | |
| Rust | unbounded | not quoted | 0.514 [0.509, 0.519] | 0.730 [0.702, 0.756] | |

Reading. Precision is high wherever it can be judged (TypeScript 0.995 at k = 3; Go and Rust precision is not quoted): the wrong members are lidx's name-based tiers (`bare_name` joins `promise.catch`, `Option::or`, `.toString()`, `.map` on a standard type to an in-repo method of the same name: TypeScript 20 of 20 sampled extra pairs, Rust 7 of 20) and, in Go, interface dispatch that fans out to the implementers of the interface that declares the method instead of the receiver's own (`Source` embeds `Identifiable`; a `WritableResolver` call reaches `FiletreeResolver`). Recall is the weak side. TypeScript misses calls through typed members, `new` of a `core.$constructor` const (recorded as `USES`), module-level initialiser calls and element-access calls (20 of 20 sampled missing pairs). Rust has no trait dispatch, so a trait-method caller never reaches the implementation (20 of 20). Go recall against the closure without function-value edges (an upper-bound view that excludes real function-value calls lidx cannot resolve, built post hoc; no verdict) was 0.876 at depth 3 and 0.957 unbounded, the gap being mostly the dispatch hop that cost a depth level (19 of 20 sampled pairs of that closure); with the hop free (#481) it is 0.990 at depth 3 and 0.969 unbounded (the unbounded figure is lower because depth 10 is lidx's cap, and the pooled recall leaves out the seeds it truncated). The sampled pairs below were drawn before that change. The full Go closure additionally holds the function-value edges, where 14 of 20 sampled pairs were RTA conflations and not real calls, so its recall is not quoted.

With the dispatch hop free (#481) the calls-only upstream set at k = 3 grows: pooled Go recall 0.421 to 0.477, TypeScript 0.726 to 0.743, Rust unchanged (it has no dispatch overlay), and the pooled Go precision fell from 0.9984 to 0.9870 because the embedded-interface fan-out (61 `altsrc` tests reached through a `FlagInputSourceExtension` call, in three urfavecli seeds) was now found inside three levels instead of four. #482 fixes the cause: a Go `DISPATCH` edge keeps the interface its receiver is typed as (`receiver_scope`, set by the interface-edge re-judging pass) when that embeds the interface declaring the method, and the traversals fan out to the implementations of that interface only (the interfaces of the implementation's type and of the types embedding it); an embedder that redeclares the method shadows it and is skipped (it reaches the inner method only from its own body: the first version of the narrowing credited it, which left 122 wrong pairs at k = 3 in urfavecli `tests`, a chain of four calls counted as three), and the walk over embedders runs to a fixpoint. With it Go precision is 1.000 at k = 3 (0 wrong pairs, 30 on main) and unbounded (0, 76 on main), recall unchanged. The default call (every edge kind) adds members the oracle cannot judge, which lowers its precision against a call graph (TypeScript 0.677 at k = 3, zod 0.45; Go and Rust figures are not quoted); the added members are references, `USES` and imports. Depth: at depth 3 the set is under half of the unbounded oracle set on a majority of seeds in 9 of 32 cells (Go cobra, chi, syft and urfavecli `tests`, Rust just and lidx), and in 3 of them (syft `no_tests`, urfavecli `tests`, just) it also holds for the diagnostic closure and for lidx's own depth 3 against depth 10.

**Test reach** (Go and Rust `tests` cells; the pairs were not read one by one, the missing and extra chains of the upstream samples go through the same traversal). Pooled over the cells: Go 0.991 precision and 0.396 recall at k = 3 (0.992 and 0.271 unbounded), Rust 0.898 and 0.201 (0.925 and 0.224). `Benchmark*` and `Fuzz*` roots are never found (Go: 18 of 55 missing gitleaks pairs at k = 3, 98 of 1,346 in chi), which puts gitleaks' test-reach recall (0.786) below its upstream recall (0.938); the other Go cells and Rust are at or above their upstream recall, and their low recall is the upstream recall (dispatch and function values). 272 Go test pairs at k = 3 reach the seed only through a reference or use and are counted as "beyond calls", not wrong.

**Stop reasons** (`trace_flow`, leaves; pooled over the cells). False leaf, before #478: Go 54 of 2,170 (2.5% [1.9, 3.2]) at k = 3, TypeScript 8 of 678 (1.2% [0.6, 2.3]), Rust 1 of 1,183 (0.1%); after: Go 0 of 2,731, TypeScript 1 of 672 (h3 `status`, a getter whose `new` call is attributed to the module), Rust 1 of 1,185. Every Go false leaf was a function whose body calls a function-typed parameter, receiver or slice element (`parg(cmd, args)` in cobra's `MatchAll`, `middlewares[i](h)` in chi's `chain`, `fn(v)` in viper's `apply`, `proc(resolver, ..)` in syft's `selectFiles`): lidx recorded neither an edge nor an unresolved reference, so the leaf said `no_callees`; it is now an `unresolved_references` row with reason `function_value` and the leaf says `unresolved_calls` (the gap rate in the table grows by those leaves). Seven of the eight TypeScript ones were calls in the initialiser of a `const` that merges with an `interface` of the same name (zod's `ZodUnion`): the call belonged to the interface symbol, now to the const. Cells above 1% before: chi 12.9% / 9.4%, cobra `tests` 4.9%, syft `no_tests` 4.8%, viper 3.2% / 3.1%, urfavecli `no_tests` 1.3%, syft `tests` 1.5% (Go); zod 7.7%, h3 1.5% (TypeScript); after: h3 1.6% (1 of 62) only. Gap with no in-repo callee left (not a claim of wrongness): 52.6% (Go; 59.5% before the function-value gaps joined it), 94.0% (TypeScript) and 90.8% (Rust) of `unresolved_calls` leaves have no oracle callee left, and most name only external calls (615 of 725 of the TypeScript ones are tagged `external` by lidx itself yet still reported `unresolved_calls`), so the signal is uninformative. `lower_bound` and completeness agree on 60.3% (Go), 20.5% (TypeScript) and 25.3% (Rust) of seeds at k = 3 (60.0%, 21.0%, 25.3% before): it is `true` on most seeds whatever is missing.

**Spot-check** (`spotcheck_impact.py`; 20 extra and 20 missing pairs per language, 18 extra in Go where that is all there are, plus 20 missing pairs of the Go diagnostic closure; each chain read against the source). Artefact plus mis-bucket rate: TypeScript 0/40 (0.0%), Rust 13/40 (32.5%: 13 of the 20 extra pairs, 11 in tokei, whose `LanguageType` is generated by `build.rs` that the sandbox does not run, so rust-analyzer cannot resolve calls on it, and 2 in nextest where the oracle has no edge from a binary's `main`; 0 of the 20 missing pairs), Go 17/58 (29.3%: 3 of 18 extra pairs, 14 of 20 missing pairs of the full closure, 0 of the 20 of the diagnostic closure). Wrong pairs of the extra samples that were `lidx_error`: Go 15 of 18 (embedded-interface and receiver-interface dispatch fan-out), TypeScript 20 of 20 and Rust 7 of 20 (`bare_name`). The numbers above are quoted only for the rows with an artefact rate under 10%. The hand sample was drawn from the lists of the build before the last rebase onto main (#471 changed Rust and reference edges only); the graded tables were regenerated on the current build, Go and TypeScript call edges are byte-identical and the Rust call edges of 6 repositories changed.

Findings filed as lidx issues: #478 (`no_callees` on a function that calls a function value: false-leaf rate above 1% in 10 cells), #479 (Go test layer misses `Benchmark*` and `Fuzz*`), #480 (depth 3 hides more than half of the closure in 9 cells; `kinds` default text), #481 (an interface-dispatch hop costs two depth levels), #482 (Go dispatch at the declaring embedded interface), #483 (`bare_name` joins standard-library methods to in-repo ones).

## Value reference results

`REFERENCES` rows and function-target `USES` rows against the compiler (README "Method / Value references"; the per-cell tables, resolution-kind
tables, categories, lists and the dead-code cross-reference are in `results/report.md`, "Value references"). P = correct / (correct + wrong) over the lidx pairs the
oracle can judge, R = oracle pairs lidx has / oracle pairs whose ends are both lidx symbols, 95% Wilson intervals; pairs that are also calls are removed on both sides.
Quoted only after the hand spot-check came in under the 10% bar: 244 sampled items (20 wrong pairs, 20 missed pairs and 20 correct pairs per language where that many exist, plus up to 10 per exclusion
bucket), 0 oracle or matching artefacts, 0 mis-bucketed exclusions, all 60 sampled correct pairs confirmed in the source (0/244, [0.0%, 1.5%]). The earlier draws were over the bar and found the harness bugs listed in
`spotcheck-refs-verdicts.json` (most of all: a Go RTA edge for a call through a function value counted as a call of the declaration that stores the function, 20 of 31 Go bucket items), which were fixed and the sample redrawn.

| Cell | P [95% CI] | R [95% CI] | lidx pairs graded (wrong) | oracle pairs (matched) |
|---|---|---|---:|---:|
| go/cobra/tests | 1.000 [0.986, 1.000] | 1.000 [0.986, 1.000] | 261 (0) | 261 (261) |
| go/gitleaks/tests | 1.000 [0.741, 1.000] | 1.000 [0.741, 1.000] | 11 (0) | 11 (11) |
| go/syft/tests | 1.000 [0.991, 1.000] (1 dropped as calls) | 1.000 [0.991, 1.000] | 416 (0) | 416 (416) |
| go/syft/no_tests | 1.000 [0.985, 1.000] (1 dropped as calls) | 1.000 [0.985, 1.000] | 249 (0) | 249 (249) |
| go/chi/tests | 1.000 [0.879, 1.000] | 1.000 [0.879, 1.000] | 28 (0) | 28 (28) |
| go/urfavecli/tests | 1.000 [0.904, 1.000] | 1.000 [0.904, 1.000] | 36 (0) | 36 (36) |
| go/bubbletea/tests | 1.000 [0.851, 1.000] | 1.000 [0.851, 1.000] | 22 (0) | 22 (22) |
| go/viper | no value reference in the repository | | 0 | 0 |
| ts/zod | 0.721 [0.598, 0.818] | 0.321 [0.250, 0.403] | 61 (17) | 140 (45) |
| ts/hono | 0.974 [0.926, 0.991] (3 dropped as calls) | 0.830 [0.757, 0.884] | 115 (3) | 135 (112) |
| ts/trpc | 0.867 [0.621, 0.963] (7 dropped as calls; weakly supported) | 0.302 [0.186, 0.451] | 15 (2) | 43 (13) |
| ts/ky | 0.875 [0.529, 0.978] | 0.700 [0.397, 0.892] | 8 (1) | 10 (7) |
| ts/immer | 1.000 [0.342, 1.000] (1 dropped as calls; weakly supported) | 0.111 [0.031, 0.328] | 2 (0) | 18 (2) |
| ts/h3 | 0.854 [0.716, 0.931] (6 dropped as calls) | 0.633 [0.493, 0.753] | 41 (6) | 49 (31) |
| rust/ripgrep/path_tests_excluded | 1.000 [0.832, 1.000] (+14 lineless ungraded (42% of pairs)) | 0.679 [0.493, 0.821] | 19 (0) | 28 (19) |
| rust/fd/path_tests_excluded | 1.000 [0.610, 1.000] (+6 lineless ungraded (50% of pairs)) | 0.857 [0.487, 0.974] | 6 (0) | 7 (6) |
| rust/bat/path_tests_excluded | 1.000 [0.610, 1.000] (+5 lineless ungraded (45% of pairs)) | 0.857 [0.487, 0.974] | 6 (0) | 7 (6) |
| rust/tokei/path_tests_excluded | 1.000 [0.342, 1.000] (+2 lineless ungraded (50% of pairs)) | 0.667 [0.208, 0.939] | 2 (0) | 3 (2) |
| rust/xsv/path_tests_excluded | no pair graded | 0.000 [0.000, 0.354] | 0 | 7 (0) |
| rust/lidx/path_tests_excluded | 1.000 [0.938, 1.000] (+9 lineless ungraded (13% of pairs)) | 0.935 [0.846, 0.975] | 58 (0) | 62 (58) |
| rust/just/path_tests_excluded | 1.000 [0.954, 1.000] (+13 lineless ungraded (14% of pairs)) | 0.862 [0.778, 0.917] | 80 (0) | 94 (81) |
| rust/nextest/path_tests_excluded | 0.954 [0.845, 0.987] (+9 lineless ungraded (17% of pairs)) | 0.820 [0.692, 0.902] | 43 (2) | 50 (41) |

**Read the notes beside P.** Rust precision is over pairs with a source line only: the lineless module-level USES rows lidx records for `Type::f` paths (`no_evidence_line`) are not graded and are 29-50% of lidx's pairs in several cells, so the Rust 1.000 figures say little about them. Pairs dropped because lidx also has a call are not graded either; cells where they exceed 20% of the graded pairs are weakly supported. The Rust cell variants (`path_tests_excluded`, `no_tests`, `tests`, `_all_features`) duplicate each other for value references (identical or nearly so), so do not count them as independent evidence.

(The `no_tests` and `tests` cells of a repository overlap; the other Go `no_tests` cells, the Rust `no_tests` / `tests` / `all_features` cells and the per-cell lists are in the report.)

Reading:

- **Where lidx says a function is used as a value it is right in Go and TypeScript, and almost always in Rust.** No Go or TypeScript cell has a wrong pair; Rust has two (nextest, a shorthand `VersionInfo { short, long }` whose fields are locals that lidx bound to functions of the same name).
  The TypeScript errors of the earlier draw (a getter read recorded as a reference to the accessor, a call through an `any` cast recorded as a reference, a primitive type name `bigint` bound to a function of that name, a local variable shadowing a function, `typeof f` in a type) were fixed by #474: a property read or write that lands on an accessor is a `USES` edge with the `member` call shape, which this grading leaves out. `same_name_elsewhere` (a name-tier guess binding the reference to another declaration of the same name) is empty in every cell: the resolver refuses bare-name guesses here.
- **Recall is 0.67 to 1.00 depending on how the codebase writes its callbacks.** The edges lidx has are the call-argument, struct-field, object-literal and member forms the extractors were written for. Top missed shapes (distinct pairs over the cells of a language):
  Go (none missed in the cells above).
  TypeScript (6 missed, all in h3): a bare function name read in a position the extractor does not classify (4), a property access read as a value (2).
  Rust (66 missed): a `Type::method` / `Trait::method` path passed as a callback (`.map(Node::tree)`, `.map_err(Error::f)`), 35 (24 of them lidx records only as a module-level USES row with no line and no declaration to pair with, the category `coarse_module_level`; the other 11 not at all), a parser function named in a combinator (`alt((a, b))`, `f.parse_next(..)`, 20), a function coerced in a `let` (4).
- **What is recorded as a call stays a call.** Call wins removes 0 to 14 oracle pairs and 0 to 7 lidx pairs per cell (the largest: zod 14, trpc 13/7, h3 5/6); one `call_as_reference` wrong pair (hono) and no `reference_as_call` miss.
  A wrong lidx reference that coincides with a real call of the same pair is dropped rather than counted (the `ReturnType<typeof f>` pairs of TypeScript tests).
- **Dead code.** Of the declarations `dead_symbols` claims dead although the oracle judged them live, the compiler sees a value reference to 4 of 9 in cobra, 18 of 304 in syft, 4 of 23 in urfavecli, 17 of 113 in nextest, 2 of 14 in xsv and 1 of 27 in just (unreferenced column; TypeScript 0 of 2): the dead claim is a missing reference there. Findings filed as lidx issues: #473 (Go method values on constructor-built locals, package-level `var _ T = f`, returns), #474 (TypeScript object-literal entries, bound members, local callbacks; getter reads recorded as references), #475 (Rust `Type::f` paths: module-level USES rows with no line, or missing); #476 is the harness follow-up for the call oracle's byte columns.

Not graded here: value references of lidx's other languages, `USES` rows to classes and constants (type and value uses), and repowise (its `references` edge is graded with the same matcher in a separate issue).

**Issue #483 and #454 (bare_name on std methods, std paths).** The impact spot-check above was drawn before these fixes: its `lidx_error` pairs (TypeScript 20 of 20, Rust 7 of 20 `bare_name`) are the ones the fixes target. After them, upstream precision over calls (k = 3, pooled) is TypeScript 0.9955 -> 0.9989 (17 -> 4 wrong members) and Rust 0.9750 -> 0.9887 (222 -> 99), with recall within 0.1 point; the call-edge tables are in `results/report.md`. The Rust std-method list (`STD_METHOD_NAMES`) and the TypeScript builtin list (`JS_TS_BUILTIN_METHODS`) carry their per-name true/false edge counts in their doc comments.

Overfitting caveat for #483: the 16 added Rust names were selected on the same eight `path_tests_excluded` cells they are scored on (no held-out repository remains for them), and the evidence is concentrated: `just` holds 72 of the 104 removed false edges. The TypeScript builtin list was chosen on the six TypeScript cells; a repository outside the corpus (got) gave precision 0.979 -> 0.995 with no true edge lost, which is the only unseen check so far.
