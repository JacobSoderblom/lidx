# lidx oracle benchmark: call graph graded by compilers

lidx's own benchmarks (`benchmarks/answers`) score against answer keys we wrote. This one scores lidx's
call graph against the answer a **compiler** gives for the same checkout of a public repository, so the
number cannot be tuned by editing a key:

- **precision**: of the call edges lidx emits (with a resolved in-repo target), how many does the compiler confirm;
- **recall**: of the compiler's call edges, how many does lidx have.

Every figure carries a 95% Wilson interval and the count it is computed over. The code here is our own;
nothing was copied from repowise-bench (which has no licence). repowise's *published numbers* are quoted
below for orientation only.

## Run it

```bash
cargo build --release                       # the lidx binary under test
python3 benchmarks/oracle/run.py            # all 7 repositories, 10 cells, writes benchmarks/oracle/results/
python3 benchmarks/oracle/run.py --only '^ts/'        # a subset (regex over cell ids)
python3 benchmarks/oracle/run.py --binary /path/to/lidx --cache /big/disk/lidx-oracle
python3 benchmarks/oracle/run.py --report-only        # rebuild report.md from results/
python3 -m unittest discover -s benchmarks/oracle -p 'test_*.py'
```

Needs `go` (1.24+; the right toolchain for each corpus module is fetched by Go itself), `node` + `npm`,
`rust-analyzer` with the `scip` subcommand (1.94 used), `git` and Python 3 (stdlib only). The first run
clones the corpus into the cache dir (default `~/.cache/lidx-oracle`, outside the repository) and runs
`npm ci --ignore-scripts` once inside `ts_oracle/`. A full run takes about 15 minutes, dominated by Go's
SSA build of syft, which needs roughly 9 GB of memory with tests included.

Outputs in `results/`: `report.md` (the report), `cells.json` (all numbers), `lists/<cell>.{fn,fp,capture_gap}.json`
(every miss, wrong edge and capture-gap edge with the source locations and a category, for fixing),
`spotcheck.json` / `spotcheck-sample.json` (the hand check, below).

## Corpus

Pinned in `corpus.lock.json` (url, ref, full commit sha). Never committed to this repository.

| Language | Repositories | Compiler | Variants |
|---|---|---|---|
| Go | cobra v1.10.2, gitleaks v8.30.1, syft v1.54.1 | `golang.org/x/tools/go/callgraph/rta` over the type-checked program | with and without test files |
| TypeScript | zod v4.6.6, hono (main, 2026-10-12) | `typescript` 5.9.3 checker API | production code (tests excluded) |
| Rust | ripgrep (master, 2026-10-12), fd v10.5.0 | rust-analyzer 1.94 `scip` | whole workspace |

Corpus repositories are treated as untrusted: nothing from them is built, tested or installed. Go is only
type-checked (`go list` may download modules); TypeScript uses the compiler API without installing the
repository's dependencies, so calls into packages that are not in the repository are simply out of scope;
rust-analyzer runs with build scripts and proc-macro servers disabled.

## Method

**Oracle edges** are distinct (caller declaration, callee declaration) pairs, restricted to declarations
inside the repository. A declaration is identified by file and the line of its *name identifier*, so
decorators, attributes and doc comments above it do not matter.

- *Go* (`go_oracle/`): `packages.Load` + SSA + RTA. Roots are `main`/`init` of main packages; with tests also
  every `Test*`/`Benchmark*`/`Fuzz*`/`Example*`; a library with no `main` package (cobra without tests) uses
  its exported functions and methods. Closures resolve to their enclosing declaration, generic instances to
  their origin, synthetic wrappers are collapsed, range-over-func bodies belong to the enclosing function.
  Only functions RTA proved reachable can be judged as callers.
- *TypeScript* (`ts_oracle/`): every `CallExpression`, `NewExpression`, JSX component element and tagged
  template, resolved with `getResolvedSignature`. The callee is the implementation declaration for overloads;
  when the signature comes from an interface or function type the declaration is the variable or field that
  holds the function. The caller is the nearest named function-like; module-level code is a `<module>`
  caller (line 0). Call sites the compiler cannot resolve are recorded and never held against lidx.
- *Rust* (`rust_oracle/`): rust-analyzer's SCIP index, decoded with a small protobuf reader. A reference to a
  function symbol counts as a call when the identifier is followed by `(` (optionally through a turbofish); a
  function value used elsewhere is not a call. Tuple-struct constructors count; enum-variant constructors are
  not declarations lidx models and their call sites are not judged.

**lidx side** (`match.py`): each repo is indexed with a fresh database (`lidx reindex --repo R --db DB`). We take
CALLS edges (and, separately, CALLS+REFERENCES+USES) whose source and target are in-repo symbols and
deduplicate them to (caller, callee) pairs. lidx symbols are keyed by the same file + name-identifier line,
recovered from the source. A lidx edge matches an oracle edge when both ends land on the same declaration.
Declarations are compared by (file, qualname) so an interface and a const of the same name (TypeScript
declaration merging) or a set of overloads count as one declaration.

**Excluded, and reported rather than dropped:**

| Bucket | Definition | Where it goes |
|---|---|---|
| callee outside the repo | oracle cannot judge | excluded on both sides |
| capture gap | oracle edge whose caller, or callee, is not a symbol lidx indexes (macro-generated Rust test functions, nested functions) | own count and list; kept out of recall; `recall incl. capture gap` puts them back |
| dispatch | interface method call, function-value call, trait method call: the compiler lists *possible* targets | own count; headline recall excludes it, `recall incl. dispatch` includes it |
| ungradable lidx edge | caller file or function not in the analysed program, callee file not analysed, or a call site the compiler left unresolved | counted per reason, not in precision |
| lifted caller | oracle caller is not a symbol lidx indexes (e.g. an object-literal method) | matched against the nearest enclosing declaration lidx does index; counted |

**Classification** of every miss and every wrong edge (`lists/`): FP `external_callee_at_site` (the compiler says the
site calls code outside the repo, so a name-based guess hit an in-repo symbol), `wrong_target`,
`reference_as_call` (the name appears at the site but is not called), `no_call_at_site`, `caller_attribution`,
`closure_callee_at_site`; FN `no_edge_at_site`, `other_call_on_line_missed` (a sibling call on the line was
found), `wrong_target`, `marked_external`, `unresolved`, `recorded_as_uses`, `attributed_to_other_caller`, `dispatch`.

**Precision by `resolution_kind`**: each lidx pair is counted once under its most specific resolution kind
(`exact`, `receiver_type`, `import`, `two_segment`, `inherited`, `bare_name`).

**Hand spot-check**: 20 random FPs and 20 random FNs per language (seeded, drawn from the committed lists)
were read against the source and classified `lidx_error`, `oracle_artefact` or `matching_artefact`
(`spotcheck.py sample` then `merge`; verdicts and a note per item are in `spotcheck-verdicts.json`). The rule above was
changed until the artefact rate was under 10%; the history of that iteration is in the report.

## Results next to repowise

Ours: this repository's main at the time of the report (`results/report.md` has intervals, resolution-kind
tables and categories). repowise: numbers they publish in `docs/BENCHMARKS.md` on repowise main (2026-10-03), "Call graph against a
compiler, five tools" for Go and TypeScript and "Accuracy by language" (rust-analyzer key) for Rust.
Precision / recall. **Do not read the columns as a like-for-like race**: corpus commits, dedup and scoping differ
(for example our TypeScript cells analyse every non-test source file in the repository, benchmarks and docs included).
For recall compare repowise against *recall incl. dispatch*; our headline recall leaves dispatch edges out.

| Cell | oracle n (static / incl. dispatch) | graded lidx n | lidx P [95% CI] | lidx R [95% CI] | lidx R incl. dispatch | repowise P / R |
|---|---:|---:|---|---|---:|---|
| go/cobra/tests | 1527 / 1595 | 1197 | 0.932 [0.917, 0.945] | 0.723 [0.700, 0.745] | 0.700 | 0.995 / 0.705 |
| go/cobra/no_tests | 556 / 581 | 384 | 0.964 [0.940, 0.978] | 0.665 [0.625, 0.703] | 0.637 | not published |
| go/gitleaks/no_tests | 1556 / 1572 | 764 | 0.988 [0.978, 0.994] | 0.485 [0.461, 0.510] | 0.480 | 0.991 / 0.971 |
| go/gitleaks/tests | 1677 / 1705 | 808 | 0.986 [0.976, 0.992] | 0.475 [0.451, 0.499] | 0.467 | 0.989 / 0.939 |
| go/syft/no_tests | 5229 / 7826 | 3741 | 0.891 [0.880, 0.900] | 0.632 [0.619, 0.645] | 0.426 | 0.979 / 0.539 |
| go/syft/tests | 10762 / 23949 | 5815 | 0.885 [0.877, 0.893] | 0.474 [0.465, 0.484] | 0.215 | 0.982 / 0.341 |
| ts/zod | 2532 / 2891 | 2624 | 0.853 [0.838, 0.866] | 0.888 [0.875, 0.900] | 0.819 | 0.995 / 0.796 |
| ts/hono | 860 / 963 | 1145 | 0.715 [0.689, 0.741] | 0.892 [0.869, 0.911] | 0.866 | 0.976 / 0.806 |
| rust/ripgrep | 4716 / 5401 | 4111 | 0.882 [0.872, 0.891] | 0.696 [0.682, 0.709] | 0.671 | 0.852 / 0.532 |
| rust/fd | 513 / 516 | 524 | 0.943 [0.919, 0.960] | 0.957 [0.936, 0.972] | 0.957 | 0.899 / 0.619 |

Repowise rows: cobra without tests is not published; their TypeScript with-tests variants are void; ripgrep and fd come from their per-language table (4,555 and 231 compiler edges). Ours for the same cells are in the first six columns.

