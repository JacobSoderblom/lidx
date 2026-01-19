# lidx Feature Roadmap

**Date:** 2026-02-06
**Author:** Staff Engineer Review
**Status:** Proposed -- pending implementation planning

---

## Context and Constraints

**What lidx has today (implemented and working):**
- Tree-sitter AST parsing for 8 languages with symbol/edge extraction
- SQLite graph database with CALLS, IMPORTS, CONTAINS, EXTENDS, IMPLEMENTS, INHERITS, RPC_IMPL edges
- Cross-language XREFs (C#->SQL, C#->Python, Proto->C# via RPC_IMPL)
- Workflow methods: `explain_symbol`, `analyze_diff`, `find_tests_for`, `trace_flow`, `module_map`
- `gather_context` with budget-controlled context assembly (100% utilization at 50KB)
- `analyze_impact` / `analyze_impact_v2` with 4-layer architecture (direct, test, historical, semantic)
- Semantic search via fastembed/ollama (opt-in feature flag)
- Incremental indexing at 20,900 files/sec with stable symbol IDs
- File watching with adaptive debouncing
- Response size caps, token budget parameters, signature-only format
- MCP server (stdio) and JSON-RPC server
- Evaluation score: 3.53/4.0

**What does not work well (known gaps from evaluation):**
- Impact analysis stays within single file (C# short qualname resolution fails cross-file)
- Class-level references return empty (only method-level works)
- Search score differentiation is weak (fuzzy matches all score 3.5)
- `analyze_impact_v2` only has direct layer active; historical/semantic/test layers disabled in practice
- `gather_context` symbol seeds stay in 1 file (search seeds reach 3 files)

**Design principles for this roadmap:**
1. Leverage what is unique: offline deterministic AST graph, cross-language edges, zero external deps
2. Do not duplicate what LLMs do well (writing code, explaining concepts, pattern matching in text)
3. Do not propose embeddings/vector search as a feature (already implemented as opt-in)
4. Focus on compacting context, not expanding it -- LLMs produce better code with 200 lines of *right* context than 10,000 lines of grep
5. Every feature must have a concrete, measurable success metric

---

## Epic 1: Cross-File Impact Resolution

**Priority:** P0 (must have)
**Estimated complexity:** M
**Dependencies:** Existing `analyze_impact`, `analyze_impact_v2`, edge graph, `db::lookup_symbol_id`

### Problem it solves

Impact analysis is the single most requested capability ("what breaks if I change this?"), and lidx already has a 4-layer impact engine. But it produces single-file results because cross-file CALLS edges use short qualnames (`_svc.DeployAsync`) that do not resolve to target symbols stored with full qualnames (`dpb.cs.DataProductService.DeployAsync`). This makes the impact analysis feature appear broken for the exact use case developers care about most: understanding blast radius *across* file boundaries.

The evaluation report confirms this is the largest remaining gap (Impact Analysis grade: B-, the lowest category). The v2 multi-layer engine (historical, test, semantic layers) is architecturally complete but effectively disabled because the direct layer -- which feeds the others -- cannot traverse cross-file edges.

### What it does

1. **Fuzzy qualname resolver in the database layer.** When an exact qualname match fails, fall back to suffix matching (`qualname LIKE '%.' || ?`), then to name-only matching with language filtering. Cache resolved mappings per graph version so the cost is paid once.

2. **Edge-following mode for BFS.** Instead of resolving qualnames to symbol IDs and then looking up edges, optionally follow edges directly by `target_symbol_id` when it is non-null (many CALLS edges already have resolved `target_symbol_id` from indexing). This bypasses qualname resolution entirely for edges that were resolved at index time.

3. **Activate v2 layers.** Once the direct layer crosses files, the test layer (which depends on finding test files that call production code) and historical layer (which depends on finding co-changed symbols across files) become meaningful. Enable them by default in `analyze_impact_v2`.

4. **Cross-file edge resolution during indexing.** At index time, when emitting CALLS edges with a target qualname, attempt to resolve the target to a symbol ID immediately. Store the resolved `target_symbol_id` on the edge. This front-loads the cost so queries are fast.

### Success metric

- `analyze_impact(symbol)` returns symbols from 3+ files for a symbol with known cross-file callers (measured on the dpb test repo: DeployAsync should show DeployerServiceImpl.Deploy from a different file)
- `analyze_impact_v2` returns results from all 4 layers (direct, test, historical, semantic) rather than only direct
- Impact Analysis evaluation grade moves from B- to A-

---

## Epic 2: Compact Architecture Digest

**Priority:** P0 (must have)
**Estimated complexity:** M
**Dependencies:** Existing `module_map`, `repo_overview`, `repo_insights`, symbol graph

### Problem it solves

The #1 finding from market research is that context compaction is the top need. Aider's repo map proves that a compact architecture overview in under 500 tokens lets LLMs reason about code structure without reading thousands of lines. Currently, an LLM using lidx needs to call `repo_overview` + `module_map` + `repo_insights` + multiple `find_symbol` calls to build a mental model of a codebase. That burns 5+ round trips and 20K+ tokens before the LLM can even start answering a question.

lidx already has `module_map` (directory-level aggregation) and `repo_insights` (complexity/duplicates). But neither produces the specific artifact an LLM needs: a single compact text block listing key modules, their public interfaces, and how they connect -- the "table of contents" for the codebase.

### What it does

1. **`repo_map` method.** Returns a single structured text block (under 2KB for a typical repo, hard cap at 10KB) containing:
   - Top-level modules with file counts and dominant language
   - For each module: 3-5 most important symbols (by fan-in, i.e., most-called), showing only `kind`, `name`, and `signature`
   - Inter-module dependency summary: "module A calls module B (47 edges), module C imports module D (12 edges)"
   - Key architectural patterns detected: "gRPC services: 6", "test coverage: 73% of public functions have test callers"

2. **Importance ranking.** Symbols are ranked by fan-in (number of incoming CALLS edges). This surfaces the symbols that matter most -- the ones everything else depends on. This is a deterministic, graph-derived signal that no LLM can compute from raw file reading.

3. **Budget-aware output.** Accept `max_bytes` parameter (default 4000, roughly 1000 tokens). The method fills the budget by including more symbols per module or more modules until the budget is exhausted. At minimum budget, show only module names and edge counts. At maximum budget, include signatures for top symbols.

4. **Stable ordering.** Output is deterministically ordered (modules sorted by symbol count descending, symbols by fan-in descending). Same codebase state produces identical output. This allows LLMs to cache and diff.

### Success metric

- `repo_map()` returns a complete architecture overview in under 2KB for a 50-file repo and under 10KB for a 500-file repo
- An LLM given only the `repo_map` output can correctly answer "which module handles X?" for 80%+ of questions (manual evaluation on 20 questions against the dpb test repo)
- Single call replaces the current 5-call pattern (`repo_overview` + `module_map` + `repo_insights` + `find_symbol` * N)

---

## Epic 3: Git Co-Change Intelligence

**Priority:** P1 (high value)
**Estimated complexity:** L
**Dependencies:** Existing graph versioning, `git log`, impact analysis layers

### Problem it solves

Market research finding #6: git history intelligence is unique and differentiating. No competitor mines co-change patterns from version control history. The historical layer in `analyze_impact_v2` exists architecturally but has no data to work with -- it needs actual git history analysis to populate co-change relationships.

Developers intuitively know that "when file A changes, file B usually needs to change too" -- but this knowledge is tribal and invisible. Making it queryable turns implicit coupling into explicit, actionable intelligence. This is especially valuable for onboarding (new developer changes file A, lidx warns "historically, file B changes 78% of the time when A changes").

### What it does

1. **`git log` mining at index time.** Parse `git log --numstat` to build a co-change matrix: for each commit, record which files changed together. Aggregate into pairwise co-change counts with time decay (recent commits weighted more heavily). Store in a new `co_changes` table: `(file_a, file_b, count, last_seen, confidence)`.

2. **Symbol-level co-change.** Map file-level co-changes to symbol-level by intersecting with the symbol graph: if files A and B co-change, and symbol X is in A and symbol Y is in B, and there is a CALLS edge between them, then X and Y are co-change candidates with boosted confidence.

3. **`co_changes` query method.** Given a symbol or file, return the top co-change partners ranked by confidence. Include the evidence: "changed together in 14 of 18 commits over the last 3 months."

4. **Feed into impact analysis.** The historical layer in `analyze_impact_v2` consumes co-change data to boost confidence for symbols that historically co-change with the target. A symbol with graph distance 2 but high co-change frequency should rank higher than a symbol at distance 1 with no co-change history.

5. **Coupling hotspot detection.** `repo_insights` gains a new section: "implicit coupling hotspots" -- pairs of files/symbols that co-change frequently but have no direct graph edge. These represent hidden dependencies that should either be made explicit (add an edge) or decoupled.

### Success metric

- `co_changes(symbol)` returns 5+ co-change partners with confidence scores for symbols in a repo with 100+ commits
- Impact analysis with historical layer enabled produces measurably different (and more accurate) results than direct-layer-only: predicted files include at least 1 file that graph traversal alone misses but was historically co-changed
- Coupling hotspot detection identifies at least 1 hidden dependency pair per 1000 symbols (validated manually)
- Mining 1000 commits completes in under 10 seconds

---

## Epic 4: Precise Context Budget Assembly

**Priority:** P1 (high value)
**Estimated complexity:** M
**Dependencies:** Existing `gather_context`, `explain_symbol`, symbol graph, search

### Problem it solves

Market research finding #1: context compaction is the top need. The ACE framework targets 40-60% context window utilization. lidx's `gather_context` has improved to 100% budget utilization at 50KB, but it has two problems: (a) symbol seeds stay within 1 file, and (b) it assembles raw file content without structural intelligence -- it does not know which parts of a file are relevant versus boilerplate.

The key insight from the research: LLMs produce better code with 200 lines of *right* context than 10,000 lines of grep. "Right" means: the changed function, its callers (signatures only), its tests (names only), and any cross-language boundaries it touches. This is exactly what the symbol graph can provide, and what raw file reading cannot.

### What it does

1. **Cross-file expansion for symbol seeds.** When a symbol seed is provided, follow outgoing CALLS edges to symbols in other files. Include those symbols' source code (not the whole file) in the context. This addresses the known gap where symbol seeds stay in 1 file.

2. **Structural pruning.** Instead of including entire file ranges, include only the relevant symbol bodies. For a 500-line file where the relevant function is 30 lines, include those 30 lines plus 5 lines of surrounding context (imports at top, class declaration). This could reduce per-file token cost by 5-10x.

3. **Tiered detail levels.** For the seed symbol: full source code. For direct callers/callees: signature + call site evidence snippet. For transitive dependencies: signature only. This mirrors how a human developer would think about context -- full detail on what you are changing, decreasing detail as you move outward.

4. **gather_context v2 mode.** Add a `strategy` parameter: `"file"` (current behavior, include file ranges) or `"symbol"` (new behavior, include symbol bodies with structural pruning). Default to `"symbol"` for symbol seeds and `"file"` for file seeds.

5. **Deduplication by content hash.** When multiple seeds resolve to overlapping file ranges, deduplicate by content. The current deduplication works by file path, but two symbol seeds from the same file should not produce duplicate content.

### Success metric

- `gather_context` with symbol seed and `strategy: "symbol"` returns content from 3+ files (vs current 1 file)
- Token efficiency: for a 50KB budget, the symbol strategy delivers 3x more unique symbols than the file strategy
- Budget utilization stays above 80% with the new strategy
- Context Assembly evaluation grade moves from B+ to A

---

## Epic 5: Change Review Workflow

**Priority:** P1 (high value)
**Estimated complexity:** L
**Dependencies:** Epic 1 (cross-file impact), existing `analyze_diff`, `find_tests_for`, `explain_symbol`

### Problem it solves

Market research finding #4: change impact prediction matters most to developers. 48% of AI-generated code has security vulnerabilities. The question "what breaks if I change this?" is the most valuable question a code intelligence tool can answer.

lidx already has `analyze_diff` (diff-aware impact analysis) and `find_tests_for` (test mapping). But these are separate methods that an LLM must orchestrate. The real workflow is: developer makes changes -> LLM reviews the diff -> LLM needs to understand blast radius, test coverage, and risk in one shot. This is a *workflow*, not a single method.

Additionally, `analyze_diff` currently works from file paths or symbol names, but the most natural input for an LLM reviewing code is the actual git diff text. And the risk assessment is simplistic (high fan-in = high risk).

### What it does

1. **Enhanced diff parsing.** Accept raw `git diff` text as input. Parse unified diff format to extract changed file paths and line ranges. Map changed lines to symbols using the symbol graph's line ranges. Classify symbols as "modified" (existing symbol's body changed), "signature_changed" (signature/interface changed -- higher risk), "added", or "deleted".

2. **Signature change detection.** When a function signature changes (parameters added/removed/reordered, return type changed), flag this as higher risk than body-only changes. Signature changes break callers; body changes usually do not. Use the symbol's `signature` field to diff before/after.

3. **Transitive test coverage.** For each changed symbol, not only find direct test callers but also determine whether the *callers* of the changed symbol have tests. If function A changes, and B calls A, and B has tests, those tests provide indirect coverage. Report coverage as: "direct" (test calls the changed symbol), "indirect" (test calls a caller of the changed symbol), or "uncovered" (no test path exists).

4. **Risk model with concrete factors.** Go beyond fan-in counting. Risk factors include:
   - Signature change on a symbol with 10+ callers: critical
   - Cross-language boundary change (C# function called from Python): high
   - Change to a symbol with no test coverage: medium
   - Change to a symbol that historically co-changes with other files (Epic 3): medium (those other files may need updates too)
   - Change to an interface/trait method: high (all implementors affected)

5. **Review checklist output.** Return a structured `review_checklist` field: a list of specific things the reviewer should verify, generated from the analysis. Example: "Verify callers of `DeployAsync` in `DeployerServiceImpl.cs` handle the new parameter", "Run tests in `test_deploy.py` -- indirect coverage only", "Check Python caller `trigger_deploy.py` for compatibility with signature change".

### Success metric

- `analyze_diff(diff="<git diff text>")` correctly identifies changed symbols and maps them to the graph
- Signature changes are detected and flagged as higher risk than body-only changes
- Risk assessment produces actionable, specific factors (not just "high fan-in")
- Review checklist contains at least 1 concrete, verifiable action item per changed symbol
- End-to-end: LLM receives diff + `analyze_diff` output and can produce a meaningful code review without reading any additional files, for 70%+ of changes (manual evaluation on 10 real diffs)

---

## Epic 6: Cross-Language Data Flow Tracing

**Priority:** P2 (nice to have)
**Estimated complexity:** L
**Dependencies:** Existing `trace_flow`, cross-language XREFs, RPC_IMPL edges

### Problem it solves

Market research finding #5: cross-language/cross-repo intelligence is underserved. lidx already detects cross-language edges (C#->SQL stored procs, C#->Python, Proto->C# via RPC_IMPL). But `trace_flow` currently follows CALLS edges within a single language well; crossing language boundaries is unreliable because edge resolution at boundaries depends on qualname matching conventions that differ per language.

The real value is answering questions like: "trace the request from the gRPC endpoint in the proto definition, through the C# service implementation, to the SQL stored procedure it calls." This is a linear narrative that crosses 3 languages and is impossible to construct from grep or file reading.

### What it does

1. **Language-boundary-aware BFS.** When `trace_flow` encounters an edge that crosses a language boundary (detected by comparing file extensions of source and target), apply language-specific qualname resolution. For Proto->C# (RPC_IMPL), match the proto service method to the C# implementation class. For C#->SQL (XREF), match the stored procedure name. For C#->Python (XREF), match the Python class/function name.

2. **Boundary annotations.** Each hop in the trace that crosses a language boundary gets annotated with the crossing type: "gRPC call (proto -> C#)", "stored procedure invocation (C# -> SQL)", "cross-language reference (C# -> Python)". This tells the LLM *how* the boundary is crossed, not just that it is crossed.

3. **Protocol-aware tracing.** For gRPC services, automatically include the proto message definitions for request/response types at boundary hops. When tracing from a C# gRPC handler, include the proto request message schema so the LLM understands the contract.

4. **Bidirectional cross-language trace.** Support tracing upstream from a SQL stored procedure to find all C# callers, then further upstream to find the gRPC endpoints that trigger those callers. The `direction: "upstream"` mode follows reverse edges across language boundaries.

### Success metric

- `trace_flow(start_qualname="TriggerService.Trigger", direction="downstream")` produces a trace that crosses from Proto to C# to SQL, with correct boundary annotations at each crossing
- Trace includes proto message definitions at gRPC boundaries
- Upstream trace from a SQL stored procedure reaches the gRPC endpoint that triggers it
- Cross-language traces complete in under 500ms for a 5-hop path

---

## Epic 7: Stale Code and Dead Symbol Detection

**Priority:** P2 (nice to have)
**Estimated complexity:** S
**Dependencies:** Existing symbol graph (fan-in/fan-out), git history (Epic 3 optional), test detection

### Problem it solves

Developers and LLMs waste time reading and reasoning about code that is effectively dead -- functions with zero callers, imports that are never used, test files that test deleted functions. The symbol graph already contains the information to detect this, but it is not surfaced as a first-class query.

This is low-hanging fruit: the data already exists in the graph. It requires only aggregation queries, no new indexing.

### What it does

1. **`dead_symbols` method.** Query symbols with zero incoming CALLS/IMPORTS edges (fan-in = 0), excluding entry points (main functions, gRPC handlers, test functions, exported module members). Return ranked by last-modified date (stale + uncalled = highest confidence dead code).

2. **`unused_imports` method.** Find IMPORTS edges where the target symbol has no CALLS edges from the importing file. These are imports that were added but never used, or were used but the usage was later removed.

3. **`orphan_tests` method.** Find test functions (detected via `test_detection.rs`) whose target symbol (the function being tested, inferred from naming convention or CALLS edges) no longer exists in the graph. These are tests for deleted code.

4. **Integration with `repo_insights`.** Add a `staleness` section to `repo_insights` output: count of dead symbols, unused imports, and orphan tests. This gives a quick health check without requiring separate method calls.

### Success metric

- `dead_symbols()` returns symbols with zero fan-in that are not entry points, with false positive rate under 20% (manual validation on 50 results)
- `unused_imports()` correctly identifies imports where the target is never called from the importing file
- `orphan_tests()` identifies test functions whose target no longer exists
- Results are actionable: an LLM or developer can use the output to safely remove dead code

---

## Sequencing and Dependencies

```
Epic 1: Cross-File Impact Resolution (P0, M)
    |
    +-- Unblocks Epic 5 (change review depends on cross-file impact)
    +-- Unblocks Epic 3 historical layer (needs cross-file traversal)
    |
Epic 2: Compact Architecture Digest (P0, M)
    |
    +-- Independent, can start immediately in parallel with Epic 1
    |
Epic 3: Git Co-Change Intelligence (P1, L)
    |
    +-- Soft dependency on Epic 1 (historical layer activation)
    +-- Feeds into Epic 5 (risk model uses co-change data)
    |
Epic 4: Precise Context Budget Assembly (P1, M)
    |
    +-- Depends on Epic 1 (cross-file symbol expansion)
    |
Epic 5: Change Review Workflow (P1, L)
    |
    +-- Depends on Epic 1 (cross-file impact for blast radius)
    +-- Benefits from Epic 3 (co-change data for risk model)
    |
Epic 6: Cross-Language Data Flow Tracing (P2, L)
    |
    +-- Independent, can start after Epic 1
    |
Epic 7: Stale Code and Dead Symbol Detection (P2, S)
    |
    +-- Independent, can start immediately
```

**Recommended execution order:**

| Phase | Epics | Rationale |
|-------|-------|-----------|
| Phase 1 (parallel) | Epic 1 + Epic 2 + Epic 7 | Fix the biggest gap (impact), deliver highest-value new capability (repo_map), grab low-hanging fruit (dead code). All independent. |
| Phase 2 (parallel) | Epic 3 + Epic 4 | Git intelligence feeds the system; context assembly improves LLM experience. Both benefit from Epic 1 being done. |
| Phase 3 (parallel) | Epic 5 + Epic 6 | Full change review workflow and cross-language tracing. Both are compound features that build on everything before them. |

**Total estimated effort:** 2 P0/M + 2 P1/M + 2 P1-P2/L + 1 P2/S = roughly 8-12 weeks of focused development.

---

## What This Roadmap Does NOT Include (and Why)

1. **Multi-repository workspace analysis.** The existing roadmap proposes this as a 22-week epic. It is premature. Single-repo impact analysis does not work cross-file yet (Epic 1). Fix single-repo first. Multi-repo is a 2027 concern.

2. **Collaborative annotations / knowledge graph.** Adds organizational value but does not improve the core code intelligence that differentiates lidx. Defer until the core is strong.

3. **Embedding model improvements.** Semantic search is already implemented as an opt-in feature. Improving embedding quality is incremental tuning, not a feature epic.

4. **IDE integrations.** lidx is protocol-first (MCP/JSON-RPC). IDE plugins are distribution, not product. The protocol is the product.

5. **Real-time streaming / incremental query results.** Nice for UX but does not change the quality of intelligence delivered. lidx queries already complete in under 100ms for most operations.
