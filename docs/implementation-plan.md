# lidx Feature Implementation Plan

**Date:** 2026-02-06
**Author:** Backend Architect
**Status:** Authoritative implementation plan for Epics 1-7
**Schema Version Baseline:** 9 (current, see `src/db/migrations.rs`)

---

## Table of Contents

1. [Epic 1: Cross-File Impact Resolution](#epic-1-cross-file-impact-resolution)
2. [Epic 2: Compact Architecture Digest](#epic-2-compact-architecture-digest)
3. [Epic 3: Git Co-Change Intelligence](#epic-3-git-co-change-intelligence)
4. [Epic 4: Precise Context Budget Assembly](#epic-4-precise-context-budget-assembly)
5. [Epic 5: Change Review Workflow](#epic-5-change-review-workflow)
6. [Epic 6: Cross-Language Data Flow Tracing](#epic-6-cross-language-data-flow-tracing)
7. [Epic 7: Stale Code and Dead Symbol Detection](#epic-7-stale-code-and-dead-symbol-detection)

---

## Conventions Used in This Document

- **File paths** are relative to the repo root `/Users/jacob/work/lidx/`
- **LOC estimates** are rough (+/- 30%) and include tests
- **PR** = Pull Request. Each numbered step is designed to be a single PR
- **Schema migrations** increment `SCHEMA_VERSION` in `src/db/migrations.rs`

---

## Epic 1: Cross-File Impact Resolution

**Priority:** P0 | **Size:** M | **Blocks:** Epics 3, 4, 5

### Current State

The following already works (implemented 2026-02-06):

- `db.lookup_symbol_id_fuzzy()` in `src/db/mod.rs` (line ~1302): suffix-based qualname matching
- `db.incoming_edges_by_qualname_pattern()` in `src/db/mod.rs` (line ~1419): LIKE-based incoming edge lookup
- Direct layer BFS in `src/impact/layers/direct.rs` calls both methods at query time
- `analyze_impact` upstream now returns 21 results across 10 files on the dpb test repo
- `analyze_impact_v2` direct layer crosses files

What remains:

1. **Index-time edge resolution** -- resolve `target_symbol_id` on CALLS edges during indexing so BFS uses direct ID lookups instead of LIKE queries
2. **Activate test and historical layers** in `analyze_impact_v2` by default
3. **Performance index** on `edges(target_qualname)` for the LIKE fallback

### Architecture Decisions

**Why index-time resolution matters:**
The current query-time LIKE resolution (`target_qualname LIKE '%.DeployAsync'`) works but is O(n) per edge traversal step. For large repos (10K+ symbols), this degrades BFS performance. Index-time resolution writes the resolved `target_symbol_id` once, making every subsequent BFS hop a single integer comparison.

**Where resolution happens:**
In `src/db/mod.rs::insert_edges()` (line ~869). The function already calls `resolve_symbol_id()` with an exact-match lookup. We extend it to fall back to fuzzy matching when exact match fails.

**Backward compatibility:**
The LIKE fallback in the BFS (direct layer) remains active. Index-time resolution is an optimization that accelerates the common case. Edges that cannot be resolved at index time are still traversable via the query-time fallback. This means the feature works incrementally -- old edges are traversed via LIKE, new edges via ID.

### Implementation Steps

#### Step 1.1: Add target_qualname index and fuzzy resolution at insert time

**Files changed:**
- `src/db/migrations.rs` -- Add migration 10: `CREATE INDEX IF NOT EXISTS idx_edges_target_qualname ON edges(target_qualname)` and `CREATE INDEX IF NOT EXISTS idx_symbols_name_kind ON symbols(name, kind)`
- `src/db/mod.rs` -- Modify `insert_edges()` (~line 869) and `insert_edges_batch()` to call `lookup_symbol_id_fuzzy()` when `resolve_symbol_id()` returns None
- `src/db/mod.rs` -- Add `resolve_symbol_id_fuzzy()` helper that wraps the existing lookup_stmt with a fuzzy fallback

**What changes:**
```
// In insert_edges(), after resolve_symbol_id returns None for target:
let target_id = resolve_symbol_id(&edge.target_qualname, symbol_map, &mut lookup_stmt)?
    .or_else(|| {
        // Fuzzy fallback: try suffix match across all symbols
        edge.target_qualname.as_ref().and_then(|qn| {
            self.lookup_symbol_id_fuzzy(qn, None, graph_version).ok().flatten()
        })
    });
```

**Estimated LOC:** ~60
**Risk:** Low. The fuzzy lookup is already tested in query-time path. This just calls it at insert time too.

#### Step 1.2: Activate test and historical layers by default in analyze_impact_v2

**Files changed:**
- `src/impact/types.rs` -- Change `TestConfig::default().enabled` from `false` to `true` (line ~183)
- `src/impact/types.rs` -- Change `HistoricalConfig::default().enabled` from `false` to `true` (line ~205)
- `src/rpc.rs` -- In the `analyze_impact_v2` handler, ensure the config builder respects user opt-in/opt-out params (`enable_test`, `enable_historical`) but defaults to `true`

**What changes:**
The defaults flip from disabled to enabled. The RPC handler already supports `enable_test` and `enable_historical` params for explicit control. Users who want v1 behavior can pass `enable_test: false, enable_historical: false`.

**Estimated LOC:** ~20
**Risk:** Low. The layers already exist and are tested. Enabling them just means they run and contribute results. If a layer fails, the orchestrator's graceful degradation catches the error and continues with other layers.

#### Step 1.3: Batch re-resolution of existing edges

**Files changed:**
- `src/db/mod.rs` -- Add `resolve_null_target_edges()` method that does:
  ```sql
  UPDATE edges SET target_symbol_id = (
      SELECT s.id FROM symbols s
      WHERE s.qualname = edges.target_qualname
      AND s.graph_version = edges.graph_version
      LIMIT 1
  )
  WHERE target_symbol_id IS NULL
  AND target_qualname IS NOT NULL
  ```
  Then a second pass with fuzzy (suffix) matching for remaining NULLs.
- `src/rpc.rs` -- Add optional `resolve_edges: true` param to `reindex` method that triggers the resolution pass after reindexing

**Estimated LOC:** ~80
**Risk:** Medium. The batch UPDATE touches potentially thousands of rows. Must run in a transaction with progress logging. Include a `LIMIT` per batch (1000 rows) with loop to avoid long lock holds.

### Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Fuzzy resolution matches wrong symbol (e.g., two classes with same method name) | Medium | Already mitigated by preferring shortest qualname. Add language filter when file language is known. |
| Batch re-resolution takes too long on large repos | Low | LIMIT-per-batch with WAL mode means reads are not blocked |
| Enabling all layers makes analyze_impact_v2 slower | Low | Parallel execution already implemented in orchestrator. Test layer adds ~50ms, historical ~20ms |

### Testing Strategy

**Unit tests:**
- `src/db/mod.rs` test: insert edge with short qualname, verify `target_symbol_id` is resolved at insert time
- `src/impact/orchestrator.rs` test: verify `MultiLayerConfig::default()` has all three layers enabled

**Integration tests (dpb repo):**
- `analyze_impact_v2(qualname="dpb.cs.DataProductService.DeployAsync", direction="upstream")` returns results from 3+ files
- `analyze_impact_v2` response includes `layers.test.result_count > 0` and `layers.historical.enabled: true`
- Before/after benchmark: measure `analyze_impact_v2` latency to ensure < 500ms

**Success criteria:**
- Impact Analysis evaluation grade moves from B- to A-
- `analyze_impact_v2` returns results from all 3 active layers

---

## Epic 2: Compact Architecture Digest

**Priority:** P0 | **Size:** M | **Blocks:** None

### Architecture Decisions

**New RPC method `repo_map`** returns a single structured text block. This is a new read-only query method, not a modification to existing methods. It composes data from existing DB queries (fan-in ranking, module summary, edge counts) into a compact output.

**Why not extend `repo_overview` or `module_map`:**
- `repo_overview` returns numeric counts, not symbol data
- `module_map` returns directory-level aggregation without symbol ranking
- `repo_map` combines both with fan-in-based importance ranking -- a fundamentally different view

**Budget-aware output:**
The `max_bytes` parameter (default 4000) controls output size. The method fills the budget in priority order:
1. Module list with file counts (always included, ~200 bytes)
2. Inter-module edge summary (always included, ~300 bytes)
3. Top symbols per module by fan-in (fills remaining budget)
4. Architectural patterns (appended if budget remains)

**Data flow:**
```
repo_map(max_bytes) ->
  1. db.module_summary(depth=1)          -> module list
  2. db.module_edges(depth=1)            -> inter-module edges
  3. db.top_fan_in(limit=N)              -> globally ranked symbols
  4. db.count_by_kind()                  -> pattern detection
  -> assemble into text within max_bytes
```

### Implementation Steps

#### Step 2.1: Add `top_fan_in_by_module()` and `count_symbols_by_kind()` to DB layer

**Files changed:**
- `src/db/mod.rs` -- Add two new query methods:

`top_fan_in_by_module()`:
```sql
SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.signature,
       COUNT(e.id) as fan_in,
       SUBSTR(f.path, 1, INSTR(f.path, '/') - 1) as module
FROM symbols s
JOIN files f ON s.file_id = f.id
LEFT JOIN edges e ON e.target_symbol_id = s.id AND e.kind = 'CALLS'
WHERE s.graph_version = ? AND s.kind IN ('function','method','class','struct','interface','service')
GROUP BY s.id
ORDER BY fan_in DESC
```

`count_symbols_by_kind()`:
```sql
SELECT s.kind, COUNT(*) as cnt
FROM symbols s
JOIN files f ON s.file_id = f.id
WHERE s.graph_version = ? AND (f.deleted_version IS NULL OR f.deleted_version > ?)
GROUP BY s.kind ORDER BY cnt DESC
```

**Estimated LOC:** ~100

#### Step 2.2: Implement `repo_map` assembly logic and RPC handler

**Files changed:**
- `src/repo_map.rs` -- New file (~250 lines). Contains:
  - `RepoMapConfig` struct: `max_bytes`, `languages`, `graph_version`
  - `RepoMapResult` struct: `text: String`, `modules: usize`, `symbols: usize`, `bytes: usize`
  - `build_repo_map(db, config) -> Result<RepoMapResult>` function:
    1. Query module summary
    2. Query module edges
    3. Query top fan-in symbols grouped by module
    4. Query kind counts for pattern detection
    5. Assemble text within budget
- `src/model.rs` -- Add `RepoMapResult` struct
- `src/rpc.rs` -- Add `"repo_map"` match arm in `handle_method()` dispatch table (~line 1527). Add `RepoMapParams` struct. Add entry to `METHOD_LIST` and `METHODS_DOCS`.
- `src/mcp.rs` -- Add `repo_map` to MCP tool list with parameter descriptions
- `src/lib.rs` or `src/main.rs` -- Add `mod repo_map;`

**Assembly algorithm (budget-aware):**
```
fn build_repo_map(db, config) -> Result<RepoMapResult> {
    let mut out = String::new();
    let budget = config.max_bytes;

    // Phase 1: Module header (always fits)
    let modules = db.module_summary(1, ...)?;
    for m in &modules {
        writeln!(out, "## {path} ({file_count} files, {symbol_count} symbols, {language})");
    }

    // Phase 2: Inter-module edges
    let edges = db.module_edges(1, ...)?;
    writeln!(out, "\n## Dependencies");
    for e in &edges {
        writeln!(out, "  {src} -> {dst} ({calls} calls, {imports} imports)");
    }

    // Phase 3: Top symbols per module (fill remaining budget)
    let fan_in_symbols = db.top_fan_in_by_module(...)?;
    let symbols_by_module: HashMap<String, Vec<_>> = group_by_module(fan_in_symbols);
    for (module, symbols) in symbols_by_module {
        for sym in symbols.iter().take(5) {
            let line = format!("  - {kind} {name}: {signature} (fan-in: {count})");
            if out.len() + line.len() > budget { break; }
            out.push_str(&line);
        }
    }

    // Phase 4: Patterns (if budget remains)
    let kinds = db.count_symbols_by_kind(...)?;
    let services = kinds.get("service").unwrap_or(&0);
    let test_coverage = compute_test_coverage_ratio(db, ...)?;
    if out.len() + 100 < budget {
        writeln!(out, "\n## Patterns");
        writeln!(out, "  gRPC services: {services}");
        writeln!(out, "  Test coverage: {test_coverage}% of public functions");
    }

    Ok(RepoMapResult { text: out, ... })
}
```

**Estimated LOC:** ~350

#### Step 2.3: Add `next_hops` and format parameter

**Files changed:**
- `src/repo_map.rs` -- Add `next_hops` generation: for each module, suggest `find_symbol(query=module_path)` and `module_map(path=module_path, depth=2)`
- `src/rpc.rs` -- Support `format: "compact"` in `RepoMapParams` that omits signatures

**Estimated LOC:** ~60

### Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Fan-in query slow on large repos (requires counting all edges per symbol) | Medium | Pre-compute fan-in counts during indexing (store in symbol_metrics). Fallback: query-time with timeout. |
| Output format not useful for LLMs | Medium | Test with real LLM evaluation on 20 questions against dpb repo. Iterate on format. |
| Budget overflow (module list alone exceeds max_bytes) | Low | Module list for a 500-file repo is ~2KB. Default budget is 4KB. Handle by truncating module list if needed. |

### Testing Strategy

**Unit tests:**
- `src/repo_map.rs` test: verify budget is respected (output.len() <= max_bytes)
- Test deterministic ordering: same input produces identical output
- Test minimum budget (512 bytes): only module names and edge counts

**Integration tests (dpb repo):**
- `repo_map()` returns valid output with modules, symbols, and dependencies
- `repo_map(max_bytes=1000)` returns output under 1000 bytes
- `repo_map(max_bytes=10000)` returns output with signatures for top symbols
- Compare output against manual validation: "which module handles deployment?" answerable from output

**Success criteria:**
- Single call returns complete architecture overview
- Under 2KB for 50-file repo, under 10KB for 500-file repo
- LLM can answer 80%+ of "which module handles X?" questions from output alone

---

## Epic 3: Git Co-Change Intelligence

**Priority:** P1 | **Size:** L | **Soft dependency on:** Epic 1

### Architecture Decisions

**New `co_changes` table:**
This is the first feature requiring external data (git history). Design decision: mine `git log` output into a dedicated SQLite table rather than querying git on every request. This gives us:
- Sub-millisecond query latency
- Ability to join co-change data with the symbol graph
- Persistence across sessions (mine once, query many times)

**Schema (migration 10 or 11, depending on Epic 1 ordering):**
```sql
CREATE TABLE IF NOT EXISTS co_changes (
    id INTEGER PRIMARY KEY,
    file_a TEXT NOT NULL,
    file_b TEXT NOT NULL,
    co_change_count INTEGER NOT NULL DEFAULT 0,
    total_commits_a INTEGER NOT NULL DEFAULT 0,
    total_commits_b INTEGER NOT NULL DEFAULT 0,
    confidence REAL NOT NULL DEFAULT 0.0,
    last_commit_sha TEXT,
    last_commit_ts INTEGER,
    mined_at INTEGER NOT NULL,
    UNIQUE(file_a, file_b)
);

CREATE INDEX IF NOT EXISTS idx_co_changes_file_a ON co_changes(file_a);
CREATE INDEX IF NOT EXISTS idx_co_changes_file_b ON co_changes(file_b);
CREATE INDEX IF NOT EXISTS idx_co_changes_confidence ON co_changes(confidence DESC);
```

**Why file-level, not symbol-level:**
Git log gives us file-level diffs. Symbol-level co-change requires mapping changed lines to symbols, which adds complexity and requires the file to be parseable at every commit. File-level is:
- Cheap to compute (parse `git log --numstat`)
- Sufficient for the historical layer (symbol-level boosting is done by intersecting file-level co-changes with graph edges)
- Correct for the most common use case (files that change together)

Symbol-level boosting happens at query time: if files A and B co-change, and symbol X is in A, and symbol Y is in B, and there is a CALLS edge X->Y, then Y gets boosted confidence in the historical layer. This requires no additional table -- just a JOIN.

**Time decay:**
Recent commits are more valuable. Apply exponential decay: `weight = exp(-age_days / half_life)` where `half_life = 90 days`. This means a commit from 6 months ago has ~12% the weight of a commit from today. The weighted count replaces the raw count in the confidence calculation.

**New RPC method `co_changes`:**
Returns co-change partners for a given file or symbol, ranked by confidence.

### Implementation Steps

#### Step 3.1: Add `co_changes` table and git log mining

**Files changed:**
- `src/db/migrations.rs` -- Add migration (next version): `co_changes` table with indexes
- `src/db/mod.rs` -- Add methods:
  - `insert_co_changes_batch(entries: &[CoChangeEntry]) -> Result<usize>` -- bulk upsert
  - `co_changes_for_file(path, limit, min_confidence) -> Result<Vec<CoChangeEntry>>` -- query
  - `co_changes_for_files(paths, limit, min_confidence) -> Result<Vec<CoChangeEntry>>` -- batch query
  - `clear_co_changes() -> Result<()>` -- for re-mining
- `src/git_mining.rs` -- New file (~300 lines). Contains:
  - `GitMiner` struct: `repo_root`, `max_commits`, `since_date`
  - `mine_co_changes(repo_root, max_commits, since_days) -> Result<Vec<FileCoChange>>` function:
    1. Run `git log --numstat --format="%H %at" --since={since} -n {max_commits}` via `std::process::Command`
    2. Parse output: for each commit, collect changed files
    3. For each pair of files in the same commit, increment co-change count
    4. Apply time decay weighting
    5. Compute confidence = weighted_co_changes / min(total_a, total_b)
  - `CoChangeEntry` struct: `file_a, file_b, co_change_count, confidence, last_commit_sha, last_commit_ts`

**Design detail -- pairwise counting:**
For a commit touching files [A, B, C], emit pairs: (A,B), (A,C), (B,C). This is O(k^2) per commit where k is files changed. For typical commits (k < 20), this is negligible. For merge commits (k > 100), skip them to avoid noise.

**Estimated LOC:** ~400

#### Step 3.2: Wire mining into reindex and add `co_changes` RPC method

**Files changed:**
- `src/rpc.rs` -- Add `"co_changes"` match arm in `handle_method()`. Add `CoChangesParams` struct: `{path|paths, qualname, limit, min_confidence, graph_version}`. When `qualname` is provided, resolve to file path first, then query co-changes for that file.
- `src/rpc.rs` -- In `"reindex"` handler, add optional `mine_git: true` param that triggers `mine_co_changes()` after reindexing
- `src/mcp.rs` -- Add `co_changes` to MCP tool list
- `src/rpc.rs` -- Add to `METHOD_LIST` and `METHODS_DOCS`

**Estimated LOC:** ~150

#### Step 3.3: Feed co-change data into historical impact layer

**Files changed:**
- `src/impact/layers/historical.rs` -- Replace the current graph-version-based co-change mining with DB-backed co-change lookup:
  - `find_co_changes()` now queries `co_changes` table instead of computing co-changes from symbol versions
  - Map file-level co-changes to symbol-level by JOINing with symbols table
  - Boost confidence for co-change partners that also have graph edges
  - Return `ImpactSource::CoChange` evidence with actual git data (commit count, last timestamp)

**What changes from current implementation:**
The existing `HistoricalImpactLayer::find_co_changes()` (line ~170 in historical.rs) currently uses `get_changed_symbols_between_versions()` which compares graph versions. This is a proxy for git history but misses actual commit-level co-change patterns. The new implementation reads from the pre-computed `co_changes` table, which is both faster and more accurate.

**Estimated LOC:** ~200

#### Step 3.4: Add coupling hotspot detection to repo_insights

**Files changed:**
- `src/db/mod.rs` -- Add `coupling_hotspots(limit, min_confidence) -> Result<Vec<CouplingHotspot>>`: query co_changes pairs with high confidence but no direct graph edge between files
  ```sql
  SELECT cc.file_a, cc.file_b, cc.confidence, cc.co_change_count
  FROM co_changes cc
  WHERE cc.confidence > ?
    AND NOT EXISTS (
      SELECT 1 FROM edges e
      JOIN files fa ON e.file_id = fa.id
      JOIN symbols s ON e.target_symbol_id = s.id
      JOIN files fb ON s.file_id = fb.id
      WHERE fa.path = cc.file_a AND fb.path = cc.file_b
    )
  ORDER BY cc.confidence DESC
  LIMIT ?
  ```
- `src/model.rs` -- Add `CouplingHotspot` struct: `file_a, file_b, confidence, co_change_count`
- `src/rpc.rs` -- In `"repo_insights"` handler, add `coupling_hotspots` field to response

**Estimated LOC:** ~100

### Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| `git log` command fails (not a git repo, shallow clone) | Medium | Detect non-git repos before mining. Log warning and skip. Historical layer returns empty gracefully. |
| Pairwise counting explodes for large commits | Low | Skip commits touching > 50 files. Skip merge commits. |
| Mining 10K commits takes too long | Low | `git log --numstat` is fast (~2s for 10K commits). Parsing is linear. Target: < 10s for 1000 commits. |
| Co-change data stale after many file renames | Medium | Use file paths as-is. When files are renamed, the old path's co-changes become irrelevant naturally. Re-mine periodically. |

### Testing Strategy

**Unit tests:**
- `src/git_mining.rs` test: parse known git log output, verify co-change counts
- `src/git_mining.rs` test: verify time decay weighting
- `src/db/mod.rs` test: insert and query co_changes table

**Integration tests (dpb repo):**
- `mine_co_changes(repo_root, 500, 365)` returns non-empty results
- `co_changes(path="src/DataProductService.cs")` returns 5+ co-change partners
- `analyze_impact_v2` with historical layer returns results with `CoChange` evidence source
- Mining 500 commits completes in < 5 seconds

**Success criteria:**
- `co_changes(symbol)` returns 5+ partners with confidence scores
- Historical layer produces measurably different results than direct-only
- Coupling hotspot detection identifies at least 1 hidden dependency

---

## Epic 4: Precise Context Budget Assembly

**Priority:** P1 | **Size:** M | **Depends on:** Epic 1

### Architecture Decisions

**Symbol-level gather strategy:**
The key insight is that for a 500-line file where the relevant function is 30 lines, including all 500 lines wastes 94% of the budget. The new `"symbol"` strategy includes only symbol bodies with surrounding context, achieving 5-10x better token efficiency.

**Tiered detail levels:**
- **Tier 0 (seed):** Full source code of the symbol
- **Tier 1 (direct callers/callees):** Signature + evidence snippet (the call site line)
- **Tier 2 (transitive):** Signature only

This mirrors how a developer thinks: full detail on what you are changing, decreasing detail outward.

**Extending existing GatherConfig, not replacing it:**
Add a `strategy` field to `GatherConfig` (default: `"file"`). When `strategy: "symbol"`, the `collect_content()` function uses symbol bodies instead of file ranges. This preserves backward compatibility -- existing callers get unchanged behavior.

### Implementation Steps

#### Step 4.1: Add symbol-body extraction to gather_context

**Files changed:**
- `src/gather_context.rs` -- Add `strategy: Option<String>` to `GatherConfig` (line ~12). Default to `"file"`.
- `src/gather_context.rs` -- Add `collect_symbol_content()` function (~150 lines):
  1. For each resolved symbol seed, read only `start_byte..end_byte` from the file
  2. Prepend file header (imports, class declaration) as context (first 10 lines of file, capped at 500 bytes)
  3. Track byte usage per tier:
     - Tier 0: full body (seed symbol)
     - Tier 1: signature + call site evidence from edge
     - Tier 2: signature only
  4. Cross-file expansion: follow outgoing CALLS edges to symbols in other files, include those at Tier 1

**Detail on cross-file expansion:**
```
fn expand_symbol_cross_file(db, symbol_id, graph_version, budget_remaining) -> Vec<ContextItem> {
    let edges = db.edges_for_symbol(symbol_id, None, graph_version)?;
    let mut items = Vec::new();
    for edge in edges {
        if edge.kind == "CALLS" {
            if let Some(target_id) = edge.target_symbol_id
                .or_else(|| edge.target_qualname.as_ref()
                    .and_then(|qn| db.lookup_symbol_id_fuzzy(qn, None, graph_version).ok().flatten()))
            {
                if let Some(target_sym) = db.get_symbol_by_id(target_id)? {
                    // Tier 1: signature + evidence
                    let content = format_tier1(&target_sym, &edge);
                    if items_total_bytes + content.len() > budget_remaining { break; }
                    items.push(ContextItem { ... });
                }
            }
        }
    }
    items
}
```

**Estimated LOC:** ~250

#### Step 4.2: Add tiered detail rendering

**Files changed:**
- `src/gather_context.rs` -- Add rendering functions:
  - `format_tier0(symbol, file_content) -> String` -- full source body
  - `format_tier1(symbol, edge) -> String` -- signature + evidence snippet
  - `format_tier2(symbol) -> String` -- signature only
- `src/gather_context.rs` -- Modify `collect_content()` to dispatch to `collect_symbol_content()` when `strategy == "symbol"`

**Output format for Tier 1:**
```
// File: src/services/deploy.cs (caller)
public async Task<DeployResult> Deploy(DeployRequest request)
  // calls: DeployAsync at line 47
```

**Estimated LOC:** ~120

#### Step 4.3: Wire strategy parameter through RPC

**Files changed:**
- `src/rpc.rs` -- Add `strategy: Option<String>` to `GatherContextParams`. Pass through to `GatherConfig`. Default: `"symbol"` when all seeds are symbol seeds, `"file"` when any seed is a file seed.
- `src/rpc.rs` -- Update method docs for `gather_context` to document the new parameter

**Estimated LOC:** ~30

#### Step 4.4: Content-hash deduplication

**Files changed:**
- `src/gather_context.rs` -- Modify `DeduplicationTracker` to also track by content hash (blake3 of content bytes) in addition to byte range. When two symbol seeds from the same file produce overlapping content, deduplicate by hash.

**Estimated LOC:** ~40

### Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Symbol body extraction reads wrong bytes (off-by-one in start_byte/end_byte) | Medium | The byte offsets come from tree-sitter AST, which is authoritative. Add assertion: extracted content starts with expected keyword (fn, def, class). |
| Cross-file expansion blows budget on one seed | Medium | Cap cross-file expansion at 30% of remaining budget per seed. Move to next seed if budget exhausted. |
| Tier rendering format confuses LLMs | Low | Test with actual LLM. The format is standard: file header + code. |

### Testing Strategy

**Unit tests:**
- `src/gather_context.rs` test: `strategy: "symbol"` returns symbol bodies, not full files
- Test tier rendering: verify Tier 0 has full source, Tier 1 has signature + evidence, Tier 2 has signature only
- Test budget compliance: total output <= max_bytes
- Test cross-file expansion: symbol seed expands to callers/callees in other files

**Integration tests (dpb repo):**
- `gather_context(seeds=[{qualname: "dpb.cs.DataProductService.DeployAsync"}], strategy: "symbol")` returns content from 3+ files
- Compare token count: `strategy: "symbol"` delivers 3x more unique symbols than `strategy: "file"` for same budget
- Budget utilization stays above 80%

**Success criteria:**
- Cross-file expansion works for symbol seeds
- 3x improvement in unique symbols per budget unit
- Budget utilization >= 80%

---

## Epic 5: Change Review Workflow

**Priority:** P1 | **Size:** L | **Depends on:** Epic 1, benefits from Epic 3

### Architecture Decisions

**Enhance `analyze_diff`, not create new method:**
The existing `analyze_diff` method already accepts `diff` text and `paths`. We enhance it with signature detection, transitive test coverage, a richer risk model, and review checklist output. This avoids method proliferation and preserves backward compatibility.

**Signature change detection:**
Compare the `signature` field of symbols before and after the diff. The `signature` field is stored in the `symbols` table and contains the function/method signature as extracted by tree-sitter. When a symbol's signature changes between the old and new version, it is flagged as a signature change (higher risk than body-only changes).

**How to get "before" signatures:**
The diff provides changed line ranges. For each changed symbol:
1. Look up the symbol by qualname at the current graph version (this is the "after" state)
2. Look up the symbol by stable_id at the previous graph version (this is the "before" state)
3. Compare signatures

If there is no previous graph version (first index), all changes are treated as "added".

**Transitive test coverage:**
For each changed symbol S:
1. Direct coverage: find tests that call S directly (`find_tests_for(S)`)
2. Indirect coverage: find callers of S (symbols that call S), then find tests that call those callers
3. Uncovered: no test path exists (neither direct nor indirect)

This reuses the existing `find_tests_for` logic with an additional hop.

**Risk model factors:**
Each factor has a severity and a human-readable description. Factors are:
- `signature_change_high_fanin`: severity=critical if signature changed AND fan-in > 10
- `cross_language_boundary`: severity=high if changed symbol has cross-language callers
- `no_test_coverage`: severity=medium if no direct or indirect test path
- `high_co_change_missing`: severity=medium if co-change partner not in diff (requires Epic 3)
- `interface_change`: severity=high if symbol kind is interface/trait and body changed

### Implementation Steps

#### Step 5.1: Add unified diff parser with symbol mapping

**Files changed:**
- `src/diff_parser.rs` -- New file (~200 lines). Contains:
  - `parse_unified_diff(diff_text: &str) -> Vec<DiffHunk>` -- parse `git diff` format
  - `DiffHunk` struct: `file_path, old_start, old_count, new_start, new_count, lines: Vec<DiffLine>`
  - `DiffLine` struct: `kind: Added|Removed|Context, content: String, line_no: usize`
  - `map_hunks_to_symbols(db, hunks, graph_version) -> Vec<ChangedSymbolDetail>` -- for each changed line range, find enclosing symbol via `db.enclosing_symbol_for_line()`
  - `ChangedSymbolDetail` struct: `symbol, change_type: "modified"|"signature_changed"|"added"|"deleted", old_signature: Option<String>, new_signature: Option<String>`

**Estimated LOC:** ~250

#### Step 5.2: Add signature change detection

**Files changed:**
- `src/diff_parser.rs` -- Add `detect_signature_changes()`:
  1. For each modified symbol, look up previous graph version's symbol by stable_id
  2. Compare `signature` fields
  3. If different, classify as `signature_changed`
- `src/db/mod.rs` -- Add `get_symbol_by_stable_id_at_version(stable_id, graph_version) -> Result<Option<Symbol>>`:
  ```sql
  SELECT ... FROM symbols s JOIN files f ON s.file_id = f.id
  WHERE s.stable_id = ? AND s.graph_version = ?
  ```

**Estimated LOC:** ~100

#### Step 5.3: Add transitive test coverage analysis

**Files changed:**
- `src/rpc.rs` -- In `analyze_diff` handler, after collecting changed symbols:
  1. For each changed symbol, call existing `find_tests_for` logic (direct tests)
  2. For indirect coverage: find callers of changed symbol, then find tests for each caller
  3. Classify coverage: `"direct"` (test calls changed symbol), `"indirect"` (test calls caller of changed symbol), `"uncovered"` (no test path)
- `src/model.rs` -- `TestCoverageEntry` already has the right shape. Add `coverage_type: "direct"|"indirect"` to `TestRef`.

**Estimated LOC:** ~120

#### Step 5.4: Implement risk model with concrete factors

**Files changed:**
- `src/risk_model.rs` -- New file (~150 lines). Contains:
  - `assess_risk(changed_symbols: &[ChangedSymbolDetail], db, graph_version) -> RiskAssessment`
  - Factor calculations:
    - Signature change + fan-in > 10: critical
    - Cross-language callers (check if any caller is in a different language): high
    - No test coverage: medium
    - Interface/trait modification: high
    - Co-change partner not in diff (if co_changes table exists): medium
  - Overall risk level: `"critical"` if any critical factor, `"high"` if any high factor, `"medium"` if any medium, `"low"` otherwise
- `src/model.rs` -- `RiskAssessment` and `RiskFactor` already exist and have the right shape

**Estimated LOC:** ~180

#### Step 5.5: Add review checklist generation

**Files changed:**
- `src/risk_model.rs` -- Add `generate_review_checklist(changed_symbols, risk, test_coverage) -> Vec<String>`:
  For each changed symbol with notable risk:
  - Signature changed + callers: "Verify callers of `{qualname}` in `{caller_files}` handle the new parameter"
  - No test coverage: "Add tests for `{qualname}` -- currently uncovered"
  - Cross-language: "Check {language} caller `{caller_qualname}` in `{file}` for compatibility"
  - Co-change partner not in diff: "Consider updating `{partner_file}` -- historically changes with `{file}` ({N}% of the time)"
- `src/model.rs` -- Add `review_checklist: Option<Vec<String>>` to `AnalyzeDiffResult`
- `src/rpc.rs` -- Wire checklist into `analyze_diff` response

**Estimated LOC:** ~120

### Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Unified diff parsing is fragile | Medium | Use well-tested regex patterns. Handle malformed diffs gracefully (skip unparseable hunks). |
| Signature comparison is too strict (formatting changes trigger false positives) | Medium | Normalize signatures before comparison: strip whitespace, normalize type aliases. |
| Transitive test coverage is too slow (N callers * M tests per caller) | Low | Cap at 50 callers, 10 tests per caller. Total test lookup limited to 500 symbols. |
| Review checklist is too generic to be useful | Medium | Each checklist item references specific files, qualnames, and line numbers. Test with real diffs. |

### Testing Strategy

**Unit tests:**
- `src/diff_parser.rs` test: parse real `git diff` output, verify hunks and line ranges
- `src/diff_parser.rs` test: map hunks to symbols correctly
- `src/risk_model.rs` test: signature change + high fan-in produces critical risk
- `src/risk_model.rs` test: no test coverage produces medium risk

**Integration tests (dpb repo):**
- `analyze_diff(diff="<real git diff>")` returns changed symbols with correct classification
- Signature changes are detected when function parameters change
- Review checklist contains at least 1 item per changed symbol with callers
- Risk factors reference specific files and qualnames

**Success criteria:**
- Signature changes detected and flagged as higher risk
- Risk assessment produces actionable, specific factors
- Review checklist has at least 1 concrete action item per changed symbol
- LLM can produce meaningful review from diff + analyze_diff output alone

---

## Epic 6: Cross-Language Data Flow Tracing

**Priority:** P2 | **Size:** L | **Depends on:** Epic 1

### Architecture Decisions

**Enhance existing `trace_flow`, not create new method:**
The current `trace_flow` in `src/rpc.rs` already does BFS with `TraceHop` output that includes `cross_language: bool`. The gap is that cross-language edge resolution is unreliable. This epic improves resolution at language boundaries and adds boundary annotations.

**Language boundary detection:**
Compare the file extension (or language field) of source and target symbols. When they differ, the hop is a cross-language boundary. This information is already available: `Symbol.file_path` contains the extension, and the `files` table has a `language` column.

**RPC_IMPL edges are the primary cross-language connector:**
Proto -> C# is mediated by RPC_IMPL edges (emitted by `src/indexer/proto.rs`). C# -> SQL is mediated by XREF edges (emitted by `src/indexer/xref.rs`). The BFS must follow these edge kinds specifically at boundaries.

**Protocol-aware context:**
When crossing a gRPC boundary (Proto -> C#), include the proto message definitions for request/response types. This means: at the boundary hop, look up the proto service definition and its request/response message types, include their signatures in the trace output.

### Implementation Steps

#### Step 6.1: Add language-boundary resolution to trace_flow BFS

**Files changed:**
- `src/rpc.rs` -- In the `trace_flow` handler (~3000+ lines in), modify the BFS loop:
  1. When following an edge, detect language boundary: `source_file.language != target_file.language`
  2. At boundaries, expand edge kind filter to include `RPC_IMPL`, `XREF`, `IMPLEMENTS`
  3. For RPC_IMPL edges: resolve target via proto service name -> C# implementation class
  4. For XREF edges: resolve target via cross-language qualname matching (already in `xref.rs`)
- `src/model.rs` -- Add `boundary_type: Option<String>` to `TraceHop`: `"grpc"`, `"stored_procedure"`, `"cross_language_ref"`

**Estimated LOC:** ~150

#### Step 6.2: Add boundary annotations to TraceHop

**Files changed:**
- `src/model.rs` -- Add fields to `TraceHop`:
  - `boundary_type: Option<String>` -- "grpc", "stored_procedure", "xref"
  - `boundary_detail: Option<String>` -- human-readable description
  - `protocol_context: Option<Value>` -- proto message definitions at gRPC boundaries
- `src/rpc.rs` -- In trace_flow BFS, when a boundary is detected:
  ```rust
  if is_boundary {
      hop.boundary_type = Some(detect_boundary_type(&edge, &source_sym, &target_sym));
      hop.boundary_detail = Some(format!(
          "{} ({} -> {})",
          boundary_type, source_sym.file_path.extension(), target_sym.file_path.extension()
      ));
  }
  ```

**Estimated LOC:** ~80

#### Step 6.3: Add protocol context at gRPC boundaries

**Files changed:**
- `src/rpc.rs` -- When boundary_type is "grpc", look up the proto service definition:
  1. Find the RPC_IMPL edge connecting proto method to C# implementation
  2. Find the proto service symbol (parent of the proto method)
  3. Extract request/response message type names from the proto method's detail field
  4. Look up those message types as symbols
  5. Include their signatures in `protocol_context`
- `src/db/mod.rs` -- Add `get_contained_symbols(parent_id, graph_version) -> Result<Vec<Symbol>>` for finding proto message fields

**Estimated LOC:** ~120

#### Step 6.4: Support bidirectional cross-language tracing

**Files changed:**
- `src/rpc.rs` -- Ensure `direction: "upstream"` correctly follows reverse edges across language boundaries. Current implementation follows `target_symbol_id -> source_symbol_id` for upstream. For cross-language edges where `target_symbol_id` may be NULL, use qualname-based resolution (already implemented in Epic 1).

**Estimated LOC:** ~60

### Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Proto -> C# resolution fails (proto method names don't match C# implementation names) | Medium | RPC_IMPL edges are emitted during indexing with explicit qualname mapping. If edge exists, resolution works. If edge is missing, the proto extractor needs improvement (separate issue). |
| Protocol context lookup adds latency | Low | Only triggered at boundary hops. Typically 1-2 boundaries per trace. Each lookup is a single symbol query. |
| Upstream tracing from SQL is unreliable | Medium | SQL stored procedure names must match C# XREF qualnames. This depends on xref.rs quality. Log warnings when resolution fails. |

### Testing Strategy

**Unit tests:**
- Language boundary detection: file with `.cs` -> file with `.sql` = boundary
- Boundary type classification: RPC_IMPL edge = "grpc", XREF edge to .sql = "stored_procedure"

**Integration tests (dpb repo):**
- `trace_flow(start_qualname="TriggerService.Trigger", direction="downstream")` crosses from Proto to C#
- Trace includes `boundary_type: "grpc"` at the Proto -> C# hop
- `trace_flow(start_qualname="<sql_stored_proc>", direction="upstream")` reaches the C# caller
- Cross-language trace completes in < 500ms

**Success criteria:**
- Traces cross 2+ language boundaries with correct annotations
- Protocol context included at gRPC boundaries
- Upstream trace from SQL reaches gRPC endpoint

---

## Epic 7: Stale Code and Dead Symbol Detection

**Priority:** P2 | **Size:** S | **Blocks:** None

### Architecture Decisions

**Three new RPC methods (`dead_symbols`, `unused_imports`, `orphan_tests`) rather than one combined method:**
Each serves a distinct use case and has different filtering needs. Combining them would make the parameter surface confusing.

**No new tables required:**
All data is already in the symbol graph. These are aggregation queries over existing tables:
- `dead_symbols`: symbols with zero incoming CALLS edges, filtered by entry point exclusion
- `unused_imports`: IMPORTS edges where the target has no CALLS from the same file
- `orphan_tests`: test symbols whose inferred target no longer exists

**Entry point exclusion list for dead_symbols:**
Symbols that are "entry points" and should not be flagged as dead:
- `kind IN ('module', 'package', 'namespace')` -- structural, not callable
- Functions named `main`, `__init__`, `setup`, `teardown`
- gRPC handlers (symbols with incoming RPC_IMPL edges)
- Test functions (symbols matching `is_test_symbol()`)
- Exported module members (symbols that are IMPORTS targets from other files)

### Implementation Steps

#### Step 7.1: Add dead_symbols query and RPC method

**Files changed:**
- `src/db/mod.rs` -- Add `dead_symbols(limit, languages, paths, graph_version) -> Result<Vec<Symbol>>`:
  ```sql
  SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
         s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
         s.graph_version, s.commit_sha, s.stable_id
  FROM symbols s
  JOIN files f ON s.file_id = f.id
  WHERE s.graph_version = ?
    AND (f.deleted_version IS NULL OR f.deleted_version > ?)
    AND s.kind IN ('function', 'method', 'class', 'struct')
    AND s.name NOT IN ('main', '__init__', 'setup', 'teardown', 'configure', 'register')
    AND s.id NOT IN (
      SELECT DISTINCT e.target_symbol_id FROM edges e
      WHERE e.target_symbol_id IS NOT NULL
        AND e.kind IN ('CALLS', 'IMPORTS', 'RPC_IMPL', 'IMPLEMENTS', 'EXTENDS')
        AND e.graph_version = ?
    )
    AND s.id NOT IN (
      SELECT DISTINCT e.target_symbol_id FROM edges e
      WHERE e.target_symbol_id IS NOT NULL
        AND e.kind = 'IMPORTS'
        AND e.graph_version = ?
        AND e.file_id != s.file_id
    )
  ORDER BY s.qualname
  LIMIT ?
  ```
- `src/rpc.rs` -- Add `"dead_symbols"` match arm. Add `DeadSymbolsParams` struct: `{limit, languages, path|paths, graph_version}`. Filter results through `is_test_symbol()` to exclude test functions.
- `src/mcp.rs` -- Add to MCP tool list

**Estimated LOC:** ~120

#### Step 7.2: Add unused_imports query and RPC method

**Files changed:**
- `src/db/mod.rs` -- Add `unused_imports(limit, languages, paths, graph_version) -> Result<Vec<Edge>>`:
  ```sql
  SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
         e.target_qualname, e.detail, e.evidence_snippet,
         e.evidence_start_line, e.evidence_end_line, e.confidence,
         e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
  FROM edges e
  JOIN files f ON e.file_id = f.id
  WHERE e.kind = 'IMPORTS'
    AND e.graph_version = ?
    AND (f.deleted_version IS NULL OR f.deleted_version > ?)
    AND e.target_qualname IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM edges e2
      WHERE e2.kind = 'CALLS'
        AND e2.file_id = e.file_id
        AND e2.target_qualname = e.target_qualname
        AND e2.graph_version = ?
    )
  ORDER BY f.path, e.evidence_start_line
  LIMIT ?
  ```
- `src/rpc.rs` -- Add `"unused_imports"` match arm. Add `UnusedImportsParams` struct.
- `src/model.rs` -- Add `UnusedImportResult` struct: `{ edge: Edge, source_symbol: Option<Symbol> }`

**Estimated LOC:** ~100

#### Step 7.3: Add orphan_tests query and RPC method

**Files changed:**
- `src/db/mod.rs` -- Add `orphan_tests(limit, languages, paths, graph_version) -> Result<Vec<Symbol>>`:
  1. Find all test symbols (using `is_test_symbol` heuristic via name pattern)
  2. For each test, extract target name via `extract_test_target_name()`
  3. Check if a symbol with that name exists in the graph
  4. If not, the test is an orphan

Implementation as SQL + Rust hybrid:
```rust
fn orphan_tests(limit, languages, paths, graph_version) -> Result<Vec<Symbol>> {
    let test_symbols = find_test_symbols(limit * 3, languages, paths, graph_version)?;
    let mut orphans = Vec::new();
    for test in test_symbols {
        if let Some(target_name) = extract_test_target_name(&test.name) {
            let exists = find_symbols(&target_name, 1, languages, graph_version)?;
            if exists.is_empty() {
                orphans.push(test);
            }
        }
    }
    orphans.truncate(limit);
    Ok(orphans)
}
```
- `src/rpc.rs` -- Add `"orphan_tests"` match arm. Add `OrphanTestsParams` struct.

**Estimated LOC:** ~100

#### Step 7.4: Integrate staleness metrics into repo_insights

**Files changed:**
- `src/model.rs` -- Add `StalenessMetrics` struct: `dead_symbol_count, unused_import_count, orphan_test_count`
- `src/model.rs` -- Add `staleness: Option<StalenessMetrics>` to `RepoInsights`
- `src/rpc.rs` -- In `"repo_insights"` handler, compute staleness counts:
  ```rust
  let dead_count = db.dead_symbols(0, ...).map(|v| v.len()).unwrap_or(0);
  let unused_count = db.unused_imports(0, ...).map(|v| v.len()).unwrap_or(0);
  let orphan_count = db.orphan_tests(0, ...).map(|v| v.len()).unwrap_or(0);
  ```
  (Using limit=0 to get count only, or add separate count methods)

**Estimated LOC:** ~60

### Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| High false positive rate on dead_symbols (exported symbols appear dead) | High | The entry point exclusion list handles most cases. For languages with module-level exports (JS/TS), check if symbol is in a file matching `index.{ts,js}` or has `export` in signature. Target: < 20% false positive rate. |
| unused_imports query is slow (NOT EXISTS subquery) | Low | Both `edges.file_id` and `edges.target_qualname` are indexed. The query touches edges table twice but with index support. |
| extract_test_target_name fails for non-standard naming | Medium | This is a heuristic. The existing implementation in `test_detection.rs` handles `test_foo`, `TestFoo`, `FooSpec` patterns. Unknown patterns are skipped (not reported as orphan). |

### Testing Strategy

**Unit tests:**
- `dead_symbols` returns symbols with zero fan-in, excludes `main`, `__init__`, test functions
- `unused_imports` returns IMPORTS edges with no corresponding CALLS in same file
- `orphan_tests` returns test functions whose target symbol name doesn't exist

**Integration tests (dpb repo):**
- `dead_symbols()` returns non-empty results (some symbols are always dead)
- `dead_symbols()` does NOT include `Main`, gRPC handlers, or test functions
- `unused_imports()` returns results (some imports are typically unused)
- `repo_insights()` includes `staleness` section with counts

**Success criteria:**
- `dead_symbols()` false positive rate < 20% (manual validation on 50 results)
- `unused_imports()` correctly identifies unused imports
- `orphan_tests()` identifies tests for deleted code
- All three methods complete in < 100ms

---

## Cross-Epic Concerns

### Schema Migration Sequencing

All new migrations must be additive (CREATE TABLE IF NOT EXISTS, ALTER TABLE ADD COLUMN IF NOT EXISTS). No destructive changes.

| Migration Version | Epic | Change |
|-------------------|------|--------|
| 10 | Epic 1 | `CREATE INDEX idx_edges_target_qualname ON edges(target_qualname)` |
| 10 | Epic 1 | `CREATE INDEX idx_symbols_name_kind ON symbols(name, kind)` |
| 11 | Epic 3 | `CREATE TABLE co_changes(...)` with indexes |

Epics 2, 4, 5, 6, 7 require no schema changes -- they use existing tables.

### New Files Summary

| File | Epic | Purpose | Est. LOC |
|------|------|---------|----------|
| `src/repo_map.rs` | 2 | Compact architecture digest assembly | 350 |
| `src/git_mining.rs` | 3 | Git log parsing and co-change mining | 400 |
| `src/diff_parser.rs` | 5 | Unified diff parsing and symbol mapping | 250 |
| `src/risk_model.rs` | 5 | Risk assessment and review checklist | 300 |

### Modified Files Summary

| File | Epics | Changes |
|------|-------|---------|
| `src/db/mod.rs` | 1, 2, 3, 7 | New query methods, fuzzy resolution at insert time |
| `src/db/migrations.rs` | 1, 3 | Schema version 10-11 |
| `src/rpc.rs` | All | New method handlers, method docs, METHOD_LIST entries |
| `src/mcp.rs` | 2, 3, 7 | MCP tool list additions |
| `src/model.rs` | 2, 3, 5, 7 | New result structs |
| `src/impact/types.rs` | 1 | Enable test/historical layers by default |
| `src/impact/layers/historical.rs` | 3 | Use co_changes table instead of graph version diff |
| `src/impact/layers/direct.rs` | 1 | Already modified (no further changes needed) |
| `src/gather_context.rs` | 4 | Symbol-level strategy, cross-file expansion, tiered rendering |
| `src/indexer/mod.rs` | 1 | (No direct changes -- insert_edges is in db/mod.rs) |

### Execution Phasing

**Phase 1 (parallel, ~3 weeks):**
- Epic 1 Steps 1.1-1.3 (cross-file impact completion)
- Epic 2 Steps 2.1-2.3 (repo_map)
- Epic 7 Steps 7.1-7.4 (dead code detection)

**Phase 2 (parallel, ~3 weeks, after Phase 1):**
- Epic 3 Steps 3.1-3.4 (git co-change intelligence)
- Epic 4 Steps 4.1-4.4 (precise context assembly)

**Phase 3 (parallel, ~4 weeks, after Phase 2):**
- Epic 5 Steps 5.1-5.5 (change review workflow)
- Epic 6 Steps 6.1-6.4 (cross-language tracing)

### Agent Decision Authority

**Agents can decide without escalation:**
- Internal variable names, function signatures within the stated interfaces
- Choice of SQL query optimization approach (as long as results are correct)
- Error message wording and logging detail
- Test file organization and naming
- Performance optimizations that do not change the API contract

**Agents must escalate:**
- Any schema change not described in this plan
- Adding new RPC methods not specified here
- Changing existing RPC method signatures (adding required params, changing return shape)
- Adding new crate dependencies
- Changing the migration version numbering scheme
- Any change to how the MCP protocol layer works

### Open Questions

1. **Fan-in pre-computation (Epic 2):** Should we store fan-in counts in `symbol_metrics` during indexing, or compute them at query time? Query-time is simpler but slower for large repos. **Recommendation:** Query-time for Phase 1, pre-compute in Phase 2 if latency exceeds 200ms.

2. **Git mining trigger (Epic 3):** Should mining happen automatically on `reindex`, or only when explicitly requested via `mine_git: true`? **Recommendation:** Explicit opt-in initially. Auto-mine when co_changes table is empty and git repo is detected.

3. **Orphan test heuristic quality (Epic 7):** The `extract_test_target_name()` function may miss non-standard naming patterns. **Recommendation:** Accept the limitation. Document the supported patterns. Offer `pattern` param for custom regex in future iteration.
