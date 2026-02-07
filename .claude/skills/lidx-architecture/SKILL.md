---
name: "lidx-architecture"
description: "Navigate lidx's architecture and codebase. Use when designing features, understanding module boundaries, or finding where things live."
---

# lidx Architecture Navigation

## When to Use This Skill

- Designing a new feature that spans multiple modules
- Understanding module boundaries before making changes
- Finding where a specific piece of functionality lives
- Planning architectural changes

---

## Module Map

```
src/
  main.rs            -- Entry point, clap dispatch to serve/reindex/mcp-serve
  cli.rs             -- CLI arg definitions (clap Parser/Subcommand)
  lib.rs             -- Module declarations
  config.rs          -- Runtime config, env var overrides (LIDX_*)
  model.rs           -- All response/output types (Symbol, Edge, etc.)

  rpc.rs             -- JSONL RPC dispatch: handle_method() with ~30 methods
  mcp.rs             -- MCP stdio server: wraps rpc as single "lidx" tool

  db/
    mod.rs           -- Db struct, r2d2 pool, all SQL queries as methods
    migrations.rs    -- Schema DDL, version-gated ALTER TABLE migrations

  indexer/
    mod.rs           -- Indexer struct, orchestrates scanning + extraction + DB writes
    scan.rs          -- File discovery (ignore crate), extension-to-language mapping
    batch.rs         -- Batched DB writes for symbols/edges
    extract.rs       -- SymbolInput, EdgeInput, ExtractedFile types
    differ.rs        -- Symbol diff detection between versions
    stable_id.rs     -- Content-hash-based stable symbol identifiers
    python.rs        -- Python tree-sitter extractor
    rust.rs          -- Rust tree-sitter extractor
    javascript.rs    -- JS/TS/TSX tree-sitter extractors
    csharp.rs        -- C# tree-sitter extractor
    sql.rs           -- SQL tree-sitter extractor
    markdown.rs      -- Markdown heading extractor
    proto.rs         -- Protobuf service/method extractor + RPC edge detection
    xref.rs          -- Cross-language reference detection (C#->SQL, C#->Python)
    channel.rs       -- CHANNEL_PUBLISH/SUBSCRIBE detection (message bus)
    http.rs          -- HTTP_CALL/HTTP_ROUTE detection
    test_detection.rs -- Identifies test files and test functions

  impact/
    mod.rs           -- Impact analysis module root
    orchestrator.rs  -- Multi-layer impact orchestration
    layers/          -- Impact layers (direct, test, historical)
    types.rs         -- Impact-specific types
    confidence.rs    -- Confidence scoring for impact edges
    config.rs        -- Impact analysis configuration

  search.rs          -- Text search: ripgrep subprocess + result parsing
  subgraph.rs        -- Multi-root BFS expansion over the symbol graph
  gather_context.rs  -- Budget-aware context assembly (byte/node/depth limits)
  watch.rs           -- File watching (notify crate), debounce, fallback polling
  git_mining.rs      -- Git log mining for co-change history
  metrics.rs         -- File and symbol metrics (LOC, complexity, duplication)
  diagnostics.rs     -- Diagnostic ingestion (SARIF, inline)
  repo_map.rs        -- Repository map generation
  util.rs            -- Shared utilities
```

---

## Data Flow

**Indexing**: filesystem -> `scan.rs` (discover files) -> language extractor (tree-sitter AST -> SymbolInput/EdgeInput) -> post-passes (xref, channel, http) -> `batch.rs` (bulk insert) -> SQLite

**Querying**: stdin -> `mcp.rs`/`rpc.rs` (parse request) -> `handle_method()` (dispatch) -> `Db` methods (SQL queries) -> response JSON with `next_hops` -> stdout

**Watch loop**: `notify` events -> debounce -> batch of changed paths -> `Indexer::sync_paths()` -> re-extract + diff + update DB

---

## Key Types

- `Symbol` / `SymbolCompact` -- full and abbreviated symbol representations
- `Edge` -- graph edge with kind, source/target, evidence, confidence
- `ExtractedFile` -- output of a language extractor (symbols + edges + metrics)
- `Indexer` -- owns all extractors, DB handle, scan options
- `Db` -- SQLite pool + write mutex, all query methods

---

## Finding Things

- **RPC method implementation**: search for `"method_name" =>` in `src/rpc.rs`
- **DB query**: search for the method name in `src/db/mod.rs`
- **Edge kind usage**: grep for the kind string (e.g., `"CHANNEL_PUBLISH"`)
- **Response types**: `src/model.rs`
- **Language-specific extraction**: `src/indexer/{language}.rs`
- **Tests**: `tests/{feature}.rs`, fixtures in `tests/fixtures/`

---

## Quick Reference

**RPC dispatch**: `src/rpc.rs` -> `handle_method()`
**DB queries**: `src/db/mod.rs`
**Migrations**: `src/db/migrations.rs`
**Language extractors**: `src/indexer/{lang}.rs`
**Response types**: `src/model.rs`
**Tests**: `tests/`, fixtures in `tests/fixtures/`
