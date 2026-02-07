---
name: "lidx-operations"
description: "Operations and troubleshooting workflows for lidx. Use when debugging indexing issues, MCP server problems, or managing releases."
---

# lidx Operations & Troubleshooting

## When to Use This Skill

- Debugging indexing issues (missing files, wrong symbols, stale index)
- Troubleshooting MCP server problems
- Investigating performance issues
- Creating releases

---

## Common Workflows

### Debugging Indexing Issues

**File not indexed**:
1. Check if the extension is mapped: `src/indexer/scan.rs` has the extension-to-language mapping
2. Check .gitignore: by default lidx respects .gitignore (use `--no-ignore` to override)
3. Check for parse errors: tree-sitter failures produce a module-level fallback symbol only
4. Verify reindex ran: `{"method":"index_status","params":{"include_paths":true}}`

**Symbols missing or wrong**:
1. Check extractor output: write a minimal fixture, run the specific test
2. Qualname construction: Python uses dots, Rust uses `::`, C# uses dots
3. Edge targets: most CALLS edges only have `target_qualname`, not `target_symbol_id`

**Stale index**:
- Force reindex: `{"method":"reindex","params":{}}` or `lidx reindex --repo /path`
- Check changed files: `{"method":"changed_files","params":{}}`
- Watch mode may have missed events: restart the server

---

### Debugging MCP Server Issues

**MCP not responding**:
1. Test directly: `echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{}}}' | cargo run --release -- mcp-serve --repo /path`
2. Check stderr for errors (MCP uses stdout for protocol, stderr for logs)
3. Verify .mcp.json points to correct binary path

**Binary caching**: Claude Desktop and Claude Code cache the lidx binary. To test new builds, use `cargo run --release -- mcp-serve` directly.

**Tool not showing up**: The MCP server exposes one tool `lidx`. If the client doesn't see methods, check the `tools/list` response.

**Wrong repo path**: Use absolute paths. `~/work/repo` is treated as relative. Use `/Users/name/work/repo`.

---

### Performance Issues

**Slow queries**:
- Check `EXPLAIN QUERY PLAN` for the SQL in `src/db/mod.rs`
- Ensure indexes exist for columns in WHERE/JOIN clauses
- Large `edges` table (500k+) is the usual bottleneck
- `references` with `direction=in` does qualname LIKE queries -- ensure idx_edges_target_qualname exists

**Slow indexing**:
- Tree-sitter parsing is fast; DB writes are the bottleneck
- Batch writes are used (`src/indexer/batch.rs`) -- check batch sizes
- WAL mode should be enabled (check PRAGMA journal_mode)
- Large repos (100k+ files): expect 2-5 minutes for full reindex

**SQLite WAL growth**:
- WAL file can grow during bulk indexing
- Checkpointing is automatic; manual: `PRAGMA wal_checkpoint(TRUNCATE)`

---

### Creating a Release

```bash
git tag v0.X.Y
git push origin v0.X.Y
```

This triggers `.github/workflows/release.yml` which:
1. Builds for 4 targets (x86_64/aarch64 x linux/darwin)
2. Packages as `lidx-{target}.tar.gz`
3. Uploads to GitHub Release

**Targets**: x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu, x86_64-apple-darwin, aarch64-apple-darwin

---

## Configuration

**Environment variables** (see `src/config.rs`):
- `LIDX_SEARCH_TIMEOUT_SECS` -- ripgrep timeout (default: 30)
- `LIDX_PATTERN_MAX_LENGTH` -- max regex pattern length (default: 10000)
- `LIDX_POOL_SIZE` -- SQLite read pool size (default: 10)
- `LIDX_POOL_MIN_IDLE` -- minimum idle connections (default: 2)

**CLI flags** on `serve`/`mcp-serve`:
- `--watch auto|on|off` -- file watching mode
- `--watch-debounce-ms 300` -- event coalescing window
- `--watch-fallback-secs 300` -- polling interval when watch unavailable
- `--watch-batch-max 1000` -- threshold for triggering full reindex
- `--no-ignore` -- include .gitignore'd files

---

## Health Checks

```json
{"method": "index_status", "params": {"include_paths": false}}
{"method": "repo_overview", "params": {"summary": true}}
{"method": "list_languages", "params": {}}
{"method": "changed_files", "params": {}}
```

---

## Quick Reference

**Force reindex**: `lidx reindex --repo /path` or `{"method":"reindex","params":{}}`
**Check status**: `{"method":"index_status","params":{}}`
**Test MCP**: pipe initialize JSON to `cargo run --release -- mcp-serve`
**Create release**: `git tag v0.X.Y && git push origin v0.X.Y`
**Env vars**: `src/config.rs`
