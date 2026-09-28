---
name: "lidx-rust-workflows"
description: "Common Rust development workflows for lidx. Use when adding migrations, RPC methods, language extractors, or writing tests."
---

# lidx Development Workflows

## When to Use This Skill

- Adding a new RPC method
- Adding a new language extractor
- Adding a database migration
- Writing or running tests
- Running the project locally

---

## Common Workflows

### Adding a New RPC Method

1. **Define params** in `src/rpc/mod.rs`:
   ```rust
   #[derive(Deserialize, schemars::JsonSchema)]
   struct MyMethodParams {
       field: String,
       limit: Option<usize>,
       #[serde(flatten)]
       common: CommonParams, // or LangVersionParams if it doesn't filter by path
   }
   ```

2. **Add the method name** to `METHOD_LIST` (same file)

3. **Add a dispatch arm** in `handle_method()` (same file):
   ```rust
   "my_method" => handlers::handle_my_method(indexer, params)?,
   ```
   Route to `handlers::` for a one-off addition, or a dedicated feature
   module (declared with `mod reading;` etc. in `rpc/mod.rs`, following the
   same `use super::*;` wiring as `handlers`/`reading`) when the method and
   its helpers are substantial enough to deserve their own file -- see
   `src/rpc/reading.rs` (`outline`/`read_symbol`) for the pattern.

4. **Register the schema** in `src/rpc/schema.rs`'s `method_param_schema()`:
   ```rust
   "my_method" => schema_value::<MyMethodParams>(),
   ```
   (add `MyMethodParams` to that function's `use super::{...}` import list too)

5. **Implement the handler**, in `src/rpc/handlers.rs` or the feature module
   from step 3:
   ```rust
   pub(super) fn handle_my_method(indexer: &mut Indexer, params: Value) -> Result<Value> {
       let params: MyMethodParams = serde_json::from_value(params)?;
       let result = json!({ "data": data, "next_hops": next_hops });
       Ok(result)
   }
   ```

6. **Add a response type** to `src/model.rs` if the shape is reused or worth
   typing instead of ad hoc `json!` (derive `Debug, Serialize, Clone`, and
   `#[serde(skip_serializing_if = "Option::is_none")]` on optional fields so
   they're omitted rather than emitted as `null`)

7. **Add a DB query** to `src/db/` (e.g. `graph_query.rs`, `overview.rs`) if needed

8. **Write tests** by calling `rpc::handle_method(&mut indexer, "my_method", json!({...}))`
   directly -- see `tests/reading_methods.rs` for the pattern

---

### Adding a Language Extractor

1. **Add tree-sitter dependency**: `cargo add tree-sitter-{lang}`

2. **Create `src/indexer/{lang}.rs`**:
   ```rust
   pub struct LangExtractor { parser: Parser }
   impl LangExtractor {
       pub fn new() -> Result<Self> { /* init parser */ }
       pub fn extract(&mut self, source: &str, module: &str) -> Result<ExtractedFile> { /* walk AST */ }
   }
   ```

3. **Register in `src/indexer/mod.rs`**: add to LanguageKind enum, instantiate in `Indexer::new_with_options`, add match in `index_file`

4. **Map extensions in `src/indexer/scan.rs`**: add to the extension match

5. **Write test**: `tests/{lang}_extract.rs` with fixture in `tests/fixtures/sample.{ext}`

---

### Adding a DB Migration

1. Open `src/db/migrations.rs`
2. Bump `SCHEMA_VERSION` (currently 11)
3. Add version-gated block:
   ```rust
   if existing < 12 {
       if !has_column(conn, "table", "new_col")? {
           conn.execute("ALTER TABLE table ADD COLUMN new_col TEXT", [])?;
       }
   }
   ```
4. Add index if the column will be queried:
   ```rust
   conn.execute("CREATE INDEX IF NOT EXISTS idx_table_col ON table(new_col)", [])?;
   ```

---

### Writing Tests

- **Integration tests**: `tests/` directory, each file is a separate test binary
- **Fixtures**: `tests/fixtures/` -- real source files for extraction tests
- **Pattern**: create temp dir, init Indexer, index fixtures, assert on Db query results
- **Run all**: `cargo test`
- **Run specific**: `cargo test --test my_test_file`
- **With output**: `cargo test -- --nocapture`
- **Single test fn**: `cargo test --test my_test_file test_fn_name`

---

### Running the Project

```bash
# MCP server
cargo run --release -- mcp-serve --repo /absolute/path/to/repo

# JSONL RPC server
cargo run --release -- serve --repo /absolute/path/to/repo

# One-shot reindex
cargo run --release -- reindex --repo /absolute/path/to/repo

# Single RPC request
cargo run --release -- request --repo /path --method find_symbol --params '{"query":"Indexer"}'

# Test a method via pipe
echo '{"method":"repo_overview","params":{"summary":true}}' | cargo run --release -- serve --repo /path
```

Always use absolute paths -- `~` and relative paths are not expanded.

---

### Pre-commit Checks

```bash
cargo fmt --check && cargo clippy && cargo test
```

---

## Quick Reference

**Add RPC method**: params struct (rpc/mod.rs) -> METHOD_LIST -> dispatch arm in handle_method -> schema in rpc/schema.rs -> handler (handlers.rs or a feature module) -> model type -> DB query -> tests via rpc::handle_method
**Add extractor**: tree-sitter dep -> `indexer/{lang}.rs` -> register in `mod.rs` -> map extension in `scan.rs` -> test
**Add migration**: bump SCHEMA_VERSION -> version-gated block in `migrations.rs`
**Run tests**: `cargo test` or `cargo test --test <name>`
**Run server**: `cargo run --release -- mcp-serve --repo /absolute/path`
