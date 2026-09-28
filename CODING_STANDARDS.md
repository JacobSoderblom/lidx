Use `anyhow::Result` for fallible functions in application code. Use specific error types only at module boundaries that are part of the public API.

---

Prefer `?` operator over `.unwrap()` or `.expect()`. Reserve `.unwrap()` for cases where the invariant is proven and a panic is the correct behavior (document why with a comment). Never use `.unwrap()` in production code paths without justification.

---

All public functions and types must have doc comments (`///`). Internal helper functions do not require doc comments unless their purpose is non-obvious.

---

## Edge Kinds and Domain Vocabulary

Use the vocabulary defined in `CONTEXT.md`. When adding or modifying edge kinds, symbol types, or graph traversal logic, check CONTEXT.md for the correct terminology. Key terms: Symbol Graph, Edge Kind, Bridge Edge, XREF. See CONTEXT.md for full definitions and anti-patterns to avoid.

---

## Testing

### Core Principle

Tests verify behavior through public interfaces, not implementation details. Code can change entirely; tests shouldn't break unless behavior changed.

### Good Tests

Integration-style tests that exercise real code paths through public APIs. They describe _what_ the system does, not _how_.

```rust
// GOOD: Tests observable behavior through the public interface
#[test]
fn index_file_produces_expected_symbols() {
    let db = setup_test_db();
    index_file(&db, "fixtures/sample.rs");
    let symbols = query_symbols(&db, "sample.rs");
    assert!(symbols.iter().any(|s| s.name == "main"));
}
```

- Test behavior users/callers care about
- Use the public API only
- Survive internal refactors
- One logical assertion per test

### Bad Tests

```rust
// BAD: Tests internal implementation detail
#[test]
fn walk_node_calls_visit_in_order() {
    // Testing call order of internal traversal is fragile
}

// BAD: Trivial function where test mirrors implementation
#[test]
fn span_returns_adjusted_coordinates() {
    let node = mock_node(0, 0, 5, 10, 0, 50);
    assert_eq!(span(&node), (1, 1, 6, 11, 0, 50));
    // This test just restates the +1 arithmetic
}
```

Red flags:

- Mocking internal collaborators (your own structs/modules)
- Testing private functions directly
- Test breaks when refactoring without behavior change
- Test name describes HOW not WHAT

### Mocking

Mock at **system boundaries** only:
- File system or databases when a real instance isn't practical
- External processes (tree-sitter parser edge cases)
- Time/randomness

**Never mock your own modules or internal types.** If something is hard to test without mocking internals, redesign the interface.

### TDD Workflow: Vertical Slices

One test, one implementation, repeat:

```
RED->GREEN: test1->impl1
RED->GREEN: test2->impl2
RED->GREEN: test3->impl3
```

Each test responds to what you learned from the previous cycle. Never refactor while RED.

---

## Module Organization

Prefer deep modules: small public interface, deep implementation. A few public functions with simple parameters hiding complex logic.

When adding new functionality, check if it belongs in an existing module before creating a new one. The module structure in `src/` reflects architectural boundaries — do not create modules for single-use helpers.

---

## Commit Conventions

- Past tense, descriptive title (no Conventional Commits prefixes like `feat:` or `fix:`)
- No `Co-Authored-By` lines
- No Claude Code mentions
- Body format when non-trivial:

```
<Short descriptive title in past tense>

Summary
<1–3 sentences explaining what and why.>

Key changes
- <What changed and why it matters>
```

---

## SQL and Schema

Migrations live in `src/db/migrations/`. Use the existing migration numbering scheme. All schema changes require a migration. Never modify existing migrations — always create a new one.

---

## Error Handling

- Use `anyhow::Context` to add context to errors as they propagate up
- Log at the boundary (RPC handler), not deep in library code
- Return errors, don't panic — the MCP server must stay up
