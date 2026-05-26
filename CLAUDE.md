## Build & test

```bash
# Install dependencies (Rust toolchain required)
cargo fetch

# Build
cargo build

# Build (release)
cargo build --release

# Run all tests
cargo test

# Run a specific test
cargo test <test_name>

# Integration tests (in tests/ directory)
cargo test --test <test_file_name>

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt

# Format check (CI-style, no write)
cargo fmt -- --check
```

## Commit conventions

- Past tense, descriptive title (no Conventional Commits prefixes)
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

## Agent skills

### Issue tracker

GitHub Issues on JacobSoderblom/lidx. See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary (needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (one CONTEXT.md + docs/adr/ at root). See `docs/agents/domain.md`.
