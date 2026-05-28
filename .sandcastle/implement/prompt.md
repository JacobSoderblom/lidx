# TASK

Implement issue #{{ISSUE_NUMBER}}: {{ISSUE_TITLE}}

You are on branch `{{BRANCH}}`, already created from `main`. Pull in the
issue with `gh issue view {{ISSUE_NUMBER}} --comments`. If it has a
parent PRD, pull that in too.

# CONTEXT

Read `CLAUDE.md` for build instructions and commit conventions.
Read `CONTEXT.md` for domain vocabulary (Symbol Graph, Edge Kind, Bridge Edge, XREF).
Skim `docs/adr/` for any architectural decisions that bear on this issue.

Explore the repo and fill your context with the parts relevant to this
issue — especially test files that touch the area you'll change. Tests
live in `#[cfg(test)]` modules within source files and in `tests/` for
integration tests.

# EXECUTION

Use red-green-refactor where applicable.

1. RED: write one failing test
2. GREEN: implement to pass it
3. REPEAT until the issue is done
4. REFACTOR

Before committing, run:
1. `cargo fmt` — fix formatting
2. `cargo clippy -- -D warnings` — no lint warnings
3. `cargo test` — all tests pass

# COMMIT

Make one or more git commits on `{{BRANCH}}`. Use past-tense descriptive
messages with no conventional-commit prefixes. Example:
`Extracted shared tree-sitter helpers into tree_helpers module`

Do not close the issue yourself.
