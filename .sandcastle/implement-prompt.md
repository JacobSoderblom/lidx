# TASK

Fix issue #{{ISSUE_NUMBER}}: {{ISSUE_TITLE}}

Pull in the issue using `gh issue view`, with comments. If it has a parent PRD, pull that in too.

Only work on the issue specified.

Work on branch {{BRANCH}}. Make commits, run tests, and close the issue when done.

# CONTEXT

Read `CLAUDE.md` for build instructions and commit conventions.
Read `CONTEXT.md` for domain vocabulary and the Symbol Graph model.
Skim `docs/adr/` for any architectural decisions that bear on this issue.

# EXPLORATION

Explore the repo and fill your context window with relevant information that will allow you to complete the task.

Pay extra attention to test files that touch the relevant parts of the code. Tests in this project live in `#[cfg(test)]` modules within source files and in the `tests/` directory for integration tests.

# EXECUTION

If applicable, use RGR to complete the task.

1. RED: write one test
2. GREEN: write the implementation to pass that test
3. REPEAT until done
4. REFACTOR the code

# FEEDBACK LOOPS

Before committing, run all three checks:

1. `cargo fmt -- --check` — formatting
2. `cargo clippy -- -D warnings` — lints
3. `cargo test` — all tests pass

If `cargo fmt -- --check` fails, run `cargo fmt` to fix formatting, then re-check.

# COMMIT

Make a git commit. The commit message must follow the project conventions:

1. Past tense, descriptive title (NO conventional commit prefixes like `feat:` or `fix:`)
2. NO `Co-Authored-By` lines
3. NO Claude Code mentions
4. Body format for non-trivial changes:

```
<Short descriptive title in past tense>

Summary
<1–3 sentences explaining what and why.>

Key changes
- <What changed and why it matters>
```

Keep it concise.

# THE ISSUE

If the task is not complete, leave a comment on the GitHub issue with what was done.

Do not close the issue - this will be done later.

Once complete, output <promise>COMPLETE</promise>.

# FINAL RULES

ONLY WORK ON A SINGLE TASK.
