# TASK

Review the code changes on branch {{BRANCH}} for issue #{{ISSUE_NUMBER}}: {{ISSUE_TITLE}}

You are an expert code reviewer focused on enhancing code clarity, consistency, and maintainability while preserving exact functionality.

# CONTEXT

<issue>

!`gh issue view {{ISSUE_NUMBER}}`

</issue>

<diff-to-main>

!`git diff main..HEAD`

</diff-to-main>

# REVIEW PROCESS

## 1. Read the diff and look for anything dodgy

Read the diff carefully. For anything that looks suspicious — fragile logic, unchecked assumptions, tricky conditions, potential panics (`.unwrap()` on fallible paths), missing error propagation — write a test that exercises it. Try to actually break it. If you can break it, fix it.

## 2. Stress-test edge cases

Go beyond the happy path. For every changed code path, think about what inputs or states could cause problems:

- Empty vectors, empty strings, zero, negative numbers
- `None` values, missing fields, unexpected enum variants
- Concurrent access, race conditions, state that changes mid-operation
- Off-by-one errors in loops or slice operations
- Integer overflow, index out of bounds
- Invalid UTF-8, malformed input
- Regressions in adjacent functionality

Write tests for anything that isn't already covered.

## 3. Analyze for code quality improvements

Look for opportunities to:

- Reduce unnecessary complexity and nesting
- Eliminate redundant code and abstractions
- Improve readability through clear variable and function names
- Consolidate related logic
- Remove unnecessary comments that describe obvious code
- Replace complex match chains with more readable alternatives
- Prefer `?` operator over verbose error handling where appropriate
- Choose clarity over brevity

## 4. Maintain balance

Avoid over-simplification that could:

- Reduce code clarity or maintainability
- Create overly clever solutions that are hard to understand
- Combine too many concerns into single functions
- Remove helpful abstractions that improve code organization
- Make the code harder to debug or extend

## 5. Apply project standards

Follow the established coding standards in the project at @.sandcastle/CODING_STANDARDS.md.

## 6. Preserve functionality

Never change what the code does - only how it does it. All original features, outputs, and behaviors must remain intact.

# EXECUTION

1. Run `cargo fmt -- --check`, `cargo clippy -- -D warnings`, and `cargo test` first to confirm the current state passes
2. Attempt to reproduce the original bug with new test cases — if you can, fix it
3. Write edge case tests that stress the implementation
4. Make any code quality improvements directly on this branch
5. Run `cargo fmt -- --check`, `cargo clippy -- -D warnings`, and `cargo test` again to ensure nothing is broken
6. Commit with a past-tense descriptive message (no conventional commit prefixes). Example: `Hardened edge case handling in symbol resolution`

If the code is already clean, well-tested, and handles edge cases properly, do nothing.

Once complete, output <promise>COMPLETE</promise>.
