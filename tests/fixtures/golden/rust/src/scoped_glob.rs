pub mod glob_source {
    /// Decoy: same name as `fixture_source::build`, reached only through
    /// the file-scope glob import below.
    pub fn build() -> &'static str {
        "glob"
    }
}

pub mod fixture_source {
    /// Decoy: same name as `glob_source::build`, reached only through
    /// `tests`' own scoped `use`.
    pub fn build() -> &'static str {
        "fixture"
    }
}

use glob_source::*;

/// `use glob_source::*` at file scope binds no specific name (a glob binds
/// no single resolvable name), so this bare call must stay unresolved —
/// not fall through to `fixture_source::build` by way of the unrelated
/// `tests` module's own (differently scoped) `use`.
pub fn run() -> &'static str {
    build()
}

pub mod tests {
    use crate::scoped_glob::fixture_source::build;

    /// `tests`' own `use`, scoped to `tests` only — an import-tier match,
    /// isolated from `run`'s glob above.
    pub fn uses_fixture_build() -> &'static str {
        build()
    }
}
