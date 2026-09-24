use super::*;

/// Bare call to `owner_secret`, private to the parent module
/// `child_visibility_owner`. A child module (this one) can still see it —
/// Rust module-privacy flows downward, not just within the same file. No
/// import candidate reaches this call: `use super::*` is a glob, which
/// binds no single resolvable name.
pub fn call_owner_secret() -> &'static str {
    owner_secret()
}
