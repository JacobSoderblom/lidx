/// Same name as `ambiguous_fallback_y::shared_probe` -- both public, both
/// free functions, neither imported by `ambiguous_fallback_caller`. #75
/// fixture: the guarded name fallback must see two same-language,
/// same-kind, visible candidates and refuse (`Unresolved(ambiguous)`),
/// not silently pick whichever SQLite returns first.
pub fn shared_probe() -> &'static str {
    "x"
}
