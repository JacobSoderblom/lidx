/// Bare, unimported call to a name declared identically (public, free
/// function) in two other files -- see `ambiguous_fallback_x`/`_y`.
pub fn calls_ambiguous_probe() -> &'static str {
    shared_probe()
}
