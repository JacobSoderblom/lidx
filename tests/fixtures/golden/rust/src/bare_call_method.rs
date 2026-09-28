pub struct Widget;

impl Widget {
    /// Same name as `bare_caller`'s target call below -- the only `process`
    /// in this fixture, and it's a `method`-kind symbol. Before #75, a
    /// receiver-less `process()` call would fuzzy-bind to this via the
    /// bare-name fallback (`LIKE '%::process'`); #75 bars a bare call (no
    /// receiver at all) from binding to any `method`-kind candidate, so
    /// this must stay `UNRESOLVED` instead.
    pub fn process(&self) -> &'static str {
        "widget"
    }
}

/// Bare call, no receiver: `process` is not declared or imported into this
/// module, so the only way it could resolve is the name fallback -- which
/// must refuse a `method`-kind candidate for a receiver-less call.
pub fn bare_caller() -> &'static str {
    process()
}
