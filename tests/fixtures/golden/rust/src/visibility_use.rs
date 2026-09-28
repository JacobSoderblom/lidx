pub mod inner {
    /// Real target of the `pub(crate) use` below.
    pub fn shout() -> &'static str {
        "loud"
    }
}

/// `pub(crate) use` (any `pub(...)` visibility, not just plain `use`/`pub
/// use`) must still be parsed as an import binding, not treated as
/// unparseable text. Absolute (`crate::`-rooted) target, so this exercises
/// only visibility-prefix parsing, not the separate (unimplemented)
/// question of a bare `use inner::x` resolving relative to this module.
pub(crate) use crate::visibility_use::inner::shout;

pub fn calls_shout() -> &'static str {
    shout()
}
