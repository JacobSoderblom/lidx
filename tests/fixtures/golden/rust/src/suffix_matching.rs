pub mod util {
    pub mod log {
        /// Decoy: a local module happening to be named `log`, like the
        /// external `log` crate `use log::info` below refers to. Must
        /// never suffix-match it.
        pub fn info() -> &'static str {
            "local"
        }
    }
}

pub mod other {
    /// Second decoy, so the name-based fallback this miss falls through to
    /// (Rust's import-miss policy) is also ambiguous, rather than
    /// accidentally landing on `util::log::info` by being the only
    /// `info` left.
    pub fn info() -> &'static str {
        "other"
    }
}

use log::info;

/// `log` here names an external crate this repo doesn't index — nothing
/// in this file's own `crate::`/`self::`/`super::` tree is called `log`.
/// The import tier must miss outright (never suffix-match `util::log`'s
/// `info`, a same-named *local* module); falling through to the
/// name-based tiers must also refuse, ambiguous between the two decoys
/// above.
pub fn run() -> &'static str {
    info()
}
