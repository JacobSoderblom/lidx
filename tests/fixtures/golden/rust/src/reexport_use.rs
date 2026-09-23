pub mod real {
    /// Real target of the re-export chain below.
    pub fn announce() -> &'static str {
        "real"
    }
}

mod prelude {
    pub use crate::reexport_use::real::announce;
}

use prelude::announce;

/// An import bound through a same-repo re-export chain (`pub use` inside a
/// `prelude`-style module) has no single resolvable target, but Rust's
/// import-miss policy still falls through to the name-based tiers —
/// unlike a genuinely external miss.
pub fn calls_announce() -> &'static str {
    announce()
}
