use crate::trait_default_visibility_owner::{Announcer, Quiet};

/// UFCS call to the trait's default method, from a different file than
/// where it's declared. Before the fix, `Announcer::zannounce` was
/// wrongly recorded private (no `pub` on a trait-body default method), so
/// this bare-name fallback call would refuse.
pub fn call_default_announce(q: &Quiet) -> &'static str {
    Quiet::zannounce(q)
}
