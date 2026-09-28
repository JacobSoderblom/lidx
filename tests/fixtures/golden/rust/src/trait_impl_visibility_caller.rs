use crate::trait_impl_visibility_owner::Conf;

/// UFCS call to the trait-impl method, from a different file than where
/// it's declared. Before the fix, `Default::default` was wrongly recorded
/// private (no `pub` on a trait impl method), so this bare-name fallback
/// call would refuse.
pub fn make_conf() -> Conf {
    Conf::default()
}
