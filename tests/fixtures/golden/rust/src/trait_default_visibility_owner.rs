pub trait Announcer {
    /// Default method, not overridden anywhere in this fixture. No `pub`
    /// (trait-body items don't carry one) but exactly as visible as the
    /// trait itself -- must never be recorded private (issue #75
    /// follow-up, finding B).
    fn zannounce(&self) -> &'static str {
        "default"
    }
}

pub struct Quiet;

impl Announcer for Quiet {}
